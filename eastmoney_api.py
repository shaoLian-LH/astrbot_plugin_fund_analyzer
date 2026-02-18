"""
东方财富数据接口模块
直接使用 aiohttp 调用东方财富 API，绕过 akshare
解决 'Connection aborted' 和 'RemoteDisconnected' 错误
"""

import asyncio
import json
import re
import time
from datetime import datetime, timedelta
from typing import Any, Optional
import aiohttp
import random

from astrbot.api import logger

# 请求头，模拟浏览器访问
HEADERS_LIST = [
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Referer": "https://quote.eastmoney.com/",
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,en-US;q=0.5,en;q=0.3",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Referer": "https://quote.eastmoney.com/",
    },
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh-Hans;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Referer": "https://quote.eastmoney.com/",
    },
]

# 伪 IP 前缀池（用于降低同一来源高频请求的风控概率）
PSEUDO_IP_PREFIXES = [
    "1.12",
    "14.17",
    "27.38",
    "36.112",
    "39.155",
    "42.120",
    "58.30",
    "59.63",
    "101.6",
    "111.13",
    "112.64",
    "113.87",
    "114.80",
    "115.28",
    "116.62",
    "117.79",
    "118.89",
    "119.29",
    "120.92",
    "121.40",
    "123.125",
    "124.65",
    "125.88",
    "139.196",
    "140.205",
    "175.24",
    "180.76",
    "182.254",
    "183.60",
    "218.30",
    "219.135",
    "221.179",
    "222.73",
]

OTC_JSONP_PATTERN = re.compile(r"jsonpgz\((.*?)\)\s*;?\s*$", re.S)

# 超时设置
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=30, connect=15)


class EastMoneyAPI:
    """东方财富数据 API 封装"""

    # API 地址
    # 单只基金/股票实时行情 (场内)
    QUOTE_API = "http://push2.eastmoney.com/api/qt/stock/get"
    # LOF/ETF 基金列表 (备用，可能不稳定)
    LOF_LIST_API = "http://push2.eastmoney.com/api/qt/clist/get"
    # K线历史数据 (场内)
    KLINE_API = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
    # 天天基金搜索 API (更稳定)
    FUND_SEARCH_API = "https://fundsuggest.eastmoney.com/FundSearch/api/FundSearchAPI.ashx"
    # 场外基金实时估值 API
    OTC_FUND_API = "https://fundgz.1234567.com.cn/js/{}.js"
    # 场外基金历史净值 API
    OTC_HISTORY_API = "https://api.fund.eastmoney.com/f10/lsjz"

    def __init__(self):
        # 缓存
        self._lof_list_cache: Optional[list] = None
        self._lof_cache_time: Optional[datetime] = None
        self._cache_ttl = 1800  # 30分钟缓存

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        """安全转换为浮点数"""
        if value is None or value == "":
            return default
        try:
            return float(value)
        except (ValueError, TypeError):
            return default

    @staticmethod
    def _with_cache_buster(params: Optional[dict], key: str) -> dict:
        """为请求参数补充防缓存时间戳"""
        request_params = dict(params or {})
        request_params.setdefault(key, str(int(time.time() * 1000)))
        return request_params

    @staticmethod
    def _create_connector() -> aiohttp.TCPConnector:
        """创建连接器（禁用代理，减少连接复用问题）"""
        return aiohttp.TCPConnector(
            ssl=False,
            force_close=True,
            enable_cleanup_closed=True,
        )

    def _normalize_fund_codes(self, fund_codes: list[str]) -> list[str]:
        """去重并标准化基金代码列表"""
        unique_codes: list[str] = []
        seen = set()
        for code in fund_codes:
            code_str = str(code).strip()
            if not code_str:
                continue
            if code_str.isdigit():
                code_str = code_str.zfill(6)
            if code_str in seen:
                continue
            seen.add(code_str)
            unique_codes.append(code_str)
        return unique_codes

    @staticmethod
    def _normalize_fund_code(fund_code: Any) -> str:
        """标准化单个基金代码（补齐前导 0）"""
        code_str = str(fund_code or "").strip()
        if code_str.isdigit():
            return code_str.zfill(6)
        return code_str

    @staticmethod
    def _price_gap_ratio(price_a: Any, price_b: Any) -> float:
        """计算两价格偏离比例（以 price_b 为基准）"""
        a = EastMoneyAPI._safe_float(price_a)
        b = EastMoneyAPI._safe_float(price_b)
        if a <= 0 or b <= 0:
            return 0.0
        return abs(a - b) / b

    def _parse_otc_valuation_jsonp(
        self, text: str, fund_code: str
    ) -> Optional[dict]:
        """解析场外基金 JSONP 估值响应"""
        match = OTC_JSONP_PATTERN.search(text.strip())
        if not match:
            logger.debug(f"场外基金估值响应格式异常: {fund_code}")
            return None

        try:
            data = json.loads(match.group(1))
        except json.JSONDecodeError as e:
            logger.debug(f"场外基金估值 JSON 解析失败: {fund_code}, {e}")
            return None

        estimate_value = self._safe_float(data.get("gsz"))
        unit_value = self._safe_float(data.get("dwjz"))
        latest_value = estimate_value if estimate_value > 0 else unit_value
        change_amount = estimate_value - unit_value if unit_value > 0 else 0.0

        return {
            "code": data.get("fundcode", fund_code),
            "name": data.get("name", ""),
            # 估值缺失时回退到最新单位净值，保证现价可用
            "latest_price": latest_value,  # 兼容旧字段
            "estimate_value": estimate_value,  # 估算净值
            "prev_close": unit_value,  # 兼容旧字段
            "unit_value": unit_value,  # 单位净值
            "change_rate": self._safe_float(data.get("gszzl")),  # 估算涨跌幅
            "change_amount": change_amount,  # 估算涨跌额
            "update_time": data.get("gztime", ""),
            "valuation_date": data.get("jzrq", ""),
            "is_otc": True,  # 标记为场外基金
        }

    def _random_pseudo_ip(self) -> str:
        """生成伪造来源 IP（请求头层面）"""
        prefix = random.choice(PSEUDO_IP_PREFIXES)
        return f"{prefix}.{random.randint(1, 254)}.{random.randint(1, 254)}"

    def _build_headers(self, referer: str = "https://quote.eastmoney.com/") -> dict:
        """构建请求头：随机 UA + 伪 IP 池"""
        headers = dict(random.choice(HEADERS_LIST))
        fake_ip = self._random_pseudo_ip()
        headers.update(
            {
                "Referer": referer,
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "X-Forwarded-For": fake_ip,
                "X-Real-IP": fake_ip,
                "CLIENT-IP": fake_ip,
                "Forwarded": f"for={fake_ip};proto=https",
            }
        )
        return headers

    async def _request(
        self,
        url: str,
        params: dict,
        max_retries: int = 3,
    ) -> Optional[dict]:
        """
        发送 HTTP 请求，带重试机制
        
        Args:
            url: API 地址
            params: 请求参数
            max_retries: 最大重试次数
            
        Returns:
            JSON 响应或 None
        """
        for attempt in range(max_retries):
            try:
                # 随机请求头 + 防缓存参数
                headers = self._build_headers()
                request_params = self._with_cache_buster(params, "_")
                
                # 创建 connector，禁用代理
                connector = self._create_connector()
                
                async with aiohttp.ClientSession(
                    headers=headers,
                    timeout=REQUEST_TIMEOUT,
                    connector=connector,
                    trust_env=False,  # 忽略系统代理设置
                ) as session:
                    async with session.get(url, params=request_params) as response:
                        if response.status == 200:
                            # 有些 API 返回 text/plain，需要手动解析 JSON
                            text = await response.text()
                            try:
                                return json.loads(text)
                            except json.JSONDecodeError as e:
                                logger.warning(f"JSON 解析失败: {e}")
                                return None
                        else:
                            logger.warning(f"HTTP {response.status}: {url}")
            except asyncio.TimeoutError:
                logger.warning(f"请求超时 (第{attempt + 1}次): {url}")
            except aiohttp.ClientError as e:
                logger.warning(f"请求失败 (第{attempt + 1}次): {e}")
            except Exception as e:
                logger.error(f"请求异常: {e}")
            
            # 重试前等待（逐渐增加等待时间）
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 3 + random.uniform(0, 2)
                await asyncio.sleep(wait_time)
        
        return None

    async def _request_text(
        self,
        url: str,
        params: Optional[dict] = None,
        referer: str = "https://fund.eastmoney.com/",
        max_retries: int = 3,
    ) -> Optional[str]:
        """发送文本请求（用于 JSONP 接口）"""
        for attempt in range(max_retries):
            try:
                headers = self._build_headers(referer=referer)
                request_params = self._with_cache_buster(params, "rt")
                connector = self._create_connector()

                async with aiohttp.ClientSession(
                    headers=headers,
                    timeout=REQUEST_TIMEOUT,
                    connector=connector,
                    trust_env=False,
                ) as session:
                    async with session.get(url, params=request_params) as response:
                        if response.status == 200:
                            return await response.text()
                        if response.status == 404:
                            return None
                        logger.warning(f"HTTP {response.status}: {url}")
            except asyncio.TimeoutError:
                logger.warning(f"请求超时 (第{attempt + 1}次): {url}")
            except aiohttp.ClientError as e:
                logger.warning(f"请求失败 (第{attempt + 1}次): {e}")
            except Exception as e:
                logger.error(f"请求异常: {e}")

            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 2 + random.uniform(0, 1)
                await asyncio.sleep(wait_time)

        return None

    def _get_market_code(self, fund_code: str) -> str:
        """
        根据基金代码判断市场代码
        
        Args:
            fund_code: 基金代码
            
        Returns:
            市场代码 (0=深交所, 1=上交所)
        """
        # 上交所: 5开头的ETF/LOF, 6开头的股票
        # 深交所: 1开头的LOF, 0/3开头的股票
        if fund_code.startswith(("5", "6")):
            return "1"
        return "0"

    def _is_otc_fund(self, fund_code: str) -> bool:
        """
        判断是否为场外基金
        
        场外基金代码通常是0开头的6位数字，但不包括以下场内基金:
        - 000xxx 部分是场内基金
        - 00xxxx 部分是场内基金
        
        场内基金代码特征:
        - 1xxxxx: 深交所LOF/ETF
        - 5xxxxx: 上交所ETF
        - 6xxxxx: 上交所股票
        
        场外基金代码特征:
        - 0xxxxx: 大部分场外基金
        - 2xxxxx: 部分场外基金
        - 3xxxxx: 创业板股票 (不处理)
        """
        if not fund_code or len(fund_code) != 6:
            return False
        
        # 1开头或5开头通常是场内ETF/LOF
        if fund_code.startswith(("1", "5")):
            return False
        
        # 0开头的大部分是场外基金
        if fund_code.startswith("0"):
            return True
        
        # 2开头的是场外基金
        if fund_code.startswith("2"):
            return True
        
        return False

    @staticmethod
    def _is_meaningful_realtime(data: Optional[dict]) -> bool:
        """判断实时数据是否包含可用信息（名称或价格）"""
        if not data:
            return False
        name = str(data.get("name") or "").strip()
        latest_price = EastMoneyAPI._safe_float(data.get("latest_price"))
        prev_close = EastMoneyAPI._safe_float(data.get("prev_close"))
        return bool(name) or latest_price > 0 or prev_close > 0

    async def _search_fund_snapshot(self, fund_code: str) -> Optional[dict]:
        """通过搜索接口补齐基金基础信息（名称/净值）"""
        results = await self.search_fund(fund_code, fetch_realtime=False)
        if not results:
            return None

        normalized_target = self._normalize_fund_code(fund_code)
        exact_match = None
        for item in results:
            item_code = self._normalize_fund_code(item.get("code"))
            if item_code == normalized_target:
                exact_match = item
                break

        if exact_match is None:
            return None

        target = exact_match
        latest_price = self._safe_float(target.get("latest_price"))
        return {
            "code": self._normalize_fund_code(target.get("code")) or normalized_target,
            "name": str(target.get("name") or "").strip(),
            "latest_price": latest_price,
            "prev_close": latest_price if latest_price > 0 else 0.0,
            "fund_type": str(target.get("fund_type") or "").strip(),
            "nav_date": str(target.get("nav_date") or "").strip(),
        }

    async def _get_otc_latest_nav_snapshot(self, fund_code: str) -> Optional[dict]:
        """通过历史净值渠道获取最新单位净值快照"""
        history = await self._get_otc_fund_history(fund_code, days=2)
        if not history:
            return None

        latest = history[-1]
        latest_price = self._safe_float(latest.get("close"))
        if latest_price <= 0:
            return None

        prev = history[-2] if len(history) >= 2 else latest
        prev_close = self._safe_float(prev.get("close"))
        return {
            "code": self._normalize_fund_code(fund_code),
            "latest_price": latest_price,
            "prev_close": prev_close if prev_close > 0 else latest_price,
            "change_rate": self._safe_float(latest.get("change_rate")),
            "nav_date": str(latest.get("date") or "").strip(),
        }

    async def get_fund_realtime(self, fund_code: str) -> Optional[dict]:
        """
        获取单只基金实时行情（自动判断场内/场外）
        
        Args:
            fund_code: 基金代码
            
        Returns:
            行情数据字典或 None
        """
        fund_code = self._normalize_fund_code(fund_code)
        if not fund_code:
            return None

        data: Optional[dict] = None
        snapshot: Optional[dict] = None

        # 主路径：按代码特征判断场内/场外
        if self._is_otc_fund(fund_code):
            data = await self._get_otc_fund_realtime(fund_code)
            # 兜底：场外接口失败时尝试场内行情
            if not self._is_meaningful_realtime(data):
                fallback = await self._get_exchange_fund_realtime(fund_code)
                if self._is_meaningful_realtime(fallback):
                    data = fallback
        else:
            data = await self._get_exchange_fund_realtime(fund_code)
            # 兜底：场内接口失败时尝试场外估值
            if not self._is_meaningful_realtime(data):
                fallback = await self._get_otc_fund_realtime(fund_code)
                if self._is_meaningful_realtime(fallback):
                    data = fallback

        if not data:
            data = {"code": fund_code}

        # 先用搜索渠道补齐名称/净值，并处理明显冲突（如 160517 这类净值基金）
        try:
            snapshot = await self._search_fund_snapshot(fund_code)
        except Exception as e:
            logger.debug(f"搜索补齐基金信息失败: {fund_code}, {e}")
            snapshot = None

        if snapshot:
            if not str(data.get("name") or "").strip() and snapshot.get("name"):
                data["name"] = snapshot.get("name")

            current_price = self._safe_float(data.get("latest_price"))
            snapshot_price = self._safe_float(snapshot.get("latest_price"))
            price_conflict = self._price_gap_ratio(current_price, snapshot_price) >= 0.2

            # 场内行情缺失，或与基金净值偏差过大时，优先采用基金净值渠道
            if snapshot_price > 0 and (current_price <= 0 or price_conflict):
                data["latest_price"] = snapshot_price
                data["price_source"] = "fund_search"
                if snapshot.get("name"):
                    data["name"] = snapshot.get("name")
                snapshot_prev_close = self._safe_float(snapshot.get("prev_close"))
                if snapshot_prev_close > 0:
                    data["prev_close"] = snapshot_prev_close

            if self._safe_float(data.get("prev_close")) <= 0 and self._safe_float(
                snapshot.get("prev_close")
            ) > 0:
                data["prev_close"] = self._safe_float(snapshot.get("prev_close"))
            if not data.get("code"):
                data["code"] = snapshot.get("code", fund_code)

        # 历史净值兜底（渠道三）：若当前价格仍不可用，或与净值渠道偏离过大，则回退到最新净值
        current_price = self._safe_float(data.get("latest_price"))
        snapshot_price = self._safe_float(snapshot.get("latest_price")) if snapshot else 0.0
        should_try_nav = current_price <= 0 or self._price_gap_ratio(current_price, snapshot_price) >= 0.2
        if should_try_nav:
            try:
                nav_snapshot = await self._get_otc_latest_nav_snapshot(fund_code)
            except Exception as e:
                logger.debug(f"历史净值渠道补齐失败: {fund_code}, {e}")
                nav_snapshot = None

            if nav_snapshot:
                nav_price = self._safe_float(nav_snapshot.get("latest_price"))
                nav_conflict = self._price_gap_ratio(current_price, nav_price) >= 0.2
                if nav_price > 0 and (current_price <= 0 or nav_conflict):
                    data["latest_price"] = nav_price
                    data["price_source"] = "fund_nav"
                    nav_prev_close = self._safe_float(nav_snapshot.get("prev_close"))
                    if nav_prev_close > 0:
                        data["prev_close"] = nav_prev_close
                    nav_change_rate = self._safe_float(nav_snapshot.get("change_rate"))
                    if nav_change_rate:
                        data["change_rate"] = nav_change_rate
                    elif self._safe_float(data.get("prev_close")) > 0:
                        change_amount = nav_price - self._safe_float(data.get("prev_close"))
                        data["change_amount"] = change_amount
                        data["change_rate"] = change_amount / self._safe_float(
                            data.get("prev_close")
                        ) * 100
                if self._safe_float(data.get("prev_close")) <= 0 and self._safe_float(
                    nav_snapshot.get("prev_close")
                ) > 0:
                    data["prev_close"] = self._safe_float(nav_snapshot.get("prev_close"))
                if nav_snapshot.get("nav_date"):
                    data["nav_date"] = nav_snapshot.get("nav_date")

        # 价格兜底：latest_price 缺失时用可用的净值/昨收补齐
        if self._safe_float(data.get("latest_price")) <= 0:
            for key in ("estimate_value", "unit_value", "prev_close"):
                fallback_price = self._safe_float(data.get(key))
                if fallback_price > 0:
                    data["latest_price"] = fallback_price
                    break

        # 名称或价格缺失时，使用搜索接口二次补齐
        name = str(data.get("name") or "").strip()
        latest_price = self._safe_float(data.get("latest_price"))
        if not name or latest_price <= 0:
            if snapshot:
                if not name and snapshot.get("name"):
                    data["name"] = snapshot.get("name")
                if latest_price <= 0 and self._safe_float(snapshot.get("latest_price")) > 0:
                    data["latest_price"] = self._safe_float(snapshot.get("latest_price"))
                if self._safe_float(data.get("prev_close")) <= 0 and self._safe_float(
                    snapshot.get("prev_close")
                ) > 0:
                    data["prev_close"] = self._safe_float(snapshot.get("prev_close"))
                if not data.get("code"):
                    data["code"] = snapshot.get("code", fund_code)

        if not self._is_meaningful_realtime(data):
            return None
        return data

    async def _get_otc_fund_realtime(self, fund_code: str) -> Optional[dict]:
        """
        获取场外基金实时估值
        
        Args:
            fund_code: 基金代码
            
        Returns:
            估值数据字典或 None
        """
        url = self.OTC_FUND_API.format(fund_code)
        text = await self._request_text(
            url=url,
            params={"rt": str(int(time.time() * 1000))},
            referer=f"https://fund.eastmoney.com/{fund_code}.html",
            max_retries=3,
        )
        if not text:
            return None

        return self._parse_otc_valuation_jsonp(text, fund_code)

    async def get_fund_valuation(self, fund_code: str) -> Optional[dict]:
        """
        获取场外基金实时估值（ssgz 指令使用）

        Args:
            fund_code: 基金代码

        Returns:
            估值数据字典或 None
        """
        fund_code = str(fund_code).strip()
        if not fund_code:
            return None
        return await self._get_otc_fund_realtime(fund_code)

    async def get_fund_valuation_batch(
        self,
        fund_codes: list[str],
        max_concurrency: int = 6,
    ) -> dict[str, dict]:
        """
        批量获取场外基金实时估值（并发 + 伪IP池）

        Args:
            fund_codes: 基金代码列表
            max_concurrency: 最大并发数

        Returns:
            {基金代码: 估值数据}
        """
        if not fund_codes:
            return {}

        unique_codes = self._normalize_fund_codes(fund_codes)
        if not unique_codes:
            return {}

        concurrency = max(1, min(max_concurrency, 20))
        semaphore = asyncio.Semaphore(concurrency)

        async def fetch_one(code: str):
            async with semaphore:
                data = await self.get_fund_valuation(code)
                return code, data

        tasks = [asyncio.create_task(fetch_one(code)) for code in unique_codes]
        raw_results = await asyncio.gather(*tasks, return_exceptions=True)

        results: dict[str, dict] = {}
        for item in raw_results:
            if isinstance(item, Exception):
                continue
            code, data = item
            if data:
                results[code] = data

        return results

    async def _get_exchange_fund_realtime(self, fund_code: str) -> Optional[dict]:
        """
        获取场内基金（ETF/LOF）实时行情
        
        Args:
            fund_code: 基金代码
            
        Returns:
            行情数据字典或 None
        """
        market = self._get_market_code(fund_code)
        
        params = {
            "secid": f"{market}.{fund_code}",
            "fields": "f43,f44,f45,f46,f47,f48,f57,f58,f60,f168,f169,f170",
        }
        
        data = await self._request(self.QUOTE_API, params)
        if not data or data.get("rc") != 0:
            return None
        
        result = data.get("data", {})
        if not result:
            return None
        
        def safe_float(val, divisor=1):
            if val is None or val == "-":
                return 0.0
            try:
                return float(val) / divisor
            except (ValueError, TypeError):
                return 0.0
        
        return {
            "code": str(result.get("f57", fund_code)),
            "name": str(result.get("f58", "")),
            "latest_price": safe_float(result.get("f43"), 1000),
            "change_amount": safe_float(result.get("f169"), 1000),
            "change_rate": safe_float(result.get("f170"), 100),
            "open_price": safe_float(result.get("f46"), 1000),
            "high_price": safe_float(result.get("f44"), 1000),
            "low_price": safe_float(result.get("f45"), 1000),
            "prev_close": safe_float(result.get("f60"), 1000),
            "volume": safe_float(result.get("f47")),
            "amount": safe_float(result.get("f48")),
            "turnover_rate": safe_float(result.get("f168"), 100),
        }

    async def get_fund_history(
        self,
        fund_code: str,
        days: int = 30,
        adjust: str = "qfq",
    ) -> Optional[list]:
        """
        获取基金历史数据（自动判断场内/场外）
        
        Args:
            fund_code: 基金代码
            days: 获取天数
            adjust: 复权类型 (qfq=前复权, hfq=后复权, 空=不复权)
            
        Returns:
            历史数据列表或 None
        """
        fund_code = str(fund_code).strip()
        
        # 判断是场内还是场外基金
        if self._is_otc_fund(fund_code):
            primary_history = await self._get_otc_fund_history(fund_code, days)
            if primary_history:
                return primary_history
            return await self._get_exchange_fund_history(fund_code, days, adjust)
        else:
            primary_history = await self._get_exchange_fund_history(fund_code, days, adjust)
            if not primary_history:
                return await self._get_otc_fund_history(fund_code, days)

            # 场内数据与基金净值渠道偏离明显时，优先使用净值历史
            try:
                nav_snapshot = await self._get_otc_latest_nav_snapshot(fund_code)
            except Exception as e:
                logger.debug(f"历史数据渠道对比失败: {fund_code}, {e}")
                nav_snapshot = None

            if nav_snapshot and primary_history:
                latest_close = self._safe_float(primary_history[-1].get("close"))
                nav_price = self._safe_float(nav_snapshot.get("latest_price"))
                if self._price_gap_ratio(latest_close, nav_price) >= 0.2:
                    fallback_history = await self._get_otc_fund_history(fund_code, days)
                    if fallback_history:
                        return fallback_history

            return primary_history

    async def _get_otc_fund_history(
        self,
        fund_code: str,
        days: int = 30,
    ) -> Optional[list]:
        """
        获取场外基金历史净值
        
        Args:
            fund_code: 基金代码
            days: 获取天数
            
        Returns:
            历史数据列表或 None
        """
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://fund.eastmoney.com/",
        }
        
        params = {
            "fundCode": fund_code,
            "pageIndex": "1",
            "pageSize": str(days),
        }
        
        for attempt in range(3):
            try:
                connector = aiohttp.TCPConnector(ssl=False, force_close=True)
                
                async with aiohttp.ClientSession(
                    headers=headers,
                    timeout=REQUEST_TIMEOUT,
                    connector=connector,
                    trust_env=False,
                ) as session:
                    async with session.get(self.OTC_HISTORY_API, params=params) as response:
                        if response.status == 200:
                            text = await response.text()
                            data = json.loads(text)
                            
                            if data.get("ErrCode") != 0:
                                logger.warning(f"获取场外基金历史失败: {data.get('ErrMsg')}")
                                return None
                            
                            lsjz_list = data.get("Data", {}).get("LSJZList", [])
                            if not lsjz_list:
                                return None
                            
                            history = []
                            prev_close = None
                            
                            # 倒序处理（API返回的是从新到旧）
                            for item in reversed(lsjz_list):
                                def safe_float(val):
                                    if val is None or val == "" or val == "--":
                                        return 0.0
                                    try:
                                        return float(val)
                                    except (ValueError, TypeError):
                                        return 0.0
                                
                                close = safe_float(item.get("DWJZ"))
                                
                                # 计算涨跌幅
                                change_rate = 0.0
                                jzzzl = item.get("JZZZL")
                                if jzzzl and jzzzl != "--":
                                    change_rate = safe_float(jzzzl)
                                elif prev_close and prev_close > 0:
                                    change_rate = (close - prev_close) / prev_close * 100
                                
                                history.append({
                                    "date": item.get("FSRQ", ""),
                                    "open": close,  # 场外基金没有开盘价
                                    "close": close,
                                    "high": close,
                                    "low": close,
                                    "volume": 0.0,
                                    "amount": 0.0,
                                    "change_rate": change_rate,
                                })
                                
                                prev_close = close
                            
                            return history
            except Exception as e:
                logger.debug(f"获取场外基金历史失败 (第{attempt + 1}次): {e}")
            
            if attempt < 2:
                await asyncio.sleep((attempt + 1) * 2)
        
        return None

    async def _get_exchange_fund_history(
        self,
        fund_code: str,
        days: int = 30,
        adjust: str = "qfq",
    ) -> Optional[list]:
        """
        获取场内基金（ETF/LOF）历史K线数据
        
        Args:
            fund_code: 基金代码
            days: 获取天数
            adjust: 复权类型
            
        Returns:
            历史数据列表或 None
        """
        market = self._get_market_code(fund_code)
        
        # 复权类型转换
        fq_map = {"qfq": "1", "hfq": "2", "": "0"}
        fq = fq_map.get(adjust, "1")
        
        # 计算日期范围（多获取一些以覆盖节假日）
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days * 3 + 60)
        
        params = {
            "secid": f"{market}.{fund_code}",
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "klt": "101",  # 日K线
            "fqt": fq,
            "beg": start_date.strftime("%Y%m%d"),
            "end": end_date.strftime("%Y%m%d"),
            "lmt": str(days * 3),  # 限制数量
        }
        
        data = await self._request(self.KLINE_API, params)
        if not data or data.get("rc") != 0:
            logger.error(f"获取历史数据失败: {fund_code}")
            return None
        
        result = data.get("data", {})
        klines = result.get("klines", [])
        
        if not klines:
            logger.warning(f"未找到历史数据: {fund_code}")
            return None
        
        history = []
        for line in klines:
            # 格式: 日期,开盘,收盘,最高,最低,成交量,成交额,振幅,涨跌幅,涨跌额,换手率
            parts = line.split(",")
            if len(parts) >= 11:
                try:
                    history.append({
                        "date": parts[0],
                        "open": float(parts[1]),
                        "close": float(parts[2]),
                        "high": float(parts[3]),
                        "low": float(parts[4]),
                        "volume": float(parts[5]),
                        "amount": float(parts[6]),
                        "change_rate": float(parts[8]) if parts[8] else 0.0,
                    })
                except (ValueError, IndexError) as e:
                    logger.debug(f"解析K线数据失败: {line}, 错误: {e}")
                    continue
        
        # 只返回最近 N 天
        return history[-days:] if len(history) > days else history

    async def get_lof_list(self, use_cache: bool = True) -> Optional[list]:
        """
        获取 LOF 基金列表
        
        Args:
            use_cache: 是否使用缓存
            
        Returns:
            基金列表或 None
        """
        now = datetime.now()
        
        # 检查缓存
        if use_cache and self._lof_list_cache is not None:
            if self._lof_cache_time and (now - self._lof_cache_time).total_seconds() < self._cache_ttl:
                logger.debug("使用缓存的LOF基金列表")
                return self._lof_list_cache
        
        # LOF 基金分类: MK0404(上交所LOF), MK0405(深交所LOF), MK0406, MK0407
        params = {
            "pn": "1",
            "pz": "500",  # 每页500条
            "po": "1",
            "np": "1",
            "fltt": "2",
            "invt": "2",
            "fid": "f3",
            "fs": "b:MK0404,b:MK0405,b:MK0406,b:MK0407",
            "fields": "f2,f3,f4,f5,f6,f7,f12,f14,f15,f16,f17,f18",
        }
        
        data = await self._request(self.LOF_LIST_API, params)
        if not data or data.get("rc") != 0:
            logger.error("获取LOF基金列表失败")
            # 如果有旧缓存，返回旧缓存
            if self._lof_list_cache:
                logger.warning("使用过期的缓存数据")
                return self._lof_list_cache
            return None
        
        result = data.get("data", {})
        diff = result.get("diff", [])
        
        if not diff:
            logger.warning("LOF基金列表为空")
            return None
        
        fund_list = []
        for item in diff:
            def safe_float(val, divisor=1):
                if val is None or val == "-":
                    return 0.0
                try:
                    return float(val) / divisor
                except (ValueError, TypeError):
                    return 0.0
            
            fund_list.append({
                "code": str(item.get("f12", "")),
                "name": str(item.get("f14", "")),
                "latest_price": safe_float(item.get("f2")),
                "change_rate": safe_float(item.get("f3")),
                "change_amount": safe_float(item.get("f4")),
                "volume": safe_float(item.get("f5")),
                "amount": safe_float(item.get("f6")),
                "open_price": safe_float(item.get("f17")),
                "high_price": safe_float(item.get("f15")),
                "low_price": safe_float(item.get("f16")),
                "prev_close": safe_float(item.get("f18")),
            })
        
        # 更新缓存
        self._lof_list_cache = fund_list
        self._lof_cache_time = now
        logger.info(f"LOF基金列表获取成功，共 {len(fund_list)} 只基金")
        
        return fund_list

    async def search_fund(self, keyword: str, fetch_realtime: bool = True) -> list:
        """
        搜索基金（使用天天基金搜索 API，更稳定）
        
        Args:
            keyword: 搜索关键词（代码或名称）
            fetch_realtime: 是否获取实时行情（涨跌幅等）
            
        Returns:
            匹配的基金列表
        """
        if not keyword or not keyword.strip():
            return []
        
        keyword = keyword.strip()
        
        params = {
            "m": "1",
            "key": keyword,
        }
        
        data = await self._request(self.FUND_SEARCH_API, params)
        if not data or data.get("ErrCode") != 0:
            logger.warning(f"搜索基金失败: {keyword}")
            return []
        
        datas = data.get("Datas", [])
        if not datas:
            return []
        
        results = []
        for item in datas:
            # 只处理基金类型 (CATEGORY=700)
            category = str(item.get("CATEGORY") or "").strip()
            if category and category != "700":
                continue
            
            code = self._normalize_fund_code(item.get("CODE", ""))
            name = str(item.get("NAME", "")).strip()
            
            # 获取更详细的基金信息
            fund_info = item.get("FundBaseInfo", {})
            
            result = {
                "code": code,
                "name": name,
                "fund_type": fund_info.get("FTYPE", ""),
                "latest_price": 0.0,
                "change_rate": 0.0,
                "change_amount": 0.0,
                "nav_date": str(
                    fund_info.get("PDATE")
                    or fund_info.get("JZRQ")
                    or fund_info.get("GXRQ")
                    or ""
                ).strip(),
            }
            
            # 如果有净值信息
            if fund_info:
                try:
                    dwjz = fund_info.get("DWJZ")
                    if dwjz is not None:
                        result["latest_price"] = float(dwjz)
                except (ValueError, TypeError):
                    pass
            
            results.append(result)
            
            if len(results) >= 10:  # 最多返回10条
                break
        
        # 获取实时行情数据（涨跌幅等）
        if fetch_realtime and results:
            await self._enrich_with_realtime(results)
        
        return results
    
    async def _enrich_with_realtime(self, fund_list: list) -> None:
        """
        为基金列表补充实时行情数据
        
        Args:
            fund_list: 基金列表（会被原地修改）
        """
        # 并发获取所有基金的实时行情
        tasks = []
        for fund in fund_list:
            code = fund.get("code", "")
            if code:
                tasks.append(self.get_fund_realtime(code))
        
        if not tasks:
            return
        
        realtime_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for i, realtime in enumerate(realtime_results):
            if isinstance(realtime, Exception) or realtime is None:
                continue
            
            fund = fund_list[i]
            # 更新实时数据
            if realtime.get("latest_price"):
                fund["latest_price"] = realtime["latest_price"]
            if realtime.get("change_rate") is not None:
                fund["change_rate"] = realtime["change_rate"]
            if realtime.get("change_amount") is not None:
                fund["change_amount"] = realtime["change_amount"]
    
    async def validate_fund_code(self, fund_code: str) -> bool:
        """
        验证基金代码是否有效
        
        Args:
            fund_code: 基金代码
            
        Returns:
            是否有效
        """
        fund_code = str(fund_code).strip()
        
        # 使用搜索 API 验证
        results = await self.search_fund(fund_code)
        for r in results:
            if r.get("code") == fund_code:
                return True
        
        # 使用实时行情 API 验证
        realtime = await self.get_fund_realtime(fund_code)
        if realtime and realtime.get("name"):
            return True
        
        return False


# 全局实例
_api: Optional[EastMoneyAPI] = None


def get_api() -> EastMoneyAPI:
    """获取全局 API 实例"""
    global _api
    if _api is None:
        _api = EastMoneyAPI()
    return _api
