"""
AstrBot 基金数据分析插件
使用 AKShare 开源库获取基金数据，进行分析和展示
默认分析：国投瑞银白银期货(LOF)A (代码: 161226)
"""

import asyncio
import json
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timedelta
from pathlib import Path
from typing import Any

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, StarTools, register
from astrbot.core.utils.t2i.renderer import HtmlRenderer

# 导入股票分析模块
from .stock import StockAnalyzer, StockInfo

# 导入本地图片生成器
from .image_generator import render_fund_image, PLAYWRIGHT_AVAILABLE

# 导入东方财富 API 模块（直接 HTTP 请求，不依赖 akshare）
from .eastmoney_api import get_api as get_eastmoney_api
from .data_handler import DataHandler
from .services.position_service import PositionService
from .services.nav_sync_service import NavSyncService
from .services.market_service import MarketService
from .services.analysis_service import AnalysisService
from .formatters.position_formatter import (
    format_clear_history,
    format_clear_position_result,
    format_nav_sync_result,
    format_position_add_result,
    format_position_repair_result,
    format_position_realtime_snapshot,
    format_position_overview,
)
from .formatters.fund_formatter import (
    ssgz_usage_text,
    ssgz_invalid_code_text,
    ssgz_not_found_text,
    format_ssgz_fallback_text,
    format_fund_info,
    format_realtime_valuation,
    format_analysis,
    format_stock_info,
    format_precious_metal_prices,
)

# 默认超时时间（秒）- AKShare获取LOF数据需要较长时间
DEFAULT_TIMEOUT = 120  # 2分钟
# 数据缓存有效期（秒）
CACHE_TTL = 1800  # 30分钟


@dataclass
class FundInfo:
    """基金基本信息"""

    code: str  # 基金代码
    name: str  # 基金名称
    latest_price: float  # 最新价
    change_amount: float  # 涨跌额
    change_rate: float  # 涨跌幅
    open_price: float  # 开盘价
    high_price: float  # 最高价
    low_price: float  # 最低价
    prev_close: float  # 昨收
    volume: float  # 成交量
    amount: float  # 成交额
    turnover_rate: float  # 换手率

    @property
    def change_symbol(self) -> str:
        """涨跌符号"""
        if self.change_rate > 0:
            return "📈"
        elif self.change_rate < 0:
            return "📉"
        return "➡️"

    @property
    def trend_emoji(self) -> str:
        """趋势表情"""
        if self.change_rate >= 3:
            return "🚀"
        elif self.change_rate >= 1:
            return "↗️"
        elif self.change_rate > 0:
            return "↑"
        elif self.change_rate <= -3:
            return "💥"
        elif self.change_rate <= -1:
            return "↘️"
        elif self.change_rate < 0:
            return "↓"
        return "➡️"


class FundAnalyzer:
    """基金分析核心类"""

    # 默认基金代码：国投瑞银白银期货(LOF)A
    DEFAULT_FUND_CODE = "161226"
    DEFAULT_FUND_NAME = "国投瑞银白银期货(LOF)A"

    def __init__(self):
        # 使用东方财富 API 模块（不再依赖 akshare）
        self._api = get_eastmoney_api()
        self._initialized = True

    def _safe_float(self, value, default: float = 0.0) -> float:
        """安全地将值转换为float，处理NaN和None"""
        if value is None:
            return default
        try:
            import math

            if isinstance(value, float) and math.isnan(value):
                return default
            result = float(value)
            if math.isnan(result):
                return default
            return result
        except (ValueError, TypeError):
            return default

    async def get_lof_realtime(self, fund_code: str = None) -> FundInfo | None:
        """
        获取LOF基金实时行情

        Args:
            fund_code: 基金代码，默认为国投瑞银白银期货LOF

        Returns:
            FundInfo 对象或 None
        """
        if fund_code is None:
            fund_code = self.DEFAULT_FUND_CODE

        fund_code = str(fund_code).strip()

        try:
            data = await self._api.get_fund_realtime(fund_code)
            if not data:
                logger.warning(f"未找到基金数据: {fund_code}")
                return None

            return FundInfo(
                code=data.get("code", fund_code),
                name=data.get("name", ""),
                latest_price=data.get("latest_price", 0.0),
                change_amount=data.get("change_amount", 0.0),
                change_rate=data.get("change_rate", 0.0),
                open_price=data.get("open_price", 0.0),
                high_price=data.get("high_price", 0.0),
                low_price=data.get("low_price", 0.0),
                prev_close=data.get("prev_close", 0.0),
                volume=data.get("volume", 0.0),
                amount=data.get("amount", 0.0),
                turnover_rate=data.get("turnover_rate", 0.0),
            )
        except Exception as e:
            logger.error(f"获取LOF基金实时行情失败: {e}")
            return None

    async def get_realtime_valuation(self, fund_code: str) -> dict | None:
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

        try:
            return await self._api.get_fund_valuation(fund_code)
        except Exception as e:
            logger.error(f"获取基金实时估值失败: {e}")
            return None

    async def get_realtime_valuation_batch(
        self, fund_codes: list[str], max_concurrency: int = 6
    ) -> dict[str, dict]:
        """
        批量获取场外基金实时估值（并发）

        Args:
            fund_codes: 基金代码列表
            max_concurrency: 最大并发数

        Returns:
            {基金代码: 估值数据}
        """
        try:
            return await self._api.get_fund_valuation_batch(
                fund_codes, max_concurrency=max_concurrency
            )
        except Exception as e:
            logger.error(f"批量获取基金实时估值失败: {e}")
            return {}

    async def get_lof_history(
        self, fund_code: str = None, days: int = 30, adjust: str = "qfq"
    ) -> list[dict] | None:
        """
        获取LOF基金历史行情

        Args:
            fund_code: 基金代码
            days: 获取天数
            adjust: 复权类型 qfq-前复权, hfq-后复权, ""-不复权

        Returns:
            历史数据列表或 None
        """
        if fund_code is None:
            fund_code = self.DEFAULT_FUND_CODE

        fund_code = str(fund_code).strip()

        try:
            history = await self._api.get_fund_history(fund_code, days, adjust)
            return history
        except Exception as e:
            logger.error(f"获取LOF基金历史行情失败: {e}")
            return None

    async def search_fund(
        self,
        keyword: str,
        fetch_realtime: bool = True,
    ) -> list[dict]:
        """
        搜索LOF基金

        Args:
            keyword: 搜索关键词（基金名称或代码）
            fetch_realtime: 是否补充实时行情

        Returns:
            匹配的基金列表
        """
        try:
            results = await self._api.search_fund(
                keyword,
                fetch_realtime=fetch_realtime,
            )
            return results
        except Exception as e:
            logger.error(f"搜索基金失败: {e}")
            return []

    def is_otc_fund_code(self, fund_code: str) -> bool:
        """判断基金代码是否更偏向场外基金。"""
        code = str(fund_code or "").strip()
        if not code:
            return False
        try:
            return bool(self._api.is_otc_fund_code(code))
        except Exception as e:
            logger.debug(f"判断基金场内/场外失败: {code}, {e}")
            return False

    def calculate_technical_indicators(
        self, history_data: list[dict]
    ) -> dict[str, Any]:
        """
        计算技术指标（委托给 quant.py 中的完整实现）

        Args:
            history_data: 历史数据列表

        Returns:
            技术指标字典
        """
        if not history_data or len(history_data) < 5:
            return {}

        # 使用 quant.py 中的量化分析器
        from .ai_analyzer.quant import QuantAnalyzer

        quant = QuantAnalyzer()
        indicators = quant.calculate_all_indicators(history_data)
        perf = quant.calculate_performance(history_data)

        closes = [d["close"] for d in history_data]
        current_price = closes[-1] if closes else 0

        # 计算区间收益率
        def calc_return(days):
            if len(closes) > days:
                prev = closes[-(days + 1)]
                if prev != 0:
                    return (current_price - prev) / prev * 100
            return None

        # 转换为兼容格式
        return {
            "ma5": round(indicators.ma5, 4) if indicators.ma5 else None,
            "ma10": round(indicators.ma10, 4) if indicators.ma10 else None,
            "ma20": round(indicators.ma20, 4) if indicators.ma20 else None,
            "return_5d": calc_return(5),
            "return_10d": calc_return(10),
            "return_20d": calc_return(20),
            "volatility": perf.volatility if perf else None,
            "high_20d": max(closes[-20:]) if len(closes) >= 20 else max(closes),
            "low_20d": min(closes[-20:]) if len(closes) >= 20 else min(closes),
            "trend": indicators.signal,
            "current_price": current_price,
        }


# 贵金属价格缓存TTL（15分钟）
METAL_CACHE_TTL = 900
# 持仓基金净值定时同步间隔（秒）
NAV_SYNC_INTERVAL_SECONDS = 1800
NAV_SYNC_DEFAULT_FETCH_DAYS = 120
NAV_SYNC_MAX_FETCH_DAYS = 365
NAV_SYNC_FETCH_BUFFER_DAYS = 5


@register(
    "astrbot_plugin_fund_analyzer",
    "2529huang",
    "基金数据分析插件 - 使用AKShare获取LOF/ETF基金数据",
    "1.2.0",
)
class FundAnalyzerPlugin(Star):
    """基金分析插件主类"""

    # 用户设置文件名
    SETTINGS_FILE = "user_settings.json"
    QDII_NAME_KEYWORDS = (
        "qdii",
        "全球",
        "海外",
        "美国",
        "纳斯达克",
        "标普",
        "恒生",
        "日经",
        "道琼斯",
        "msci",
    )

    def __init__(self, context: Context):
        super().__init__(context)
        self.analyzer = FundAnalyzer()
        self.data_handler = DataHandler()
        # 初始化股票分析器
        self.stock_analyzer = StockAnalyzer()
        # 领域服务
        self.position_service = PositionService(
            normalize_fund_code=self._normalize_ssgz_fund_code,
            logger=logger,
        )
        self.market_service = MarketService(logger=logger, metal_cache_ttl=METAL_CACHE_TTL)
        self.analysis_service = AnalysisService(logger=logger)
        self.nav_sync_service = NavSyncService(
            data_handler=self.data_handler,
            analyzer=self.analyzer,
            logger=logger,
            interval_seconds=NAV_SYNC_INTERVAL_SECONDS,
            default_fetch_days=NAV_SYNC_DEFAULT_FETCH_DAYS,
            max_fetch_days=NAV_SYNC_MAX_FETCH_DAYS,
            fetch_buffer_days=NAV_SYNC_FETCH_BUFFER_DAYS,
        )
        # 初始化图片渲染器
        self.image_renderer = HtmlRenderer()
        # 是否使用本地图片生成器（优先使用）
        self.use_local_renderer = PLAYWRIGHT_AVAILABLE
        # 延迟初始化 AI 分析器
        self._ai_analyzer = None
        # 获取插件数据目录
        self._data_dir = Path(StarTools.get_data_dir("fund_analyzer"))
        self._data_dir.mkdir(parents=True, exist_ok=True)
        # 加载用户设置
        self.user_fund_settings: dict[str, str] = self._load_user_settings()
        # QDII 识别缓存（跨命令复用）
        self._qdii_flag_cache: dict[str, bool] = {}
        # sscc 专用：QDII 最近收盘净值缓存（按自然日复用）
        self._sscc_qdii_close_cache: dict[str, dict[str, Any] | None] = {}
        self._sscc_qdii_close_cache_day = date.today().isoformat()
        # 检查依赖
        self._check_dependencies()
        self._ensure_nav_sync_task()
        logger.info("基金分析插件已加载")

    def _check_dependencies(self):
        """检查必要依赖是否已安装"""
        try:
            import akshare  # noqa: F401
            import pandas  # noqa: F401
        except ImportError as e:
            logger.warning(
                f"基金分析插件依赖未完全安装: {e}\n请执行: pip install akshare pandas"
            )

    def _load_user_settings(self) -> dict[str, str]:
        """从文件加载用户设置"""
        settings_path = self._data_dir / self.SETTINGS_FILE
        if settings_path.exists():
            try:
                with open(settings_path, encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"加载用户设置失败: {e}")
        return {}

    def _save_user_settings(self):
        """保存用户设置到文件"""
        settings_path = self._data_dir / self.SETTINGS_FILE
        try:
            with open(settings_path, "w", encoding="utf-8") as f:
                json.dump(self.user_fund_settings, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"保存用户设置失败: {e}")

    @property
    def ai_analyzer(self):
        """延迟初始化 AI 分析器"""
        if self._ai_analyzer is None:
            from .ai_analyzer import AIFundAnalyzer

            self._ai_analyzer = AIFundAnalyzer(self.context)
        return self._ai_analyzer

    def _get_user_fund(self, user_id: str) -> str:
        """获取用户设置的默认基金代码"""
        return self.user_fund_settings.get(user_id, FundAnalyzer.DEFAULT_FUND_CODE)

    def _normalize_fund_code(self, code: str | int | None) -> str | None:
        """标准化基金代码，补齐前导0到6位

        Args:
            code: 基金代码，可能是字符串、整数或None

        Returns:
            标准化后的6位基金代码字符串，如果输入为None则返回None
        """
        if code is None:
            return None
        # 转换为字符串并去除空格
        code_str = str(code).strip()
        if not code_str:
            return None
        # 补齐前导0到6位
        return code_str.zfill(6)

    def _normalize_ssgz_fund_code(self, code: str | int | None) -> str | None:
        """标准化并校验 ssgz 使用的基金代码（6位数字）"""
        normalized_code = self._normalize_fund_code(code)
        if not normalized_code:
            return None
        if len(normalized_code) != 6 or not normalized_code.isdigit():
            return None
        return normalized_code

    def _extract_command_payload(
        self, event: AstrMessageEvent, command_name: str
    ) -> str:
        return self.position_service.extract_command_payload(event, command_name)

    def _resolve_position_owner(self, event: AstrMessageEvent) -> tuple[str, str]:
        return self.position_service.resolve_position_owner(event)

    @staticmethod
    def _fund_position_usage_text() -> str:
        return PositionService.fund_position_usage_text()

    @staticmethod
    def _clear_position_usage_text() -> str:
        return PositionService.clear_position_usage_text()

    def _parse_position_records(
        self, payload: str
    ) -> tuple[list[dict[str, Any]], str | None]:
        return self.position_service.parse_position_records(payload)

    def _parse_clear_payload(self, payload: str) -> tuple[dict[str, Any] | None, str | None]:
        return self.position_service.parse_clear_payload(payload)

    def _resolve_sell_shares(
        self,
        holding_shares: float,
        clear_payload: dict[str, Any],
    ) -> tuple[float | None, str | None]:
        return self.position_service.resolve_sell_shares(holding_shares, clear_payload)

    def _is_qdii_fund(self, fund_name: str) -> bool:
        text = str(fund_name or "").strip().lower()
        if not text:
            return False
        if "qdii" in text:
            return True
        return any(keyword in text for keyword in self.QDII_NAME_KEYWORDS if keyword != "qdii")

    @staticmethod
    def _is_qdii_by_fund_type(fund_type: str) -> bool:
        text = str(fund_type or "").strip().lower()
        if not text:
            return False
        if "qdii" in text:
            return True
        return "海外" in text or "全球" in text

    async def _resolve_is_qdii(self, fund_code: str, fund_name: str) -> bool:
        code = str(fund_code or "").strip()
        if self._is_qdii_fund(fund_name):
            if code:
                self._qdii_flag_cache[code] = True
            return True

        if code in self._qdii_flag_cache:
            return bool(self._qdii_flag_cache.get(code))

        if code:
            try:
                search_results = await self.analyzer.search_fund(
                    code,
                    fetch_realtime=False,
                )
                for item in search_results:
                    item_code = self._normalize_fund_code(item.get("code"))
                    if item_code != code:
                        continue
                    fund_type = str(item.get("fund_type") or "").strip()
                    if fund_type:
                        is_qdii = self._is_qdii_by_fund_type(fund_type)
                        self._qdii_flag_cache[code] = is_qdii
                        return is_qdii
            except Exception as e:
                logger.debug(f"通过 API 判断 QDII 失败，回退名称判断: {code}, {e}")
        return False

    @staticmethod
    def _calc_expected_settlement_date(
        trade_time: datetime,
        is_qdii: bool,
    ) -> tuple[date, str]:
        before_cutoff = trade_time.time() < dt_time(hour=15, minute=0)
        if is_qdii:
            base_offset = 2 if before_cutoff else 3
            rule_text = (
                "QDII 基金：15点前按 T+2，15点后按 T+3；若净值未更新则顺延到可用净值日"
            )
        else:
            base_offset = 1 if before_cutoff else 2
            rule_text = (
                "非 QDII 基金：15点前按 T+1，15点后按 T+2；按结算日可用最新净值计算"
            )
        expected_date = trade_time.date() + timedelta(days=base_offset)
        return expected_date, rule_text

    def _resolve_settlement_nav(
        self,
        fund_code: str,
        expected_settlement_date: date,
        is_qdii: bool,
    ) -> tuple[dict[str, Any] | None, str]:
        expected_date_text = expected_settlement_date.isoformat()

        nav = self.data_handler.get_nav_on_or_after(
            fund_code=fund_code,
            start_date=expected_date_text,
            end_date=expected_date_text,
        )
        if nav:
            return nav, ""

        if is_qdii:
            fallback_date_text = (expected_settlement_date + timedelta(days=1)).isoformat()
            nav = self.data_handler.get_nav_on_or_after(
                fund_code=fund_code,
                start_date=fallback_date_text,
                end_date=fallback_date_text,
            )
            if nav:
                return nav, "QDII 结算日顺延 1 天后匹配到净值"

        nav = self.data_handler.get_nav_on_or_after(
            fund_code=fund_code,
            start_date=expected_date_text,
        )
        if nav:
            nav_date_text = str(nav.get("nav_date") or "").strip()
            if nav_date_text and nav_date_text != expected_date_text:
                return nav, f"按结算日后首个可用净值 {nav_date_text} 计算"
            return nav, ""

        latest_nav = self.data_handler.get_latest_nav_record(fund_code=fund_code)
        if latest_nav:
            latest_date = str(latest_nav.get("nav_date") or "").strip()
            return latest_nav, f"未命中结算日净值，使用最新可用净值 {latest_date}"

        return None, "未获取到历史净值，收益按成本价估算"

    async def _batch_fetch_fund_infos(
        self, fund_codes: list[str], max_concurrency: int = 6
    ) -> dict[str, FundInfo]:
        return await self.position_service.batch_fetch_fund_infos(
            analyzer=self.analyzer,
            fund_codes=fund_codes,
            max_concurrency=max_concurrency,
        )

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        if value is None or value == "":
            return default
        try:
            result = float(value)
        except (TypeError, ValueError):
            return default
        if result != result:
            return default
        return result

    def _is_otc_fund_code(self, fund_code: str) -> bool:
        code = self._normalize_ssgz_fund_code(fund_code)
        if not code:
            return False

        try:
            if self.analyzer.is_otc_fund_code(code):
                return True
        except Exception as e:
            logger.debug(f"判断基金场内/场外失败: {code}, {e}")

        if code.startswith(("1", "5")):
            return False
        return code.startswith(("0", "2"))

    def _build_fund_info_from_valuation(
        self,
        fund_code: str,
        valuation: dict[str, Any],
    ) -> FundInfo:
        estimate_value = self._safe_float(valuation.get("estimate_value"))
        latest_price = self._safe_float(valuation.get("latest_price"))
        unit_value = self._safe_float(valuation.get("unit_value"))
        prev_close = self._safe_float(valuation.get("prev_close"))

        current_price = estimate_value if estimate_value > 0 else latest_price
        if current_price <= 0 and unit_value > 0:
            current_price = unit_value
        if current_price <= 0 and prev_close > 0:
            current_price = prev_close

        change_amount = self._safe_float(valuation.get("change_amount"))
        change_rate = self._safe_float(valuation.get("change_rate"))
        if change_amount == 0 and current_price > 0 and prev_close > 0:
            change_amount = current_price - prev_close
        if change_rate == 0 and prev_close > 0 and change_amount != 0:
            change_rate = change_amount / prev_close * 100

        return FundInfo(
            code=str(valuation.get("code") or fund_code).strip() or fund_code,
            name=str(valuation.get("name") or "").strip(),
            latest_price=current_price,
            change_amount=change_amount,
            change_rate=change_rate,
            open_price=0.0,
            high_price=0.0,
            low_price=0.0,
            prev_close=prev_close if prev_close > 0 else unit_value,
            volume=0.0,
            amount=0.0,
            turnover_rate=0.0,
        )

    async def _batch_fetch_position_realtime_infos(
        self,
        fund_codes: list[str],
        max_concurrency: int = 6,
    ) -> dict[str, FundInfo]:
        unique_codes: list[str] = []
        seen = set()
        for code in fund_codes:
            normalized = self._normalize_ssgz_fund_code(code)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            unique_codes.append(normalized)

        if not unique_codes:
            return {}

        otc_codes = [code for code in unique_codes if self._is_otc_fund_code(code)]
        fund_infos: dict[str, FundInfo] = {}

        if otc_codes:
            valuation_map = await self.analyzer.get_realtime_valuation_batch(
                otc_codes,
                max_concurrency=max_concurrency,
            )
            for code in otc_codes:
                valuation = valuation_map.get(code)
                if valuation:
                    fund_infos[code] = self._build_fund_info_from_valuation(
                        code,
                        valuation,
                    )

        unresolved_codes = [code for code in unique_codes if code not in fund_infos]
        if unresolved_codes:
            fallback_infos = await self._batch_fetch_fund_infos(
                unresolved_codes,
                max_concurrency=max_concurrency,
            )
            fund_infos.update(fallback_infos)

        return fund_infos

    @staticmethod
    def _extract_latest_close_change(
        history_data: list[dict[str, Any]] | None,
    ) -> dict[str, Any] | None:
        if not history_data:
            return None

        latest = history_data[-1] or {}
        close_date = str(latest.get("date") or "").strip()

        raw_change_rate = latest.get("change_rate")
        change_rate: float | None = None
        if raw_change_rate not in (None, "", "--"):
            try:
                parsed = float(raw_change_rate)
                if parsed == parsed:
                    change_rate = parsed
            except (TypeError, ValueError):
                change_rate = None

        return {
            "close_date": close_date or "--",
            "change_rate": change_rate,
        }

    def _rollover_sscc_qdii_close_cache(self) -> None:
        today = date.today().isoformat()
        if self._sscc_qdii_close_cache_day != today:
            self._sscc_qdii_close_cache_day = today
            self._sscc_qdii_close_cache.clear()

    def _get_cached_sscc_qdii_close_change(
        self, fund_code: str
    ) -> tuple[dict[str, Any] | None, bool]:
        self._rollover_sscc_qdii_close_cache()
        if fund_code not in self._sscc_qdii_close_cache:
            return None, False
        cached = self._sscc_qdii_close_cache.get(fund_code)
        if cached is None:
            return None, True
        return dict(cached), True

    def _save_cached_sscc_qdii_close_change(
        self,
        fund_code: str,
        close_change: dict[str, Any] | None,
    ) -> None:
        self._rollover_sscc_qdii_close_cache()
        self._sscc_qdii_close_cache[fund_code] = dict(close_change) if close_change else None

    async def _batch_fetch_position_close_changes(
        self,
        positions: list[dict[str, Any]],
        fund_infos: dict[str, FundInfo],
        max_concurrency: int = 4,
    ) -> tuple[dict[str, dict[str, Any]], int]:
        unique_codes: list[str] = []
        seen = set()
        name_map: dict[str, str] = {}
        for item in positions:
            code = self._normalize_ssgz_fund_code(item.get("fund_code"))
            if not code:
                continue
            if code not in seen:
                seen.add(code)
                unique_codes.append(code)
            local_name = str(item.get("fund_name") or "").strip()
            if local_name and code not in name_map:
                name_map[code] = local_name

        if not unique_codes:
            return {}, 0

        semaphore = asyncio.Semaphore(max(1, min(max_concurrency, 12)))

        async def fetch_one(code: str) -> tuple[str, dict[str, Any], bool]:
            is_otc = self._is_otc_fund_code(code)
            is_qdii = False
            close_change: dict[str, Any] | None = None
            cache_hit = False

            async with semaphore:
                try:
                    info = fund_infos.get(code)
                    fund_name = (
                        info.name
                        if info and getattr(info, "name", "")
                        else name_map.get(code, "")
                    )
                    is_qdii = await self._resolve_is_qdii(code, fund_name)

                    if is_qdii:
                        cached, cache_hit = self._get_cached_sscc_qdii_close_change(code)
                        if cache_hit:
                            close_change = cached

                    if close_change is None and not cache_hit:
                        history = await self.analyzer.get_lof_history(code, days=5)
                        close_change = self._extract_latest_close_change(history)
                        if is_qdii:
                            self._save_cached_sscc_qdii_close_change(code, close_change)
                except Exception as e:
                    logger.debug(f"获取基金最近收盘涨跌幅失败: {code}, {e}")
                    if is_qdii:
                        self._save_cached_sscc_qdii_close_change(code, None)
                    close_change = None

            payload = dict(close_change or {"close_date": "--", "change_rate": None})
            payload["is_otc"] = is_otc
            payload["is_qdii"] = is_qdii
            return code, payload, cache_hit

        tasks = [asyncio.create_task(fetch_one(code)) for code in unique_codes]
        raw_results = await asyncio.gather(*tasks, return_exceptions=True)

        close_change_map: dict[str, dict[str, Any]] = {}
        qdii_cache_hits = 0
        for item in raw_results:
            if isinstance(item, Exception):
                continue
            code, payload, cache_hit = item
            close_change_map[code] = payload
            if cache_hit:
                qdii_cache_hits += 1

        return close_change_map, qdii_cache_hits

    @staticmethod
    def _format_position_add_result(
        saved_records: list[dict[str, Any]],
        fund_infos: dict[str, FundInfo],
    ) -> str:
        return format_position_add_result(saved_records, fund_infos)

    @staticmethod
    def _format_position_overview(
        positions: list[dict[str, Any]],
        fund_infos: dict[str, FundInfo],
    ) -> str:
        return format_position_overview(positions, fund_infos)

    @staticmethod
    def _format_position_realtime_snapshot(
        positions: list[dict[str, Any]],
        fund_infos: dict[str, FundInfo],
        close_change_map: dict[str, dict[str, Any]],
        qdii_cache_hits: int = 0,
    ) -> str:
        return format_position_realtime_snapshot(
            positions=positions,
            fund_infos=fund_infos,
            close_change_map=close_change_map,
            qdii_cache_hits=qdii_cache_hits,
        )

    @staticmethod
    def _format_position_repair_result(stats: dict[str, Any]) -> str:
        return format_position_repair_result(stats)

    @staticmethod
    def _format_clear_position_result(result: dict[str, Any]) -> str:
        return format_clear_position_result(result)

    @staticmethod
    def _format_clear_history(logs: list[dict[str, Any]]) -> str:
        return format_clear_history(logs)

    def _ensure_nav_sync_task(self) -> None:
        self.nav_sync_service.ensure_task()

    async def _sync_position_funds_nav(
        self,
        fund_codes: list[str] | None = None,
        force_full: bool = False,
        trigger: str = "manual",
    ) -> dict[str, Any]:
        return await self.nav_sync_service.sync_position_funds_nav(
            fund_codes=fund_codes,
            force_full=force_full,
            trigger=trigger,
        )

    @staticmethod
    def _format_nav_sync_result(stats: dict[str, Any], title: str) -> str:
        return format_nav_sync_result(stats, title)

    @staticmethod
    def _ssgz_usage_text() -> str:
        return ssgz_usage_text()

    @staticmethod
    def _ssgz_invalid_code_text(raw_code: str) -> str:
        return ssgz_invalid_code_text(raw_code)

    @staticmethod
    def _ssgz_not_found_text(fund_code: str) -> str:
        return ssgz_not_found_text(fund_code)

    def _format_ssgz_fallback_text(self, fund_code: str, realtime: FundInfo) -> str:
        return format_ssgz_fallback_text(fund_code, realtime)

    async def _query_ssgz_text(self, fund_code: str) -> str:
        """查询 ssgz 文本结果（估值优先，场内行情兜底）"""
        valuation = await self.analyzer.get_realtime_valuation(fund_code)
        if valuation:
            return self._format_realtime_valuation(valuation)

        realtime = await self.analyzer.get_lof_realtime(fund_code)
        if realtime:
            return self._format_ssgz_fallback_text(fund_code, realtime)

        return self._ssgz_not_found_text(fund_code)

    def _format_fund_info(self, info: FundInfo) -> str:
        return format_fund_info(info)

    def _format_realtime_valuation(self, valuation: dict) -> str:
        return format_realtime_valuation(valuation)

    def _format_analysis(self, info: FundInfo, indicators: dict) -> str:
        return format_analysis(info, indicators)

    def _format_stock_info(self, info: StockInfo) -> str:
        return format_stock_info(info)

    async def _fetch_precious_metal_prices(self) -> dict:
        return await self.market_service.fetch_precious_metal_prices()

    def _format_precious_metal_prices(self, prices: dict) -> str:
        return format_precious_metal_prices(prices)

    @filter.command("今日行情")
    async def today_market(self, event: AstrMessageEvent):
        """
        查询今日贵金属行情
        用法: 今日行情
        返回国际金价、银价及涨跌幅
        """
        try:
            yield event.plain_result("🔍 正在获取今日贵金属行情...")

            prices = await self._fetch_precious_metal_prices()

            if prices:
                yield event.plain_result(self._format_precious_metal_prices(prices))
            else:
                yield event.plain_result("❌ 获取贵金属行情失败，请稍后重试")

        except Exception as e:
            logger.error(f"获取今日行情出错: {e}")
            yield event.plain_result(f"❌ 获取行情失败: {str(e)}")

    @filter.command("股票")
    async def stock_query(self, event: AstrMessageEvent, code: str = ""):
        """
        查询A股实时行情
        用法: 股票 <股票代码>
        示例: 股票 000001
        示例: 股票 600519
        """
        try:
            if not code:
                yield event.plain_result(
                    "❌ 请输入股票代码\n"
                    "💡 用法: 股票 <股票代码>\n"
                    "💡 示例: 股票 000001 (平安银行)\n"
                    "💡 示例: 股票 600519 (贵州茅台)"
                )
                return

            stock_code = str(code).strip().zfill(6)
            yield event.plain_result(f"🔍 正在查询股票 {stock_code} 的实时行情...")

            info = await self.stock_analyzer.get_stock_realtime(stock_code)

            if info:
                yield event.plain_result(self._format_stock_info(info))
            else:
                yield event.plain_result(
                    f"❌ 未找到股票代码 {stock_code}\n"
                    "💡 请使用「搜索股票 关键词」来搜索正确的股票代码\n"
                    "💡 示例: 搜索股票 茅台"
                )

        except ImportError:
            yield event.plain_result(
                "❌ AKShare 库未安装\n请管理员执行: pip install akshare"
            )
        except TimeoutError as e:
            yield event.plain_result(f"⏰ {str(e)}\n💡 数据源响应较慢，请稍后再试")
        except Exception as e:
            logger.error(f"查询股票行情出错: {e}")
            yield event.plain_result(f"❌ 查询失败: {str(e)}")

    @filter.command("搜索股票")
    async def search_stock(self, event: AstrMessageEvent, keyword: str = ""):
        """
        搜索A股股票
        用法: 搜索股票 <关键词>
        示例: 搜索股票 茅台
        """
        try:
            if not keyword:
                yield event.plain_result(
                    "❌ 请输入搜索关键词\n"
                    "💡 用法: 搜索股票 <关键词>\n"
                    "💡 示例: 搜索股票 茅台"
                )
                return

            yield event.plain_result(f"🔍 正在搜索包含 '{keyword}' 的股票...")

            results = await self.stock_analyzer.search_stock(keyword)

            if not results:
                yield event.plain_result(f"❌ 未找到包含 '{keyword}' 的股票")
                return

            # 格式化搜索结果
            lines = [f"🔍 搜索结果: '{keyword}'", "━━━━━━━━━━━━━━━━━"]
            for i, stock in enumerate(results, 1):
                change_emoji = (
                    "🔴"
                    if stock["change_rate"] < 0
                    else "🟢"
                    if stock["change_rate"] > 0
                    else "⚪"
                )
                lines.append(
                    f"{i}. {stock['name']} ({stock['code']})\n"
                    f"   💰 {stock['price']:.2f} {change_emoji} {stock['change_rate']:+.2f}%"
                )
            lines.append("━━━━━━━━━━━━━━━━━")
            lines.append("💡 使用「股票 代码」查看详细行情")

            yield event.plain_result("\n".join(lines))

        except ImportError:
            yield event.plain_result(
                "❌ AKShare 库未安装\n请管理员执行: pip install akshare"
            )
        except TimeoutError as e:
            yield event.plain_result(f"⏰ {str(e)}\n💡 数据源响应较慢，请稍后再试")
        except Exception as e:
            logger.error(f"搜索股票出错: {e}")
            yield event.plain_result(f"❌ 搜索失败: {str(e)}")

    @filter.command("ssgz")
    async def fund_realtime_valuation(self, event: AstrMessageEvent, code: str = ""):
        """
        查询场外基金实时估值
        用法: ssgz <基金代码>
        示例: ssgz 001632
        """
        try:
            raw_code = str(code).strip()
            if not raw_code:
                yield event.plain_result(self._ssgz_usage_text())
                return

            fund_code = self._normalize_ssgz_fund_code(raw_code)
            if not fund_code:
                yield event.plain_result(self._ssgz_invalid_code_text(raw_code))
                return

            yield event.plain_result(f"🔍 正在查询基金 {fund_code} 的实时估值...")
            yield event.plain_result(await self._query_ssgz_text(fund_code))

        except Exception as e:
            logger.error(f"查询基金实时估值出错: {e}")
            yield event.plain_result(f"❌ 查询失败: {str(e)}")

    @filter.command("基金")
    async def fund_query(self, event: AstrMessageEvent, code: str = ""):
        """
        查询基金实时行情
        用法: 基金 [基金代码]
        示例: 基金 161226
        """
        try:
            user_id = event.get_sender_id()
            # 标准化基金代码，补齐前导0
            normalized_code = self._normalize_fund_code(code)
            fund_code = normalized_code or self._get_user_fund(user_id)

            yield event.plain_result(f"🔍 正在查询基金 {fund_code} 的实时行情...")

            info = await self.analyzer.get_lof_realtime(fund_code)

            if info:
                yield event.plain_result(self._format_fund_info(info))
            else:
                # 区分是基金代码错误还是数据源问题
                if not normalized_code:
                    yield event.plain_result(f"❌ 基金代码不能为空")
                    return

                # 如果代码是6位数字，通常是有效的基金代码格式，但未找到数据
                if len(normalized_code) == 6 and normalized_code.isdigit():
                    # 尝试再次搜索确认是否存在
                    try:
                        search_res = await self.analyzer.search_fund(normalized_code)
                        if not search_res:
                            yield event.plain_result(
                                f"❌ 未找到基金代码 {fund_code}\n"
                                "💡 请检查代码是否正确，或使用「搜索基金 关键词」查找"
                            )
                            return
                    except Exception:
                        pass  # 搜索出错忽略，继续下面的判断

                yield event.plain_result(
                    f"⚠️ 暂时无法获取基金 {fund_code} 的数据\n"
                    "💡 可能是数据源暂时不可用，或该基金为非LOF基金\n"
                    "💡 请稍后重试"
                )

        except ImportError:
            yield event.plain_result(
                "❌ AKShare 库未安装\n请管理员执行: pip install akshare"
            )
        except TimeoutError as e:
            yield event.plain_result(f"⏰ {str(e)}\n💡 数据源响应较慢，请稍后再试")
        except Exception as e:
            logger.error(f"查询基金行情出错: {e}")
            yield event.plain_result(f"❌ 查询失败: {str(e)}")

    @filter.command("基金分析")
    async def fund_analysis(self, event: AstrMessageEvent, code: str = ""):
        """
        基金技术分析
        用法: 基金分析 [基金代码]
        示例: 基金分析 161226
        """
        try:
            user_id = event.get_sender_id()
            # 标准化基金代码，补齐前导0
            normalized_code = self._normalize_fund_code(code)
            fund_code = normalized_code or self._get_user_fund(user_id)

            yield event.plain_result(f"📊 正在生成基金 {fund_code} 分析报告...")

            # 获取实时行情
            info = await self.analyzer.get_lof_realtime(fund_code)
            if not info:
                # 区分是基金代码错误还是数据源问题
                if not normalized_code:
                    yield event.plain_result(f"❌ 基金代码不能为空")
                    return

                # 如果代码是6位数字，通常是有效的基金代码格式，但未找到数据
                if len(normalized_code) == 6 and normalized_code.isdigit():
                    # 尝试再次搜索确认是否存在
                    try:
                        search_res = await self.analyzer.search_fund(normalized_code)
                        if not search_res:
                            yield event.plain_result(
                                f"❌ 未找到基金代码 {fund_code}\n"
                                "💡 请检查代码是否正确，或使用「搜索基金 关键词」查找"
                            )
                            return
                    except Exception:
                        pass  # 搜索出错忽略，继续下面的判断

                yield event.plain_result(
                    f"⚠️ 暂时无法获取基金 {fund_code} 的数据\n"
                    "💡 可能是数据源暂时不可用，或该基金为非LOF基金\n"
                    "💡 请稍后重试"
                )
                return

            # 获取历史数据进行分析
            history = await self.analyzer.get_lof_history(fund_code, days=30)

            # 计算技术指标
            indicators = {}
            if history:
                indicators = self.analyzer.calculate_technical_indicators(history)
                # 绘制小图用于报告
                plot_img = await asyncio.to_thread(
                    self._plot_history_chart, history, info.name
                )
            else:
                plot_img = None

            # 准备模板数据
            ma_data = []
            if indicators:
                for ma in ["ma5", "ma10", "ma20"]:
                    if indicators.get(ma):
                        ma_data.append({"name": ma.upper(), "value": indicators[ma]})

            data = {
                "fund_name": info.name,
                "fund_code": info.code,
                "latest_price": info.latest_price,
                "change_amount": info.change_amount,
                "change_rate": info.change_rate,
                "plot_img": plot_img,
                "trend": indicators.get("trend", "数据不足"),
                "volatility": indicators.get("volatility"),
                "return_5d": indicators.get("return_5d"),
                "return_10d": indicators.get("return_10d"),
                "return_20d": indicators.get("return_20d"),
                "ma_data": ma_data,
                "generated_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }

            # 读取模板
            template_path = self._data_dir / "templates" / "analysis_report.html"
            # 如果不在数据目录，尝试检查插件目录
            if not template_path.exists():
                template_path = (
                    Path(__file__).parent / "templates" / "analysis_report.html"
                )

            if not template_path.exists():
                # 降级到文本模式
                yield event.plain_result(self._format_analysis(info, indicators))
                return

            with open(template_path, "r", encoding="utf-8") as f:
                template_str = f.read()

            # 渲染图片
            img_url = await self.image_renderer.render_custom_template(
                tmpl_str=template_str, tmpl_data=data, return_url=True
            )

            # 发送图片
            yield event.image_result(img_url)

        except ImportError:
            yield event.plain_result(
                "❌ AKShare 库未安装\n请管理员执行: pip install akshare"
            )
        except TimeoutError as e:
            yield event.plain_result(f"⏰ {str(e)}\n💡 数据源响应较慢，请稍后再试")
        except Exception as e:
            logger.error(f"基金分析出错: {e}")
            yield event.plain_result(f"❌ 分析失败: {str(e)}")

    def _plot_history_chart(self, history: list[dict], fund_name: str) -> str | None:
        return self.analysis_service.plot_history_chart(history, fund_name)

    @filter.command("基金历史")
    async def fund_history(
        self, event: AstrMessageEvent, code: str = "", days: str = "10"
    ):
        """
        查询基金历史行情
        用法: 基金历史 [基金代码] [天数]
        示例: 基金历史 161226 10
        """
        try:
            user_id = event.get_sender_id()
            # 标准化基金代码，补齐前导0
            normalized_code = self._normalize_fund_code(code)
            fund_code = normalized_code or self._get_user_fund(user_id)

            try:
                num_days = int(days)
                if num_days < 1:
                    num_days = 10
                elif num_days > 60:
                    num_days = 60
            except ValueError:
                num_days = 10

            yield event.plain_result(
                f"📜 正在生成基金 {fund_code} 近 {num_days} 日行情报告..."
            )

            # 获取基金名称
            info = await self.analyzer.get_lof_realtime(fund_code)
            fund_name = info.name if info else fund_code

            history = await self.analyzer.get_lof_history(fund_code, days=num_days)

            if history:
                # 绘制走势图
                plot_img = await asyncio.to_thread(
                    self._plot_history_chart, history, fund_name
                )

                # 计算区间统计
                closes = [d["close"] for d in history]
                total_return = (
                    ((closes[-1] - closes[0]) / closes[0]) * 100 if closes[0] else 0
                )

                # 准备模板数据
                data = {
                    "fund_name": fund_name,
                    "fund_code": fund_code,
                    "days": num_days,
                    "history_list": list(reversed(history)),  # 倒序显示，最近的在前面
                    "plot_img": plot_img,
                    "total_return": total_return,
                    "max_price": max(closes),
                    "min_price": min(closes),
                    "generated_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }

                # 读取模板
                template_path = (
                    Path(__file__).parent / "templates" / "history_report.html"
                )
                if not template_path.exists():
                    yield event.plain_result(f"❌ 模板文件不存在: {template_path}")
                    return

                # 渲染图片 - 优先使用本地渲染器
                if self.use_local_renderer:
                    try:
                        img_path = await render_fund_image(
                            template_path=template_path, template_data=data, width=420
                        )
                        yield event.image_result(img_path)
                    except Exception as e:
                        logger.warning(f"本地渲染失败，回退到网络渲染: {e}")
                        # 回退到网络渲染
                        with open(template_path, "r", encoding="utf-8") as f:
                            template_str = f.read()
                        img_url = await self.image_renderer.render_custom_template(
                            tmpl_str=template_str,
                            tmpl_data=data,
                            return_url=True,
                        )
                        yield event.image_result(img_url)
                else:
                    # 使用网络渲染
                    with open(template_path, "r", encoding="utf-8") as f:
                        template_str = f.read()
                    img_url = await self.image_renderer.render_custom_template(
                        tmpl_str=template_str,
                        tmpl_data=data,
                        return_url=True,
                    )
                    yield event.image_result(img_url)

            else:
                yield event.plain_result(f"❌ 未找到基金 {fund_code} 的历史数据")

        except ImportError:
            yield event.plain_result(
                "❌ AKShare 库未安装\n请管理员执行: pip install akshare matplotlib"
            )
        except TimeoutError as e:
            yield event.plain_result(f"⏰ {str(e)}\n💡 数据源响应较慢，请稍后再试")
        except Exception as e:
            logger.error(f"查询基金历史出错: {e}")
            yield event.plain_result(f"❌ 查询失败: {str(e)}")

    @filter.command("搜索基金")
    async def search_fund(self, event: AstrMessageEvent, keyword: str = ""):
        """
        搜索LOF基金
        用法: 搜索基金 关键词
        示例: 搜索基金 白银
        """
        if not keyword:
            yield event.plain_result(
                "❓ 请输入搜索关键词\n用法: 搜索基金 关键词\n示例: 搜索基金 白银"
            )
            return

        try:
            yield event.plain_result(f"🔍 正在搜索包含「{keyword}」的基金...")

            results = await self.analyzer.search_fund(keyword)

            if results:
                text_lines = [
                    f"📋 搜索结果 (共 {len(results)} 条)",
                    "━━━━━━━━━━━━━━━━━",
                ]

                for fund in results:
                    price = fund.get("latest_price", 0)
                    change = fund.get("change_rate", 0)
                    # 价格为0通常表示暂无数据（原始数据为NaN）
                    if price == 0:
                        price_str = "暂无数据"
                        change_str = ""
                    else:
                        emoji = "🟢" if change > 0 else "🔴" if change < 0 else "⚪"
                        price_str = f"{price:.4f}"
                        change_str = f" {emoji}{change:+.2f}%"
                    text_lines.append(
                        f"{fund['code']} | {fund['name']}\n"
                        f"    💰 {price_str}{change_str}"
                    )

                text_lines.append("━━━━━━━━━━━━━━━━━")
                text_lines.append("💡 使用「基金 代码」查看详情")
                text_lines.append("💡 使用「设置基金 代码」设为默认")

                yield event.plain_result("\n".join(text_lines))
            else:
                yield event.plain_result(
                    f"❌ 未找到包含「{keyword}」的LOF基金\n💡 尝试使用其他关键词搜索"
                )

        except ImportError:
            yield event.plain_result(
                "❌ AKShare 库未安装\n请管理员执行: pip install akshare"
            )
        except TimeoutError as e:
            yield event.plain_result(f"⏰ {str(e)}\n💡 数据源响应较慢，请稍后再试")
        except Exception as e:
            logger.error(f"搜索基金出错: {e}")
            yield event.plain_result(f"❌ 搜索失败: {str(e)}")

    @filter.command("设置基金")
    async def set_default_fund(self, event: AstrMessageEvent, code: str = ""):
        """
        设置默认关注的基金
        用法: 设置基金 基金代码
        示例: 设置基金 161226
        """
        if not code:
            user_id = event.get_sender_id()
            current = self._get_user_fund(user_id)
            yield event.plain_result(
                f"💡 当前默认基金: {current}\n"
                "用法: 设置基金 基金代码\n"
                "示例: 设置基金 161226"
            )
            return

        try:
            # 标准化基金代码，补齐前导0
            code = self._normalize_fund_code(code) or code
            # 验证基金代码是否有效
            info = await self.analyzer.get_lof_realtime(code)

            if info:
                user_id = event.get_sender_id()
                self.user_fund_settings[user_id] = code
                self._save_user_settings()  # 持久化保存
                yield event.plain_result(
                    f"✅ 已设置默认基金\n"
                    f"📊 {info.code} - {info.name}\n"
                    f"💰 当前价格: {info.latest_price:.4f}"
                )
            else:
                yield event.plain_result(
                    f"❌ 无效的基金代码: {code}\n"
                    "💡 请使用「搜索基金 关键词」查找正确代码"
                )

        except ImportError:
            yield event.plain_result(
                "❌ AKShare 库未安装\n请管理员执行: pip install akshare"
            )
        except TimeoutError as e:
            yield event.plain_result(f"⏰ {str(e)}\n💡 数据源响应较慢，请稍后再试")
        except Exception as e:
            logger.error(f"设置默认基金出错: {e}")
            yield event.plain_result(f"❌ 设置失败: {str(e)}")

    @filter.command("增加基金持仓")
    async def add_fund_positions(self, event: AstrMessageEvent, payload: str = ""):
        """
        增加基金持仓（支持批量）
        用法: 增加基金持仓 {基金代码,平均成本,持有份额}
        示例: 增加基金持仓 {161226,1.0234,1200} {001632,2.1456,500}
        """
        try:
            self._ensure_nav_sync_task()
            raw_payload = self._extract_command_payload(event, "增加基金持仓")
            payload_text = raw_payload or str(payload or "").strip()
            records, error = self._parse_position_records(payload_text)
            if error:
                yield event.plain_result(error)
                return

            platform, user_id = self._resolve_position_owner(event)
            if not user_id:
                yield event.plain_result("❌ 无法识别当前用户 ID，请稍后再试")
                return

            yield event.plain_result(f"📝 正在记录 {len(records)} 条基金持仓...")

            fund_infos = await self._batch_fetch_fund_infos(
                [str(item["fund_code"]) for item in records],
                max_concurrency=4,
            )
            for record in records:
                info = fund_infos.get(str(record["fund_code"]))
                if info and info.name:
                    record["fund_name"] = info.name

            saved_records = self.data_handler.add_or_merge_positions(
                platform=platform,
                user_id=user_id,
                records=records,
            )
            yield event.plain_result(
                self._format_position_add_result(saved_records, fund_infos)
            )

        except ValueError as e:
            yield event.plain_result(f"❌ {str(e)}")
        except Exception as e:
            logger.error(f"增加基金持仓失败: {e}")
            yield event.plain_result(f"❌ 持仓记录失败: {str(e)}")

    @filter.command("清仓基金")
    async def clear_fund_position(self, event: AstrMessageEvent, payload: str = ""):
        """
        清仓或卖出指定基金份额（支持按份额或百分比）
        用法: 清仓基金 [基金代码] [份额|百分比]
        示例: 清仓基金 161226 500
        示例: 清仓基金 161226 25%
        """
        try:
            self._ensure_nav_sync_task()
            raw_payload = self._extract_command_payload(event, "清仓基金")
            payload_text = raw_payload or str(payload or "").strip()
            clear_payload, error = self._parse_clear_payload(payload_text)
            if error:
                yield event.plain_result(error)
                return
            if not clear_payload:
                yield event.plain_result(self._clear_position_usage_text())
                return

            platform, user_id = self._resolve_position_owner(event)
            if not user_id:
                yield event.plain_result("❌ 无法识别当前用户 ID，请稍后再试")
                return

            positions = self.data_handler.list_positions(platform=platform, user_id=user_id)
            if not positions:
                yield event.plain_result(
                    "📭 当前没有基金持仓记录\n"
                    "💡 请先使用：增加基金持仓 {基金代码,平均成本,持有份额}"
                )
                return

            position_map: dict[str, dict[str, Any]] = {}
            for item in positions:
                code = str(item.get("fund_code") or "").strip()
                if code:
                    position_map[code] = item

            target_code = str(clear_payload.get("fund_code") or "").strip()
            if not target_code:
                sender_id = str(event.get_sender_id() or "").strip()
                default_code = self._normalize_fund_code(self._get_user_fund(sender_id))
                if default_code and default_code in position_map:
                    target_code = default_code
                elif len(position_map) == 1:
                    target_code = next(iter(position_map.keys()))
                else:
                    available_codes = "、".join(sorted(position_map.keys())[:8])
                    yield event.plain_result(
                        "❌ 你当前持有多只基金，请指定基金代码\n"
                        "💡 用法: 清仓基金 [基金代码] [份额|百分比]\n"
                        f"💡 当前持仓代码: {available_codes}"
                    )
                    return

            position = position_map.get(target_code)
            if position is None:
                yield event.plain_result(
                    f"❌ 未找到基金 {target_code} 的持仓记录\n"
                    "💡 使用 ckcc 查看当前持仓"
                )
                return

            holding_shares = float(position.get("shares", 0) or 0)
            sell_shares, error = self._resolve_sell_shares(holding_shares, clear_payload)
            if error:
                yield event.plain_result(error)
                return
            if sell_shares is None or sell_shares <= 0:
                yield event.plain_result("❌ 卖出份额必须大于 0")
                return

            yield event.plain_result("🧮 正在计算清仓结算净值并更新持仓...")

            try:
                await self._sync_position_funds_nav(
                    fund_codes=[target_code],
                    force_full=False,
                    trigger="clear",
                )
            except Exception as sync_error:
                logger.debug(f"清仓前增量刷新净值失败: {sync_error}")

            fund_infos = await self._batch_fetch_fund_infos(
                [target_code],
                max_concurrency=2,
            )
            info = fund_infos.get(target_code)
            fund_name = (
                info.name
                if info and getattr(info, "name", "")
                else str(position.get("fund_name") or "").strip()
            )
            if not fund_name:
                fund_name = target_code

            is_qdii = await self._resolve_is_qdii(
                fund_code=target_code,
                fund_name=fund_name,
            )
            trade_time = datetime.now()
            expected_settlement_date, settlement_rule = self._calc_expected_settlement_date(
                trade_time=trade_time,
                is_qdii=is_qdii,
            )
            nav_record, nav_note = self._resolve_settlement_nav(
                fund_code=target_code,
                expected_settlement_date=expected_settlement_date,
                is_qdii=is_qdii,
            )

            avg_cost = float(position.get("avg_cost", 0) or 0)
            settlement_nav = None
            settlement_nav_date = None
            if nav_record:
                nav_value = float(nav_record.get("unit_nav", 0) or 0)
                if nav_value > 0:
                    settlement_nav = nav_value
                nav_date_text = str(nav_record.get("nav_date") or "").strip()
                settlement_nav_date = nav_date_text or None

            settlement_for_profit = settlement_nav if settlement_nav and settlement_nav > 0 else avg_cost
            profit_amount = (settlement_for_profit - avg_cost) * float(sell_shares)
            action = "clear" if float(sell_shares) >= holding_shares - 1e-8 else "sell"

            if clear_payload.get("share_mode") == "all":
                requested_text = "全仓"
            elif clear_payload.get("share_mode") == "percent":
                requested_text = f"{clear_payload.get('share_raw', '')} (银行家舍入)"
            else:
                requested_text = str(clear_payload.get("share_raw") or "").strip()

            result = self.data_handler.reduce_position_with_log(
                platform=platform,
                user_id=user_id,
                fund_code=target_code,
                shares=sell_shares,
                action=action,
                settlement_nav=settlement_nav,
                settlement_nav_date=settlement_nav_date,
                expected_settlement_date=expected_settlement_date.isoformat(),
                settlement_rule=settlement_rule,
                profit_amount=profit_amount,
                note=nav_note,
                fund_name=fund_name,
            )

            result["fund_name"] = fund_name
            result["settlement_nav"] = settlement_nav
            result["settlement_nav_date"] = settlement_nav_date
            result["expected_settlement_date"] = expected_settlement_date.isoformat()
            result["settlement_rule"] = settlement_rule
            result["profit_amount"] = profit_amount
            result["requested_text"] = requested_text

            yield event.plain_result(self._format_clear_position_result(result))

        except ValueError as e:
            yield event.plain_result(f"❌ {str(e)}")
        except Exception as e:
            logger.error(f"清仓基金失败: {e}")
            yield event.plain_result(f"❌ 清仓失败: {str(e)}")

    @filter.command("sscc")
    async def snapshot_position_realtime(self, event: AstrMessageEvent):
        """
        实时查看当前用户持仓基金现价与最近收盘日涨跌幅。
        用法: sscc
        """
        try:
            self._ensure_nav_sync_task()
            platform, user_id = self._resolve_position_owner(event)
            if not user_id:
                yield event.plain_result("❌ 无法识别当前用户 ID，请稍后再试")
                return

            positions = self.data_handler.list_positions(platform=platform, user_id=user_id)
            if not positions:
                yield event.plain_result(
                    "📭 当前没有基金持仓记录\n"
                    "💡 用法: 增加基金持仓 {基金代码,平均成本,持有份额}\n"
                    "💡 示例: 增加基金持仓 {161226,1.0234,1200}"
                )
                return

            fund_codes: list[str] = []
            seen = set()
            for item in positions:
                code = self._normalize_ssgz_fund_code(item.get("fund_code"))
                if not code or code in seen:
                    continue
                seen.add(code)
                fund_codes.append(code)

            yield event.plain_result("⚡ 正在查询你持仓基金的现价与最近收盘涨跌幅...")

            fund_infos = await self._batch_fetch_position_realtime_infos(
                fund_codes,
                max_concurrency=6,
            )
            close_change_map, qdii_cache_hits = await self._batch_fetch_position_close_changes(
                positions=positions,
                fund_infos=fund_infos,
                max_concurrency=4,
            )

            yield event.plain_result(
                self._format_position_realtime_snapshot(
                    positions=positions,
                    fund_infos=fund_infos,
                    close_change_map=close_change_map,
                    qdii_cache_hits=qdii_cache_hits,
                )
            )
        except Exception as e:
            logger.error(f"sscc 实时持仓查询失败: {e}")
            yield event.plain_result(f"❌ 实时持仓查询失败: {str(e)}")

    @filter.command("ckcc")
    async def check_fund_positions(self, event: AstrMessageEvent):
        """
        查看当前基金持仓和收益
        用法: ckcc
        """
        try:
            self._ensure_nav_sync_task()
            platform, user_id = self._resolve_position_owner(event)
            if not user_id:
                yield event.plain_result("❌ 无法识别当前用户 ID，请稍后再试")
                return

            positions = self.data_handler.list_positions(platform=platform, user_id=user_id)
            if not positions:
                yield event.plain_result(
                    "📭 当前没有基金持仓记录\n"
                    "💡 用法: 增加基金持仓 {基金代码,平均成本,持有份额}\n"
                    "💡 示例: 增加基金持仓 {161226,1.0234,1200}"
                )
                return

            yield event.plain_result("📊 正在统计当前持仓收益...")
            fund_infos = await self._batch_fetch_fund_infos(
                [str(item.get("fund_code", "")) for item in positions]
            )
            yield event.plain_result(self._format_position_overview(positions, fund_infos))

        except Exception as e:
            logger.error(f"查看持仓失败: {e}")
            yield event.plain_result(f"❌ 持仓查询失败: {str(e)}")

    @filter.command("修复基金持仓数据")
    async def repair_fund_position_data(self, event: AstrMessageEvent):
        """
        修复当前用户的持仓相关基金数据（代码标准化、名称补齐、持仓重关联）。
        用法: 修复基金持仓数据
        """
        try:
            self._ensure_nav_sync_task()
            platform, user_id = self._resolve_position_owner(event)
            if not user_id:
                yield event.plain_result("❌ 无法识别当前用户 ID，请稍后再试")
                return

            positions = self.data_handler.list_positions(platform=platform, user_id=user_id)
            if not positions:
                yield event.plain_result(
                    "📭 当前没有基金持仓记录\n"
                    "💡 请先使用：增加基金持仓 {基金代码,平均成本,持有份额}"
                )
                return

            yield event.plain_result("🛠️ 正在修复你的持仓相关基金数据...")

            normalized_codes: list[str] = []
            seen_codes = set()
            fund_name_map: dict[str, str] = {}
            for item in positions:
                raw_code = str(item.get("fund_code") or "").strip()
                normalized_code = self._normalize_fund_code(raw_code) or raw_code
                if normalized_code and normalized_code not in seen_codes:
                    seen_codes.add(normalized_code)
                    normalized_codes.append(normalized_code)
                local_name = str(item.get("fund_name") or "").strip()
                if normalized_code and local_name and normalized_code not in fund_name_map:
                    fund_name_map[normalized_code] = local_name

            fund_infos = await self._batch_fetch_fund_infos(
                normalized_codes,
                max_concurrency=4,
            )
            for code, info in fund_infos.items():
                if info and getattr(info, "name", ""):
                    normalized_code = self._normalize_fund_code(code) or str(code).strip()
                    if normalized_code:
                        fund_name_map[normalized_code] = str(info.name).strip()

            stats = self.data_handler.repair_user_position_funds(
                platform=platform,
                user_id=user_id,
                fund_name_map=fund_name_map,
            )
            yield event.plain_result(self._format_position_repair_result(stats))
        except ValueError as e:
            yield event.plain_result(f"❌ {str(e)}")
        except Exception as e:
            logger.error(f"修复基金持仓数据失败: {e}")
            yield event.plain_result(f"❌ 修复失败: {str(e)}")

    @filter.command("ckqcjl")
    async def check_clear_history(self, event: AstrMessageEvent):
        """
        查看清仓/卖出历史记录
        用法: ckqcjl [条数]
        """
        try:
            payload_text = self._extract_command_payload(event, "ckqcjl")
            limit = 30
            if payload_text:
                try:
                    limit = int(payload_text.strip())
                except ValueError:
                    yield event.plain_result("❌ 条数必须是数字\n💡 用法: ckqcjl [条数]")
                    return
            limit = max(1, min(limit, 100))

            platform, user_id = self._resolve_position_owner(event)
            if not user_id:
                yield event.plain_result("❌ 无法识别当前用户 ID，请稍后再试")
                return

            logs = self.data_handler.list_position_logs(
                platform=platform,
                user_id=user_id,
                limit=limit,
                actions=["sell", "clear"],
            )
            if not logs:
                yield event.plain_result("📭 暂无清仓/卖出历史记录")
                return

            yield event.plain_result(self._format_clear_history(logs))
        except Exception as e:
            logger.error(f"查看清仓历史失败: {e}")
            yield event.plain_result(f"❌ 清仓历史查询失败: {str(e)}")

    @filter.command("更新持仓基金净值")
    async def refresh_position_fund_nav(self, event: AstrMessageEvent):
        """
        主动刷新当前用户持仓基金的历史净值（增量）。
        用法: 更新持仓基金净值
        """
        try:
            self._ensure_nav_sync_task()
            platform, user_id = self._resolve_position_owner(event)
            if not user_id:
                yield event.plain_result("❌ 无法识别当前用户 ID，请稍后再试")
                return

            positions = self.data_handler.list_positions(platform=platform, user_id=user_id)
            if not positions:
                yield event.plain_result(
                    "📭 当前没有基金持仓记录\n"
                    "💡 请先使用：增加基金持仓 {基金代码,平均成本,持有份额}"
                )
                return

            fund_codes: list[str] = []
            seen_codes = set()
            for item in positions:
                code = str(item.get("fund_code", "")).strip()
                if code and code not in seen_codes:
                    seen_codes.add(code)
                    fund_codes.append(code)

            yield event.plain_result(
                f"🔄 正在刷新你持仓的 {len(fund_codes)} 只基金净值（增量）..."
            )
            stats = await self._sync_position_funds_nav(
                fund_codes=fund_codes,
                force_full=False,
                trigger="manual",
            )
            yield event.plain_result(
                self._format_nav_sync_result(stats, "✅ 持仓基金净值刷新完成")
            )
        except Exception as e:
            logger.error(f"手动刷新持仓基金净值失败: {e}")
            yield event.plain_result(f"❌ 净值刷新失败: {str(e)}")

    @filter.command("智能分析")
    async def ai_fund_analysis(self, event: AstrMessageEvent, code: str = ""):
        """
        使用大模型进行智能基金分析（含量化数据）
        用法: 智能分析 [基金代码]
        示例: 智能分析 161226
        """
        try:
            user_id = event.get_sender_id()
            # 标准化基金代码，补齐前导0
            normalized_code = self._normalize_fund_code(code)
            fund_code = normalized_code or self._get_user_fund(user_id)

            yield event.plain_result(
                f"🤖 正在对基金 {fund_code} 进行智能分析...\n"
                "📊 收集数据中，请稍候（约需30秒）..."
            )

            # 1. 获取基金基本信息
            info = await self.analyzer.get_lof_realtime(fund_code)
            if not info:
                # 区分是基金代码错误还是数据源问题
                if not normalized_code:
                    yield event.plain_result(f"❌ 基金代码不能为空")
                    return

                # 如果代码是6位数字，通常是有效的基金代码格式，但未找到数据
                if len(normalized_code) == 6 and normalized_code.isdigit():
                    # 尝试再次搜索确认是否存在
                    try:
                        search_res = await self.analyzer.search_fund(normalized_code)
                        if not search_res:
                            yield event.plain_result(
                                f"❌ 未找到基金代码 {fund_code}\n"
                                "💡 请检查代码是否正确，或使用「搜索基金 关键词」查找"
                            )
                            return
                    except Exception:
                        pass  # 搜索出错忽略，继续下面的判断

                yield event.plain_result(
                    f"⚠️ 暂时无法获取基金 {fund_code} 的数据\n"
                    "💡 可能是数据源暂时不可用，或该基金为非LOF基金\n"
                    "💡 请稍后重试"
                )
                return
                return

            # 2. 获取历史数据（获取60天以支持更多回测策略）
            history = await self.analyzer.get_lof_history(fund_code, days=60)

            # 3. 计算技术指标（保留旧方法兼容性）
            indicators = {}
            if history:
                indicators = self.analyzer.calculate_technical_indicators(history)

            # 4. 检查大模型是否可用
            provider = self.context.get_using_provider()
            if not provider:
                yield event.plain_result(
                    "❌ 未配置大模型提供商\n"
                    "💡 请在 AstrBot 管理面板配置 LLM 提供商后再试"
                )
                return

            yield event.plain_result(
                "🧠 AI 正在分析数据，生成报告中...\n📈 正在计算量化指标和策略回测..."
            )

            # 5. 使用 AI 分析器执行分析（含量化数据）
            try:
                analysis_result = await self.ai_analyzer.analyze(
                    fund_info=info,
                    history_data=history or [],
                    technical_indicators=indicators,
                    user_id=user_id,
                )

                # 获取技术信号
                signal, score = self.ai_analyzer.get_technical_signal(history or [])

                # 使用 markdown 库将 Markdown 转换为 HTML
                try:
                    import markdown
                    formatted_content = markdown.markdown(
                        analysis_result,
                        extensions=['nl2br', 'tables', 'fenced_code']
                    )
                except ImportError:
                    # 如果 markdown 库不可用，回退到简单的正则替换
                    import re
                    formatted_content = re.sub(
                        r"\*\*(.*?)\*\*", r"<strong>\1</strong>", analysis_result
                    )
                    # 处理换行
                    formatted_content = formatted_content.replace("\n", "<br>")

                # 准备模板数据
                data = {
                    "fund_name": info.name,
                    "fund_code": info.code,
                    "latest_price": info.latest_price,
                    "change_amount": info.change_amount,
                    "change_rate": info.change_rate,
                    "signal": signal,
                    "score": score,
                    "analysis_content": formatted_content,
                    "generated_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }

                # 读取模板
                template_path = self._data_dir / "templates" / "ai_analysis_report.html"
                if not template_path.exists():
                    template_path = (
                        Path(__file__).parent / "templates" / "ai_analysis_report.html"
                    )

                if not template_path.exists():
                    # 降级到文本模式
                    header = f"""
🤖 【{info.name}】智能量化分析报告
━━━━━━━━━━━━━━━━━
📅 分析时间: {datetime.now().strftime("%Y-%m-%d %H:%M")}
💰 当前价格: {info.latest_price:.4f} ({info.change_rate:+.2f}%)
📊 技术信号: {signal} (评分: {score})
━━━━━━━━━━━━━━━━━
""".strip()
                    yield event.plain_result(f"{header}\n\n{analysis_result}")
                else:
                    # 渲染图片 - 优先使用本地渲染器
                    if self.use_local_renderer:
                        try:
                            img_path = await render_fund_image(
                                template_path=template_path,
                                template_data=data,
                                width=480
                            )
                            yield event.image_result(img_path)
                        except Exception as e:
                            logger.warning(f"本地渲染失败，回退到网络渲染: {e}")
                            with open(template_path, "r", encoding="utf-8") as f:
                                template_str = f.read()
                            img_url = await self.image_renderer.render_custom_template(
                                tmpl_str=template_str, tmpl_data=data, return_url=True
                            )
                            yield event.image_result(img_url)
                    else:
                        with open(template_path, "r", encoding="utf-8") as f:
                            template_str = f.read()
                        img_url = await self.image_renderer.render_custom_template(
                            tmpl_str=template_str, tmpl_data=data, return_url=True
                        )
                        yield event.image_result(img_url)

                # 添加免责声明 (如果是图片模式，免责声明已包含在图片底部，这里可以省略，或者发一条简短的)
                # yield event.plain_result("⚠️ 投资有风险，决策需谨慎。")

            except ValueError as e:
                yield event.plain_result(f"❌ {str(e)}")
            except Exception as e:
                logger.error(f"AI分析失败: {e}")
                yield event.plain_result(
                    f"❌ AI 分析失败: {str(e)}\n"
                    "💡 可能是大模型服务暂时不可用，请稍后再试"
                )

        except ImportError:
            yield event.plain_result(
                "❌ AKShare 库未安装\n请管理员执行: pip install akshare"
            )
        except TimeoutError as e:
            yield event.plain_result(f"⏰ {str(e)}\n💡 数据源响应较慢，请稍后再试")
        except Exception as e:
            logger.error(f"智能分析出错: {e}")
            yield event.plain_result(f"❌ 分析失败: {str(e)}")

    @filter.command("量化分析")
    async def quant_analysis(self, event: AstrMessageEvent, code: str = ""):
        """
        纯量化分析（无需大模型）
        包含绩效指标、技术指标、策略回测
        用法: 量化分析 [基金代码]
        示例: 量化分析 161226
        """
        try:
            user_id = event.get_sender_id()
            # 标准化基金代码，补齐前导0
            normalized_code = self._normalize_fund_code(code)
            fund_code = normalized_code or self._get_user_fund(user_id)

            yield event.plain_result(
                f"📊 正在对基金 {fund_code} 进行量化分析...\n"
                "🔢 计算绩效指标、技术指标、策略回测中..."
            )

            # 1. 获取基金基本信息
            info = await self.analyzer.get_lof_realtime(fund_code)
            if not info:
                # 区分是基金代码错误还是数据源问题
                if not normalized_code:
                    yield event.plain_result(f"❌ 基金代码不能为空")
                    return

                # 如果代码是6位数字，通常是有效的基金代码格式，但未找到数据
                if len(normalized_code) == 6 and normalized_code.isdigit():
                    # 尝试再次搜索确认是否存在
                    try:
                        search_res = await self.analyzer.search_fund(normalized_code)
                        if not search_res:
                            yield event.plain_result(
                                f"❌ 未找到基金代码 {fund_code}\n"
                                "💡 请检查代码是否正确，或使用「搜索基金 关键词」查找"
                            )
                            return
                    except Exception:
                        pass  # 搜索出错忽略，继续下面的判断

                yield event.plain_result(
                    f"⚠️ 暂时无法获取基金 {fund_code} 的数据\n"
                    "💡 可能是数据源暂时不可用，或该基金为非LOF基金\n"
                    "💡 请稍后重试"
                )
                return
                return

            # 2. 获取60天历史数据
            history = await self.analyzer.get_lof_history(fund_code, days=60)

            if not history or len(history) < 20:
                yield event.plain_result(
                    f"📊 【{info.name}】\n"
                    "⚠️ 历史数据不足（需要至少20天），无法进行量化分析"
                )
                return

            # 3. 使用量化分析器生成报告（无需 LLM）
            quant_report = self.ai_analyzer.get_quant_summary(history)

            # 4. 输出报告
            header = f"""
📈 【{info.name}】量化分析报告
━━━━━━━━━━━━━━━━━
🔢 基金代码: {info.code}
💰 当前价格: {info.latest_price:.4f}
📊 今日涨跌: {info.change_rate:+.2f}%
📅 分析时间: {datetime.now().strftime("%Y-%m-%d %H:%M")}
━━━━━━━━━━━━━━━━━
""".strip()

            yield event.plain_result(f"{header}\n\n{quant_report}")

            # 添加说明
            yield event.plain_result(
                "━━━━━━━━━━━━━━━━━\n"
                "📌 指标说明:\n"
                "• 夏普比率 > 1 表示风险调整后收益较好\n"
                "• 最大回撤反映历史最大亏损幅度\n"
                "• VaR 95% 表示95%概率下的最大日亏损\n"
                "• 策略回测基于历史数据模拟\n"
                "━━━━━━━━━━━━━━━━━\n"
                "💡 使用「智能分析」可获取 AI 深度解读"
            )

        except ImportError:
            yield event.plain_result(
                "❌ AKShare 库未安装\n请管理员执行: pip install akshare"
            )
        except TimeoutError as e:
            yield event.plain_result(f"⏰ {str(e)}\n💡 数据源响应较慢，请稍后再试")
        except Exception as e:
            logger.error(f"量化分析出错: {e}")
            yield event.plain_result(f"❌ 分析失败: {str(e)}")

    def _plot_comparison_chart(
        self,
        history_a: list[dict],
        name_a: str,
        history_b: list[dict],
        name_b: str,
    ) -> str | None:
        return self.analysis_service.plot_comparison_chart(
            history_a=history_a,
            name_a=name_a,
            history_b=history_b,
            name_b=name_b,
        )

    @filter.command("基金对比")
    async def fund_compare(
        self, event: AstrMessageEvent, code1: str = "", code2: str = ""
    ):
        """
        对比两只基金的表现
        用法: 基金对比 [代码1] [代码2]
        示例: 基金对比 161226 160220
        """
        if not code1 or not code2:
            yield event.plain_result(
                "❌ 请提供两个基金代码\n用法: 基金对比 代码1 代码2\n示例: 基金对比 161226 160220"
            )
            return

        try:
            # 标准化代码
            code1 = self._normalize_fund_code(code1) or code1
            code2 = self._normalize_fund_code(code2) or code2

            yield event.plain_result(f"⚖️ 正在对比基金 {code1} vs {code2}...")

            # 并发获取两个基金的信息和历史数据
            # 使用 gather 提高效率
            task1 = self.analyzer.get_lof_realtime(code1)
            task2 = self.analyzer.get_lof_realtime(code2)
            task3 = self.analyzer.get_lof_history(code1, days=60)
            task4 = self.analyzer.get_lof_history(code2, days=60)

            info1, info2, hist1, hist2 = await asyncio.gather(
                task1, task2, task3, task4
            )

            if not info1:
                # 尝试区分错误原因 (基金1)
                if len(code1) == 6 and code1.isdigit():
                    try:
                        search_res = await self.analyzer.search_fund(code1)
                        if not search_res:
                            yield event.plain_result(
                                f"❌ 未找到基金代码 {code1}\n"
                                "💡 请检查代码是否正确，或使用「搜索基金 关键词」查找"
                            )
                            return
                    except Exception:
                        pass

                yield event.plain_result(
                    f"⚠️ 暂时无法获取基金 {code1} 的数据\n"
                    "💡 可能是数据源暂时不可用，或该基金为非LOF基金\n"
                    "💡 请稍后重试"
                )
                return

            if not info2:
                # 尝试区分错误原因 (基金2)
                if len(code2) == 6 and code2.isdigit():
                    try:
                        search_res = await self.analyzer.search_fund(code2)
                        if not search_res:
                            yield event.plain_result(
                                f"❌ 未找到基金代码 {code2}\n"
                                "💡 请检查代码是否正确，或使用「搜索基金 关键词」查找"
                            )
                            return
                    except Exception:
                        pass

                yield event.plain_result(
                    f"⚠️ 暂时无法获取基金 {code2} 的数据\n"
                    "💡 可能是数据源暂时不可用，或该基金为非LOF基金\n"
                    "💡 请稍后重试"
                )
                return
            if not hist1 or len(hist1) < 10:
                yield event.plain_result(f"⚠️ 基金 {code1} 历史数据不足")
                return
            if not hist2 or len(hist2) < 10:
                yield event.plain_result(f"⚠️ 基金 {code2} 历史数据不足")
                return

            # 计算量化指标
            from .ai_analyzer.quant import QuantAnalyzer

            quant = QuantAnalyzer()

            perf1 = quant.calculate_performance(hist1)
            perf2 = quant.calculate_performance(hist2)

            if not perf1 or not perf2:
                yield event.plain_result("❌ 计算绩效指标失败")
                return

            # 绘制对比图
            plot_img = await asyncio.to_thread(
                self._plot_comparison_chart, hist1, info1.name, hist2, info2.name
            )

            # 准备模板数据
            data = {
                "fund_a_name": info1.name,
                "fund_b_name": info2.name,
                "fund_a_code": info1.code,
                "fund_b_code": info2.code,
                "days": 60,
                "metrics_a": perf1,
                "metrics_b": perf2,
                "plot_img": plot_img,
                "generated_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }

            # 渲染模板
            template_path = self._data_dir / "templates" / "comparison_report.html"
            if not template_path.exists():
                template_path = (
                    Path(__file__).parent / "templates" / "comparison_report.html"
                )

            if not template_path.exists():
                yield event.plain_result("❌ 模板文件缺失")
                return

            with open(template_path, "r", encoding="utf-8") as f:
                template_str = f.read()

            img_url = await self.image_renderer.render_custom_template(
                tmpl_str=template_str, tmpl_data=data, return_url=True
            )

            yield event.image_result(img_url)

        except Exception as e:
            logger.error(f"基金对比出错: {e}")
            yield event.plain_result(f"❌ 对比失败: {str(e)}")

    @filter.command("基金帮助")
    async def fund_help(self, event: AstrMessageEvent):
        """显示基金分析插件帮助信息"""
        help_text = """
📊 基金/股票分析插件帮助
━━━━━━━━━━━━━━━━━
💰 贵金属行情:
🔹 今日行情 - 查询金价银价实时行情
━━━━━━━━━━━━━━━━━
📈 A股实时行情 (缓存10分钟):
🔹 股票 <代码> - 查询A股实时行情
🔹 搜索股票 关键词 - 搜索A股股票
━━━━━━━━━━━━━━━━━
📊 LOF基金功能:
🔹 ssgz <代码> - 查询基金实时估值（场外基金）
🔹 基金 [代码] - 查询基金实时行情
🔹 基金分析 [代码] - 技术分析(均线/趋势)
🔹 基金对比 [代码1] [代码2] - ⚖️对比两只基金
🔹 量化分析 [代码] - 📈专业量化指标分析
🔹 智能分析 [代码] - 🤖AI量化深度分析
🔹 基金历史 [代码] [天数] - 查看历史行情
🔹 搜索基金 关键词 - 搜索LOF基金
🔹 设置基金 代码 - 设置默认基金
🔹 增加基金持仓 {代码,成本,份额} - 记录个人持仓（支持批量）
🔹 清仓基金 [基金代码] [份额|百分比] - 卖出基金份额（默认全仓）
🔹 sscc - 查看当前持仓基金现价与最近收盘涨跌幅
🔹 ckcc - 查看当前持仓与收益
🔹 修复基金持仓数据 - 修复当前用户的持仓相关基金数据
🔹 ckqcjl [条数] - 查看清仓/卖出历史记录
🔹 更新持仓基金净值 - 主动刷新持仓基金净值（增量）
🔹 基金帮助 - 显示本帮助
━━━━━━━━━━━━━━━━━
💡 默认基金: 国投瑞银白银期货(LOF)A
   基金代码: 161226
━━━━━━━━━━━━━━━━━
📈 示例:
  • 今日行情 (金银价格)
  • 股票 000001 (平安银行)
  • 搜索股票 茅台
  • ssgz 001632
  • 基金 161226
  • 基金分析
  • 基金对比 161226 513100
  • 量化分析 161226
  • 智能分析 161226
  • 基金历史 161226 20
  • 搜索基金 白银
  • 增加基金持仓 {161226,1.0234,1200} {001632,2.1456,500}
  • 清仓基金 161226 25%
  • sscc
  • ckqcjl 20
  • ckcc
  • 修复基金持仓数据
  • 更新持仓基金净值
━━━━━━━━━━━━━━━━━
🤖 智能分析功能说明:
  调用AI大模型+量化数据，综合分析:
  - 量化绩效评估和风险分析
  - 技术指标深度解读
  - 策略回测结果解读
  - 相关市场动态和新闻
  - 上涨趋势和概率预测
━━━━━━━━━━━━━━━━━
⚠️ 数据来源: AKShare/国际金价网
💡 A股数据缓存10分钟，仅供参考
💡 投资有风险，入市需谨慎！
""".strip()
        yield event.plain_result(help_text)

    async def terminate(self):
        """插件停止时的清理工作"""
        await self.nav_sync_service.stop()
        logger.info("基金分析插件已停止")
