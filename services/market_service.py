import re
from datetime import date, datetime
from html import unescape
from typing import Any

import aiohttp

try:
    from playwright.async_api import TimeoutError as PlaywrightTimeoutError
    from playwright.async_api import async_playwright

    PLAYWRIGHT_AVAILABLE = True
except Exception:
    PlaywrightTimeoutError = RuntimeError
    async_playwright = None
    PLAYWRIGHT_AVAILABLE = False


class MarketService:
    """市场数据服务（当前版本仅提供黄金行情）。"""

    EASTMONEY_GOLD_URL = "https://quote.eastmoney.com/globalfuture/GC00Y.html"
    GOOGLE_USD_CNY_URL = (
        "https://www.google.com/search?q=%E7%BE%8E%E5%85%83%E5%85%91%E4%BA%BA%E6%B0%91%E5%B8%81"
    )
    USD_OUNCE_TO_GRAM = 31.1035
    EXCHANGE_RATE_HINT = "未查询到今日汇率，请发送：更新今日汇率 <1美元兑人民币>"
    EXCHANGE_RATE_STALE_HINT = "今日汇率数据可能已经产生了变化，请注意甄别"

    def __init__(
        self,
        logger: Any,
        metal_cache_ttl: int = 900,
        data_handler: Any | None = None,
    ):
        self._logger = logger
        self._metal_cache_ttl = metal_cache_ttl
        self._data_handler = data_handler
        self._metal_cache: dict[str, Any] = {}
        self._metal_cache_time: datetime | None = None
        self._exchange_rate_cache: dict[str, Any] = {}
        self._exchange_rate_query_day: str | None = None

    def update_today_exchange_rate(self, rate: float) -> dict[str, Any]:
        """手动更新当日美元兑人民币汇率。"""
        if rate <= 0:
            raise ValueError("汇率必须大于0")

        today = date.today().isoformat()
        now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        payload = {
            "date": today,
            "rate": float(rate),
            "source": "manual",
            "source_text": "用户手动更新",
            "update_time": now_text,
            "query_time": now_text,
        }

        record = self._persist_exchange_rate_record(payload)
        self._exchange_rate_cache = dict(record)
        self._exchange_rate_query_day = today
        # 汇率变化会影响折算后的国内金价，清空行情缓存以便立即生效。
        self._metal_cache = {}
        self._metal_cache_time = None
        return record

    async def fetch_precious_metal_prices(self) -> dict[str, Any]:
        """
        获取贵金属行情（当前仅黄金）：
        1) COMEX 黄金数据来自东方财富页面（Playwright 抓取）。
        2) 美元兑人民币汇率每天从 Google 查询一次。
        3) 国内金价按公式换算：COMEX黄金价格 * 汇率 / 31.1035。
        """
        now = datetime.now()
        if (
            self._metal_cache
            and self._metal_cache_time is not None
            and (now - self._metal_cache_time).total_seconds() < self._metal_cache_ttl
        ):
            self._logger.debug("使用贵金属行情缓存")
            return self._metal_cache

        try:
            comex_gold = await self._fetch_comex_gold_from_eastmoney()
            if not comex_gold:
                if self._metal_cache:
                    self._logger.info("黄金行情抓取失败，使用过期缓存")
                    return self._metal_cache
                return {}

            exchange_rate = await self._get_today_usd_cny_rate()
            result: dict[str, Any] = {
                "comex_gold": comex_gold,
                "exchange_rate": exchange_rate or {},
                "domestic_gold": {},
                "rate_missing": False,
                "rate_missing_hint": "",
            }

            price_usd = float(comex_gold.get("price", 0) or 0)
            rate = float((exchange_rate or {}).get("rate", 0) or 0)
            if price_usd > 0 and rate > 0:
                domestic_price = price_usd * rate / self.USD_OUNCE_TO_GRAM
                result["domestic_gold"] = {
                    "price_cny_per_gram": domestic_price,
                    "formula": "COMEX黄金价格 * 美元兑人民币汇率 / 31.1035",
                    "base_price_usd_per_ounce": price_usd,
                    "usd_cny_rate": rate,
                }
            else:
                result["rate_missing"] = True
                result["rate_missing_hint"] = self.EXCHANGE_RATE_HINT

            self._metal_cache = result
            self._metal_cache_time = now
            self._logger.info("贵金属行情已更新并缓存15分钟（当前仅黄金）")
            return result
        except Exception as e:
            self._logger.error(f"获取贵金属行情失败: {e}")
            if self._metal_cache:
                self._logger.info("使用过期贵金属缓存数据")
                return self._metal_cache
            return {}

    async def _fetch_comex_gold_from_eastmoney(self) -> dict[str, Any] | None:
        if not PLAYWRIGHT_AVAILABLE or async_playwright is None:
            self._logger.error("Playwright 不可用，无法抓取东方财富黄金行情")
            return None

        page = None
        browser = None
        playwright = None
        try:
            playwright = await async_playwright().start()
            browser = await playwright.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-blink-features=AutomationControlled",
                ],
            )
            page = await browser.new_page()
            await page.goto(
                self.EASTMONEY_GOLD_URL,
                wait_until="domcontentloaded",
                timeout=60000,
            )
            await page.wait_for_selector(
                "div.zsquote3l.zs_brief div.brief_info_c table",
                timeout=15000,
            )

            snapshot: dict[str, Any] | None = None
            for _ in range(8):
                snapshot = await page.evaluate(
                    """() => {
                        const root = document.querySelector("div.zsquote3l.zs_brief");
                        if (!root) return null;
                        const latest = root
                            .querySelector("div.quote_quotenums .zxj")
                            ?.innerText?.trim() || "";
                        const quoteTime = document
                            .querySelector("div.quote_title span.quote_title_time")
                            ?.innerText?.trim() || "";
                        const cells = Array.from(
                            root.querySelectorAll("div.brief_info_c table td")
                        ).map((td) => (td.innerText || "").replace(/\\s+/g, " ").trim());
                        return { latest, quoteTime, cells };
                    }"""
                )
                if self._has_valid_eastmoney_snapshot(snapshot):
                    break
                await page.wait_for_timeout(1000)

            if not snapshot:
                self._logger.error("东方财富页面结构异常：未获取到行情快照")
                return None

            comex_gold = self._build_comex_gold_data(snapshot)
            if not comex_gold:
                self._logger.error("东方财富页面未解析到有效黄金数据")
                return None
            return comex_gold
        except PlaywrightTimeoutError:
            self._logger.error("抓取东方财富黄金行情超时")
            return None
        except Exception as e:
            self._logger.error(f"抓取东方财富黄金行情失败: {e}")
            return None
        finally:
            try:
                if page:
                    await page.close()
            except Exception:
                pass
            try:
                if browser:
                    await browser.close()
            except Exception:
                pass
            try:
                if playwright:
                    await playwright.stop()
            except Exception:
                pass

    async def _get_today_usd_cny_rate(self) -> dict[str, Any] | None:
        today = date.today().isoformat()

        today_rate = self._get_exchange_rate_on_date(today)
        if today_rate:
            today_rate["is_fallback"] = False
            today_rate["stale_hint"] = ""
            return today_rate

        # 每天只主动查询一次 Google 汇率，避免频繁请求。
        if self._exchange_rate_query_day != today:
            self._exchange_rate_query_day = today
            latest_rate = await self._fetch_usd_cny_rate_from_google()
            if latest_rate:
                stored_rate = self._persist_exchange_rate_record(latest_rate)
                stored_rate["is_fallback"] = False
                stored_rate["stale_hint"] = ""
                return stored_rate

        # 当日汇率无法获取时，回退到最近一次有效汇率（手动/自动皆可）。
        latest_valid_rate = self._get_latest_valid_exchange_rate()
        if latest_valid_rate:
            latest_valid_rate["is_fallback"] = True
            latest_valid_rate["stale_hint"] = self.EXCHANGE_RATE_STALE_HINT
            return latest_valid_rate

        return None

    async def _fetch_usd_cny_rate_from_google(self) -> dict[str, Any] | None:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    self.GOOGLE_USD_CNY_URL,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as response:
                    if response.status != 200:
                        self._logger.warning(
                            f"Google 汇率查询失败: HTTP {response.status}"
                        )
                        return None
                    html = await response.text(errors="ignore")
        except Exception as e:
            self._logger.warning(f"Google 汇率查询异常: {e}")
            return None

        rate = self._extract_float_with_patterns(
            html,
            [
                r'data-exchange-rate="([0-9]+(?:\.[0-9]+)?)"',
                r'class="DFlfde SwHCTb"[^>]*data-value="([0-9]+(?:\.[0-9]+)?)"',
                r'1</span>\s*<span[^>]*>美元</span>\s*等于.*?data-value="([0-9]+(?:\.[0-9]+)?)"',
            ],
        )
        if rate <= 0:
            self._logger.warning("Google 页面未解析到有效美元兑人民币汇率")
            return None

        source_text = ""
        source_match = re.search(r'<div class="k0Rg6d hqAUc">(.*?)</div>', html, re.S)
        if source_match:
            source_text = self._clean_html_text(source_match.group(1))

        now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return {
            "date": date.today().isoformat(),
            "rate": float(rate),
            "source": "google",
            "source_text": source_text or "Google 汇率换算",
            "update_time": source_text or now_text,
            "query_time": now_text,
        }

    def _has_valid_eastmoney_snapshot(self, snapshot: Any) -> bool:
        if not isinstance(snapshot, dict):
            return False
        latest = str(snapshot.get("latest", "")).strip()
        if self._is_valid_value_text(latest):
            return True
        cells = snapshot.get("cells")
        if not isinstance(cells, list):
            return False
        for item in cells:
            _, value = self._split_cell_text(item)
            if self._is_valid_value_text(value):
                return True
        return False

    def _build_comex_gold_data(self, snapshot: dict[str, Any]) -> dict[str, Any] | None:
        cells = snapshot.get("cells", [])
        if not isinstance(cells, list):
            return None

        fields: dict[str, str] = {}
        for item in cells:
            key, value = self._split_cell_text(item)
            if key:
                fields[key] = value

        latest_text = str(snapshot.get("latest", "")).strip()
        latest_price = self._parse_decimal(latest_text)
        prev_close = self._parse_decimal(fields.get("昨结", ""))
        change = self._parse_decimal(fields.get("涨跌额", ""))

        if latest_price <= 0 and prev_close > 0 and self._is_valid_value_text(fields.get("涨跌额", "")):
            latest_price = prev_close + change
            latest_text = f"{latest_price:.2f}"

        if latest_price <= 0 and not self._is_valid_value_text(latest_text):
            return None

        return {
            "name": "COMEX黄金",
            "symbol": "GC00Y",
            "unit": "美元/盎司",
            "price": latest_price,
            "price_text": latest_text or "-",
            "open": self._parse_decimal(fields.get("今开", "")),
            "high": self._parse_decimal(fields.get("最高", "")),
            "low": self._parse_decimal(fields.get("最低", "")),
            "prev_close": prev_close,
            "change": change,
            "change_rate": fields.get("涨跌幅", "-"),
            "volume_text": fields.get("成交量", "-"),
            "position_text": fields.get("持仓量", "-"),
            "outer_text": fields.get("外盘", "-"),
            "inner_text": fields.get("内盘", "-"),
            "spread_text": fields.get("仓差", "-"),
            "day_increment_text": fields.get("日增", "-"),
            "update_time": str(snapshot.get("quoteTime", "")).strip(),
            "source_url": self.EASTMONEY_GOLD_URL,
            "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    @staticmethod
    def _split_cell_text(text: Any) -> tuple[str, str]:
        cell_text = str(text or "").strip()
        for sep in (":", "："):
            if sep in cell_text:
                key, value = cell_text.split(sep, 1)
                return key.strip(), value.strip()
        return cell_text, ""

    @staticmethod
    def _is_valid_value_text(text: Any) -> bool:
        value = str(text or "").strip()
        return value not in {"", "-", "--", "—"}

    @staticmethod
    def _parse_decimal(value: Any) -> float:
        text = str(value or "").strip()
        if text in {"", "-", "--", "—"}:
            return 0.0
        text = text.replace(",", "").replace("，", "").replace("+", "").replace("%", "")
        try:
            return float(text)
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def _extract_float_with_patterns(text: str, patterns: list[str]) -> float:
        for pattern in patterns:
            match = re.search(pattern, text, re.S)
            if not match:
                continue
            try:
                return float(match.group(1))
            except (TypeError, ValueError):
                continue
        return 0.0

    @staticmethod
    def _clean_html_text(raw: str) -> str:
        text = re.sub(r"<[^>]+>", " ", raw or "")
        text = unescape(text)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    @staticmethod
    def _normalize_exchange_rate_record(raw: dict[str, Any]) -> dict[str, Any]:
        rate_date = str(raw.get("date", raw.get("rate_date", "")) or "").strip()
        return {
            "date": rate_date,
            "rate": float(raw.get("rate", 0) or 0),
            "source": str(raw.get("source", "") or "").strip(),
            "source_text": str(raw.get("source_text", "") or "").strip(),
            "update_time": str(raw.get("update_time", "") or "").strip(),
            "query_time": str(raw.get("query_time", "") or "").strip(),
        }

    @staticmethod
    def _is_valid_exchange_rate_record(record: dict[str, Any] | None) -> bool:
        if not isinstance(record, dict):
            return False
        date_text = str(record.get("date", "")).strip()
        if not date_text:
            return False
        rate = float(record.get("rate", 0) or 0)
        return rate > 0

    def _persist_exchange_rate_record(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = self._normalize_exchange_rate_record(payload)
        if not self._is_valid_exchange_rate_record(normalized):
            raise ValueError("无效汇率记录")

        if self._data_handler is not None:
            try:
                saved = self._data_handler.add_exchange_rate_record(
                    currency_pair="USD/CNY",
                    rate=normalized["rate"],
                    rate_date=normalized["date"],
                    source=normalized["source"],
                    source_text=normalized["source_text"],
                    update_time=normalized["update_time"],
                    query_time=normalized["query_time"],
                )
                normalized = self._normalize_exchange_rate_record(saved)
            except Exception as e:
                self._logger.warning(f"写入汇率历史表失败: {e}")

        self._exchange_rate_cache = dict(normalized)
        return dict(normalized)

    def _get_exchange_rate_on_date(self, rate_date: str) -> dict[str, Any] | None:
        cached = self._get_latest_valid_exchange_rate()
        if cached and str(cached.get("date", "")).strip() == str(rate_date or "").strip():
            return dict(cached)

        if self._data_handler is None:
            return None

        try:
            row = self._data_handler.get_exchange_rate_on_date(
                currency_pair="USD/CNY",
                rate_date=rate_date,
            )
        except Exception as e:
            self._logger.warning(f"读取当日汇率失败: {e}")
            return None

        if not isinstance(row, dict):
            return None
        normalized = self._normalize_exchange_rate_record(row)
        if not self._is_valid_exchange_rate_record(normalized):
            return None
        self._exchange_rate_cache = dict(normalized)
        return dict(normalized)

    def _get_latest_valid_exchange_rate(self) -> dict[str, Any] | None:
        if self._is_valid_exchange_rate_record(self._exchange_rate_cache):
            return dict(self._exchange_rate_cache)

        if self._data_handler is None:
            return None

        try:
            row = self._data_handler.get_latest_exchange_rate(currency_pair="USD/CNY")
        except Exception as e:
            self._logger.warning(f"读取最新汇率失败: {e}")
            return None

        if not isinstance(row, dict):
            return None
        normalized = self._normalize_exchange_rate_record(row)
        if not self._is_valid_exchange_rate_record(normalized):
            return None
        self._exchange_rate_cache = dict(normalized)
        return dict(normalized)
