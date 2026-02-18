from datetime import datetime
from typing import Any

import aiohttp


class MarketService:
    """市场数据服务（当前聚焦贵金属行情）。"""

    def __init__(self, logger: Any, metal_cache_ttl: int = 900):
        self._logger = logger
        self._metal_cache_ttl = metal_cache_ttl
        self._metal_cache: dict[str, Any] = {}
        self._metal_cache_time: datetime | None = None

    async def fetch_precious_metal_prices(self) -> dict:
        """
        从 NowAPI 获取上海黄金交易所贵金属价格。
        黄金使用 1051，白银使用 1052，缓存 15 分钟。
        """
        now = datetime.now()
        if (
            self._metal_cache
            and self._metal_cache_time is not None
            and (now - self._metal_cache_time).total_seconds() < self._metal_cache_ttl
        ):
            self._logger.debug("使用贵金属价格缓存")
            return self._metal_cache

        api_url = "http://api.k780.com/"
        base_params = {
            "app": "finance.gold_price",
            "appkey": "78365",
            "sign": "776f93b557ce6e6afeb860b103a587c7",
            "format": "json",
        }

        prices: dict[str, Any] = {}

        async def fetch_metal(gold_id: str, key: str, name: str) -> dict | None:
            params = {**base_params, "goldid": gold_id}
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        api_url, params=params, timeout=aiohttp.ClientTimeout(total=30)
                    ) as response:
                        if response.status != 200:
                            self._logger.error(f"获取{name}价格失败: HTTP {response.status}")
                            return None

                        data = await response.json()
                        if data.get("success") != "1":
                            error_msg = data.get("msg", "未知错误")
                            self._logger.error(f"NowAPI返回错误({name}): {error_msg}")
                            return None

                        result = data.get("result", {})
                        dt_list = result.get("dtList", {})
                        if gold_id not in dt_list:
                            return None

                        metal_data = dt_list[gold_id]
                        return {
                            "name": metal_data.get("varietynm", name),
                            "variety": metal_data.get("variety", ""),
                            "price": float(metal_data.get("last_price", 0) or 0),
                            "buy_price": float(metal_data.get("buy_price", 0) or 0),
                            "sell_price": float(metal_data.get("sell_price", 0) or 0),
                            "high": float(metal_data.get("high_price", 0) or 0),
                            "low": float(metal_data.get("low_price", 0) or 0),
                            "open": float(metal_data.get("open_price", 0) or 0),
                            "prev_close": float(metal_data.get("yesy_price", 0) or 0),
                            "change": float(metal_data.get("change_price", 0) or 0),
                            "change_rate": metal_data.get("change_margin", "0%"),
                            "update_time": metal_data.get("uptime", ""),
                        }
            except Exception as e:
                self._logger.error(f"获取{name}价格出错: {e}")
                return None

        try:
            gold_data = await fetch_metal("1051", "au_td", "黄金")
            if gold_data:
                prices["au_td"] = gold_data

            silver_data = await fetch_metal("1052", "ag_td", "白银")
            if silver_data:
                prices["ag_td"] = silver_data

            if prices:
                self._metal_cache = prices
                self._metal_cache_time = now
                self._logger.info("贵金属价格已更新并缓存15分钟")
            return prices
        except Exception as e:
            self._logger.error(f"获取贵金属价格出错: {e}")
            if self._metal_cache:
                self._logger.info("使用过期的贵金属缓存数据")
                return self._metal_cache
            return {}

