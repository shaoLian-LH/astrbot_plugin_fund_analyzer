import asyncio
from datetime import date, datetime
from typing import Any


class NavSyncService:
    """净值同步服务：后台定时 + 手动增量同步。"""

    def __init__(
        self,
        data_handler: Any,
        analyzer: Any,
        logger: Any,
        interval_seconds: int = 1800,
        default_fetch_days: int = 120,
        max_fetch_days: int = 365,
        fetch_buffer_days: int = 5,
    ):
        self._data_handler = data_handler
        self._analyzer = analyzer
        self._logger = logger
        self._interval_seconds = interval_seconds
        self._default_fetch_days = default_fetch_days
        self._max_fetch_days = max_fetch_days
        self._fetch_buffer_days = fetch_buffer_days
        self._sync_task: asyncio.Task | None = None
        self._sync_lock = asyncio.Lock()

    def ensure_task(self) -> None:
        """确保后台净值增量同步任务已启动。"""
        if self._sync_task and not self._sync_task.done():
            return

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return

        self._sync_task = loop.create_task(self._sync_loop())

    async def stop(self) -> None:
        if self._sync_task and not self._sync_task.done():
            self._sync_task.cancel()
            try:
                await self._sync_task
            except asyncio.CancelledError:
                pass

    @staticmethod
    def _normalize_nav_date_text(value: Any) -> str | None:
        text = str(value or "").strip()
        if not text:
            return None
        text = text[:10]
        try:
            datetime.strptime(text, "%Y-%m-%d")
        except ValueError:
            return None
        return text

    def _calc_nav_fetch_days(
        self, latest_nav_date: str | None, force_full: bool = False
    ) -> int:
        if force_full or not latest_nav_date:
            return self._default_fetch_days

        latest_date_text = self._normalize_nav_date_text(latest_nav_date)
        if not latest_date_text:
            return self._default_fetch_days

        latest_day = datetime.strptime(latest_date_text, "%Y-%m-%d").date()
        delta_days = (date.today() - latest_day).days
        if delta_days < 0:
            return self._default_fetch_days

        return max(
            self._fetch_buffer_days,
            min(delta_days + self._fetch_buffer_days, self._max_fetch_days),
        )

    def _build_nav_records_from_history(
        self,
        history: list[dict[str, Any]],
        latest_nav_date: str | None = None,
    ) -> list[dict[str, Any]]:
        nav_map: dict[str, dict[str, Any]] = {}
        latest_date_text = self._normalize_nav_date_text(latest_nav_date)

        for item in history or []:
            nav_date = self._normalize_nav_date_text(item.get("date"))
            if not nav_date:
                continue
            if latest_date_text and nav_date <= latest_date_text:
                continue

            try:
                unit_nav = float(item.get("close"))
            except (TypeError, ValueError):
                continue
            if unit_nav <= 0:
                continue

            raw_change_rate = item.get("change_rate")
            change_rate: float | None = None
            if raw_change_rate not in (None, "", "--"):
                try:
                    change_rate = float(raw_change_rate)
                except (TypeError, ValueError):
                    change_rate = None

            nav_map[nav_date] = {
                "nav_date": nav_date,
                "unit_nav": unit_nav,
                "change_rate": change_rate,
            }

        return [nav_map[nav_date] for nav_date in sorted(nav_map.keys())]

    async def sync_position_funds_nav(
        self,
        fund_codes: list[str] | None = None,
        force_full: bool = False,
        trigger: str = "manual",
    ) -> dict[str, Any]:
        """
        同步持仓基金净值到本地库（增量）。
        fund_codes 为空时同步所有“有持仓”的基金；不为空时只同步指定基金。
        """
        async with self._sync_lock:
            funds = self._data_handler.list_position_funds()
            if fund_codes:
                code_set = {
                    str(code).strip()
                    for code in fund_codes
                    if str(code).strip()
                }
                funds = [fund for fund in funds if str(fund.get("fund_code")) in code_set]

            stats: dict[str, Any] = {
                "funds_total": len(funds),
                "funds_synced": 0,
                "funds_no_new_data": 0,
                "funds_failed": 0,
                "nav_rows_upserted": 0,
                "errors": [],
            }

            for fund in funds:
                fund_code = str(fund.get("fund_code", "")).strip()
                fund_name = str(fund.get("fund_name", "")).strip()
                if not fund_code:
                    continue

                latest_nav_date = None
                if not force_full:
                    latest_nav_date = self._data_handler.get_latest_nav_date(fund_code)

                fetch_days = self._calc_nav_fetch_days(latest_nav_date, force_full=force_full)

                try:
                    history = await self._analyzer.get_lof_history(fund_code, days=fetch_days)
                    if not history:
                        stats["funds_failed"] += 1
                        stats["errors"].append(f"{fund_code} 无历史数据")
                        continue

                    nav_records = self._build_nav_records_from_history(
                        history=history,
                        latest_nav_date=None if force_full else latest_nav_date,
                    )
                    if not nav_records:
                        stats["funds_no_new_data"] += 1
                        continue

                    upserted = self._data_handler.upsert_fund_nav_history(
                        fund_code=fund_code,
                        fund_name=fund_name,
                        nav_records=nav_records,
                        source=f"{trigger}:eastmoney",
                    )
                    stats["funds_synced"] += 1
                    stats["nav_rows_upserted"] += int(upserted)
                except Exception as e:
                    stats["funds_failed"] += 1
                    stats["errors"].append(f"{fund_code} {str(e)}")

            return stats

    async def _sync_loop(self) -> None:
        """后台定时增量同步持仓基金净值。"""
        try:
            await asyncio.sleep(15)
            while True:
                try:
                    stats = await self.sync_position_funds_nav(trigger="scheduled")
                    if int(stats.get("funds_total", 0)) > 0:
                        self._logger.info(
                            "定时净值同步完成: "
                            f"基金{stats['funds_total']}只, "
                            f"成功{stats['funds_synced']}只, "
                            f"净值写入{stats['nav_rows_upserted']}条, "
                            f"失败{stats['funds_failed']}只"
                        )
                except Exception as e:
                    self._logger.warning(f"定时净值同步失败: {e}")

                await asyncio.sleep(self._interval_seconds)
        except asyncio.CancelledError:
            self._logger.info("定时净值同步任务已停止")
            raise

