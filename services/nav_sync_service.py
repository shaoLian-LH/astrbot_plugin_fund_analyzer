import asyncio
from datetime import datetime, time as dt_time, timedelta, timezone
from typing import Any

import aiohttp

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]


class NavSyncService:
    """净值同步服务：每日判定 + 盘中3分钟同步 + 手动增量同步。"""

    HOLIDAY_API_URL = "http://api.haoshenqi.top/holiday"
    HOLIDAY_MAX_RETRIES = 3
    HOLIDAY_TIMEOUT_SECONDS = 8
    INTRADAY_START_TIME = dt_time(hour=9, minute=40)
    INTRADAY_END_TIME = dt_time(hour=14, minute=55)
    INTRADAY_INTERVAL_SECONDS = 180

    def __init__(
        self,
        data_handler: Any,
        analyzer: Any,
        logger: Any,
        interval_seconds: int = INTRADAY_INTERVAL_SECONDS,
        default_fetch_days: int = 120,
        max_fetch_days: int = 365,
        fetch_buffer_days: int = 5,
        intraday_start_time: dt_time = INTRADAY_START_TIME,
        intraday_end_time: dt_time = INTRADAY_END_TIME,
        holiday_api_url: str = HOLIDAY_API_URL,
        holiday_max_retries: int = HOLIDAY_MAX_RETRIES,
        holiday_timeout_seconds: int = HOLIDAY_TIMEOUT_SECONDS,
        timezone_name: str = "Asia/Shanghai",
    ):
        self._data_handler = data_handler
        self._analyzer = analyzer
        self._logger = logger
        self._interval_seconds = max(60, int(interval_seconds or self.INTRADAY_INTERVAL_SECONDS))
        self._default_fetch_days = max(1, int(default_fetch_days or 120))
        self._max_fetch_days = max(self._default_fetch_days, int(max_fetch_days or 365))
        self._fetch_buffer_days = max(1, int(fetch_buffer_days or 5))
        self._intraday_start_time = (
            intraday_start_time
            if isinstance(intraday_start_time, dt_time)
            else self.INTRADAY_START_TIME
        )
        self._intraday_end_time = (
            intraday_end_time
            if isinstance(intraday_end_time, dt_time)
            else self.INTRADAY_END_TIME
        )
        if self._intraday_start_time > self._intraday_end_time:
            self._logger.warning(
                "盘中同步时间配置无效（start > end），已回退默认窗口 09:40-14:55"
            )
            self._intraday_start_time = self.INTRADAY_START_TIME
            self._intraday_end_time = self.INTRADAY_END_TIME
        self._holiday_api_url = str(holiday_api_url or self.HOLIDAY_API_URL).strip() or self.HOLIDAY_API_URL
        self._holiday_max_retries = max(1, int(holiday_max_retries or self.HOLIDAY_MAX_RETRIES))
        self._holiday_timeout_seconds = max(
            3,
            int(holiday_timeout_seconds or self.HOLIDAY_TIMEOUT_SECONDS),
        )
        self._daily_task: asyncio.Task | None = None
        self._intraday_task: asyncio.Task | None = None
        self._intraday_task_day: str | None = None
        self._planned_day: str | None = None
        self._non_workday_synced_day: str | None = None
        self._workday_cache: dict[str, bool] = {}
        self._sync_lock = asyncio.Lock()
        self._tz = self._resolve_timezone(timezone_name)

    def ensure_task(self) -> None:
        """确保后台每日净值调度任务已启动。"""
        if self._daily_task and not self._daily_task.done():
            return

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return

        self._daily_task = loop.create_task(self._daily_loop())
        self._logger.info("净值每日调度任务已启动")

    async def stop(self) -> None:
        if self._intraday_task and not self._intraday_task.done():
            self._intraday_task.cancel()
            try:
                await self._intraday_task
            except asyncio.CancelledError:
                pass
            self._intraday_task = None
            self._intraday_task_day = None

        if self._daily_task and not self._daily_task.done():
            self._daily_task.cancel()
            try:
                await self._daily_task
            except asyncio.CancelledError:
                pass
            self._daily_task = None

    def _resolve_timezone(self, timezone_name: str) -> timezone | Any:
        target_name = str(timezone_name or "").strip() or "Asia/Shanghai"
        if ZoneInfo is not None:
            try:
                return ZoneInfo(target_name)
            except Exception:
                try:
                    return ZoneInfo("Asia/Shanghai")
                except Exception:
                    pass
        return timezone(timedelta(hours=8))

    def _now(self) -> datetime:
        return datetime.now(self._tz)

    def _today_text(self) -> str:
        return self._now().date().isoformat()

    @staticmethod
    def _seconds_until_next_day(now_dt: datetime) -> int:
        tomorrow = (now_dt + timedelta(days=1)).date()
        next_midnight = datetime.combine(tomorrow, dt_time.min, tzinfo=now_dt.tzinfo)
        delta = int((next_midnight - now_dt).total_seconds())
        return max(delta, 60)

    @staticmethod
    def _normalize_fund_code_text(value: Any) -> str:
        text = str(value or "").strip()
        if text.isdigit():
            return text.zfill(6)
        return text

    @staticmethod
    def _append_error(stats: dict[str, Any], message: str, limit: int = 20) -> None:
        errors = stats.setdefault("errors", [])
        if len(errors) < limit:
            errors.append(message)

    @staticmethod
    def _is_weekday(date_text: str) -> bool:
        day = datetime.strptime(date_text, "%Y-%m-%d").date()
        return day.weekday() < 5

    async def _fetch_holiday_status(self, date_text: str) -> int:
        timeout = aiohttp.ClientTimeout(total=self._holiday_timeout_seconds)
        async with aiohttp.ClientSession(timeout=timeout, trust_env=False) as session:
            async with session.get(self._holiday_api_url, params={"date": date_text}) as response:
                if response.status != 200:
                    raise RuntimeError(f"holiday API HTTP {response.status}")
                payload = await response.json(content_type=None)
                if not isinstance(payload, dict):
                    raise RuntimeError("holiday API 响应不是 JSON 对象")

        status = payload.get("status")
        try:
            status_int = int(status)
        except (TypeError, ValueError) as e:
            raise RuntimeError(f"holiday API status 非法: {status}") from e

        if status_int not in {0, 1, 2, 3}:
            raise RuntimeError(f"holiday API status 不支持: {status_int}")
        return status_int

    async def _is_workday(self, date_text: str) -> bool:
        cached = self._workday_cache.get(date_text)
        if cached is not None:
            return cached

        last_error = ""
        for attempt in range(1, self._holiday_max_retries + 1):
            try:
                status = await self._fetch_holiday_status(date_text)
                is_workday = status in (0, 2)
                self._workday_cache[date_text] = is_workday
                return is_workday
            except Exception as e:
                last_error = str(e)
                self._logger.warning(
                    f"工作日判定失败（第{attempt}次）: {date_text}, {last_error}"
                )
                if attempt < self._holiday_max_retries:
                    await asyncio.sleep(attempt)

        is_workday = self._is_weekday(date_text)
        self._workday_cache[date_text] = is_workday
        self._logger.warning(
            "holiday API 连续失败，已降级本地规则: "
            f"{date_text}, fallback={'workday' if is_workday else 'holiday'}, last_error={last_error}"
        )
        return is_workday

    async def _sync_for_non_workday(self, date_text: str) -> None:
        if self._non_workday_synced_day == date_text:
            return
        stats = await self.sync_registered_funds_nav(trigger="scheduled:non_workday")
        self._non_workday_synced_day = date_text
        self._logger.info(
            "非工作日净值同步完成: "
            f"基金{stats.get('funds_total', 0)}只, "
            f"成功{stats.get('funds_synced', 0)}只, "
            f"净值写入{stats.get('nav_rows_upserted', 0)}条, "
            f"失败{stats.get('funds_failed', 0)}只"
        )

    def _calc_next_intraday_trigger(self, now_dt: datetime, day_text: str) -> datetime | None:
        day_value = datetime.strptime(day_text, "%Y-%m-%d").date()
        start_dt = datetime.combine(day_value, self._intraday_start_time, tzinfo=self._tz)
        end_dt = datetime.combine(day_value, self._intraday_end_time, tzinfo=self._tz)

        if now_dt <= start_dt:
            return start_dt
        if now_dt > end_dt:
            return None

        elapsed_seconds = (now_dt - start_dt).total_seconds()
        steps = int(elapsed_seconds // self._interval_seconds)
        if elapsed_seconds % self._interval_seconds != 0:
            steps += 1
        next_dt = start_dt + timedelta(seconds=steps * self._interval_seconds)
        if next_dt > end_dt:
            return None
        return next_dt

    async def _start_intraday_task(self, date_text: str) -> None:
        if (
            self._intraday_task
            and not self._intraday_task.done()
            and self._intraday_task_day == date_text
        ):
            return

        if self._intraday_task and not self._intraday_task.done():
            self._intraday_task.cancel()
            try:
                await self._intraday_task
            except asyncio.CancelledError:
                pass

        loop = asyncio.get_running_loop()
        self._intraday_task_day = date_text
        self._intraday_task = loop.create_task(self._intraday_loop(date_text))
        self._logger.info(f"已启动盘中3分钟净值同步任务: {date_text}")

    async def _stop_intraday_task(self) -> None:
        if self._intraday_task and not self._intraday_task.done():
            self._intraday_task.cancel()
            try:
                await self._intraday_task
            except asyncio.CancelledError:
                pass
        self._intraday_task = None
        self._intraday_task_day = None

    async def _intraday_loop(self, date_text: str) -> None:
        try:
            while True:
                now_dt = self._now()
                if now_dt.date().isoformat() != date_text:
                    self._logger.info(f"盘中任务跨日结束: {date_text}")
                    return

                next_trigger = self._calc_next_intraday_trigger(now_dt, date_text)
                if next_trigger is None:
                    self._logger.info(f"盘中任务窗口结束: {date_text}")
                    return

                sleep_seconds = (next_trigger - now_dt).total_seconds()
                if sleep_seconds > 0:
                    await asyncio.sleep(sleep_seconds)

                now_dt = self._now()
                if now_dt.date().isoformat() != date_text:
                    self._logger.info(f"盘中任务跨日结束: {date_text}")
                    return

                try:
                    stats = await self.sync_registered_funds_nav(
                        trigger="scheduled:intraday_3m"
                    )
                    if int(stats.get("funds_total", 0)) > 0:
                        self._logger.info(
                            "盘中3分钟净值同步完成: "
                            f"基金{stats['funds_total']}只, "
                            f"成功{stats['funds_synced']}只, "
                            f"净值写入{stats['nav_rows_upserted']}条, "
                            f"失败{stats['funds_failed']}只"
                        )
                except Exception as e:
                    self._logger.warning(f"盘中3分钟净值同步失败: {e}")
        except asyncio.CancelledError:
            self._logger.info(f"盘中3分钟净值同步任务已停止: {date_text}")
            raise

    async def _plan_today(self) -> None:
        today_text = self._today_text()
        if self._planned_day == today_text:
            return
        self._planned_day = today_text
        self._workday_cache.pop(today_text, None)

        is_workday = await self._is_workday(today_text)
        self._logger.info(
            f"今日工作日判定: {today_text}, is_workday={'yes' if is_workday else 'no'}"
        )

        if is_workday:
            await self._start_intraday_task(today_text)
            return

        await self._stop_intraday_task()
        await self._sync_for_non_workday(today_text)

    async def _daily_loop(self) -> None:
        """每日判定任务：决定是否启动盘中3分钟同步。"""
        try:
            await asyncio.sleep(3)
            while True:
                wait_seconds = self._seconds_until_next_day(self._now())
                try:
                    await self._plan_today()
                    wait_seconds = self._seconds_until_next_day(self._now())
                except Exception as e:
                    self._logger.warning(f"每日净值调度执行失败: {e}")
                    self._planned_day = None
                    wait_seconds = 60
                await asyncio.sleep(wait_seconds)
        except asyncio.CancelledError:
            self._logger.info("净值每日调度任务已停止")
            raise

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

    def _calc_nav_fetch_days(self, latest_nav_date: str | None, force_full: bool = False) -> int:
        if force_full or not latest_nav_date:
            return self._default_fetch_days

        latest_date_text = self._normalize_nav_date_text(latest_nav_date)
        if not latest_date_text:
            return self._default_fetch_days

        latest_day = datetime.strptime(latest_date_text, "%Y-%m-%d").date()
        delta_days = (self._now().date() - latest_day).days
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
        fund_code: str = "",
        stats: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        nav_map: dict[str, dict[str, Any]] = {}
        latest_date_text = self._normalize_nav_date_text(latest_nav_date)

        for item in history or []:
            nav_date = self._normalize_nav_date_text(item.get("date"))
            if not nav_date:
                if stats is not None:
                    stats["invalid_rows_skipped"] = int(stats.get("invalid_rows_skipped", 0)) + 1
                    self._append_error(stats, f"{fund_code} 跳过无效日期记录")
                continue
            if latest_date_text and nav_date <= latest_date_text:
                continue

            try:
                unit_nav = float(item.get("close"))
            except (TypeError, ValueError):
                if stats is not None:
                    stats["invalid_rows_skipped"] = int(stats.get("invalid_rows_skipped", 0)) + 1
                    self._append_error(stats, f"{fund_code} 跳过无效净值记录: {nav_date}")
                continue
            if unit_nav <= 0:
                if stats is not None:
                    stats["invalid_rows_skipped"] = int(stats.get("invalid_rows_skipped", 0)) + 1
                    self._append_error(stats, f"{fund_code} 跳过非正净值记录: {nav_date}")
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

    @staticmethod
    def _filter_funds_by_codes(
        funds: list[dict[str, Any]],
        fund_codes: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        if not fund_codes:
            return funds

        code_set = {
            str(code).strip().zfill(6)
            for code in fund_codes
            if str(code).strip()
        }
        return [
            fund
            for fund in funds
            if str(fund.get("fund_code", "")).strip().zfill(6) in code_set
        ]

    async def _sync_funds_nav(
        self,
        funds: list[dict[str, Any]],
        force_full: bool = False,
        trigger: str = "manual",
    ) -> dict[str, Any]:
        async with self._sync_lock:
            stats: dict[str, Any] = {
                "funds_total": len(funds),
                "funds_synced": 0,
                "funds_no_new_data": 0,
                "funds_failed": 0,
                "nav_rows_upserted": 0,
                "invalid_rows_skipped": 0,
                "errors": [],
            }

            for fund in funds:
                fund_code = self._normalize_fund_code_text(fund.get("fund_code"))
                fund_name = str(fund.get("fund_name", "")).strip()
                if not fund_code or not fund_code.isdigit() or len(fund_code) != 6:
                    stats["funds_failed"] += 1
                    self._append_error(stats, f"{fund_code or 'unknown'} 基金代码无效")
                    continue

                latest_nav_date = None
                if not force_full:
                    latest_nav_date = self._data_handler.get_latest_nav_date(fund_code)

                fetch_days = self._calc_nav_fetch_days(latest_nav_date, force_full=force_full)

                try:
                    history = await self._analyzer.get_lof_history(fund_code, days=fetch_days)
                    if not history:
                        stats["funds_failed"] += 1
                        self._append_error(stats, f"{fund_code} 无历史数据")
                        continue

                    nav_records = self._build_nav_records_from_history(
                        history=history,
                        latest_nav_date=None if force_full else latest_nav_date,
                        fund_code=fund_code,
                        stats=stats,
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
                    self._append_error(stats, f"{fund_code} {str(e)}")

            return stats

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
        funds = self._filter_funds_by_codes(
            funds=self._data_handler.list_position_funds(),
            fund_codes=fund_codes,
        )
        return await self._sync_funds_nav(
            funds=funds,
            force_full=force_full,
            trigger=trigger,
        )

    async def sync_registered_funds_nav(
        self,
        fund_codes: list[str] | None = None,
        force_full: bool = False,
        trigger: str = "scheduled",
    ) -> dict[str, Any]:
        """同步已注册基金净值到本地库（增量）。"""
        funds = self._filter_funds_by_codes(
            funds=self._data_handler.list_funds(),
            fund_codes=fund_codes,
        )
        return await self._sync_funds_nav(
            funds=funds,
            force_full=force_full,
            trigger=trigger,
        )
