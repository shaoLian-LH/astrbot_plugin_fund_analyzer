import asyncio
import re
from decimal import Decimal, ROUND_HALF_EVEN
from typing import Any, Callable


class PositionService:
    """持仓领域服务：参数解析、用户归属解析、基金并发查询。"""

    def __init__(
        self,
        normalize_fund_code: Callable[[str | int | None], str | None],
        logger: Any,
    ):
        self._normalize_fund_code = normalize_fund_code
        self._logger = logger

    def extract_command_payload(self, event: Any, command_name: str) -> str:
        """获取命令后面的原始参数文本。"""
        message_text = str(getattr(event, "message_str", "") or "").strip()
        if not message_text:
            message_obj = getattr(event, "message_obj", None)
            message_text = str(
                getattr(message_obj, "message_str", "")
                or getattr(message_obj, "text", "")
                or ""
            ).strip()
        if not message_text:
            return ""

        slash_command = f"/{command_name}"
        if message_text.startswith(slash_command):
            return message_text[len(slash_command) :].strip()
        if message_text.startswith(command_name):
            return message_text[len(command_name) :].strip()
        return message_text

    def resolve_position_owner(self, event: Any) -> tuple[str, str]:
        """
        解析持仓归属用户：
        优先使用 raw_message 的 pingtai + id，失败时回退到 unified_msg_origin + sender_id。
        """
        raw_platform = ""
        raw_user_id = ""

        message_obj = getattr(event, "message_obj", None)
        raw_message = getattr(message_obj, "raw_message", None) if message_obj else None

        if isinstance(raw_message, dict):
            raw_platform = str(
                raw_message.get("pingtai") or raw_message.get("platform") or ""
            ).strip()
            raw_user_id = str(
                raw_message.get("id") or raw_message.get("user_id") or ""
            ).strip()
        elif raw_message is not None:
            raw_platform = str(
                getattr(raw_message, "pingtai", "")
                or getattr(raw_message, "platform", "")
                or ""
            ).strip()
            raw_user_id = str(
                getattr(raw_message, "id", "")
                or getattr(raw_message, "user_id", "")
                or ""
            ).strip()

        unified_origin = str(getattr(event, "unified_msg_origin", "") or "").strip()
        origin_platform = unified_origin.split(":", 1)[0].strip() if unified_origin else ""

        platform = raw_platform or origin_platform or "unknown"
        sender_id = str(event.get_sender_id() or "").strip()
        user_id = raw_user_id or sender_id
        return platform, user_id

    @staticmethod
    def fund_position_usage_text() -> str:
        return (
            "❌ 持仓格式错误\n"
            "💡 用法: 增加基金持仓 {基金代码,平均成本,持有份额}\n"
            "💡 批量: 增加基金持仓 {161226,1.0234,1200} {001632,2.1456,500}"
        )

    @staticmethod
    def clear_position_usage_text() -> str:
        return (
            "❌ 清仓参数格式错误\n"
            "💡 用法1: 清仓基金（默认按当前默认基金全仓卖出）\n"
            "💡 用法2: 清仓基金 基金代码\n"
            "💡 用法3: 清仓基金 [基金代码] [份额|百分比]\n"
            "💡 示例: 清仓基金 161226 500\n"
            "💡 示例: 清仓基金 161226 25%\n"
            "💡 示例: 清仓基金 50%"
        )

    @staticmethod
    def parse_positive_float(value: str) -> float | None:
        try:
            number = float(str(value).strip())
        except (TypeError, ValueError):
            return None
        if number <= 0:
            return None
        return number

    def parse_position_records(
        self, payload: str
    ) -> tuple[list[dict[str, Any]], str | None]:
        text = str(payload or "").strip()
        if not text:
            return [], self.fund_position_usage_text()

        block_matches = re.findall(r"\{([^{}]+)\}", text)
        if block_matches:
            raw_items = [item.strip() for item in block_matches if item.strip()]
        else:
            raw_items = [item.strip() for item in re.split(r"[;；]", text) if item.strip()]

        if not raw_items:
            return [], self.fund_position_usage_text()

        records: list[dict[str, Any]] = []
        for raw_item in raw_items:
            parts = [
                part.strip().strip("<>").strip()
                for part in re.split(r"[,，]", raw_item)
                if part.strip()
            ]
            if len(parts) != 3:
                return [], (
                    f"❌ 持仓格式错误: {raw_item}\n"
                    "💡 正确格式: {基金代码,平均成本,持有份额}"
                )

            fund_code = self._normalize_fund_code(parts[0])
            if not fund_code:
                return [], f"❌ 基金代码格式错误: {parts[0]}（需为 6 位数字）"

            avg_cost = self.parse_positive_float(parts[1])
            if avg_cost is None:
                return [], f"❌ 平均成本必须是大于 0 的数字: {parts[1]}"

            shares = self.parse_positive_float(parts[2])
            if shares is None:
                return [], f"❌ 持有份额必须是大于 0 的数字: {parts[2]}"

            records.append(
                {
                    "fund_code": fund_code,
                    "avg_cost": avg_cost,
                    "shares": shares,
                }
            )

        return records, None

    @staticmethod
    def _bankers_round(value: float, digits: int = 4) -> float:
        quantizer = Decimal("1").scaleb(-digits)
        return float(
            Decimal(str(value)).quantize(quantizer, rounding=ROUND_HALF_EVEN)
        )

    def parse_clear_payload(self, payload: str) -> tuple[dict[str, Any] | None, str | None]:
        text = str(payload or "").strip()
        if not text:
            return {
                "fund_code": None,
                "share_mode": "all",
                "share_value": None,
                "share_raw": "",
            }, None

        tokens = [item for item in re.split(r"\s+", text) if item]
        if len(tokens) > 2:
            return None, self.clear_position_usage_text()

        def parse_fund_code_token(code_text: str) -> str | None:
            text_value = str(code_text or "").strip()
            if not re.fullmatch(r"\d{6}", text_value):
                return None
            return self._normalize_fund_code(text_value)

        def parse_share_token(share_text: str) -> tuple[dict[str, Any] | None, str | None]:
            raw_text = str(share_text or "").strip()
            if not raw_text:
                return None, "❌ 卖出份额不能为空"

            if raw_text.endswith("%"):
                percent_text = raw_text[:-1].strip()
                percent = self.parse_positive_float(percent_text)
                if percent is None:
                    return None, f"❌ 百分比格式错误: {raw_text}"
                if percent > 100:
                    return None, "❌ 百分比不能超过 100%"
                return {
                    "share_mode": "percent",
                    "share_value": percent,
                    "share_raw": raw_text,
                }, None

            shares = self.parse_positive_float(raw_text)
            if shares is None:
                return None, f"❌ 卖出份额必须是大于 0 的数字: {raw_text}"
            return {
                "share_mode": "shares",
                "share_value": shares,
                "share_raw": raw_text,
            }, None

        if len(tokens) == 1:
            single = tokens[0]
            maybe_code = parse_fund_code_token(single)
            if maybe_code:
                return {
                    "fund_code": maybe_code,
                    "share_mode": "all",
                    "share_value": None,
                    "share_raw": "",
                }, None

            share_part, error = parse_share_token(single)
            if error:
                return None, error
            return {
                "fund_code": None,
                **(share_part or {}),
            }, None

        fund_code = parse_fund_code_token(tokens[0])
        if not fund_code:
            return None, f"❌ 基金代码格式错误: {tokens[0]}（需为 6 位数字）"

        share_part, error = parse_share_token(tokens[1])
        if error:
            return None, error
        return {
            "fund_code": fund_code,
            **(share_part or {}),
        }, None

    def resolve_sell_shares(
        self,
        holding_shares: float,
        clear_payload: dict[str, Any],
        percent_round_digits: int = 4,
    ) -> tuple[float | None, str | None]:
        if holding_shares <= 0:
            return None, "❌ 当前持仓份额为 0，无法清仓"

        mode = str(clear_payload.get("share_mode") or "all").strip().lower()
        value = clear_payload.get("share_value")

        if mode == "all":
            return float(holding_shares), None

        if mode == "shares":
            shares = float(value or 0)
            if shares <= 0:
                return None, "❌ 卖出份额必须大于 0"
            if shares > holding_shares + 1e-8:
                return (
                    None,
                    f"❌ 卖出份额不能超过当前持仓（当前: {holding_shares:,.4f}）",
                )
            return shares, None

        if mode == "percent":
            percent = float(value or 0)
            if percent <= 0 or percent > 100:
                return None, "❌ 百分比必须在 (0, 100] 范围内"
            raw_shares = holding_shares * percent / 100
            shares = self._bankers_round(raw_shares, digits=percent_round_digits)
            if shares <= 0:
                return None, "❌ 百分比过小，按银行家舍入后卖出份额为 0"
            if shares > holding_shares:
                shares = float(holding_shares)
            return shares, None

        return None, "❌ 未知的清仓参数类型"

    async def batch_fetch_fund_infos(
        self, analyzer: Any, fund_codes: list[str], max_concurrency: int = 6
    ) -> dict[str, Any]:
        unique_codes: list[str] = []
        seen = set()
        for code in fund_codes:
            code_str = str(code).strip()
            if not code_str or code_str in seen:
                continue
            seen.add(code_str)
            unique_codes.append(code_str)

        if not unique_codes:
            return {}

        semaphore = asyncio.Semaphore(max(1, min(max_concurrency, 20)))

        async def fetch_one(code: str) -> tuple[str, Any]:
            async with semaphore:
                try:
                    return code, await analyzer.get_lof_realtime(code)
                except Exception as e:
                    self._logger.debug(f"获取基金 {code} 行情失败: {e}")
                    return code, None

        tasks = [asyncio.create_task(fetch_one(code)) for code in unique_codes]
        raw_results = await asyncio.gather(*tasks, return_exceptions=True)

        results: dict[str, Any] = {}
        for item in raw_results:
            if isinstance(item, Exception):
                continue
            code, info = item
            if info:
                results[code] = info
        return results
