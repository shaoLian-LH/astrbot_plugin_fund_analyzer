import asyncio
import re
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

