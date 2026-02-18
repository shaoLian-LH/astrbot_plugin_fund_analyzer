from datetime import datetime
from typing import Any


def format_position_add_result(
    saved_records: list[dict[str, Any]],
    fund_infos: dict[str, Any],
) -> str:
    lines = [f"✅ 已记录 {len(saved_records)} 条基金持仓", "━━━━━━━━━━━━━━━━━"]
    for index, record in enumerate(saved_records, start=1):
        code = str(record.get("fund_code", "")).strip()
        avg_cost = float(record.get("avg_cost", 0) or 0)
        shares = float(record.get("shares", 0) or 0)
        info = fund_infos.get(code)
        name = (
            info.name
            if info and getattr(info, "name", "")
            else str(record.get("fund_name") or "").strip() or "未知基金"
        )
        lines.append(f"{index}. {name} ({code})")
        lines.append(f"   💰 平均成本: {avg_cost:.4f}")
        lines.append(f"   📦 持有份额: {shares:,.4f}")

    lines.append("━━━━━━━━━━━━━━━━━")
    lines.append("💡 发送 ckcc 查看持仓收益")
    return "\n".join(lines)


def format_position_overview(
    positions: list[dict[str, Any]],
    fund_infos: dict[str, Any],
) -> str:
    if not positions:
        return "📭 当前没有基金持仓记录"

    lines = ["💼 当前基金持仓", "━━━━━━━━━━━━━━━━━"]
    total_cost = 0.0
    total_market = 0.0
    missing_quotes = 0

    for index, position in enumerate(positions, start=1):
        code = str(position.get("fund_code", "")).strip()
        avg_cost = float(position.get("avg_cost", 0) or 0)
        shares = float(position.get("shares", 0) or 0)
        if shares <= 0:
            continue

        info = fund_infos.get(code)
        name = (
            info.name
            if info and getattr(info, "name", "")
            else str(position.get("fund_name") or "").strip() or "未知基金"
        )

        cost_amount = avg_cost * shares
        if info and float(getattr(info, "latest_price", 0) or 0) > 0:
            latest_price = float(info.latest_price)
        else:
            latest_price = avg_cost
            missing_quotes += 1

        market_value = latest_price * shares
        profit = market_value - cost_amount
        profit_rate = (profit / cost_amount * 100) if cost_amount > 0 else 0.0
        profit_emoji = "🟢" if profit > 0 else "🔴" if profit < 0 else "⚪"

        lines.append(f"{index}. {name} ({code})")
        lines.append(f"   📦 份额: {shares:,.4f}")
        lines.append(f"   💰 成本价: {avg_cost:.4f} | 现价: {latest_price:.4f}")
        lines.append(f"   💵 成本: {cost_amount:,.2f} | 市值: {market_value:,.2f}")
        lines.append(f"   {profit_emoji} 收益: {profit:+,.2f} ({profit_rate:+.2f}%)")
        lines.append("━━━━━━━━━━━━━━━━━")

        total_cost += cost_amount
        total_market += market_value

    total_profit = total_market - total_cost
    total_profit_rate = (total_profit / total_cost * 100) if total_cost > 0 else 0.0
    total_emoji = "🟢" if total_profit > 0 else "🔴" if total_profit < 0 else "⚪"

    lines.append("📊 汇总")
    lines.append(f"💰 总成本: {total_cost:,.2f}")
    lines.append(f"💵 总市值: {total_market:,.2f}")
    lines.append(f"{total_emoji} 总收益: {total_profit:+,.2f} ({total_profit_rate:+.2f}%)")
    lines.append(f"⏰ 统计时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if missing_quotes > 0:
        lines.append(f"⚠️ {missing_quotes} 只基金未获取到实时价格，已按成本价估算该部分市值")

    return "\n".join(lines)


def format_nav_sync_result(stats: dict[str, Any], title: str) -> str:
    lines = [title, "━━━━━━━━━━━━━━━━━"]
    lines.append(f"📌 目标基金数: {int(stats.get('funds_total', 0))}")
    lines.append(f"✅ 成功同步: {int(stats.get('funds_synced', 0))}")
    lines.append(f"🆕 净值写入/更新: {int(stats.get('nav_rows_upserted', 0))} 条")
    lines.append(f"⏭️ 无新增数据: {int(stats.get('funds_no_new_data', 0))}")
    lines.append(f"❌ 同步失败: {int(stats.get('funds_failed', 0))}")

    errors = stats.get("errors") or []
    if errors:
        lines.append("━━━━━━━━━━━━━━━━━")
        lines.append("⚠️ 失败详情（最多3条）:")
        for item in errors[:3]:
            lines.append(f"• {item}")

    lines.append("━━━━━━━━━━━━━━━━━")
    lines.append(f"⏰ 完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    return "\n".join(lines)

