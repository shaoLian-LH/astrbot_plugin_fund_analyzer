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
    lines.append(f"🏦 总持仓金额: {total_market:,.2f}")
    lines.append(f"💵 总市值: {total_market:,.2f}")
    lines.append(f"{total_emoji} 总收益: {total_profit:+,.2f} ({total_profit_rate:+.2f}%)")
    lines.append(f"⏰ 统计时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if missing_quotes > 0:
        lines.append(f"⚠️ {missing_quotes} 只基金未获取到实时价格，已按成本价估算该部分市值")

    return "\n".join(lines)


def format_clear_position_result(result: dict[str, Any]) -> str:
    fund_name = str(result.get("fund_name") or "").strip() or "未知基金"
    fund_code = str(result.get("fund_code") or "").strip()
    action = str(result.get("action") or "sell").strip().lower()
    action_text = "清仓" if action == "clear" else "卖出"
    shares_before = float(result.get("shares_before", 0) or 0)
    shares_sold = float(result.get("shares_sold", 0) or 0)
    shares_after = float(result.get("shares_after", 0) or 0)
    avg_cost = float(result.get("avg_cost", 0) or 0)
    settlement_nav = result.get("settlement_nav")
    settlement_nav_date = str(result.get("settlement_nav_date") or "").strip()
    expected_settlement_date = str(result.get("expected_settlement_date") or "").strip()
    settlement_rule = str(result.get("settlement_rule") or "").strip()
    profit_amount = result.get("profit_amount")
    request_text = str(result.get("requested_text") or "").strip()

    lines = [f"✅ {action_text}基金成功", "━━━━━━━━━━━━━━━━━"]
    lines.append(f"📌 基金: {fund_name} ({fund_code})")
    lines.append(f"📦 卖出份额: {shares_sold:,.4f}")
    lines.append(f"📦 卖出前份额: {shares_before:,.4f}")
    lines.append(f"📦 卖出后份额: {shares_after:,.4f}")
    if request_text:
        lines.append(f"🧾 指令参数: {request_text}")
    lines.append(f"💰 持仓成本价: {avg_cost:.4f}")

    if settlement_nav is not None:
        lines.append(f"💵 结算净值: {float(settlement_nav):.4f}")
    else:
        lines.append("💵 结算净值: 暂无（按成本价估算）")

    if settlement_nav_date:
        lines.append(f"📅 结算净值日期: {settlement_nav_date}")
    elif expected_settlement_date:
        lines.append(f"📅 预计结算日期: {expected_settlement_date}")

    if settlement_rule:
        lines.append(f"📐 结算规则: {settlement_rule}")

    if profit_amount is not None:
        profit = float(profit_amount)
        profit_emoji = "🟢" if profit > 0 else "🔴" if profit < 0 else "⚪"
        lines.append(f"{profit_emoji} 本次收益: {profit:+,.2f}")

    lines.append(f"⏰ 记录时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    return "\n".join(lines)


def format_clear_history(logs: list[dict[str, Any]]) -> str:
    if not logs:
        return "📭 暂无清仓/卖出历史记录"

    lines = ["🧾 清仓/卖出历史", "━━━━━━━━━━━━━━━━━"]
    for index, item in enumerate(logs, start=1):
        action = str(item.get("action") or "").strip().lower()
        action_text = "清仓" if action == "clear" else "卖出"
        fund_name = str(item.get("fund_name") or "").strip() or "未知基金"
        fund_code = str(item.get("fund_code") or "").strip()
        shares_delta = float(item.get("shares_delta", 0) or 0)
        shares_sold = abs(shares_delta)
        shares_after = float(item.get("shares_after", 0) or 0)
        settlement_nav = item.get("settlement_nav")
        settlement_nav_date = str(item.get("settlement_nav_date") or "").strip()
        expected_settlement_date = str(item.get("expected_settlement_date") or "").strip()
        profit_amount = item.get("profit_amount")
        rule_text = str(item.get("settlement_rule") or "").strip()
        note = str(item.get("note") or "").strip()
        created_at = int(item.get("created_at", 0) or 0)
        created_text = (
            datetime.fromtimestamp(created_at).strftime("%Y-%m-%d %H:%M:%S")
            if created_at > 0
            else "--"
        )

        lines.append(f"{index}. {action_text} | {fund_name} ({fund_code})")
        lines.append(f"   📦 份额: {shares_sold:,.4f} | 剩余: {shares_after:,.4f}")
        if settlement_nav is not None:
            lines.append(f"   💵 结算净值: {float(settlement_nav):.4f}")
        if settlement_nav_date:
            lines.append(f"   📅 结算日期: {settlement_nav_date}")
        elif expected_settlement_date:
            lines.append(f"   📅 预计结算: {expected_settlement_date}")
        if profit_amount is not None:
            lines.append(f"   📊 收益: {float(profit_amount):+,.2f}")
        if rule_text:
            lines.append(f"   📐 规则: {rule_text}")
        if note:
            lines.append(f"   📝 备注: {note}")
        lines.append(f"   ⏰ 时间: {created_text}")
        lines.append("━━━━━━━━━━━━━━━━━")

    lines.append("💡 使用: 清仓基金 [基金代码] [份额|百分比]")
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
