from datetime import datetime
from typing import Any


def ssgz_usage_text() -> str:
    return (
        "❌ 请输入基金代码\n"
        "💡 用法: ssgz <基金代码>\n"
        "💡 示例: ssgz 001632"
    )


def ssgz_invalid_code_text(raw_code: str) -> str:
    return (
        f"❌ 基金代码格式错误: {raw_code}\n"
        "💡 请使用 6 位数字代码，例如: ssgz 001632"
    )


def ssgz_not_found_text(fund_code: str) -> str:
    return (
        f"❌ 未获取到基金 {fund_code} 的实时估值\n"
        "💡 该接口主要支持场外基金估值数据\n"
        "💡 建议使用「搜索基金 关键词」先确认基金代码"
    )


def format_fund_info(info: Any) -> str:
    if float(getattr(info, "latest_price", 0) or 0) == 0:
        return f"""
📊 【{info.name}】
━━━━━━━━━━━━━━━━━
⚠️ 暂无实时行情数据
━━━━━━━━━━━━━━━━━
🔢 基金代码: {info.code}
💡 可能原因: 停牌/休市/数据源未更新
⏰ 查询时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
""".strip()

    change_rate = float(getattr(info, "change_rate", 0) or 0)
    change_color = "🔴" if change_rate < 0 else "🟢" if change_rate > 0 else "⚪"

    return f"""
📊 【{info.name}】实时行情 {info.trend_emoji}
━━━━━━━━━━━━━━━━━
💰 最新价: {float(info.latest_price):.4f}
{change_color} 涨跌额: {float(info.change_amount):+.4f}
{change_color} 涨跌幅: {change_rate:+.2f}%
━━━━━━━━━━━━━━━━━
📈 今开: {float(info.open_price):.4f}
📊 最高: {float(info.high_price):.4f}
📉 最低: {float(info.low_price):.4f}
📋 昨收: {float(info.prev_close):.4f}
━━━━━━━━━━━━━━━━━
📦 成交量: {float(info.volume):,.0f}
💵 成交额: {float(info.amount):,.2f}
🔄 换手率: {float(info.turnover_rate):.2f}%
━━━━━━━━━━━━━━━━━
🔢 基金代码: {info.code}
⏰ 更新时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
""".strip()


def format_ssgz_fallback_text(fund_code: str, realtime: Any) -> str:
    return (
        f"⚠️ 基金 {fund_code} 暂无场外估值数据，返回场内实时行情：\n\n"
        f"{format_fund_info(realtime)}"
    )


def format_realtime_valuation(valuation: dict[str, Any]) -> str:
    def safe_float(value: Any, default: float = 0.0) -> float:
        if value is None:
            return default
        try:
            return float(value)
        except (ValueError, TypeError):
            return default

    code = str(valuation.get("code", "")).strip()
    name = str(valuation.get("name", "")).strip() or "未知基金"
    estimate_value = safe_float(
        valuation.get("estimate_value", valuation.get("latest_price"))
    )
    unit_value = safe_float(valuation.get("unit_value", valuation.get("prev_close")))
    change_rate = safe_float(valuation.get("change_rate"))
    change_amount = safe_float(
        valuation.get("change_amount", estimate_value - unit_value)
    )
    update_time = str(valuation.get("update_time", "")).strip() or "--"
    valuation_date = str(valuation.get("valuation_date", "")).strip() or "--"

    change_color = "🔴" if change_rate < 0 else "🟢" if change_rate > 0 else "⚪"
    trend = "📈" if change_rate > 0 else "📉" if change_rate < 0 else "➡️"

    return f"""
📍 【{name}】实时估值 {trend}
━━━━━━━━━━━━━━━━━
💰 估算净值: {estimate_value:.4f}
📋 单位净值: {unit_value:.4f}
{change_color} 估算涨跌额: {change_amount:+.4f}
{change_color} 估算涨跌幅: {change_rate:+.2f}%
━━━━━━━━━━━━━━━━━
🔢 基金代码: {code}
🕐 估值时间: {update_time}
📅 净值日期: {valuation_date}
⏰ 查询时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
💡 数据来源: 天天基金估值接口（盘中为估算值）
""".strip()


def format_analysis(info: Any, indicators: dict[str, Any]) -> str:
    if not indicators:
        return "📊 暂无足够数据进行技术分析"

    trend_emoji = {
        "强势上涨": "🚀",
        "上涨趋势": "📈",
        "强势下跌": "💥",
        "下跌趋势": "📉",
        "震荡": "↔️",
    }.get(indicators.get("trend", "震荡"), "❓")

    ma_status = []
    current = indicators.get("current_price", 0)
    if indicators.get("ma5"):
        status = "上" if current > indicators["ma5"] else "下"
        ma_status.append(f"MA5({indicators['ma5']:.4f}){status}")
    if indicators.get("ma10"):
        status = "上" if current > indicators["ma10"] else "下"
        ma_status.append(f"MA10({indicators['ma10']:.4f}){status}")
    if indicators.get("ma20"):
        status = "上" if current > indicators["ma20"] else "下"
        ma_status.append(f"MA20({indicators['ma20']:.4f}){status}")

    return f"""
📈 【{info.name}】技术分析
━━━━━━━━━━━━━━━━━
{trend_emoji} 趋势判断: {indicators.get("trend", "未知")}
━━━━━━━━━━━━━━━━━
📊 均线分析:
  • {" | ".join(ma_status) if ma_status else "数据不足"}
━━━━━━━━━━━━━━━━━
📈 区间收益率:
  • 5日收益: {indicators.get("return_5d", "--"):+.2f}%
  • 10日收益: {indicators.get("return_10d", "--"):+.2f}%
  • 20日收益: {indicators.get("return_20d", "--"):+.2f}%
━━━━━━━━━━━━━━━━━
📉 波动分析:
  • 20日波动率: {indicators.get("volatility", "--"):.4f}
  • 20日最高: {indicators.get("high_20d", "--"):.4f}
  • 20日最低: {indicators.get("low_20d", "--"):.4f}
━━━━━━━━━━━━━━━━━
💡 投资建议: 请结合自身风险承受能力谨慎投资
""".strip()


def format_stock_info(info: Any) -> str:
    if float(getattr(info, "latest_price", 0) or 0) == 0:
        return f"""
📊 【{info.name}】
━━━━━━━━━━━━━━━━━
⚠️ 暂无实时行情数据
━━━━━━━━━━━━━━━━━
🔢 股票代码: {info.code}
💡 可能原因: 停牌/休市/数据源未更新
⏰ 查询时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
""".strip()

    change_rate = float(getattr(info, "change_rate", 0) or 0)
    change_color = "🔴" if change_rate < 0 else "🟢" if change_rate > 0 else "⚪"

    def format_market_cap(value: float) -> str:
        if value >= 100000000:
            return f"{value / 100000000:.2f}亿"
        if value >= 10000:
            return f"{value / 10000:.2f}万"
        return f"{value:.2f}"

    return f"""
📊 【{info.name}】实时行情 {info.trend_emoji}
━━━━━━━━━━━━━━━━━
💰 最新价: {float(info.latest_price):.2f}
{change_color} 涨跌额: {float(info.change_amount):+.2f}
{change_color} 涨跌幅: {change_rate:+.2f}%
📏 振幅: {float(info.amplitude):.2f}%
━━━━━━━━━━━━━━━━━
📈 今开: {float(info.open_price):.2f}
📊 最高: {float(info.high_price):.2f}
📉 最低: {float(info.low_price):.2f}
📋 昨收: {float(info.prev_close):.2f}
━━━━━━━━━━━━━━━━━
📦 成交量: {float(info.volume):,.0f}手
💵 成交额: {format_market_cap(float(info.amount))}
🔄 换手率: {float(info.turnover_rate):.2f}%
━━━━━━━━━━━━━━━━━
📈 市盈率(动态): {float(info.pe_ratio):.2f}
📊 市净率: {float(info.pb_ratio):.2f}
💰 总市值: {format_market_cap(float(info.total_market_cap))}
💎 流通市值: {format_market_cap(float(info.circulating_market_cap))}
━━━━━━━━━━━━━━━━━
🔢 股票代码: {info.code}
⏰ 更新时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
💡 数据缓存10分钟，仅供参考
""".strip()


def format_precious_metal_prices(prices: dict[str, Any]) -> str:
    if not prices:
        return "❌ 获取贵金属价格失败，请稍后重试"

    def parse_change_rate(rate_str: str) -> float:
        try:
            return float(rate_str.replace("%", "").replace("+", ""))
        except (ValueError, AttributeError):
            return 0.0

    def format_item(data: dict[str, Any], unit: str = "美元/盎司", divisor: float = 1.0) -> str:
        if not data:
            return "  暂无数据"

        change_rate = parse_change_rate(data.get("change_rate", "0%"))
        change_emoji = "🔴" if change_rate < 0 else "🟢" if change_rate > 0 else "⚪"
        trend_emoji = "📈" if change_rate > 0 else "📉" if change_rate < 0 else "➡️"

        price = float(data["price"]) / divisor
        change = float(data.get("change", 0)) / divisor
        open_p = float(data.get("open", 0)) / divisor
        high_p = float(data.get("high", 0)) / divisor
        low_p = float(data.get("low", 0)) / divisor
        buy_p = float(data.get("buy_price", 0)) / divisor
        sell_p = float(data.get("sell_price", 0)) / divisor

        return f"""  {trend_emoji} 最新价: {price:.2f} {unit}
  {change_emoji} 涨跌: {change:+.2f} ({data.get("change_rate", "0%")})
  📊 今开: {open_p:.2f} | 最高: {high_p:.2f} | 最低: {low_p:.2f}
  💹 买入: {buy_p:.2f} | 卖出: {sell_p:.2f}"""

    lines = [
        "💰 今日贵金属行情（国际现货）",
        "━━━━━━━━━━━━━━━━━",
    ]

    if "au_td" in prices:
        lines.append("🥇 黄金")
        lines.append(format_item(prices["au_td"], "美元/盎司", 1.0))
        if prices["au_td"].get("update_time"):
            lines.append(f"  🕐 更新: {prices['au_td']['update_time']}")
        lines.append("")

    if "ag_td" in prices:
        lines.append("🥈 白银")
        silver_price = prices["ag_td"].get("price", 0)
        divisor = 100.0 if float(silver_price or 0) > 1000 else 1.0
        lines.append(format_item(prices["ag_td"], "美元/盎司", divisor))
        if prices["ag_td"].get("update_time"):
            lines.append(f"  🕐 更新: {prices['ag_td']['update_time']}")
        lines.append("")

    lines.append("━━━━━━━━━━━━━━━━━")
    lines.append("📌 国际现货24小时交易")
    lines.append("💡 数据来源: NowAPI | 缓存15分钟")

    return "\n".join(lines)

