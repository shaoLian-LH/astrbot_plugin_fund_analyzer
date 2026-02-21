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
        return "❌ 获取贵金属行情失败，请稍后重试"

    comex = prices.get("comex_gold") or {}
    domestic = prices.get("domestic_gold") or {}
    fx = prices.get("exchange_rate") or {}

    def parse_change_rate(rate_str: str) -> float:
        try:
            return float(str(rate_str).replace("%", "").replace("+", "").strip())
        except (ValueError, TypeError):
            return 0.0

    def format_number(value: Any, fallback: str = "-") -> str:
        try:
            num = float(value)
            if num == 0:
                return fallback
            return f"{num:.2f}"
        except (TypeError, ValueError):
            return fallback

    comex_price = float(comex.get("price", 0) or 0)
    change_rate_text = str(comex.get("change_rate", "0%") or "0%")
    change_rate_value = parse_change_rate(change_rate_text)
    trend_emoji = "📈" if change_rate_value > 0 else "📉" if change_rate_value < 0 else "➡️"

    lines = [
        "💰 贵金属行情（黄金）",
        "━━━━━━━━━━━━━━━━━",
        "🇨🇳 国内金价（元/克）",
    ]

    if domestic:
        lines.append(f"  💴 最新: {float(domestic.get('price_cny_per_gram', 0)):.2f} 元/克")
        lines.append(
            "  🧮 公式: "
            f"{domestic.get('formula', 'COMEX黄金价格 * 美元兑人民币汇率 / 31.1035')}"
        )
        lines.append(
            "  📌 基础值: "
            f"{float(domestic.get('base_price_usd_per_ounce', 0)):.2f} 美元/盎司 × "
            f"{float(domestic.get('usd_cny_rate', 0)):.4f}"
        )
    else:
        lines.append("  ⚠️ 暂无法完成人民币换算（缺少当日美元兑人民币汇率）")
        hint = str(prices.get("rate_missing_hint", "")).strip()
        if hint:
            lines.append(f"  💡 {hint}")

    lines.extend(
        [
            "",
            "🌍 COMEX黄金（美元/盎司）",
            f"  {trend_emoji} 最新: {comex_price:.2f}" if comex_price > 0 else "  📌 最新: -",
            f"  📊 涨跌: {format_number(comex.get('change', 0), '-')} ({change_rate_text})",
            (
                "  📈 今开: "
                f"{format_number(comex.get('open', 0), '-')} | "
                f"最高: {format_number(comex.get('high', 0), '-')} | "
                f"最低: {format_number(comex.get('low', 0), '-')}"
            ),
            f"  📋 昨结: {format_number(comex.get('prev_close', 0), '-')}",
            (
                "  📦 成交量: "
                f"{comex.get('volume_text', '-')} | 持仓量: {comex.get('position_text', '-')}"
            ),
            (
                "  🔄 外盘: "
                f"{comex.get('outer_text', '-')} | 内盘: {comex.get('inner_text', '-')}"
            ),
            (
                "  🧾 仓差: "
                f"{comex.get('spread_text', '-')} | 日增: {comex.get('day_increment_text', '-')}"
            ),
        ]
    )

    update_time = str(comex.get("update_time", "")).strip()
    if update_time:
        lines.append(f"  🕐 行情时间: {update_time}")
    elif comex.get("fetched_at"):
        lines.append(f"  🕐 抓取时间: {comex.get('fetched_at')}")

    if fx:
        lines.append("")
        lines.append(
            "💱 汇率: "
            f"1美元 = {float(fx.get('rate', 0)):.4f}人民币 "
            f"({fx.get('source', 'unknown')})"
        )
        if fx.get("source_text"):
            lines.append(f"📌 汇率来源: {fx.get('source_text')}")

    lines.append("━━━━━━━━━━━━━━━━━")
    lines.append("💡 当前版本仅提供黄金行情")
    lines.append("💡 数据来源: 东方财富(COMEX黄金) + Google(美元兑人民币，日更)")

    return "\n".join(lines)
