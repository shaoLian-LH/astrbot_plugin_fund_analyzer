from typing import Any


class AnalysisService:
    """分析领域服务（绘图相关）。"""

    def __init__(self, logger: Any):
        self._logger = logger

    def plot_history_chart(self, history: list[dict], fund_name: str) -> str | None:
        """绘制历史行情走势图 (价格+均线+成交量) 并返回 Base64 字符串。"""
        try:
            import base64
            import io
            import matplotlib.dates as mdates
            import matplotlib.gridspec as gridspec
            import matplotlib.pyplot as plt
            import pandas as pd

            plt.rcParams["font.sans-serif"] = [
                "SimHei",
                "Arial Unicode MS",
                "Microsoft YaHei",
                "WenQuanYi Micro Hei",
                "sans-serif",
            ]
            plt.rcParams["axes.unicode_minus"] = False

            df = pd.DataFrame(history)
            if df.empty:
                return None

            df["date"] = pd.to_datetime(df["date"])
            dates = df["date"]
            closes = df["close"]
            volumes = df["volume"]

            df["ma5"] = df["close"].rolling(window=5).mean()
            df["ma10"] = df["close"].rolling(window=10).mean()
            df["ma20"] = df["close"].rolling(window=20).mean()

            fig = plt.figure(figsize=(10, 6), dpi=100)
            gs = gridspec.GridSpec(2, 1, height_ratios=[3, 1], hspace=0.15)

            ax1 = plt.subplot(gs[0])
            ax1.plot(dates, closes, label="收盘价", color="#333333", linewidth=1.5)
            ax1.plot(
                dates, df["ma5"], label="MA5", color="#f5222d", linewidth=1.0, alpha=0.8
            )
            ax1.plot(
                dates,
                df["ma10"],
                label="MA10",
                color="#faad14",
                linewidth=1.0,
                alpha=0.8,
            )

            if len(df) >= 20:
                ax1.plot(
                    dates,
                    df["ma20"],
                    label="MA20",
                    color="#52c41a",
                    linewidth=1.0,
                    alpha=0.8,
                )

            ax1.set_title(f"{fund_name} - 价格走势", fontsize=14, pad=10)
            ax1.grid(True, linestyle="--", alpha=0.3)
            ax1.legend(loc="upper left", frameon=True, fontsize=9)

            ax2 = plt.subplot(gs[1], sharex=ax1)
            colors = []
            for i in range(len(df)):
                if i == 0:
                    c = "#f5222d" if df.iloc[i].get("change_rate", 0) > 0 else "#52c41a"
                else:
                    change = df.iloc[i]["close"] - df.iloc[i - 1]["close"]
                    c = "#f5222d" if change >= 0 else "#52c41a"
                colors.append(c)

            ax2.bar(dates, volumes, color=colors, alpha=0.8)
            ax2.set_ylabel("成交量", fontsize=10)
            ax2.grid(True, linestyle="--", alpha=0.3)

            ax1.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d"))
            plt.setp(ax1.get_xticklabels(), visible=False)
            plt.gcf().autofmt_xdate()
            plt.tight_layout()

            buffer = io.BytesIO()
            plt.savefig(buffer, format="png", bbox_inches="tight")
            buffer.seek(0)

            image_base64 = base64.b64encode(buffer.read()).decode("utf-8")
            plt.close(fig)
            return image_base64
        except Exception as e:
            self._logger.error(f"绘图失败: {e}")
            return None

    def plot_comparison_chart(
        self,
        history_a: list[dict],
        name_a: str,
        history_b: list[dict],
        name_b: str,
    ) -> str | None:
        """绘制双基金对比走势图 (归一化收益率)。"""
        try:
            import base64
            import io
            import matplotlib.dates as mdates
            import matplotlib.pyplot as plt
            import pandas as pd

            plt.rcParams["font.sans-serif"] = [
                "SimHei",
                "Arial Unicode MS",
                "Microsoft YaHei",
                "WenQuanYi Micro Hei",
                "sans-serif",
            ]
            plt.rcParams["axes.unicode_minus"] = False

            df_a = pd.DataFrame(history_a)
            df_b = pd.DataFrame(history_b)
            if df_a.empty or df_b.empty:
                return None

            df_a["date"] = pd.to_datetime(df_a["date"])
            df_b["date"] = pd.to_datetime(df_b["date"])
            df_a = df_a.sort_values("date")
            df_b = df_b.sort_values("date")

            common_dates = pd.merge(
                df_a[["date"]], df_b[["date"]], on="date", how="inner"
            )["date"]
            if common_dates.empty:
                return None

            df_a = df_a[df_a["date"].isin(common_dates)]
            df_b = df_b[df_b["date"].isin(common_dates)]

            base_a = df_a.iloc[0]["close"]
            base_b = df_b.iloc[0]["close"]
            if base_a == 0 or base_b == 0:
                return None

            df_a["norm_close"] = (df_a["close"] - base_a) / base_a * 100
            df_b["norm_close"] = (df_b["close"] - base_b) / base_b * 100

            fig, ax = plt.subplots(figsize=(10, 5), dpi=100)
            ax.plot(
                df_a["date"], df_a["norm_close"], label=f"{name_a}", color="#1890ff", linewidth=2
            )
            ax.plot(
                df_b["date"], df_b["norm_close"], label=f"{name_b}", color="#eb2f96", linewidth=2
            )

            ax.fill_between(
                df_a["date"],
                df_a["norm_close"],
                df_b["norm_close"],
                where=(df_a["norm_close"] > df_b["norm_close"]),
                interpolate=True,
                color="#1890ff",
                alpha=0.1,
            )
            ax.fill_between(
                df_a["date"],
                df_a["norm_close"],
                df_b["norm_close"],
                where=(df_a["norm_close"] < df_b["norm_close"]),
                interpolate=True,
                color="#eb2f96",
                alpha=0.1,
            )

            ax.set_title("累计收益率对比 (%)", fontsize=14, pad=10)
            ax.grid(True, linestyle="--", alpha=0.3)
            ax.legend(loc="upper left", frameon=True)

            import matplotlib.ticker as mtick

            ax.yaxis.set_major_formatter(mtick.PercentFormatter())
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d"))
            plt.gcf().autofmt_xdate()
            plt.tight_layout()

            buffer = io.BytesIO()
            plt.savefig(buffer, format="png", bbox_inches="tight")
            buffer.seek(0)

            image_base64 = base64.b64encode(buffer.read()).decode("utf-8")
            plt.close(fig)
            return image_base64
        except Exception as e:
            self._logger.error(f"对比绘图失败: {e}")
            return None

