# AstrBot 基金分析插件

![astrbot_plugin_fund_analyzer](https://count.getloli.com/@astrbot_plugin_fund_analyzer)

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python 3.10+](https://img.shields.io/badge/Python-3.10+-green.svg)](https://www.python.org/downloads/)
[![AstrBot](https://img.shields.io/badge/AstrBot-v4.0+-purple.svg)](https://astrbot.app)

>  **AI 驱动的智能基金分析工具** —— 让投资决策更科学、更高效

为 AstrBot 提供专业的基金数据分析功能。**支持 AI 智能分析**、实时行情、技术分析、量化指标、策略回测等丰富特性。

---

## 🤖 核心亮点：AI 智能分析

### ✨ 让 AI 成为你的投资顾问

- 🧠 **深度量化解读** - AI 结合夏普比率、最大回撤等专业指标，给出通俗易懂的解读
- 📈 **技术面综合研判** - 融合 MACD、RSI、KDJ、布林带等多维度指标
- 🔮 **趋势预测建议** - 短期/中期走势分析，把握入场时机
- ⚠️ **智能风险预警** - 基于历史数据和市场情绪的风险评估
- 💡 **个性化投资建议** - 根据基金类型定制分析策略

### 📸 智能分析效果展示

<details>
<summary>👆 点击展开查看示例图片</summary>

![智能分析示例](templates/example.png)

</details>

> 💡 **使用方法**：发送 `智能分析 161226` 即可获取 AI 分析报告
> 
> ⚙️ **配置要求**：需在 AstrBot 管理面板配置 LLM 提供商（OpenAI、Claude、Gemini 等）

---

## ✨ 功能特色

### 📈 实时行情查询
- **LOF 基金行情**：实时价格、涨跌幅、成交量等
- **基金搜索**：支持代码和名称模糊搜索
- **个性化设置**：设置默认关注基金

### 📊 专业技术分析
- **均线系统**：MA5/MA10/MA20/MA60 移动平均线
- **趋势指标**：MACD（DIF、DEA、柱状图）
- **动量指标**：RSI（6日、14日）、KDJ
- **波动指标**：布林带、ATR 平均真实波幅

### 🔢 量化绩效分析
- **收益指标**：累计收益、年化收益、日均收益
- **风险指标**：年化波动率、最大回撤、VaR 风险价值
- **风险调整收益**：夏普比率、索提诺比率、卡玛比率

### 🔄 策略回测系统
- **MA 交叉策略**：金叉买入、死叉卖出
- **RSI 策略**：超卖反弹、超买卖出
- **回测指标**：胜率、盈亏比、最大回撤

## 📦 安装配置

### 系统要求
- Python 3.10 或更高版本
- AstrBot v4.0 或更高版本
- 网络连接（获取行情数据）

### 安装步骤

1. **克隆插件到 AstrBot 插件目录**
```bash
cd AstrBot/data/plugins/
git clone https://github.com/2529huang/astrbot_plugin_zhouzhou.git astrbot_plugin_fund_analyzer
```

2. **安装 Python 依赖**
```bash
cd astrbot_plugin_fund_analyzer
pip install -r requirements.txt
```

或手动安装：
```bash
pip install akshare pandas
```

3. **重启 AstrBot 或热重载插件**


## 📋 命令一览

| 命令                     | 说明             | 示例                 |
| ------------------------ | ---------------- | -------------------- |
| `ssgz <代码>`            | 基金实时估值      | `ssgz 001632`        |
| `基金 [代码]`            | 查询基金实时行情 | `基金 161226`        |
| `智能分析 [代码]`        | 🤖 AI量化深度分析 | `智能分析 161226`    |
| `基金分析 [代码]`        | 技术分析         | `基金分析`           |
| `量化分析 [代码]`        | 📊 专业量化指标   | `量化分析 161226`    |
| `基金历史 [代码] [天数]` | 历史行情         | `基金历史 161226 20` |
| `搜索基金 关键词`        | 搜索基金         | `搜索基金 白银`      |
| `设置基金 代码`          | 设置默认基金     | `设置基金 161226`    |
| `增加基金持仓 {...}`     | 记录个人基金持仓 | `增加基金持仓 {161226,1.0234,1200}` |
| `清仓基金 [代码] [份额/百分比]` | 卖出基金份额（默认全仓） | `清仓基金 161226 25%` |
| `ckcc`                   | 查看持仓与收益   | `ckcc`               |
| `ckqcjl [条数]`          | 查看清仓/卖出历史 | `ckqcjl 20`          |
| `更新持仓基金净值`       | 增量刷新持仓基金净值 | `更新持仓基金净值` |
| `基金帮助`               | 显示帮助         | `基金帮助`           |

## 📊 量化指标说明

| 指标类型     | 指标名称   | 说明                      |
| ------------ | ---------- | ------------------------- |
| **收益指标** | 夏普比率   | >1 表示风险调整后收益较好 |
| **收益指标** | 索提诺比率 | 只考虑下行风险的收益比    |
| **收益指标** | 卡玛比率   | 年化收益 / 最大回撤       |
| **风险指标** | 最大回撤   | 历史最大亏损幅度          |
| **风险指标** | VaR (95%)  | 95%概率下的最大日亏损     |
| **风险指标** | 年化波动率 | 价格波动程度              |
| **技术指标** | RSI        | >70 超买，<30 超卖        |
| **技术指标** | MACD       | 红柱看涨，绿柱看跌        |

## 🔧 高级配置

### 自定义 AI 提示词
编辑 `ai_analyzer/prompts.py` 可自定义 AI 分析的提示词模板：
- `SYSTEM_PROMPT` - 系统角色设定
- `ANALYSIS_PROMPT_TEMPLATE` - 主分析模板
- `QUICK_ANALYSIS_PROMPT` - 快速分析模板

## ⚠️ 免责声明

**投资有风险，入市需谨慎！**

- 本插件仅提供数据查询和技术分析功能，**不构成任何投资建议**
- 量化回测基于历史数据，**不代表未来表现**
- 投资者应根据自身风险承受能力做出独立判断
- 开发者不对因使用本插件产生的任何损失负责

## 🙏 感谢 & 参考

- [AKShare](https://github.com/akfamily/akshare) - 开源金融数据接口
- [AstrBot](https://github.com/AstrBotDevs/AstrBot) - 多平台 LLM 聊天机器人框架

## 📄 开源许可

本项目采用 [Apache License 2.0](LICENSE) 开源许可证。
