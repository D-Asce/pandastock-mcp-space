---
title: PandaStock A-share MCP
emoji: 📈
colorFrom: red
colorTo: yellow
sdk: gradio
app_file: app.py
pinned: false
short_description: A股实时行情 MCP Server — NATS 推送，Level-2 / DDX / 资金流
tags:
  - mcp
  - model-context-protocol
  - finance
  - a-share
  - stock-data
  - market-data
  - nats
  - china-stock
---

# 🐼 pandaData A股实时行情 MCP Server

把 A 股实时行情 / K 线 / 资金流 / 龙虎榜 / DDX 大单，接进任意支持 MCP 的 AI 工具
（Cursor、Claude Desktop、Cline、Trae、Zed…）。**免安装、免注册。**

## 🚀 一键接入（不用装任何东西）

本 Space 是一个 **托管的 MCP Server**，点一下就能加进你的 MCP 客户端：

1. 登录 https://huggingface.co/settings/mcp
2. 找到 **PandaStock A-share MCP**（或搜索 https://huggingface.co/spaces?search=panda）
3. 点击 Space 卡片上的 **MCP 徽章** → `Add to MCP tools` → 确认
4. 重启你的 MCP 客户端

配置片段（若需手动添加）：

```json
{
  "mcpServers": {
    "panda-stock": {
      "url": "https://asce-pandastock-mcp.hf.space/gradio_api/mcp/"
    }
  }
}
```

> ⚠️ **URL 必须带尾斜杠 `/gradio_api/mcp/`**。不带斜杠会 307 重定向，部分客户端不跟随重定向会连接失败。

> ⚠️ 本 Space 为**公开测试账号**（`phone=pandastock`），按来源 IP 限频。
> 自建部署见下方「本地运行」。

## 🛠️ 提供的 8 个工具

| 工具 | 参数 | 说明 |
|---|---|---|
| `ch_stock_list` | — | 全部 A 股列表（代码/名称） |
| `ch_one_stock_real` | `code` | 单只实时行情，如 `600519` |
| `ch_stock_front_day_history` | `code` | 前复权日线历史 |
| `ch_stock_minute_history` | `code`, `minute`, `date` | 分钟线（1/5/15/30/60） |
| `ch_core_news` | `date` | 核心市场资讯（YYYYMMDD） |
| `ch_market_fund_flow` | — | 全市场资金流向 |
| `ch_lhb_data` | `date` | 龙虎榜 |
| `ch_ddx_stock_data` | `code` | DDX 大单跟踪 |

## 💬 试试这样问

- 「查一下 600519 最近一根 K 线」
- 「今天有哪些股票涨停」
- 「最近市场资金流向怎么样」
- 「拉一下 000001 这周的分钟数据」

## 🏠 本地运行（不受限频影响）

```bash
pip install -e ".[dev]"     # 或 pip install pandastock-mcp
```

```json
{
  "mcpServers": {
    "panda-stock": {
      "command": "pandastock-mcp",
      "env": { "PANDA_PHONE": "pandastock", "PANDA_NID": "pandastock" }
    }
  }
}
```

## 🔧 技术栈

- **传输**：MCP over Streamable HTTP（Gradio `mcp_server=True`）
- **数据**：NATS request/reply + publish/subscribe，不是 REST 轮询
- **依赖**：`gradio[mcp]`、`nats-py==2.14.0`（Python 3.9+）

## 🔍 验证

`verify_mcp_schema.py` 是本仓库自带的端到端验证脚本，实测通过真实 MCP JSON-RPC
（`initialize` + `tools/list`）确认 **8 个工具全部正确暴露**，参数 schema 由 type hints
正确推导。完整报告见 [VERIFICATION.md](./VERIFICATION.md)。

## 🔗 主仓库

- GitHub：https://github.com/D-Asce/pandastocksdk
- 接口清单：`INTERFACES.md`（176 个服务端接口，22 个开放接口）
- 命名空间标记：`io.github.D-Asce/pandastocksdk`（官方 MCP Registry 所有权标记）

---

<pandaData 开源实现 · NATS 推送行情 / Level-2 / DDX 大单 / 资金流 / AI 选股></pandaData>