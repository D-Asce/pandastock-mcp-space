"""Hugging Face Space entrypoint: expose pandaData A-share market data as an
MCP server (Gradio mcp_server=True). Anyone can add this Space as a tool from
https://huggingface.co/settings/mcp with a single click.

Uses the PUBLIC free test account (phone/nid = "pandastock"), documented in
INTERFACES.md -- no registration, no key. Rate-limited by source IP.

Each gr.api() endpoint becomes an MCP tool (function name = tool name,
docstring = description, type hints = parameters). All delegate to
PandaStock.__request_sync, the NATS request/response subject protocol, over a
single long-lived connection.
"""
import os
import json

# panda_stock.py ships in this same directory (self-contained Space), so it is
# importable directly -- no sys.path manipulation needed when HF runs app.py
# from the Space root.
import gradio as gr
from panda_stock import PandaStock

# Public free test account (see README "测试账号").
PHONE = os.getenv("PANDA_PHONE", "pandastock")
NID = os.getenv("PANDA_NID", "pandastock")

_ps = PandaStock(phone=PHONE, nid=NID)
_ps.connect_server(timeout=20)


def _call(method, *args):
    """Run a PandaStock request/response call; reconnect once if dropped."""
    global _ps
    if not _ps.connected:
        _ps = PandaStock(phone=PHONE, nid=NID)
        _ps.connect_server(timeout=20)
    return getattr(_ps, method)(*args)


# ===== MCP tools (pure API endpoints -- no UI, exposed only to MCP) =====

def ch_stock_list() -> str:
    """Get the list of all A-share stocks (code/name). No arguments."""
    return json.dumps(_call("get_ch_stock"), ensure_ascii=False, default=str)


def ch_one_stock_real(code: str) -> str:
    """Get realtime quote for ONE A-share stock.
    Args:
        code (str): 6-digit ticker, e.g. "600519" (贵州茅台 / Kweichow Moutai).
    """
    return json.dumps(_call("get_ch_one_stock_real", code), ensure_ascii=False, default=str)


def ch_stock_front_day_history(code: str) -> str:
    """Get forward-adjusted daily K-line history for one A-share stock.
    Args:
        code (str): ticker like "600519".
    """
    return json.dumps(_call("get_ch_stock_front_day_history", code), ensure_ascii=False, default=str)


def ch_stock_minute_history(code: str, minute: str, date: str) -> str:
    """Get minute-level K-line history for one A-share stock.
    Args:
        code (str): ticker like "600519".
        minute (str): 1/5/15/30/60.
        date (str): YYYYMMDD like "20260818".
    """
    return json.dumps(_call("get_ch_stock_minute_history", code, int(minute), date), ensure_ascii=False, default=str)


def ch_core_news(date: str) -> str:
    """Get core market news for a date.
    Args:
        date (str): YYYYMMDD like "20260818".
    """
    return json.dumps(_call("get_core_new", date), ensure_ascii=False, default=str)


def ch_market_fund_flow() -> str:
    """Get whole-market capital flow. No arguments."""
    return json.dumps(_call("get_ch_market_fund_flow"), ensure_ascii=False, default=str)


def ch_lhb_data(date: str) -> str:
    """Get Dragon-Tiger list (top traded stocks) data.
    Args:
        date (str): YYYYMMDD.
    """
    return json.dumps(_call("get_lhb_data", date), ensure_ascii=False, default=str)


def ch_ddx_stock_data(code: str) -> str:
    """Get DDX (large order tracking) data for one A-share stock.
    Args:
        code (str): ticker like "600519".
    """
    return json.dumps(_call("get_ch_stock_ddx_data", code), ensure_ascii=False, default=str)


# Register every tool as an MCP-only API endpoint (no UI chrome).
with gr.Blocks() as demo:
    gr.Markdown(
        """
        # pandaData A-share MCP Space
        A-share real-time quotes, K-line history, news, fund flow and DDX via NATS.
        Public free test account -- add this Space as a tool from
        https://huggingface.co/settings/mcp
        """
    )
    gr.api(ch_stock_list)
    gr.api(ch_one_stock_real)
    gr.api(ch_stock_front_day_history)
    gr.api(ch_stock_minute_history)
    gr.api(ch_core_news)
    gr.api(ch_market_fund_flow)
    gr.api(ch_lhb_data)
    gr.api(ch_ddx_stock_data)

_, url, _ = demo.launch(mcp_server=True)
print("MCP server URL:", url)