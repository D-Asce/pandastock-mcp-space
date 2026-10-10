"""End-to-end HF Space verification (single process).

1. Build the same 8 gr.api() MCP tools as hf-space/app.py.
2. Launch Gradio with mcp_server=True on a background thread.
3. From the main thread, probe candidate schema paths AND do a real MCP
   JSON-RPC tools/list against the MCP endpoint.
4. Print the discovered tool names, then exit.

NATS is NOT connected: the MCP schema derives from tool signatures only.
"""
import json
import threading
import time
import urllib.error
import urllib.request

import gradio as gr


def ch_stock_list() -> str:
    """Get the list of all A-share stocks (code/name). No arguments."""
    return "{}"


def ch_one_stock_real(code: str) -> str:
    """Get realtime quote for ONE A-share stock.
    Args:
        code (str): 6-digit ticker, e.g. "600519".
    """
    return "{}"


def ch_stock_front_day_history(code: str) -> str:
    """Get forward-adjusted daily K-line history for one A-share stock.
    Args:
        code (str): ticker like "600519".
    """
    return "{}"


def ch_stock_minute_history(code: str, minute: str, date: str) -> str:
    """Get minute-level K-line history for one A-share stock.
    Args:
        code (str): ticker like "600519".
        minute (str): 1/5/15/30/60.
        date (str): YYYYMMDD like "20260818".
    """
    return "{}"


def ch_core_news(date: str) -> str:
    """Get core market news for a date.
    Args:
        date (str): YYYYMMDD like "20260818".
    """
    return "{}"


def ch_market_fund_flow() -> str:
    """Get whole-market capital flow. No arguments."""
    return "{}"


def ch_lhb_data(date: str) -> str:
    """Get Dragon-Tiger list (top traded stocks) data.
    Args:
        date (str): YYYYMMDD.
    """
    return "{}"


def ch_ddx_stock_data(code: str) -> str:
    """Get DDX (large order tracking) data for one A-share stock.
    Args:
        code (str): ticker like "600519".
    """
    return "{}"


TOOLS = [
    ch_stock_list, ch_one_stock_real, ch_stock_front_day_history,
    ch_stock_minute_history, ch_core_news, ch_market_fund_flow,
    ch_lhb_data, ch_ddx_stock_data,
]

holder: dict = {}


def build_and_launch():
    with gr.Blocks() as demo:
        gr.Markdown("# pandaData A-share MCP Space")
        for fn in TOOLS:
            gr.api(fn)
    try:
        _, url, _ = demo.launch(
            mcp_server=True,
            prevent_thread_lock=True,
            quiet=True,
            server_port=7863,
        )
        holder["url"] = url
    except BaseException as exc:  # noqa: BLE001
        holder["error"] = f"{type(exc).__name__}: {exc}"
        import traceback
        holder["tb"] = traceback.format_exc()


def get(url, timeout=15):
    req = urllib.request.Request(url, headers={"Accept": "application/json,*/*"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.read().decode("utf-8", "replace")


def post_mcp(url, method, params, rid, timeout=25):
    payload = json.dumps(
        {"jsonrpc": "2.0", "id": rid, "method": method, "params": params}
    ).encode()
    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.read().decode("utf-8", "replace")


def main() -> int:
    threading.Thread(target=build_and_launch, daemon=True).start()
    base = None
    for _ in range(90):
        time.sleep(1)
        if "url" in holder:
            base = holder["url"]
            break
        if "error" in holder:
            print("LAUNCH_ERROR", holder["error"])
            print(holder.get("tb", "")[-1200:])
            return 1
    if not base:
        print("LAUNCH_FAILED")
        return 1
    base = base.rstrip("/")
    print("GRADIO_URL", base)
    time.sleep(4)

    print("\n=== GET probes ===")
    for p in ("/", "/config", "/gradio_api/info", "/gradio_api/mcp/schema",
              "/gradio_api/openapi.json", "/gradio_api/mcp"):
        try:
            status, body = get(base + p)
            print(f"{status} {p} len={len(body)}")
        except urllib.error.HTTPError as e:
            print(f"{e.code} {p}")
        except Exception as e:
            print(f"ERR {p} {str(e)[:70]}")

    print("\n=== MCP JSON-RPC probes ===")
    found = False
    for p in ("/gradio_api/mcp", "/gradio_api/mcp/", "/mcp"):
        url = base + p
        try:
            status, body = post_mcp(url, "initialize", {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "verify", "version": "1"},
            }, 1)
            print(f"initialize {p} -> {status} len={len(body)}")
            print("  head:", body[:300].replace("\n", " "))
        except urllib.error.HTTPError as e:
            print(f"initialize {p} -> HTTP {e.code}")
            try:
                print("  body:", e.read().decode("utf-8", "replace")[:200])
            except Exception:
                pass
            continue
        except Exception as e:
            print(f"initialize {p} -> ERR {str(e)[:90]}")
            continue
        try:
            status, body = post_mcp(url, "tools/list", {}, 2)
            print(f"tools/list {p} -> {status} len={len(body)}")
            # Gradio returns Server-Sent Events: "event: message\ndata: {json}\n\n"
            payload = None
            for line in body.splitlines():
                line = line.strip()
                if line.startswith("data:"):
                    payload = line[len("data:"):].strip()
            if payload is None and body.lstrip().startswith("{"):
                payload = body
            if payload is None:
                print("  raw:", body[:300].replace("\n", " "))
                raise ValueError("no data: frame in SSE body")
            data = json.loads(payload)
            tools = data.get("result", {}).get("tools", [])
            print(f"TOOLS_COUNT {len(tools)}")
            for t in tools:
                desc = (t.get("description") or "").replace("\n", " ")
                props = list((t.get("inputSchema") or {}).get("properties", {}).keys())
                print(f"  - {t.get('name')} args={props} :: {desc[:60]}")
            found = len(tools) > 0
            if found:
                break
        except Exception as e:
            print(f"tools/list {p} -> ERR {str(e)[:90]}")

    print("\nRESULT", "MCP_SCHEMA_VERIFIED" if found else "MCP_SCHEMA_NOT_FOUND")
    return 0 if found else 2


if __name__ == "__main__":
    raise SystemExit(main())