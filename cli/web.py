import html
import os
import traceback
from datetime import date
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs

from dotenv import load_dotenv

from tradingagents.default_config import DEFAULT_CONFIG
from tradingagents.graph.trading_graph import TradingAgentsGraph

load_dotenv()

HOST = os.getenv("TRADINGAGENTS_WEB_HOST", "127.0.0.1")
PORT = int(os.getenv("TRADINGAGENTS_WEB_PORT", "8088"))

PROVIDERS = ["openai", "google", "anthropic", "xai", "openrouter", "ollama"]


def _escape(text: str) -> str:
    return html.escape(text or "")


def _render_page(values: dict | None = None, result: dict | None = None, error: str = "") -> str:
    values = values or {}
    result = result or {}

    ticker = values.get("ticker", "NVDA")
    trade_date = values.get("trade_date", str(date.today()))
    provider = values.get("provider", "openai")
    deep_model = values.get("deep_model", DEFAULT_CONFIG["deep_think_llm"])
    quick_model = values.get("quick_model", DEFAULT_CONFIG["quick_think_llm"])
    rounds = values.get("rounds", "1")

    provider_options = "".join(
        f'<option value="{p}"{" selected" if p == provider else ""}>{p}</option>' for p in PROVIDERS
    )

    result_block = ""
    if result:
        result_block = f"""
        <section class=\"card\">
          <h2>Result</h2>
          <p><strong>Processed Decision:</strong> {_escape(result.get('decision', ''))}</p>
          <h3>Final Trade Decision</h3>
          <pre>{_escape(result.get('final_trade_decision', ''))}</pre>
          <h3>Investment Plan</h3>
          <pre>{_escape(result.get('investment_plan', ''))}</pre>
        </section>
        """

    error_block = f'<section class="card error"><h2>Error</h2><pre>{_escape(error)}</pre></section>' if error else ""

    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>TradingAgents Web UI</title>
    <style>
      :root {{
        --bg: #f4f6f8;
        --fg: #15202b;
        --card: #ffffff;
        --line: #d0d7de;
        --primary: #0b6bcb;
        --error: #8a1c1c;
      }}
      * {{ box-sizing: border-box; }}
      body {{ margin: 0; font-family: ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: linear-gradient(120deg, #eef4ff, #f8fbff 40%, #eff7f1); color: var(--fg); }}
      .wrap {{ max-width: 980px; margin: 28px auto; padding: 0 16px 24px; }}
      h1 {{ margin: 0 0 16px; font-size: 28px; }}
      .card {{ background: var(--card); border: 1px solid var(--line); border-radius: 12px; padding: 16px; margin-bottom: 16px; box-shadow: 0 8px 24px rgba(20, 31, 41, 0.06); }}
      .grid {{ display: grid; gap: 12px; grid-template-columns: repeat(2, minmax(0, 1fr)); }}
      label {{ font-weight: 600; font-size: 14px; display: block; margin-bottom: 6px; }}
      input, select {{ width: 100%; border: 1px solid var(--line); border-radius: 8px; padding: 10px 12px; font-size: 14px; background: #fff; }}
      button {{ border: 0; border-radius: 10px; background: var(--primary); color: #fff; padding: 10px 16px; font-size: 14px; font-weight: 700; cursor: pointer; margin-top: 12px; }}
      pre {{ background: #f6f8fa; border: 1px solid var(--line); border-radius: 8px; padding: 12px; white-space: pre-wrap; word-break: break-word; }}
      .error {{ border-color: #e2b5b5; }}
      .error h2 {{ color: var(--error); }}
      .tip {{ margin: 0; font-size: 13px; color: #4f5d69; }}
      @media (max-width: 720px) {{ .grid {{ grid-template-columns: 1fr; }} }}
    </style>
  </head>
  <body>
    <main class="wrap">
      <h1>TradingAgents Web UI</h1>
      <section class="card">
        <form method="post" action="/run">
          <div class="grid">
            <div>
              <label for="ticker">Ticker</label>
              <input id="ticker" name="ticker" value="{_escape(ticker)}" required />
            </div>
            <div>
              <label for="trade_date">Trade Date (YYYY-MM-DD)</label>
              <input id="trade_date" name="trade_date" type="date" value="{_escape(trade_date)}" required />
            </div>
            <div>
              <label for="provider">LLM Provider</label>
              <select id="provider" name="provider">{provider_options}</select>
            </div>
            <div>
              <label for="rounds">Debate Rounds</label>
              <input id="rounds" name="rounds" type="number" min="1" max="6" value="{_escape(rounds)}" />
            </div>
            <div>
              <label for="deep_model">Deep Think Model</label>
              <input id="deep_model" name="deep_model" value="{_escape(deep_model)}" required />
            </div>
            <div>
              <label for="quick_model">Quick Think Model</label>
              <input id="quick_model" name="quick_model" value="{_escape(quick_model)}" required />
            </div>
          </div>
          <button type="submit">Run Analysis</button>
        </form>
        <p class="tip">Run may take several minutes depending on model/data latency.</p>
      </section>
      {error_block}
      {result_block}
    </main>
  </body>
</html>
"""


class TradingAgentsWebHandler(BaseHTTPRequestHandler):
    def _send_html(self, body: str, status: int = HTTPStatus.OK) -> None:
        body_bytes = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body_bytes)))
        self.end_headers()
        self.wfile.write(body_bytes)

    def do_GET(self) -> None:
        if self.path != "/":
            self._send_html("<h1>Not Found</h1>", status=HTTPStatus.NOT_FOUND)
            return
        self._send_html(_render_page())

    def do_POST(self) -> None:
        if self.path != "/run":
            self._send_html("<h1>Not Found</h1>", status=HTTPStatus.NOT_FOUND)
            return

        content_length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(content_length).decode("utf-8")
        form = {k: v[0] for k, v in parse_qs(raw).items()}

        try:
            ticker = (form.get("ticker") or "").strip().upper()
            if not ticker:
                raise ValueError("Ticker is required")

            trade_date = form.get("trade_date") or str(date.today())
            date.fromisoformat(trade_date)

            provider = (form.get("provider") or "openai").strip().lower()
            if provider not in PROVIDERS:
                raise ValueError(f"Invalid provider: {provider}")

            rounds = int(form.get("rounds") or "1")
            if rounds < 1 or rounds > 6:
                raise ValueError("Debate rounds must be between 1 and 6")

            deep_model = (form.get("deep_model") or "").strip()
            quick_model = (form.get("quick_model") or "").strip()
            if not deep_model or not quick_model:
                raise ValueError("Model names are required")

            config = DEFAULT_CONFIG.copy()
            config["llm_provider"] = provider
            config["deep_think_llm"] = deep_model
            config["quick_think_llm"] = quick_model
            config["max_debate_rounds"] = rounds

            ta = TradingAgentsGraph(debug=False, config=config)
            final_state, decision = ta.propagate(ticker, trade_date)

            result = {
                "decision": decision,
                "final_trade_decision": final_state.get("final_trade_decision", ""),
                "investment_plan": final_state.get("investment_plan", ""),
            }
            self._send_html(_render_page(values=form, result=result))
        except Exception:
            self._send_html(_render_page(values=form, error=traceback.format_exc()), status=HTTPStatus.BAD_REQUEST)


def main() -> None:
    server = ThreadingHTTPServer((HOST, PORT), TradingAgentsWebHandler)
    print(f"TradingAgents Web UI running at http://{HOST}:{PORT}")
    server.serve_forever()


if __name__ == "__main__":
    main()
