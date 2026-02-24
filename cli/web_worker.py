import argparse
import json
import re
import threading
import textwrap
import time
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    def load_dotenv() -> None:
        return

from tradingagents.default_config import DEFAULT_CONFIG

load_dotenv()

REPORT_FIELDS = [
    "market_report",
    "sentiment_report",
    "news_report",
    "fundamentals_report",
    "investment_plan",
    "trader_investment_plan",
    "final_trade_decision",
]
REPORT_TITLES = {
    "market_report": "Market Report",
    "sentiment_report": "Sentiment Report",
    "news_report": "News Report",
    "fundamentals_report": "Fundamentals Report",
    "investment_plan": "Investment Plan",
    "trader_investment_plan": "Trader Investment Plan",
    "final_trade_decision": "Final Trade Decision",
}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


def read_json(path: Path, default: dict | None = None) -> dict:
    if not path.exists():
        return default or {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default or {}


def write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def append_event(task_dir: Path, event_type: str, message: str, data: dict | None = None) -> None:
    event = {
        "ts": now_iso(),
        "type": event_type,
        "message": message,
        "data": data or {},
    }
    with (task_dir / "events.jsonl").open("a", encoding="utf-8") as f:
        f.write(json.dumps(event, ensure_ascii=False) + "\n")


def update_status(task_dir: Path, **fields) -> dict:
    status_path = task_dir / "status.json"
    status = read_json(status_path, default={})
    status.update(fields)
    status["updated_at"] = now_iso()
    write_json(status_path, status)
    return status


def _message_to_text(message_obj) -> str:
    content = getattr(message_obj, "content", message_obj)
    if isinstance(content, list):
        return str(content)
    return str(content)


def _safe_filename(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_-]+", "_", name).strip("_").lower()


def _collect_markdown_docs(final_state: dict, decision: str) -> dict[str, str]:
    docs: dict[str, str] = {}
    for field in REPORT_FIELDS:
        value = final_state.get(field)
        if value:
            docs[field] = str(value)
    docs["result_summary"] = (
        f"# Result Summary\n\n"
        f"- Decision: `{decision}`\n"
        f"- Ticker: `{final_state.get('company_of_interest', '')}`\n"
        f"- Trade Date: `{final_state.get('trade_date', '')}`\n"
    )
    return docs


def _write_markdown_bundle(root: Path, docs: dict[str, str]) -> list[str]:
    root.mkdir(parents=True, exist_ok=True)
    out = []
    for idx, (key, text) in enumerate(docs.items(), start=1):
        title = REPORT_TITLES.get(key, key.replace("_", " ").title())
        name = f"{idx:02d}_{_safe_filename(key)}.md"
        p = root / name
        p.write_text(f"# {title}\n\n{text}\n", encoding="utf-8")
        out.append(str(p))
    return out


def _markdown_to_plain_text(md: str) -> str:
    text = md.replace("\r\n", "\n")
    text = re.sub(r"```.*?```", "", text, flags=re.S)
    text = re.sub(r"`([^`]*)`", r"\1", text)
    text = re.sub(r"^#{1,6}\s*", "", text, flags=re.M)
    text = re.sub(r"^\s*[-*+]\s+", "• ", text, flags=re.M)
    text = re.sub(r"\[(.*?)\]\((.*?)\)", r"\1 (\2)", text)
    return text.strip()


def _write_pdf(text: str, pdf_path: Path, chinese: bool = False) -> None:
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.cidfonts import UnicodeCIDFont
    from reportlab.pdfgen import canvas

    pdf_path.parent.mkdir(parents=True, exist_ok=True)

    c = canvas.Canvas(str(pdf_path), pagesize=A4)
    width, height = A4
    margin_x = 40
    margin_y = 40
    y = height - margin_y

    if chinese:
        font_name = "STSong-Light"
        try:
            pdfmetrics.getFont(font_name)
        except Exception:
            pdfmetrics.registerFont(UnicodeCIDFont(font_name))
        c.setFont(font_name, 10)
        wrap_width = 46
    else:
        font_name = "Helvetica"
        c.setFont(font_name, 10)
        wrap_width = 95

    lines = []
    for raw in text.splitlines():
        line = raw.rstrip()
        if not line:
            lines.append("")
            continue
        wrapped = textwrap.wrap(line, width=wrap_width, break_long_words=True, break_on_hyphens=False)
        lines.extend(wrapped if wrapped else [""])

    for line in lines:
        if y < margin_y:
            c.showPage()
            c.setFont(font_name, 10)
            y = height - margin_y
        c.drawString(margin_x, y, line)
        y -= 14

    c.save()


def _translate_to_zh(task: dict, markdown: str) -> str:
    from tradingagents.llm_clients import create_llm_client

    provider = task["provider"]
    model = task["quick_model"]
    client = create_llm_client(
        provider=provider,
        model=model,
        base_url=DEFAULT_CONFIG.get("backend_url"),
    )
    llm = client.get_llm()
    prompt = (
        "Translate the following markdown content into Simplified Chinese. "
        "Keep markdown structure, headings, bullet points, code blocks, and tables unchanged in format. "
        "Only translate natural language text.\n\n"
        f"{markdown}"
    )
    resp = llm.invoke(prompt)
    text = _message_to_text(resp).strip()
    if not text:
        raise RuntimeError("Empty translation result")
    return text


def run_task(task_dir: Path) -> None:
    task = read_json(task_dir / "task.json", default={})
    if not task:
        raise RuntimeError("task.json missing or invalid")

    ticker = task["ticker"]
    trade_date = task["trade_date"]

    config = DEFAULT_CONFIG.copy()
    config["llm_provider"] = task["provider"]
    config["deep_think_llm"] = task["deep_model"]
    config["quick_think_llm"] = task["quick_model"]
    config["max_debate_rounds"] = int(task["rounds"])

    analysts = task.get("analysts") or ["market", "social", "news", "fundamentals"]

    status = read_json(task_dir / "status.json", default={})
    attempt = int(status.get("attempt") or 1)

    append_event(
        task_dir,
        "start",
        f"Starting analysis: ticker={ticker}, date={trade_date}, provider={task['provider']}, attempt={attempt}",
        {"analysts": analysts, "attempt": attempt},
    )
    update_status(task_dir, status="running", attempt=attempt)

    # Heartbeat so UI always has live progress even when upstream calls are slow.
    heartbeat_stop = threading.Event()
    started = time.time()

    def heartbeat() -> None:
        while not heartbeat_stop.wait(5):
            elapsed = int(time.time() - started)
            append_event(task_dir, "heartbeat", f"Task running... {elapsed}s elapsed")

    hb_thread = threading.Thread(target=heartbeat, daemon=True)
    hb_thread.start()

    append_event(task_dir, "phase", "Loading TradingAgents graph")
    from tradingagents.graph.trading_graph import TradingAgentsGraph

    append_event(task_dir, "phase", "Initializing graph instance")
    ta = TradingAgentsGraph(selected_analysts=analysts, debug=False, config=config)
    append_event(task_dir, "phase", "Building initial state")
    init_state = ta.propagator.create_initial_state(ticker, trade_date)
    graph_args = ta.propagator.get_graph_args()
    append_event(task_dir, "phase", "Starting graph stream")

    final_state = None
    last_reports = {}

    try:
        for idx, chunk in enumerate(ta.graph.stream(init_state, **graph_args), start=1):
            final_state = chunk
            keys = sorted(chunk.keys())
            append_event(task_dir, "chunk", f"Chunk {idx}: {', '.join(keys)}")

            messages = chunk.get("messages") or []
            if messages:
                text = _message_to_text(messages[-1]).strip()
                if text:
                    append_event(task_dir, "message", text[:4000])

            for field in REPORT_FIELDS:
                if field in chunk and chunk.get(field):
                    value = str(chunk[field])
                    if last_reports.get(field) != value:
                        last_reports[field] = value
                        append_event(
                            task_dir,
                            "report",
                            f"{field} updated",
                            {"field": field, "content": value[:12000]},
                        )
    finally:
        heartbeat_stop.set()

    if final_state is None:
        raise RuntimeError("No graph output produced")

    decision = ta.process_signal(final_state.get("final_trade_decision", ""))
    result = {
        "decision": decision,
        "final_trade_decision": final_state.get("final_trade_decision", ""),
        "investment_plan": final_state.get("investment_plan", ""),
        "trade_date": trade_date,
        "ticker": ticker,
    }

    docs_en = _collect_markdown_docs(final_state, decision)
    artifacts = {
        "reports_en": [],
        "reports_zh": [],
        "pdf_en": [],
        "pdf_zh": [],
    }

    append_event(task_dir, "phase", "Exporting English markdown reports")
    en_files = _write_markdown_bundle(task_dir / "reports_en", docs_en)
    artifacts["reports_en"] = [str(Path(p).relative_to(task_dir)) for p in en_files]

    if task.get("export_pdf", True):
        append_event(task_dir, "phase", "Converting English markdown reports to PDF")
        for md_path in en_files:
            md_file = Path(md_path)
            text = _markdown_to_plain_text(md_file.read_text(encoding="utf-8"))
            pdf_path = task_dir / "pdf_en" / f"{md_file.stem}.pdf"
            try:
                _write_pdf(text, pdf_path, chinese=False)
                artifacts["pdf_en"].append(str(pdf_path.relative_to(task_dir)))
            except Exception as pdf_err:
                append_event(task_dir, "pdf_error", f"{md_file.name} PDF export failed", {"error": str(pdf_err)})

    if task.get("translate_to_zh", True):
        append_event(task_dir, "phase", "Translating reports to Chinese")
        docs_zh: dict[str, str] = {}
        for key, text in docs_en.items():
            try:
                docs_zh[key] = _translate_to_zh(task, text)
                append_event(task_dir, "translation", f"{key} translated to Chinese")
            except Exception as trans_err:
                append_event(task_dir, "translation_error", f"{key} translation failed", {"error": str(trans_err)})
                docs_zh[key] = text

        zh_files = _write_markdown_bundle(task_dir / "reports_zh", docs_zh)
        artifacts["reports_zh"] = [str(Path(p).relative_to(task_dir)) for p in zh_files]

        if task.get("export_pdf", True):
            append_event(task_dir, "phase", "Converting Chinese markdown reports to PDF")
            for md_path in zh_files:
                md_file = Path(md_path)
                text = _markdown_to_plain_text(md_file.read_text(encoding="utf-8"))
                pdf_path = task_dir / "pdf_zh" / f"{md_file.stem}.pdf"
                try:
                    _write_pdf(text, pdf_path, chinese=True)
                    artifacts["pdf_zh"].append(str(pdf_path.relative_to(task_dir)))
                except Exception as pdf_err:
                    append_event(task_dir, "pdf_error", f"{md_file.name} PDF export failed", {"error": str(pdf_err)})

    result["artifacts"] = artifacts
    write_json(task_dir / "result.json", result)
    append_event(task_dir, "completed", f"Task completed with decision: {decision}")
    update_status(task_dir, status="completed", pid=None, next_retry_at=None, last_error=None)


def main() -> None:
    parser = argparse.ArgumentParser(description="TradingAgents web background worker")
    parser.add_argument("--task-dir", required=True, help="Path to task directory")
    args = parser.parse_args()

    task_dir = Path(args.task_dir).resolve()
    task_dir.mkdir(parents=True, exist_ok=True)

    try:
        run_task(task_dir)
    except Exception:
        err = traceback.format_exc()
        append_event(task_dir, "error", "Task failed", {"traceback": err})
        task = read_json(task_dir / "task.json", default={})
        status = read_json(task_dir / "status.json", default={})
        attempt = int(status.get("attempt") or 1)
        max_retries = int(task.get("max_retries", 2))
        retry_delay_seconds = int(task.get("retry_delay_seconds", 15))
        max_attempts = 1 + max_retries

        if attempt < max_attempts:
            next_retry_at = now_iso() if retry_delay_seconds <= 0 else (
                datetime.now(timezone.utc) + timedelta(seconds=retry_delay_seconds)
            ).isoformat()
            update_status(
                task_dir,
                status="queued",
                pid=None,
                last_error=err.splitlines()[-1] if err else "unknown error",
                next_retry_at=next_retry_at,
            )
            append_event(
                task_dir,
                "retry_scheduled",
                f"Retry scheduled: attempt {attempt + 1}/{max_attempts}",
                {"next_retry_at": next_retry_at, "retry_delay_seconds": retry_delay_seconds},
            )
        else:
            update_status(
                task_dir,
                status="failed",
                pid=None,
                last_error=err.splitlines()[-1] if err else "unknown error",
                next_retry_at=None,
            )


if __name__ == "__main__":
    main()
