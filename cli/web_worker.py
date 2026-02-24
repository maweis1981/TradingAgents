import argparse
import json
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


def run_task(task_dir: Path) -> None:
    from tradingagents.graph.trading_graph import TradingAgentsGraph

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

    ta = TradingAgentsGraph(selected_analysts=analysts, debug=False, config=config)
    init_state = ta.propagator.create_initial_state(ticker, trade_date)
    graph_args = ta.propagator.get_graph_args()

    final_state = None
    last_reports = {}

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
