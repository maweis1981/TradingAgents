import html
import hashlib
import json
import secrets
import os
import subprocess
import sys
import threading
import time
import uuid
import sqlite3
from datetime import date, datetime, timedelta, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import mimetypes
from pathlib import Path
from urllib.parse import parse_qs, quote, urlparse

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    def load_dotenv() -> None:
        return

from tradingagents.default_config import DEFAULT_CONFIG

load_dotenv()

HOST = os.getenv("TRADINGAGENTS_WEB_HOST", "127.0.0.1")
PORT = int(os.getenv("TRADINGAGENTS_WEB_PORT", "8088"))
TASKS_DIR = Path(
    os.getenv(
        "TRADINGAGENTS_WEB_TASKS_DIR",
        str(Path(DEFAULT_CONFIG["results_dir"]) / "web_tasks"),
    )
).resolve()
TASKS_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = Path(os.getenv("TRADINGAGENTS_WEB_DB", str(TASKS_DIR / "web_app.db"))).resolve()
TASK_CONTROL_LOCK = threading.Lock()
RECONCILE_INTERVAL_SECONDS = int(os.getenv("TRADINGAGENTS_WEB_RECONCILE_INTERVAL", "3"))
SESSION_COOKIE = "ta_session_id"

PLANS = [
    {"plan_id": "free", "name": "Free", "price_usd": 0, "ticker_limit": 2, "daily_query_limit": 2, "is_paid": 0},
    {"plan_id": "pro", "name": "Pro", "price_usd": 49, "ticker_limit": 20, "daily_query_limit": 50, "is_paid": 1},
    {"plan_id": "elite", "name": "Elite", "price_usd": 199, "ticker_limit": 100, "daily_query_limit": 300, "is_paid": 1},
]

PROVIDERS = ["openai", "google", "anthropic", "xai", "openrouter", "ollama"]
DEFAULT_ANALYSTS = ["market", "social", "news", "fundamentals"]
REPORT_FIELDS = [
    "market_report",
    "sentiment_report",
    "news_report",
    "fundamentals_report",
    "investment_plan",
    "trader_investment_plan",
    "final_trade_decision",
]


def _db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _init_db() -> None:
    conn = _db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            plan_id TEXT NOT NULL DEFAULT 'free',
            created_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sessions (
            session_id TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            last_seen TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS plans (
            plan_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            price_usd INTEGER NOT NULL,
            ticker_limit INTEGER NOT NULL,
            daily_query_limit INTEGER NOT NULL,
            is_paid INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS user_usage (
            user_id INTEGER NOT NULL,
            usage_date TEXT NOT NULL,
            query_count INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (user_id, usage_date)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS user_tickers (
            user_id INTEGER NOT NULL,
            ticker TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (user_id, ticker)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_suggestions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date TEXT NOT NULL,
            symbol TEXT NOT NULL,
            decision TEXT NOT NULL,
            summary TEXT NOT NULL,
            details TEXT NOT NULL
        )
        """
    )

    for p in PLANS:
        cur.execute(
            """
            INSERT INTO plans (plan_id, name, price_usd, ticker_limit, daily_query_limit, is_paid)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(plan_id) DO UPDATE SET
                name=excluded.name,
                price_usd=excluded.price_usd,
                ticker_limit=excluded.ticker_limit,
                daily_query_limit=excluded.daily_query_limit,
                is_paid=excluded.is_paid
            """,
            (p["plan_id"], p["name"], p["price_usd"], p["ticker_limit"], p["daily_query_limit"], p["is_paid"]),
        )

    today = str(date.today())
    cnt = cur.execute("SELECT COUNT(*) FROM daily_suggestions WHERE trade_date = ?", (today,)).fetchone()[0]
    if cnt == 0:
        seeds = [
            ("NVDA", "BUY", "AI momentum remains strong", "Earnings revisions and data-center demand remain resilient."),
            ("TSLA", "NO_TRADE", "Volatility elevated before catalysts", "Await clearer delivery and margin trend before position."),
            ("MSFT", "BUY", "Cloud quality plus AI optionality", "Commercial cloud growth quality remains solid with durable cash flow."),
            ("AAPL", "NO_TRADE", "Mixed hardware demand signals", "Valuation support exists but near-term product cycle uncertainty remains."),
        ]
        for sym, dec, summary, details in seeds:
            cur.execute(
                """
                INSERT INTO daily_suggestions (trade_date, symbol, decision, summary, details)
                VALUES (?, ?, ?, ?, ?)
                """,
                (today, sym, dec, summary, details),
            )
    conn.commit()
    conn.close()


def _hash_password(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def _parse_cookies(cookie_header: str | None) -> dict[str, str]:
    if not cookie_header:
        return {}
    out: dict[str, str] = {}
    for part in cookie_header.split(";"):
        if "=" not in part:
            continue
        k, v = part.split("=", 1)
        out[k.strip()] = v.strip()
    return out


def _get_plan(plan_id: str) -> dict:
    conn = _db_connect()
    row = conn.execute("SELECT * FROM plans WHERE plan_id = ?", (plan_id,)).fetchone()
    conn.close()
    if not row:
        return {k: v for k, v in PLANS[0].items()}
    return dict(row)


def _create_user(username: str, password: str) -> tuple[bool, str]:
    if len(username) < 3 or len(password) < 6:
        return False, "Username must be >=3 chars and password must be >=6 chars."
    conn = _db_connect()
    try:
        conn.execute(
            "INSERT INTO users (username, password_hash, plan_id, created_at) VALUES (?, ?, 'free', ?)",
            (username, _hash_password(password), _now_iso()),
        )
        conn.commit()
        return True, ""
    except sqlite3.IntegrityError:
        return False, "Username already exists."
    finally:
        conn.close()


def _authenticate_user(username: str, password: str) -> dict | None:
    conn = _db_connect()
    row = conn.execute(
        "SELECT * FROM users WHERE username = ? AND password_hash = ?",
        (username, _hash_password(password)),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def _create_session(user_id: int) -> str:
    sid = secrets.token_urlsafe(24)
    conn = _db_connect()
    conn.execute(
        "INSERT INTO sessions (session_id, user_id, created_at, last_seen) VALUES (?, ?, ?, ?)",
        (sid, user_id, _now_iso(), _now_iso()),
    )
    conn.commit()
    conn.close()
    return sid


def _delete_session(session_id: str) -> None:
    conn = _db_connect()
    conn.execute("DELETE FROM sessions WHERE session_id = ?", (session_id,))
    conn.commit()
    conn.close()


def _current_user_from_session(session_id: str | None) -> dict | None:
    if not session_id:
        return None
    conn = _db_connect()
    row = conn.execute(
        """
        SELECT u.*
        FROM sessions s
        JOIN users u ON u.id = s.user_id
        WHERE s.session_id = ?
        """,
        (session_id,),
    ).fetchone()
    if row:
        conn.execute("UPDATE sessions SET last_seen = ? WHERE session_id = ?", (_now_iso(), session_id))
        conn.commit()
    conn.close()
    return dict(row) if row else None


def _usage_stats(user: dict) -> dict:
    uid = int(user["id"])
    today = str(date.today())
    conn = _db_connect()
    usage_row = conn.execute(
        "SELECT query_count FROM user_usage WHERE user_id = ? AND usage_date = ?",
        (uid, today),
    ).fetchone()
    ticker_count = conn.execute(
        "SELECT COUNT(*) FROM user_tickers WHERE user_id = ?",
        (uid,),
    ).fetchone()[0]
    conn.close()
    plan = _get_plan(user["plan_id"])
    used = int(usage_row["query_count"]) if usage_row else 0
    return {
        "plan": plan,
        "used_queries": used,
        "remaining_queries": max(int(plan["daily_query_limit"]) - used, 0),
        "ticker_count": int(ticker_count),
        "remaining_tickers": max(int(plan["ticker_limit"]) - int(ticker_count), 0),
    }


def _can_submit_task(user: dict, ticker: str) -> tuple[bool, str]:
    uid = int(user["id"])
    ticker = ticker.upper()
    stats = _usage_stats(user)
    if stats["remaining_queries"] <= 0:
        return False, "Daily query limit reached for your plan."
    conn = _db_connect()
    exists = conn.execute(
        "SELECT 1 FROM user_tickers WHERE user_id = ? AND ticker = ?",
        (uid, ticker),
    ).fetchone()
    conn.close()
    if not exists and stats["remaining_tickers"] <= 0:
        return False, "Ticker subscription limit reached for your plan."
    return True, ""


def _record_task_usage(user: dict, ticker: str) -> None:
    uid = int(user["id"])
    ticker = ticker.upper()
    today = str(date.today())
    conn = _db_connect()
    conn.execute(
        """
        INSERT INTO user_usage (user_id, usage_date, query_count)
        VALUES (?, ?, 1)
        ON CONFLICT(user_id, usage_date) DO UPDATE SET query_count = query_count + 1
        """,
        (uid, today),
    )
    conn.execute(
        """
        INSERT INTO user_tickers (user_id, ticker, created_at)
        VALUES (?, ?, ?)
        ON CONFLICT(user_id, ticker) DO NOTHING
        """,
        (uid, ticker, _now_iso()),
    )
    conn.commit()
    conn.close()


def _daily_suggestions() -> list[dict]:
    conn = _db_connect()
    rows = conn.execute(
        "SELECT * FROM daily_suggestions WHERE trade_date = ? ORDER BY id ASC",
        (str(date.today()),),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _escape(text: str) -> str:
    return html.escape(text or "")


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _read_json(path: Path, default: dict | None = None) -> dict:
    if not path.exists():
        return default or {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default or {}


def _append_event(task_dir: Path, event_type: str, message: str, data: dict | None = None) -> None:
    event = {
        "ts": _now_iso(),
        "type": event_type,
        "message": message,
        "data": data or {},
    }
    with (task_dir / "events.jsonl").open("a", encoding="utf-8") as f:
        f.write(json.dumps(event, ensure_ascii=False) + "\n")


def _update_status(task_dir: Path, **fields) -> dict:
    status_path = task_dir / "status.json"
    status = _read_json(status_path, default={})
    status.update(fields)
    status["updated_at"] = _now_iso()
    _write_json(status_path, status)
    return status


def _task_paths(task_id: str) -> dict:
    task_dir = TASKS_DIR / task_id
    return {
        "dir": task_dir,
        "task": task_dir / "task.json",
        "status": task_dir / "status.json",
        "result": task_dir / "result.json",
        "events": task_dir / "events.jsonl",
    }


def _list_tasks(user_id: int | None = None) -> list[dict]:
    tasks = []
    for task_dir in sorted(TASKS_DIR.glob("*"), reverse=True):
        if not task_dir.is_dir():
            continue
        task = _read_json(task_dir / "task.json", default={})
        if not task:
            continue
        owner = task.get("user_id")
        if user_id is not None and owner != user_id:
            continue
        status = _read_json(task_dir / "status.json", default={})
        tasks.append(
            {
                "task_id": task.get("task_id", task_dir.name),
                "ticker": task.get("ticker", ""),
                "trade_date": task.get("trade_date", ""),
                "provider": task.get("provider", ""),
                "status": status.get("status", "queued"),
                "created_at": task.get("created_at", ""),
                "updated_at": status.get("updated_at", ""),
            }
        )
    return tasks


def _read_events(task_dir: Path, offset: int) -> tuple[list[dict], int]:
    events_path = task_dir / "events.jsonl"
    if not events_path.exists():
        return [], offset

    events = []
    next_offset = 0
    with events_path.open("r", encoding="utf-8") as f:
        for idx, line in enumerate(f):
            if idx >= offset:
                line = line.strip()
                if not line:
                    continue
                try:
                    events.append(json.loads(line))
                except Exception:
                    events.append(
                        {
                            "ts": _now_iso(),
                            "type": "parse_error",
                            "message": "Failed to parse an event line",
                            "data": {"raw": line[:300]},
                        }
                    )
            next_offset = idx + 1
    return events, next_offset


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


def _is_pid_alive(pid: int | None) -> bool:
    if not pid:
        return False
    try:
        os.kill(int(pid), 0)
        return True
    except Exception:
        return False


def _max_attempts(task: dict) -> int:
    max_retries = int(task.get("max_retries", 2))
    if max_retries < 0:
        max_retries = 0
    return 1 + max_retries


def _retry_delay_seconds(task: dict) -> int:
    delay = int(task.get("retry_delay_seconds", 15))
    return max(delay, 0)


def _launch_worker(task_dir: Path, attempt: int) -> int:
    stdout_file = (task_dir / "worker.stdout.log").open("a", encoding="utf-8")
    stderr_file = (task_dir / "worker.stderr.log").open("a", encoding="utf-8")
    cmd = [
        sys.executable,
        "-m",
        "cli.web_worker",
        "--task-dir",
        str(task_dir),
    ]
    proc = subprocess.Popen(
        cmd,
        cwd=str(Path(__file__).resolve().parents[1]),
        stdout=stdout_file,
        stderr=stderr_file,
        start_new_session=True,
    )
    stdout_file.close()
    stderr_file.close()
    _update_status(task_dir, status="queued", pid=proc.pid, attempt=attempt)
    _append_event(
        task_dir,
        "worker_started",
        "Background worker started",
        {"pid": proc.pid, "attempt": attempt},
    )
    return proc.pid


def _schedule_retry_or_fail(task_dir: Path, task: dict, status: dict, reason: str) -> dict:
    attempt = int(status.get("attempt") or 0)
    max_attempts = _max_attempts(task)
    if attempt < max_attempts:
        delay_seconds = _retry_delay_seconds(task)
        next_retry_at = (
            datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)
        ).isoformat()
        updated = _update_status(
            task_dir,
            status="queued",
            pid=None,
            next_retry_at=next_retry_at,
            last_error=reason,
        )
        _append_event(
            task_dir,
            "retry_scheduled",
            f"Retry scheduled: attempt {attempt + 1}/{max_attempts}",
            {"reason": reason, "next_retry_at": next_retry_at, "retry_delay_seconds": delay_seconds},
        )
        return updated

    updated = _update_status(task_dir, status="failed", pid=None, next_retry_at=None, last_error=reason)
    _append_event(task_dir, "error", reason)
    return updated


def _reconcile_task(task_dir: Path) -> dict:
    task = _read_json(task_dir / "task.json", default={})
    status = _read_json(task_dir / "status.json", default={})
    if not task or not status:
        return status

    state = status.get("status", "queued")
    pid = status.get("pid")
    alive = _is_pid_alive(pid) if pid else False

    if state == "completed":
        return status

    if state == "failed":
        return status

    if state == "running" and not alive:
        return _schedule_retry_or_fail(task_dir, task, status, "Worker exited unexpectedly while running")

    if state == "queued":
        next_retry = _parse_iso(status.get("next_retry_at"))
        now = datetime.now(timezone.utc)
        due = (next_retry is None) or (next_retry <= now)
        if alive:
            return status
        if due:
            attempt = int(status.get("attempt") or 0) + 1
            _launch_worker(task_dir, attempt)
            return _read_json(task_dir / "status.json", default={})

    return status


def _reconcile_all_tasks_once() -> None:
    with TASK_CONTROL_LOCK:
        for task_dir in sorted(TASKS_DIR.glob("*")):
            if not task_dir.is_dir():
                continue
            _reconcile_task(task_dir)


def _reconcile_loop() -> None:
    while True:
        try:
            _reconcile_all_tasks_once()
        except Exception:
            pass
        time.sleep(RECONCILE_INTERVAL_SECONDS)


def _validate_form(form: dict) -> dict:
    ticker = (form.get("ticker") or "").strip().upper()
    if not ticker:
        raise ValueError("Ticker is required")

    trade_date = (form.get("trade_date") or str(date.today())).strip()
    date.fromisoformat(trade_date)

    provider = (form.get("provider") or "openai").strip().lower()
    if provider not in PROVIDERS:
        raise ValueError(f"Invalid provider: {provider}")

    rounds = int(form.get("rounds") or "1")
    if rounds < 1 or rounds > 6:
        raise ValueError("Debate rounds must be between 1 and 6")
    max_retries = int(form.get("max_retries") or "2")
    retry_delay_seconds = int(form.get("retry_delay_seconds") or "15")
    if max_retries < 0 or max_retries > 10:
        raise ValueError("max_retries must be between 0 and 10")
    if retry_delay_seconds < 0 or retry_delay_seconds > 3600:
        raise ValueError("retry_delay_seconds must be between 0 and 3600")
    export_pdf = form.get("export_pdf", "on") == "on"
    translate_to_zh = form.get("translate_to_zh", "on") == "on"

    deep_model = (form.get("deep_model") or DEFAULT_CONFIG["deep_think_llm"]).strip()
    quick_model = (form.get("quick_model") or DEFAULT_CONFIG["quick_think_llm"]).strip()
    if not deep_model or not quick_model:
        raise ValueError("Model names are required")

    analysts = []
    for a in DEFAULT_ANALYSTS:
        if form.get(f"analyst_{a}") == "on":
            analysts.append(a)
    if not analysts:
        analysts = DEFAULT_ANALYSTS[:]

    return {
        "ticker": ticker,
        "trade_date": trade_date,
        "provider": provider,
        "rounds": rounds,
        "deep_model": deep_model,
        "quick_model": quick_model,
        "analysts": analysts,
        "max_retries": max_retries,
        "retry_delay_seconds": retry_delay_seconds,
        "export_pdf": export_pdf,
        "translate_to_zh": translate_to_zh,
    }


def _spawn_task(payload: dict, user_id: int) -> str:
    task_id = f"{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"
    paths = _task_paths(task_id)
    task_dir = paths["dir"]
    task_dir.mkdir(parents=True, exist_ok=True)

    task_data = {
        "task_id": task_id,
        "created_at": _now_iso(),
        "user_id": user_id,
        **payload,
    }

    _write_json(paths["task"], task_data)
    _write_json(
        paths["status"],
        {
            "status": "queued",
            "pid": None,
            "updated_at": _now_iso(),
        },
    )
    _append_event(task_dir, "created", "Task created", {"task_id": task_id, **payload})

    _launch_worker(task_dir, attempt=1)
    return task_id


def _render_landing(user: dict | None = None, error: str = "") -> str:
    suggestions = _daily_suggestions()
    paid = False
    if user:
        plan = _get_plan(user["plan_id"])
        paid = bool(plan.get("is_paid"))
    cards = []
    for s in suggestions:
        detail_html = (
            f'<div style="margin-top:8px;color:#0f5132;">Reason: {_escape(s["details"])}</div>'
            if paid
            else '<div style="margin-top:8px;color:#8a6d3b;">Reason locked. Subscribe to view full report.</div>'
        )
        cards.append(
            f"""
            <div class="card">
              <h3 style="margin:0 0 8px;">{_escape(s["symbol"])} · {_escape(s["decision"])}</h3>
              <div>{_escape(s["summary"])}</div>
              {detail_html}
            </div>
            """
        )

    auth_html = (
        f'<a href="/task-center">Task Center</a> | <a href="/pricing">Pricing</a> | <a href="/logout">Logout ({_escape(user["username"])})</a>'
        if user
        else '<a href="/login">Login</a> | <a href="/register">Register</a> | <a href="/pricing">Pricing</a>'
    )
    error_block = f'<section class="card error"><pre>{_escape(error)}</pre></section>' if error else ""
    return f"""<!doctype html>
<html><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>TradingAgents Service</title>
<style>
body{{margin:0;font-family:ui-sans-serif,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:linear-gradient(120deg,#eef4ff,#f5fbff,#edf7ef);color:#132033;}}
.wrap{{max-width:1120px;margin:24px auto;padding:0 14px 30px;}}
.card{{background:#fff;border:1px solid #ccd5df;border-radius:12px;padding:16px;margin-bottom:14px;}}
.grid{{display:grid;gap:12px;grid-template-columns:repeat(2,minmax(0,1fr));}}
.error{{border-color:#e9b2b2;background:#fff5f5;}}
@media (max-width:900px){{.grid{{grid-template-columns:1fr;}}}}
</style></head><body><main class="wrap">
<section class="card">
<h1 style="margin:0 0 8px;">TradingAgents Advisory Platform</h1>
<p style="margin:0 0 8px;">We provide AI-driven multi-agent stock consultation, trading recommendations, and report generation.</p>
<p style="margin:0;">{auth_html}</p>
</section>
<section class="card">
<h2 style="margin-top:0;">Business</h2>
<p>Users can log in and subscribe to plans with different ticker capacity and daily query limits, then use Task Center to generate stock consultation reports.</p>
<p>Daily stock suggestions are public. Full reasoning and full reports are available for paid subscribers.</p>
</section>
{error_block}
<section><h2>Today's Suggestions</h2></section>
{''.join(cards) if cards else '<div class="card">No suggestions today.</div>'}
</main></body></html>"""


def _render_login(error: str = "") -> str:
    err = f'<p style="color:#8a1c1c;">{_escape(error)}</p>' if error else ""
    return f"""<!doctype html><html><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width, initial-scale=1"/><title>Login</title></head>
<body style="font-family:ui-sans-serif;padding:24px;"><h2>Login</h2>{err}
<form method="post" action="/login">
<p><label>Username <input name="username" required></label></p>
<p><label>Password <input type="password" name="password" required></label></p>
<p><button type="submit">Login</button></p>
</form><p><a href="/register">Register</a> | <a href="/">Home</a></p></body></html>"""


def _render_register(error: str = "") -> str:
    err = f'<p style="color:#8a1c1c;">{_escape(error)}</p>' if error else ""
    return f"""<!doctype html><html><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width, initial-scale=1"/><title>Register</title></head>
<body style="font-family:ui-sans-serif;padding:24px;"><h2>Register</h2>{err}
<form method="post" action="/register">
<p><label>Username <input name="username" required></label></p>
<p><label>Password <input type="password" name="password" required></label></p>
<p><button type="submit">Create Account</button></p>
</form><p><a href="/login">Login</a> | <a href="/">Home</a></p></body></html>"""


def _render_pricing(user: dict | None = None, error: str = "") -> str:
    conn = _db_connect()
    plans = [dict(r) for r in conn.execute("SELECT * FROM plans ORDER BY price_usd ASC").fetchall()]
    conn.close()
    cards = []
    current = user["plan_id"] if user else None
    for p in plans:
        btn = "<a href='/login'>Login to Subscribe</a>"
        if user:
            if current == p["plan_id"]:
                btn = "<strong>Current Plan</strong>"
            else:
                btn = (
                    f"<form method='post' action='/subscribe'>"
                    f"<input type='hidden' name='plan_id' value='{_escape(p['plan_id'])}'/>"
                    f"<button type='submit'>Subscribe</button></form>"
                )
        cards.append(
            f"<div class='card'><h3>{_escape(p['name'])} (${p['price_usd']}/mo)</h3>"
            f"<p>Tickers: {p['ticker_limit']}</p><p>Daily queries: {p['daily_query_limit']}</p>{btn}</div>"
        )
    err = f"<div class='card' style='border-color:#e9b2b2;color:#8a1c1c;'>{_escape(error)}</div>" if error else ""
    return f"""<!doctype html><html><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Pricing</title><style>.card{{border:1px solid #ccd5df;border-radius:10px;padding:12px;margin:12px 0;}}</style></head>
<body style="font-family:ui-sans-serif;padding:24px;"><h2>Pricing</h2><p><a href="/">Home</a> | <a href="/task-center">Task Center</a></p>{err}{''.join(cards)}</body></html>"""


def _render_dashboard(user: dict, error: str = "") -> str:
    stats = _usage_stats(user)
    tasks = _list_tasks(user_id=int(user["id"]))

    rows = []
    for t in tasks:
        rows.append(
            """
            <tr>
              <td>{task_id}</td>
              <td>{ticker}</td>
              <td>{trade_date}</td>
              <td>{provider}</td>
              <td><span class=\"status status-{status}\">{status}</span></td>
              <td>{updated_at}</td>
              <td><a href=\"/task?id={task_id}\">View</a></td>
            </tr>
            """.format(
                task_id=_escape(t["task_id"]),
                ticker=_escape(t["ticker"]),
                trade_date=_escape(t["trade_date"]),
                provider=_escape(t["provider"]),
                status=_escape(t["status"]),
                updated_at=_escape(t["updated_at"]),
            )
        )

    task_table = (
        """
        <section class="card">
          <h2>Tasks</h2>
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Task ID</th><th>Ticker</th><th>Date</th><th>Provider</th><th>Status</th><th>Updated</th><th>Action</th>
                </tr>
              </thead>
              <tbody>{rows}</tbody>
            </table>
          </div>
        </section>
        """.format(rows="".join(rows) if rows else '<tr><td colspan="7">No tasks yet</td></tr>')
    )

    provider_options = "".join(f'<option value="{p}">{p}</option>' for p in PROVIDERS)

    analyst_checks = "".join(
        f'<label><input type="checkbox" name="analyst_{a}" checked /> {a}</label>'
        for a in DEFAULT_ANALYSTS
    )

    error_block = f'<section class="card error"><h2>Error</h2><pre>{_escape(error)}</pre></section>' if error else ""

    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <meta http-equiv="refresh" content="20" />
    <title>TradingAgents Task Center</title>
    <style>
      :root {{ --fg:#0f1d2e; --line:#ccd5df; --card:#fff; --primary:#0b6bcb; --bg:#eef4f8; --ok:#0f766e; --run:#7c5b00; --fail:#8a1c1c; }}
      * {{ box-sizing:border-box; }}
      body {{ margin:0; font-family: ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; color:var(--fg); background: radial-gradient(circle at 0% 0%, #e5f0ff, #f7fbff 45%, #f0f7f1); }}
      .wrap {{ max-width:1120px; margin:24px auto; padding:0 14px 24px; }}
      h1 {{ margin:0 0 14px; }}
      .card {{ background:var(--card); border:1px solid var(--line); border-radius:12px; padding:16px; margin-bottom:14px; box-shadow: 0 8px 20px rgba(14, 24, 34, 0.06); }}
      .grid {{ display:grid; gap:12px; grid-template-columns: repeat(3,minmax(0,1fr)); }}
      label {{ display:block; font-size:13px; font-weight:700; margin-bottom:5px; }}
      input, select {{ width:100%; border:1px solid var(--line); border-radius:8px; padding:10px 12px; font-size:14px; background:#fff; }}
      .checks {{ display:flex; gap:12px; flex-wrap: wrap; }}
      .checks label {{ font-weight:600; margin:0; }}
      button {{ border:0; background:var(--primary); color:#fff; border-radius:10px; padding:10px 14px; font-weight:700; cursor:pointer; }}
      .table-wrap {{ overflow:auto; }}
      table {{ border-collapse: collapse; width:100%; min-width:900px; }}
      th, td {{ border-bottom:1px solid #e8edf2; text-align:left; padding:10px 8px; font-size:13px; }}
      .status {{ padding:3px 8px; border-radius:999px; font-weight:700; font-size:12px; }}
      .status-completed {{ background:#d5f5ef; color:var(--ok); }}
      .status-running {{ background:#fff2c7; color:var(--run); }}
      .status-queued {{ background:#e9eef4; color:#334155; }}
      .status-failed {{ background:#fde2e2; color:var(--fail); }}
      .error {{ border-color:#e9b2b2; }}
      .error h2 {{ color:var(--fail); }}
      @media (max-width: 900px) {{ .grid {{ grid-template-columns:1fr; }} }}
    </style>
  </head>
  <body>
    <main class="wrap">
      <h1>TradingAgents Task Center</h1>
      <section class="card">
        <p style="margin:0 0 6px;"><strong>User:</strong> {_escape(user['username'])}</p>
        <p style="margin:0 0 6px;"><strong>Plan:</strong> {_escape(stats['plan']['name'])} ({stats['plan']['plan_id']})</p>
        <p style="margin:0 0 6px;"><strong>Daily Queries:</strong> {stats['used_queries']} / {stats['plan']['daily_query_limit']}</p>
        <p style="margin:0 0 6px;"><strong>Subscribed Tickers:</strong> {stats['ticker_count']} / {stats['plan']['ticker_limit']}</p>
        <p style="margin:0;"><a href="/">Home</a> | <a href="/pricing">Pricing</a> | <a href="/logout">Logout</a></p>
      </section>
      <section class="card">
        <h2>New Task</h2>
        <form method="post" action="/api/tasks">
          <div class="grid">
            <div>
              <label for="ticker">Ticker</label>
              <input id="ticker" name="ticker" value="NVDA" required />
            </div>
            <div>
              <label for="trade_date">Trade Date (YYYY-MM-DD)</label>
              <input id="trade_date" name="trade_date" type="date" value="{_escape(str(date.today()))}" required />
            </div>
            <div>
              <label for="provider">LLM Provider</label>
              <select id="provider" name="provider">{provider_options}</select>
            </div>
            <div>
              <label for="rounds">Debate Rounds</label>
              <input id="rounds" name="rounds" type="number" min="1" max="6" value="1" />
            </div>
            <div>
              <label for="deep_model">Deep Think Model</label>
              <input id="deep_model" name="deep_model" value="{_escape(DEFAULT_CONFIG['deep_think_llm'])}" required />
            </div>
            <div>
              <label for="quick_model">Quick Think Model</label>
              <input id="quick_model" name="quick_model" value="{_escape(DEFAULT_CONFIG['quick_think_llm'])}" required />
            </div>
            <div>
              <label for="max_retries">Max Retries</label>
              <input id="max_retries" name="max_retries" type="number" min="0" max="10" value="2" />
            </div>
            <div>
              <label for="retry_delay_seconds">Retry Delay (sec)</label>
              <input id="retry_delay_seconds" name="retry_delay_seconds" type="number" min="0" max="3600" value="15" />
            </div>
          </div>
          <div style="margin-top:10px;">
            <label>Analysts</label>
            <div class="checks">{analyst_checks}</div>
          </div>
          <div style="margin-top:10px;">
            <label>Output Options</label>
            <div class="checks">
              <label><input type="checkbox" name="translate_to_zh" checked /> Translate to Chinese</label>
              <label><input type="checkbox" name="export_pdf" checked /> Export PDF</label>
            </div>
          </div>
          <div style="margin-top:14px;"><button type="submit">Create Task</button></div>
        </form>
      </section>
      {error_block}
      {task_table}
    </main>
  </body>
</html>"""


def _render_task_page(task_id: str) -> str:
    paths = _task_paths(task_id)
    task = _read_json(paths["task"], default={})
    if not task:
        return "<h1>Task Not Found</h1>"

    status = _read_json(paths["status"], default={})
    result = _read_json(paths["result"], default={})

    result_html = ""
    if result:
        artifacts = result.get("artifacts") or {}
        artifact_rows = []
        for group, files in artifacts.items():
            if not files:
                continue
            for rel in files:
                rel_q = quote(rel, safe="")
                task_q = quote(task_id, safe="")
                open_url = f"/api/tasks/{task_q}/artifact?path={rel_q}"
                download_url = f"/api/tasks/{task_q}/artifact?path={rel_q}&download=1"
                artifact_rows.append(
                    f"<tr><td>{_escape(group)}</td><td>{_escape(rel)}</td>"
                    f"<td><a href=\"{_escape(open_url)}\" target=\"_blank\">Open</a> | "
                    f"<a href=\"{_escape(download_url)}\">Download</a></td></tr>"
                )
        artifacts_block = (
            "<h3>Artifacts</h3>"
            "<div class=\"table-wrap\"><table><thead><tr><th>Group</th><th>File</th><th>Action</th></tr></thead>"
            f"<tbody>{''.join(artifact_rows) if artifact_rows else '<tr><td colspan=\"3\">None</td></tr>'}</tbody></table></div>"
        )
        result_html = f"""
        <section class=\"card\">
          <h2>Result</h2>
          <p><strong>Processed Decision:</strong> {_escape(result.get('decision', ''))}</p>
          <p><strong>Operation Decision:</strong> {_escape(result.get('operation_decision', ''))} ({_escape(result.get('operation_text_zh', ''))})</p>
          <h3>Final Trade Decision</h3>
          <pre>{_escape(result.get('final_trade_decision', ''))}</pre>
          {artifacts_block}
        </section>
        """

    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Task { _escape(task_id) }</title>
    <style>
      :root {{ --fg:#0f1d2e; --line:#ccd5df; --card:#fff; --bg:#eef4f8; --primary:#0b6bcb; }}
      * {{ box-sizing:border-box; }}
      body {{ margin:0; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; background:linear-gradient(120deg,#eef4ff,#f5fbff,#edf7ef); color:var(--fg); }}
      .wrap {{ max-width:1120px; margin:20px auto; padding:0 14px 20px; }}
      .card {{ background:var(--card); border:1px solid var(--line); border-radius:12px; padding:14px; margin-bottom:12px; }}
      pre {{ background:#0b1220; color:#d2e4ff; border-radius:10px; padding:12px; min-height:280px; white-space:pre-wrap; word-break:break-word; overflow:auto; }}
      .meta p {{ margin:5px 0; font-size:14px; }}
      .table-wrap {{ overflow:auto; }}
      table {{ border-collapse: collapse; width:100%; min-width:680px; }}
      th, td {{ border-bottom:1px solid #e8edf2; text-align:left; padding:8px 6px; font-size:13px; }}
      a {{ color:#0b6bcb; }}
    </style>
  </head>
  <body>
    <main class="wrap">
      <div class="card">
        <a href="/task-center">Back to Tasks</a>
        <h2>Task {_escape(task_id)}</h2>
        <div class="meta">
          <p><strong>Ticker:</strong> {_escape(task.get('ticker', ''))}</p>
          <p><strong>Trade Date:</strong> {_escape(task.get('trade_date', ''))}</p>
          <p><strong>Status:</strong> <span id="status">{_escape(status.get('status', 'unknown'))}</span></p>
          <p><strong>PID:</strong> <span id="pid">{_escape(str(status.get('pid', '')))}</span></p>
          <p><strong>Attempt:</strong> <span id="attempt">{_escape(str(status.get('attempt', '')))}</span></p>
          <p><strong>Last Error:</strong> <span id="last_error">{_escape(str(status.get('last_error', '')))}</span></p>
        </div>
      </div>
      <section class="card">
        <h3>Live Progress</h3>
        <pre id="log">Waiting for task events...</pre>
      </section>
      {result_html}
    </main>
    <script>
      const taskId = {json.dumps(task_id)};
      let offset = 0;
      let bootstrapped = false;

      function lineForEvent(ev) {{
        const ts = ev.ts || "";
        const typ = ev.type || "event";
        const msg = ev.message || "";
        if (typ === "stdout" || typ === "stderr") {{
          return msg;
        }}
        return `[${{ts}}] ${{typ}} | ${{msg}}`;
      }}

      async function poll() {{
        try {{
          const evResp = await fetch(`/api/tasks/${{taskId}}/events?offset=${{offset}}`, {{ cache: "no-store" }});
          if (evResp.ok) {{
            const evData = await evResp.json();
            const log = document.getElementById("log");
            if ((evData.events || []).length > 0 && log.textContent === "Waiting for task events...") {{
              log.textContent = "";
            }}
            for (const ev of evData.events || []) {{
              const text = lineForEvent(ev);
              log.textContent += (log.textContent ? "\\n" : "") + text;
              if (ev.data && ev.data.content) {{
                log.textContent += "\\n" + ev.data.content + "\\n";
              }}
            }}
            offset = evData.next_offset || offset;
            if (!bootstrapped) {{
              log.scrollTop = log.scrollHeight;
              bootstrapped = true;
            }} else {{
              log.scrollTop = log.scrollHeight;
            }}
          }}

          const stResp = await fetch(`/api/tasks/${{taskId}}`, {{ cache: "no-store" }});
          if (stResp.ok) {{
            const st = await stResp.json();
            document.getElementById("status").textContent = st.status || "unknown";
            document.getElementById("pid").textContent = st.pid ?? "";
            document.getElementById("attempt").textContent = st.attempt ?? "";
            document.getElementById("last_error").textContent = st.last_error ?? "";
            if (st.status === "completed" || st.status === "failed") {{
              // keep polling slowly for final events
              setTimeout(poll, 5000);
              return;
            }}
          }}
        }} catch (_) {{
          // keep retrying
        }}
        setTimeout(poll, 1500);
      }}

      poll();
    </script>
  </body>
</html>"""


class TradingAgentsWebHandler(BaseHTTPRequestHandler):
    def _send_html(self, body: str, status: int = HTTPStatus.OK) -> None:
        body_bytes = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body_bytes)))
        self.end_headers()
        self.wfile.write(body_bytes)

    def _send_json(self, payload: dict, status: int = HTTPStatus.OK) -> None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _redirect(self, location: str) -> None:
        self.send_response(HTTPStatus.SEE_OTHER)
        self.send_header("Location", location)
        self.end_headers()

    def _set_session_cookie(self, session_id: str) -> None:
        self.send_header("Set-Cookie", f"{SESSION_COOKIE}={session_id}; Path=/; HttpOnly; SameSite=Lax")

    def _clear_session_cookie(self) -> None:
        self.send_header(
            "Set-Cookie",
            f"{SESSION_COOKIE}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0",
        )

    def _current_user(self) -> dict | None:
        cookies = _parse_cookies(self.headers.get("Cookie"))
        sid = cookies.get(SESSION_COOKIE)
        return _current_user_from_session(sid)

    def _redirect_with_cookie(self, location: str, session_id: str | None = None, clear: bool = False) -> None:
        self.send_response(HTTPStatus.SEE_OTHER)
        self.send_header("Location", location)
        if session_id:
            self._set_session_cookie(session_id)
        if clear:
            self._clear_session_cookie()
        self.end_headers()

    def _send_file(self, file_path: Path, download: bool = False) -> None:
        if not file_path.exists() or not file_path.is_file():
            self._send_json(
                {"error": "file not found", "path": str(file_path)},
                status=HTTPStatus.NOT_FOUND,
            )
            return
        data = file_path.read_bytes()
        content_type = mimetypes.guess_type(str(file_path))[0] or "application/octet-stream"
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        disposition = "attachment" if download else "inline"
        self.send_header("Content-Disposition", f'{disposition}; filename="{file_path.name}"')
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        user = self._current_user()

        if path == "/artifact":
            q = parse_qs(parsed.query)
            task_id = (q.get("task_id") or [""])[0]
            rel_path = (q.get("path") or [""])[0]
            download = (q.get("download") or ["0"])[0] == "1"
            if not task_id or not rel_path:
                self._send_json({"error": "task_id and path are required"}, status=HTTPStatus.BAD_REQUEST)
                return
            paths = _task_paths(task_id)
            if not paths["dir"].exists():
                self._send_json({"error": "task not found"}, status=HTTPStatus.NOT_FOUND)
                return
            task = _read_json(paths["task"], default={})
            if not user or task.get("user_id") != int(user["id"]):
                self._send_json({"error": "forbidden"}, status=HTTPStatus.FORBIDDEN)
                return
            candidate = (paths["dir"] / rel_path).resolve()
            try:
                candidate.relative_to(paths["dir"].resolve())
            except Exception:
                self._send_json({"error": "invalid path"}, status=HTTPStatus.BAD_REQUEST)
                return
            self._send_file(candidate, download=download)
            return

        if path == "/":
            self._send_html(_render_landing(user=user))
            return

        if path == "/login":
            if user:
                self._redirect("/task-center")
                return
            self._send_html(_render_login())
            return

        if path == "/register":
            if user:
                self._redirect("/task-center")
                return
            self._send_html(_render_register())
            return

        if path == "/pricing":
            self._send_html(_render_pricing(user=user))
            return

        if path == "/logout":
            cookies = _parse_cookies(self.headers.get("Cookie"))
            sid = cookies.get(SESSION_COOKIE)
            if sid:
                _delete_session(sid)
            self._redirect_with_cookie("/", clear=True)
            return

        if path == "/task-center":
            if not user:
                self._redirect("/login")
                return
            self._send_html(_render_dashboard(user=user))
            return

        if path == "/task":
            if not user:
                self._redirect("/login")
                return
            q = parse_qs(parsed.query)
            task_id = (q.get("id") or [""])[0]
            if not task_id:
                self._send_html("<h1>Task ID is required</h1>", status=HTTPStatus.BAD_REQUEST)
                return
            task = _read_json(_task_paths(task_id)["task"], default={})
            if task.get("user_id") != int(user["id"]):
                self._send_html("<h1>Forbidden</h1>", status=HTTPStatus.FORBIDDEN)
                return
            self._send_html(_render_task_page(task_id))
            return

        segments = path.strip("/").split("/")
        if segments[:2] == ["api", "tasks"]:
            if not user:
                self._send_json({"error": "unauthorized"}, status=HTTPStatus.UNAUTHORIZED)
                return
            if len(segments) == 2:
                self._send_json({"tasks": _list_tasks(user_id=int(user["id"]))})
                return

            task_id = segments[2]
            paths = _task_paths(task_id)
            if not paths["dir"].exists():
                self._send_json({"error": "Task not found"}, status=HTTPStatus.NOT_FOUND)
                return
            task = _read_json(paths["task"], default={})
            if task.get("user_id") != int(user["id"]):
                self._send_json({"error": "forbidden"}, status=HTTPStatus.FORBIDDEN)
                return

            if len(segments) == 3:
                with TASK_CONTROL_LOCK:
                    status = _reconcile_task(paths["dir"])
                result = _read_json(paths["result"], default={})
                self._send_json({**status, "result": result, "task_id": task_id})
                return

            if len(segments) == 4 and segments[3] == "events":
                q = parse_qs(parsed.query)
                try:
                    offset = int((q.get("offset") or ["0"])[0])
                except ValueError:
                    offset = 0
                events, next_offset = _read_events(paths["dir"], max(offset, 0))
                self._send_json({"task_id": task_id, "events": events, "next_offset": next_offset})
                return

            if len(segments) >= 4 and segments[3] == "artifact":
                q = parse_qs(parsed.query)
                rel_path = (q.get("path") or [""])[0]
                download = (q.get("download") or ["0"])[0] == "1"
                if not rel_path:
                    self._send_json({"error": "path is required"}, status=HTTPStatus.BAD_REQUEST)
                    return
                candidate = (paths["dir"] / rel_path).resolve()
                try:
                    candidate.relative_to(paths["dir"].resolve())
                except Exception:
                    self._send_json({"error": "invalid path"}, status=HTTPStatus.BAD_REQUEST)
                    return
                self._send_file(candidate, download=download)
                return

        self._send_html("<h1>Not Found</h1>", status=HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        user = self._current_user()

        if path == "/register":
            content_length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(content_length).decode("utf-8")
            form = {k: v[0] for k, v in parse_qs(raw).items()}
            ok, msg = _create_user((form.get("username") or "").strip(), (form.get("password") or "").strip())
            if not ok:
                self._send_html(_render_register(error=msg), status=HTTPStatus.BAD_REQUEST)
                return
            authed = _authenticate_user((form.get("username") or "").strip(), (form.get("password") or "").strip())
            sid = _create_session(int(authed["id"]))
            self._redirect_with_cookie("/task-center", session_id=sid)
            return

        if path == "/login":
            content_length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(content_length).decode("utf-8")
            form = {k: v[0] for k, v in parse_qs(raw).items()}
            authed = _authenticate_user((form.get("username") or "").strip(), (form.get("password") or "").strip())
            if not authed:
                self._send_html(_render_login(error="Invalid username or password"), status=HTTPStatus.UNAUTHORIZED)
                return
            sid = _create_session(int(authed["id"]))
            self._redirect_with_cookie("/task-center", session_id=sid)
            return

        if path == "/subscribe":
            if not user:
                self._redirect("/login")
                return
            content_length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(content_length).decode("utf-8")
            form = {k: v[0] for k, v in parse_qs(raw).items()}
            plan_id = (form.get("plan_id") or "").strip()
            conn = _db_connect()
            row = conn.execute("SELECT 1 FROM plans WHERE plan_id = ?", (plan_id,)).fetchone()
            if row:
                conn.execute("UPDATE users SET plan_id = ? WHERE id = ?", (plan_id, int(user["id"])))
                conn.commit()
            conn.close()
            self._redirect("/pricing")
            return

        if path != "/api/tasks":
            self._send_html("<h1>Not Found</h1>", status=HTTPStatus.NOT_FOUND)
            return
        if not user:
            self._send_json({"error": "unauthorized"}, status=HTTPStatus.UNAUTHORIZED)
            return

        content_length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(content_length).decode("utf-8")
        form = {k: v[0] for k, v in parse_qs(raw).items()}

        try:
            payload = _validate_form(form)
            allowed, reason = _can_submit_task(user, payload["ticker"])
            if not allowed:
                self._send_html(_render_dashboard(user=user, error=reason), status=HTTPStatus.BAD_REQUEST)
                return
            with TASK_CONTROL_LOCK:
                _record_task_usage(user, payload["ticker"])
                task_id = _spawn_task(payload, user_id=int(user["id"]))
            self._redirect(f"/task?id={task_id}")
        except Exception as exc:
            self._send_html(_render_dashboard(user=user, error=str(exc)), status=HTTPStatus.BAD_REQUEST)


def main() -> None:
    _init_db()
    t = threading.Thread(target=_reconcile_loop, daemon=True)
    t.start()
    server = ThreadingHTTPServer((HOST, PORT), TradingAgentsWebHandler)
    print(f"TradingAgents Web UI running at http://{HOST}:{PORT}")
    print(f"Task store: {TASKS_DIR}")
    print(f"Reconcile interval: {RECONCILE_INTERVAL_SECONDS}s")
    server.serve_forever()


if __name__ == "__main__":
    main()
