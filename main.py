"""
╔══════════════════════════════════════════════════════════════════════════════╗
║   ENERGY DETECTOR — WEBHOOK SERVER                                          ║
║   Receives TradingView alerts, logs signals, sends Telegram notifications   ║
║   Tracks D+1 performance and generates weekly optimization reports          ║
║   Extended with MARKET LAB research endpoint                                ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import os
import json
import sqlite3
import logging
import asyncio
import httpx
import yfinance as yf
from datetime import datetime, timedelta
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse, HTMLResponse
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
WEBHOOK_SECRET   = os.getenv("WEBHOOK_SECRET", "energy_detector_2026")
DB_PATH          = os.getenv("DB_PATH", "data/signals.db")

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger(__name__)

app = FastAPI(title="Energy Detector Webhook", version="1.1")
scheduler = AsyncIOScheduler()

def to_float(value):
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None

def init_db():
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            received_at   TEXT NOT NULL,
            ticker        TEXT NOT NULL,
            exchange      TEXT,
            interval      TEXT,
            price_alert   REAL,
            score         REAL,
            signal_type   TEXT,
            tv_time       TEXT,
            price_close   REAL,
            price_d1_open REAL,
            price_d1_close REAL,
            gain_at_close REAL,
            gain_d1_close REAL,
            d1_checked    INTEGER DEFAULT 0,
            notes         TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS marketlab (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lab_id TEXT,
            received_at TEXT NOT NULL,
            ticker TEXT NOT NULL,
            exchange TEXT,
            interval TEXT,
            tv_time TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            g1 REAL,
            g2 REAL,
            g3 REAL,
            g23_clean_first REAL,
            relvol REAL,
            compression REAL,
            absorption REAL,
            shockgate REAL,
            break20 REAL,
            strongclose REAL,
            quality REAL,
            lumira_now REAL,
            lumira_recent3 REAL,
            lumira_recent5 REAL,
            lumira_before_g23_5 REAL,
            g23_before_lumira_5 REAL,
            lumira_same_g23 REAL,
            score REAL,
            notes TEXT
        )
    """)

    conn.commit()
    conn.close()
    log.info("Database initialized: %s", DB_PATH)

def insert_signal(data: dict) -> int:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        INSERT INTO signals (received_at, ticker, exchange, interval, price_alert, score, signal_type, tv_time)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        datetime.utcnow().isoformat(),
        data.get("ticker", "UNKNOWN"),
        data.get("exchange", ""),
        data.get("interval", ""),
        data.get("price"),
        data.get("score"),
        data.get("signal", "PRIME_ENERGY"),
        data.get("time", ""),
    ))
    signal_id = c.lastrowid
    conn.commit()
    conn.close()
    return signal_id

def insert_marketlab(data: dict) -> int:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    received_at = datetime.utcnow().isoformat()
    ticker = data.get("ticker", "UNKNOWN").upper()

    c.execute("""
        INSERT INTO marketlab (
            received_at, ticker, exchange, interval, tv_time,
            open, high, low, close, volume,
            g1, g2, g3, g23_clean_first, relvol,
            compression, absorption, shockgate, break20, strongclose, quality,
            lumira_now, lumira_recent3, lumira_recent5,
            lumira_before_g23_5, g23_before_lumira_5, lumira_same_g23,
            score
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        received_at,
        ticker,
        data.get("exchange", ""),
        data.get("interval", ""),
        data.get("time", ""),
        to_float(data.get("open")),
        to_float(data.get("high")),
        to_float(data.get("low")),
        to_float(data.get("close")),
        to_float(data.get("volume")),
        to_float(data.get("G1")),
        to_float(data.get("G2")),
        to_float(data.get("G3")),
        to_float(data.get("G23CleanFirst")),
        to_float(data.get("RelVol")),
        to_float(data.get("Compression")),
        to_float(data.get("Absorption")),
        to_float(data.get("ShockGate")),
        to_float(data.get("Break20")),
        to_float(data.get("StrongClose")),
        to_float(data.get("Quality")),
        to_float(data.get("LumiraNow")),
        to_float(data.get("LumiraRecent3")),
        to_float(data.get("LumiraRecent5")),
        to_float(data.get("LumiraBeforeG23_5")),
        to_float(data.get("G23BeforeLumira_5")),
        to_float(data.get("LumiraSameG23")),
        to_float(data.get("Score")),
    ))

    signal_id = c.lastrowid
    lab_id = f"LAB-{signal_id:06d}"
    c.execute("UPDATE marketlab SET lab_id=? WHERE id=?", (lab_id, signal_id))

    conn.commit()
    conn.close()
    return signal_id

def get_pending_d1_signals():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    cutoff = (datetime.utcnow() - timedelta(hours=20)).isoformat()
    c.execute("""
        SELECT id, ticker, price_alert, received_at
        FROM signals
        WHERE d1_checked = 0 AND received_at < ?
    """, (cutoff,))
    rows = c.fetchall()
    conn.close()
    return rows

def update_d1_performance(signal_id: int, price_d1_open: float, price_d1_close: float, price_alert: float):
    gain_d1 = ((price_d1_close - price_alert) / price_alert * 100) if price_alert else None
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        UPDATE signals
        SET price_d1_open=?, price_d1_close=?, gain_d1_close=?, d1_checked=1
        WHERE id=?
    """, (price_d1_open, price_d1_close, gain_d1, signal_id))
    conn.commit()
    conn.close()

def get_weekly_stats():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    week_ago = (datetime.utcnow() - timedelta(days=7)).isoformat()
    c.execute("""
        SELECT ticker, score, price_alert, gain_d1_close, signal_type, received_at
        FROM signals
        WHERE received_at > ? AND d1_checked = 1
        ORDER BY received_at DESC
    """, (week_ago,))
    rows = c.fetchall()
    conn.close()
    return rows

def get_all_signals(limit=50):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        SELECT id, received_at, ticker, interval, price_alert, score, gain_d1_close, d1_checked
        FROM signals ORDER BY received_at DESC LIMIT ?
    """, (limit,))
    rows = c.fetchall()
    conn.close()
    return rows

async def send_telegram(message: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram not configured — skipping notification")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(url, json=payload, timeout=10)
            if resp.status_code == 200:
                log.info("Telegram sent: %s", message[:60])
            else:
                log.error("Telegram failed: %s", resp.text)
    except Exception as e:
        log.error("Telegram error: %s", e)

async def check_d1_performance():
    log.info("Running D+1 performance check...")
    pending = get_pending_d1_signals()
    if not pending:
        log.info("No pending D+1 signals.")
        return

    checked = 0
    for signal_id, ticker, price_alert, received_at in pending:
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="5d", auto_adjust=True)
            if hist.empty or len(hist) < 1:
                continue
            latest = hist.iloc[-1]
            d1_open = float(latest["Open"])
            d1_close = float(latest["Close"])
            update_d1_performance(signal_id, d1_open, d1_close, price_alert)
            checked += 1
        except Exception as e:
            log.error("D+1 check failed for %s: %s", ticker, e)

    if checked > 0:
        await send_telegram(f"<b>D+1 Performance Check Complete</b>\n{checked} signals updated.\nUse /report for weekly summary.")

async def send_weekly_report():
    log.info("Generating weekly performance report...")
    rows = get_weekly_stats()
    if not rows:
        await send_telegram("<b>Weekly Report</b>\nNo completed signals this week.")
        return

    total = len(rows)
    profitable = sum(1 for r in rows if r[3] and r[3] > 0)
    losing = sum(1 for r in rows if r[3] and r[3] <= 0)
    gains = [r[3] for r in rows if r[3] is not None]
    avg_gain = sum(gains) / len(gains) if gains else 0
    best = max(rows, key=lambda r: r[3] if r[3] else -999)
    worst = min(rows, key=lambda r: r[3] if r[3] else 999)
    hit_rate = (profitable / total * 100) if total > 0 else 0

    msg = f"""<b>ENERGY DETECTOR — WEEKLY REPORT</b>
Week ending: {datetime.utcnow().strftime('%Y-%m-%d')}

<b>Total Signals:</b> {total}
<b>Profitable:</b> {profitable} ({hit_rate:.1f}% hit rate)
<b>Losing:</b> {losing}
<b>Avg D+1 Gain:</b> {avg_gain:+.2f}%

<b>Best Signal:</b> {best[0]} | Score {best[1]} | {best[3]:+.1f}%
<b>Worst Signal:</b> {worst[0]} | Score {worst[1]} | {worst[3]:+.1f}%

<b>Recent Signals (last 5):</b>"""

    for row in rows[:5]:
        ticker, score, price, gain, sig_type, ts = row
        gain_str = f"{gain:+.1f}%" if gain is not None else "pending"
        msg += f"\n• {ticker} | Score {score} | ${price:.2f} | {gain_str}"

    msg += "\n\n<i>Send /optimize for script improvement suggestions</i>"
    await send_telegram(msg)

def analyze_for_optimization():
    rows = get_weekly_stats()
    if len(rows) < 5:
        return "Not enough data yet (need at least 5 completed signals)."

    gains = [(r[0], r[1], r[3]) for r in rows if r[3] is not None]
    winners = [g for g in gains if g[2] > 0]
    losers = [g for g in gains if g[2] <= 0]

    avg_winner_score = sum(g[1] for g in winners) / len(winners) if winners else 0
    avg_loser_score = sum(g[1] for g in losers) / len(losers) if losers else 0

    suggestions = []

    if avg_winner_score > avg_loser_score + 5:
        suggestions.append(f"Raise Prime Energy threshold from 75 to {int(avg_winner_score) - 2} — winners averaged score {avg_winner_score:.0f} vs losers {avg_loser_score:.0f}")

    if len(losers) > len(winners):
        suggestions.append("Hit rate below 50% — consider adding minimum P7 (Institutional) >= 4 as a required condition")

    if not suggestions:
        suggestions.append("Performance looks solid — no major optimizations needed yet. Keep collecting data.")

    return "\n".join(f"• {s}" for s in suggestions)

@app.on_event("startup")
async def startup():
    init_db()
    scheduler.add_job(check_d1_performance, CronTrigger(hour=21, minute=30))
    scheduler.add_job(send_weekly_report, CronTrigger(day_of_week="sun", hour=18))
    scheduler.start()
    log.info("Energy Detector Webhook Server started.")
    await send_telegram("<b>Energy Detector Webhook Server is ONLINE</b>\nReady to receive TradingView alerts.")

@app.on_event("shutdown")
async def shutdown():
    scheduler.shutdown()

@app.post("/webhook")
async def receive_webhook(request: Request, background_tasks: BackgroundTasks):
    secret = request.headers.get("X-Webhook-Secret", "")
    if secret != WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="Invalid webhook secret")

    try:
        body = await request.body()
        data = json.loads(body)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    ticker = data.get("ticker", "UNKNOWN").upper()
    price = data.get("price")
    score = data.get("score")
    interval = data.get("interval", "")
    exchange = data.get("exchange", "")
    tv_time = data.get("time", "")

    log.info("ALERT RECEIVED: %s | Score: %s | Price: %s | Interval: %s", ticker, score, price, interval)

    signal_id = insert_signal(data)

    score_bar = "█" * int((score or 0) / 10) + "░" * (10 - int((score or 0) / 10))
    tg_msg = f"""<b>PRIME ENERGY ALERT</b>
<b>Ticker:</b> {ticker} ({exchange})
<b>Score:</b> {score}/100  [{score_bar}]
<b>Price:</b> ${price}
<b>Interval:</b> {interval}
<b>Time:</b> {tv_time}
<b>Signal ID:</b> #{signal_id}

<i>D+1 performance will be tracked automatically.</i>"""

    background_tasks.add_task(send_telegram, tg_msg)

    return JSONResponse({"status": "ok", "signal_id": signal_id, "ticker": ticker})

@app.post("/marketlab")
async def receive_marketlab(request: Request, background_tasks: BackgroundTasks):
    try:
        body = await request.body()
        data = json.loads(body)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    signal_id = insert_marketlab(data)
    lab_id = f"LAB-{signal_id:06d}"

    ticker = data.get("ticker", "UNKNOWN").upper()
    close = data.get("close", "")
    interval = data.get("interval", "")
    score = data.get("Score", "")
    relvol = data.get("RelVol", "")
    lumira5 = data.get("LumiraRecent5", "")
    quality = data.get("Quality", "")

    log.info("MARKET LAB RECEIVED: %s | %s | Score: %s | RelVol: %s", ticker, lab_id, score, relvol)

    tg_msg = f"""<b>MARKET LAB ALERT</b>
<b>ID:</b> {lab_id}
<b>Ticker:</b> {ticker}
<b>Close:</b> {close}
<b>Interval:</b> {interval}
<b>Score:</b> {score}
<b>RelVol:</b> {relvol}
<b>LumiraRecent5:</b> {lumira5}
<b>Quality:</b> {quality}

<i>Research record saved.</i>"""

    background_tasks.add_task(send_telegram, tg_msg)

    return JSONResponse({"status": "ok", "lab_id": lab_id, "ticker": ticker})

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    signals = get_all_signals(50)
    rows_html = ""
    for s in signals:
        sid, ts, ticker, interval, price, score, gain, checked = s
        gain_str = f"{gain:+.1f}%" if gain is not None else "pending"
        gain_color = "green" if (gain and gain > 0) else ("red" if (gain and gain <= 0) else "gray")
        checked_str = "Yes" if checked else "No"
        rows_html += f"""<tr>
            <td>#{sid}</td><td>{ts[:16]}</td><td><b>{ticker}</b></td>
            <td>{interval}</td><td>${price}</td><td>{score}</td>
            <td style="color:{gain_color}"><b>{gain_str}</b></td><td>{checked_str}</td>
        </tr>"""

    total = len(signals)
    checked = sum(1 for s in signals if s[7])
    wins = sum(1 for s in signals if s[6] and s[6] > 0)
    hit = f"{wins/checked*100:.1f}%" if checked > 0 else "N/A"

    return f"""<!DOCTYPE html>
<html><head><title>Energy Detector Webhook</title>
<style>
  body {{ font-family: Arial; background: #0d1117; color: #e6edf3; padding: 20px; }}
  h1 {{ color: #58a6ff; }} h2 {{ color: #3fb950; }}
  table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
  th {{ background: #161b22; padding: 10px; text-align: left; color: #58a6ff; }}
  td {{ padding: 8px; border-bottom: 1px solid #21262d; }}
  tr:hover {{ background: #161b22; }}
  .stat {{ display: inline-block; background: #161b22; padding: 15px 25px; margin: 10px; border-radius: 8px; text-align: center; }}
  .stat-val {{ font-size: 2em; font-weight: bold; color: #3fb950; }}
  .stat-lbl {{ color: #8b949e; font-size: 0.85em; }}
</style></head><body>
<h1>Energy Detector — Webhook Dashboard</h1>
<div>
  <div class="stat"><div class="stat-val">{total}</div><div class="stat-lbl">Total Signals</div></div>
  <div class="stat"><div class="stat-val">{checked}</div><div class="stat-lbl">D+1 Checked</div></div>
  <div class="stat"><div class="stat-val">{wins}</div><div class="stat-lbl">Winners</div></div>
  <div class="stat"><div class="stat-val">{hit}</div><div class="stat-lbl">Hit Rate</div></div>
</div>
<h2>Recent Signals</h2>
<table>
  <tr><th>#</th><th>Time (UTC)</th><th>Ticker</th><th>TF</th><th>Price</th><th>Score</th><th>D+1 Gain</th><th>Checked</th></tr>
  {rows_html}
</table>
</body></html>"""

@app.get("/report")
async def manual_report(background_tasks: BackgroundTasks):
    background_tasks.add_task(send_weekly_report)
    return {"status": "Report generating and sending to Telegram..."}

@app.get("/optimize")
async def optimization_suggestions(background_tasks: BackgroundTasks):
    suggestions = analyze_for_optimization()
    msg = f"<b>Script Optimization Suggestions</b>\n\n{suggestions}"
    background_tasks.add_task(send_telegram, msg)
    return {"suggestions": suggestions}

@app.get("/health")
async def health():
    return {"status": "online", "time": datetime.utcnow().isoformat()}

@app.get("/check-d1")
async def manual_d1_check(background_tasks: BackgroundTasks):
    background_tasks.add_task(check_d1_performance)
    return {"status": "D+1 check triggered"}
