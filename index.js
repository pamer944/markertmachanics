// GoldMultiStrategy dashboard - Railway (Node.js / Express)
// The MT5 EA POSTs its status (with M15 candles + chart drawings) every few seconds
// and every trade event. Open the Railway URL to watch everything live. No login.
// The EA also posts "bot chat" messages (/api/msg); you can chat back: status, price, trades, close all.
//
// Railway variables:
//   API_KEY       secret shared with the EA (required to accept data)
//   DATABASE_URL  optional - add a Railway Postgres to keep trade history across redeploys
//   TZ_NAME       timezone for "today" (default Africa/Lusaka)
//   DAILY_TARGET  daily profit target in USD (default 100)
//   CONTROL_PIN   PIN for the Close / CLOSE ALL buttons (default: same as API_KEY)

const express = require("express");
const fs = require("fs");
const path = require("path");

const API_KEY = process.env.API_KEY || "";
const DATABASE_URL = process.env.DATABASE_URL || "";
const TZ_NAME = process.env.TZ_NAME || "Africa/Lusaka";
const DAILY_TARGET = parseFloat(process.env.DAILY_TARGET || "100");
const CONTROL_PIN = process.env.CONTROL_PIN || API_KEY;
const OFFLINE_AFTER = 60; // seconds without status = bot offline
const PORT = process.env.PORT || 3000;

const app = express();
app.use(express.json({ limit: "5mb" }));

// latest status per bot (account|symbol) - the EA resends it every few seconds
const STATUS = {};

// ------------------------------------------------------------------ commands from the app buttons
// one pending command per bot; the bot picks it up in the reply to its next tick/status (~1 s)
const PENDING = {};        // key -> { cmd, cmd_id, ts }
const LAST_RESULT = {};    // key -> { cmd, result, ts }
const CMD_TTL = 120;       // seconds - an old command is dropped if the bot was offline
function takeCommand(key) {
  const p = PENDING[key];
  if (!p) return {};
  if (Date.now() / 1000 - p.ts > CMD_TTL) {
    LAST_RESULT[key] = { cmd: p.cmd, result: "not delivered - bot offline", ts: Date.now() / 1000 };
    delete PENDING[key];
    return {};
  }
  return { cmd: p.cmd, cmd_id: p.cmd_id, ticket: String(p.ticket || "") };
}

// ------------------------------------------------------------------ live push (Server-Sent Events)
const clients = new Set();
function broadcast(msg) {
  const data = `data: ${JSON.stringify(msg)}\n\n`;
  for (const res of clients) res.write(data);
}
app.get("/api/stream", (req, res) => {
  res.set({ "Content-Type": "text/event-stream", "Cache-Control": "no-cache, no-transform",
            Connection: "keep-alive", "X-Accel-Buffering": "no" });
  res.flushHeaders();
  res.write("retry: 2000\n\n");
  clients.add(res);
  req.on("close", () => clients.delete(res));
});
setInterval(() => { for (const res of clients) res.write(": ping\n\n"); }, 15000);

// ------------------------------------------------------------------ storage
let pool = null;
let memEvents = [];
const FILE = path.join(__dirname, "events.json");
const MSG_FILE = path.join(__dirname, "msgs.json");
let MSGS = [];   // bot chat, newest last

async function initDb() {
  if (DATABASE_URL) {
    const { Pool } = require("pg");
    pool = new Pool({
      connectionString: DATABASE_URL,
      ssl: DATABASE_URL.includes("railway.internal") ? false : { rejectUnauthorized: false },
    });
    await pool.query(`CREATE TABLE IF NOT EXISTS events (
      id SERIAL PRIMARY KEY, ts DOUBLE PRECISION, account TEXT, symbol TEXT, currency TEXT,
      position_id TEXT, event TEXT, side TEXT, volume DOUBLE PRECISION, price DOUBLE PRECISION,
      sl DOUBLE PRECISION, tp DOUBLE PRECISION, tp1 DOUBLE PRECISION, profit DOUBLE PRECISION,
      tag TEXT, reason TEXT)`);
    await pool.query(`CREATE TABLE IF NOT EXISTS msgs (
      id SERIAL PRIMARY KEY, ts DOUBLE PRECISION, account TEXT, symbol TEXT, sender TEXT, text TEXT)`);
    try {
      const r = await pool.query("SELECT ts, account, symbol, sender, text FROM msgs ORDER BY id DESC LIMIT 200");
      MSGS = r.rows.reverse().map(m => ({ ts: m.ts, account: m.account, symbol: m.symbol, from: m.sender, text: m.text }));
    } catch (e) { console.error(e.message); }
    console.log("Storage: Postgres");
  } else {
    try { memEvents = JSON.parse(fs.readFileSync(FILE, "utf8")); } catch { memEvents = []; }
    try { MSGS = JSON.parse(fs.readFileSync(MSG_FILE, "utf8")); } catch { MSGS = []; }
    console.log("Storage: file (add Postgres on Railway to keep history across redeploys)");
  }
}

async function addEvent(e) {
  if (pool) {
    await pool.query(
      `INSERT INTO events (ts, account, symbol, currency, position_id, event, side, volume, price, sl, tp, tp1, profit, tag, reason)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`,
      [e.ts, e.account, e.symbol, e.currency, e.position_id, e.event, e.side, e.volume, e.price,
       e.sl, e.tp, e.tp1, e.profit, e.tag, e.reason]);
  } else {
    memEvents.push(e);
    if (memEvents.length > 5000) memEvents = memEvents.slice(-5000);
    fs.writeFile(FILE, JSON.stringify(memEvents), () => {});
  }
}

async function loadEvents(limit) {
  if (pool) {
    const r = await pool.query(
      `SELECT ts, account, symbol, currency, position_id, event, side, volume, price, sl, tp, tp1, profit, tag, reason
       FROM events ORDER BY id DESC LIMIT $1`, [limit]);
    return r.rows;
  }
  return memEvents.slice(-limit).reverse();
}

// ------------------------------------------------------------------ helpers
const num = v => (v === null || v === undefined || isNaN(Number(v)) ? 0 : Number(v));
const isCent = c => ["USC", "USCENT"].includes(String(c || "").toUpperCase());
const toUsd = (v, c) => (isCent(c) ? v / 100 : v);
const dayOf = ts => new Date(ts * 1000).toLocaleDateString("en-CA", { timeZone: TZ_NAME }); // YYYY-MM-DD
const nowStr = () => new Date().toLocaleString("sv-SE", { timeZone: TZ_NAME });

function checkKey(req, res) {
  if (!API_KEY) { res.status(503).json({ detail: "API_KEY is not set on the server" }); return false; }
  if ((req.get("x-api-key") || "") !== API_KEY) { res.status(401).json({ detail: "wrong API key" }); return false; }
  return true;
}

// ------------------------------------------------------------------ EA -> server
app.post("/api/status", (req, res) => {
  if (!checkKey(req, res)) return;
  const b = req.body || {};
  b.received = Date.now() / 1000;
  const key = `${b.account || "?"}|${b.symbol || "?"}`;
  STATUS[key] = b;
  res.json({ ok: true, ...takeCommand(key) });
  broadcast({ type: "status", key });
});

// fast price update every second: price, current candle, equity, live P/L
app.post("/api/tick", (req, res) => {
  if (!checkKey(req, res)) return;
  const t = req.body || {};
  const key = `${t.account || "?"}|${t.symbol || "?"}`;
  const s = STATUS[key];
  res.json({ ok: true, ...takeCommand(key) });
  if (!s) return; // wait for the first full status
  s.bid = t.bid; s.ask = t.ask;
  if (t.equity !== undefined) s.equity = t.equity;
  if (t.floating !== undefined) s.floating = t.floating;
  s.received = Date.now() / 1000;
  if (Array.isArray(t.candle) && Array.isArray(s.candles)) {
    const last = s.candles[s.candles.length - 1];
    if (last && last[0] === t.candle[0]) s.candles[s.candles.length - 1] = t.candle;
    else if (!last || t.candle[0] > last[0]) { s.candles.push(t.candle); if (s.candles.length > 400) s.candles.shift(); }
  }
  const live = {};
  (t.positions || []).forEach(p => { live[p.ticket] = p; });
  (s.positions || []).forEach(p => {
    const l = live[p.ticket];
    if (l) { p.profit = l.profit; if (l.sl) p.sl = l.sl; }
  });
  broadcast({ type: "tick", key, bid: t.bid, ask: t.ask, candle: t.candle,
              equity: s.equity, floating: s.floating, positions: t.positions || [] });
});

app.post("/api/event", async (req, res) => {
  if (!checkKey(req, res)) return;
  const e = req.body || {};
  try {
    await addEvent({
      ts: Date.now() / 1000, account: String(e.account || ""), symbol: e.symbol || "", currency: e.currency || "",
      position_id: String(e.position_id || ""), event: e.event || "", side: e.side || "",
      volume: num(e.volume), price: num(e.price), sl: num(e.sl), tp: num(e.tp), tp1: num(e.tp1),
      profit: num(e.profit), tag: e.tag || "", reason: e.reason || "",
    });
    res.json({ ok: true });
  } catch (err) {
    console.error(err);
    res.status(500).json({ detail: "db error" });
  }
});

// app button -> queue a command for one bot or all bots
app.post("/api/command", (req, res) => {
  if (!CONTROL_PIN) return res.status(503).json({ detail: "Set API_KEY (or CONTROL_PIN) on Railway first" });
  if ((req.get("x-control-pin") || "") !== CONTROL_PIN) return res.status(401).json({ detail: "wrong PIN" });
  const { key, cmd, ticket } = req.body || {};
  if (!["close_all", "close_ticket"].includes(cmd)) return res.status(400).json({ detail: "unknown command" });
  const sent = queueCommand(key, cmd, ticket);
  if (!sent.length) return res.status(404).json({ detail: "no bot connected" });
  console.log(`COMMAND ${cmd} -> ${sent.join(", ")}`);
  broadcast({ type: "status", key: "*" });
  res.json({ ok: true, sent });
});

function queueCommand(key, cmd, ticket) {
  const keys = key === "ALL" ? Object.keys(STATUS) : [key];
  const sent = [];
  for (const k of keys) {
    if (!STATUS[k]) continue;
    PENDING[k] = { cmd, ticket, cmd_id: `${Date.now()}-${Math.random().toString(36).slice(2, 7)}`, ts: Date.now() / 1000 };
    sent.push(k);
  }
  return sent;
}

// ------------------------------------------------------------------ bot chat
async function addMsg(m) {
  m.ts = m.ts || Date.now() / 1000;
  m.text = String(m.text || "").slice(0, 600);
  MSGS.push(m);
  if (MSGS.length > 300) MSGS = MSGS.slice(-300);
  if (pool) {
    try {
      await pool.query("INSERT INTO msgs (ts, account, symbol, sender, text) VALUES ($1,$2,$3,$4,$5)",
        [m.ts, m.account || "", m.symbol || "", m.from || "bot", m.text]);
    } catch (e) { console.error("msg save:", e.message); }
  } else {
    fs.writeFile(MSG_FILE, JSON.stringify(MSGS), () => {});
  }
  broadcast({ type: "msg", m });
}

// EA -> chat
app.post("/api/msg", async (req, res) => {
  if (!checkKey(req, res)) return;
  const b = req.body || {};
  await addMsg({ account: String(b.account || ""), symbol: b.symbol || "", from: "bot", text: b.text || "" });
  res.json({ ok: true });
});

app.get("/api/msgs", (req, res) => res.json(MSGS.slice(-150)));

// you -> chat. Read-only questions need no PIN; "close all" needs the control PIN.
app.post("/api/chat", async (req, res) => {
  const text = String((req.body || {}).text || "").trim().slice(0, 300);
  if (!text) return res.status(400).json({ detail: "empty" });
  await addMsg({ from: "you", text });
  const q = text.toLowerCase();
  const now = Date.now() / 1000;
  const bots = Object.values(STATUS);
  const say = t => addMsg({ from: "server", text: t });
  const name = b => `${b.symbol} (${b.account})`;
  const dg = b => b.digits ?? 2;
  let reply;

  if (/close\s*all|close everything|emergency/.test(q)) {
    if (!CONTROL_PIN || (req.get("x-control-pin") || "") !== CONTROL_PIN) {
      await say("🔒 Closing trades needs your control PIN.");
      return res.status(401).json({ detail: "wrong PIN" });
    }
    const sent = queueCommand("ALL", "close_all", "");
    reply = sent.length ? `⛔ CLOSE ALL sent to ${sent.map(k => k.split("|")[1]).join(", ")} – the bots confirm here in ~1 s.`
                        : "No bot is connected.";
    broadcast({ type: "status", key: "*" });
  } else if (/price|quote/.test(q)) {
    reply = bots.length ? bots.map(b => `${b.symbol}: bid ${Number(b.bid).toFixed(dg(b))} / ask ${Number(b.ask).toFixed(dg(b))}`).join("\n")
                        : "No bot connected yet.";
  } else if (/trade|position|open/.test(q)) {
    const lines = bots.flatMap(b => (b.positions || []).map(p =>
      `${b.symbol} ${p.type} ${p.volume} @ ${p.open} | SL ${p.sl} | TP ${p.tp} | P/L ${Number(p.profit || 0).toFixed(2)} ${b.currency}`));
    reply = lines.length ? lines.join("\n") : "No open trades right now.";
  } else if (/status|bias|how|analysis|what/.test(q)) {
    reply = bots.length ? bots.map(b => {
      const age = Math.round(now - (b.received || 0));
      return `${name(b)} – ${age <= OFFLINE_AFTER ? "🟢 online" : "🔴 offline " + age + "s"}\n` +
        `price ${Number(b.bid).toFixed(dg(b))} · H1 ${b.h1 || "-"} · M15 ${b.m15 || "-"} · ${b.regime || "-"}\n` +
        `last break: ${b.breakout || "-"}\n` +
        `bot: ${b.bot_status || "-"}\n` +
        `open trades: ${(b.positions || []).length} · today ${Number(b.today_pl || 0).toFixed(2)} ${b.currency}`;
    }).join("\n\n") : "No bot connected yet. Attach the EA with your Railway URL and API key.";
  } else {
    reply = "I can answer: status · price · trades · close all (needs PIN). The bots post every sweep, setup, fill and close here automatically.";
  }
  await say(reply);
  res.json({ ok: true });
});

// bot confirms it carried out the command
app.post("/api/ack", (req, res) => {
  if (!checkKey(req, res)) return;
  const { account, symbol, cmd_id, result } = req.body || {};
  const key = `${account}|${symbol}`;
  const p = PENDING[key];
  if (p && p.cmd_id === cmd_id) {
    LAST_RESULT[key] = { cmd: p.cmd, result: result || "done", ts: Date.now() / 1000 };
    delete PENDING[key];
  }
  res.json({ ok: true });
  broadcast({ type: "status", key });
});

// ------------------------------------------------------------------ dashboard data
app.get("/api/state", async (req, res) => {
  const now = Date.now() / 1000;
  const bots = Object.values(STATUS).map(s => {
    const { candles, draw, ...rest } = s; // chart data comes from /api/chart
    const age = Math.round(now - (s.received || 0));
    const key = `${s.account}|${s.symbol}`;
    return { ...rest, key, age, online: age <= OFFLINE_AFTER,
             pending: PENDING[key] ? PENDING[key].cmd : null, last_command: LAST_RESULT[key] || null };
  }).sort((a, b) => `${a.account}${a.symbol}`.localeCompare(`${b.account}${b.symbol}`));

  let events = [];
  try { events = await loadEvents(2000); } catch (err) { console.error(err); }

  // group closing deals per position -> finished trades
  const trades = {};
  for (const e of [...events].reverse()) {
    const k = `${e.account}|${e.position_id}`;
    const t = trades[k] || (trades[k] = {
      position_id: e.position_id, account: e.account, symbol: e.symbol, currency: e.currency,
      side: e.side, tag: "", open_price: null, close_price: null, sl: null, tp: null,
      opened: null, closed: null, profit: 0, closed_flag: false, reason: "",
    });
    if (e.event === "open") {
      Object.assign(t, { open_price: e.price, sl: e.sl, tp: e.tp, tag: e.tag, opened: e.ts, side: e.side, volume: e.volume });
    } else if (e.event === "partial" || e.event === "close") {
      t.profit += num(e.profit);
      t.reason = e.reason;
      if (e.event === "close") { t.closed_flag = true; t.closed = e.ts; t.close_price = e.price; }
    }
  }
  const finished = Object.values(trades).filter(t => t.closed_flag).sort((a, b) => (b.closed || 0) - (a.closed || 0));

  const stats = items => {
    const wins = items.filter(t => t.profit > 0).length;
    const losses = items.filter(t => t.profit < 0).length;
    return {
      trades: items.length, wins, losses, breakeven: items.length - wins - losses,
      win_rate: items.length ? Math.round(1000 * wins / items.length) / 10 : 0,
      profit_usd: Math.round(100 * items.reduce((a, t) => a + toUsd(t.profit, t.currency), 0)) / 100,
    };
  };
  const today = dayOf(now);

  res.json({
    server_time: nowStr(),
    daily_target_usd: DAILY_TARGET,
    bots,
    today: stats(finished.filter(t => t.closed && dayOf(t.closed) === today)),
    all_time: stats(finished),
    history: finished.slice(0, 100),
    events: events.slice(0, 50),
    storage: pool ? "postgres" : "file",
  });
});

app.get("/api/chart", (req, res) => {
  const s = STATUS[`${req.query.account}|${req.query.symbol}`];
  if (!s) return res.status(404).json({ detail: "no data for this bot yet" });
  res.json({
    symbol: s.symbol, tf: s.tf || "M15", digits: s.digits ?? 2, bid: s.bid, ask: s.ask,
    candles: s.candles || [], draw: s.draw || [], positions: s.positions || [],
    age: Math.round(Date.now() / 1000 - (s.received || 0)),
  });
});

app.get("/health", (req, res) => res.json({ ok: true }));
app.get("/", (req, res) => res.type("html").send(PAGE));

// ------------------------------------------------------------------ dashboard page
const PAGE = "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n<title>GoldMultiStrategy Dashboard</title>\n<style>\n  :root {\n    --bg: #0d1117; --card: #161b22; --line: #262c36; --text: #e6edf3; --muted: #8b949e;\n    --green: #2ecc71; --red: #ff5c5c; --gold: #f2c14e; --blue: #58a6ff;\n  }\n  * { box-sizing: border-box; }\n  body { margin: 0; background: var(--bg); color: var(--text);\n         font: 14px/1.45 -apple-system, \"Segoe UI\", Roboto, Arial, sans-serif; }\n  header { display: flex; align-items: center; justify-content: space-between;\n           padding: 14px 16px; border-bottom: 1px solid var(--line); position: sticky; top: 0;\n           background: var(--bg); z-index: 2; }\n  header h1 { font-size: 17px; margin: 0; color: var(--gold); letter-spacing: .3px; }\n  header .time { color: var(--muted); font-size: 12px; }\n  main { padding: 12px; min-width: 0; max-width: 1100px; margin: 0 auto; display: grid; gap: 14px; }\n  .grid { display: grid; gap: 14px; grid-template-columns: repeat(auto-fit, minmax(min(300px, 100%), 1fr)); }\n  .card { min-width: 0; background: var(--card); border: 1px solid var(--line); border-radius: 12px; padding: 14px; }\n  .card h2 { font-size: 13px; text-transform: uppercase; letter-spacing: .8px; color: var(--muted);\n             margin: 0 0 10px; display: flex; justify-content: space-between; align-items: center; }\n  .big { font-size: 26px; font-weight: 700; }\n  .row { display: flex; justify-content: space-between; padding: 4px 0; border-bottom: 1px dashed var(--line); }\n  .row:last-child { border-bottom: 0; }\n  .row > span:first-child { color: var(--muted); }\n  .row > span:last-child { text-align: right; }\n  .pos { color: var(--green); } .neg { color: var(--red); } .muted { color: var(--muted); }\n  .dot { display: inline-block; width: 9px; height: 9px; border-radius: 50%; margin-right: 6px; }\n  .on { background: var(--green); box-shadow: 0 0 8px var(--green); } .off { background: var(--red); }\n  .pill { font-size: 11px; padding: 2px 8px; border-radius: 99px; border: 1px solid var(--line); }\n  .bar { height: 10px; background: #0b0f14; border-radius: 99px; overflow: hidden; border: 1px solid var(--line); margin-top: 8px; }\n  .bar > div { height: 100%; background: linear-gradient(90deg, var(--gold), var(--green)); width: 0; transition: width .5s; }\n  table { width: 100%; border-collapse: collapse; font-size: 13px; }\n  th, td { padding: 7px 6px; text-align: left; border-bottom: 1px solid var(--line); white-space: nowrap; }\n  th { color: var(--muted); font-weight: 600; font-size: 12px; }\n  .scroll { overflow-x: auto; }\n  .stats { display: grid; grid-template-columns: repeat(4, 1fr); gap: 8px; text-align: center; }\n  .stats div { background: #0b0f14; border-radius: 10px; padding: 10px 4px; }\n  .stats b { display: block; font-size: 18px; }\n  .stats small { color: var(--muted); }\n  .empty { color: var(--muted); padding: 10px 0; text-align: center; }\n  .chartbar { display: flex; gap: 6px; align-items: center; flex-wrap: wrap; margin-bottom: 8px; }\n  .btn { background: #0b0f14; color: var(--text); border: 1px solid var(--line); border-radius: 8px;\n         padding: 5px 11px; font-size: 13px; cursor: pointer; }\n  .btn.active { border-color: var(--gold); color: var(--gold); }\n  .chartwrap { position: relative; width: 100%; height: 440px; touch-action: none; user-select: none;\n               background: #0a0d12; border-radius: 10px; overflow: hidden; border: 1px solid var(--line); }\n  .chartwrap canvas { width: 100%; height: 100%; display: block; cursor: grab; }\n  .chartwrap .note { position: absolute; inset: 0; display: flex; align-items: center; justify-content: center;\n                     color: var(--muted); text-align: center; padding: 20px; }\n  @media (min-width: 800px) { .chartwrap { height: 560px; } }\n  .btn.danger { background: #3a0d10; border-color: #ff5c5c; color: #ff8585; font-weight: 700; }\n  .btn.warn { border-color: #f2c14e; color: #f2c14e; }\n  .actions { display: flex; gap: 8px; margin-top: 12px; flex-wrap: wrap; }\n  .actions .btn { flex: 1; padding: 9px 10px; }\n  .chat { height: 320px; overflow-y: auto; display: flex; flex-direction: column; gap: 6px; padding: 4px 2px; }\n  .msg { max-width: 88%; padding: 7px 10px; border-radius: 12px; background: #0b0f14; border: 1px solid var(--line);\n         font-size: 13px; white-space: pre-wrap; word-wrap: break-word; align-self: flex-start; }\n  .msg.you { align-self: flex-end; background: #1d2b3a; border-color: #2f4b66; }\n  .msg.server { border-color: #4a3f1c; }\n  .msg small { display: block; color: var(--muted); font-size: 10px; margin-top: 3px; }\n  .chatform { display: flex; gap: 8px; margin-top: 10px; }\n  .chatform input { flex: 1; min-width: 0; background: #0b0f14; color: var(--text); border: 1px solid var(--line);\n                    border-radius: 8px; padding: 9px 10px; font-size: 14px; }\n  .quick { display: flex; gap: 6px; flex-wrap: wrap; margin-top: 8px; }\n  .quick .btn { font-size: 12px; padding: 4px 9px; }\n  #toast { position: fixed; left: 50%; bottom: 20px; transform: translateX(-50%); background: #161b22;\n           border: 1px solid var(--gold); color: var(--text); padding: 10px 16px; border-radius: 10px;\n           font-size: 14px; display: none; z-index: 10; max-width: 90%; text-align: center; }\n</style>\n</head>\n<body>\n<header>\n  <h1>⚜ GoldMultiStrategy</h1>\n  <div class=\"time\" id=\"clock\">connecting…</div>\n</header>\n\n<main>\n  <div class=\"card\">\n    <h2><span>Live chart <span class=\"muted\" id=\"chartInfo\"></span></span><span class=\"pill\" id=\"chartAge\"></span></h2>\n    <div class=\"chartbar\">\n      <span id=\"symTabs\"></span>\n      <span style=\"flex:1\"></span>\n      <button class=\"btn\" id=\"zoomOut\">−</button>\n      <button class=\"btn\" id=\"zoomIn\">+</button>\n      <button class=\"btn\" id=\"liveBtn\">● Live</button>\n    </div>\n    <div class=\"chartwrap\" id=\"chartWrap\">\n      <canvas id=\"chart\"></canvas>\n      <div class=\"note\" id=\"chartNote\">Waiting for chart data from the bot…</div>\n    </div>\n  </div>\n\n  <div class=\"card\">\n    <h2><span>💬 Bot chat</span><span class=\"pill\" id=\"chatLive\">connecting…</span></h2>\n    <div class=\"chat\" id=\"chat\"><div class=\"empty\">No messages yet</div></div>\n    <div class=\"quick\">\n      <button class=\"btn\" data-ask=\"status\">Status</button>\n      <button class=\"btn\" data-ask=\"price\">Price</button>\n      <button class=\"btn\" data-ask=\"trades\">Trades</button>\n    </div>\n    <form class=\"chatform\" id=\"chatForm\">\n      <input id=\"chatIn\" placeholder=\"Ask the bot: status, price, trades, close all…\" autocomplete=\"off\">\n      <button class=\"btn\" type=\"submit\">Send</button>\n    </form>\n  </div>\n\n  <div class=\"grid\">\n    <div class=\"card\">\n      <h2>Today <span class=\"pill\" id=\"targetPill\"></span></h2>\n      <div class=\"big\" id=\"todayPL\">–</div>\n      <div class=\"muted\" id=\"todayInfo\"></div>\n      <div class=\"bar\"><div id=\"targetBar\"></div></div>\n    </div>\n    <div class=\"card\">\n      <h2>Account</h2>\n      <div id=\"account\"><div class=\"empty\">Waiting for the bot…</div></div>\n    </div>\n  </div>\n\n  <div class=\"grid\" id=\"bots\"></div>\n\n  <div class=\"card\">\n    <h2>Open trades <button class=\"btn danger\" data-cmd=\"close_all\" data-key=\"ALL\">⛔ CLOSE ALL</button></h2>\n    <div class=\"scroll\"><table>\n      <thead><tr><th></th><th>Symbol</th><th>Side</th><th>Lots</th><th>Entry</th><th>SL</th><th>TP1</th><th>TP2</th><th>P/L</th><th>Setup</th></tr></thead>\n      <tbody id=\"open\"></tbody>\n    </table></div>\n  </div>\n\n  <div class=\"card\">\n    <h2>Performance</h2>\n    <div class=\"stats\" id=\"stats\"></div>\n  </div>\n\n  <div class=\"card\">\n    <h2>Trade history</h2>\n    <div class=\"scroll\"><table>\n      <thead><tr><th>Closed</th><th>Symbol</th><th>Side</th><th>Entry</th><th>Exit</th><th>Result</th><th>Setup</th><th>By</th></tr></thead>\n      <tbody id=\"history\"></tbody>\n    </table></div>\n  </div>\n</main>\n\n<div id=\"toast\"></div>\n<script>\nconst fmt = (v, d = 2) => (v === null || v === undefined || isNaN(v)) ? \"–\" : Number(v).toFixed(d);\nconst cls = v => v > 0 ? \"pos\" : v < 0 ? \"neg\" : \"\";\nconst isCent = c => [\"USC\", \"USCENT\"].includes((c || \"\").toUpperCase());\nconst toUsd = (v, c) => isCent(c) ? v / 100 : v;\nconst money = (v, c) => {\n  if (v === null || v === undefined) return \"–\";\n  const s = (v >= 0 ? \"+\" : \"\") + fmt(v);\n  return isCent(c) ? `${s} USC <span class=\"muted\">(${(toUsd(v, c) >= 0 ? \"+$\" : \"-$\") + fmt(Math.abs(toUsd(v, c)))})</span>` : `${s} ${c || \"\"}`;\n};\nconst t = ts => ts ? new Date(ts * 1000).toLocaleString([], { month: \"short\", day: \"2-digit\", hour: \"2-digit\", minute: \"2-digit\" }) : \"–\";\nconst esc = s => String(s ?? \"\").replace(/[&<>\"]/g, ch => ({ \"&\": \"&amp;\", \"<\": \"&lt;\", \">\": \"&gt;\", '\"': \"&quot;\" }[ch]));\n\nfunction row(label, value) { return `<div class=\"row\"><span>${label}</span><span>${value}</span></div>`; }\n\nfunction render(s) {\n  document.getElementById(\"clock\").textContent = \"Updated \" + s.server_time;\n  const bots = s.bots || [];\n\n  // ---- today + target (USD) — closed trades from the server, plus the EA's broker-day figure\n  const todayUsd = s.today.profit_usd;\n  const target = s.daily_target_usd;\n  const el = document.getElementById(\"todayPL\");\n  el.textContent = (todayUsd >= 0 ? \"+$\" : \"-$\") + fmt(Math.abs(todayUsd));\n  el.className = \"big \" + cls(todayUsd);\n  const floatUsd = bots.reduce((a, b) => a + toUsd(b.floating || 0, b.currency), 0);\n  document.getElementById(\"todayInfo\").innerHTML =\n    `${s.today.trades} closed trades · ${s.today.wins}W / ${s.today.losses}L · open P/L <span class=\"${cls(floatUsd)}\">${floatUsd >= 0 ? \"+\" : \"-\"}${fmt(Math.abs(floatUsd))}</span>`;\n  document.getElementById(\"targetPill\").textContent = `target ${fmt(target, 0)}`;\n  document.getElementById(\"targetBar\").style.width = Math.max(0, Math.min(100, todayUsd / target * 100)) + \"%\";\n\n  // ---- account (one per account number)\n  const accounts = {};\n  bots.forEach(b => { if (!accounts[b.account] || b.received > accounts[b.account].received) accounts[b.account] = b; });\n  const acc = Object.values(accounts);\n  document.getElementById(\"account\").innerHTML = acc.length ? acc.map(a =>\n    row(\"Account\", `${esc(a.account)} <span class=\"muted\">${esc(a.server)}</span>`) +\n    row(\"Balance\", `${fmt(a.balance)} ${esc(a.currency)}${isCent(a.currency) ? ` <span class=\"muted\">(${fmt(a.balance / 100)})</span>` : \"\"}`) +\n    row(\"Equity\", `${fmt(a.equity)} ${esc(a.currency)}`) +\n    row(\"Free margin\", `${fmt(a.margin_free)} ${esc(a.currency)}`)\n  ).join('<div style=\"height:10px\"></div>') : '<div class=\"empty\">Waiting for the bot…</div>';\n\n  // ---- one card per bot (symbol)\n  document.getElementById(\"bots\").innerHTML = bots.length ? bots.map(b => `\n    <div class=\"card\">\n      <h2><span><span class=\"dot ${b.online ? \"on\" : \"off\"}\"></span>${esc(b.symbol)}</span>\n          <span class=\"pill\">${b.online ? \"online\" : \"offline \" + b.age + \"s\"}</span></h2>\n      <div class=\"big\" data-price=\"${esc(b.account + \"|\" + b.symbol)}\">${fmt(b.bid, b.digits ?? 2)}</div>\n      ${row(\"H1 regime\", esc(b.regime))}\n      ${row(\"H1 / M15 bias\", `${esc(b.h1)} / ${esc(b.m15)}`)}\n      ${row(\"Structure\", `<span class=\"${(b.structure||\"\").startsWith(\"Bull\") ? \"pos\" : (b.structure||\"\").startsWith(\"Bear\") ? \"neg\" : \"\"}\">${esc(b.structure)}</span>`)}\n      ${row(\"Liquidity sweep\", (b.sweep_bull ? \"SSL taken (buys) \" : \"\") + (b.sweep_bear ? \"BSL taken (sells)\" : \"\") || \"none\")}\n      ${row(\"Breakout\", esc(b.breakout))}\n      ${row(\"Session\", b.session_open ? \"OPEN\" : \"closed\")}\n      ${row(\"Trading\", b.trading_allowed ? '<span class=\"pos\">ALLOWED</span>' : '<span class=\"neg\">BLOCKED – turn on Algo Trading</span>')}\n      ${b.bot_status ? row(\"Bot\", esc(b.bot_status)) : \"\"}\n      ${row(\"Today (broker)\", `<span class=\"${cls(b.today_pl)}\">${money(b.today_pl, b.currency)}</span>`)}\n      ${row(\"Last trade\", esc(b.last_signal))}\n      ${b.last_command ? row(\"Last command\", esc(b.last_command.result)) : \"\"}\n      ${b.pending ? row(\"Command\", `<span class=\"pos\" style=\"color:var(--gold)\">${esc(b.pending)} – sending…</span>`) : \"\"}\n    </div>`).join(\"\") : '<div class=\"card\"><div class=\"empty\">No bot connected yet. Attach the EA with your dashboard URL and API key.</div></div>';\n\n  // ---- open positions\n  const open = bots.flatMap(b => (b.positions || []).map(p => ({ ...p, key: b.key, symbol: b.symbol, currency: b.currency, digits: b.digits })));\n  document.getElementById(\"open\").innerHTML = open.length ? open.map(p => `\n    <tr><td><button class=\"btn danger\" data-cmd=\"close_ticket\" data-key=\"${esc(p.key)}\" data-ticket=\"${p.ticket}\">Close</button></td>\n        <td>${esc(p.symbol)}</td><td class=\"${p.type === \"BUY\" ? \"pos\" : \"neg\"}\">${p.type}</td><td>${fmt(p.volume)}</td>\n        <td>${fmt(p.open, p.digits)}</td><td>${fmt(p.sl, p.digits)}</td><td>${p.tp1 ? fmt(p.tp1, p.digits) : \"hit\"}</td>\n        <td>${fmt(p.tp, p.digits)}</td><td class=\"${cls(p.profit)}\">${money(p.profit, p.currency)}</td><td>${esc(p.comment)}</td></tr>`).join(\"\")\n    : '<tr><td colspan=\"10\" class=\"empty\">No open trades</td></tr>';\n\n  // ---- stats\n  const a = s.all_time, d = s.today;\n  document.getElementById(\"stats\").innerHTML = `\n    <div><b>${a.trades}</b><small>trades</small></div>\n    <div><b>${a.win_rate}%</b><small>win rate</small></div>\n    <div><b>${a.wins}/${a.losses}</b><small>W / L</small></div>\n    <div><b class=\"${cls(a.profit_usd)}\">${a.profit_usd >= 0 ? \"+\" : \"-\"}${fmt(Math.abs(a.profit_usd))}</b><small>total</small></div>`;\n\n  // ---- history\n  document.getElementById(\"history\").innerHTML = s.history.length ? s.history.map(h => `\n    <tr><td>${t(h.closed)}</td><td>${esc(h.symbol)}</td><td class=\"${h.side === \"BUY\" ? \"pos\" : \"neg\"}\">${esc(h.side)}</td>\n        <td>${fmt(h.open_price, 3)}</td><td>${fmt(h.close_price, 3)}</td>\n        <td class=\"${cls(h.profit)}\">${money(h.profit, h.currency)}</td><td>${esc(h.tag)}</td><td>${esc(h.reason)}</td></tr>`).join(\"\")\n    : '<tr><td colspan=\"8\" class=\"empty\">No closed trades yet</td></tr>';\n}\n\n\n// ====================================================================\n//  LIVE CHART: candles + the bot's own drawings + open trades\n// ====================================================================\nconst CH = { sel: null, data: null, barW: window.innerWidth < 600 ? 4.5 : 8, offset: 0, rightSpace: 10 };\nconst canvas = document.getElementById(\"chart\");\nconst ctx = canvas.getContext(\"2d\");\n\nfunction chartTabs(bots) {\n  const keys = bots.map(b => `${b.account}|${b.symbol}`);\n  if (!CH.sel || !keys.includes(CH.sel)) CH.sel = keys[0] || null;\n  document.getElementById(\"symTabs\").innerHTML = bots.map(b => {\n    const k = `${b.account}|${b.symbol}`;\n    return `<button class=\"btn ${k === CH.sel ? \"active\" : \"\"}\" data-k=\"${esc(k)}\">${esc(b.symbol)}</button>`;\n  }).join(\" \");\n  document.querySelectorAll(\"#symTabs .btn\").forEach(el => el.onclick = () => {\n    CH.sel = el.dataset.k; CH.offset = 0; CH.data = null; loadChart(); chartTabs(bots);\n  });\n}\n\nasync function loadChart() {\n  if (!CH.sel) return;\n  const [account, symbol] = CH.sel.split(\"|\");\n  try {\n    const r = await fetch(`/api/chart?account=${encodeURIComponent(account)}&symbol=${encodeURIComponent(symbol)}`, { cache: \"no-store\" });\n    if (!r.ok) return;\n    CH.data = await r.json();\n    drawChart();\n  } catch (e) {}\n}\n\nfunction drawChart() {\n  const d = CH.data, note = document.getElementById(\"chartNote\");\n  const dpr = window.devicePixelRatio || 1;\n  const W = canvas.clientWidth, H = canvas.clientHeight;\n  if (canvas.width !== W * dpr || canvas.height !== H * dpr) { canvas.width = W * dpr; canvas.height = H * dpr; }\n  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);\n  ctx.clearRect(0, 0, W, H);\n  if (!d || !d.candles || d.candles.length < 2) { note.style.display = \"flex\"; return; }\n  note.style.display = \"none\";\n\n  const C = d.candles, n = C.length, dg = d.digits ?? 2, step = 900;\n  document.getElementById(\"chartInfo\").textContent = `${d.symbol} · ${d.tf}`;\n  document.getElementById(\"chartAge\").textContent = d.age <= 60 ? \"live\" : `last update ${d.age}s ago`;\n\n  const AX = 62, BT = 20;                                   // price axis width, time axis height\n  const PL = 0, PR = W - AX, PT = 4, PB = H - BT;\n  const bw = CH.barW, count = Math.floor((PR - PL) / bw);\n  const maxOff = Math.max(0, n - 20);\n  CH.offset = Math.min(Math.max(CH.offset, 0), maxOff);\n  const endIdx = n - 1 + CH.rightSpace - CH.offset;\n  const startIdx = endIdx - count + 1;\n\n  // time -> fractional bar index (handles weekend gaps and future times)\n  const idxOf = t => {\n    if (t <= C[0][0]) return (t - C[0][0]) / step;\n    if (t >= C[n - 1][0]) return n - 1 + (t - C[n - 1][0]) / step;\n    let lo = 0, hi = n - 1;\n    while (hi - lo > 1) { const m = (lo + hi) >> 1; if (C[m][0] <= t) lo = m; else hi = m; }\n    return lo + (t - C[lo][0]) / Math.max(1, C[hi][0] - C[lo][0]);\n  };\n  const X = i => PL + (i - startIdx) * bw + bw / 2;\n  const XT = t => X(idxOf(t));\n\n  // price range from visible candles (+ open trade levels near price)\n  let lo = Infinity, hi = -Infinity;\n  for (let i = Math.max(0, startIdx); i <= Math.min(n - 1, endIdx); i++) { lo = Math.min(lo, C[i][3]); hi = Math.max(hi, C[i][2]); }\n  if (!isFinite(lo)) { lo = C[n - 1][3]; hi = C[n - 1][2]; }\n  const span0 = hi - lo || 1;\n  (d.positions || []).forEach(p => [p.sl, p.tp, p.tp1, p.open].forEach(v => {\n    if (v && Math.abs(v - (hi + lo) / 2) < span0 * 1.5) { lo = Math.min(lo, v); hi = Math.max(hi, v); }\n  }));\n  const pad = (hi - lo) * 0.08; lo -= pad; hi += pad;\n  const Y = p => PT + (hi - p) / (hi - lo) * (PB - PT);\n\n  // grid + price axis\n  ctx.font = \"11px -apple-system, Segoe UI, Arial\"; ctx.lineWidth = 1;\n  const rawStep = (hi - lo) / 7, mag = Math.pow(10, Math.floor(Math.log10(rawStep)));\n  const gstep = [1, 2, 2.5, 5, 10].map(m => m * mag).find(v => v >= rawStep);\n  ctx.strokeStyle = \"#161c25\"; ctx.fillStyle = \"#8b949e\"; ctx.textAlign = \"left\"; ctx.textBaseline = \"middle\";\n  for (let p = Math.ceil(lo / gstep) * gstep; p < hi; p += gstep) {\n    const y = Y(p); ctx.beginPath(); ctx.moveTo(PL, y); ctx.lineTo(PR, y); ctx.stroke();\n    ctx.fillText(p.toFixed(dg > 2 ? 2 : dg), PR + 6, y);\n  }\n\n  // time axis\n  ctx.textAlign = \"center\"; ctx.textBaseline = \"top\";\n  const every = Math.max(1, Math.round(90 / bw));\n  for (let i = Math.max(0, Math.ceil(startIdx / every) * every); i <= Math.min(n - 1, endIdx); i += every) {\n    const x = X(i), dt = new Date(C[i][0] * 1000);\n    ctx.strokeStyle = \"#161c25\"; ctx.beginPath(); ctx.moveTo(x, PT); ctx.lineTo(x, PB); ctx.stroke();\n    const hhmm = String(dt.getUTCHours()).padStart(2, \"0\") + \":\" + String(dt.getUTCMinutes()).padStart(2, \"0\");\n    const lbl = hhmm === \"00:00\" ? dt.toLocaleDateString([], { day: \"2-digit\", month: \"short\", timeZone: \"UTC\" }) : hhmm;\n    ctx.fillText(lbl, x, PB + 4);\n  }\n\n  // clip to plot area for everything on the chart\n  ctx.save(); ctx.beginPath(); ctx.rect(PL, PT, PR - PL, PB - PT); ctx.clip();\n  const D = d.draw || [];\n\n  // 1) session lines + zones\n  D.filter(o => o.k === \"vline\").forEach(o => {\n    ctx.strokeStyle = o.c; ctx.setLineDash([2, 4]); ctx.beginPath(); const x = XT(o.t);\n    ctx.moveTo(x, PT); ctx.lineTo(x, PB); ctx.stroke(); ctx.setLineDash([]);\n  });\n  D.filter(o => o.k === \"box\").forEach(o => {\n    const x1 = XT(o.t1), x2 = XT(o.t2), y1 = Y(Math.max(o.p1, o.p2)), y2 = Y(Math.min(o.p1, o.p2));\n    ctx.globalAlpha = 0.85; ctx.fillStyle = o.c; ctx.fillRect(x1, y1, x2 - x1, Math.max(1, y2 - y1)); ctx.globalAlpha = 1;\n  });\n\n  // 2) lines (BOS, liquidity, Fib, trendlines)\n  D.filter(o => o.k === \"line\").forEach(o => {\n    let x1 = XT(o.t1), y1 = Y(o.p1), x2 = XT(o.t2), y2 = Y(o.p2);\n    if (o.r && x2 !== x1) { const k = (y2 - y1) / (x2 - x1); x2 = PR; y2 = y1 + k * (PR - x1); }\n    ctx.strokeStyle = o.c; ctx.lineWidth = o.w || 1; ctx.setLineDash(o.d ? [5, 4] : []);\n    ctx.beginPath(); ctx.moveTo(x1, y1); ctx.lineTo(x2, y2); ctx.stroke();\n  });\n  ctx.setLineDash([]); ctx.lineWidth = 1;\n\n  // 3) candles\n  for (let i = Math.max(0, startIdx); i <= Math.min(n - 1, endIdx); i++) {\n    const [, o, h, l, c] = C[i], x = X(i), up = c >= o;\n    ctx.strokeStyle = ctx.fillStyle = up ? \"#2ecc71\" : \"#ff5c5c\";\n    ctx.beginPath(); ctx.moveTo(x, Y(h)); ctx.lineTo(x, Y(l)); ctx.stroke();\n    const bwid = Math.max(1, bw * 0.7), yo = Y(o), yc = Y(c);\n    ctx.fillRect(x - bwid / 2, Math.min(yo, yc), bwid, Math.max(1, Math.abs(yc - yo)));\n  }\n\n  // 4) labels (HH/LL, BOS, OB names, liquidity names…)\n  D.filter(o => o.k === \"text\").forEach(o => {\n    ctx.font = `${Math.max(9, (o.z || 8) + 2)}px -apple-system, Segoe UI, Arial`; ctx.fillStyle = o.c;\n    const a = o.a || \"ANCHOR_LEFT_LOWER\";\n    ctx.textAlign = a.includes(\"LEFT\") ? \"left\" : a.includes(\"RIGHT\") ? \"right\" : \"center\";\n    ctx.textBaseline = a.endsWith(\"LOWER\") ? \"bottom\" : a.endsWith(\"UPPER\") ? \"top\" : \"middle\";\n    let x = XT(o.t); const w = ctx.measureText(o.s).width;\n    if (ctx.textAlign === \"left\" && x + w > PR - 2) x = PR - w - 2;   // keep right-side labels visible\n    ctx.fillText(o.s, x, Y(o.p));\n  });\n\n  // 5) open trades: shaded from the entry candle, labels on the right\n  const tag = (y, color, text) => {\n    ctx.font = \"bold 11px -apple-system, Segoe UI, Arial\"; ctx.textAlign = \"right\"; ctx.textBaseline = \"middle\";\n    const w = ctx.measureText(text).width + 10;\n    ctx.fillStyle = \"rgba(10,13,18,0.85)\"; ctx.fillRect(PR - w - 2, y - 8, w, 16);\n    ctx.strokeStyle = color; ctx.strokeRect(PR - w - 2, y - 8, w, 16);\n    ctx.fillStyle = color; ctx.fillText(text, PR - 7, y);\n  };\n  (d.positions || []).forEach(p => {\n    const x0 = Math.max(PL, p.time ? XT(p.time) : X(n - 30)), x1 = PR;\n    ctx.globalAlpha = 0.18;\n    ctx.fillStyle = \"#2ecc71\"; ctx.fillRect(x0, Math.min(Y(p.open), Y(p.tp)), x1 - x0, Math.abs(Y(p.tp) - Y(p.open)));\n    ctx.fillStyle = \"#ff5c5c\"; ctx.fillRect(x0, Math.min(Y(p.open), Y(p.sl)), x1 - x0, Math.abs(Y(p.sl) - Y(p.open)));\n    ctx.globalAlpha = 1;\n    const line = (price, color, dash) => {\n      if (!price) return; const y = Y(price);\n      ctx.strokeStyle = color; ctx.lineWidth = 1.5; ctx.setLineDash(dash ? [6, 4] : []);\n      ctx.beginPath(); ctx.moveTo(x0, y); ctx.lineTo(x1, y); ctx.stroke(); ctx.setLineDash([]); ctx.lineWidth = 1;\n    };\n    line(p.open, \"#58a6ff\"); line(p.sl, \"#ff5c5c\"); if (p.tp1) line(p.tp1, \"#2ecc71\", true); line(p.tp, \"#2ecc71\");\n    // entry arrow\n    const ya = Y(p.open); ctx.fillStyle = p.type === \"BUY\" ? \"#2ecc71\" : \"#ff5c5c\"; ctx.beginPath();\n    if (p.type === \"BUY\") { ctx.moveTo(x0, ya + 2); ctx.lineTo(x0 - 6, ya + 12); ctx.lineTo(x0 + 6, ya + 12); }\n    else { ctx.moveTo(x0, ya - 2); ctx.lineTo(x0 - 6, ya - 12); ctx.lineTo(x0 + 6, ya - 12); }\n    ctx.fill();\n    tag(Y(p.open), \"#58a6ff\", `${p.type} ${fmt(p.volume)} ${p.profit >= 0 ? \"+\" : \"\"}${fmt(p.profit)}`);\n    tag(Y(p.sl), \"#ff5c5c\", `SL ${fmt(p.sl, dg)}`);\n    if (p.tp1) tag(Y(p.tp1), \"#2ecc71\", `TP1 ${fmt(p.tp1, dg)}`);\n    tag(Y(p.tp), \"#2ecc71\", `TP2 ${fmt(p.tp, dg)}`);\n  });\n  ctx.restore();\n\n  // 6) live price tag\n  const bid = d.bid ?? C[n - 1][4], yb = Y(bid);\n  ctx.strokeStyle = \"#f2c14e\"; ctx.setLineDash([3, 3]); ctx.beginPath(); ctx.moveTo(PL, yb); ctx.lineTo(PR, yb); ctx.stroke(); ctx.setLineDash([]);\n  ctx.fillStyle = \"#f2c14e\"; ctx.fillRect(PR, yb - 9, AX, 18);\n  ctx.fillStyle = \"#0d1117\"; ctx.font = \"bold 11px -apple-system, Segoe UI, Arial\"; ctx.textAlign = \"left\"; ctx.textBaseline = \"middle\";\n  ctx.fillText(Number(bid).toFixed(dg > 2 ? 2 : dg), PR + 4, yb);\n  document.getElementById(\"liveBtn\").classList.toggle(\"active\", CH.offset === 0);\n}\n\n// drag to scroll back in time, buttons to zoom\n(() => {\n  let dragX = null, startOff = 0;\n  canvas.addEventListener(\"pointerdown\", e => { dragX = e.clientX; startOff = CH.offset; canvas.setPointerCapture(e.pointerId); canvas.style.cursor = \"grabbing\"; });\n  canvas.addEventListener(\"pointermove\", e => { if (dragX === null) return; CH.offset = Math.round(startOff + (e.clientX - dragX) / CH.barW); drawChart(); });\n  const end = () => { dragX = null; canvas.style.cursor = \"grab\"; };\n  canvas.addEventListener(\"pointerup\", end); canvas.addEventListener(\"pointercancel\", end);\n  canvas.addEventListener(\"wheel\", e => { e.preventDefault(); CH.barW = Math.min(24, Math.max(3, CH.barW * (e.deltaY < 0 ? 1.15 : 0.87))); drawChart(); }, { passive: false });\n  document.getElementById(\"zoomIn\").onclick = () => { CH.barW = Math.min(24, CH.barW * 1.25); drawChart(); };\n  document.getElementById(\"zoomOut\").onclick = () => { CH.barW = Math.max(3, CH.barW / 1.25); drawChart(); };\n  document.getElementById(\"liveBtn\").onclick = () => { CH.offset = 0; drawChart(); };\n  window.addEventListener(\"resize\", drawChart);\n})();\n\nasync function refresh() {\n  try {\n    const r = await fetch(\"/api/state\", { cache: \"no-store\" });\n    const st = await r.json();\n    render(st);\n    chartTabs(st.bots || []);\n    loadChart();\n  } catch (e) {\n    document.getElementById(\"clock\").textContent = \"Connection lost – retrying…\";\n  }\n}\n// ---- control buttons (Emergency close / Pause / Resume)\nfunction toast(msg, ms = 3500) {\n  const t = document.getElementById(\"toast\"); t.innerHTML = msg; t.style.display = \"block\";\n  clearTimeout(t._h); t._h = setTimeout(() => t.style.display = \"none\", ms);\n}\nfunction getPin(force) {\n  let pin = null;\n  try { pin = force ? null : localStorage.getItem(\"gms_pin\"); } catch (e) {}\n  if (!pin) {\n    pin = prompt(\"Enter your control PIN (the API_KEY or CONTROL_PIN set on Railway):\");\n    if (pin) try { localStorage.setItem(\"gms_pin\", pin); } catch (e) {}\n  }\n  return pin;\n}\nconst CMD_TEXT = {\n  close_all: \"⛔ CLOSE ALL\\n\\nClose ALL open bot trades (%s) now at market price?\",\n  close_ticket: \"⛔ Close this %s trade now at market price?\",\n};\nasync function sendCommand(cmd, key, label, ticket) {\n  if (!confirm(CMD_TEXT[cmd].replace(\"%s\", label))) return;\n  let pin = getPin(false);\n  if (!pin) return;\n  for (let attempt = 0; attempt < 2; attempt++) {\n    try {\n      const r = await fetch(\"/api/command\", { method: \"POST\",\n        headers: { \"Content-Type\": \"application/json\", \"X-Control-Pin\": pin }, body: JSON.stringify({ key, cmd, ticket }) });\n      if (r.status === 401) { pin = getPin(true); if (!pin) return; continue; }\n      const j = await r.json();\n      if (!r.ok) return toast(\"❌ \" + (j.detail || \"failed\"));\n      toast(\"⛔ Close sent – the bot closes it within about 1 second\");\n      refresh();\n      return;\n    } catch (e) { return toast(\"❌ No connection to the server\"); }\n  }\n  toast(\"❌ Wrong PIN\");\n}\ndocument.addEventListener(\"click\", e => {\n  const b = e.target.closest(\"[data-cmd]\");\n  if (b) sendCommand(b.dataset.cmd, b.dataset.key, b.dataset.key === \"ALL\" ? \"all symbols\" : b.dataset.key.split(\"|\")[1], b.dataset.ticket || \"\");\n});\n\n\n// ---- live push: price every second, instant refresh when the bot sends new analysis\nfunction applyTick(m) {\n  document.querySelectorAll(\"[data-price]\").forEach(el => {\n    if (el.dataset.price === m.key) el.textContent = fmt(m.bid, CH.data && CH.sel === m.key ? (CH.data.digits ?? 2) : 2);\n  });\n  if (CH.data && CH.sel === m.key) {\n    CH.data.bid = m.bid; CH.data.ask = m.ask; CH.data.age = 0;\n    const C = CH.data.candles;\n    if (m.candle && C && C.length) {\n      if (C[C.length - 1][0] === m.candle[0]) C[C.length - 1] = m.candle;\n      else if (m.candle[0] > C[C.length - 1][0]) C.push(m.candle);\n    }\n    const live = {}; (m.positions || []).forEach(p => live[p.ticket] = p);\n    (CH.data.positions || []).forEach(p => { const l = live[p.ticket]; if (l) { p.profit = l.profit; if (l.sl) p.sl = l.sl; } });\n    drawChart();\n  }\n}\n// ---- bot chat\nconst chatEl = document.getElementById(\"chat\");\nfunction addChat(m, force) {\n  if (chatEl.querySelector(\".empty\")) chatEl.innerHTML = \"\";\n  const atBottom = chatEl.scrollHeight - chatEl.scrollTop - chatEl.clientHeight < 80;\n  const d = document.createElement(\"div\");\n  const who = m.from === \"you\" ? \"you\" : m.from === \"server\" ? \"assistant\" : (m.symbol || \"bot\");\n  d.className = \"msg \" + (m.from || \"bot\");\n  d.innerHTML = esc(m.text) + `<small>${esc(who)} · ${new Date(m.ts * 1000).toLocaleTimeString([], { hour: \"2-digit\", minute: \"2-digit\" })}</small>`;\n  chatEl.appendChild(d);\n  while (chatEl.children.length > 200) chatEl.removeChild(chatEl.firstChild);\n  if (force || atBottom) chatEl.scrollTop = chatEl.scrollHeight;\n}\nasync function loadChat() {\n  try {\n    const r = await fetch(\"/api/msgs\", { cache: \"no-store\" });\n    const list = await r.json();\n    chatEl.innerHTML = list.length ? \"\" : '<div class=\"empty\">No messages yet</div>';\n    list.forEach(m => addChat(m, true));\n  } catch (e) {}\n}\nasync function ask(text) {\n  let pin = \"\";\n  if (/close/i.test(text)) {\n    if (!confirm(\"⛔ Send '\" + text + \"' to the bot? Open trades will be closed at market price.\")) return;\n    pin = getPin(false); if (!pin) return;\n  }\n  try {\n    const r = await fetch(\"/api/chat\", { method: \"POST\",\n      headers: { \"Content-Type\": \"application/json\", \"X-Control-Pin\": pin }, body: JSON.stringify({ text }) });\n    if (r.status === 401) { getPin(true); toast(\"❌ Wrong PIN – send it again\"); }\n  } catch (e) { toast(\"❌ No connection to the server\"); }\n}\ndocument.getElementById(\"chatForm\").onsubmit = e => {\n  e.preventDefault();\n  const inp = document.getElementById(\"chatIn\"), text = inp.value.trim();\n  if (text) { inp.value = \"\"; ask(text); }\n};\ndocument.querySelectorAll(\"[data-ask]\").forEach(b => b.onclick = () => ask(b.dataset.ask));\n\nlet refreshTimer = null;\nfunction connectStream() {\n  const es = new EventSource(\"/api/stream\");\n  es.onmessage = e => {\n    const m = JSON.parse(e.data);\n    if (m.type === \"tick\") applyTick(m);\n    else if (m.type === \"msg\") addChat(m.m);\n    else if (m.type === \"status\") { clearTimeout(refreshTimer); refreshTimer = setTimeout(refresh, 200); }\n  };\n  es.onopen = () => { document.getElementById(\"chatLive\").textContent = \"live\"; loadChat(); };\n  es.onerror = () => { document.getElementById(\"chatLive\").textContent = \"reconnecting…\"; es.close(); setTimeout(connectStream, 3000); };\n}\nconnectStream();\nrefresh();\nsetInterval(refresh, 5000);   // full refresh as a backup\n</script>\n</body>\n</html>\n";

initDb()
  .catch(err => { console.error("DB init failed, using file storage:", err.message); pool = null; })
  .finally(() => app.listen(PORT, () => console.log(`Dashboard running on port ${PORT}`)));
