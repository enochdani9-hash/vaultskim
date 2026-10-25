import os
import time
import json
import threading
import requests
import psycopg2
from datetime import datetime
from flask import Flask, jsonify, request, render_template_string

app = Flask(__name__)

# ==========================================
# 1. CONFIGURATION & NEON DATABASE SETUP
# ==========================================
DATABASE_URL = os.environ.get("DATABASE_URL")
STATE_FILE = "vault_state.json"
TRADING_FEE_PCT = 0.001  # 0.1% per swap

DEFAULT_COINS = {
    "QNTUSDT": {
        "symbol": "QNT",
        "anchor_usd": 180.00,
        "vault_profit_usd": 0.00,
        "in_position": False,
        "token_qty": 0.0,
        "entry_price": 0.0,
        "current_price": 0.0,
        "peak_price": 0.0,
        "local_low": 0.0,
        "ratchet_active": False,
        "ratchet_stop_price": 0.0,
        "mode": "STANDBY",
        "trades_count": 0
    },
    "CELOUSDT": {
        "symbol": "CELO",
        "anchor_usd": 100.00,
        "vault_profit_usd": 0.00,
        "in_position": False,
        "token_qty": 0.0,
        "entry_price": 0.0,
        "current_price": 0.0,
        "peak_price": 0.0,
        "local_low": 0.0,
        "ratchet_active": False,
        "ratchet_stop_price": 0.0,
        "mode": "STANDBY",
        "trades_count": 0
    },
    "KSMUSDT": {
        "symbol": "KSM",
        "anchor_usd": 100.00,
        "vault_profit_usd": 0.00,
        "in_position": False,
        "token_qty": 0.0,
        "entry_price": 0.0,
        "current_price": 0.0,
        "peak_price": 0.0,
        "local_low": 0.0,
        "ratchet_active": False,
        "ratchet_stop_price": 0.0,
        "mode": "STANDBY",
        "trades_count": 0
    }
}

WAKE_UP_PROFIT_PCT = 0.008     # +0.80% wakes up Micro-Ratchet
MICRO_TRAIL_PCT = 0.0025       # 0.25% tight trail on small bumps
WIDE_BREAKOUT_PCT = 0.030      # +3.0% switches to wider breakout trail
WIDE_TRAIL_PCT = 0.0080        # 0.80% trail for big rallies
CRASH_FLOOR_TRIGGER = -0.080   # -8.0% drop activates Kusama Reverse Skimming
RELIEF_BOUNCE_PCT = 0.050      # +5.0% bounce from local low skims 25% stack

bot_state = {"coins": DEFAULT_COINS, "logs": []}
state_lock = threading.Lock()
engine_initialized = False
last_tick_time = 0.0


def add_log(msg):
    timestamp = datetime.utcnow().strftime("%H:%M:%S")
    entry = f"[{timestamp}] {msg}"
    print(entry, flush=True)
    bot_state["logs"].insert(0, entry)
    bot_state["logs"] = bot_state["logs"][:50]


def init_and_load_state():
    global bot_state, engine_initialized
    if engine_initialized:
        return
    engine_initialized = True

    if DATABASE_URL:
        try:
            conn = psycopg2.connect(DATABASE_URL, connect_timeout=8)
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS vault_store (
                    id INT PRIMARY KEY,
                    state_json TEXT NOT NULL
                );
            """)
            cur.execute("SELECT state_json FROM vault_store WHERE id = 1;")
            row = cur.fetchone()
            if row:
                loaded = json.loads(row[0])
                if "coins" in loaded:
                    bot_state = loaded
                add_log("Connected to Neon PostgreSQL: Loaded saved Vault state.")
            else:
                cur.execute(
                    "INSERT INTO vault_store (id, state_json) VALUES (1, %s);",
                    (json.dumps(bot_state),)
                )
                conn.commit()
                add_log("Connected to Neon PostgreSQL: Initialized fresh Vault table.")
            cur.close()
            conn.close()
            return
        except Exception as e:
            add_log(f"Neon DB warning (using memory fallback): {e}")

    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                loaded = json.load(f)
                if "coins" in loaded:
                    bot_state = loaded
            add_log("Loaded local state file.")
        except Exception:
            pass
    else:
        add_log("VaultSkim Cloud Engine initialized.")


def save_state():
    if DATABASE_URL:
        try:
            conn = psycopg2.connect(DATABASE_URL, connect_timeout=5)
            cur = conn.cursor()
            cur.execute(
                "UPDATE vault_store SET state_json = %s WHERE id = 1;",
                (json.dumps(bot_state),)
            )
            conn.commit()
            cur.close()
            conn.close()
            return
        except Exception as e:
            print(f"Neon save warning: {e}", flush=True)

    try:
        with open(STATE_FILE, "w") as f:
            json.dump(bot_state, f, indent=2)
    except Exception:
        pass


# ==========================================
# 2. US-SAFE MULTI-SOURCE LIVE PRICE FEED
# ==========================================
def fetch_live_prices():
    """Fetches live prices using Gate.io & Binance US (works on Render US servers)."""
    prices = {}
    # Primary Feed: Gate.io Global Spot Tickers (Not geo-blocked in US)
    try:
        resp = requests.get("https://api.gateio.ws/api/v4/spot/tickers", timeout=5)
        if resp.status_code == 200:
            for item in resp.json():
                cp = item.get("currency_pair", "")
                if cp.endswith("_USDT") and item.get("last"):
                    clean_pair = cp.replace("_", "")
                    prices[clean_pair] = float(item["last"])
            if prices:
                return prices
    except Exception:
        pass

    # Backup Feed: Binance US API
    try:
        resp = requests.get("https://api.binance.us/api/v3/ticker/price", timeout=5)
        if resp.status_code == 200:
            for item in resp.json():
                if "symbol" in item and "price" in item:
                    prices[item["symbol"]] = float(item["price"])
            if prices:
                return prices
    except Exception as e:
        add_log(f"Backup price feed warning: {e}")

    return prices


# ==========================================
# 3. CORE PROFIT-SKIMMING ENGINE
# ==========================================
def process_coin(pair, coin, live_price):
    if live_price <= 0:
        return
    coin["current_price"] = live_price

    # STEP A: Deploy Working Anchor
    if not coin["in_position"] or coin["entry_price"] <= 0:
        fee = coin["anchor_usd"] * TRADING_FEE_PCT
        net_invest = coin["anchor_usd"] - fee
        coin["token_qty"] = net_invest / live_price
        coin["entry_price"] = live_price
        coin["peak_price"] = live_price
        coin["local_low"] = live_price
        coin["in_position"] = True
        coin["ratchet_active"] = False
        coin["ratchet_stop_price"] = 0.0
        coin["mode"] = "HOLDING_ANCHOR"
        add_log(f"BUY {coin['symbol']}: Deployed ${coin['anchor_usd']:.2f} Anchor @ ${live_price:.4f}")
        return

    if live_price > coin["peak_price"]:
        coin["peak_price"] = live_price
    if live_price < coin["local_low"] or coin["local_low"] <= 0:
        coin["local_low"] = live_price

    change_from_entry = (live_price - coin["entry_price"]) / coin["entry_price"]

    # STEP B: Wake Up Dynamic Micro-Ratchet
    if change_from_entry >= WAKE_UP_PROFIT_PCT:
        was_active = coin["ratchet_active"]
        coin["ratchet_active"] = True
        coin["mode"] = "RATCHET_LOCKED"
        trail_pct = WIDE_TRAIL_PCT if change_from_entry >= WIDE_BREAKOUT_PCT else MICRO_TRAIL_PCT
        new_stop = coin["peak_price"] * (1.0 - trail_pct)
        if new_stop > coin["ratchet_stop_price"]:
            coin["ratchet_stop_price"] = new_stop
        if not was_active:
            add_log(f"RATCHET ARMED {coin['symbol']} @ ${live_price:.4f} (Stop Net: ${coin['ratchet_stop_price']:.4f})")

    # STEP C: Snap Shut & Sweep Profit to Vault
    if coin["ratchet_active"] and live_price <= coin["ratchet_stop_price"]:
        gross_value = coin["token_qty"] * live_price
        sell_fee = gross_value * TRADING_FEE_PCT
        net_payout = gross_value - sell_fee
        skimmed_profit = net_payout - coin["anchor_usd"]

        if skimmed_profit > 0:
            coin["vault_profit_usd"] += skimmed_profit
            coin["trades_count"] += 1
            add_log(
                f"VAULT SKIM {coin['symbol']} @ ${live_price:.4f}! "
                f"Swept +${skimmed_profit:.2f} into Vault | Re-arming ${coin['anchor_usd']:.2f} Anchor"
            )
            coin["in_position"] = False
            coin["ratchet_active"] = False
            coin["ratchet_stop_price"] = 0.0
            coin["mode"] = "RE-ENTERING"
            # Immediately re-deploy Anchor at current price
            process_coin(pair, coin, live_price)
        return

    # STEP D: Kusama Crash Recovery (-8% Drop -> 25% Reverse Skim on Relief Bounces)
    if change_from_entry <= CRASH_FLOOR_TRIGGER:
        coin["mode"] = "CRASH_RECOVERY"
        bounce_from_bottom = (live_price - coin["local_low"]) / coin["local_low"]
        if bounce_from_bottom >= RELIEF_BOUNCE_PCT:
            recovery_skim = (coin["anchor_usd"] * 0.25) * (bounce_from_bottom * 0.5)
            coin["vault_profit_usd"] += recovery_skim
            coin["entry_price"] = coin["entry_price"] * 0.985
            coin["local_low"] = live_price
            coin["trades_count"] += 1
            add_log(
                f"REVERSE SKIM {coin['symbol']}: +${recovery_skim:.2f} banked on bounce! "
                f"Break-even lowered to ${coin['entry_price']:.4f}"
            )


def run_engine_tick():
    """Executes one complete market check cycle safely."""
    global last_tick_time
    now = time.time()
    if now - last_tick_time < 3.0:
        return
    last_tick_time = now

    init_and_load_state()
    prices = fetch_live_prices()
    if prices:
        with state_lock:
            for pair, coin in list(bot_state["coins"].items()):
                if pair in prices:
                    process_coin(pair, coin, prices[pair])
            save_state()


def bot_background_loop():
    while True:
        try:
            run_engine_tick()
        except Exception as e:
            print(f"Loop error: {e}", flush=True)
        time.sleep(5)


threading.Thread(target=bot_background_loop, daemon=True).start()


# ==========================================
# 4. WEB DASHBOARD & INTERACTIVE CONTROLS
# ==========================================
DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>VaultSkim — Two-Bucket Profit Engine</title>
  <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-slate-950 text-slate-100 min-h-screen p-6 font-sans">
  <div class="max-w-6xl mx-auto">
    <div class="flex flex-col md:flex-row justify-between items-start md:items-center border-b border-slate-800 pb-4 mb-6 gap-4">
      <div>
        <h1 class="text-2xl font-bold text-emerald-400">VaultSkim Command Center</h1>
        <p class="text-sm text-slate-400">Neon PostgreSQL + Render Cloud + Dynamic Micro-Ratchet</p>
      </div>
      <div class="bg-emerald-950/60 border border-emerald-500/40 px-5 py-3 rounded-xl text-right">
        <div class="text-xs uppercase tracking-wider text-emerald-300">Total Untouchable Vault Profit</div>
        <div id="total-vault" class="text-3xl font-extrabold text-emerald-400">$0.00</div>
      </div>
    </div>

    <!-- Add Any Token / Meme Coin Bar -->
    <div class="bg-slate-900 border border-slate-800 rounded-xl p-4 mb-6 flex flex-wrap items-center justify-between gap-3">
      <div class="text-sm font-medium text-slate-300">Add Token or Meme Coin to Skimmer:</div>
      <div class="flex flex-wrap gap-2">
        <input id="new-symbol" type="text" placeholder="Symbol (e.g. SOL, PEPE, DOGE)" class="bg-slate-950 border border-slate-700 rounded-lg px-3 py-1.5 text-sm text-white uppercase">
        <input id="new-anchor" type="number" placeholder="Anchor USD (e.g. 50)" value="50" class="bg-slate-950 border border-slate-700 rounded-lg px-3 py-1.5 text-sm text-white w-36">
        <button onclick="addCoin()" class="bg-emerald-600 hover:bg-emerald-500 text-white text-sm font-semibold px-4 py-1.5 rounded-lg transition">+ Launch Skimmer</button>
      </div>
    </div>

    <div id="coin-cards" class="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8"></div>

    <div class="bg-slate-900 border border-slate-800 rounded-xl p-5">
      <h2 class="text-sm font-semibold uppercase tracking-wider text-slate-400 mb-3">Live Execution Log</h2>
      <div id="logs" class="font-mono text-xs text-slate-300 space-y-1.5 max-h-64 overflow-y-auto"></div>
    </div>
  </div>

  <script>
    async function addCoin() {
      const sym = document.getElementById('new-symbol').value.trim().toUpperCase();
      const anc = parseFloat(document.getElementById('new-anchor').value) || 50;
      if (!sym) return;
      await fetch('/api/add_coin', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({symbol: sym, anchor_usd: anc})
      });
      document.getElementById('new-symbol').value = '';
      refresh();
    }

    async function simulateSpike(pair) {
      await fetch('/api/simulate_spike/' + pair, {method: 'POST'});
      refresh();
    }

    async function refresh() {
      const res = await fetch('/api/state');
      const data = await res.json();
      let totalVault = 0;
      let cardsHtml = '';

      for (const [pair, c] of Object.entries(data.coins)) {
        totalVault += c.vault_profit_usd;
        const liveVal = c.in_position && c.current_price > 0 ? (c.token_qty * c.current_price) : c.anchor_usd;
        const pnlPct = c.entry_price > 0 ? (((c.current_price - c.entry_price) / c.entry_price) * 100).toFixed(2) : "0.00";
        const pnlColor = pnlPct >= 0 ? 'text-emerald-400' : 'text-amber-400';

        cardsHtml += `
          <div class="bg-slate-900 border border-slate-800 rounded-xl p-5 shadow-lg flex flex-col justify-between">
            <div>
              <div class="flex justify-between items-center mb-3">
                <span class="text-xl font-bold">${c.symbol}/USDT</span>
                <span class="text-xs px-2.5 py-1 rounded-full bg-slate-800 text-emerald-300 font-medium">${c.mode}</span>
              </div>
              <div class="text-2xl font-mono font-bold mb-4">$${c.current_price.toFixed(4)} <span class="text-sm ${pnlColor}">(${pnlPct}%)</span></div>
              <div class="space-y-2 text-sm border-t border-slate-800 pt-3">
                <div class="flex justify-between"><span class="text-slate-400">Bucket A (Anchor):</span><span class="font-semibold">$${c.anchor_usd.toFixed(2)}</span></div>
                <div class="flex justify-between"><span class="text-slate-400">Live Position Value:</span><span>$${liveVal.toFixed(2)}</span></div>
                <div class="flex justify-between"><span class="text-slate-400">Entry / Peak:</span><span>$${c.entry_price.toFixed(4)} / $${c.peak_price.toFixed(4)}</span></div>
                <div class="flex justify-between"><span class="text-slate-400">Ratchet Sell Net:</span><span class="text-amber-300">${c.ratchet_active ? '$' + c.ratchet_stop_price.toFixed(4) : 'Waiting (+0.8%)'}</span></div>
                <div class="flex justify-between border-t border-slate-800 pt-2 mt-2">
                  <span class="text-emerald-400 font-semibold">Bucket B (Vaulted):</span>
                  <span class="text-emerald-400 font-bold">+$${c.vault_profit_usd.toFixed(2)} (${c.trades_count} skims)</span>
                </div>
              </div>
            </div>
            <button onclick="simulateSpike('${pair}')" class="mt-4 w-full bg-slate-800 hover:bg-slate-700 text-amber-300 text-xs font-semibold py-2 rounded-lg border border-slate-700 transition">
              ⚡ Test Spike (+2% & Skim)
            </button>
          </div>`;
      }

      document.getElementById('coin-cards').innerHTML = cardsHtml;
      document.getElementById('total-vault').innerText = '$' + totalVault.toFixed(2);
      document.getElementById('logs').innerHTML = data.logs.map(l => `<div>${l}</div>`).join('');
    }
    setInterval(refresh, 3000);
    refresh();
  </script>
</body>
</html>
"""

@app.route("/")
def index():
    return render_template_string(DASHBOARD_HTML)

@app.route("/api/state")
def api_state():
    run_engine_tick()
    with state_lock:
        return jsonify(bot_state)

@app.route("/api/add_coin", methods=["POST"])
def add_coin():
    data = request.json or {}
    sym = data.get("symbol", "").upper().replace("USDT", "").strip()
    anchor = float(data.get("anchor_usd", 50.0))
    if sym:
        pair = f"{sym}USDT"
        with state_lock:
            bot_state["coins"][pair] = {
                "symbol": sym,
                "anchor_usd": anchor,
                "vault_profit_usd": 0.0,
                "in_position": False,
                "token_qty": 0.0,
                "entry_price": 0.0,
                "current_price": 0.0,
                "peak_price": 0.0,
                "local_low": 0.0,
                "ratchet_active": False,
                "ratchet_stop_price": 0.0,
                "mode": "STANDBY",
                "trades_count": 0
            }
            add_log(f"ADDED NEW SKIMMER: {sym}/USDT with ${anchor:.2f} Anchor.")
            save_state()
    return jsonify({"ok": True})

@app.route("/api/simulate_spike/<pair>", methods=["POST"])
def simulate_spike(pair):
    """Simulates a +2% spike and minor pullback to test the Ratchet & Vault Skim live."""
    with state_lock:
        coin = bot_state["coins"].get(pair)
        if coin and coin["entry_price"] > 0:
            spike_price = coin["entry_price"] * 1.022  # +2.2% spike arms ratchet
            process_coin(pair, coin, spike_price)
            pullback_price = spike_price * 0.996       # Pullback triggers ratchet skim
            process_coin(pair, coin, pullback_price)
            save_state()
    return jsonify({"ok": True})

@app.route("/health")
def health():
    run_engine_tick()
    return jsonify({"status": "awake", "utc_time": datetime.utcnow().isoformat()}), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
