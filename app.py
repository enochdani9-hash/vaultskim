import os
import time
from flask import Flask, request, jsonify, render_template_string
import ccxt

app = Flask(__name__)

# =====================================================================
# BOT MEMORY & CREDENTIAL STATE (Supports In-App API Key Insertion!)
# =====================================================================
BOT_STATE = {
    "api_key": (os.environ.get("GATE_API_KEY") or "").strip(),
    "api_secret": (os.environ.get("GATE_SECRET") or "").strip(),
    "keys_connected": False,
    "frozen": False,
    "usdt_free": 0.0,
    "usdt_total": 0.0,
    "realized_usdt_profit": 0.0,
    "completed_skims": 0,
    "coins": {},      # Active coin cards
    "ledger": [],     # Verified Skim Ledger entries
    "logs": []        # Live execution terminal logs
}

# Profitable Strategy Parameters (Clears Gate.io 0.20% round-trip fees!)
TRIGGER_UP_PCT = 1.50 / 100.0      # Arm Trailing Peak Chaser at +1.50%
TRAIL_PULLBACK_PCT = 0.35 / 100.0  # Sell when price pulls back 0.35% from peak
REBUY_DIP_PCT = 1.20 / 100.0       # Place Protected Share Limit Buy 1.20% lower


def add_log(msg):
    timestamp = time.strftime("%H:%M:%S")
    BOT_STATE["logs"].insert(0, f"[{timestamp}] {msg}")
    BOT_STATE["logs"] = BOT_STATE["logs"][:50]


def get_exchange():
    """Creates a Gate.io client using keys entered in the UI (or Render Env Vars)."""
    key = BOT_STATE["api_key"].strip()
    secret = BOT_STATE["api_secret"].strip()
    if not key or not secret:
        return None
    return ccxt.gate({
        "apiKey": key,
        "secret": secret,
        "enableRateLimit": True,
        "options": {"defaultType": "spot"}
    })


def sync_existing_wallet(exchange, balance):
    """Auto-detects any coin worth >= $3 USDT in your Gate.io Spot wallet."""
    ignored = {"USDT", "USDC", "USD", "POINT", "GT"}
    for asset, total_amt in balance.get("total", {}).items():
        if asset not in ignored and float(total_amt or 0) > 0:
            symbol = f"{asset}/USDT"
            if symbol in exchange.markets and symbol not in BOT_STATE["coins"]:
                ticker = exchange.fetch_ticker(symbol)
                price = float(ticker["last"])
                shares = float(total_amt)
                val_usdt = shares * price
                if val_usdt >= 3.0:
                    BOT_STATE["coins"][symbol] = {
                        "symbol": symbol,
                        "base": asset,
                        "protected_shares": shares,
                        "real_shares": shares,
                        "entry_price": price,
                        "live_price": price,
                        "position_value": val_usdt,
                        "harvest_target": price * (1 + TRIGGER_UP_PCT),
                        "highest_peak": price,
                        "trailing_active": False,
                        "realized_profit": 0.0,
                        "skims": 0,
                        "status": "RATCHET_ARMED"
                    }
                    add_log(f"🛡️ Auto-Synced Gate.io Spot Position: {shares:.6f} {asset} (${val_usdt:.2f})")


# =====================================================================
# CORE TRADING ENGINE
# =====================================================================
def execute_engine_tick():
    exchange = get_exchange()
    if not exchange:
        BOT_STATE["keys_connected"] = False
        return {
            "keys_connected": False,
            "usdt_free": 0.0,
            "usdt_total": 0.0,
            "total_profit": BOT_STATE["realized_usdt_profit"],
            "skims": BOT_STATE["completed_skims"],
            "coins": BOT_STATE["coins"],
            "ledger": BOT_STATE["ledger"],
            "logs": BOT_STATE["logs"],
            "frozen": BOT_STATE["frozen"]
        }

    exchange.load_markets()
    balance = exchange.fetch_balance()
    BOT_STATE["keys_connected"] = True

    free_usdt = float(balance.get("free", {}).get("USDT", 0.0))
    total_usdt = float(balance.get("total", {}).get("USDT", 0.0))
    BOT_STATE["usdt_free"] = free_usdt
    BOT_STATE["usdt_total"] = total_usdt

    if not BOT_STATE["coins"]:
        sync_existing_wallet(exchange, balance)

    if BOT_STATE["frozen"]:
        return {
            "keys_connected": True,
            "usdt_free": free_usdt,
            "usdt_total": total_usdt,
            "total_profit": BOT_STATE["realized_usdt_profit"],
            "skims": BOT_STATE["completed_skims"],
            "coins": BOT_STATE["coins"],
            "ledger": BOT_STATE["ledger"],
            "logs": BOT_STATE["logs"],
            "frozen": True
        }

    for symbol, card in list(BOT_STATE["coins"].items()):
        base = card["base"]
        ticker = exchange.fetch_ticker(symbol)
        live_price = float(ticker["last"])
        card["live_price"] = live_price

        real_shares = float(balance.get("total", {}).get(base, 0.0))
        free_shares = float(balance.get("free", {}).get(base, 0.0))
        card["real_shares"] = real_shares
        card["position_value"] = real_shares * live_price

        open_orders = exchange.fetch_open_orders(symbol)

        # 1. OPEN REBUY ORDER WAITING ON GATE.IO (100% Share Protection)
        if len(open_orders) > 0:
            order = open_orders[0]
            card["status"] = f"WAITING_REBUY @ ${float(order['price']):.4f}"
            card["active_order_price"] = float(order["price"])
            continue

        # 2. HOLDING SHARES -> WATCH FOR +1.50% & TRAIL THE PEAK UPWARD
        if free_shares * live_price >= 3.0:
            if real_shares > card["protected_shares"]:
                card["protected_shares"] = real_shares

            if card.get("status", "").startswith("WAITING_REBUY"):
                card["entry_price"] = card.get("active_order_price", live_price)
                card["highest_peak"] = live_price
                card["trailing_active"] = False
                add_log(f"✅ {base} REBUY FILLED! Restored {real_shares:.6f} {base} (Protected Share Floor Intact)")

            entry = card["entry_price"]
            trigger_target = entry * (1 + TRIGGER_UP_PCT)
            card["harvest_target"] = trigger_target

            if live_price > card["highest_peak"]:
                card["highest_peak"] = live_price

            if live_price >= trigger_target and not card["trailing_active"]:
                card["trailing_active"] = True
                add_log(f"🚀 {base} hit +{TRIGGER_UP_PCT*100:.2f}% (${live_price:.4f})! Trailing Peak Chaser ARMED.")

            if card["trailing_active"]:
                stop_price = card["highest_peak"] * (1 - TRAIL_PULLBACK_PCT)
                card["status"] = f"TRAILING_PEAK (Stop ${stop_price:.4f})"

                if live_price <= stop_price and live_price > entry * 1.006:
                    sell_amt = float(exchange.amount_to_precision(symbol, free_shares))
                    exchange.create_market_sell_order(symbol, sell_amt)

                    rebuy_price = float(exchange.price_to_precision(symbol, live_price * (1 - REBUY_DIP_PCT)))
                    target_rebuy_shares = max(card["protected_shares"], sell_amt) * 1.002
                    rebuy_amt = float(exchange.amount_to_precision(symbol, target_rebuy_shares))

                    exchange.create_limit_buy_order(symbol, rebuy_amt, rebuy_price)

                    usdt_from_sell = (sell_amt * live_price) * 0.999
                    usdt_for_rebuy = rebuy_amt * rebuy_price
                    net_skim = max(0.0, usdt_from_sell - usdt_for_rebuy)

                    card["realized_profit"] += net_skim
                    card["skims"] += 1
                    BOT_STATE["realized_usdt_profit"] += net_skim
                    BOT_STATE["completed_skims"] += 1

                    card["trailing_active"] = False
                    card["status"] = f"WAITING_REBUY @ ${rebuy_price:.4f}"
                    card["active_order_price"] = rebuy_price

                    ts = time.strftime("%H:%M:%S")
                    BOT_STATE["ledger"].insert(0, {
                        "base": base,
                        "sell_price": live_price,
                        "rebuy_price": rebuy_price,
                        "profit": net_skim,
                        "shares": rebuy_amt,
                        "time": ts
                    })
                    BOT_STATE["ledger"] = BOT_STATE["ledger"][:20]

                    add_log(
                        f"💰 {base} PEAK SKIMMED @ ${live_price:.4f}! "
                        f"Bagged +${net_skim:.4f} USDT & placed Rebuy for {rebuy_amt} {base} @ ${rebuy_price:.4f}"
                    )
            else:
                card["status"] = "RATCHET_ARMED"

    return {
        "keys_connected": True,
        "usdt_free": free_usdt,
        "usdt_total": total_usdt,
        "total_profit": BOT_STATE["realized_usdt_profit"],
        "skims": BOT_STATE["completed_skims"],
        "coins": BOT_STATE["coins"],
        "ledger": BOT_STATE["ledger"],
        "logs": BOT_STATE["logs"],
        "frozen": BOT_STATE["frozen"]
    }


# =====================================================================
# API ENDPOINTS
# =====================================================================
@app.route("/connect_keys", methods=["POST"])
def connect_keys():
    try:
        body = request.get_json() or {}
        api_key = (body.get("apiKey") or "").strip()
        api_secret = (body.get("apiSecret") or "").strip()

        if not api_key or not api_secret:
            return jsonify({"error": "Both Gate.io API Key and Secret are required."}), 400

        test_ex = ccxt.gate({
            "apiKey": api_key,
            "secret": api_secret,
            "enableRateLimit": True,
            "options": {"defaultType": "spot"}
        })
        test_ex.load_markets()
        bal = test_ex.fetch_balance()

        BOT_STATE["api_key"] = api_key
        BOT_STATE["api_secret"] = api_secret
        BOT_STATE["keys_connected"] = True

        free_usdt = float(bal.get("free", {}).get("USDT", 0.0))
        BOT_STATE["usdt_free"] = free_usdt
        add_log(f"🔑 Gate.io Live Bridge Connected! Available Spot USDT: ${free_usdt:.4f}")
        sync_existing_wallet(test_ex, bal)

        return jsonify({"status": "connected", "usdt_free": free_usdt}), 200
    except Exception as e:
        add_log(f"❌ API Key Connection Failed: {str(e)}")
        return jsonify({"error": f"Gate.io rejected keys: {str(e)}"}), 400


@app.route("/tick", methods=["GET", "POST"])
def tick():
    try:
        # Allow browser to auto-restore keys if Render container slept
        header_key = request.headers.get("X-Gate-Key", "").strip()
        header_secret = request.headers.get("X-Gate-Secret", "").strip()
        if header_key and header_secret and not BOT_STATE["api_key"]:
            BOT_STATE["api_key"] = header_key
            BOT_STATE["api_secret"] = header_secret

        data = execute_engine_tick()
        return jsonify({"status": "ok", "data": data}), 200
    except Exception as e:
        add_log(f"⚠️ Tick Error: {str(e)}")
        return jsonify({"status": "error", "message": str(e), "data": {
            "keys_connected": BOT_STATE["keys_connected"],
            "usdt_free": BOT_STATE["usdt_free"],
            "total_profit": BOT_STATE["realized_usdt_profit"],
            "skims": BOT_STATE["completed_skims"],
            "coins": BOT_STATE["coins"],
            "ledger": BOT_STATE["ledger"],
            "logs": BOT_STATE["logs"],
            "frozen": BOT_STATE["frozen"]
        }}), 200


@app.route("/launch", methods=["POST"])
def launch_coin():
    try:
        body = request.get_json() or {}
        if body.get("apiKey") and body.get("apiSecret") and not BOT_STATE["api_key"]:
            BOT_STATE["api_key"] = body["apiKey"].strip()
            BOT_STATE["api_secret"] = body["apiSecret"].strip()

        exchange = get_exchange()
        if not exchange:
            return jsonify({"error": "Tap '🔑 Live Exchange Bridge' at the top and paste your Gate.io API Keys first!"}), 400

        raw_coin = (body.get("symbol") or "").strip().upper()
        usdt_budget = float(body.get("budget") or 7.0)
        if not raw_coin:
            return jsonify({"error": "Enter a coin symbol first (e.g. QNT, LTC, SOL)."}), 400

        symbol = raw_coin if "/" in raw_coin else f"{raw_coin}/USDT"
        base = symbol.split("/")[0]

        exchange.load_markets()
        if symbol not in exchange.markets:
            return jsonify({"error": f"{symbol} pair not found on Gate.io Spot."}), 400

        balance = exchange.fetch_balance()
        existing_shares = float(balance.get("total", {}).get(base, 0.0))
        free_usdt = float(balance.get("free", {}).get("USDT", 0.0))
        BOT_STATE["usdt_free"] = free_usdt

        ticker = exchange.fetch_ticker(symbol)
        live_price = float(ticker["last"])

        if existing_shares * live_price >= 3.0:
            shares = existing_shares
            add_log(f"🎯 Locked onto existing {shares:.6f} {base} (${shares*live_price:.2f}) in Gate.io Spot!")
        else:
            if free_usdt < usdt_budget or usdt_budget < 3.0:
                return jsonify({
                    "error": f"You have ${free_usdt:.4f} Spot USDT available and {existing_shares:.6f} {base}. Need at least ${usdt_budget:.2f} USDT (or existing {base} shares) to launch!"
                }), 400
            buy_amt = float(exchange.amount_to_precision(symbol, (usdt_budget / live_price) * 0.998))
            exchange.create_market_buy_order(symbol, buy_amt)
            shares = buy_amt * 0.999
            add_log(f"✅ Bought initial {shares:.6f} {base} @ ${live_price:.4f} (${usdt_budget:.2f})")

        BOT_STATE["coins"][symbol] = {
            "symbol": symbol,
            "base": base,
            "protected_shares": shares,
            "real_shares": shares,
            "entry_price": live_price,
            "live_price": live_price,
            "position_value": shares * live_price,
            "harvest_target": live_price * (1 + TRIGGER_UP_PCT),
            "highest_peak": live_price,
            "trailing_active": False,
            "realized_profit": 0.0,
            "skims": 0,
            "status": "RATCHET_ARMED"
        }
        return jsonify({"status": "launched", "symbol": symbol}), 200
    except Exception as e:
        add_log(f"❌ Launch Error: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route("/toggle_freeze", methods=["POST"])
def toggle_freeze():
    BOT_STATE["frozen"] = not BOT_STATE["frozen"]
    state_txt = "FROZEN ❄️" if BOT_STATE["frozen"] else "RESUMED 🔥"
    add_log(f"System state changed: {state_txt}")
    return jsonify({"frozen": BOT_STATE["frozen"]}), 200


# =====================================================================
# PEAK COMMAND CENTER UI (Dark Cyberpunk Layout + In-App API Key Vault)
# =====================================================================
HTML_UI = """
<!DOCTYPE html>
<html>
<head>
  <meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1">
  <title>VaultSkim Command Center v6.0</title>
  <style>
    * { box-sizing: border-box; }
    body {
      background: #060a0f;
      color: #e6edf3;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, monospace;
      margin: 0;
      padding: 12px;
    }
    .card {
      background: linear-gradient(180deg, #0d1520 0%, #090f17 100%);
      border: 1px solid #1b2a3e;
      border-radius: 14px;
      padding: 14px;
      margin-bottom: 12px;
      box-shadow: 0 6px 20px rgba(0,0,0,0.45);
    }
    .header-title { font-size: 19px; font-weight: 800; letter-spacing: -0.3px; margin: 0; color: #fff; }
    .sub-caption { font-size: 11px; color: #7d8590; margin-top: 4px; }
    .pill-row { display: flex; flex-wrap: wrap; gap: 7px; margin-top: 10px; }
    .btn-gold {
      background: linear-gradient(90deg, #f59e0b, #eab308);
      color: #000;
      font-weight: 800;
      border: none;
      padding: 7px 12px;
      border-radius: 8px;
      font-size: 12px;
      cursor: pointer;
    }
    .btn-purple {
      background: #2e1065;
      color: #c4b5fd;
      border: 1px solid #5b21b6;
      padding: 7px 11px;
      border-radius: 8px;
      font-size: 12px;
      font-weight: 700;
      cursor: pointer;
    }
    .btn-red {
      background: #3b0d15;
      color: #fca5a5;
      border: 1px solid #7f1d1d;
      padding: 7px 11px;
      border-radius: 8px;
      font-size: 12px;
      font-weight: 700;
      cursor: pointer;
    }
    .status-pill {
      background: #0b1622;
      border: 1px solid #1f3247;
      padding: 6px 10px;
      border-radius: 8px;
      font-size: 12px;
      font-weight: 700;
      display: inline-flex;
      align-items: center;
      gap: 6px;
    }
    .dot-green { width: 8px; height: 8px; background: #22c55e; border-radius: 50%; display: inline-block; box-shadow: 0 0 8px #22c55e; }
    .dot-red { width: 8px; height: 8px; background: #ef4444; border-radius: 50%; display: inline-block; }
    .profit-hero {
      font-size: 36px;
      font-weight: 900;
      color: #22c55e;
      text-shadow: 0 0 18px rgba(34,197,94,0.35);
      margin: 6px 0;
    }
    .input-row { display: flex; gap: 8px; margin-top: 10px; }
    input {
      background: #05080c;
      border: 1px solid #263850;
      color: #fff;
      padding: 11px;
      border-radius: 9px;
      font-size: 14px;
      width: 100%;
    }
    .btn-launch {
      background: linear-gradient(90deg, #16a34a, #22c55e);
      color: #04130a;
      font-weight: 900;
      font-size: 14px;
      border: none;
      padding: 12px;
      border-radius: 9px;
      width: 100%;
      cursor: pointer;
    }
    .kv-row {
      display: flex;
      justify-content: space-between;
      padding: 6px 0;
      font-size: 13px;
      border-bottom: 1px solid #131e2c;
    }
    .green { color: #22c55e; font-weight: 700; }
    .cyan { color: #38bdf8; font-weight: 700; }
    .gold { color: #facc15; font-weight: 700; }
    .progress-bg { background: #070b10; height: 7px; border-radius: 6px; overflow: hidden; margin: 8px 0; border: 1px solid #182638; }
    .progress-bar { background: linear-gradient(90deg, #10b981, #38bdf8); height: 100%; width: 15%; transition: width 0.4s; }
    .log-box {
      background: #04070b;
      border: 1px solid #162232;
      font-family: monospace;
      font-size: 11.5px;
      padding: 10px;
      border-radius: 9px;
      max-height: 210px;
      overflow-y: auto;
      color: #4ade80;
      line-height: 1.5;
    }
    .ledger-item {
      background: #081018;
      border: 1px solid #162638;
      border-radius: 8px;
      padding: 8px 10px;
      margin-bottom: 6px;
      display: flex;
      justify-content: space-between;
      align-items: center;
      font-size: 12px;
    }
    #key-drawer { display: none; margin-top: 12px; padding-top: 12px; border-top: 1px dashed #263850; }
  </style>
</head>
<body>

  <!-- TOP COMMAND HEADER -->
  <div class="card">
    <div style="display:flex; justify-content:space-between; align-items:flex-start;">
      <div>
        <h1 class="header-title">VaultSkim Command Center</h1>
        <div class="sub-caption">v6.0 • Trailing Peak (+1.5% / 0.35% Pullback) • 100% Protected Share Floor</div>
      </div>
      <div class="status-pill" id="tick-clock">⏱️ Live</div>
    </div>

    <div class="pill-row">
      <button class="btn-gold" onclick="toggleKeyDrawer()">🔑 Live Exchange Bridge (API Keys)</button>
      <button class="btn-purple" onclick="refresh()">⚡ Sync & Auto-Heal</button>
      <button class="btn-red" onclick="toggleFreeze()" id="freeze-btn">❄️ Freeze Live</button>
    </div>

    <div class="pill-row">
      <div class="status-pill">
        <span id="bridge-dot" class="dot-red"></span>
        <span id="bridge-text">BRIDGE: ENTER KEYS</span>
      </div>
      <div class="status-pill">
        Gate.io Spot USDT: <span class="gold" id="usdt-bal">$0.0000</span>
      </div>
    </div>

    <!-- IN-APP API KEY VAULT DRAWER -->
    <div id="key-drawer">
      <div style="font-size:12px; font-weight:700; color:#facc15; margin-bottom:6px;">
        🔐 Paste Gate.io Spot API v4 Keys (Saved locally & synced to engine)
      </div>
      <div class="input-row">
        <input id="gate-key-input" placeholder="Gate.io API Key" autocomplete="off" />
      </div>
      <div class="input-row">
        <input id="gate-secret-input" type="password" placeholder="Gate.io Secret Key" autocomplete="off" />
      </div>
      <div class="input-row">
        <button class="btn-launch" onclick="saveAndConnectKeys()">⚡ Connect & Verify Gate.io Wallet</button>
      </div>
      <div id="key-msg" style="font-size:12px; margin-top:6px;"></div>
    </div>
  </div>

  <!-- TOTAL REALIZED PROFIT HERO -->
  <div class="card" style="text-align:center;">
    <div style="font-size:11px; color:#8b949e; font-weight:700; letter-spacing:0.5px;">
      TOTAL REALIZED GATE.IO PROFIT (PURE SKIMMED USDT)
    </div>
    <div class="profit-hero" id="total-profit">$0.0000</div>
    <div style="font-size:12px; color:#8b949e;">
      <span class="cyan" id="skim-count">0</span> Completed Peak Wave Skims • Zero Share Loss Guarantee
    </div>
  </div>

  <!-- LAUNCH OR LOCK EXISTING POSITION -->
  <div class="card">
    <div style="font-weight:800; font-size:13.5px;">🚀 Launch Any Coin or Lock Existing Gate.io Spot Position</div>
    <div class="sub-caption">If you already own the token on Gate.io, it locks onto your shares automatically!</div>
    <div class="input-row">
      <input id="coin-input" placeholder="Symbol (e.g. QNT, LTC, SOL, PEPE)" style="flex:2.2;" />
      <input id="budget-input" type="number" value="7" placeholder="USDT $" style="flex:1;" />
    </div>
    <div class="input-row">
      <button class="btn-launch" onclick="launchCoin()">+ Snipe & Launch Position</button>
    </div>
    <div id="err-msg" style="color:#f87171; font-size:12.5px; margin-top:8px; font-weight:600;"></div>
  </div>

  <!-- LIVE COIN POSITION CARDS -->
  <div id="coins-container"></div>

  <!-- VERIFIED SKIM LEDGER -->
  <div class="card">
    <div style="font-weight:800; font-size:13px; margin-bottom:8px;">📊 VERIFIED GATE.IO SKIM LEDGER</div>
    <div id="ledger-container" style="font-size:12px; color:#7d8590;">
      No completed skims yet — waiting for first +1.50% peak harvest.
    </div>
  </div>

  <!-- LIVE EXECUTION TERMINAL -->
  <div class="card">
    <div style="font-weight:800; font-size:13px; margin-bottom:8px;">⚡ LIVE EXECUTION & SHARE-SHIELD LOGGER</div>
    <div class="log-box" id="logs">Waiting for Gate.io API Key connection...</div>
  </div>

<script>
// Load saved keys from phone localStorage on boot
window.addEventListener('DOMContentLoaded', () => {
  const savedKey = localStorage.getItem('VAULTSKIM_GATE_KEY') || '';
  const savedSec = localStorage.getItem('VAULTSKIM_GATE_SECRET') || '';
  if (savedKey && savedSec) {
    document.getElementById('gate-key-input').value = savedKey;
    document.getElementById('gate-secret-input').value = savedSec;
  }
  refresh();
  setInterval(refresh, 12000);
});

function toggleKeyDrawer() {
  const el = document.getElementById('key-drawer');
  el.style.display = (el.style.display === 'block') ? 'none' : 'block';
}

async function saveAndConnectKeys() {
  const apiKey = document.getElementById('gate-key-input').value.trim();
  const apiSecret = document.getElementById('gate-secret-input').value.trim();
  const msgEl = document.getElementById('key-msg');

  if (!apiKey || !apiSecret) {
    msgEl.style.color = '#f87171';
    msgEl.innerText = 'Please paste both your Gate.io API Key and Secret Key.';
    return;
  }

  msgEl.style.color = '#38bdf8';
  msgEl.innerText = 'Connecting to Gate.io Spot wallet...';

  localStorage.setItem('VAULTSKIM_GATE_KEY', apiKey);
  localStorage.setItem('VAULTSKIM_GATE_SECRET', apiSecret);

  const res = await fetch('/connect_keys', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({apiKey, apiSecret})
  });
  const data = await res.json();
  if (data.error) {
    msgEl.style.color = '#f87171';
    msgEl.innerText = data.error;
  } else {
    msgEl.style.color = '#22c55e';
    msgEl.innerText = '✅ Connected! Available Spot USDT: $' + (data.usdt_free || 0).toFixed(4);
    setTimeout(() => { document.getElementById('key-drawer').style.display = 'none'; }, 1200);
    refresh();
  }
}

async function refresh() {
  const savedKey = localStorage.getItem('VAULTSKIM_GATE_KEY') || '';
  const savedSec = localStorage.getItem('VAULTSKIM_GATE_SECRET') || '';

  const res = await fetch('/tick', {
    headers: {
      'X-Gate-Key': savedKey,
      'X-Gate-Secret': savedSec
    }
  });
  const json = await res.json();
  const d = json.data;
  if (!d) return;

  const now = new Date();
  document.getElementById('tick-clock').innerText = '⏱️ ' + now.toLocaleTimeString();
  document.getElementById('usdt-bal').innerText = '$' + (d.usdt_free || 0).toFixed(4);
  document.getElementById('total-profit').innerText = '$' + (d.total_profit || 0).toFixed(4);
  document.getElementById('skim-count').innerText = (d.skims || 0);

  const dot = document.getElementById('bridge-dot');
  const btxt = document.getElementById('bridge-text');
  if (d.keys_connected) {
    dot.className = 'dot-green';
    btxt.innerText = 'LIVE BRIDGE ARMED (GATE.IO)';
  } else {
    dot.className = 'dot-red';
    btxt.innerText = 'TAP GOLD BUTTON TO INSERT KEYS';
    document.getElementById('key-drawer').style.display = 'block';
  }

  document.getElementById('freeze-btn').innerText = d.frozen ? '🔥 Unfreeze Bot' : '❄️ Freeze Live';

  // Render Active Coin Cards
  const container = document.getElementById('coins-container');
  container.innerHTML = '';
  Object.values(d.coins || {}).forEach(c => {
    const pctChange = c.entry_price ? (((c.live_price - c.entry_price) / c.entry_price) * 100) : 0;
    const progressPct = Math.max(5, Math.min(100, (pctChange / 1.5) * 100));
    const sign = pctChange >= 0 ? '+' : '';

    container.innerHTML += `
      <div class="card">
        <div style="display:flex; justify-content:space-between; align-items:center;">
          <div>
            <span style="font-size:18px; font-weight:900;">${c.symbol}</span>
            <div style="font-size:11px; color:#38bdf8;">● VERIFIED CEX (GATE.IO SPOT)</div>
          </div>
          <span class="status-pill" style="color:#4ade80; border-color:#166534;">${c.status}</span>
        </div>

        <div style="display:flex; align-items:baseline; gap:10px; margin:10px 0 4px 0;">
          <span style="font-size:28px; font-weight:900;">$${(c.live_price || 0).toFixed(4)}</span>
          <span class="${pctChange >= 0 ? 'green' : ''}" style="font-size:13px;">(${sign}${pctChange.toFixed(2)}% from Entry)</span>
        </div>

        <div class="progress-bg"><div class="progress-bar" style="width:${progressPct}%;"></div></div>

        <div class="kv-row"><span>Real Shares Owned (Gate.io):</span><span class="green">${(c.real_shares || 0).toFixed(6)} ${c.base}</span></div>
        <div class="kv-row"><span>Protected Share Floor:</span><span class="cyan">🛡️ ${(c.protected_shares || 0).toFixed(6)} ${c.base}</span></div>
        <div class="kv-row"><span>Total Position Value:</span><span style="font-weight:800;">$${(c.position_value || 0).toFixed(2)}</span></div>
        <div class="kv-row"><span>Entry / Anchor Price:</span><span>$${(c.entry_price || 0).toFixed(4)}</span></div>
        <div class="kv-row"><span>Next Trailing Harvest (+1.50%):</span><span class="gold">$${(c.harvest_target || 0).toFixed(4)}</span></div>
        <div class="kv-row"><span>Highest Peak Tracked:</span><span>$${(c.highest_peak || 0).toFixed(4)}</span></div>
        <div class="kv-row"><span>Realized USDT Profit (${c.base}):</span><span class="green">+$${(c.realized_profit || 0).toFixed(4)} (${c.skims} skims)</span></div>
      </div>`;
  });

  // Render Verified Skim Ledger
  const ledgerEl = document.getElementById('ledger-container');
  if (d.ledger && d.ledger.length > 0) {
    ledgerEl.innerHTML = d.ledger.map(item => `
      <div class="ledger-item">
        <div>
          <strong class="cyan">${item.base} [LIVE]</strong> Protected Re-Buy @ $${item.rebuy_price.toFixed(4)}
          <div style="font-size:11px; color:#7d8590;">Sold Peak @ $${item.sell_price.toFixed(4)} • ${item.time}</div>
        </div>
        <div style="text-align:right;">
          <div class="green">+$${item.profit.toFixed(4)} USDT</div>
          <div style="font-size:11px; color:#9ca3af;">${item.shares.toFixed(6)} sh protected</div>
        </div>
      </div>
    `).join('');
  }

  document.getElementById('logs').innerHTML = (d.logs || []).join('<br>') || 'Connected. Ready to launch or sync positions.';
}

async function launchCoin() {
  const symbol = document.getElementById('coin-input').value;
  const budget = document.getElementById('budget-input').value;
  const apiKey = localStorage.getItem('VAULTSKIM_GATE_KEY') || '';
  const apiSecret = localStorage.getItem('VAULTSKIM_GATE_SECRET') || '';

  const errEl = document.getElementById('err-msg');
  errEl.style.color = '#38bdf8';
  errEl.innerText = 'Checking Gate.io Spot wallet...';

  const res = await fetch('/launch', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({symbol, budget, apiKey, apiSecret})
  });
  const data = await res.json();
  if (data.error) {
    errEl.style.color = '#f87171';
    errEl.innerText = data.error;
  } else {
    errEl.innerText = '';
    document.getElementById('coin-input').value = '';
    refresh();
  }
}

async function toggleFreeze() {
  await fetch('/toggle_freeze', {method: 'POST'});
  refresh();
}
</script>
</body>
</html>
"""

@app.route("/", methods=["GET"])
def dashboard():
    return render_template_string(HTML_UI)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
