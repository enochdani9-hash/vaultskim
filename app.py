import os
import json
import time
from flask import Flask, request, jsonify, render_template_string
import ccxt

app = Flask(__name__)

STATE_FILE = "vault_state.json"

# =====================================================================
# BOT MEMORY & PERSISTENT STATE
# =====================================================================
DEFAULT_STATE = {
    "api_key": (os.environ.get("GATE_API_KEY") or "").strip(),
    "api_secret": (os.environ.get("GATE_SECRET") or "").strip(),
    "keys_connected": False,
    "frozen": False,
    "usdt_free": 0.0,
    "usdt_total": 0.0,
    "realized_usdt_profit": 0.0,
    "completed_skims": 0,
    "coins": {},
    "ledger": [],
    "logs": []
}

# Strategy Settings: +1.50% from EVERY NEW LOW, 0.35% Peak Pullback, -1.20% Rebuy
TRIGGER_UP_PCT = 1.50 / 100.0
TRAIL_PULLBACK_PCT = 0.35 / 100.0
REBUY_DIP_PCT = 1.20 / 100.0


def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                saved = json.load(f)
                for k, v in DEFAULT_STATE.items():
                    if k not in saved:
                        saved[k] = v
                return saved
        except Exception:
            pass
    return dict(DEFAULT_STATE)


def save_state():
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(BOT_STATE, f)
    except Exception:
        pass


BOT_STATE = load_state()


def add_log(msg):
    timestamp = time.strftime("%H:%M:%S")
    if not BOT_STATE["logs"] or not BOT_STATE["logs"][0].endswith(msg):
        BOT_STATE["logs"].insert(0, f"[{timestamp}] {msg}")
        BOT_STATE["logs"] = BOT_STATE["logs"][:50]
        save_state()


def get_exchange():
    key = BOT_STATE.get("api_key", "").strip()
    secret = BOT_STATE.get("api_secret", "").strip()
    if not key or not secret:
        return None
    return ccxt.gate({
        "apiKey": key,
        "secret": secret,
        "enableRateLimit": True,
        "options": {"defaultType": "spot"}
    })


def sync_existing_wallet(exchange, balance):
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
                        "initial_entry": price,
                        "lowest_low": price,
                        "live_price": price,
                        "position_value": val_usdt,
                        "harvest_target": price * (1 + TRIGGER_UP_PCT),
                        "highest_peak": price,
                        "trailing_active": False,
                        "realized_profit": 0.0,
                        "skims": 0,
                        "status": "SLIDING_LOW_ARMED",
                        "ai_note": f"Tracking bottom at ${price:.4f}. Target +1.50% bounce."
                    }
                    add_log(f"🛡️ Auto-Synced {shares:.6f} {asset} (${val_usdt:.2f}) | Sliding Low: ${price:.4f}")
    save_state()


# =====================================================================
# CORE AI SLIDING-LOW & PEAK HARVEST ENGINE
# =====================================================================
def execute_engine_tick():
    exchange = get_exchange()
    if not exchange:
        BOT_STATE["keys_connected"] = False
        return BOT_STATE

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
        save_state()
        return BOT_STATE

    for symbol, card in list(BOT_STATE["coins"].items()):
        base = card["base"]
        ticker = exchange.fetch_ticker(symbol)
        live_price = float(ticker["last"])
        card["live_price"] = live_price

        real_shares = float(balance.get("total", {}).get(base, 0.0))
        free_shares = float(balance.get("free", {}).get(base, 0.0))
        card["real_shares"] = real_shares
        card["position_value"] = real_shares * live_price

        # Ensure backwards compatibility if upgrading from v6.0
        if "lowest_low" not in card or card["lowest_low"] <= 0:
            card["lowest_low"] = card.get("entry_price", live_price)

        open_orders = exchange.fetch_open_orders(symbol)

        # -----------------------------------------------------------------
        # STATE 1: WAITING FOR PROTECTED LIMIT RE-BUY ON GATE.IO
        # -----------------------------------------------------------------
        if len(open_orders) > 0:
            order = open_orders[0]
            rebuy_px = float(order["price"])
            dist_pct = ((live_price - rebuy_px) / live_price) * 100.0
            card["status"] = f"WAITING_REBUY @ ${rebuy_px:.4f}"
            card["active_order_price"] = rebuy_px
            card["ai_note"] = f"Waiting for {dist_pct:.2f}% dip to ${rebuy_px:.4f} to restore {order['amount']} {base}."
            continue

        # -----------------------------------------------------------------
        # STATE 2: HOLDING SHARES -> SLIDE WITH EVERY NEW LOW & CATCH BOUNCE
        # -----------------------------------------------------------------
        if free_shares * live_price >= 3.0:
            if real_shares > card["protected_shares"]:
                card["protected_shares"] = real_shares

            # Did our Limit Rebuy just fill? Reset lowest_low and peak to current price!
            if card.get("status", "").startswith("WAITING_REBUY"):
                filled_px = card.get("active_order_price", live_price)
                card["lowest_low"] = min(live_price, filled_px)
                card["highest_peak"] = live_price
                card["trailing_active"] = False
                add_log(f"✅ {base} REBUY FILLED! Restored {real_shares:.6f} {base}. New Sliding Low: ${card['lowest_low']:.4f}")

            # YOUR UPGRADE: If price hits a new low while waiting, slide the anchor DOWN immediately!
            if not card["trailing_active"] and live_price < card["lowest_low"]:
                old_low = card["lowest_low"]
                card["lowest_low"] = live_price
                card["highest_peak"] = live_price
                card["harvest_target"] = live_price * (1 + TRIGGER_UP_PCT)
                if (old_low - live_price) / old_low >= 0.002:  # Log significant slides
                    add_log(
                        f"📉 {base} Sliding Low adjusted to ${live_price:.4f} "
                        f"-> New +1.50% Harvest Target lowered to ${card['harvest_target']:.4f}"
                    )

            low_anchor = card["lowest_low"]
            trigger_target = low_anchor * (1 + TRIGGER_UP_PCT)
            card["harvest_target"] = trigger_target

            if live_price > card["highest_peak"]:
                card["highest_peak"] = live_price

            bounce_pct = ((live_price - low_anchor) / low_anchor) * 100.0

            # Check if bounce from the lowest low has reached +1.50%
            if live_price >= trigger_target and not card["trailing_active"]:
                card["trailing_active"] = True
                add_log(f"🚀 {base} bounced +{bounce_pct:.2f}% from Low (${low_anchor:.4f})! Trailing Peak Chaser ARMED.")

            # If Trailing Peak Chaser is armed, ride the pump and sell on 0.35% pullback
            if card["trailing_active"]:
                stop_price = card["highest_peak"] * (1 - TRAIL_PULLBACK_PCT)
                card["status"] = f"TRAILING_PEAK (Stop ${stop_price:.4f})"
                card["ai_note"] = f"Riding pump! Peak: ${card['highest_peak']:.4f}. Will harvest if price pulls back to ${stop_price:.4f}."

                if live_price <= stop_price and live_price > low_anchor * 1.006:
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
                    card["ai_note"] = f"Harvested bounce! Waiting for rebuy at ${rebuy_price:.4f}."

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
                        f"💰 {base} BOUNCE HARVESTED @ ${live_price:.4f}! "
                        f"Locked +${net_skim:.4f} USDT & placed Protected Rebuy @ ${rebuy_price:.4f}"
                    )
            else:
                card["status"] = "SLIDING_LOW_ARMED"
                need_pct = max(0.0, 1.50 - bounce_pct)
                card["ai_note"] = f"Sliding Low locked at ${low_anchor:.4f}. Needs +{need_pct:.2f}% more bounce to arm harvest."

    save_state()
    return BOT_STATE


# =====================================================================
# FLASK ROUTES
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
        BOT_STATE["usdt_free"] = float(bal.get("free", {}).get("USDT", 0.0))

        add_log(f"🔑 Gate.io Bridge Armed! Spot USDT: ${BOT_STATE['usdt_free']:.4f}")
        sync_existing_wallet(test_ex, bal)
        save_state()

        return jsonify({"status": "connected", "usdt_free": BOT_STATE["usdt_free"]}), 200
    except Exception as e:
        add_log(f"❌ API Key Error: {str(e)}")
        return jsonify({"error": f"Gate.io rejected keys: {str(e)}"}), 400


@app.route("/tick", methods=["GET", "POST"])
def tick():
    try:
        header_key = request.headers.get("X-Gate-Key", "").strip()
        header_secret = request.headers.get("X-Gate-Secret", "").strip()
        if header_key and header_secret and not BOT_STATE.get("api_key"):
            BOT_STATE["api_key"] = header_key
            BOT_STATE["api_secret"] = header_secret
            save_state()

        data = execute_engine_tick()
        return jsonify({"status": "ok", "data": data}), 200
    except Exception as e:
        add_log(f"⚠️ Tick Notice: {str(e)}")
        return jsonify({"status": "error", "message": str(e), "data": BOT_STATE}), 200


@app.route("/launch", methods=["POST"])
def launch_coin():
    try:
        body = request.get_json() or {}
        if body.get("apiKey") and body.get("apiSecret") and not BOT_STATE.get("api_key"):
            BOT_STATE["api_key"] = body["apiKey"].strip()
            BOT_STATE["api_secret"] = body["apiSecret"].strip()

        exchange = get_exchange()
        if not exchange:
            return jsonify({"error": "Tap '🔑 Live Exchange Bridge' and connect your Gate.io API Keys first!"}), 400

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
            add_log(f"🎯 Locked onto existing {shares:.6f} {base} (${shares*live_price:.2f}) at Low ${live_price:.4f}!")
        else:
            if free_usdt < usdt_budget or usdt_budget < 3.0:
                return jsonify({
                    "error": f"Spot USDT is ${free_usdt:.4f} and {base} is {existing_shares:.6f}. Need ${usdt_budget:.2f} USDT or existing {base} shares!"
                }), 400
            buy_amt = float(exchange.amount_to_precision(symbol, (usdt_budget / live_price) * 0.998))
            exchange.create_market_buy_order(symbol, buy_amt)
            shares = buy_amt * 0.999
            add_log(f"✅ Sniped {shares:.6f} {base} @ ${live_price:.4f} (${usdt_budget:.2f})")

        BOT_STATE["coins"][symbol] = {
            "symbol": symbol,
            "base": base,
            "protected_shares": shares,
            "real_shares": shares,
            "initial_entry": live_price,
            "lowest_low": live_price,
            "live_price": live_price,
            "position_value": shares * live_price,
            "harvest_target": live_price * (1 + TRIGGER_UP_PCT),
            "highest_peak": live_price,
            "trailing_active": False,
            "realized_profit": 0.0,
            "skims": 0,
            "status": "SLIDING_LOW_ARMED",
            "ai_note": f"Sliding Low initialized at ${live_price:.4f}."
        }
        save_state()
        return jsonify({"status": "launched", "symbol": symbol}), 200
    except Exception as e:
        add_log(f"❌ Launch Error: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route("/reset_low", methods=["POST"])
def reset_low():
    """Manual button to snap the Sliding Low directly to current live price anytime."""
    body = request.get_json() or {}
    symbol = body.get("symbol")
    if symbol in BOT_STATE["coins"]:
        c = BOT_STATE["coins"][symbol]
        px = c.get("live_price", c.get("lowest_low"))
        c["lowest_low"] = px
        c["highest_peak"] = px
        c["harvest_target"] = px * (1 + TRIGGER_UP_PCT)
        c["trailing_active"] = False
        add_log(f"🔄 Manually snapped {symbol} Sliding Low to ${px:.4f} -> Target: ${c['harvest_target']:.4f}")
        save_state()
    return jsonify({"status": "ok"}), 200


@app.route("/toggle_freeze", methods=["POST"])
def toggle_freeze():
    BOT_STATE["frozen"] = not BOT_STATE["frozen"]
    state_txt = "FROZEN ❄️" if BOT_STATE["frozen"] else "RESUMED 🔥"
    add_log(f"System state changed: {state_txt}")
    save_state()
    return jsonify({"frozen": BOT_STATE["frozen"]}), 200


# =====================================================================
# SMOOTH v7.0 COMMAND CENTER UI (Sliding Low + AI Status Brain)
# =====================================================================
HTML_UI = """
<!DOCTYPE html>
<html>
<head>
  <meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1">
  <title>VaultSkim AI v7.0 — Sliding-Low Edition</title>
  <style>
    * { box-sizing: border-box; -webkit-tap-highlight-color: transparent; }
    body {
      background: #05080d;
      color: #e6edf3;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, monospace;
      margin: 0;
      padding: 12px;
    }
    .card {
      background: linear-gradient(180deg, #0d1624 0%, #080d15 100%);
      border: 1px solid #1c2d42;
      border-radius: 15px;
      padding: 14px;
      margin-bottom: 12px;
      box-shadow: 0 8px 24px rgba(0,0,0,0.5);
    }
    .header-title { font-size: 19px; font-weight: 900; letter-spacing: -0.3px; margin: 0; color: #fff; }
    .sub-caption { font-size: 11px; color: #8b949e; margin-top: 4px; line-height: 1.35; }
    .pill-row { display: flex; flex-wrap: wrap; gap: 7px; margin-top: 10px; }
    button { transition: transform 0.12s ease, opacity 0.15s ease; }
    button:active { transform: scale(0.97); opacity: 0.85; }
    .btn-gold {
      background: linear-gradient(90deg, #f59e0b, #eab308);
      color: #000;
      font-weight: 800;
      border: none;
      padding: 8px 12px;
      border-radius: 9px;
      font-size: 12px;
      cursor: pointer;
    }
    .btn-purple {
      background: #2e1065;
      color: #ddd6fe;
      border: 1px solid #6d28d9;
      padding: 8px 12px;
      border-radius: 9px;
      font-size: 12px;
      font-weight: 700;
      cursor: pointer;
    }
    .btn-red {
      background: #3b0d15;
      color: #fca5a5;
      border: 1px solid #991b1b;
      padding: 8px 12px;
      border-radius: 9px;
      font-size: 12px;
      font-weight: 700;
      cursor: pointer;
    }
    .status-pill {
      background: #0a1420;
      border: 1px solid #1e334a;
      padding: 6px 10px;
      border-radius: 8px;
      font-size: 11.5px;
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
      text-shadow: 0 0 20px rgba(34,197,94,0.35);
      margin: 6px 0;
    }
    .input-row { display: flex; gap: 8px; margin-top: 10px; }
    input {
      background: #04070b;
      border: 1px solid #263850;
      color: #fff;
      padding: 11px;
      border-radius: 9px;
      font-size: 14px;
      width: 100%;
    }
    .btn-launch {
      background: linear-gradient(90deg, #16a34a, #22c55e);
      color: #031108;
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
      padding: 6.5px 0;
      font-size: 13px;
      border-bottom: 1px solid #121d2b;
    }
    .green { color: #22c55e; font-weight: 700; }
    .cyan { color: #38bdf8; font-weight: 700; }
    .gold { color: #facc15; font-weight: 700; }
    .progress-bg { background: #05090f; height: 8px; border-radius: 6px; overflow: hidden; margin: 9px 0; border: 1px solid #19283c; }
    .progress-bar { background: linear-gradient(90deg, #10b981, #38bdf8, #facc15); height: 100%; width: 8%; transition: width 0.6s cubic-bezier(0.22, 1, 0.36, 1); }
    .ai-brain-box {
      background: rgba(56, 189, 248, 0.08);
      border: 1px solid rgba(56, 189, 248, 0.28);
      color: #bae6fd;
      padding: 8px 10px;
      border-radius: 8px;
      font-size: 12px;
      margin: 8px 0;
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 8px;
    }
    .btn-mini {
      background: #172554;
      color: #93c5fd;
      border: 1px solid #1d4ed8;
      padding: 5px 8px;
      border-radius: 6px;
      font-size: 11px;
      font-weight: 700;
      white-space: nowrap;
      cursor: pointer;
    }
    .log-box {
      background: #030609;
      border: 1px solid #152232;
      font-family: monospace;
      font-size: 11.5px;
      padding: 10px;
      border-radius: 9px;
      max-height: 210px;
      overflow-y: auto;
      color: #4ade80;
      line-height: 1.55;
    }
    .ledger-item {
      background: #070e17;
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
        <div class="sub-caption">v7.0 AI • Sliding-Low Ratchet (+1.50% From Every New Low) • 100% Share Floor</div>
      </div>
      <div class="status-pill" id="tick-clock">⏱️ Live</div>
    </div>

    <div class="pill-row">
      <button class="btn-gold" onclick="toggleKeyDrawer()">🔑 Live Exchange Bridge</button>
      <button class="btn-purple" onclick="refresh(true)">⚡ Sync & Auto-Heal</button>
      <button class="btn-red" onclick="toggleFreeze()" id="freeze-btn">❄️ Freeze Live</button>
    </div>

    <div class="pill-row">
      <div class="status-pill">
        <span id="bridge-dot" class="dot-red"></span>
        <span id="bridge-text">CHECKING BRIDGE...</span>
      </div>
      <div class="status-pill">
        Gate.io Spot USDT: <span class="gold" id="usdt-bal">$0.0000</span>
      </div>
    </div>

    <!-- IN-APP API KEY VAULT DRAWER -->
    <div id="key-drawer">
      <div style="font-size:12px; font-weight:700; color:#facc15; margin-bottom:6px;">
        🔐 Gate.io Spot API v4 Keys (Saved in Phone + Server Vault)
      </div>
      <div class="input-row">
        <input id="gate-key-input" placeholder="Gate.io API Key" autocomplete="off" />
      </div>
      <div class="input-row">
        <input id="gate-secret-input" type="password" placeholder="Gate.io Secret Key" autocomplete="off" />
      </div>
      <div class="input-row">
        <button class="btn-launch" onclick="saveAndConnectKeys()">⚡ Connect & Lock Gate.io Wallet</button>
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
      <span class="cyan" id="skim-count">0</span> Completed Wave Skims • Sliding-Low Bounce Catcher Active
    </div>
  </div>

  <!-- LAUNCH OR LOCK EXISTING POSITION -->
  <div class="card">
    <div style="font-weight:800; font-size:13.5px;">🚀 Launch Any Coin or Lock Existing Gate.io Spot Position</div>
    <div class="sub-caption">Automatically tracks every new dip low and triggers harvest on any +1.50% rebound!</div>
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
      No completed skims yet — watching for +1.50% bounce from Sliding Low.
    </div>
  </div>

  <!-- LIVE EXECUTION TERMINAL -->
  <div class="card">
    <div style="font-weight:800; font-size:13px; margin-bottom:8px;">⚡ LIVE EXECUTION & SHARE-SHIELD LOGGER</div>
    <div class="log-box" id="logs">Syncing live Gate.io state...</div>
  </div>

<script>
let isRefreshing = false;

window.addEventListener('DOMContentLoaded', () => {
  const savedKey = localStorage.getItem('VAULTSKIM_GATE_KEY') || '';
  const savedSec = localStorage.getItem('VAULTSKIM_GATE_SECRET') || '';
  if (savedKey && savedSec) {
    document.getElementById('gate-key-input').value = savedKey;
    document.getElementById('gate-secret-input').value = savedSec;
  }
  refresh();
  setInterval(refresh, 6000); // Smooth 6-second live pulse
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
    setTimeout(() => { document.getElementById('key-drawer').style.display = 'none'; }, 1000);
    refresh(true);
  }
}

async function snapLowNow(symbol) {
  await fetch('/reset_low', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({symbol})
  });
  refresh(true);
}

async function refresh(force = false) {
  if (isRefreshing && !force) return;
  isRefreshing = true;

  try {
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
    document.getElementById('total-profit').innerText = '$' + (d.realized_usdt_profit || 0).toFixed(4);
    document.getElementById('skim-count').innerText = (d.completed_skims || 0);

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

    // Render Coin Cards
    const container = document.getElementById('coins-container');
    const coinsList = Object.values(d.coins || {});
    container.innerHTML = coinsList.map(c => {
      const lowPx = c.lowest_low || c.entry_price || c.live_price || 1;
      const bouncePct = Math.max(0, ((c.live_price - lowPx) / lowPx) * 100);
      const progressPct = Math.max(4, Math.min(100, (bouncePct / 1.50) * 100));

      return `
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
            <span class="green" style="font-size:13px;">(+${bouncePct.toFixed(2)}% from Sliding Low)</span>
          </div>

          <div class="progress-bg"><div class="progress-bar" style="width:${progressPct}%;"></div></div>

          <div class="ai-brain-box">
            <span>🧠 <strong>AI:</strong> ${c.ai_note || 'Tracking live order book...'}</span>
            <button class="btn-mini" onclick="snapLowNow('${c.symbol}')">Snap Low Here</button>
          </div>

          <div class="kv-row"><span>Real Shares Owned (Gate.io):</span><span class="green">${(c.real_shares || 0).toFixed(6)} ${c.base}</span></div>
          <div class="kv-row"><span>Protected Share Floor:</span><span class="cyan">🛡️ ${(c.protected_shares || 0).toFixed(6)} ${c.base}</span></div>
          <div class="kv-row"><span>Total Position Value:</span><span style="font-weight:800;">$${(c.position_value || 0).toFixed(2)}</span></div>
          <div class="kv-row"><span>Sliding Low Anchor (Auto-Follows Dips):</span><span class="cyan">$${lowPx.toFixed(4)}</span></div>
          <div class="kv-row"><span>Next Trailing Harvest (+1.50% from Low):</span><span class="gold">$${(c.harvest_target || 0).toFixed(4)}</span></div>
          <div class="kv-row"><span>Highest Peak Tracked:</span><span>$${(c.highest_peak || 0).toFixed(4)}</span></div>
          <div class="kv-row"><span>Realized USDT Profit (${c.base}):</span><span class="green">+$${(c.realized_profit || 0).toFixed(4)} (${c.skims} skims)</span></div>
        </div>`;
    }).join('');

    // Render Ledger
    const ledgerEl = document.getElementById('ledger-container');
    if (d.ledger && d.ledger.length > 0) {
      ledgerEl.innerHTML = d.ledger.map(item => `
        <div class="ledger-item">
          <div>
            <strong class="cyan">${item.base} [LIVE]</strong> Protected Re-Buy @ $${item.rebuy_price.toFixed(4)}
            <div style="font-size:11px; color:#7d8590;">Sold Bounce Peak @ $${item.sell_price.toFixed(4)} • ${item.time}</div>
          </div>
          <div style="text-align:right;">
            <div class="green">+$${item.profit.toFixed(4)} USDT</div>
            <div style="font-size:11px; color:#9ca3af;">${item.shares.toFixed(6)} sh protected</div>
          </div>
        </div>
      `).join('');
    }

    document.getElementById('logs').innerHTML = (d.logs || []).join('<br>') || 'Engine armed and watching sliding lows.';
  } catch (e) {
    console.error(e);
  } finally {
    isRefreshing = false;
  }
}

async function launchCoin() {
  const symbol = document.getElementById('coin-input').value;
  const budget = document.getElementById('budget-input').value;
  const apiKey = localStorage.getItem('VAULTSKIM_GATE_KEY') || '';
  const apiSecret = localStorage.getItem('VAULTSKIM_GATE_SECRET') || '';

  const errEl = document.getElementById('err-msg');
  errEl.style.color = '#38bdf8';
  errEl.innerText = 'Syncing with Gate.io Spot wallet...';

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
    refresh(true);
  }
}

async function toggleFreeze() {
  await fetch('/toggle_freeze', {method: 'POST'});
  refresh(true);
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
