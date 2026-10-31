import os
import time
from flask import Flask, request, jsonify, render_template_string
import ccxt

app = Flask(__name__)

# Connect to Gate.io using ccxt.gate and your 2 Render Env Vars
exchange = ccxt.gate({
    'apiKey': os.environ.get('GATE_API_KEY'),
    'secret': os.environ.get('GATE_SECRET'),
    'enableRateLimit': True,
    'options': {'defaultType': 'spot'}
})

# =====================================================================
# BOT MEMORY STATE (Tracks Active Coins, Protected Share Floor & Logs)
# =====================================================================
BOT_STATE = {
    "frozen": False,
    "realized_usdt_profit": 0.0,
    "completed_skims": 0,
    "coins": {},   # e.g. "QNT/USDT": { ...card state... }
    "logs": []     # Live Execution Logger
}

# Fixed, Profitable Percentages (Clears Gate.io 0.2% round-trip fee + spread!)
TRIGGER_UP_PCT = 1.5 / 100.0      # Activate Trailing Peak Chaser at +1.50%
TRAIL_PULLBACK_PCT = 0.35 / 100.0 # Sell only when peak pulls back by 0.35%
REBUY_DIP_PCT = 1.2 / 100.0       # Rebuy 1.20% below sell price (100% Share Protection)


def add_log(msg):
    timestamp = time.strftime("%H:%M:%S")
    BOT_STATE["logs"].insert(0, f"[{timestamp}] {msg}")
    BOT_STATE["logs"] = BOT_STATE["logs"][:40]  # Keep latest 40 logs


def sync_existing_wallet():
    """Auto-imports any coin you already hold on Gate.io (like your QNT) on startup!"""
    try:
        exchange.load_markets()
        balance = exchange.fetch_balance()
        ignored = {'USDT', 'USDC', 'USD', 'POINT'}

        for asset, total_amt in balance.get('total', {}).items():
            if asset not in ignored and float(total_amt) > 0:
                symbol = f"{asset}/USDT"
                if symbol in exchange.markets and symbol not in BOT_STATE["coins"]:
                    ticker = exchange.fetch_ticker(symbol)
                    price = float(ticker['last'])
                    val_usdt = float(total_amt) * price
                    if val_usdt >= 3.0:
                        BOT_STATE["coins"][symbol] = {
                            "symbol": symbol,
                            "base": asset,
                            "protected_shares": float(total_amt),
                            "real_shares": float(total_amt),
                            "entry_price": price,
                            "live_price": price,
                            "position_value": val_usdt,
                            "harvest_target": price * (1 + TRIGGER_UP_PCT),
                            "highest_peak": price,
                            "trailing_active": False,
                            "realized_profit": 0.0,
                            "skims": 0,
                            "status": "RATCHET_ARMED (Watching Pump)"
                        }
                        add_log(f"Synced existing Gate.io position: {float(total_amt):.6f} {asset} (${val_usdt:.2f})")
    except Exception as e:
        add_log(f"Startup sync check: {str(e)}")


# =====================================================================
# CORE TRADING ENGINE (Called by Cronjob /tick AND UI Refresh)
# =====================================================================
def execute_engine_tick():
    if BOT_STATE["frozen"]:
        return {"status": "frozen"}

    exchange.load_markets()
    balance = exchange.fetch_balance()
    free_usdt = float(balance.get('free', {}).get('USDT', 0.0))

    # Auto-detect existing coin if none loaded yet
    if not BOT_STATE["coins"]:
        sync_existing_wallet()

    for symbol, card in list(BOT_STATE["coins"].items()):
        base = card["base"]
        ticker = exchange.fetch_ticker(symbol)
        live_price = float(ticker['last'])
        card["live_price"] = live_price

        real_shares = float(balance.get('total', {}).get(base, 0.0))
        free_shares = float(balance.get('free', {}).get(base, 0.0))
        card["real_shares"] = real_shares
        card["position_value"] = real_shares * live_price

        open_orders = exchange.fetch_open_orders(symbol)

        # -----------------------------------------------------------------
        # 1. IF AN OPEN REBUY ORDER IS ON GATE.IO -> WAIT FOR FILL
        #    (NO TOXIC +0.20% UPWARD GUARD! NEVER REBUY FEWER SHARES!)
        # -----------------------------------------------------------------
        if len(open_orders) > 0:
            order = open_orders[0]
            card["status"] = f"WAITING_REBUY @ ${order['price']}"
            card["active_order_price"] = float(order['price'])
            continue

        # -----------------------------------------------------------------
        # 2. IF WE HAVE OUR SHARES -> TRAIL THE PUMP & SELL AT THE PEAK
        # -----------------------------------------------------------------
        if free_shares * live_price >= 3.0:
            # Update Protected Share Baseline if our shares grew!
            if real_shares > card["protected_shares"]:
                card["protected_shares"] = real_shares

            # Did our Limit Rebuy just fill? Reset entry_price to our rebuy price
            if card.get("status", "").startswith("WAITING_REBUY"):
                card["entry_price"] = card.get("active_order_price", live_price)
                card["highest_peak"] = live_price
                card["trailing_active"] = False
                add_log(f"✅ {base} REBUY FILLED! Restored {real_shares:.6f} {base} (Protected Floor Intact!)")

            entry = card["entry_price"]
            trigger_target = entry * (1 + TRIGGER_UP_PCT)
            card["harvest_target"] = trigger_target

            # Track highest peak reached
            if live_price > card["highest_peak"]:
                card["highest_peak"] = live_price

            # Activate trailing once price crosses +1.50%
            if live_price >= trigger_target and not card["trailing_active"]:
                card["trailing_active"] = True
                add_log(f"🚀 {base} crossed +{TRIGGER_UP_PCT*100:.2f}% (${live_price:.4f})! Trailing Peak Chaser ARMED.")

            # If Trailing is Armed, check if it pulled back from the highest peak
            if card["trailing_active"]:
                stop_price = card["highest_peak"] * (1 - TRAIL_PULLBACK_PCT)
                card["status"] = f"TRAILING_PEAK (Sell Stop: ${stop_price:.4f})"

                if live_price <= stop_price and live_price > entry * 1.006:
                    # 1. Sell at the pullback from the peak!
                    sell_amt = float(exchange.amount_to_precision(symbol, free_shares))
                    exchange.create_market_sell_order(symbol, sell_amt)

                    # 2. Calculate Strict Rebuy Price (-1.2% below sell price)
                    rebuy_price = float(exchange.price_to_precision(symbol, live_price * (1 - REBUY_DIP_PCT)))

                    # 3. Guarantee we rebuy AT LEAST 100% of our Protected Shares (+0.2% fee buffer)
                    target_rebuy_shares = max(card["protected_shares"], sell_amt) * 1.002
                    rebuy_amt = float(exchange.amount_to_precision(symbol, target_rebuy_shares))

                    # Place Limit Buy on Gate.io immediately
                    exchange.create_limit_buy_order(symbol, rebuy_amt, rebuy_price)

                    # Calculate pure USDT profit skimmed and left untouched in wallet
                    usdt_from_sell = (sell_amt * live_price) * 0.999
                    usdt_for_rebuy = (rebuy_amt * rebuy_price)
                    net_skim = max(0.0, usdt_from_sell - usdt_for_rebuy)

                    card["realized_profit"] += net_skim
                    card["skims"] += 1
                    BOT_STATE["realized_usdt_profit"] += net_skim
                    BOT_STATE["completed_skims"] += 1

                    card["trailing_active"] = False
                    card["status"] = f"WAITING_REBUY @ ${rebuy_price}"
                    card["active_order_price"] = rebuy_price

                    add_log(
                        f"💰 {base} PEAK HARVESTED @ ${live_price:.4f}! "
                        f"Locked +${net_skim:.4f} USDT profit & placed Protected Rebuy for {rebuy_amt} {base} @ ${rebuy_price:.4f}"
                    )
            else:
                card["status"] = "RATCHET_ARMED (Watching Pump)"

    return {
        "usdt_free": free_usdt,
        "total_profit": BOT_STATE["realized_usdt_profit"],
        "skims": BOT_STATE["completed_skims"],
        "coins": BOT_STATE["coins"],
        "logs": BOT_STATE["logs"],
        "frozen": BOT_STATE["frozen"]
    }


# =====================================================================
# API ROUTES (Launch Coin From Input Box, Cron Tick, Freeze)
# =====================================================================
@app.route('/tick', methods=['GET', 'POST'])
def tick():
    try:
        data = execute_engine_tick()
        return jsonify({"status": "ok", "data": data}), 200
    except Exception as e:
        add_log(f"⚠️ Tick Error: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/launch', methods=['POST'])
def launch_coin():
    try:
        body = request.get_json() or {}
        raw_coin = (body.get("symbol") or "").strip().upper()
        usdt_budget = float(body.get("budget") or 7.0)

        if not raw_coin:
            return jsonify({"error": "Type a coin symbol first (e.g. QNT, LTC, SOL)"}), 400

        symbol = raw_coin if "/" in raw_coin else f"{raw_coin}/USDT"
        base = symbol.split("/")[0]

        exchange.load_markets()
        if symbol not in exchange.markets:
            return jsonify({"error": f"{symbol} does not exist on Gate.io Spot!"}), 400

        balance = exchange.fetch_balance()
        existing_shares = float(balance.get('total', {}).get(base, 0.0))
        free_usdt = float(balance.get('free', {}).get('USDT', 0.0))
        ticker = exchange.fetch_ticker(symbol)
        live_price = float(ticker['last'])

        # Case 1: You ALREADY hold this coin in Gate.io (e.g. your QNT!) -> Lock onto it!
        if existing_shares * live_price >= 3.0:
            shares = existing_shares
            add_log(f"🎯 Locked onto existing {shares:.6f} {base} (${shares*live_price:.2f}) in Gate.io!")
        # Case 2: You don't hold it yet -> Check if you actually have enough USDT to buy it!
        else:
            if free_usdt < usdt_budget or usdt_budget < 3.0:
                return jsonify({
                    "error": f"Insufficient USDT! You asked to launch ${usdt_budget} of {base}, but your Gate.io Spot USDT is only ${free_usdt:.2f}."
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
            "status": "RATCHET_ARMED (Watching Pump)"
        }
        return jsonify({"status": "launched", "symbol": symbol}), 200

    except Exception as e:
        add_log(f"❌ Launch Error: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route('/remove', methods=['POST'])
def remove_coin():
    symbol = (request.get_json() or {}).get("symbol")
    if symbol in BOT_STATE["coins"]:
        del BOT_STATE["coins"][symbol]
        add_log(f"Removed {symbol} card from dashboard.")
    return jsonify({"status": "removed"}), 200


# =====================================================================
# MOBILE DARK-MODE COMMAND CENTER UI (No URLs Needed!)
# =====================================================================
HTML_UI = """
<!DOCTYPE html>
<html>
<head>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>VaultSkim Pro — Trailing Peak & Share Shield</title>
  <style>
    body { background:#0b1017; color:#e6edf3; font-family:system-ui,-apple-system,sans-serif; margin:0; padding:12px; }
    .card { background:#121a24; border:1px solid #1f2d3d; border-radius:12px; padding:14px; margin-bottom:14px; }
    .green { color:#2ea043; font-weight:700; }
    .cyan { color:#38bdf8; font-weight:700; }
    .gold { color:#facc15; font-weight:700; }
    .badge { background:#0f291e; color:#3fb950; border:1px solid #238636; padding:4px 8px; border-radius:6px; font-size:12px; }
    .input-row { display:flex; gap:8px; margin-top:10px; }
    input { background:#090d13; border:1px solid #30363d; color:#fff; padding:10px; border-radius:8px; font-size:15px; }
    button { background:#1f883d; color:#fff; border:none; padding:10px 14px; border-radius:8px; font-weight:700; cursor:pointer; }
    .row { display:flex; justify-content:space-between; padding:5px 0; font-size:14px; border-bottom:1px solid #1a2430; }
    .log-box { background:#070a0e; font-family:monospace; font-size:12px; padding:10px; border-radius:8px; max-height:220px; overflow-y:auto; color:#3fb950; }
  </style>
</head>
<body>
  <div class="card">
    <div style="display:flex;justify-content:space-between;align-items:center;">
      <h3 style="margin:0;">🛡️ VaultSkim v2 (Share-Shield)</h3>
      <span class="badge" id="usdt-bal">Gate.io USDT: $...</span>
    </div>
    <p style="font-size:12px;color:#8b949e;margin:6px 0 0 0;">
      Trailing Peak (+1.5% Trigger / 0.35% Pullback) • Strict 100% Share Floor (No Toxic +0.20% Rebuy)
    </p>
  </div>

  <div class="card" style="text-align:center;">
    <div style="font-size:12px;color:#8b949e;">REALIZED GATE.IO USDT PROFIT (PURE SKIMS)</div>
    <div style="font-size:32px;margin:6px 0;" class="green" id="total-profit">$0.0000</div>
    <div style="font-size:12px;color:#8b949e;" id="skim-count">0 Completed Peak Skims</div>
  </div>

  <div class="card">
    <div style="font-weight:700;font-size:14px;">🚀 Launch Coin or Lock Existing Gate.io Position</div>
    <div class="input-row">
      <input id="coin-input" placeholder="Symbol (e.g. QNT, LTC, SOL)" style="flex:2;" />
      <input id="budget-input" type="number" value="7" placeholder="USDT $" style="flex:1;width:60px;" />
    </div>
    <div class="input-row">
      <button onclick="launchCoin()" style="width:100%;">+ Snipe & Launch</button>
    </div>
    <div id="err-msg" style="color:#f85149;font-size:13px;margin-top:8px;"></div>
  </div>

  <div id="coins-container"></div>

  <div class="card">
    <div style="font-weight:700;font-size:13px;margin-bottom:8px;">⚡ LIVE EXECUTION & SHARE-SHIELD LOGGER</div>
    <div class="log-box" id="logs">Loading Gate.io state...</div>
  </div>

<script>
async function refresh() {
  const res = await fetch('/tick');
  const json = await res.json();
  if (!json.data) return;
  const d = json.data;
  document.getElementById('usdt-bal').innerText = 'Gate.io USDT: $' + (d.usdt_free || 0).toFixed(4);
  document.getElementById('total-profit').innerText = '$' + (d.total_profit || 0).toFixed(4);
  document.getElementById('skim-count').innerText = (d.skims || 0) + ' Completed Peak Skims';

  const container = document.getElementById('coins-container');
  container.innerHTML = '';
  Object.values(d.coins || {}).forEach(c => {
    container.innerHTML += `
      <div class="card">
        <div style="display:flex;justify-content:space-between;align-items:center;">
          <span style="font-size:18px;font-weight:800;">${c.symbol}</span>
          <span class="badge">${c.status}</span>
        </div>
        <div style="font-size:26px;font-weight:800;margin:8px 0;">$${(c.live_price || 0).toFixed(4)}</div>
        <div class="row"><span>Real Shares Owned (Gate.io):</span><span class="green">${(c.real_shares || 0).toFixed(6)} ${c.base}</span></div>
        <div class="row"><span>Protected Share Floor (Locked):</span><span class="cyan">${(c.protected_shares || 0).toFixed(6)} ${c.base}</span></div>
        <div class="row"><span>Total Position Value:</span><span>$${(c.position_value || 0).toFixed(2)}</span></div>
        <div class="row"><span>Entry / Reference Price:</span><span>$${(c.entry_price || 0).toFixed(4)}</span></div>
        <div class="row"><span>Trailing Activation (+1.5%):</span><span class="gold">$${(c.harvest_target || 0).toFixed(4)}</span></div>
        <div class="row"><span>Highest Peak Seen:</span><span>$${(c.highest_peak || 0).toFixed(4)}</span></div>
        <div class="row"><span>Realized USDT Profit (${c.base}):</span><span class="green">+$${(c.realized_profit || 0).toFixed(4)} (${c.skims} skims)</span></div>
      </div>`;
  });

  document.getElementById('logs').innerHTML = (d.logs || []).join('<br>') || 'No logs yet.';
}

async function launchCoin() {
  const symbol = document.getElementById('coin-input').value;
  const budget = document.getElementById('budget-input').value;
  document.getElementById('err-msg').innerText = 'Checking Gate.io wallet...';
  const res = await fetch('/launch', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({symbol, budget})
  });
  const data = await res.json();
  if (data.error) {
    document.getElementById('err-msg').innerText = data.error;
  } else {
    document.getElementById('err-msg').innerText = '';
    document.getElementById('coin-input').value = '';
    refresh();
  }
}

refresh();
setInterval(refresh, 15000);
</script>
</body>
</html>
"""

@app.route('/', methods=['GET'])
def dashboard():
    return render_template_string(HTML_UI)


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
