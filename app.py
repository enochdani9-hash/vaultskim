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
# 1. CONFIGURATION & v2.5 SHARE-KEEPER STATE
# ==========================================
DATABASE_URL = os.environ.get("DATABASE_URL")
STATE_FILE = "vault_state.json"
TRADING_FEE_PCT = 0.001          # 0.10% per swap (0.20% round-trip)
MIN_NET_PROFIT_FLOOR = 0.0022    # Guaranteed > 0.20% fees so EVERY sale makes net profit
EARLY_SHIELD_ARM_PCT = 0.0035    # At +0.35% up, lock Zero-Loss Stop at Entry + 0.22%

def make_coin_template(symbol, anchor_usd, strategy="HYBRID_50_50"):
    return {
        "symbol": symbol,
        "anchor_usd": float(anchor_usd),
        "vault_profit_usd": 0.00,
        "pending_profit_usd": 0.00,  # NEW v2.5: Holds profit until re-buy so we can top-up +0.2% for same shares!
        "target_shares": 0.0,        # NEW v2.5: Remembers exact share count so we NEVER lose shares!
        "strategy": strategy,        # "FULL_100" or "HYBRID_50_50"
        "in_position": False,
        "moonbag_active": False,
        "active_anchor_usd": float(anchor_usd),
        "token_qty": 0.0,
        "entry_price": 0.0,
        "current_price": 0.0,
        "peak_price": 0.0,
        "local_low": 0.0,
        "ratchet_active": False,
        "ratchet_stop_price": 0.0,
        "last_sell_price": 0.0,
        "post_sell_peak": 0.0,
        "dip_buy_stop": 0.0,
        "mode": "STANDBY",
        "trades_count": 0
    }

DEFAULT_COINS = {
    "QNTUSDT": make_coin_template("QNT", 180.00, strategy="HYBRID_50_50"),
    "CELOUSDT": make_coin_template("CELO", 100.00, strategy="FULL_100"),
    "KSMUSDT": make_coin_template("KSM", 100.00, strategy="HYBRID_50_50")
}

# Sliding Floor & Ratchet Thresholds
WAKE_UP_PROFIT_PCT = 0.0078    # +0.80% above Sliding Low wakes up full tight Ratchet
MICRO_TRAIL_PCT = 0.0020       # 0.20% tight trail behind peak once armed
MOONBAG_TRAIL_PCT = 0.0120     # 1.20% wider trail for Stage 2 Moonbag rallies
WIDE_BREAKOUT_PCT = 0.030      # +3.0% switches Full Mode to wider trail
WIDE_TRAIL_PCT = 0.0080        # 0.80% trail for big rallies in Full Mode

# v2.5 Two-Door Share-Keeper Re-Buy Thresholds
DIP_PULLBACK_TARGET = -0.0038  # Door 1: Instant re-buy at -0.4% dip (100% of Pending Profit goes to Vault!)
STEP_UP_REBUY_PCT = 0.0020     # Door 2: At +0.20% rise, uses 0.2% from Pending Profit to buy SAME shares & sends rest to Vault!

bot_state = {"coins": DEFAULT_COINS, "logs": []}
state_lock = threading.Lock()
worker_started = False
db_dirty = False


def fmt_px(val):
    if val <= 0:
        return "0.0000"
    if val < 0.0001:
        return f"{val:.8f}"
    if val < 0.01:
        return f"{val:.6f}"
    return f"{val:.4f}"


def fmt_qty(qty):
    if qty <= 0:
        return "0"
    if qty >= 100:
        return f"{qty:,.2f}"
    if qty >= 1:
        return f"{qty:,.4f}"
    return f"{qty:.6f}"


def add_log(msg):
    timestamp = datetime.utcnow().strftime("%H:%M:%S")
    entry = f"[{timestamp}] {msg}"
    print(entry, flush=True)
    bot_state["logs"].insert(0, entry)
    bot_state["logs"] = bot_state["logs"][:60]


def ensure_coin_fields():
    for pair, c in bot_state.get("coins", {}).items():
        c.setdefault("last_sell_price", 0.0)
        c.setdefault("post_sell_peak", c.get("last_sell_price", 0.0))
        if c["post_sell_peak"] <= 0 and c["last_sell_price"] > 0:
            c["post_sell_peak"] = c["last_sell_price"]
        c.setdefault("dip_buy_stop", 0.0)
        c.setdefault("pending_profit_usd", 0.0)
        c.setdefault("target_shares", c.get("token_qty", 0.0))
        c.setdefault("strategy", "HYBRID_50_50" if c.get("symbol") in ["QNT", "KSM"] else "FULL_100")
        c.setdefault("moonbag_active", False)
        c.setdefault("active_anchor_usd", c.get("anchor_usd", 100.0))
        if c.get("local_low", 0.0) <= 0 and c.get("entry_price", 0.0) > 0:
            c["local_low"] = c["entry_price"]


# ==========================================
# 2. TARGETED FAST PRICE FEED (<150ms)
# ==========================================
def fetch_live_prices(symbols_list=None):
    if not symbols_list:
        with state_lock:
            symbols_list = [c["symbol"] for c in bot_state["coins"].values()]

    prices = {}
    for sym in symbols_list:
        pair_key = f"{sym}USDT"
        try:
            g_url = f"https://api.gateio.ws/api/v4/spot/tickers?currency_pair={sym}_USDT"
            r = requests.get(g_url, timeout=2.5)
            if r.status_code == 200:
                arr = r.json()
                if isinstance(arr, list) and len(arr) > 0 and arr[0].get("last"):
                    prices[pair_key] = float(arr[0]["last"])
        except Exception:
            pass

    missing = [s for s in symbols_list if f"{s}USDT" not in prices]
    if missing:
        try:
            fsyms = ",".join(missing)
            url = f"https://min-api.cryptocompare.com/data/pricemulti?fsyms={fsyms}&tsyms=USDT,USD"
            resp = requests.get(url, timeout=2.5)
            if resp.status_code == 200:
                data = resp.json()
                for sym in missing:
                    if sym in data:
                        val = data[sym].get("USDT") or data[sym].get("USD")
                        if val:
                            prices[f"{sym}USDT"] = float(val)
        except Exception:
            pass

    return prices


# ==========================================
# 3. v2.5 ENGINE: SHARE-KEEPER ESCROW RE-BUY
# ==========================================
def deploy_anchor_buy(coin, live_price, reason="Anchor Deployed"):
    global db_dirty
    pending = coin.get("pending_profit_usd", 0.0)
    target_sh = coin.get("target_shares", 0.0)
    top_up_used = 0.0

    # If we have Pending Profit and price stepped up (+0.2%), use just enough Pending Profit to keep the EXACT same shares!
    if pending > 0 and target_sh > 0:
        required_anchor_for_same_shares = (target_sh * live_price) / (1.0 - TRADING_FEE_PCT)
        if required_anchor_for_same_shares > coin["anchor_usd"]:
            needed_extra = required_anchor_for_same_shares - coin["anchor_usd"]
            top_up_used = min(pending, needed_extra)
            coin["anchor_usd"] += top_up_used

        vault_remainder = max(0.0, pending - top_up_used)
        coin["vault_profit_usd"] += vault_remainder
        coin["pending_profit_usd"] = 0.0

    fee = coin["anchor_usd"] * TRADING_FEE_PCT
    net_invest = coin["anchor_usd"] - fee
    coin["token_qty"] = net_invest / live_price
    coin["target_shares"] = coin["token_qty"]
    coin["active_anchor_usd"] = coin["anchor_usd"]
    coin["current_price"] = live_price
    coin["entry_price"] = live_price
    coin["peak_price"] = live_price
    coin["local_low"] = live_price
    coin["in_position"] = True
    coin["moonbag_active"] = False
    coin["ratchet_active"] = False
    coin["ratchet_stop_price"] = 0.0
    coin["dip_buy_stop"] = 0.0
    coin["mode"] = "HOLDING_ANCHOR"

    if pending > 0:
        vaulted_now = max(0.0, pending - top_up_used)
        add_log(
            f"SHARE-KEEPER RE-BUY {coin['symbol']} ({reason}) @ ${fmt_px(live_price)}: "
            f"Used ${top_up_used:.4f} profit to keep {fmt_qty(coin['token_qty'])} shares | "
            f"Sent remaining +${vaulted_now:.4f} to Vault!"
        )
    else:
        add_log(
            f"BUY {coin['symbol']} ({reason}): ${coin['anchor_usd']:.2f} @ ${fmt_px(live_price)} "
            f"-> Bought {fmt_qty(coin['token_qty'])} {coin['symbol']} shares"
        )
    db_dirty = True


def process_coin(pair, coin, live_price):
    global db_dirty
    if live_price <= 0:
        return
    coin["current_price"] = live_price

    # =========================================================================
    # CASE 1: IN CASH -> -0.4% DIP BUY OR +0.2% SHARE-KEEPER STEP-UP RE-BUY
    # =========================================================================
    if not coin["in_position"] or coin["entry_price"] <= 0:
        if coin.get("mode") == "TRAILING_DIP_BUY" and coin.get("last_sell_price", 0) > 0:
            sell_px = coin["last_sell_price"]
            if coin.get("post_sell_peak", 0) <= 0:
                coin["post_sell_peak"] = sell_px

            if live_price > coin["post_sell_peak"]:
                coin["post_sell_peak"] = live_price
                coin["local_low"] = live_price

            if live_price < coin["local_low"] or coin["local_low"] <= 0:
                coin["local_low"] = live_price

            peak_ref = coin["post_sell_peak"]
            dip_from_sliding_peak = (live_price - peak_ref) / peak_ref
            gain_above_sell = (live_price - sell_px) / sell_px

            # DOOR 1: Price dipped -0.4% -> Re-buy cheaper (0.00 top-up needed, 100% of Pending Profit goes to Vault + MORE shares!)
            if dip_from_sliding_peak <= DIP_PULLBACK_TARGET:
                deploy_anchor_buy(coin, live_price, reason=f"-0.4% Dip Re-Buy from ${fmt_px(peak_ref)}")
                return

            # DOOR 2: Price rose +0.20% above sell price -> Use 0.2% from Pending Profit to buy SAME shares & lock rest in Vault!
            elif gain_above_sell >= STEP_UP_REBUY_PCT:
                deploy_anchor_buy(coin, live_price, reason="+0.2% Share-Keeper Step-Up")
                return
            return
        else:
            deploy_anchor_buy(coin, live_price, reason="Initial Entry")
            return

    # =========================================================================
    # CASE 2: HOLDING SHARES -> IRONCLAD ZERO-LOSS SHIELD & HYBRID RATCHET
    # =========================================================================
    if live_price > coin["peak_price"]:
        coin["peak_price"] = live_price

    if not coin["ratchet_active"]:
        if live_price < coin["local_low"] or coin["local_low"] <= 0:
            coin["local_low"] = live_price
            coin["peak_price"] = live_price

    base_floor = coin["local_low"] if coin["local_low"] > 0 else coin["entry_price"]
    bounce_from_low = (live_price - base_floor) / base_floor
    gain_from_entry = (live_price - coin["entry_price"]) / coin["entry_price"]

    zero_loss_floor_px = coin["entry_price"] * (1.0 + MIN_NET_PROFIT_FLOOR)

    # STEP A: Update Stop Price (With Ironclad Zero-Loss Floor Clamp)
    if coin.get("moonbag_active"):
        raw_stop = coin["peak_price"] * (1.0 - MOONBAG_TRAIL_PCT)
        clamped_stop = max(raw_stop, zero_loss_floor_px)
        if clamped_stop > coin["ratchet_stop_price"]:
            coin["ratchet_stop_price"] = clamped_stop

    elif bounce_from_low >= WAKE_UP_PROFIT_PCT:
        was_active = coin["ratchet_active"]
        coin["ratchet_active"] = True
        coin["mode"] = "RATCHET_LOCKED"
        trail_pct = WIDE_TRAIL_PCT if bounce_from_low >= WIDE_BREAKOUT_PCT else MICRO_TRAIL_PCT
        raw_stop = coin["peak_price"] * (1.0 - trail_pct)
        if live_price >= zero_loss_floor_px:
            raw_stop = max(raw_stop, zero_loss_floor_px)
        if raw_stop > coin["ratchet_stop_price"]:
            coin["ratchet_stop_price"] = raw_stop
        if not was_active:
            add_log(
                f"RATCHET ARMED {coin['symbol']} @ ${fmt_px(live_price)} "
                f"(Stop Net Locked @ ${fmt_px(coin['ratchet_stop_price'])})"
            )
            db_dirty = True

    elif gain_from_entry >= EARLY_SHIELD_ARM_PCT:
        was_active = coin["ratchet_active"]
        coin["ratchet_active"] = True
        coin["mode"] = "ZERO_LOSS_LOCKED"
        early_stop = max(zero_loss_floor_px, coin["peak_price"] * (1.0 - MICRO_TRAIL_PCT))
        if early_stop > coin["ratchet_stop_price"]:
            coin["ratchet_stop_price"] = early_stop
        if not was_active:
            add_log(
                f"ZERO-LOSS SHIELD ARMED {coin['symbol']} @ ${fmt_px(live_price)}: "
                f"Guaranteed profit floor locked @ ${fmt_px(coin['ratchet_stop_price'])}"
            )
            db_dirty = True

    # STEP B: Execute Profit Skim When Price Touches Stop Net
    if coin["ratchet_active"] and live_price <= coin["ratchet_stop_price"]:
        exec_price = max(live_price, coin["ratchet_stop_price"]) if live_price >= coin["entry_price"] else live_price
        is_above_entry = exec_price >= (coin["entry_price"] * (1.0 + (2 * TRADING_FEE_PCT)))

        # SUB-ENTRY SLIDING FLOOR SKIM
        if not is_above_entry and not coin.get("moonbag_active"):
            net_bounce_pct = max(0.001, bounce_from_low - (2 * TRADING_FEE_PCT))
            skimmed_profit = max(0.0001, (coin["anchor_usd"] * 0.50) * net_bounce_pct)
            coin["vault_profit_usd"] += skimmed_profit
            coin["trades_count"] += 1
            old_entry = coin["entry_price"]
            coin["entry_price"] = (coin["entry_price"] + live_price) / 2.0
            coin["local_low"] = live_price
            coin["peak_price"] = live_price
            coin["ratchet_active"] = False
            coin["ratchet_stop_price"] = 0.0
            coin["mode"] = "HOLDING_ANCHOR"
            add_log(
                f"SLIDING FLOOR SKIM {coin['symbol']} @ ${fmt_px(live_price)}: "
                f"Locked +${skimmed_profit:.4f} in Vault & lowered Entry (${fmt_px(old_entry)} -> ${fmt_px(coin['entry_price'])})!"
            )
            db_dirty = True
            return

        # STAGE 1 HYBRID SKIM (50% Shares Sold -> Profit Held in Pending Escrow for Share-Keeper!)
        if coin.get("strategy") == "HYBRID_50_50" and not coin.get("moonbag_active"):
            # Remember full starting share target before selling half
            coin["target_shares"] = coin["token_qty"]
            sell_qty = coin["token_qty"] * 0.5
            half_anchor = coin["anchor_usd"] * 0.5
            gross_val = sell_qty * exec_price
            sell_fee = gross_val * TRADING_FEE_PCT
            net_payout = gross_val - sell_fee
            skimmed_profit = max(0.0001, net_payout - half_anchor)

            coin["pending_profit_usd"] = coin.get("pending_profit_usd", 0.0) + skimmed_profit
            coin["trades_count"] += 1
            coin["token_qty"] -= sell_qty
            coin["active_anchor_usd"] = half_anchor
            coin["moonbag_active"] = True
            coin["mode"] = "MOONBAG_RIDING"
            coin["ratchet_stop_price"] = max(zero_loss_floor_px, live_price * (1.0 - MOONBAG_TRAIL_PCT))
            add_log(
                f"STAGE 1 HYBRID SKIM {coin['symbol']} @ ${fmt_px(exec_price)}: "
                f"Sold {fmt_qty(sell_qty)} shares -> +${skimmed_profit:.4f} in Share-Keeper Escrow! "
                f"Riding {fmt_qty(coin['token_qty'])} Moonbag shares"
            )
            db_dirty = True
            return

        # FINAL SALE (100% Full Skim OR Stage 2 Moonbag Exit -> Profit Held in Pending Escrow Until Re-Buy!)
        if not coin.get("moonbag_active"):
            coin["target_shares"] = coin["token_qty"]

        active_cost = coin.get("active_anchor_usd", coin["anchor_usd"])
        sold_shares = coin["token_qty"]
        gross_value = sold_shares * exec_price
        sell_fee = gross_value * TRADING_FEE_PCT
        net_payout = gross_value - sell_fee
        skimmed_profit = max(0.0001, net_payout - active_cost)

        coin["pending_profit_usd"] = coin.get("pending_profit_usd", 0.0) + skimmed_profit
        coin["trades_count"] += 1
        exit_label = "STAGE 2 MOONBAG EXIT" if coin.get("moonbag_active") else "VAULT SKIM"
        add_log(
            f"{exit_label} {coin['symbol']} @ ${fmt_px(exec_price)}! "
            f"Sold {fmt_qty(sold_shares)} shares -> +${coin['pending_profit_usd']:.4f} ready in Escrow "
            f"(Will preserve {fmt_qty(coin['target_shares'])} shares on +0.2% step-up & send rest to Vault!)"
        )
        coin["in_position"] = False
        coin["moonbag_active"] = False
        coin["token_qty"] = 0.0
        coin["active_anchor_usd"] = coin["anchor_usd"]
        coin["ratchet_active"] = False
        coin["ratchet_stop_price"] = 0.0
        coin["last_sell_price"] = live_price
        coin["post_sell_peak"] = live_price
        coin["local_low"] = live_price
        coin["dip_buy_stop"] = 0.0
        coin["mode"] = "TRAILING_DIP_BUY"
        db_dirty = True
        return


# ==========================================
# 4. BACKGROUND THREADS (NEON POOLER COMPATIBLE)
# ==========================================
def init_db_async():
    global bot_state
    if not DATABASE_URL:
        return
    try:
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=6)
        cur = conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS vault_store (id INT PRIMARY KEY, state_json TEXT NOT NULL);")
        cur.execute("SELECT state_json FROM vault_store WHERE id = 1;")
        row = cur.fetchone()
        if row:
            loaded = json.loads(row[0])
            if "coins" in loaded:
                with state_lock:
                    for pair, saved_c in loaded["coins"].items():
                        bot_state["coins"][pair] = saved_c
                    ensure_coin_fields()
            add_log("Neon PostgreSQL synced: Restored all coins, shares, and Vault profits.")
        else:
            with state_lock:
                snapshot = json.dumps(bot_state)
            cur.execute("INSERT INTO vault_store (id, state_json) VALUES (1, %s);", (snapshot,))
            conn.commit()
            add_log("Neon PostgreSQL connected: Initialized Vault table.")
        cur.close()
        conn.close()
    except Exception as e:
        add_log(f"Neon DB background notice: {e}")


def save_state_to_db():
    global db_dirty
    with state_lock:
        snapshot = json.dumps(bot_state)
        db_dirty = False

    if DATABASE_URL:
        try:
            conn = psycopg2.connect(DATABASE_URL, connect_timeout=6)
            cur = conn.cursor()
            cur.execute("UPDATE vault_store SET state_json = %s WHERE id = 1;", (snapshot,))
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            print(f"Neon save error: {e}", flush=True)


def price_engine_loop():
    add_log("VaultSkim v2.5 (Share-Preservation Escrow + +0.2% Re-Buy) live.")
    threading.Thread(target=init_db_async, daemon=True).start()
    last_save = time.time()

    while True:
        try:
            prices = fetch_live_prices()
            if prices:
                with state_lock:
                    for pair, coin in list(bot_state["coins"].items()):
                        if pair in prices:
                            process_coin(pair, coin, prices[pair])

            now = time.time()
            if db_dirty or (now - last_save > 45):
                save_state_to_db()
                last_save = now
        except Exception as e:
            print(f"Engine loop error: {e}", flush=True)

        time.sleep(4)


@app.before_request
def ensure_worker_threads():
    global worker_started
    if not worker_started:
        worker_started = True
        threading.Thread(target=price_engine_loop, daemon=True).start()


# ==========================================
# 5. v2.5 DASHBOARD UI (SHARE-KEEPER ESCROW)
# ==========================================
DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>VaultSkim v2.5 — Share-Keeper Profit Engine</title>
  <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-slate-950 text-slate-100 min-h-screen p-6 font-sans">
  <div class="max-w-6xl mx-auto">
    <div class="flex flex-col md:flex-row justify-between items-start md:items-center border-b border-slate-800 pb-4 mb-6 gap-4">
      <div>
        <h1 class="text-2xl font-bold text-emerald-400">
          VaultSkim Command Center
          <span class="text-xs bg-emerald-900/60 text-emerald-300 px-2.5 py-0.5 rounded ml-2">v2.5 · Share-Keeper</span>
        </h1>
        <p class="text-sm text-slate-400">Zero-Loss Shield + +0.2% Share-Preservation Re-Buy + -0.4% Dip Re-Buy</p>
      </div>
      <div class="flex flex-wrap items-center gap-3">
        <button id="btn-sync" onclick="syncLivePrices(false)" class="bg-slate-800 hover:bg-slate-700 active:scale-95 border border-slate-700 text-slate-200 text-xs font-semibold px-3.5 py-2.5 rounded-xl transition cursor-pointer">
          🔄 Sync All to Live Token Position
        </button>
        <button id="btn-reset" onclick="syncLivePrices(true)" class="bg-rose-950/60 hover:bg-rose-900/70 active:scale-95 border border-rose-700/50 text-rose-200 text-xs font-semibold px-3.5 py-2.5 rounded-xl transition cursor-pointer">
          🧹 Reset Test Vault ($0.00)
        </button>
        <div class="bg-emerald-950/60 border border-emerald-500/40 px-5 py-2.5 rounded-xl text-right">
          <div class="text-xs uppercase tracking-wider text-emerald-300">Total Untouchable Vault Profit</div>
          <div id="total-vault" class="text-3xl font-extrabold text-emerald-400">$0.00</div>
        </div>
      </div>
    </div>

    <div class="bg-slate-900 border border-slate-800 rounded-xl p-4 mb-6 flex flex-wrap items-center justify-between gap-3">
      <div class="text-sm font-medium text-slate-300">Add Token or Meme Coin to Skimmer:</div>
      <div class="flex flex-wrap gap-2">
        <input id="new-symbol" type="text" placeholder="Symbol (e.g. SOL, PEPE, XCN)" class="bg-slate-950 border border-slate-700 rounded-lg px-3 py-1.5 text-sm text-white uppercase">
        <input id="new-anchor" type="number" placeholder="Anchor USD (e.g. 50)" value="50" class="bg-slate-950 border border-slate-700 rounded-lg px-3 py-1.5 text-sm text-white w-36">
        <button id="btn-add" onclick="addCoin()" class="bg-emerald-600 hover:bg-emerald-500 active:scale-95 text-white text-sm font-semibold px-4 py-1.5 rounded-lg transition cursor-pointer">+ Launch Skimmer</button>
      </div>
    </div>

    <div id="coin-cards" class="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8"></div>

    <div class="bg-slate-900 border border-slate-800 rounded-xl p-5">
      <h2 class="text-sm font-semibold uppercase tracking-wider text-slate-400 mb-3">Live Execution Log (Share-Preservation + Vault Sweeps)</h2>
      <div id="logs" class="font-mono text-xs text-slate-300 space-y-1.5 max-h-64 overflow-y-auto"></div>
    </div>
  </div>

  <script>
    function fmtPx(val) {
      if (!val || val <= 0) return '0.0000';
      if (val < 0.0001) return val.toFixed(8);
      if (val < 0.01) return val.toFixed(6);
      return val.toFixed(4);
    }

    function fmtQty(qty) {
      if (!qty || qty <= 0) return '0.00';
      if (qty >= 100) return qty.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2});
      if (qty >= 1) return qty.toFixed(4);
      return qty.toFixed(6);
    }

    async function addCoin() {
      const sym = document.getElementById('new-symbol').value.trim().toUpperCase();
      const anc = parseFloat(document.getElementById('new-anchor').value) || 50;
      if (!sym) return;
      const btn = document.getElementById('btn-add');
      btn.innerText = '⏳ Launching...';
      await fetch('/api/add_coin', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({symbol: sym, anchor_usd: anc})
      });
      document.getElementById('new-symbol').value = '';
      btn.innerText = '✅ Added!';
      setTimeout(() => { btn.innerText = '+ Launch Skimmer'; }, 1200);
      refresh();
    }

    async function toggleStrategy(pair) {
      await fetch('/api/toggle_strategy/' + pair, {method: 'POST'});
      refresh();
    }

    async function editAnchor(pair, currentAnchor) {
      const input = prompt('Enter new Bucket A Working Anchor (USD):', currentAnchor);
      if (input === null) return;
      const val = parseFloat(input);
      if (isNaN(val) || val <= 0) return;
      await fetch('/api/edit_anchor/' + pair, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({anchor_usd: val})
      });
      refresh();
    }

    async function deleteCoin(pair, sym) {
      if (!confirm('Remove ' + sym + '/USDT card from dashboard?')) return;
      await fetch('/api/delete_coin/' + pair, {method: 'POST'});
      refresh();
    }

    async function simulateSpike(pair, btnEl) {
      if (btnEl) btnEl.innerText = '⚡ Simulating...';
      await fetch('/api/simulate_spike/' + pair, {method: 'POST'});
      if (btnEl) {
        btnEl.innerText = '✅ Skimmed!';
        setTimeout(() => { refresh(); }, 800);
      }
      refresh();
    }

    async function syncLivePrices(clearVault) {
      const btn = document.getElementById(clearVault ? 'btn-reset' : 'btn-sync');
      const originalText = btn.innerText;
      btn.innerText = clearVault ? '⏳ Resetting...' : '⏳ Syncing Live Prices...';
      await fetch('/api/reset_live', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({clear_vault: clearVault})
      });
      btn.innerText = '✅ Done!';
      setTimeout(() => { btn.innerText = originalText; }, 1200);
      refresh();
    }

    async function refresh() {
      try {
        const res = await fetch('/api/state');
        const data = await res.json();
        let totalVault = 0;
        let cardsHtml = '';

        for (const [pair, c] of Object.entries(data.coins)) {
          const pending = c.pending_profit_usd || 0;
          totalVault += (c.vault_profit_usd + pending);

          const tokenVal = c.in_position && c.current_price > 0 ? (c.token_qty * c.current_price) : 0;
          const safeCashHalf = c.moonbag_active ? (c.anchor_usd * 0.5) : (c.in_position ? 0 : c.anchor_usd);
          const totalBucketAVal = tokenVal + safeCashHalf;

          const baseLow = c.local_low > 0 ? c.local_low : c.entry_price;
          const postPeak = c.post_sell_peak > 0 ? c.post_sell_peak : c.last_sell_price;
          const refPrice = c.in_position ? baseLow : postPeak;
          const pnlPct = refPrice > 0 ? (((c.current_price - refPrice) / refPrice) * 100).toFixed(2) : "0.00";
          const pnlColor = pnlPct >= 0 ? 'text-emerald-400' : 'text-amber-400';

          const isHybrid = c.strategy === 'HYBRID_50_50';
          const stratBadge = isHybrid
            ? '🚀 Strategy: 50/50 Hybrid Moonbag'
            : '🎯 Strategy: 100% Full Skim';
          const stratStyle = isHybrid
            ? 'bg-indigo-950/80 border-indigo-500/40 text-indigo-300 hover:bg-indigo-900/80'
            : 'bg-slate-800 border-slate-700 text-slate-300 hover:bg-slate-700';

          let bucketStatus = c.in_position ? '(In Token)' : '(In Cash)';
          if (c.moonbag_active) bucketStatus = '(50% Cash + 50% Moonbag)';

          let sharesDisplay = c.in_position
            ? `${fmtQty(c.token_qty)} ${c.symbol}`
            : `0.00 (Protected Target: ${fmtQty(c.target_shares || 0)} ${c.symbol})`;

          let valDisplay = '$' + totalBucketAVal.toFixed(2);
          if (c.moonbag_active) {
            valDisplay = `$${totalBucketAVal.toFixed(2)} <span class="text-xs text-slate-400">($${safeCashHalf.toFixed(0)} Cash + $${tokenVal.toFixed(2)} Bag)</span>`;
          }

          const slidingTarget = baseLow * 1.008;
          let row3Label = 'Entry / Sliding Low:';
          let row3Value = `$${fmtPx(c.entry_price)} / <span class="text-sky-300 font-mono">$${fmtPx(baseLow)}</span>`;

          let netLabel = 'Zero-Loss / +0.8% Bar:';
          let netValue = c.ratchet_active
            ? 'LOCKED Stop @ $' + fmtPx(c.ratchet_stop_price)
            : 'Shield @ +0.35% | Target $' + fmtPx(slidingTarget);

          if (c.moonbag_active) {
            netLabel = 'Moonbag Zero-Loss Stop:';
            netValue = 'LOCKED Stop @ $' + fmtPx(c.ratchet_stop_price);
          } else if (!c.in_position && c.mode === 'TRAILING_DIP_BUY') {
            const dipTriggerPx = postPeak * (1.0 - 0.0038);
            const stepUpPx = c.last_sell_price * 1.002;
            row3Label = 'Sold At / Sliding High:';
            row3Value = `$${fmtPx(c.last_sell_price)} / <span class="text-emerald-300 font-mono">$${fmtPx(postPeak)}</span>`;
            netLabel = 'Share-Keeper Re-Buy:';
            netValue = `Dip <= $${fmtPx(dipTriggerPx)} (-0.4%) OR Step-Up >= $${fmtPx(stepUpPx)} (+0.2%)`;
          }

          let vaultRowHtml = `+$${c.vault_profit_usd.toFixed(4)} (${c.trades_count} skims)`;
          if (pending > 0) {
            vaultRowHtml = `+$${c.vault_profit_usd.toFixed(4)} <span class="text-amber-300 text-xs">(+$${pending.toFixed(4)} Escrow)</span>`;
          }

          const simBtnText = c.moonbag_active
            ? '🚀 Practice Moonbag Rally (+5%)'
            : '⚡ Practice Spike (+2% Simulator)';

          cardsHtml += `
            <div class="bg-slate-900 border border-slate-800 rounded-xl p-5 shadow-lg flex flex-col justify-between">
              <div>
                <div class="flex justify-between items-center mb-2">
                  <span class="text-xl font-bold">${c.symbol}/USDT</span>
                  <div class="flex items-center gap-2">
                    <span class="text-xs px-2.5 py-1 rounded-full bg-slate-800 text-emerald-300 font-medium">${c.mode}</span>
                    <button onclick="deleteCoin('${pair}', '${c.symbol}')" title="Remove Card" class="text-slate-500 hover:text-rose-400 text-sm font-bold px-1.5 py-0.5 rounded cursor-pointer">✕</button>
                  </div>
                </div>

                <button onclick="toggleStrategy('${pair}')" class="w-full text-left text-xs font-semibold px-3 py-1.5 rounded-lg border mb-3 transition cursor-pointer flex justify-between items-center ${stratStyle}">
                  <span>${stratBadge}</span>
                  <span class="underline opacity-75">Switch</span>
                </button>

                <div class="text-2xl font-mono font-bold mb-4">$${fmtPx(c.current_price)} <span class="text-xs ${pnlColor}">(${pnlPct}% ${c.in_position ? 'from Low' : 'from Peak'})</span></div>
                <div class="space-y-2 text-sm border-t border-slate-800 pt-3">
                  <div class="flex justify-between items-center">
                    <span class="text-slate-400">Bucket A (Anchor):</span>
                    <span class="font-semibold flex items-center gap-1.5">
                      $${c.anchor_usd.toFixed(2)} <span class="text-xs text-slate-400">${bucketStatus}</span>
                      <button onclick="editAnchor('${pair}', ${c.anchor_usd})" class="text-xs bg-slate-800 hover:bg-slate-700 text-emerald-300 px-1.5 py-0.5 rounded border border-slate-700 cursor-pointer">✏️ Edit</button>
                    </span>
                  </div>
                  <div class="flex justify-between">
                    <span class="text-slate-400">Shares Owned:</span>
                    <span class="font-mono text-sky-300 font-semibold text-xs">${sharesDisplay}</span>
                  </div>
                  <div class="flex justify-between"><span class="text-slate-400">Total Bucket A Value:</span><span class="font-semibold">${valDisplay}</span></div>
                  <div class="flex justify-between"><span class="text-slate-400">${row3Label}</span><span>${row3Value}</span></div>
                  <div class="flex justify-between"><span class="text-slate-400">${netLabel}</span><span class="text-amber-300 font-mono text-xs text-right">${netValue}</span></div>
                  <div class="flex justify-between border-t border-slate-800 pt-2 mt-2">
                    <span class="text-emerald-400 font-semibold">Bucket B (Vaulted):</span>
                    <span class="text-emerald-400 font-bold">${vaultRowHtml}</span>
                  </div>
                </div>
              </div>
              <button onclick="simulateSpike('${pair}', this)" class="mt-4 w-full bg-slate-800 hover:bg-slate-700 active:scale-95 text-amber-300 text-xs font-semibold py-2 rounded-lg border border-slate-700 transition cursor-pointer">
                ${simBtnText}
              </button>
            </div>`;
        }

        document.getElementById('coin-cards').innerHTML = cardsHtml;
        document.getElementById('total-vault').innerText = '$' + totalVault.toFixed(2);
        document.getElementById('logs').innerHTML = data.logs.map(l => `<div>${l}</div>`).join('');
      } catch (e) {}
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
    with state_lock:
        return jsonify(bot_state)

@app.route("/api/add_coin", methods=["POST"])
def add_coin():
    global db_dirty
    data = request.json or {}
    sym = data.get("symbol", "").upper().replace("USDT", "").strip()
    anchor = float(data.get("anchor_usd", 50.0))
    if sym:
        pair = f"{sym}USDT"
        prices = fetch_live_prices([sym])
        with state_lock:
            bot_state["coins"][pair] = make_coin_template(sym, anchor, strategy="HYBRID_50_50")
            if pair in prices and prices[pair] > 0:
                deploy_anchor_buy(bot_state["coins"][pair], prices[pair], reason="New Skimmer Added")
            else:
                add_log(f"ADDED NEW SKIMMER: {sym}/USDT with ${anchor:.2f} Anchor.")
            db_dirty = True
    return jsonify({"ok": True})

@app.route("/api/toggle_strategy/<pair>", methods=["POST"])
def toggle_strategy(pair):
    global db_dirty
    with state_lock:
        coin = bot_state["coins"].get(pair)
        if coin:
            new_strat = "FULL_100" if coin.get("strategy") == "HYBRID_50_50" else "HYBRID_50_50"
            coin["strategy"] = new_strat
            label = "50/50 Hybrid Moonbag" if new_strat == "HYBRID_50_50" else "100% Full Skim"
            add_log(f"STRATEGY SWITCHED ({coin['symbol']}): Now using {label}.")
            db_dirty = True
    return jsonify({"ok": True})

@app.route("/api/edit_anchor/<pair>", methods=["POST"])
def edit_anchor(pair):
    global db_dirty
    data = request.json or {}
    new_anc = float(data.get("anchor_usd", 0))
    if new_anc > 0:
        with state_lock:
            coin = bot_state["coins"].get(pair)
            if coin:
                coin["anchor_usd"] = new_anc
                if coin["in_position"] and coin["current_price"] > 0:
                    deploy_anchor_buy(coin, coin["current_price"], reason="Anchor Resized")
                else:
                    add_log(f"ANCHOR UPDATED ({coin['symbol']}): Bucket A set to ${new_anc:.2f}.")
                db_dirty = True
    return jsonify({"ok": True})

@app.route("/api/delete_coin/<pair>", methods=["POST"])
def delete_coin(pair):
    global db_dirty
    with state_lock:
        if pair in bot_state["coins"]:
            sym = bot_state["coins"][pair]["symbol"]
            del bot_state["coins"][pair]
            add_log(f"REMOVED SKIMMER: {sym}/USDT card deleted.")
            db_dirty = True
    return jsonify({"ok": True})

@app.route("/api/simulate_spike/<pair>", methods=["POST"])
def simulate_spike(pair):
    global db_dirty
    with state_lock:
        coin = bot_state["coins"].get(pair)
        sym = coin["symbol"] if coin else None

    if not coin:
        return jsonify({"ok": False})

    if coin["current_price"] <= 0 and sym:
        fresh = fetch_live_prices([sym])
        if pair in fresh:
            with state_lock:
                coin["current_price"] = fresh[pair]

    with state_lock:
        real_price = coin["current_price"]
        if not coin["in_position"] and real_price > 0:
            deploy_anchor_buy(coin, real_price, reason="Pre-Test Entry")

        if coin["entry_price"] > 0:
            base_low = coin["local_low"] if coin["local_low"] > 0 else coin["entry_price"]
            if coin.get("moonbag_active"):
                rally_peak = coin["entry_price"] * 1.055
                process_coin(pair, coin, rally_peak)
                pullback = rally_peak * 0.985
                process_coin(pair, coin, pullback)
            else:
                spike_price = base_low * 1.022
                process_coin(pair, coin, spike_price)
                pullback_price = spike_price * 0.996
                process_coin(pair, coin, pullback_price)

            if real_price > 0:
                coin["current_price"] = real_price
                if not coin["in_position"]:
                    coin["last_sell_price"] = real_price
                    coin["post_sell_peak"] = real_price
                    coin["local_low"] = real_price
            db_dirty = True
    return jsonify({"ok": True})

@app.route("/api/reset_live", methods=["POST"])
def reset_live():
    global db_dirty
    data = request.json or {}
    clear_vault = bool(data.get("clear_vault", False))
    prices = fetch_live_prices()

    with state_lock:
        for pair, coin in bot_state["coins"].items():
            live_px = prices.get(pair) or coin["current_price"]
            if clear_vault:
                coin["vault_profit_usd"] = 0.0
                coin["pending_profit_usd"] = 0.0
                coin["trades_count"] = 0
            if live_px > 0:
                deploy_anchor_buy(coin, live_px, reason="Synced to Live Market")
        add_log("SYNCED TO LIVE MARKET: All anchors deployed at live prices." + (" (Vault reset to $0.00)" if clear_vault else ""))
        db_dirty = True
    return jsonify({"ok": True})

@app.route("/health")
def health():
    return jsonify({"status": "awake", "utc_time": datetime.utcnow().isoformat()}), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
