import os
import json
import time
from flask import Flask, request, jsonify, render_template_string
import ccxt

app = Flask(__name__)

STATE_FILE = "/tmp/vaultskim_state_v12.json"

DEFAULT_STATE = {
    "api_key": (os.environ.get("GATE_API_KEY") or "").strip(),
    "api_secret": (os.environ.get("GATE_SECRET") or "").strip(),
    "keys_connected": False,
    "frozen": False,
    "usdt_free": 0.0,  
    "usdt_total": 0.0,
    "realized_usdt_profit": 0.0,
    "completed_skims": 0,
    "processed_sell_ids": [],
    "dismissed_symbols": [],
    "coins": {},
    "ledger": [],
    "logs": []
}

# =====================================================================
# FAST-SKIM 1.00% STRATEGY PARAMETERS
# =====================================================================
TRIGGER_UP_PCT = 1.00 / 100.0       # +1.00% bounce from Sliding Low arms Trailing Peak Chaser
TRAIL_PULLBACK_PCT = 0.20 / 100.0   # 0.20% pullback from highest peak triggers Market Sell
REBUY_DIP_PCT = 0.75 / 100.0        # -0.75% below sell price places Protected Limit Rebuy
RUNAWAY_DRIFT_PCT = 2.00 / 100.0    # Option 1: If price runs +2.0% above rebuy, check Profit Vault top-up

CACHED_EXCHANGE = None
CACHED_KEY = None


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
    state = dict(DEFAULT_STATE)
    for old_path in ["/tmp/vaultskim_state_v11.json", "/tmp/vaultskim_state_v10.json", "/tmp/vaultskim_state_v9.json"]:
        if os.path.exists(old_path):
            try:
                with open(old_path, "r") as f:
                    old = json.load(f)
                    if old.get("api_key") and old.get("api_secret"):
                        state["api_key"] = old["api_key"]
                        state["api_secret"] = old["api_secret"]
                        break
            except Exception:
                pass
    return state


def save_state(state):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f)
    except Exception:
        pass


BOT_STATE = load_state()


def get_public_state():
    s = load_state()
    BOT_STATE.update(s)
    return {
        "keys_connected": bool(BOT_STATE.get("api_key") and BOT_STATE.get("api_secret")),
        "frozen": BOT_STATE.get("frozen", False),
        "usdt_free": BOT_STATE.get("usdt_free", 0.0),
        "usdt_total": BOT_STATE.get("usdt_total", 0.0),
        "realized_usdt_profit": BOT_STATE.get("realized_usdt_profit", 0.0),
        "completed_skims": BOT_STATE.get("completed_skims", 0),
        "coins": BOT_STATE.get("coins", {}),
        "ledger": BOT_STATE.get("ledger", []),
        "logs": BOT_STATE.get("logs", [])
    }


def add_log(msg):
    timestamp = time.strftime("%H:%M:%S")
    if not BOT_STATE["logs"] or not BOT_STATE["logs"][0].endswith(msg):
        BOT_STATE["logs"].insert(0, f"[{timestamp}] {msg}")
        BOT_STATE["logs"] = BOT_STATE["logs"][:45]
        save_state(BOT_STATE)


def get_exchange():
    global CACHED_EXCHANGE, CACHED_KEY
    s = load_state()
    BOT_STATE.update(s)

    key = BOT_STATE.get("api_key", "").strip()
    secret = BOT_STATE.get("api_secret", "").strip()
    if not key or not secret:
        return None

    if CACHED_EXCHANGE is None or CACHED_KEY != key:
        ex = ccxt.gate({
            "apiKey": key,
            "secret": secret,
            "enableRateLimit": True,
            "options": {
                "defaultType": "spot",
                "createMarketBuyOrderRequiresPrice": False
            }
        })
        ex.load_markets()
        CACHED_EXCHANGE = ex
        CACHED_KEY = key

    return CACHED_EXCHANGE


def execute_spot_buy(exchange, symbol, usdt_to_spend, live_price):
    """
    Bulletproof Gate.io Spot Buy:
    1. Uses Gate.io's native quote-USDT market buy (createMarketBuyOrderRequiresPrice=False).
    2. If Gate.io ever rejects market buy, falls back to an instant-fill Limit Buy (+0.3% above ask).
    """
    try:
        exchange.options["createMarketBuyOrderRequiresPrice"] = False
        cost_usdt = float(exchange.cost_to_precision(symbol, usdt_to_spend))
        return exchange.create_order(
            symbol,
            "market",
            "buy",
            cost_usdt,
            None,
            {"createMarketBuyOrderRequiresPrice": False}
        )
    except Exception:
        # Fallback: Instant-fill Limit Buy
        limit_px = float(exchange.price_to_precision(symbol, live_price * 1.003))
        token_amt = float(exchange.amount_to_precision(symbol, (usdt_to_spend * 0.995) / limit_px))
        return exchange.create_limit_buy_order(symbol, token_amt, limit_px)


def sync_existing_wallet(exchange, balance):
    ignored = {"USDT", "USDC", "USD", "POINT", "GT"}
    dismissed = set(BOT_STATE.get("dismissed_symbols", []))

    for asset, total_amt in balance.get("total", {}).items():
        if asset not in ignored and float(total_amt or 0) > 0:
            symbol = f"{asset}/USDT"
            if symbol in dismissed or symbol not in exchange.markets or symbol in BOT_STATE["coins"]:
                continue

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
                    "lowest_low": price,
                    "live_price": price,
                    "last_sell_price": 0.0,
                    "position_value": val_usdt,
                    "locked_order_usdt": 0.0,
                    "harvest_target": price * (1 + TRIGGER_UP_PCT),
                    "highest_peak": price,
                    "trailing_active": False,
                    "realized_profit": 0.0,
                    "skims": 0,
                    "mode": "HOLDING",
                    "status": "SLIDING_LOW_ARMED",
                    "ai_note": f"Sliding Low locked @ ${price:.4f}. Waiting for +1.00% bounce."
                }
                add_log(f"🛡️ Synced Spot Holding: {shares:.6f} {asset} (${val_usdt:.2f})")
            else:
                open_orders = exchange.fetch_open_orders(symbol)
                if open_orders:
                    order = open_orders[0]
                    rebuy_px = float(order["price"])
                    rebuy_amt = float(order["amount"])
                    locked_usdt = rebuy_amt * rebuy_px
                    BOT_STATE["coins"][symbol] = {
                        "symbol": symbol,
                        "base": asset,
                        "protected_shares": shares + rebuy_amt,
                        "real_shares": shares,
                        "lowest_low": rebuy_px,
                        "live_price": price,
                        "last_sell_price": rebuy_px / (1 - REBUY_DIP_PCT),
                        "position_value": val_usdt + locked_usdt,
                        "locked_order_usdt": locked_usdt,
                        "harvest_target": price * (1 + TRIGGER_UP_PCT),
                        "highest_peak": price,
                        "trailing_active": False,
                        "realized_profit": 0.0,
                        "skims": 0,
                        "mode": "REBUYING",
                        "status": f"REBUY @ ${rebuy_px:.4f}",
                        "active_order_price": rebuy_px,
                        "ai_note": f"Open Limit Buy on Gate.io for {rebuy_amt} {asset} @ ${rebuy_px:.4f}."
                    }
                    add_log(f"🔄 Synced Open Gate.io Order: {rebuy_amt} {asset} @ ${rebuy_px:.4f}")
    save_state(BOT_STATE)


def place_protected_rebuy(exchange, symbol, card, sold_price, sold_shares, free_usdt, dust_shares):
    base = card["base"]
    rebuy_price = float(exchange.price_to_precision(symbol, sold_price * (1 - REBUY_DIP_PCT)))

    needed_shares = max(sold_shares, card["protected_shares"] - dust_shares) * 1.001
    desired_amt = float(exchange.amount_to_precision(symbol, needed_shares))

    max_spendable_usdt = min(free_usdt * 0.996, (sold_shares * sold_price) * 0.995)
    if desired_amt * rebuy_price > max_spendable_usdt:
        rebuy_amt = float(exchange.amount_to_precision(symbol, max_spendable_usdt / rebuy_price))
    else:
        rebuy_amt = desired_amt

    order = exchange.create_limit_buy_order(symbol, rebuy_amt, rebuy_price)

    usdt_from_sell = (sold_shares * sold_price) * 0.999
    usdt_for_rebuy = rebuy_amt * rebuy_price
    net_skim = max(0.0, usdt_from_sell - usdt_for_rebuy)

    card["realized_profit"] = card.get("realized_profit", 0.0) + net_skim
    card["skims"] = card.get("skims", 0) + 1
    BOT_STATE["realized_usdt_profit"] = BOT_STATE.get("realized_usdt_profit", 0.0) + net_skim
    BOT_STATE["completed_skims"] = BOT_STATE.get("completed_skims", 0) + 1

    card["trailing_active"] = False
    card["mode"] = "REBUYING"
    card["last_sell_price"] = sold_price
    card["status"] = f"REBUY @ ${rebuy_price:.4f}"
    card["active_order_price"] = rebuy_price
    card["active_order_id"] = order.get("id")
    card["locked_order_usdt"] = usdt_for_rebuy
    card["position_value"] = (dust_shares * card.get("live_price", sold_price)) + usdt_for_rebuy
    card["ai_note"] = (
        f"Sold peak @ ${sold_price:.4f} (+${net_skim:.4f} USDT profit)! "
        f"Waiting for -0.75% dip to ${rebuy_price:.4f} to restore {rebuy_amt} {base}."
    )

    ts = time.strftime("%H:%M:%S")
    BOT_STATE["ledger"].insert(0, {
        "base": base,
        "sell_price": sold_price,
        "rebuy_price": rebuy_price,
        "profit": net_skim,
        "shares": rebuy_amt + dust_shares,
        "time": ts
    })
    BOT_STATE["ledger"] = BOT_STATE["ledger"][:20]

    add_log(
        f"💰 {base} PEAK SOLD @ ${sold_price:.4f}! "
        f"Skimmed +${net_skim:.4f} USDT | Placed Rebuy: {rebuy_amt} {base} @ ${rebuy_price:.4f}"
    )


# =====================================================================
# CORE ENGINE TICK
# =====================================================================
def execute_engine_tick():
    exchange = get_exchange()
    if not exchange:
        BOT_STATE["keys_connected"] = False
        return get_public_state()

    balance = exchange.fetch_balance()
    BOT_STATE["keys_connected"] = True
    free_usdt = float(balance.get("free", {}).get("USDT", 0.0))
    total_usdt = float(balance.get("total", {}).get("USDT", 0.0))
    BOT_STATE["usdt_free"] = free_usdt
    BOT_STATE["usdt_total"] = total_usdt

    if not BOT_STATE["coins"]:
        sync_existing_wallet(exchange, balance)

    if BOT_STATE["frozen"]:
        save_state(BOT_STATE)
        return get_public_state()

    for symbol, card in list(BOT_STATE["coins"].items()):
        base = card["base"]
        ticker = exchange.fetch_ticker(symbol)
        live_price = float(ticker["last"])
        card["live_price"] = live_price

        real_shares = float(balance.get("total", {}).get(base, 0.0))
        free_shares = float(balance.get("free", {}).get(base, 0.0))
        card["real_shares"] = real_shares

        if "lowest_low" not in card or card["lowest_low"] <= 0:
            card["lowest_low"] = live_price

        open_orders = exchange.fetch_open_orders(symbol)

        # STATE 1: OPEN LIMIT REBUY ORDER LIVE ON GATE.IO
        if len(open_orders) > 0:
            order = open_orders[0]
            order_id = order["id"]
            rebuy_px = float(order["price"])
            rebuy_amt = float(order["amount"])
            locked_usdt = rebuy_amt * rebuy_px

            card["mode"] = "REBUYING"
            card["locked_order_usdt"] = locked_usdt
            card["position_value"] = (real_shares * live_price) + locked_usdt
            card["status"] = f"REBUY @ ${rebuy_px:.4f}"
            card["active_order_price"] = rebuy_px
            card["trailing_active"] = False

            dist_pct = ((live_price - rebuy_px) / live_price) * 100.0

            # OPTION 1 (PROFIT-VAULT TOP-UP):
            drift_from_rebuy = (live_price - rebuy_px) / rebuy_px
            vault_profit_available = min(free_usdt * 0.95, BOT_STATE.get("realized_usdt_profit", 0.0))

            if drift_from_rebuy >= RUNAWAY_DRIFT_PCT and vault_profit_available > 0.04:
                new_rebuy_px = float(exchange.price_to_precision(symbol, live_price * (1 - REBUY_DIP_PCT)))
                extra_usdt_needed = (rebuy_amt * new_rebuy_px) - locked_usdt

                if 0 < extra_usdt_needed <= vault_profit_available:
                    exchange.cancel_order(order_id, symbol)
                    exchange.create_limit_buy_order(symbol, rebuy_amt, new_rebuy_px)
                    BOT_STATE["realized_usdt_profit"] = max(0.0, BOT_STATE["realized_usdt_profit"] - extra_usdt_needed)
                    card["active_order_price"] = new_rebuy_px
                    card["status"] = f"REBUY @ ${new_rebuy_px:.4f}"
                    add_log(
                        f"🛡️ Option 1 Auto-Vault Top-Up: Used ${extra_usdt_needed:.4f} profit vault "
                        f"to shift {base} Rebuy from ${rebuy_px:.4f} -> ${new_rebuy_px:.4f} (100% Shares Protected!)"
                    )
                    continue

            card["ai_note"] = (
                f"Waiting for -{dist_pct:.2f}% dip to ${rebuy_px:.4f} to restore {rebuy_amt} {base} "
                f"(${locked_usdt:.2f} USDT locked). Use buttons below to Rebuy Now or Cancel."
            )
            continue

        # STATE 2: HOLDING SHARES (>= $3.00) -> SLIDING LOW & +1.00% PEAK CHASER
        card["locked_order_usdt"] = 0.0
        card["position_value"] = real_shares * live_price

        if free_shares * live_price >= 3.0:
            card["mode"] = "HOLDING"
            if real_shares > card["protected_shares"]:
                card["protected_shares"] = real_shares

            if card.get("status", "").startswith("REBUY"):
                filled_px = card.get("active_order_price", live_price)
                card["lowest_low"] = min(live_price, filled_px)
                card["highest_peak"] = live_price
                card["trailing_active"] = False
                add_log(f"✅ {base} REBUY FILLED! Restored {real_shares:.6f} {base}. New Sliding Low: ${card['lowest_low']:.4f}")

            if not card["trailing_active"] and live_price < card["lowest_low"]:
                old_low = card["lowest_low"]
                card["lowest_low"] = live_price
                card["highest_peak"] = live_price
                card["harvest_target"] = live_price * (1 + TRIGGER_UP_PCT)
                if (old_low - live_price) / old_low >= 0.0012:
                    add_log(f"📉 {base} Sliding Low lowered to ${live_price:.4f} -> +1.0% Target: ${card['harvest_target']:.4f}")

            low_anchor = card["lowest_low"]
            trigger_target = low_anchor * (1 + TRIGGER_UP_PCT)
            card["harvest_target"] = trigger_target

            if live_price > card["highest_peak"]:
                card["highest_peak"] = live_price

            bounce_pct = ((live_price - low_anchor) / low_anchor) * 100.0

            if live_price >= trigger_target and not card["trailing_active"]:
                card["trailing_active"] = True
                add_log(f"🚀 {base} hit +{bounce_pct:.2f}% from Low (${low_anchor:.4f})! Trailing Peak Chaser Armed.")

            if card["trailing_active"]:
                stop_price = card["highest_peak"] * (1 - TRAIL_PULLBACK_PCT)
                card["status"] = f"TRAILING (Stop ${stop_price:.4f})"
                card["ai_note"] = f"Riding pump! Peak ${card['highest_peak']:.4f} • Sells on 0.20% pullback to ${stop_price:.4f}"

                if live_price <= stop_price and live_price > low_anchor * 1.0055:
                    sell_amt = float(exchange.amount_to_precision(symbol, free_shares))
                    sell_order = exchange.create_market_sell_order(symbol, sell_amt)
                    dust_left = max(0.0, real_shares - sell_amt)

                    if sell_order and sell_order.get("id"):
                        BOT_STATE["processed_sell_ids"].append(str(sell_order["id"]))

                    post_bal = exchange.fetch_balance()
                    free_usdt = float(post_bal.get("free", {}).get("USDT", 0.0))
                    total_usdt = float(post_bal.get("total", {}).get("USDT", 0.0))
                    BOT_STATE["usdt_free"] = free_usdt
                    BOT_STATE["usdt_total"] = total_usdt

                    place_protected_rebuy(exchange, symbol, card, live_price, sell_amt, free_usdt, dust_left)
            else:
                card["status"] = "SLIDING_LOW_ARMED"
                need_pct = max(0.0, 1.00 - bounce_pct)
                card["ai_note"] = f"Low locked @ ${low_anchor:.4f} • Needs +{need_pct:.2f}% bounce to trigger +1.0% harvest."

        # STATE 3: CANCELED / IDLE
        else:
            card["mode"] = "CANCELED_IDLE"
            card["status"] = "ORDER_CANCELED (IDLE)"
            card["ai_note"] = (
                f"Rebuy order was canceled. Your ${free_usdt:.2f} USDT is unlocked in your wallet! "
                f"Tap '🔄 Rebuy {base} Now' or '❌ Cancel & Stop' below."
            )

    save_state(BOT_STATE)
    return get_public_state()


# =====================================================================
# ACTION ROUTES
# =====================================================================
@app.route("/cancel_coin", methods=["POST"])
def cancel_coin():
    try:
        body = request.get_json() or {}
        symbol = body.get("symbol")
        exchange = get_exchange()

        if exchange and symbol:
            open_orders = exchange.fetch_open_orders(symbol)
            for o in open_orders:
                exchange.cancel_order(o["id"], symbol)
            add_log(f"🛑 Canceled open Gate.io orders for {symbol} and stopped tracking.")

        if symbol in BOT_STATE["coins"]:
            del BOT_STATE["coins"][symbol]

        if symbol and symbol not in BOT_STATE["dismissed_symbols"]:
            BOT_STATE["dismissed_symbols"].append(symbol)

        if exchange:
            bal = exchange.fetch_balance()
            BOT_STATE["usdt_free"] = float(bal.get("free", {}).get("USDT", 0.0))
            BOT_STATE["usdt_total"] = float(bal.get("total", {}).get("USDT", 0.0))

        save_state(BOT_STATE)
        return jsonify({"status": "canceled", "data": get_public_state()}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/rebuy_now", methods=["POST"])
def rebuy_now():
    try:
        body = request.get_json() or {}
        symbol = body.get("symbol")
        exchange = get_exchange()
        if not exchange or symbol not in BOT_STATE["coins"]:
            return jsonify({"error": "Coin or exchange not active."}), 400

        card = BOT_STATE["coins"][symbol]
        base = card["base"]

        open_orders = exchange.fetch_open_orders(symbol)
        for o in open_orders:
            exchange.cancel_order(o["id"], symbol)

        bal = exchange.fetch_balance()
        free_usdt = float(bal.get("free", {}).get("USDT", 0.0))
        ticker = exchange.fetch_ticker(symbol)
        live_price = float(ticker["last"])

        if free_usdt < 3.0:
            return jsonify({"error": f"Need at least $3.00 Free USDT to rebuy (Have ${free_usdt:.2f})."}), 400

        spend_usdt = min(free_usdt * 0.994, max(7.0, card.get("locked_order_usdt", free_usdt * 0.994)))
        execute_spot_buy(exchange, symbol, spend_usdt, live_price)
        time.sleep(0.4)

        post_bal = exchange.fetch_balance()
        new_shares = float(post_bal.get("total", {}).get(base, 0.0))
        BOT_STATE["usdt_free"] = float(post_bal.get("free", {}).get("USDT", 0.0))
        BOT_STATE["usdt_total"] = float(post_bal.get("total", {}).get("USDT", 0.0))

        card["protected_shares"] = new_shares
        card["real_shares"] = new_shares
        card["lowest_low"] = live_price
        card["highest_peak"] = live_price
        card["harvest_target"] = live_price * (1 + TRIGGER_UP_PCT)
        card["locked_order_usdt"] = 0.0
        card["position_value"] = new_shares * live_price
        card["trailing_active"] = False
        card["mode"] = "HOLDING"
        card["status"] = "SLIDING_LOW_ARMED"
        card["ai_note"] = f"Rebought {new_shares:.6f} {base} @ ${live_price:.4f}! Sliding Low reset."

        add_log(f"🔄 Market Rebought {new_shares:.6f} {base} @ ${live_price:.4f} and anchored new Sliding Low!")
        save_state(BOT_STATE)
        return jsonify({"status": "rebought", "data": get_public_state()}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/launch", methods=["POST"])
def launch_coin():
    try:
        body = request.get_json() or {}
        if body.get("apiKey") and body.get("apiSecret") and not BOT_STATE.get("api_key"):
            BOT_STATE["api_key"] = body["apiKey"].strip()
            BOT_STATE["api_secret"] = body["apiSecret"].strip()
            save_state(BOT_STATE)

        exchange = get_exchange()
        if not exchange:
            return jsonify({"error": "Tap '🔑 Keys' at the top right to connect Gate.io first."}), 400

        raw_coin = (body.get("symbol") or "").strip().upper()
        usdt_budget = float(body.get("budget") or 7.0)
        if not raw_coin:
            return jsonify({"error": "Type a coin symbol first (e.g. SUI, DOGE, SOL)."}), 400

        symbol = raw_coin if "/" in raw_coin else f"{raw_coin}/USDT"
        base = symbol.split("/")[0]

        if symbol not in exchange.markets:
            return jsonify({"error": f"{symbol} does not exist on Gate.io Spot."}), 400

        if symbol in BOT_STATE.get("dismissed_symbols", []):
            BOT_STATE["dismissed_symbols"].remove(symbol)

        balance = exchange.fetch_balance()
        existing_shares = float(balance.get("total", {}).get(base, 0.0))
        free_usdt = float(balance.get("free", {}).get("USDT", 0.0))
        total_usdt = float(balance.get("total", {}).get("USDT", 0.0))
        BOT_STATE["usdt_free"] = free_usdt
        BOT_STATE["usdt_total"] = total_usdt

        ticker = exchange.fetch_ticker(symbol)
        live_price = float(ticker["last"])

        open_orders = exchange.fetch_open_orders(symbol)
        if len(open_orders) > 0:
            order = open_orders[0]
            rebuy_px = float(order["price"])
            rebuy_amt = float(order["amount"])
            locked_usdt = rebuy_amt * rebuy_px
            BOT_STATE["coins"][symbol] = {
                "symbol": symbol,
                "base": base,
                "protected_shares": existing_shares + rebuy_amt,
                "real_shares": existing_shares,
                "lowest_low": rebuy_px,
                "live_price": live_price,
                "position_value": (existing_shares * live_price) + locked_usdt,
                "locked_order_usdt": locked_usdt,
                "harvest_target": live_price * (1 + TRIGGER_UP_PCT),
                "highest_peak": live_price,
                "trailing_active": False,
                "realized_profit": 0.0,
                "skims": 0,
                "mode": "REBUYING",
                "status": f"REBUY @ ${rebuy_px:.4f}",
                "active_order_price": rebuy_px,
                "ai_note": f"Synced live Limit Buy @ ${rebuy_px:.4f} (${locked_usdt:.2f} locked)."
            }
            save_state(BOT_STATE)
            return jsonify({"status": "launched", "data": get_public_state()}), 200

        if existing_shares * live_price >= 3.0:
            shares = existing_shares
            BOT_STATE["coins"][symbol] = {
                "symbol": symbol,
                "base": base,
                "protected_shares": shares,
                "real_shares": shares,
                "lowest_low": live_price,
                "live_price": live_price,
                "position_value": shares * live_price,
                "locked_order_usdt": 0.0,
                "harvest_target": live_price * (1 + TRIGGER_UP_PCT),
                "highest_peak": live_price,
                "trailing_active": False,
                "realized_profit": 0.0,
                "skims": 0,
                "mode": "HOLDING",
                "status": "SLIDING_LOW_ARMED",
                "ai_note": f"Sliding Low locked @ ${live_price:.4f}."
            }
            add_log(f"🎯 Locked onto {shares:.6f} {base} (${shares*live_price:.2f}) @ Low ${live_price:.4f}")
            save_state(BOT_STATE)
            return jsonify({"status": "launched", "data": get_public_state()}), 200

        if free_usdt < 3.0:
            return jsonify({
                "error": f"Free Spot USDT is ${free_usdt:.2f}. Cancel any open Gate.io order first or deposit USDT."
            }), 400

        # Spend requested budget (capped safely at 99.4% of free USDT)
        spend_amount = min(usdt_budget, free_usdt * 0.994)
        execute_spot_buy(exchange, symbol, spend_amount, live_price)
        time.sleep(0.4)

        post_bal = exchange.fetch_balance()
        shares = float(post_bal.get("total", {}).get(base, 0.0))
        if shares <= 0:
            shares = (spend_amount / live_price) * 0.998
        BOT_STATE["usdt_free"] = float(post_bal.get("free", {}).get("USDT", 0.0))
        BOT_STATE["usdt_total"] = float(post_bal.get("total", {}).get("USDT", 0.0))

        add_log(f"✅ Bought {shares:.6f} {base} @ ${live_price:.4f} (${spend_amount:.2f} USDT)")

        BOT_STATE["coins"][symbol] = {
            "symbol": symbol,
            "base": base,
            "protected_shares": shares,
            "real_shares": shares,
            "lowest_low": live_price,
            "live_price": live_price,
            "position_value": shares * live_price,
            "locked_order_usdt": 0.0,
            "harvest_target": live_price * (1 + TRIGGER_UP_PCT),
            "highest_peak": live_price,
            "trailing_active": False,
            "realized_profit": 0.0,
            "skims": 0,
            "mode": "HOLDING",
            "status": "SLIDING_LOW_ARMED",
            "ai_note": f"Sliding Low locked @ ${live_price:.4f}."
        }
        save_state(BOT_STATE)
        return jsonify({"status": "launched", "data": get_public_state()}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/connect_keys", methods=["POST"])
def connect_keys():
    global CACHED_EXCHANGE, CACHED_KEY
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
            "options": {
                "defaultType": "spot",
                "createMarketBuyOrderRequiresPrice": False
            }
        })
        test_ex.load_markets()
        bal = test_ex.fetch_balance()

        CACHED_EXCHANGE = test_ex
        CACHED_KEY = api_key

        BOT_STATE["api_key"] = api_key
        BOT_STATE["api_secret"] = api_secret
        BOT_STATE["keys_connected"] = True
        BOT_STATE["usdt_free"] = float(bal.get("free", {}).get("USDT", 0.0))
        BOT_STATE["usdt_total"] = float(bal.get("total", {}).get("USDT", 0.0))

        add_log(f"🔑 Gate.io Armed! Total USDT: ${BOT_STATE['usdt_total']:.2f} (Free: ${BOT_STATE['usdt_free']:.4f})")
        sync_existing_wallet(test_ex, bal)
        save_state(BOT_STATE)

        return jsonify({"status": "connected", "data": get_public_state()}), 200
    except Exception as e:
        return jsonify({"error": f"Gate.io error: {str(e)}"}), 400


@app.route("/tick", methods=["GET", "POST"])
def tick():
    try:
        header_key = request.headers.get("X-Gate-Key", "").strip()
        header_secret = request.headers.get("X-Gate-Secret", "").strip()
        if header_key and header_secret and not BOT_STATE.get("api_key"):
            BOT_STATE["api_key"] = header_key
            BOT_STATE["api_secret"] = header_secret
            save_state(BOT_STATE)

        data = execute_engine_tick()
        return jsonify({"status": "ok", "data": data}), 200
    except Exception as e:
        add_log(f"⚠️ Engine Alert: {str(e)}")
        return jsonify({"status": "error", "message": str(e), "data": get_public_state()}), 200


@app.route("/reset_low", methods=["POST"])
def reset_low():
    body = request.get_json() or {}
    symbol = body.get("symbol")
    if symbol in BOT_STATE["coins"]:
        c = BOT_STATE["coins"][symbol]
        if c.get("mode") != "HOLDING":
            return jsonify({"error": "Can only Snap Low while holding shares!"}), 400
        px = c.get("live_price", c.get("lowest_low"))
        c["lowest_low"] = px
        c["highest_peak"] = px
        c["harvest_target"] = px * (1 + TRIGGER_UP_PCT)
        c["trailing_active"] = False
        add_log(f"🔄 Snapped {symbol} Sliding Low to ${px:.4f} -> +1.0% Target ${c['harvest_target']:.4f}")
        save_state(BOT_STATE)
    return jsonify({"status": "ok", "data": get_public_state()}), 200


@app.route("/reset_stats", methods=["POST"])
def reset_stats():
    BOT_STATE["realized_usdt_profit"] = 0.0
    BOT_STATE["completed_skims"] = 0
    BOT_STATE["ledger"] = []
    add_log("🧹 Reset skim profit counter to $0.0000.")
    save_state(BOT_STATE)
    return jsonify({"status": "ok", "data": get_public_state()}), 200


@app.route("/toggle_freeze", methods=["POST"])
def toggle_freeze():
    BOT_STATE["frozen"] = not BOT_STATE["frozen"]
    save_state(BOT_STATE)
    return jsonify({"frozen": BOT_STATE["frozen"], "data": get_public_state()}), 200


# =====================================================================
# v12.2 FAST-SKIM (+1.0%) UI
# =====================================================================
HTML_UI = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no">
  <title>VaultSkim AI — Fast 1.0% Edition</title>
  <style>
    :root {
      --bg: #06080d;
      --card: rgba(14, 19, 29, 0.92);
      --border: rgba(255, 255, 255, 0.09);
      --green: #00e676;
      --cyan: #00b0ff;
      --gold: #ffea00;
      --red: #ff5252;
      --text: #f3f6fa;
      --muted: #8290a2;
    }
    * { box-sizing: border-box; -webkit-tap-highlight-color: transparent; }
    body {
      background: radial-gradient(circle at top, #101a2b 0%, var(--bg) 65%);
      color: var(--text);
      font-family: -apple-system, BlinkMacSystemFont, "Inter", "Segoe UI", Roboto, sans-serif;
      margin: 0;
      padding: 12px;
      min-height: 100vh;
    }
    .glass {
      background: var(--card);
      backdrop-filter: blur(14px);
      border: 1px solid var(--border);
      border-radius: 18px;
      padding: 15px;
      margin-bottom: 12px;
      box-shadow: 0 10px 28px rgba(0, 0, 0, 0.45);
    }
    .top-bar { display: flex; justify-content: space-between; align-items: center; }
    .brand { display: flex; align-items: center; gap: 8px; font-size: 17px; font-weight: 900; letter-spacing: -0.4px; }
    .live-dot { width: 8px; height: 8px; border-radius: 50%; background: var(--green); box-shadow: 0 0 10px var(--green); }
    .off-dot { width: 8px; height: 8px; border-radius: 50%; background: var(--red); }
    .action-bar { display: flex; gap: 6px; }
    .chip-btn {
      background: rgba(255,255,255,0.06);
      border: 1px solid rgba(255,255,255,0.12);
      color: var(--text);
      padding: 6px 10px;
      border-radius: 10px;
      font-size: 11.5px;
      font-weight: 700;
      cursor: pointer;
    }
    .chip-btn.gold { background: rgba(255, 234, 0, 0.14); border-color: rgba(255, 234, 0, 0.35); color: var(--gold); }
    .chip-btn.red { background: rgba(255, 82, 82, 0.14); border-color: rgba(255, 82, 82, 0.35); color: #ff8a80; }
    .chip-btn.cyan { background: rgba(0, 176, 255, 0.14); border-color: rgba(0, 176, 255, 0.35); color: #80d8ff; }
    .wallet-strip {
      display: flex;
      justify-content: space-between;
      align-items: center;
      flex-wrap: wrap;
      gap: 6px;
      margin-top: 12px;
      padding-top: 10px;
      border-top: 1px solid var(--border);
      font-size: 12px;
      color: var(--muted);
    }
    .hero-grid { display: grid; grid-template-columns: 1.3fr 0.9fr; gap: 10px; align-items: center; }
    .profit-val {
      font-size: 32px;
      font-weight: 900;
      color: var(--green);
      letter-spacing: -1px;
      margin: 4px 0;
      text-shadow: 0 0 22px rgba(0, 230, 118, 0.28);
    }
    .launch-bar { display: flex; gap: 8px; }
    .input-modern {
      background: rgba(0, 0, 0, 0.45);
      border: 1px solid rgba(255, 255, 255, 0.14);
      color: #fff;
      padding: 11px 12px;
      border-radius: 12px;
      font-size: 14px;
      outline: none;
      width: 100%;
    }
    .btn-primary {
      background: linear-gradient(135deg, #00e676 0%, #00b0ff 100%);
      color: #031018;
      font-weight: 800;
      border: none;
      padding: 11px 16px;
      border-radius: 12px;
      font-size: 13.5px;
      white-space: nowrap;
      cursor: pointer;
    }
    .coin-header { display: flex; justify-content: space-between; align-items: flex-start; }
    .coin-sym { font-size: 20px; font-weight: 900; letter-spacing: -0.5px; }
    .status-tag {
      background: rgba(0, 230, 118, 0.12);
      color: var(--green);
      border: 1px solid rgba(0, 230, 118, 0.3);
      padding: 4px 9px;
      border-radius: 8px;
      font-size: 11px;
      font-weight: 800;
    }
    .status-tag.rebuy {
      background: rgba(255, 234, 0, 0.12);
      color: var(--gold);
      border-color: rgba(255, 234, 0, 0.35);
    }
    .price-big { font-size: 29px; font-weight: 900; letter-spacing: -0.8px; margin: 8px 0 2px 0; }
    .prog-track { background: rgba(255,255,255,0.06); height: 7px; border-radius: 99px; overflow: hidden; margin: 10px 0; }
    .prog-fill { background: linear-gradient(90deg, var(--cyan), var(--green)); height: 100%; border-radius: 99px; transition: width 0.4s ease; }
    .metrics-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 8px;
      margin-top: 10px;
    }
    .metric-box {
      background: rgba(0, 0, 0, 0.35);
      border: 1px solid rgba(255, 255, 255, 0.05);
      border-radius: 12px;
      padding: 9px 11px;
    }
    .m-label { font-size: 10.5px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.4px; }
    .m-val { font-size: 14px; font-weight: 800; margin-top: 3px; }
    .ai-strip {
      background: rgba(0, 176, 255, 0.08);
      border: 1px solid rgba(0, 176, 255, 0.22);
      border-radius: 11px;
      padding: 9px 11px;
      font-size: 12px;
      color: #b3e5fc;
      margin-top: 10px;
      line-height: 1.45;
    }
    .card-actions {
      display: flex;
      gap: 8px;
      margin-top: 10px;
    }
    .card-actions button { flex: 1; padding: 9px 10px; font-size: 12px; }
    .terminal {
      background: #04060a;
      border: 1px solid rgba(255,255,255,0.07);
      border-radius: 12px;
      padding: 10px;
      font-family: "JetBrains Mono", monospace;
      font-size: 11px;
      color: #69f0ae;
      max-height: 180px;
      overflow-y: auto;
      line-height: 1.55;
    }
    #key-drawer { display: none; margin-top: 12px; padding-top: 12px; border-top: 1px solid var(--border); }
  </style>
</head>
<body>

  <div class="glass">
    <div class="top-bar">
      <div class="brand">
        <span id="bridge-dot" class="live-dot"></span>
        <span>VaultSkim <span style="color:var(--cyan);">1.0% Fast</span></span>
      </div>
      <div class="action-bar">
        <button class="chip-btn gold" onclick="toggleKeyDrawer()">🔑 Keys</button>
        <button class="chip-btn" onclick="refresh(true)">⚡ Sync</button>
        <button class="chip-btn" onclick="toggleFreeze()" id="freeze-btn">❄️ Freeze</button>
      </div>
    </div>

    <div class="wallet-strip">
      <span id="bridge-status">GATE.IO ARMED</span>
      <span>Total USDT: <strong style="color:var(--gold);font-size:13.5px;" id="usdt-total">$0.00</strong> <span style="font-size:11px;color:var(--green);" id="usdt-free">(Free: $0.0000)</span></span>
      <span id="tick-clock">Live</span>
    </div>

    <div id="key-drawer">
      <div style="font-size:12px;font-weight:700;color:var(--gold);margin-bottom:8px;">Gate.io Spot API v4 Credentials</div>
      <input class="input-modern" id="gate-key-input" placeholder="API Key" style="margin-bottom:8px;" />
      <input class="input-modern" id="gate-secret-input" type="password" placeholder="Secret Key" style="margin-bottom:8px;" />
      <button class="btn-primary" style="width:100%;" onclick="saveAndConnectKeys()">Connect & Lock Wallet</button>
      <div id="key-msg" style="font-size:12px;margin-top:6px;"></div>
    </div>
  </div>

  <div class="glass">
    <div class="hero-grid">
      <div>
        <div style="font-size:10.5px;color:var(--muted);font-weight:700;letter-spacing:0.5px;">REALIZED SKIM PROFIT (+1.0% FAST MODE)</div>
        <div class="profit-val" id="total-profit">$0.0000</div>
        <div style="font-size:11.5px;color:var(--muted);">
          <strong style="color:var(--cyan);" id="skim-count">0</strong> Verified Skims •
          <span style="text-decoration:underline;cursor:pointer;" onclick="resetStats()">Reset $0</span>
        </div>
      </div>
      <div>
        <svg viewBox="0 0 120 42" width="100%" height="42">
          <defs>
            <linearGradient id="g1" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stop-color="#00e676" stop-opacity="0.35"/>
              <stop offset="100%" stop-color="#00e676" stop-opacity="0.0"/>
            </linearGradient>
          </defs>
          <path d="M2,36 Q25,32 45,22 T85,14 T118,6 L118,40 L2,40 Z" fill="url(#g1)"/>
          <path d="M2,36 Q25,32 45,22 T85,14 T118,6" fill="none" stroke="#00e676" stroke-width="2.2"/>
        </svg>
      </div>
    </div>
  </div>

  <div class="glass" style="padding:12px;">
    <div style="font-size:11px;color:var(--muted);margin-bottom:6px;font-weight:600;">
      🚀 Fast-Skim (+1.0% Bounce / 0.20% Pullback / -0.75% Rebuy):
    </div>
    <div class="launch-bar">
      <input class="input-modern" id="coin-input" placeholder="Token (e.g. SUI, DOGE, SOL)" style="flex:2;" />
      <input class="input-modern" id="budget-input" type="number" value="7.3" placeholder="$" style="flex:0.8;" />
      <button class="btn-primary" onclick="launchCoin()">+ Launch</button>
    </div>
    <div id="err-msg" style="color:#ff5252;font-size:12px;margin-top:6px;display:none;"></div>
  </div>

  <div id="coins-container"></div>

  <div class="glass">
    <div style="font-size:11.5px;font-weight:800;color:var(--muted);margin-bottom:8px;letter-spacing:0.4px;">⚡ EXECUTION STREAM</div>
    <div class="terminal" id="logs">Ready in +1.0% Fast-Skim mode!</div>
  </div>

<script>
const SERVER_INITIAL_STATE = {{ initial_state_json | safe }};
let isRefreshing = false;

function renderState(d) {
  if (!d) return;
  localStorage.setItem('VAULTSKIM_V12_STATE', JSON.stringify(d));

  const totalUsdt = Number(d.usdt_total || d.usdt_free || 0);
  const freeUsdt = Number(d.usdt_free || 0);
  document.getElementById('usdt-total').innerText = '$' + totalUsdt.toFixed(2);
  document.getElementById('usdt-free').innerText = '(Free: $' + freeUsdt.toFixed(4) + ')';

  document.getElementById('total-profit').innerText = '$' + Number(d.realized_usdt_profit || 0).toFixed(4);
  document.getElementById('skim-count').innerText = (d.completed_skims || 0);

  const dot = document.getElementById('bridge-dot');
  const bstatus = document.getElementById('bridge-status');
  if (d.keys_connected) {
    dot.className = 'live-dot';
    bstatus.innerText = 'GATE.IO ARMED';
  } else {
    dot.className = 'off-dot';
    bstatus.innerText = 'KEYS REQUIRED';
  }

  document.getElementById('freeze-btn').innerText = d.frozen ? '🔥 Resume' : '❄️ Freeze';

  const coinsList = Object.values(d.coins || {});
  const container = document.getElementById('coins-container');

  if (coinsList.length === 0) {
    container.innerHTML = `
      <div class="glass" style="text-align:center;color:var(--muted);font-size:13px;padding:22px;">
        No active coin positions running.<br>
        <span style="color:var(--text);font-weight:700;">Your $${freeUsdt.toFixed(2)} USDT is 100% free in Gate.io!</span><br>
        Type any token above (like <strong>SUI</strong>, <strong>DOGE</strong>, or <strong>SOL</strong>) and tap <strong>+ Launch</strong>.
      </div>`;
  } else {
    container.innerHTML = coinsList.map(c => {
      const isRebuy = (c.mode === 'REBUYING' || c.mode === 'CANCELED_IDLE');
      const lowPx = c.lowest_low || c.live_price || 1;
      const rebuyPx = c.active_order_price || lowPx;
      const bouncePct = Math.max(0, ((c.live_price - lowPx) / lowPx) * 100);
      const distToRebuyPct = Math.max(0, ((c.live_price - rebuyPx) / c.live_price) * 100);
      const prog = isRebuy
        ? Math.max(8, Math.min(100, (1.0 - distToRebuyPct) / 1.0 * 100))
        : Math.max(5, Math.min(100, (bouncePct / 1.00) * 100));

      const lockedUsdt = Number(c.locked_order_usdt || 0);
      const posSub = lockedUsdt > 0
        ? `$${Number(c.position_value || 0).toFixed(2)} ($${lockedUsdt.toFixed(2)} in Order)`
        : `$${Number(c.position_value || 0).toFixed(2)}`;

      return `
        <div class="glass">
          <div class="coin-header">
            <div>
              <div class="coin-sym">${c.symbol}</div>
              <div style="font-size:11px;color:var(--muted);">Position Value: <strong style="color:#fff;">${posSub}</strong></div>
            </div>
            <span class="status-tag ${isRebuy ? 'rebuy' : ''}">${c.status}</span>
          </div>

          <div style="display:flex;align-items:baseline;gap:10px;">
            <div class="price-big">$${Number(c.live_price || 0).toFixed(4)}</div>
            <div style="color:${isRebuy ? 'var(--gold)' : 'var(--green)'};font-size:13px;font-weight:700;">
              ${isRebuy ? `Needs -${distToRebuyPct.toFixed(2)}% dip to Rebuy` : `+${bouncePct.toFixed(2)}% from Sliding Low`}
            </div>
          </div>

          <div class="prog-track"><div class="prog-fill" style="width:${prog}%;"></div></div>

          <div class="metrics-grid">
            <div class="metric-box">
              <div class="m-label">${isRebuy ? 'Limit Rebuy Price (-0.75%)' : 'Sliding Low Anchor'}</div>
              <div class="m-val" style="color:var(--cyan);">$${Number(isRebuy ? rebuyPx : lowPx).toFixed(4)}</div>
            </div>
            <div class="metric-box">
              <div class="m-label">${isRebuy ? 'Last Peak Sold' : 'Harvest Target (+1.0%)'}</div>
              <div class="m-val" style="color:var(--gold);">$${Number(isRebuy ? (c.last_sell_price || c.live_price) : (c.harvest_target || 0)).toFixed(4)}</div>
            </div>
            <div class="metric-box">
              <div class="m-label">Real Gate.io Shares</div>
              <div class="m-val" style="color:var(--green);">${Number(c.real_shares || 0).toFixed(6)} ${c.base}</div>
            </div>
            <div class="metric-box">
              <div class="m-label">Protected Floor</div>
              <div class="m-val">🛡️ ${Number(c.protected_shares || 0).toFixed(6)}</div>
            </div>
          </div>

          <div class="ai-strip">
            🧠 ${c.ai_note || 'Monitoring live Gate.io order book...'}
          </div>

          <div class="card-actions">
            ${isRebuy
              ? `<button class="chip-btn cyan" onclick="rebuyNow('${c.symbol}')">🔄 Rebuy ${c.base} Now</button>`
              : `<button class="chip-btn cyan" onclick="snapLowNow('${c.symbol}')">🎯 Snap Low Here</button>`
            }
            <button class="chip-btn red" onclick="cancelAndRemove('${c.symbol}')">❌ Cancel & Stop</button>
          </div>
        </div>`;
    }).join('');
  }

  if (d.logs && d.logs.length > 0) {
    document.getElementById('logs').innerHTML = d.logs.join('<br>');
  }
}

window.addEventListener('DOMContentLoaded', () => {
  const savedKey = localStorage.getItem('VAULTSKIM_GATE_KEY') || '';
  const savedSec = localStorage.getItem('VAULTSKIM_GATE_SECRET') || '';
  if (savedKey && savedSec) {
    document.getElementById('gate-key-input').value = savedKey;
    document.getElementById('gate-secret-input').value = savedSec;
  }

  renderState(SERVER_INITIAL_STATE);
  refresh();
  setInterval(refresh, 5000);
});

function toggleKeyDrawer() {
  const el = document.getElementById('key-drawer');
  el.style.display = (el.style.display === 'block') ? 'none' : 'block';
}

async function saveAndConnectKeys() {
  const apiKey = document.getElementById('gate-key-input').value.trim();
  const apiSecret = document.getElementById('gate-secret-input').value.trim();
  const msgEl = document.getElementById('key-msg');
  msgEl.style.color = '#00b0ff';
  msgEl.innerText = 'Verifying with Gate.io...';

  localStorage.setItem('VAULTSKIM_GATE_KEY', apiKey);
  localStorage.setItem('VAULTSKIM_GATE_SECRET', apiSecret);

  const res = await fetch('/connect_keys', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({apiKey, apiSecret})
  });
  const json = await res.json();
  if (json.error) {
    msgEl.style.color = '#ff5252';
    msgEl.innerText = json.error;
  } else {
    msgEl.style.color = '#00e676';
    msgEl.innerText = '✅ Connected!';
    document.getElementById('key-drawer').style.display = 'none';
    renderState(json.data);
  }
}

async function refresh(force = false) {
  if (isRefreshing && !force) return;
  isRefreshing = true;
  try {
    const savedKey = localStorage.getItem('VAULTSKIM_GATE_KEY') || '';
    const savedSec = localStorage.getItem('VAULTSKIM_GATE_SECRET') || '';
    const res = await fetch('/tick', {
      headers: {'X-Gate-Key': savedKey, 'X-Gate-Secret': savedSec}
    });
    const json = await res.json();
    if (json.data) {
      renderState(json.data);
      document.getElementById('tick-clock').innerText = new Date().toLocaleTimeString();
    }
  } catch (e) {
    console.error(e);
  } finally {
    isRefreshing = false;
  }
}

async function cancelAndRemove(symbol) {
  const res = await fetch('/cancel_coin', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({symbol})
  });
  const json = await res.json();
  if (json.data) renderState(json.data);
}

async function rebuyNow(symbol) {
  const res = await fetch('/rebuy_now', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({symbol})
  });
  const json = await res.json();
  if (json.error) {
    alert(json.error);
  } else if (json.data) {
    renderState(json.data);
  }
}

async function snapLowNow(symbol) {
  const res = await fetch('/reset_low', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({symbol})
  });
  const json = await res.json();
  if (json.data) renderState(json.data);
}

async function resetStats() {
  const res = await fetch('/reset_stats', {method: 'POST'});
  const json = await res.json();
  if (json.data) renderState(json.data);
}

async function launchCoin() {
  const symbol = document.getElementById('coin-input').value;
  const budget = document.getElementById('budget-input').value;
  const apiKey = localStorage.getItem('VAULTSKIM_GATE_KEY') || '';
  const apiSecret = localStorage.getItem('VAULTSKIM_GATE_SECRET') || '';
  const errEl = document.getElementById('err-msg');

  errEl.style.display = 'block';
  errEl.style.color = '#00b0ff';
  errEl.innerText = 'Executing on Gate.io Spot...';

  const res = await fetch('/launch', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({symbol, budget, apiKey, apiSecret})
  });
  const json = await res.json();
  if (json.error) {
    errEl.style.color = '#ff5252';
    errEl.innerText = json.error;
  } else {
    errEl.style.display = 'none';
    document.getElementById('coin-input').value = '';
    renderState(json.data);
  }
}

async function toggleFreeze() {
  const res = await fetch('/toggle_freeze', {method: 'POST'});
  const json = await res.json();
  if (json.data) renderState(json.data); 
}
</script>
</body>
</html>
"""

@app.route("/", methods=["GET"])
def dashboard():
    initial_json = json.dumps(get_public_state())
    return render_template_string(HTML_UI, initial_state_json=initial_json)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
