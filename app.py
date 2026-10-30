# ======================================================================
# VaultSkim v6.0 - Safer Architecture
# Part 1/2 - Core engine, persistence, market data, safety checks
# ======================================================================

import json
import os
import logging
import threading
import time
import uuid
import hmac
import hashlib
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from urllib.parse import urlencode

import requests
from flask import Flask, jsonify, request, make_response, Response, render_template_string

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("VaultSkimSafe")

app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False

# --------------------------------------------------------------
# Global config
# --------------------------------------------------------------
STATE_FILE = os.environ.get("VAULTSKIM_STATE_FILE", "vaultskim_state_safe.json")
ALLOW_LIVE_TRADING = os.environ.get("ALLOW_LIVE_TRADING", "0").lower() in {"1", "true", "yes"}
GATEIO_MIN_ORDER_USD = 3.15

state_lock = threading.RLock()
tick_lock = threading.Lock()
order_cooldowns = {}
pair_rules_cache = {}
price_cache = {}

_worker_pid = None
_engine_thread = None
_last_engine_tick_ts = 0.0
_last_wallet_sync_ts = 0.0

DEFAULT_STATE = {
    "version": "v6.0-safe",
    "state_updated_ts": 0.0,
    "vault_cash": 0.0,
    "added_to_anchors": 0.0,
    "total_escrow_reserved": 0.0,
    "total_skims": 0,
    "total_withdrawn": 0.0,
    "frozen": False,
    "last_price_tick_hms": "--:--:--",
    "profit_curve": [{"time": datetime.now(timezone.utc).strftime("%H:%M:%S"), "value": 0.0}],
    "skim_ledger": [],
    "cashout_history": [],
    "logs": [
        f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] VaultSkim v6.0 safe mode initialized."
    ],
    "exchange_bridge": {
        "exchange": "gateio",
        "api_key": "",
        "api_secret": "",
        "max_order_usd": 250.0,
        "live_armed": False,
        "verified": False,
        "available_usdt": 0.0,
        "baseline_usdt": 0.0,
        "realized_usdt_profit": 0.0,
        "last_sync_ts": 0
    },
    "alerts": {
        "enabled": False,
        "ntfy_topic": "",
        "telegram_bot_token": "",
        "telegram_chat_id": "",
        "alert_on_skim": True,
        "alert_on_shield": True,
        "alert_on_buy": True
    },
    "coins": {}
}

# --------------------------------------------------------------
# Helpers
# --------------------------------------------------------------
def now_hms():
    return datetime.now(timezone.utc).strftime("%H:%M:%S")

def quantize_value(val, decimals, mode=ROUND_DOWN):
    try:
        d = int(decimals)
        q = Decimal("10") ** (-d) if d > 0 else Decimal("1")
        return str(Decimal(str(val)).quantize(q, rounding=mode))
    except Exception:
        return f"{float(val):.{int(decimals)}f}"

def safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return float(default)

def sanitize_state_for_persistence(state_obj):
    safe = json.loads(json.dumps(state_obj))
    if "exchange_bridge" in safe:
        safe["exchange_bridge"]["api_secret"] = ""
        safe["exchange_bridge"]["api_key"] = (
            (safe["exchange_bridge"].get("api_key") or "")[:4] + "..."
            if safe["exchange_bridge"].get("api_key")
            else ""
        )
    return safe

# --------------------------------------------------------------
# State persistence
# --------------------------------------------------------------
def merge_default_state(loaded):
    merged = json.loads(json.dumps(DEFAULT_STATE))
    if not isinstance(loaded, dict):
        return merged

    for k, v in loaded.items():
        if isinstance(v, dict) and k in merged and isinstance(merged[k], dict) and k != "coins":
            merged[k].update(v)
        else:
            merged[k] = v

    merged["version"] = "v6.0-safe"

    for sym, c in merged.get("coins", {}).items():
        if not isinstance(c, dict):
            continue
        c.setdefault("symbol", sym)
        c.setdefault("shares", 0.0)
        c.setdefault("protected_shares", 0.0)
        c.setdefault("current_price", 0.0)
        c.setdefault("source", "UNKNOWN")
        c.setdefault("live_enabled", False)
        c.setdefault("status", "HOLDING_ANCHOR")
        c.setdefault("entry_price", 0.0)
        c.setdefault("sliding_low", 0.0)
        c.setdefault("highest_price", 0.0)
        c.setdefault("zero_loss_stop", 0.0)
        c.setdefault("gain_from_low_pct", 0.0)
        c.setdefault("anchor_usd", 10.0)
        c.setdefault("escrow_usdt_reserved", 0.0)
        c.setdefault("escrow_sold_shares", 0.0)
        c.setdefault("escrow_sell_price", 0.0)
        c.setdefault("escrow_rebuy_target_price", 0.0)
        c.setdefault("bonus_shares_added", 0.0)
        c.setdefault("share_topup_usd", 0.0)
        c.setdefault("vaulted_usd", 0.0)
        c.setdefault("skim_count", 0)
        c.setdefault("ratchet_armed", False)
        c.setdefault("strategy", "50/50 Hybrid Moonbag")
        c.setdefault("contract", "")
    return merged

def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                return merge_default_state(data)
        except Exception as exc:
            logger.warning(f"Could not read state file: {exc}")
    return json.loads(json.dumps(DEFAULT_STATE))

def save_state():
    with state_lock:
        snapshot = json.loads(json.dumps(app_state))
        snapshot["state_updated_ts"] = time.time()
        total_escrow = sum(float(c.get("escrow_usdt_reserved") or 0.0) for c in snapshot.get("coins", {}).values())
        snapshot["total_escrow_reserved"] = round(total_escrow, 4)
        safe_snapshot = sanitize_state_for_persistence(snapshot)
    try:
        tmp = f"{STATE_FILE}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(safe_snapshot, f, indent=2)
        os.replace(tmp, STATE_FILE)
    except Exception as exc:
        logger.warning(f"State save failed: {exc}")

app_state = load_state()
save_state()

# --------------------------------------------------------------
# Alert dispatch
# --------------------------------------------------------------
def dispatch_phone_alert(title, message, category="skim"):
    with state_lock:
        alerts = dict(app_state.get("alerts", {}))

    if not alerts.get("enabled"):
        return
    if category == "skim" and not alerts.get("alert_on_skim", True):
        return
    if category == "shield" and not alerts.get("alert_on_shield", True):
        return
    if category == "buy" and not alerts.get("alert_on_buy", True):
        return

    topic = (alerts.get("ntfy_topic") or "").strip()
    tg_token = (alerts.get("telegram_bot_token") or "").strip()
    tg_chat = (alerts.get("telegram_chat_id") or "").strip()

    def _send():
        if topic:
            try:
                requests.post(
                    f"https://ntfy.sh/{topic}",
                    data=message.encode("utf-8"),
                    headers={"Title": title, "Priority": "high", "Tags": "chart_with_upwards_trend,moneybag"},
                    timeout=5
                )
            except Exception:
                pass
        if tg_token and tg_chat:
            try:
                requests.post(
                    f"https://api.telegram.org/bot{tg_token}/sendMessage",
                    json={"chat_id": tg_chat, "text": f"*{title}*\n{message}", "parse_mode": "Markdown"},
                    timeout=5
                )
            except Exception:
                pass

    threading.Thread(target=_send, daemon=True).start()

# --------------------------------------------------------------
# Logging
# --------------------------------------------------------------
def add_log(message):
    with state_lock:
        entry = f"[{now_hms()}] {message}"
        app_state["logs"].insert(0, entry)
        if len(app_state["logs"]) > 120:
            app_state["logs"] = app_state["logs"][:120]

# --------------------------------------------------------------
# Exchange signing
# --------------------------------------------------------------
def gateio_v4_sign(api_key, api_secret, method, url_path, query_string="", body_str=""):
    ts = str(int(time.time()))
    m = hashlib.sha512()
    m.update((body_str or "").encode("utf-8"))
    body_hash = m.hexdigest()
    sign_string = f"{method.upper()}\\n{url_path}\\n{query_string}\\n{body_hash}\\n{ts}"
    signature = hmac.new(api_secret.strip().encode("utf-8"), sign_string.encode("utf-8"), hashlib.sha512).hexdigest()
    return {
        "KEY": api_key.strip(),
        "Timestamp": ts,
        "SIGN": signature,
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

def binance_v3_sign(api_secret, query_params):
    query_string = urlencode(query_params)
    signature = hmac.new(api_secret.strip().encode("utf-8"), query_string.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{query_string}&signature={signature}"

# --------------------------------------------------------------
# Market data
# --------------------------------------------------------------
def get_pair_rules(symbol):
    sym = symbol.upper().strip()
    pair = f"{sym}_USDT"
    now_ts = time.time()

    if pair in pair_rules_cache and (now_ts - pair_rules_cache[pair]["ts"]) < 3600:
        return pair_rules_cache[pair]

    default_rules = {
        "amount_precision": 4,
        "price_precision": 4,
        "min_quote_amount": GATEIO_MIN_ORDER_USD,
        "min_base_amount": 0.0001,
        "fee_rate": 0.001,
        "ts": now_ts
    }

    try:
        r = requests.get(f"https://api.gateio.ws/api/v4/spot/currency_pairs/{pair}", timeout=5)
        if r.status_code == 200:
            data = r.json()
            raw_min_q = float(data.get("min_quote_amount") or 3.0)
            rules = {
                "amount_precision": int(data.get("amount_precision", 4)),
                "price_precision": int(data.get("precision", 4)),
                "min_quote_amount": max(raw_min_q, GATEIO_MIN_ORDER_USD),
                "min_base_amount": float(data.get("min_base_amount") or 0.0001),
                "fee_rate": float(data.get("fee") or 0.1) / 100.0,
                "ts": now_ts
            }
            pair_rules_cache[pair] = rules
            return rules
    except Exception:
        pass

    pair_rules_cache[pair] = default_rules
    return default_rules

def fetch_live_market_quote(symbol, contract=""):
    sym = symbol.upper().strip().replace("/USDT", "").replace("_USDT", "")
    now_ts = time.time()
    headers = {
        "Accept": "application/json",
        "Cache-Control": "no-cache, no-store, must-revalidate",
        "Pragma": "no-cache",
        "User-Agent": f"VaultSkimSafe/{int(now_ts)}"
    }

    # Gate.io primary
    try:
        r = requests.get(
            "https://api.gateio.ws/api/v4/spot/tickers",
            params={"currency_pair": f"{sym}_USDT", "_t": int(now_ts * 1000)},
            headers=headers,
            timeout=5
        )
        if r.status_code == 200:
            arr = r.json()
            if isinstance(arr, list) and len(arr) > 0:
                t_obj = None
                for item in arr:
                    if str(item.get("currency_pair") or "").upper() == f"{sym}_USDT":
                        t_obj = item
                        break
                if t_obj is None:
                    t_obj = arr[0]

                last_p = float(t_obj.get("last") or 0.0)
                ask_p = float(t_obj.get("lowest_ask") or last_p)
                bid_p = float(t_obj.get("highest_bid") or last_p)
                if ask_p > 0 and bid_p > 0 and (last_p < bid_p or last_p > ask_p):
                    last_p = round((ask_p + bid_p) / 2.0, 6)
                if last_p > 0:
                    res = {
                        "symbol": sym,
                        "price": last_p,
                        "ask": ask_p if ask_p > 0 else last_p,
                        "bid": bid_p if bid_p > 0 else last_p,
                        "change_24h": float(t_obj.get("change_percentage") or 0.0),
                        "volume_24h": float(t_obj.get("quote_volume") or 0.0),
                        "liquidity_usd": max(float(t_obj.get("quote_volume") or 0.0), 100000.0),
                        "source": "GATE.IO SPOT",
                        "is_dex": False,
                        "ts": now_ts
                    }
                    price_cache[sym] = res
                    return res
    except Exception:
        pass

    # Binance secondary
    try:
        r = requests.get(
            "https://api.binance.com/api/v3/ticker/bookTicker",
            params={"symbol": f"{sym}USDT"},
            headers=headers,
            timeout=4
        )
        if r.status_code == 200:
            data = r.json()
            bid = float(data.get("bidPrice") or 0.0)
            ask = float(data.get("askPrice") or 0.0)
            mid = (bid + ask) / 2.0 if (bid > 0 and ask > 0) else max(bid, ask)
            if mid > 0:
                res = {
                    "symbol": sym,
                    "price": mid,
                    "ask": ask if ask > 0 else mid,
                    "bid": bid if bid > 0 else mid,
                    "change_24h": 0.0,
                    "volume_24h": 100000.0,
                    "liquidity_usd": 100000.0,
                    "source": "BINANCE SPOT",
                    "is_dex": False,
                    "ts": now_ts
                }
                price_cache[sym] = res
                return res
    except Exception:
        pass

    # DEX fallback
    lookup = contract.strip() if contract and len(contract.strip()) > 15 else sym
    if lookup and len(lookup) > 8:
        try:
            url = f"https://api.dexscreener.com/latest/dex/search?q={lookup}" if len(lookup) <= 15 else f"https://api.dexscreener.com/latest/dex/tokens/{lookup}"
            r = requests.get(url, headers=headers, timeout=5)
            if r.status_code == 200:
                payload = r.json()
                pairs = payload.get("pairs") or []
                if pairs:
                    best = max(pairs, key=lambda p: float((p.get("liquidity") or {}).get("usd") or 0.0))
                    price = float(best.get("priceUsd") or 0.0)
                    liq = float((best.get("liquidity") or {}).get("usd") or 0.0)
                    if price > 0:
                        res = {
                            "symbol": (best.get("baseToken") or {}).get("symbol") or sym,
                            "price": price,
                            "ask": price * 1.001,
                            "bid": price * 0.999,
                            "change_24h": float((best.get("priceChange") or {}).get("h24") or 0.0),
                            "volume_24h": float((best.get("volume") or {}).get("h24") or 0.0),
                            "liquidity_usd": liq,
                            "source": f"SAFE DEX (${int(liq/1000)}K Liq)",
                            "is_dex": True,
                            "ts": now_ts
                        }
                        price_cache[sym] = res
                        return res
        except Exception:
            pass

    return price_cache.get(sym)

# --------------------------------------------------------------
# Wallet balance + order execution
# --------------------------------------------------------------
def fetch_live_exchange_balances():
    with state_lock:
        bridge = dict(app_state.get("exchange_bridge", {}))

    exch = (bridge.get("exchange") or "gateio").lower()
    api_key = (bridge.get("api_key") or "").strip()
    api_secret = (bridge.get("api_secret") or "").strip()

    if not api_key or not api_secret:
        return None, "Missing API key or secret."

    if exch == "gateio":
        url_path = "/api/v4/spot/accounts"
        headers = gateio_v4_sign(api_key, api_secret, "GET", url_path)
        try:
            r = requests.get(f"https://api.gateio.ws{url_path}", headers=headers, timeout=8)
            if r.status_code == 200:
                rows = r.json()
                bal_map = {}
                for item in rows:
                    curr = (item.get("currency") or "").upper()
                    avail = float(item.get("available") or 0.0)
                    locked = float(item.get("locked") or 0.0)
                    bal_map[curr] = {"available": avail, "locked": locked, "total": avail + locked}
                return bal_map, None
            return None, f"Gate.io API ({r.status_code}): {r.text}"
        except Exception as exc:
            return None, f"Gate.io connection error: {exc}"

    if exch == "binance":
        params = {"timestamp": int(time.time() * 1000), "recvWindow": 5000}
        signed_q = binance_v3_sign(api_secret, params)
        headers = {"X-MBX-APIKEY": api_key}
        try:
            r = requests.get(f"https://api.binance.com/api/v3/account?{signed_q}", headers=headers, timeout=8)
            if r.status_code == 200:
                balances = r.json().get("balances") or []
                bal_map = {}
                for b in balances:
                    curr = (b.get("asset") or "").upper()
                    avail = float(b.get("free") or 0.0)
                    locked = float(b.get("locked") or 0.0)
                    if avail > 0 or locked > 0 or curr == "USDT":
                        bal_map[curr] = {"available": avail, "locked": locked, "total": avail + locked}
                return bal_map, None
            return None, f"Binance API ({r.status_code}): {r.text}"
        except Exception as exc:
            return None, f"Binance connection error: {exc}"

    return None, f"Unsupported exchange: {exch}"

def execute_live_exchange_order(symbol, side, usd_amount=None, share_amount=None, dry_run=False):
    with state_lock:
        bridge = dict(app_state.get("exchange_bridge", {}))

    if not ALLOW_LIVE_TRADING and not dry_run:
        return False, "Live trading disabled by configuration. Set ALLOW_LIVE_TRADING=1 to enable.", None

    exch = (bridge.get("exchange") or "gateio").lower()
    api_key = (bridge.get("api_key") or "").strip()
    api_secret = (bridge.get("api_secret") or "").strip()
    max_cap = float(bridge.get("max_order_usd") or 250.0)

    if not api_key or not api_secret:
        return False, "Missing API credentials.", None

    sym = symbol.upper().strip()
    quote = fetch_live_market_quote(sym)
    if not quote or quote.get("price", 0) <= 0:
        return False, f"Unable to read live price for {sym}/USDT.", None

    balances, err = fetch_live_exchange_balances()
    if balances is None:
        return False, f"Wallet check failed: {err}", None

    usdt_avail = balances.get("USDT", {}).get("available", 0.0)
    coin_avail = balances.get(sym, {}).get("available", 0.0)

    with state_lock:
        app_state["exchange_bridge"]["available_usdt"] = round(usdt_avail, 4)

    rules = get_pair_rules(sym)
    min_order_usd = max(float(rules.get("min_quote_amount") or GATEIO_MIN_ORDER_USD), GATEIO_MIN_ORDER_USD)
    side_lower = side.lower()

    if side_lower == "buy":
        requested = min(float(usd_amount or 10.0), max_cap)
        if requested < min_order_usd:
            if usdt_avail >= (min_order_usd * 1.02):
                requested = min_order_usd * 1.02
            else:
                return False, f"Available USDT too low for {sym}; Gate.io minimum is ${min_order_usd:.2f}.", None

        if usdt_avail < requested:
            if usdt_avail >= 3.08:
                requested = usdt_avail * 0.992
            else:
                return False, f"Insufficient USDT available: ${usdt_avail:.2f}.", None

        exec_price = quote["ask"] * 1.0035
        price_str = quantize_value(exec_price, rules["price_precision"], ROUND_HALF_UP)
        exec_price_float = float(price_str)

        raw_shares = (requested * 0.994) / exec_price_float
        amount_str = quantize_value(raw_shares, rules["amount_precision"], ROUND_DOWN)
        amount_float = float(amount_str)

        if amount_float <= 0 or (amount_float * exec_price_float) < 3.00:
            return False, f"Buy order too small to satisfy Gate.io minimum notional.", None

    elif side_lower == "sell":
        if coin_avail <= 0:
            return False, f"0.0000 {sym} available to sell.", None

        if share_amount is None or float(share_amount) >= (coin_avail * 0.90):
            raw_shares = coin_avail * 0.9998
        else:
            raw_shares = min(float(share_amount), coin_avail * 0.9998)

        exec_price = quote["bid"] * 0.9965
        price_str = quantize_value(exec_price, rules["price_precision"], ROUND_HALF_UP)
        exec_price_float = float(price_str)

        if (raw_shares * exec_price_float) < min_order_usd:
            if (coin_avail * exec_price_float) >= min_order_usd:
                raw_shares = min(coin_avail * 0.9998, (min_order_usd * 1.05) / exec_price_float)
            else:
                return False, f"Position value under Gate.io $3.00 minimum.", None

        amount_str = quantize_value(raw_shares, rules["amount_precision"], ROUND_DOWN)
        amount_float = float(amount_str)
        if amount_float <= 0 or (amount_float * exec_price_float) < 3.00:
            return False, "Sell order below Gate.io minimum.", None
    else:
        return False, f"Unsupported side: {side}", None

    if exch == "gateio":
        url_path = "/api/v4/spot/orders"
        payload = {
            "currency_pair": f"{sym}_USDT",
            "type": "limit",
            "account": "spot",
            "side": side_lower,
            "amount": amount_str,
            "price": price_str,
            "time_in_force": "ioc"
        }
        body_str = json.dumps(payload)
        headers = gateio_v4_sign(api_key, api_secret, "POST", url_path, "", body_str)

        try:
            r = requests.post(f"https://api.gateio.ws{url_path}", headers=headers, data=body_str, timeout=8)
            if r.status_code in (200, 201):
                resp = r.json()
                left_amt = float(resp.get("left") or 0.0)
                orig_amt = float(resp.get("amount") or amount_str)
                filled_amt = max(0.0, orig_amt - left_amt)

                if filled_amt <= 0:
                    payload["time_in_force"] = "gtc"
                    body_str = json.dumps(payload)
                    headers = gateio_v4_sign(api_key, api_secret, "POST", url_path, "", body_str)
                    r2 = requests.post(f"https://api.gateio.ws{url_path}", headers=headers, data=body_str, timeout=8)
                    if r2.status_code in (200, 201):
                        resp = r2.json()
                        left_amt = float(resp.get("left") or 0.0)
                        orig_amt = float(resp.get("amount") or amount_str)
                        filled_amt = max(0.0, orig_amt - left_amt)

                if filled_amt <= 0:
                    return False, "Order accepted but no fill was confirmed by Gate.io.", None

                actual_filled_sh = filled_amt if filled_amt > 0 else amount_float
                filled_usdt = float(resp.get("filled_total") or (actual_filled_sh * float(quote.get("price") or exec_price_float)))
                resp["_filled_shares"] = actual_filled_sh
                resp["_filled_usdt"] = filled_usdt
                return True, "Order executed and validated.", resp

            err_txt = f"Gate.io order rejected ({r.status_code}): {r.text}"
            return False, err_txt, None
        except Exception as exc:
            return False, f"Gate.io order request failed: {exc}", None

    return False, "Only Gate.io execution is enabled in this safe build.", None

# --------------------------------------------------------------
# Conservative strategy
# --------------------------------------------------------------
def _sync_bridge_balance():
    bals, err = fetch_live_exchange_balances()
    if bals is not None:
        with state_lock:
            app_state["exchange_bridge"]["available_usdt"] = round(bals.get("USDT", {}).get("available", 0.0), 4)
            app_state["exchange_bridge"]["verified"] = True
            app_state["exchange_bridge"]["last_sync_ts"] = int(time.time())
        return bals
    return None

def _sell_tranche_to_escrow(sym, price):
    with state_lock:
        card = app_state["coins"].get(sym)
        if not card:
            return
        shares = float(card.get("shares") or 0.0)
        live_enabled = bool(card.get("live_enabled"))
        live_armed = bool(app_state["exchange_bridge"].get("live_armed"))
        if shares <= 0 or not (live_enabled and live_armed):
            return

    now_ts = time.time()
    if (now_ts - order_cooldowns.get(sym, 0)) < 12:
        return

    bag_usd = shares * price
    if bag_usd < GATEIO_MIN_ORDER_USD:
        return

    desired_tranche_usd = max(bag_usd * 0.22, 3.50)
    shares_to_sell = min(shares, desired_tranche_usd / max(price, 1e-8))
    order_cooldowns[sym] = now_ts

    ok, msg, resp = execute_live_exchange_order(sym, "sell", share_amount=shares_to_sell)
    if not ok or not resp:
        return

    sold_sh = float(resp.get("_filled_shares") or shares_to_sell)
    net_usdt = float(resp.get("_filled_usdt") or (sold_sh * price)) * 0.999
    time.sleep(0.6)

    with state_lock:
        card = app_state["coins"].get(sym)
        if not card:
            return
        card["escrow_usdt_reserved"] = round(net_usdt, 4)
        card["escrow_sold_shares"] = round(sold_sh, 6)
        card["escrow_sell_price"] = float(price)
        card["escrow_rebuy_target_price"] = round(price * 0.9958, 8)
        card["status"] = "ESCROW_HOLDING"
        card["ratchet_armed"] = False
        app_state["total_escrow_reserved"] = round(sum(float(c.get("escrow_usdt_reserved") or 0.0) for c in app_state["coins"].values()), 4)

    add_log(f"🔒 SAFER HARVEST: {sym} sold {sold_sh:.6f} at ${price:.4f}; reserve ${net_usdt:.2f} USDT")
    dispatch_phone_alert(
        f"Protected Harvest ({sym})",
        f"Sold {sold_sh:.6f} {sym} @ ${price:.4f} and locked ${net_usdt:.2f} USDT as reserve.",
        category="skim"
    )
    save_state()

def _rebuy_from_escrow(sym, current_price):
    with state_lock:
        card = app_state["coins"].get(sym)
        if not card:
            return
        escrow_usdt = float(card.get("escrow_usdt_reserved") or 0.0)
        sold_sh = float(card.get("escrow_sold_shares") or 0.0)
        if escrow_usdt <= 0 or sold_sh <= 0 or current_price <= 0:
            return
        price = current_price
        live_enabled = bool(card.get("live_enabled"))
        live_armed = bool(app_state["exchange_bridge"].get("live_armed"))

    if not (live_enabled and live_armed):
        return

    cost_to_restore = (sold_sh * current_price) * 1.0012
    raw_surplus = max(0.0, escrow_usdt - cost_to_restore)
    if raw_surplus <= 0.005:
        spend_usd = min(escrow_usdt, max(GATEIO_MIN_ORDER_USD, cost_to_restore * 1.0005))
    else:
        spend_usd = min(escrow_usdt, cost_to_restore + (raw_surplus * 0.50))

    order_cooldowns[sym] = time.time()
    ok, msg, resp = execute_live_exchange_order(sym, "buy", usd_amount=spend_usd)
    if not ok or not resp:
        return

    actual_spent = float(resp.get("_filled_usdt") or spend_usd)
    rebought_sh = float(resp.get("_filled_shares") or (spend_usd / current_price))
    pure_usdt_profit = max(0.0, round(escrow_usdt - actual_spent, 4))
    bonus_sh = max(0.0, rebought_sh - sold_sh)
    bonus_usd = round(bonus_sh * current_price, 4)

    with state_lock:
        card = app_state["coins"].get(sym)
        if not card:
            return

        card["shares"] = max(float(card.get("shares") or 0.0), rebought_sh)
        card["protected_shares"] = max(float(card.get("protected_shares") or 0.0), float(card["shares"]))
        card["bonus_shares_added"] = round(float(card.get("bonus_shares_added") or 0.0) + bonus_sh, 6)
        card["share_topup_usd"] = round(float(card.get("share_topup_usd") or 0.0) + bonus_usd, 4)
        card["vaulted_usd"] = round(float(card.get("vaulted_usd") or 0.0) + pure_usdt_profit, 4)
        card["skim_count"] = int(card.get("skim_count", 0)) + 1

        card["escrow_usdt_reserved"] = 0.0
        card["escrow_sold_shares"] = 0.0
        card["escrow_sell_price"] = 0.0
        card["escrow_rebuy_target_price"] = 0.0
        card["status"] = "HOLDING_ANCHOR"
        card["ratchet_armed"] = False
        card["zero_loss_stop"] = 0.0
        card["entry_price"] = current_price
        card["sliding_low"] = current_price
        card["highest_price"] = current_price
        card["gain_from_low_pct"] = 0.0

        app_state["vault_cash"] = round(float(app_state.get("vault_cash", 0.0)) + pure_usdt_profit, 4)
        app_state["added_to_anchors"] = round(float(app_state.get("added_to_anchors", 0.0)) + bonus_usd, 4)
        app_state["total_skims"] = int(app_state.get("total_skims", 0)) + 1
        app_state["exchange_bridge"]["realized_usdt_profit"] = round(float(app_state["exchange_bridge"].get("realized_usdt_profit") or 0.0) + pure_usdt_profit, 4)
        app_state["total_escrow_reserved"] = round(sum(float(c.get("escrow_usdt_reserved") or 0.0) for c in app_state["coins"].values()), 4)

        app_state["profit_curve"].append({"time": now_hms(), "value": round(app_state["vault_cash"] + app_state["added_to_anchors"], 4)})
        if len(app_state["profit_curve"]) > 50:
            app_state["profit_curve"] = app_state["profit_curve"][-50:]

        app_state["skim_ledger"].insert(0, {
            "id": str(uuid.uuid4())[:8],
            "symbol": sym,
            "mode": "LIVE",
            "stage": f"Safe Rebuy (+{bonus_sh:.5f} {sym})",
            "price": f"{current_price:.4f}",
            "time": now_hms(),
            "profit": round(pure_usdt_profit + bonus_usd, 4),
            "vault_cut": pure_usdt_profit,
            "topup_cut": bonus_usd,
            "shares": f"{float(card['shares']):.6f}"
        })
        if len(app_state["skim_ledger"]) > 60:
            app_state["skim_ledger"] = app_state["skim_ledger"][:60]

    add_log(f"💰 SAFE REBUY COMPLETE: {sym} restored shares + kept ${pure_usdt_profit:.4f} USDT in wallet.")
    dispatch_phone_alert(
        f"Safe Rebuy Completed ({sym})",
        f"Restored shares and kept +${pure_usdt_profit:.4f} USDT in wallet.",
        category="skim"
    )
    save_state()

# --------------------------------------------------------------
# Engine loop
# --------------------------------------------------------------
def evaluate_coin_tick(sym):
    with state_lock:
        card = app_state["coins"].get(sym)
        if not card:
            return
        live_enabled = bool(card.get("live_enabled"))
        live_armed = bool(app_state["exchange_bridge"].get("live_armed"))
        is_frozen = bool(app_state.get("frozen", False))
        current_status = card.get("status", "HOLDING_ANCHOR")
        shares = float(card.get("shares") or 0.0)
        price = float(card.get("current_price") or 0.0)

    if shares <= 0:
        return

    quote = fetch_live_market_quote(sym)
    if not quote or quote.get("price", 0) <= 0:
        return
    price = float(quote["price"])

    with state_lock:
        card = app_state["coins"].get(sym)
        if not card:
            return
        card["current_price"] = price
        card["source"] = quote.get("source", "GATE.IO SPOT")
        app_state["last_price_tick_hms"] = now_hms()

        if current_status == "ESCROW_HOLDING" and float(card.get("escrow_usdt_reserved") or 0.0) > 0:
            sell_px = float(card.get("escrow_sell_price") or price)
            rebuy_target = float(card.get("escrow_rebuy_target_price") or (price * 0.9958))
            if not is_frozen and price <= rebuy_target:
                pass
            return

        sliding_low = float(card.get("sliding_low") or price)
        if sliding_low <= 0 or price < sliding_low:
            sliding_low = price
            card["sliding_low"] = price
            card["entry_price"] = price
        gain_pct = ((price - sliding_low) / sliding_low) * 100.0 if sliding_low > 0 else 0.0
        card["gain_from_low_pct"] = round(gain_pct, 3)

        if price > float(card.get("highest_price") or price):
            card["highest_price"] = price

        if gain_pct >= 0.35 and not card.get("ratchet_armed"):
            card["ratchet_armed"] = True
            card["zero_loss_stop"] = sliding_low * 1.0022
            card["status"] = "RATCHET_ARMED"

    if not is_frozen and gain_pct >= 0.65 and live_enabled and live_armed:
        _sell_tranche_to_escrow(sym, price)
        return

    with state_lock:
        card = app_state["coins"].get(sym)
        if not card:
            return
        if card.get("status") == "ESCROW_HOLDING" and float(card.get("escrow_usdt_reserved") or 0.0) > 0:
            sell_px = float(card.get("escrow_sell_price") or price)
            rebuy_target = float(card.get("escrow_rebuy_target_price") or (sell_px * 0.9958))
            if price <= rebuy_target:
                _rebuy_from_escrow(sym, price)

def run_single_engine_cycle():
    global _last_engine_tick_ts
    if not tick_lock.acquire(blocking=False):
        return
    try:
        _last_engine_tick_ts = time.time()
        with state_lock:
            active_symbols = list(app_state.get("coins", {}).keys())

        for sym in active_symbols:
            evaluate_coin_tick(sym)

        if bool(app_state.get("exchange_bridge", {}).get("live_armed")) and (time.time() - _last_wallet_sync_ts) >= 30:
            global _last_wallet_sync_ts
            _last_wallet_sync_ts = time.time()
            _sync_bridge_balance()
    except Exception as exc:
        logger.debug(f"Engine cycle exception: {exc}")
    finally:
        tick_lock.release()

def market_engine_loop():
    while True:
        run_single_engine_cycle()
        time.sleep(2.5)

def ensure_worker_threads_running():
    global _worker_pid, _engine_thread
    curr_pid = os.getpid()
    if _worker_pid != curr_pid or _engine_thread is None or not _engine_thread.is_alive():
        _worker_pid = curr_pid
        _engine_thread = threading.Thread(target=market_engine_loop, daemon=True)
        _engine_thread.start()
        logger.info(f"Started engine thread in PID {curr_pid}")

@app.before_request
def before_any_request():
    ensure_worker_threads_running()

# --------------------------------------------------------------
# Flask routes
# --------------------------------------------------------------

@app.route("/ping")
@app.route("/health")
@app.route("/heartbeat")
def route_health():
    ensure_worker_threads_running()
    with state_lock:
        armed = bool(app_state.get("exchange_bridge", {}).get("live_armed", False))
        count = len(app_state.get("coins", {}))
        last_tick = app_state.get("last_price_tick_hms", "--:--:--")
    return jsonify({
        "ok": True,
        "status": "ALIVE",
        "version": "v6.0-safe",
        "live_bridge_armed": armed,
        "active_coins": count,
        "last_price_tick_utc": last_tick,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }), 200

@app.route("/api/state")
def route_state():
    ensure_worker_threads_running()
    with state_lock:
        bridge = dict(app_state.get("exchange_bridge", {}))
        safe_bridge = {
            "exchange": bridge.get("exchange", "gateio"),
            "masked_key": (
                bridge["api_key"][:4] + "..." + bridge["api_key"][-4:]
                if bridge.get("api_key") and len(bridge["api_key"]) > 8
                else ("Saved" if bridge.get("api_key") else "")
            ),
            "has_secret": bool(bridge.get("api_secret")),
            "max_order_usd": float(bridge.get("max_order_usd") or 250.0),
            "live_armed": bool(bridge.get("live_armed", False)),
            "verified": bool(bridge.get("verified", False)),
            "available_usdt": float(bridge.get("available_usdt") or 0.0),
            "realized_usdt_profit": float(bridge.get("realized_usdt_profit") or 0.0),
            "baseline_usdt": float(bridge.get("baseline_usdt") or 0.0),
            "last_sync_ts": bridge.get("last_sync_ts", 0)
        }

        alerts = app_state.get("alerts", {})
        safe_alerts = {
            "enabled": bool(alerts.get("enabled", False)),
            "ntfy_topic": alerts.get("ntfy_topic", ""),
            "telegram_chat_id": alerts.get("telegram_chat_id", ""),
            "has_tg_token": bool(alerts.get("telegram_bot_token")),
            "alert_on_skim": bool(alerts.get("alert_on_skim", True)),
            "alert_on_shield": bool(alerts.get("alert_on_shield", True)),
            "alert_on_buy": bool(alerts.get("alert_on_buy", True))
        }

        payload = {
            "version": app_state.get("version", "v6.0-safe"),
            "vault_cash": float(app_state.get("vault_cash") or 0.0),
            "added_to_anchors": float(app_state.get("added_to_anchors") or 0.0),
            "total_escrow_reserved": float(app_state.get("total_escrow_reserved") or 0.0),
            "total_profit": round(float(app_state.get("vault_cash") or 0.0) + float(app_state.get("added_to_anchors") or 0.0), 4),
            "total_skims": int(app_state.get("total_skims") or 0),
            "frozen": bool(app_state.get("frozen", False)),
            "last_price_tick_hms": app_state.get("last_price_tick_hms", now_hms()),
            "profit_curve": app_state.get("profit_curve", []),
            "skim_ledger": app_state.get("skim_ledger", []),
            "logs": app_state.get("logs", []),
            "exchange_bridge": safe_bridge,
            "alerts": safe_alerts,
            "coins": app_state.get("coins", {})
        }

    resp = make_response(jsonify(payload))
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    return resp

@app.route("/api/bridge/save", methods=["POST"])
def route_bridge_save():
    data = request.json or {}

    with state_lock:
        bridge = app_state["exchange_bridge"]
        exchange = (data.get("exchange") or bridge.get("exchange", "gateio")).strip().lower()
        bridge["exchange"] = exchange

        if data.get("api_key"):
            bridge["api_key"] = str(data["api_key"]).strip()
        if data.get("api_secret"):
            bridge["api_secret"] = str(data["api_secret"]).strip()
        if data.get("max_order_usd") is not None:
            try:
                bridge["max_order_usd"] = float(data["max_order_usd"])
            except Exception:
                pass

        if data.get("toggle_live"):
            bridge["live_armed"] = not bool(bridge.get("live_armed", False))
        elif "live_armed" in data:
            bridge["live_armed"] = bool(data["live_armed"])

    if not ALLOW_LIVE_TRADING and bridge["live_armed"]:
        bridge["live_armed"] = False

    try:
        balances, err = fetch_live_exchange_balances()
        if balances is None:
            return jsonify({"ok": False, "error": err}), 400
        with state_lock:
            app_state["exchange_bridge"]["available_usdt"] = round(balances.get("USDT", {}).get("available", 0.0), 4)
            app_state["exchange_bridge"]["verified"] = True
            app_state["exchange_bridge"]["last_sync_ts"] = int(time.time())
        return jsonify({
            "ok": True,
            "available_usdt": round(balances.get("USDT", {}).get("available", 0.0), 4),
            "live_armed": bool(app_state["exchange_bridge"].get("live_armed", False))
        })
    except Exception as exc:
        logger.warning(f"Bridge validation failed: {exc}")
        return jsonify({"ok": False, "error": str(exc)}), 400

@app.route("/api/coin/launch", methods=["POST"])
def route_coin_launch():
    data = request.json or {}
    raw = (data.get("symbol") or "").strip()
    try:
        anchor_usd = float(data.get("anchor_usd") or 10.0)
    except Exception:
        anchor_usd = 10.0

    if not raw:
        return jsonify({"ok": False, "error": "Enter a symbol or contract address."}), 400

    is_contract = len(raw) > 15
    sym = raw.upper().replace("/USDT", "").replace("_USDT", "") if not is_contract else raw[:8].upper()
    contract = raw if is_contract else ""

    quote = fetch_live_market_quote(sym, contract)
    if not quote or quote.get("price", 0) <= 0:
        return jsonify({"ok": False, "error": f"Could not verify market feed for {raw}."}), 400

    if quote.get("is_dex") and float(quote.get("liquidity_usd") or 0.0) < 15000.0:
        msg = f"Anti-rug firewall blocked {sym}: liquidity is below $15,000."
        add_log(f"🛡️ {msg}")
        return jsonify({"ok": False, "error": msg}), 400

    if quote.get("symbol"):
        sym = str(quote["symbol"]).upper()

    price = float(quote["price"])
    initial_shares = round(anchor_usd / price, 6) if price > 0 else 0.0

    with state_lock:
        live_armed = bool(app_state["exchange_bridge"].get("live_armed", False))
        app_state["coins"][sym] = {
            "symbol": sym,
            "contract": contract,
            "source": quote.get("source", "GATE.IO SPOT"),
            "strategy": "50/50 Hybrid Moonbag",
            "live_enabled": live_armed,
            "status": "WAITING_LIVE_BUY" if live_armed else "HOLDING_ANCHOR",
            "current_price": price,
            "entry_price": price,
            "sliding_low": price,
            "highest_price": price,
            "zero_loss_stop": 0.0,
            "gain_from_low_pct": 0.0,
            "anchor_usd": anchor_usd,
            "shares": 0.0 if live_armed else initial_shares,
            "protected_shares": 0.0 if live_armed else initial_shares,
            "escrow_usdt_reserved": 0.0,
            "escrow_sold_shares": 0.0,
            "escrow_sell_price": 0.0,
            "escrow_rebuy_target_price": 0.0,
            "bonus_shares_added": 0.0,
            "share_topup_usd": 0.0,
            "vaulted_usd": 0.0,
            "skim_count": 0,
            "ratchet_armed": False
        }
        add_log(f"🚀 Launched {sym}/USDT with ${anchor_usd:.2f} anchor.")
    save_state()
    return jsonify({"ok": True, "symbol": sym})

@app.route("/api/coin/switch_mode", methods=["POST"])
def route_coin_switch_mode():
    data = request.json or {}
    sym = (data.get("symbol") or "").upper().strip()
    with state_lock:
        card = app_state["coins"].get(sym)
        if not card:
            return jsonify({"ok": False, "error": "Coin not found."}), 404
        card["live_enabled"] = not bool(card.get("live_enabled", False))
        is_now_live = card["live_enabled"]
        if is_now_live:
            card["shares"] = 0.0
            card["status"] = "WAITING_LIVE_BUY"
        else:
            price = float(card.get("current_price") or 1.0)
            card["shares"] = round(float(card.get("anchor_usd") or 10.0) / price, 6)
            card["protected_shares"] = card["shares"]
            card["status"] = "HOLDING_ANCHOR"
    save_state()
    return jsonify({"ok": True, "live_enabled": is_now_live})

@app.route("/api/coin/cycle_strategy", methods=["POST"])
def route_cycle_strategy():
    data = request.json or {}
    sym = (data.get("symbol") or "").upper().strip()
    strategies = ["50/50 Hybrid Moonbag", "100% Share Compounder", "100% Pure Cash Skim"]
    with state_lock:
        card = app_state["coins"].get(sym)
        if not card:
            return jsonify({"ok": False, "error": "Coin not found."}), 404
        curr = card.get("strategy") or strategies[0]
        idx = (strategies.index(curr) + 1) % len(strategies) if curr in strategies else 0
        card["strategy"] = strategies[idx]
    save_state()
    return jsonify({"ok": True, "strategy": card["strategy"]})

@app.route("/api/coin/force_buy", methods=["POST"])
def route_force_buy():
    data = request.json or {}
    sym = (data.get("symbol") or "").upper().strip()
    usd_amount = data.get("usd_amount")
    try:
        buy_usd = float(usd_amount) if usd_amount is not None else None
    except Exception:
        buy_usd = None

    with state_lock:
        card = app_state["coins"].get(sym)
        if not card:
            return jsonify({"ok": False, "error": "Coin not found."}), 404
        card["live_enabled"] = True
        app_state["exchange_bridge"]["live_armed"] = True

    ok, msg, _ = execute_live_exchange_order(sym, "buy", usd_amount=buy_usd or 25.0)
    if ok:
        time.sleep(1.0)
        with state_lock:
            c = app_state["coins"].get(sym)
            if c:
                c["status"] = "HOLDING_ANCHOR"
                c["shares"] = max(float(c.get("shares") or 0.0), float(c.get("shares") or 0.0))
        save_state()
        return jsonify({"ok": True})
    return jsonify({"ok": False, "error": msg}), 400

@app.route("/api/coin/edit_anchor", methods=["POST"])
def route_edit_anchor():
    data = request.json or {}
    sym = (data.get("symbol") or "").upper().strip()
    try:
        new_anchor = float(data.get("anchor_usd") or 10.0)
    except Exception:
        return jsonify({"ok": False, "error": "Invalid anchor value."}), 400

    with state_lock:
        card = app_state["coins"].get(sym)
        if not card:
            return jsonify({"ok": False, "error": "Coin not found."}), 404
        price = float(card.get("current_price") or 1.0)
        if not bool(card.get("live_enabled")):
            card["anchor_usd"] = new_anchor
            card["shares"] = round(new_anchor / price, 6)
            card["protected_shares"] = card["shares"]
            save_state()
            return jsonify({"ok": True})
    save_state()
    return jsonify({"ok": True})

@app.route("/api/control/freeze", methods=["POST"])
def route_freeze():
    with state_lock:
        app_state["frozen"] = not bool(app_state.get("frozen", False))
        add_log(f"🛑 Execution engine set to {'FROZEN' if app_state['frozen'] else 'ACTIVE'}")
    save_state()
    return jsonify({"ok": True, "frozen": app_state["frozen"]})

@app.route("/api/control/reset", methods=["POST"])
def route_reset():
    with state_lock:
        app_state["vault_cash"] = 0.0
        app_state["added_to_anchors"] = 0.0
        app_state["total_escrow_reserved"] = 0.0
        app_state["total_skims"] = 0
        app_state["profit_curve"] = [{"time": now_hms(), "value": 0.0}]
        app_state["skim_ledger"] = []
        app_state["exchange_bridge"]["realized_usdt_profit"] = 0.0
        for sym, c in app_state.get("coins", {}).items():
            c["share_topup_usd"] = 0.0
            c["vaulted_usd"] = 0.0
            c["escrow_usdt_reserved"] = 0.0
            c["escrow_sold_shares"] = 0.0
            c["escrow_sell_price"] = 0.0
            c["escrow_rebuy_target_price"] = 0.0
            c["skim_count"] = 0
            c["ratchet_armed"] = False
            c["zero_loss_stop"] = 0.0
            sh = float(c.get("shares") or 0.0)
            px = float(c.get("current_price") or 0.0)
            if sh > 0 and px > 0:
                c["entry_price"] = px
                c["sliding_low"] = px
                c["highest_price"] = px
                c["status"] = "HOLDING_ANCHOR"
    save_state()
    return jsonify({"ok": True})

@app.route("/api/export_csv")
def route_export_csv():
    rows = ["Timestamp_UTC,Symbol,Mode,Stage,Price_USD,Total_Skim_USD,Real_USDT_Profit,Bonus_Shares_USD,Shares_Held"]
    with state_lock:
        for item in app_state.get("skim_ledger", []):
            rows.append(
                f"{item.get('time','')},{item.get('symbol','')},{item.get('mode','')},"
                f"\"{item.get('stage','')}\",{item.get('price','')},{item.get('profit',0)},"
                f"{item.get('vault_cut',0)},{item.get('topup_cut',0)},{item.get('shares',0)}"
            )
    csv_text = "\n".join(rows)
    return Response(csv_text, mimetype="text/csv", headers={"Content-Disposition": "attachment;filename=vaultskim_safe_ledger.csv"})

@app.route("/api/radar/scan")
def route_radar_scan():
    candidates = ["QNT", "LTC", "SOL", "SUI", "RENDER", "PEPE", "BONK"]
    results = []
    for sym in candidates:
        q = fetch_live_market_quote(sym)
        if q and q.get("price", 0) > 0:
            results.append({
                "symbol": sym,
                "price": q["price"],
                "change_24h": round(float(q.get("change_24h") or 0.0), 2),
                "liquidity_usd": round(float(q.get("liquidity_usd") or 100000.0), 0),
                "source": q.get("source", "GATE.IO SPOT")
            })
    return jsonify({"ok": True, "results": results})

@app.route("/api/alerts/save", methods=["POST"])
def route_alerts_save():
    data = request.json or {}
    with state_lock:
        a = app_state["alerts"]
        a["enabled"] = bool(data.get("enabled", True))
        if "ntfy_topic" in data:
            a["ntfy_topic"] = (data.get("ntfy_topic") or "").strip()
        if "telegram_chat_id" in data:
            a["telegram_chat_id"] = (data.get("telegram_chat_id") or "").strip()
        if data.get("telegram_bot_token"):
            a["telegram_bot_token"] = str(data["telegram_bot_token"]).strip()
    save_state()
    if data.get("send_test"):
        dispatch_phone_alert("VaultSkim Safe Test", "Your alert bridge is active.", category="skim")
    return jsonify({"ok": True})

@app.route("/api/cashout/record", methods=["POST"])
def route_cashout_record():
    data = request.json or {}
    try:
        amount = float(data.get("amount") or 0.0)
    except Exception:
        amount = 0.0
    dest = (data.get("destination") or "Wallet").strip()

    with state_lock:
        available = float(app_state.get("vault_cash") or 0.0)
        if amount <= 0 or amount > available:
            return jsonify({"ok": False, "error": f"Enter an amount up to ${available:.4f}."}), 400
        app_state["vault_cash"] = round(available - amount, 4)
        app_state["total_withdrawn"] = round(float(app_state.get("total_withdrawn") or 0.0) + amount, 4)
        app_state["cashout_history"].insert(0, {"time": now_hms(), "amount": round(amount, 4), "destination": dest})
    save_state()
    return jsonify({"ok": True})

# ==========================================================
# Minimal dashboard
# ==========================================================
DASHBOARD_HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>VaultSkim Safe</title>
  <style>
    body { font-family: Arial; background: #07111d; color: #dfeaf6; padding: 18px; }
    .card { background: #0d1b2b; border: 1px solid #20314a; border-radius: 12px; padding: 14px; margin-bottom: 14px; }
    .grid { display: grid; gap: 12px; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); }
    input, select, button { width: 100%; box-sizing: border-box; padding: 10px; border-radius: 8px; border: 1px solid #2d415e; background: #091723; color: white; margin-top: 6px; }
    button { cursor: pointer; background: #1b8b5a; border: none; font-weight: bold; }
    .muted { color: #9cb2c7; font-size: 12px; }
    .big { font-size: 32px; font-weight: bold; }
    .small { font-size: 12px; }
    .row { display: flex; gap: 8px; }
    .pill { display: inline-block; padding: 4px 10px; border-radius: 999px; background: #143a2d; color: #7ef0bc; font-size: 12px; }
  </style>
</head>
<body>
  <div class="card">
    <div class="row" style="justify-content: space-between; align-items: center;">
      <div>
        <div class="muted">VaultSkim Safe</div>
        <div class="big" id="totalProfit">$0.0000</div>
      </div>
      <div class="pill" id="bridgeStatus">PAPER MODE</div>
    </div>
    <div class="small" id="subProfitText">Real USDT: $0.0000 | Bonus Shares: $0.0000 | Escrow: $0.00</div>
  </div>

  <div class="card">
    <h3>Bridge</h3>
    <div class="grid">
      <div>
        <label>Exchange</label>
        <select id="bridgeExchange">
          <option value="gateio">Gate.io</option>
          <option value="binance">Binance</option>
        </select>
      </div>
      <div>
        <label>API Key</label>
        <input id="bridgeApiKey" placeholder="Paste API Key">
      </div>
      <div>
        <label>API Secret</label>
        <input id="bridgeApiSecret" type="password" placeholder="Paste API Secret">
      </div>
      <div>
        <label>Max Order USD</label>
        <input id="bridgeMaxCap" type="number" value="250">
      </div>
    </div>
    <div class="row" style="margin-top:10px;">
      <button id="verifyBridgeBtn" onclick="saveBridgeConfig()">Verify Bridge</button>
      <button onclick="toggleLive()">Toggle Live</button>
    </div>
    <div class="small" id="bridgeResult">Bridge is inactive.</div>
  </div>

  <div class="card">
    <h3>Launch Coin</h3>
    <div class="grid">
      <div>
        <label>Symbol / Contract</label>
        <input id="coinSymbol" placeholder="QNT or 0x...">
      </div>
      <div>
        <label>Anchor USD</label>
        <input id="coinAnchor" type="number" value="10">
      </div>
    </div>
    <div class="row" style="margin-top:10px;">
      <button onclick="launchCoin()">Launch Coin</button>
      <button onclick="scanRadar()">Scan Radar</button>
    </div>
    <div id="radarResults" class="small" style="margin-top:10px;"></div>
  </div>

  <div class="card">
    <h3>Coins</h3>
    <div id="coinsList"></div>
  </div>

  <div class="card">
    <h3>Logs</h3>
    <div id="logBox" class="small" style="white-space: pre-wrap;"></div>
  </div>
</body>
<script>
async function pollState() {
  const r = await fetch('/api/state?_t=' + Date.now(), { cache: 'no-store' });
  const data = await r.json();
  render(data);
}

function render(data) {
  const total = Number(data.total_profit || 0);
  document.getElementById('totalProfit').innerText = '$' + total.toFixed(4);

  const vault = Number(data.vault_cash || 0);
  const added = Number(data.added_to_anchors || 0);
  const esc = Number(data.total_escrow_reserved || 0);
  document.getElementById('subProfitText').innerText = `Real USDT: $${vault.toFixed(4)} | Bonus Shares: $${added.toFixed(4)} | Escrow: $${esc.toFixed(2)}`;

  const bridge = data.exchange_bridge || {};
  const live = !!bridge.live_armed;
  const status = live ? 'LIVE MODE' : 'PAPER MODE';
  document.getElementById('bridgeStatus').innerText = status;
  document.getElementById('bridgeResult').innerText = `Available USDT: $${Number(bridge.available_usdt || 0).toFixed(4)} • Verified: ${bridge.verified ? 'yes' : 'no'}`;

  const coins = Object.values(data.coins || {});
  const list = document.getElementById('coinsList');
  list.innerHTML = coins.length ? coins.map(c => {
    const sh = Number(c.shares || 0);
    const price = Number(c.current_price || 0);
    const statusText = c.status || 'HOLDING_ANCHOR';
    return `
      <div class="card" style="margin:0 0 8px 0;">
        <div style="display:flex; justify-content:space-between; align-items:center;">
          <strong>${c.symbol}</strong>
          <span class="pill">${statusText}</span>
        </div>
        <div class="small">Price: $${price.toFixed(4)} • Shares: ${sh.toFixed(6)} • Anchor: $${Number(c.anchor_usd || 0).toFixed(2)}</div>
      </div>
    `;
  }).join('') : '<div class="small">No coins launched yet.</div>';

  const logs = (data.logs || []).slice(0, 30).join('\\n');
  document.getElementById('logBox').innerText = logs || 'No log entries yet.';
}

async function saveBridgeConfig() {
  const payload = {
    exchange: document.getElementById('bridgeExchange').value,
    api_key: document.getElementById('bridgeApiKey').value,
    api_secret: document.getElementById('bridgeApiSecret').value,
    max_order_usd: document.getElementById('bridgeMaxCap').value
  };
  const r = await fetch('/api/bridge/save', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(payload)
  });
  const res = await r.json();
  if (r.ok && res.ok) {
    document.getElementById('bridgeResult').innerText = `Bridge validated. Available USDT: $${Number(res.available_usdt || 0).toFixed(4)}`;
  } else {
    document.getElementById('bridgeResult').innerText = res.error || 'Bridge failed.';
  }
  pollState();
}

async function toggleLive() {
  const r = await fetch('/api/bridge/save', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({ toggle_live: true })
  });
  const res = await r.json();
  if (r.ok) {
    document.getElementById('bridgeResult').innerText = 'Live toggle updated.';
  }
  pollState();
}

async function launchCoin() {
  const payload = {
    symbol: document.getElementById('coinSymbol').value,
    anchor_usd: document.getElementById('coinAnchor').value
  };
  const r = await fetch('/api/coin/launch', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(payload)
  });
  const res = await r.json();
  if (r.ok) {
    document.getElementById('coinSymbol').value = '';
  } else {
    alert(res.error || 'Launch failed');
  }
  pollState();
}

async function scanRadar() {
  const r = await fetch('/api/radar/scan');
  const res = await r.json();
  document.getElementById('radarResults').innerHTML = (res.results || []).map(item => `
    <div style="margin-top:4px;">
      ${item.symbol}: $${Number(item.price || 0).toFixed(4)} • ${Number(item.change_24h || 0).toFixed(2)}% • ${Number(item.liquidity_usd || 0).toFixed(0)} USD
    </div>
  `).join('');
}

pollState();
setInterval(pollState, 2500);
</script>
"""

@app.route("/")
def route_index():
    ensure_worker_threads_running()
    resp = make_response(render_template_string(DASHBOARD_HTML))
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    return resp

if __name__ == "__main__":
    ensure_worker_threads_running()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, threaded=True)
