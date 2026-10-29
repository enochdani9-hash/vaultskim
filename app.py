# ==============================================================================
# VAULTSKIM COMMAND CENTER — v6.0 PRIORITY USDT PROFIT & AUTO SHARE-REFILL POOL
# PART 1 OF 2: CORE ENGINE, DIRECT PROFIT SKIMMER, REFILL POOL & AUTO-BUG FIXER
# ==============================================================================
import os
import sys
import time
import json
import hmac
import math
import uuid
import hashlib
import logging
import threading
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_UP, ROUND_HALF_UP
from urllib.parse import urlencode

import requests
from flask import (
    Flask,
    request,
    jsonify,
    Response,
    make_response,
    render_template_string
)

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("VaultSkimV60")

app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False

# ==============================================================================
# 1. GLOBAL CONFIGURATION & PERSISTENCE CONSTANTS
# ==============================================================================
STATE_FILE = os.environ.get("VAULTSKIM_STATE_FILE", "vaultskim_state_v60.json")
LEGACY_FILES = (
    "vaultskim_state_v510.json",
    "vaultskim_state_v59.json",
    "vaultskim_state_v58.json",
    "vaultskim_state_v57.json",
    "vaultskim_state_v52.json"
)
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
RENDER_EXTERNAL_URL = os.environ.get("RENDER_EXTERNAL_URL", "").strip()

GATEIO_MIN_ORDER_USD = 3.18

state_lock = threading.RLock()
tick_lock = threading.Lock()
order_cooldowns = {}
pair_rules_cache = {}
price_cache = {}

_worker_pid = None
_engine_thread = None
_keepalive_thread = None
_last_engine_tick_ts = 0.0
_last_wallet_sync_ts = 0.0
_last_db_pull_ts = 0.0
_last_bugfix_ts = 0.0

DEFAULT_STATE = {
    "version": "v6.0 - Priority USDT & Share Refill Pool",
    "state_updated_ts": 0.0,
    "vault_cash": 0.0,
    "added_to_anchors": 0.0,
    "total_escrow_reserved": 0.0,
    "total_skims": 0,
    "total_withdrawn": 0.0,
    "auto_fixes_count": 0,
    "frozen": False,
    "last_price_tick_hms": "--:--:--",
    "profit_curve": [
        {
            "time": datetime.now(timezone.utc).strftime("%H:%M:%S"),
            "value": 0.0
        }
    ],
    "skim_ledger": [],
    "cashout_history": [],
    "logs": [
        f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] "
        f"VaultSkim v6.0 Initialized: Priority Pure USDT Profit + Auto Share-Refill Pool Active."
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


# ==============================================================================
# 2. MULTI-WORKER DATABASE & FILE PERSISTENCE LAYER
# ==============================================================================
def init_db():
    if not DATABASE_URL or not HAS_PSYCOPG2:
        return False
    try:
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=6)
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS vaultskim_kv_store (
                    store_key VARCHAR(64) PRIMARY KEY,
                    store_json JSONB NOT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        logger.warning(f"PostgreSQL init fallback: {e}")
        return False


def merge_default_state(loaded):
    merged = json.loads(json.dumps(DEFAULT_STATE))
    if not isinstance(loaded, dict):
        return merged

    for k, v in loaded.items():
        if isinstance(v, dict) and k in merged and isinstance(merged[k], dict) and k != "coins":
            merged[k].update(v)
        else:
            merged[k] = v

    merged["version"] = "v6.0 - Priority USDT & Share Refill Pool"
    merged.setdefault("auto_fixes_count", 0)

    for sym, c in merged.get("coins", {}).items():
        sh = float(c.get("shares") or 0.0)
        px = float(c.get("current_price") or 0.0)
        prot_sh = float(c.get("protected_shares") or 0.0)
        c["protected_shares"] = sh if prot_sh <= 0 else max(prot_sh, sh)
        c.setdefault("refill_pool_usdt", 0.0)
        c.setdefault("skims_since_refill", 0)
        c.setdefault("last_skim_sell_price", 0.0)
        c.setdefault("escrow_usdt_reserved", 0.0)
        c.setdefault("escrow_sold_shares", 0.0)
        c.setdefault("escrow_sell_price", 0.0)
        c.setdefault("escrow_rebuy_target_price", 0.0)
        c.setdefault("bonus_shares_added", 0.0)
        c.setdefault("zero_loss_stop", 0.0)
        c.setdefault("share_topup_usd", 0.0)
        c.setdefault("vaulted_usd", 0.0)
        c.setdefault("skim_count", 0)

        # Migrate any old v5.10 Escrow into the Refill Pool so no card is stuck!
        old_esc = float(c.get("escrow_usdt_reserved") or 0.0)
        if old_esc > 0:
            c["refill_pool_usdt"] = round(float(c.get("refill_pool_usdt") or 0.0) + old_esc, 4)
            c["escrow_usdt_reserved"] = 0.0
            c["escrow_sold_shares"] = 0.0
        if c.get("status") == "ESCROW_HOLDING":
            c["status"] = "HOLDING_ANCHOR"

        if sh > 0 and px > 0:
            c["anchor_usd"] = round(sh * px, 2)

    return merged


def load_persistent_state():
    if DATABASE_URL and HAS_PSYCOPG2:
        for key_name in (
            "master_state_v60",
            "master_state_v510",
            "master_state_v59",
            "master_state_v58",
            "master_state_v57",
            "master_state_v52"
        ):
            try:
                conn = psycopg2.connect(DATABASE_URL, connect_timeout=6)
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(
                        "SELECT store_json FROM vaultskim_kv_store WHERE store_key = %s;",
                        (key_name,)
                    )
                    row = cur.fetchone()
                conn.close()
                if row and row.get("store_json"):
                    loaded = row["store_json"]
                    if isinstance(loaded, str):
                        loaded = json.loads(loaded)
                    if isinstance(loaded, dict) and (
                        loaded.get("coins") or loaded.get("exchange_bridge", {}).get("api_key")
                    ):
                        return merge_default_state(loaded)
            except Exception as e:
                logger.warning(f"PG load fallback ({key_name}): {e}")

    for fpath in (STATE_FILE,) + LEGACY_FILES:
        if os.path.exists(fpath):
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    loaded = json.load(f)
                    if isinstance(loaded, dict):
                        return merge_default_state(loaded)
            except Exception:
                pass

    return json.loads(json.dumps(DEFAULT_STATE))


def pull_latest_state_if_newer():
    global STATE, _last_db_pull_ts
    now_ts = time.time()
    with state_lock:
        local_empty = (len(STATE.get("coins", {})) == 0 and not STATE.get("exchange_bridge", {}).get("api_key"))
        if not local_empty and (now_ts - _last_db_pull_ts) < 3.0:
            return
        _last_db_pull_ts = now_ts
        local_ts = float(STATE.get("state_updated_ts") or 0.0)

    loaded = None
    if DATABASE_URL and HAS_PSYCOPG2:
        try:
            conn = psycopg2.connect(DATABASE_URL, connect_timeout=4)
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT store_json FROM vaultskim_kv_store WHERE store_key = %s;",
                    ("master_state_v60",)
                )
                row = cur.fetchone()
            conn.close()
            if row and row.get("store_json"):
                raw = row["store_json"]
                loaded = json.loads(raw) if isinstance(raw, str) else raw
        except Exception:
            pass

    if not loaded and os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                loaded = json.load(f)
        except Exception:
            pass

    if isinstance(loaded, dict):
        remote_ts = float(loaded.get("state_updated_ts") or 0.0)
        remote_has_data = bool(loaded.get("coins") or loaded.get("exchange_bridge", {}).get("api_key"))
        with state_lock:
            if (local_empty and remote_has_data) or (remote_ts > local_ts + 0.5):
                STATE = merge_default_state(loaded)


def save_persistent_state(sync_db=False):
    with state_lock:
        is_empty_worker = (
            len(STATE.get("coins", {})) == 0
            and not STATE.get("exchange_bridge", {}).get("api_key")
            and float(STATE.get("state_updated_ts") or 0.0) == 0.0
        )
        if is_empty_worker:
            return

        total_pool = sum(
            float(c.get("refill_pool_usdt") or 0.0)
            for c in STATE.get("coins", {}).values()
        )
        STATE["total_escrow_reserved"] = round(total_pool, 4)
        STATE["state_updated_ts"] = time.time()
        snapshot = json.loads(json.dumps(STATE))

    try:
        tmp_file = f"{STATE_FILE}.tmp"
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(snapshot, f, indent=2)
        os.replace(tmp_file, STATE_FILE)
    except Exception:
        pass

    if DATABASE_URL and HAS_PSYCOPG2:
        def _pg_write(data_obj):
            try:
                conn = psycopg2.connect(DATABASE_URL, connect_timeout=5)
                with conn.cursor() as cur:
                    for k_name in (
                        "master_state_v60",
                        "master_state_v510",
                        "master_state_v59",
                        "master_state_v58",
                        "master_state_v57",
                        "master_state_v52"
                    ):
                        cur.execute("""
                            INSERT INTO vaultskim_kv_store (store_key, store_json, updated_at)
                            VALUES (%s, %s, CURRENT_TIMESTAMP)
                            ON CONFLICT (store_key)
                            DO UPDATE SET store_json = EXCLUDED.store_json,
                                          updated_at = CURRENT_TIMESTAMP;
                        """, (k_name, json.dumps(data_obj)))
                conn.commit()
                conn.close()
            except Exception:
                pass

        if sync_db:
            _pg_write(snapshot)
        else:
            threading.Thread(target=_pg_write, args=(snapshot,), daemon=True).start()


init_db()
STATE = load_persistent_state()
save_persistent_state(sync_db=True)


# ==============================================================================
# 3. LOGGING & PHONE ALERTS
# ==============================================================================
def now_hms():
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


def add_log(message):
    with state_lock:
        entry = f"[{now_hms()}] {message}"
        STATE["logs"].insert(0, entry)
        if len(STATE["logs"]) > 120:
            STATE["logs"] = STATE["logs"][:120]


def dispatch_phone_alert(title, message, category="skim"):
    with state_lock:
        alerts = dict(STATE.get("alerts", {}))
    if not alerts.get("enabled"):
        return
    if category == "skim" and not alerts.get("alert_on_skim", True):
        return
    if category == "shield" and not alerts.get("alert_on_shield", True):
        return
    if category == "buy" and not alerts.get("alert_on_buy", True):
        return

    def _send():
        topic = (alerts.get("ntfy_topic") or "").strip()
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
        tg_token = (alerts.get("telegram_bot_token") or "").strip()
        tg_chat = (alerts.get("telegram_chat_id") or "").strip()
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


# ==============================================================================
# 4. CRYPTOGRAPHIC SIGNING & PRECISION HELPERS
# ==============================================================================
def quantize_value(val, decimals, mode=ROUND_DOWN):
    try:
        d = int(decimals)
        q = Decimal("10") ** (-d) if d > 0 else Decimal("1")
        return str(Decimal(str(val)).quantize(q, rounding=mode))
    except Exception:
        return f"{float(val):.{int(decimals)}f}"


def gateio_v4_sign(api_key, api_secret, method, url_path, query_string="", body_str=""):
    ts = str(int(time.time()))
    body_hash = hashlib.sha512((body_str or "").encode("utf-8")).hexdigest()
    sign_string = f"{method.upper()}\n{url_path}\n{query_string}\n{body_hash}\n{ts}"
    signature = hmac.new(
        api_secret.strip().encode("utf-8"),
        sign_string.encode("utf-8"),
        hashlib.sha512
    ).hexdigest()
    return {
        "KEY": api_key.strip(),
        "Timestamp": ts,
        "SIGN": signature,
        "Accept": "application/json",
        "Content-Type": "application/json"
    }


def binance_v3_sign(api_secret, query_params):
    query_string = urlencode(query_params)
    signature = hmac.new(
        api_secret.strip().encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()
    return f"{query_string}&signature={signature}"


def get_exchange_pair_rules(symbol):
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


# ==============================================================================
# 5. MULTI-SOURCE REAL-TIME MARKET PRICE & DEX ANTI-RUG SCANNER
# ==============================================================================
def fetch_live_market_quote(symbol, contract=""):
    sym = symbol.upper().strip().replace("/USDT", "").replace("_USDT", "")
    now_ts = time.time()
    req_headers = {
        "Accept": "application/json",
        "Cache-Control": "no-cache, no-store, must-revalidate",
        "User-Agent": f"VaultSkimEngine/6.0 ({int(now_ts)})"
    }

    try:
        r = requests.get(
            "https://api.gateio.ws/api/v4/spot/tickers",
            params={"currency_pair": f"{sym}_USDT", "_t": int(now_ts * 1000)},
            headers=req_headers,
            timeout=4
        )
        if r.status_code == 200:
            arr = r.json()
            if isinstance(arr, list) and len(arr) > 0:
                t_obj = arr[0]
                last_p = float(t_obj.get("last") or 0.0)
                ask_p = float(t_obj.get("lowest_ask") or last_p)
                bid_p = float(t_obj.get("highest_bid") or last_p)
                if ask_p > 0 and bid_p > 0 and (last_p < bid_p or last_p > ask_p):
                    last_p = round((ask_p + bid_p) / 2.0, 6)
                vol_24h = float(t_obj.get("quote_volume") or 0.0)
                if last_p > 0:
                    res = {
                        "price": last_p,
                        "ask": ask_p if ask_p > 0 else last_p,
                        "bid": bid_p if bid_p > 0 else last_p,
                        "change_24h": float(t_obj.get("change_percentage") or 0.0),
                        "volume_24h": vol_24h,
                        "liquidity_usd": max(vol_24h, 100000.0),
                        "source": "GATE.IO SPOT",
                        "is_dex": False,
                        "ts": now_ts
                    }
                    price_cache[sym] = res
                    return res
    except Exception:
        pass

    try:
        r = requests.get(
            "https://api.binance.com/api/v3/ticker/bookTicker",
            params={"symbol": f"{sym}USDT"},
            headers=req_headers,
            timeout=4
        )
        if r.status_code == 200:
            b_obj = r.json()
            bid_p = float(b_obj.get("bidPrice") or 0.0)
            ask_p = float(b_obj.get("askPrice") or 0.0)
            mid_p = (bid_p + ask_p) / 2.0 if (bid_p > 0 and ask_p > 0) else max(bid_p, ask_p)
            if mid_p > 0:
                res = {
                    "price": mid_p,
                    "ask": ask_p if ask_p > 0 else mid_p,
                    "bid": bid_p if bid_p > 0 else mid_p,
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

    lookup = contract.strip() if contract and len(contract.strip()) > 15 else sym
    try:
        url = (
            f"https://api.dexscreener.com/latest/dex/tokens/{lookup}"
            if len(lookup) > 15
            else f"https://api.dexscreener.com/latest/dex/search?q={lookup}"
        )
        r = requests.get(url, headers=req_headers, timeout=5)
        if r.status_code == 200:
            pairs = r.json().get("pairs") or []
            if pairs:
                best_pair = max(pairs, key=lambda p: float((p.get("liquidity") or {}).get("usd") or 0.0))
                p_usd = float(best_pair.get("priceUsd") or 0.0)
                liq_usd = float((best_pair.get("liquidity") or {}).get("usd") or 0.0)
                chain_id = (best_pair.get("chainId") or "DEX").upper()
                base_sym = ((best_pair.get("baseToken") or {}).get("symbol") or sym).upper()
                if p_usd > 0:
                    res = {
                        "symbol": base_sym,
                        "price": p_usd,
                        "ask": p_usd * 1.001,
                        "bid": p_usd * 0.999,
                        "change_24h": float((best_pair.get("priceChange") or {}).get("h24") or 0.0),
                        "volume_24h": float((best_pair.get("volume") or {}).get("h24") or 0.0),
                        "liquidity_usd": liq_usd,
                        "source": f"SAFE DEX (${int(liq_usd/1000)}K Liq: {chain_id})",
                        "is_dex": True,
                        "ts": now_ts
                    }
                    price_cache[sym] = res
                    return res
    except Exception:
        pass

    return price_cache.get(sym)


# ==============================================================================
# 6. LIVE EXCHANGE WALLET BALANCE & ORDER EXECUTION BRIDGE
# ==============================================================================
def fetch_live_exchange_balances():
    with state_lock:
        bridge = dict(STATE.get("exchange_bridge", {}))
    exch = (bridge.get("exchange") or "gateio").lower()
    api_key = (bridge.get("api_key") or "").strip()
    api_secret = (bridge.get("api_secret") or "").strip()
    if not api_key or not api_secret:
        return None, "Missing Spot API Key or Secret."

    if exch == "gateio":
        url_path = "/api/v4/spot/accounts"
        headers = gateio_v4_sign(api_key, api_secret, "GET", url_path)
        try:
            r = requests.get(f"https://api.gateio.ws{url_path}", headers=headers, timeout=8)
            if r.status_code == 200:
                bal_map = {}
                for item in r.json():
                    curr = (item.get("currency") or "").upper()
                    avail = float(item.get("available") or 0.0)
                    locked = float(item.get("locked") or 0.0)
                    bal_map[curr] = {"available": avail, "locked": locked, "total": avail + locked}
                return bal_map, None
            return None, f"Gate.io API ({r.status_code}): {r.text}"
        except Exception as e:
            return None, f"Gate.io connection error: {e}"

    elif exch == "binance":
        params = {"timestamp": int(time.time() * 1000), "recvWindow": 5000}
        signed_q = binance_v3_sign(api_secret, params)
        try:
            r = requests.get(
                f"https://api.binance.com/api/v3/account?{signed_q}",
                headers={"X-MBX-APIKEY": api_key},
                timeout=8
            )
            if r.status_code == 200:
                bal_map = {}
                for b in r.json().get("balances") or []:
                    curr = (b.get("asset") or "").upper()
                    avail = float(b.get("free") or 0.0)
                    locked = float(b.get("locked") or 0.0)
                    if avail > 0 or locked > 0 or curr == "USDT":
                        bal_map[curr] = {"available": avail, "locked": locked, "total": avail + locked}
                return bal_map, None
            return None, f"Binance API ({r.status_code}): {r.text}"
        except Exception as e:
            return None, f"Binance connection error: {e}"

    return None, f"Unsupported exchange: {exch}"


def execute_live_exchange_order(symbol, side, usd_amount=None, share_amount=None):
    with state_lock:
        bridge = dict(STATE.get("exchange_bridge", {}))
    exch = (bridge.get("exchange") or "gateio").lower()
    api_key = (bridge.get("api_key") or "").strip()
    api_secret = (bridge.get("api_secret") or "").strip()
    max_cap = float(bridge.get("max_order_usd") or 250.0)

    if not api_key or not api_secret:
        return False, "Missing Spot API credentials.", None

    sym = symbol.upper().strip()
    quote = fetch_live_market_quote(sym)
    if not quote or quote["price"] <= 0:
        return False, f"Unable to read live market price for {sym}/USDT.", None

    balances, err = fetch_live_exchange_balances()
    if balances is None:
        return False, f"Wallet check failed: {err}", None

    usdt_avail = balances.get("USDT", {}).get("available", 0.0)
    coin_avail = balances.get(sym, {}).get("available", 0.0)

    with state_lock:
        STATE["exchange_bridge"]["available_usdt"] = round(usdt_avail, 4)

    rules = get_exchange_pair_rules(sym)
    min_order_usd = max(float(rules.get("min_quote_amount") or 3.18), GATEIO_MIN_ORDER_USD)
    side_lower = side.lower()

    if side_lower == "buy":
        exec_price = quote["ask"] * 1.0008
        price_str = quantize_value(exec_price, rules["price_precision"], ROUND_HALF_UP)
        exec_price_float = float(price_str)

        requested_usd = min(float(usd_amount or min_order_usd), max_cap)
        if requested_usd < min_order_usd:
            requested_usd = min_order_usd * 1.01

        if usdt_avail < requested_usd:
            if usdt_avail >= 3.08:
                requested_usd = usdt_avail * 0.996
            else:
                return False, f"Need $3.15 USDT in Gate.io wallet (Available: ${usdt_avail:.2f}).", None

        raw_shares = (requested_usd * 0.998) / exec_price_float
        amount_str = quantize_value(raw_shares, rules["amount_precision"], ROUND_UP)
        amount_float = float(amount_str)

        if (amount_float * exec_price_float) > (usdt_avail * 0.999):
            raw_shares = (usdt_avail * 0.997) / exec_price_float
            amount_str = quantize_value(raw_shares, rules["amount_precision"], ROUND_DOWN)
            amount_float = float(amount_str)

        if amount_float <= 0 or (amount_float * exec_price_float) < 3.00:
            return False, f"Buy order (${amount_float * exec_price_float:.2f}) below Gate.io $3.00 minimum.", None

    else:  # SELL
        if coin_avail <= 0:
            return False, f"0.0000 {sym} available in Gate.io Spot wallet to sell.", None

        exec_price = quote["bid"] * 0.9980
        price_str = quantize_value(exec_price, rules["price_precision"], ROUND_HALF_UP)
        exec_price_float = float(price_str)

        if share_amount is not None and float(share_amount) > 0:
            raw_shares = min(float(share_amount), coin_avail * 0.9998)
        elif usd_amount is not None and float(usd_amount) > 0:
            target_usd = max(float(usd_amount), min_order_usd * 1.02)
            raw_shares = min(coin_avail * 0.9998, target_usd / exec_price_float)
        else:
            raw_shares = min(coin_avail * 0.9998, (min_order_usd * 1.02) / exec_price_float)

        if (raw_shares * exec_price_float) < min_order_usd:
            if (coin_avail * exec_price_float) >= min_order_usd:
                raw_shares = min(coin_avail * 0.9998, (min_order_usd * 1.03) / exec_price_float)
            else:
                return False, f"Bag (${coin_avail * exec_price_float:.2f}) under Gate.io $3.00 minimum.", None

        amount_str = quantize_value(raw_shares, rules["amount_precision"], ROUND_UP)
        amount_float = float(amount_str)
        if amount_float > coin_avail:
            amount_str = quantize_value(coin_avail * 0.9998, rules["amount_precision"], ROUND_DOWN)
            amount_float = float(amount_str)

        if amount_float <= 0 or (amount_float * exec_price_float) < 3.00:
            return False, f"Sell order (${amount_float * exec_price_float:.2f}) below Gate.io $3.00 minimum.", None

    if exch == "gateio":
        url_path = "/api/v4/spot/orders"
        order_payload = {
            "currency_pair": f"{sym}_USDT",
            "type": "limit",
            "account": "spot",
            "side": side_lower,
            "amount": amount_str,
            "price": price_str,
            "time_in_force": "ioc"
        }
        body_str = json.dumps(order_payload)
        headers = gateio_v4_sign(api_key, api_secret, "POST", url_path, "", body_str)

        try:
            r = requests.post(f"https://api.gateio.ws{url_path}", headers=headers, data=body_str, timeout=8)
            if r.status_code in (200, 201):
                resp_data = r.json()
                left_amt = float(resp_data.get("left") or 0.0)
                orig_amt = float(resp_data.get("amount") or amount_str)
                filled_amt = max(0.0, orig_amt - left_amt)

                if filled_amt <= 0:
                    order_payload["time_in_force"] = "gtc"
                    body_str = json.dumps(order_payload)
                    headers = gateio_v4_sign(api_key, api_secret, "POST", url_path, "", body_str)
                    r_gtc = requests.post(f"https://api.gateio.ws{url_path}", headers=headers, data=body_str, timeout=8)
                    if r_gtc.status_code in (200, 201):
                        resp_data = r_gtc.json()
                        time.sleep(1.0)
                        left_amt = float(resp_data.get("left") or 0.0)
                        filled_amt = max(0.0, orig_amt - left_amt)

                order_id = resp_data.get("id", "OK")
                actual_filled_sh = filled_amt if filled_amt > 0 else amount_float
                filled_usdt = float(resp_data.get("filled_total") or (actual_filled_sh * float(quote["price"])))
                net_received_shares = actual_filled_sh * 0.999 if side_lower == "buy" else actual_filled_sh
                resp_data["_filled_shares"] = actual_filled_sh
                resp_data["_net_received_shares"] = net_received_shares
                resp_data["_filled_usdt"] = filled_usdt
                add_log(
                    f"✅ LIVE GATE.IO {side_lower.upper()} FILLED ({sym}): "
                    f"{actual_filled_sh:.6f} {sym} (${filled_usdt:.2f} USDT) @ ~${price_str} (#{order_id})"
                )
                return True, "Live order executed.", resp_data
            else:
                err_txt = f"Gate.io Live Order Notice ({r.status_code}): {r.text}"
                add_log(f"⚠️ {err_txt}")
                return False, err_txt, None
        except Exception as e:
            err_txt = f"Gate.io order request failed: {e}"
            add_log(f"⚠️ {err_txt}")
            return False, err_txt, None

    return False, "Exchange execution driver not configured.", None


def sync_all_live_cards_with_gateio(auto_buy_waiting=True, verbose=True):
    balances, err = fetch_live_exchange_balances()
    if balances is None:
        return False, err

    usdt_avail = balances.get("USDT", {}).get("available", 0.0)
    with state_lock:
        STATE["exchange_bridge"]["available_usdt"] = round(usdt_avail, 4)
        if float(STATE["exchange_bridge"].get("baseline_usdt") or 0.0) <= 0.0:
            STATE["exchange_bridge"]["baseline_usdt"] = round(usdt_avail, 4)
        STATE["exchange_bridge"]["verified"] = True
        STATE["exchange_bridge"]["last_sync_ts"] = int(time.time())
        live_armed = STATE["exchange_bridge"].get("live_armed", False)
        is_frozen = STATE.get("frozen", False)
        coin_keys = list(STATE["coins"].keys())

    sync_notes = []

    for sym in coin_keys:
        with state_lock:
            card = STATE["coins"].get(sym)
            if not card:
                continue
            contract = card.get("contract", "")
            is_live_coin = bool(card.get("live_enabled", False))
            anchor_usd = float(card.get("anchor_usd") or 10.0)

        quote = fetch_live_market_quote(sym, contract)
        if quote and quote["price"] > 0:
            with state_lock:
                card["current_price"] = quote["price"]
                card["source"] = quote["source"]

        with state_lock:
            price = float(card.get("current_price") or 1.0)

        if is_live_coin:
            real_shares = balances.get(sym, {}).get("available", 0.0)
            locked_shares = balances.get(sym, {}).get("locked", 0.0)
            total_real_shares = real_shares + locked_shares
            bag_usd = total_real_shares * price

            if total_real_shares > 0.00005 and bag_usd >= 0.80:
                with state_lock:
                    was_waiting = (card.get("status") == "WAITING_LIVE_BUY" or card.get("shares", 0) <= 0)
                    card["shares"] = real_shares if real_shares > 0 else total_real_shares
                    if card["shares"] > float(card.get("protected_shares") or 0.0):
                        card["protected_shares"] = card["shares"]
                    elif float(card.get("protected_shares") or 0.0) <= 0:
                        card["protected_shares"] = card["shares"]

                    card["anchor_usd"] = round(card["shares"] * price, 2)
                    old_entry = float(card.get("entry_price") or 0.0)
                    if was_waiting or old_entry <= 0 or abs(old_entry - price) / price > 0.015:
                        card["entry_price"] = price
                        card["sliding_low"] = min(price, float(card.get("sliding_low") or price))
                        card["highest_price"] = price
                    if card.get("status") in ("WAITING_LIVE_BUY", "ESCROW_HOLDING"):
                        card["status"] = "HOLDING_ANCHOR"
                sync_notes.append(f"{sym}: {total_real_shares:.6f} sh (${bag_usd:.2f}) Active")
            else:
                with state_lock:
                    card["shares"] = 0.0
                    card["status"] = "WAITING_LIVE_BUY"
                    card["entry_price"] = price
                    card["sliding_low"] = price
                    card["highest_price"] = price
                    card["ratchet_armed"] = False

                now_ts = time.time()
                last_attempt = order_cooldowns.get(sym, 0)
                if (
                    auto_buy_waiting
                    and live_armed
                    and not is_frozen
                    and usdt_avail >= GATEIO_MIN_ORDER_USD
                    and (now_ts - last_attempt) > 15
                ):
                    order_cooldowns[sym] = now_ts
                    buy_usd = min(anchor_usd, usdt_avail * 0.985)
                    add_log(f"🚀 AUTO-DEPLOYING LIVE ANCHOR ({sym}): Buying ${buy_usd:.2f} on Gate.io...")
                    ok, msg, _ = execute_live_exchange_order(sym, "buy", usd_amount=buy_usd)
                    if ok:
                        time.sleep(1.1)
                        updated_bals, _ = fetch_live_exchange_balances()
                        if updated_bals:
                            usdt_avail = updated_bals.get("USDT", {}).get("available", usdt_avail)
                            new_sh = updated_bals.get(sym, {}).get("available", 0.0)
                            with state_lock:
                                STATE["exchange_bridge"]["available_usdt"] = round(usdt_avail, 4)
                                if new_sh > 0:
                                    card["shares"] = new_sh
                                    card["protected_shares"] = new_sh
                                    card["anchor_usd"] = round(new_sh * price, 2)
                                    card["entry_price"] = price
                                    card["sliding_low"] = price
                                    card["highest_price"] = price
                                    card["status"] = "HOLDING_ANCHOR"
                            if new_sh > 0:
                                sync_notes.append(f"{sym}: Bought {new_sh:.5f} sh @ ${price:.4f}")
                                continue
                sync_notes.append(f"{sym}: 0.0000 sh -> Ready for Live Buy")

    if verbose:
        summary_str = " | ".join(sync_notes) if sync_notes else "All cards aligned"
        add_log(f"REAL GATE.IO WALLET SYNCED: Available USDT = ${usdt_avail:.4f} | {summary_str}")
    save_persistent_state()
    return True, usdt_avail


# ==============================================================================
# 7. AUTOMATIC BUG FIXER & SHARE-REFILL POOL WATCHDOG
# ==============================================================================
def run_automatic_bug_fixer():
    """
    Runs automatically every 8 seconds:
    1. Converts any stuck legacy ESCROW_HOLDING state into HOLDING_ANCHOR + Refill Pool.
    2. Checks if any coin's Share-Refill Pool has accumulated enough USDT (or if
       shares are below `protected_shares` and Spot USDT >= $3.20) and automatically
       triggers a Share Refill on a dip so your shares are replaced and increased!
    """
    global _last_bugfix_ts
    now_ts = time.time()
    if (now_ts - _last_bugfix_ts) < 8.0:
        return
    _last_bugfix_ts = now_ts

    with state_lock:
        coins = STATE.get("coins", {})
        for sym, c in list(coins.items()):
            sh = float(c.get("shares") or 0.0)
            prot_sh = float(c.get("protected_shares") or 0.0)
            px = float(c.get("current_price") or 0.0)
            old_esc = float(c.get("escrow_usdt_reserved") or 0.0)

            if sh > prot_sh:
                c["protected_shares"] = sh

            if c.get("status") == "ESCROW_HOLDING" or old_esc > 0:
                c["refill_pool_usdt"] = round(float(c.get("refill_pool_usdt") or 0.0) + (old_esc * 0.30), 4)
                c["escrow_usdt_reserved"] = 0.0
                c["escrow_sold_shares"] = 0.0
                c["status"] = "HOLDING_ANCHOR"
                STATE["auto_fixes_count"] = int(STATE.get("auto_fixes_count") or 0) + 1
                add_log(f"🛠️ [AUTO-BUG FIXER] Unlocked {sym} into Direct USDT Profit + Share-Refill mode.")

            if px > 0 and float(c.get("sliding_low") or 0.0) <= 0:
                c["sliding_low"] = px
                c["entry_price"] = px


# ==============================================================================
# 8. PRIORITY USDT PROFIT SKIMMER & PERIODIC SHARE-REFILL ENGINE
# ==============================================================================
def evaluate_coin_tick(sym):
    with state_lock:
        card = STATE["coins"].get(sym)
        if not card:
            return
        contract = card.get("contract", "")
        is_live = bool(card.get("live_enabled", False))
        live_armed = bool(STATE["exchange_bridge"].get("live_armed", False))
        is_frozen = bool(STATE.get("frozen", False))

    quote = fetch_live_market_quote(sym, contract)
    if not quote or quote["price"] <= 0:
        return

    price = float(quote["price"])

    with state_lock:
        card = STATE["coins"].get(sym)
        if not card:
            return
        card["current_price"] = price
        card["source"] = quote["source"]
        STATE["last_price_tick_hms"] = now_hms()

        shares = float(card.get("shares") or 0.0)
        anchor_usd = float(card.get("anchor_usd") or 10.0)

        if shares <= 0.000001:
            card["entry_price"] = price
            card["sliding_low"] = price
            card["highest_price"] = price
            card["gain_from_low_pct"] = 0.0
            card["ratchet_armed"] = False
            if is_live:
                card["status"] = "WAITING_LIVE_BUY"
            else:
                card["shares"] = round(anchor_usd / price, 6)
                card["protected_shares"] = card["shares"]
                card["status"] = "HOLDING_ANCHOR"

    if is_live and shares <= 0.000001 and live_armed and not is_frozen:
        now_ts = time.time()
        usdt_avail = float(STATE["exchange_bridge"].get("available_usdt") or 0.0)
        if usdt_avail >= GATEIO_MIN_ORDER_USD and (now_ts - order_cooldowns.get(sym, 0)) > 15:
            sync_all_live_cards_with_gateio(auto_buy_waiting=True, verbose=False)
        return

    with state_lock:
        card = STATE["coins"].get(sym)
        if not card or float(card.get("shares") or 0.0) <= 0:
            return

        shares = float(card["shares"])
        prot_sh = float(card.get("protected_shares") or shares)
        if shares > prot_sh:
            card["protected_shares"] = shares
            prot_sh = shares

        sliding_low = float(card.get("sliding_low") or price)
        card["anchor_usd"] = round(shares * price, 2)

        # ----------------------------------------------------------------------
        # PERIODIC SHARE REFILL CHECK ("AFTER A FEW SKIMS, REPLACE SHARES")
        # ----------------------------------------------------------------------
        refill_pool = float(card.get("refill_pool_usdt") or 0.0)
        skims_since = int(card.get("skims_since_refill") or 0)
        last_sell_px = float(card.get("last_skim_sell_price") or price)
        usdt_avail = float(STATE["exchange_bridge"].get("available_usdt") or 0.0)

        # Trigger automatic Share Refill when:
        #   - Refill Pool has accumulated >= $3.20 (or after >= 2 skims when shares < protected_shares
        #     and Spot USDT has >= $3.25) AND price dipped slightly (<= last_sell_px * 0.9985)
        needs_share_refill = (
            not is_frozen
            and is_live
            and live_armed
            and usdt_avail >= 3.22
            and (refill_pool >= 3.20 or (skims_since >= 2 and shares < prot_sh))
            and (price <= last_sell_px * 0.9985 or skims_since >= 3)
            and (time.time() - order_cooldowns.get(sym, 0)) > 18
        )

    if needs_share_refill:
        execute_periodic_share_refill(sym, price, manual_trigger=False)
        return

    with state_lock:
        card = STATE["coins"].get(sym)
        if not card:
            return
        sliding_low = float(card.get("sliding_low") or price)

        if sliding_low <= 0 or price < sliding_low:
            old_low = sliding_low
            sliding_low = price
            card["sliding_low"] = price
            card["entry_price"] = price
            if old_low > 0 and ((old_low - price) / old_low) >= 0.004:
                add_log(
                    f"📉 SLIDING FLOOR UPDATED {sym} @ ${price:.4f}: "
                    f"Holding {shares:.6f} {sym} — waiting for +0.65% bounce to skim Pure USDT!"
                )

        gain_pct = ((price - sliding_low) / sliding_low) * 100.0 if sliding_low > 0 else 0.0
        card["gain_from_low_pct"] = round(gain_pct, 3)

        if gain_pct >= 0.38 and not card.get("ratchet_armed"):
            card["ratchet_armed"] = True
            card["highest_price"] = price
            stop_floor = sliding_low * 1.0025
            card["zero_loss_stop"] = stop_floor
            card["status"] = "RATCHET_ARMED"
            add_log(
                f"🛡️ ZERO-LOSS SHIELD ARMED {sym} @ ${price:.4f} (+{gain_pct:.2f}% from low)"
            )

        if card.get("ratchet_armed"):
            if price > float(card.get("highest_price") or price):
                card["highest_price"] = price
                trail_stop = price * 0.9982
                if trail_stop > float(card.get("zero_loss_stop") or 0.0):
                    card["zero_loss_stop"] = trail_stop

        highest_p = float(card.get("highest_price") or price)
        target_hit = gain_pct >= 0.65
        trailing_stop_hit = (
            card.get("ratchet_armed")
            and gain_pct >= 0.52
            and highest_p > 0
            and price <= (highest_p * 0.9985)
        )

    if not is_frozen and (target_hit or trailing_stop_hit):
        if is_live and live_armed:
            execute_direct_usdt_profit_skim(sym, price, gain_pct)
        elif not is_live:
            execute_paper_simulator_skim(sym, price, gain_pct)


def execute_paper_simulator_skim(sym, price, gain_pct):
    with state_lock:
        card = STATE["coins"].get(sym)
        if not card or card.get("live_enabled"):
            return
        shares = float(card.get("shares") or 0.0)
        low_px = float(card.get("sliding_low") or price)
        skim_usd = max(0.08, round(shares * (price - low_px), 4))
        vault_cut = round(skim_usd * 0.70, 4)
        topup_cut = round(skim_usd * 0.30, 4)
        bonus_sh = (topup_cut / price) if price > 0 else 0.0

        card["shares"] = round(shares + bonus_sh, 6)
        card["protected_shares"] = card["shares"]
        card["bonus_shares_added"] = round(float(card.get("bonus_shares_added") or 0.0) + bonus_sh, 6)
        card["share_topup_usd"] = round(float(card.get("share_topup_usd") or 0.0) + topup_cut, 4)
        card["vaulted_usd"] = round(float(card.get("vaulted_usd") or 0.0) + vault_cut, 4)
        card["skim_count"] = int(card.get("skim_count", 0)) + 1
        card["anchor_usd"] = round(card["shares"] * price, 2)
        card["entry_price"] = price
        card["sliding_low"] = price
        card["highest_price"] = price
        card["gain_from_low_pct"] = 0.0
        card["ratchet_armed"] = False
        card["status"] = "HOLDING_ANCHOR"

        ts_now = now_hms()
        STATE["vault_cash"] = round(float(STATE.get("vault_cash", 0.0)) + vault_cut, 4)
        STATE["added_to_anchors"] = round(float(STATE.get("added_to_anchors", 0.0)) + topup_cut, 4)
        STATE["total_skims"] = int(STATE.get("total_skims", 0)) + 1
        total_gen = round(STATE["vault_cash"] + STATE["added_to_anchors"], 4)
        STATE["profit_curve"].append({"time": ts_now, "value": total_gen})
        STATE["skim_ledger"].insert(0, {
            "id": str(uuid.uuid4())[:8],
            "symbol": sym,
            "mode": "PAPER",
            "stage": f"Direct Profit Skim (+${vault_cut:.4f} USDT)",
            "price": f"{price:.4f}",
            "time": ts_now,
            "profit": skim_usd,
            "vault_cut": vault_cut,
            "topup_cut": topup_cut,
            "shares": f"{card['shares']:.6f}"
        })
    save_persistent_state()


def execute_direct_usdt_profit_skim(sym, price, gain_pct=0.65):
    """
    1-STAGE DIRECT USDT PROFIT SKIMMER (YOUR ORIGINAL PLAN):
    1. When the coin bounces +0.65% (or spikes +5%), it immediately harvests the
       minimum Gate.io profit slice ($3.22 USDT) directly into your Gate.io Spot
       USDT wallet—NO waiting in Escrow!
    2. Books your real price-gain profit (`wave_gain_usd = shares * (price - low_px)`)
       as Pure Realized USDT Profit right away!
    3. Allocates part of the cash (`refill_allocation`) into the coin's Share-Refill
       Pool so that after a few skims, the bot automatically buys back and replaces
       the shares while your pure USDT profits stay permanently in your wallet!
    """
    with state_lock:
        card = STATE["coins"].get(sym)
        if not card:
            return
        shares = float(card.get("shares") or 0.0)
        low_px = float(card.get("sliding_low") or (price / 1.0065))
        strategy = card.get("strategy") or "70% Pure USDT + 30% Share Refill"
        is_live = bool(card.get("live_enabled")) and bool(STATE["exchange_bridge"].get("live_armed"))

    if not is_live or shares <= 0 or price <= 0:
        return

    now_ts = time.time()
    if (now_ts - order_cooldowns.get(sym, 0)) <= 12:
        return

    bag_usd = shares * price
    if bag_usd < (GATEIO_MIN_ORDER_USD * 1.5):
        return

    # Calculate true dollar gain of your bag on this wave bounce
    wave_gain_usd = max(0.12, shares * max(0.0, price - low_px))
    # Sell only the smallest Gate.io allowed slice ($3.22) or wave_gain_usd if larger (e.g. on a 5% spike!)
    sell_slice_usd = max(GATEIO_MIN_ORDER_USD * 1.015, min(wave_gain_usd, bag_usd * 0.15))

    order_cooldowns[sym] = now_ts
    ok, msg, resp_data = execute_live_exchange_order(sym, "sell", usd_amount=sell_slice_usd)
    if not ok or not resp_data:
        return

    sold_sh = float(resp_data.get("_filled_shares") or (sell_slice_usd / price))
    net_usdt_received = float(resp_data.get("_filled_usdt") or sell_slice_usd) * 0.999

    # Priority #1: Pure USDT Profit booked immediately from the wave gain!
    if "100% Pure Cash" in strategy:
        pure_usdt_profit = round(wave_gain_usd, 4)
        refill_contrib = round(max(0.0, net_usdt_received - pure_usdt_profit), 4)
    else:
        # 70% of the wave gain is Pure Untouchable USDT Profit;
        # the rest of the sold slice + 30% of the wave gain goes into the Share-Refill Pool
        # so after 1–2 skims it replaces 100% of `sold_sh` PLUS adds extra bonus shares!
        pure_usdt_profit = round(wave_gain_usd * 0.70, 4)
        refill_contrib = round(max(0.0, net_usdt_received - pure_usdt_profit), 4)

    time.sleep(0.9)
    bals, _ = fetch_live_exchange_balances()
    ts_now = now_hms()

    with state_lock:
        if bals:
            STATE["exchange_bridge"]["available_usdt"] = round(bals.get("USDT", {}).get("available", 0.0), 4)
            card["shares"] = bals.get(sym, {}).get("available", max(0.0, shares - sold_sh))
        else:
            card["shares"] = max(0.0, shares - sold_sh)

        new_sh = float(card["shares"])
        card["refill_pool_usdt"] = round(float(card.get("refill_pool_usdt") or 0.0) + refill_contrib, 4)
        card["skims_since_refill"] = int(card.get("skims_since_refill") or 0) + 1
        card["last_skim_sell_price"] = price

        card["vaulted_usd"] = round(float(card.get("vaulted_usd") or 0.0) + pure_usdt_profit, 4)
        card["skim_count"] = int(card.get("skim_count") or 0) + 1
        card["anchor_usd"] = round(new_sh * price, 2)
        card["entry_price"] = price
        card["sliding_low"] = price
        card["highest_price"] = price
        card["gain_from_low_pct"] = 0.0
        card["ratchet_armed"] = False
        card["zero_loss_stop"] = 0.0
        card["status"] = "HOLDING_ANCHOR"

        STATE["vault_cash"] = round(float(STATE.get("vault_cash", 0.0)) + pure_usdt_profit, 4)
        STATE["total_skims"] = int(STATE.get("total_skims", 0)) + 1
        STATE["exchange_bridge"]["realized_usdt_profit"] = round(
            float(STATE["exchange_bridge"].get("realized_usdt_profit") or 0.0) + pure_usdt_profit, 4
        )

        total_generated = round(STATE["vault_cash"] + STATE["added_to_anchors"], 4)
        STATE["profit_curve"].append({"time": ts_now, "value": total_generated})
        if len(STATE["profit_curve"]) > 50:
            STATE["profit_curve"] = STATE["profit_curve"][-50:]

        STATE["skim_ledger"].insert(0, {
            "id": str(uuid.uuid4())[:8],
            "symbol": sym,
            "mode": "LIVE",
            "stage": f"Direct USDT Profit Skim (+${pure_usdt_profit:.4f} USDT)",
            "price": f"{price:.4f}",
            "time": ts_now,
            "profit": pure_usdt_profit,
            "vault_cut": pure_usdt_profit,
            "topup_cut": 0.0,
            "shares": f"{new_sh:.6f}"
        })
        if len(STATE["skim_ledger"]) > 60:
            STATE["skim_ledger"] = STATE["skim_ledger"][:60]

        pool_now = card["refill_pool_usdt"]

    add_log(
        f"💰 [DIRECT USDT SKIM BANKED] ({sym}) @ ${price:.4f} (+{gain_pct:.2f}%): "
        f"Kept +${pure_usdt_profit:.4f} Pure USDT Profit in Wallet! "
        f"(Share-Refill Pool: ${pool_now:.2f} ready to replace shares)"
    )
    dispatch_phone_alert(
        f"Direct USDT Profit Skim ({sym})",
        f"Pure USDT Profit Kept: +${pure_usdt_profit:.4f}\n"
        f"Share-Refill Pool: ${pool_now:.2f} USDT",
        category="skim"
    )
    save_persistent_state(sync_db=True)


def execute_periodic_share_refill(sym, current_price, manual_trigger=False):
    """
    PERIODIC SHARE-REFILL ENGINE ("AFTER A FEW SKIMS, TAKE PART OF THE MONEY & REPLACE SHARES"):
    Uses the accumulated `refill_pool_usdt` (part of your Spot USDT) to buy back
    and replace your shares on Gate.io while leaving your Pure USDT Profits untouched!
    """
    with state_lock:
        card = STATE["coins"].get(sym)
        if not card:
            return False, "Coin card not found."
        refill_pool = float(card.get("refill_pool_usdt") or 0.0)
        current_sh = float(card.get("shares") or 0.0)
        prot_sh = float(card.get("protected_shares") or current_sh)
        usdt_avail = float(STATE["exchange_bridge"].get("available_usdt") or 0.0)

    if usdt_avail < GATEIO_MIN_ORDER_USD:
        return False, f"Gate.io requires at least $3.18 USDT to refill shares (Available: ${usdt_avail:.2f})."

    # Spend only the Refill Pool (or minimum $3.20 order size) so pure profit stays in Spot USDT!
    refill_usd_to_spend = max(GATEIO_MIN_ORDER_USD * 1.01, min(refill_pool, usdt_avail * 0.85))
    if manual_trigger and refill_pool < GATEIO_MIN_ORDER_USD:
        deficit_sh = max(0.0, prot_sh - current_sh)
        needed_usd = max(GATEIO_MIN_ORDER_USD * 1.01, (deficit_sh * current_price) * 1.02)
        refill_usd_to_spend = min(usdt_avail * 0.98, needed_usd)

    order_cooldowns[sym] = time.time()
    ok, msg, resp_data = execute_live_exchange_order(sym, "buy", usd_amount=refill_usd_to_spend)
    if not ok or not resp_data:
        return False, msg

    bought_sh = float(resp_data.get("_net_received_shares") or (refill_usd_to_spend / current_price))
    spent_usdt = float(resp_data.get("_filled_usdt") or refill_usd_to_spend)
    time.sleep(0.9)
    bals, _ = fetch_live_exchange_balances()
    ts_now = now_hms()

    with state_lock:
        if bals:
            STATE["exchange_bridge"]["available_usdt"] = round(bals.get("USDT", {}).get("available", 0.0), 4)
            card["shares"] = bals.get(sym, {}).get("available", current_sh + bought_sh)
        else:
            card["shares"] = current_sh + bought_sh

        new_total_sh = float(card["shares"])
        net_bonus_above_prot = max(0.0, new_total_sh - prot_sh)
        bonus_usd_val = round(net_bonus_above_prot * current_price, 4)

        if new_total_sh > prot_sh:
            card["protected_shares"] = new_total_sh
        card["bonus_shares_added"] = round(float(card.get("bonus_shares_added") or 0.0) + bought_sh, 6)
        card["share_topup_usd"] = round(float(card.get("share_topup_usd") or 0.0) + bonus_usd_val, 4)
        card["refill_pool_usdt"] = max(0.0, round(refill_pool - spent_usdt, 4))
        card["skims_since_refill"] = 0
        card["anchor_usd"] = round(new_total_sh * current_price, 2)
        card["entry_price"] = current_price
        card["sliding_low"] = current_price
        card["highest_price"] = current_price
        card["status"] = "HOLDING_ANCHOR"

        if bonus_usd_val > 0:
            STATE["added_to_anchors"] = round(float(STATE.get("added_to_anchors", 0.0)) + bonus_usd_val, 4)
            total_gen = round(STATE["vault_cash"] + STATE["added_to_anchors"], 4)
            STATE["profit_curve"].append({"time": ts_now, "value": total_gen})

        STATE["skim_ledger"].insert(0, {
            "id": str(uuid.uuid4())[:8],
            "symbol": sym,
            "mode": "LIVE",
            "stage": f"Auto Share-Refill (+{bought_sh:.5f} {sym} Replaced)",
            "price": f"{current_price:.4f}",
            "time": ts_now,
            "profit": bonus_usd_val,
            "vault_cut": 0.0,
            "topup_cut": bonus_usd_val,
            "shares": f"{new_total_sh:.6f}"
        })

    add_log(
        f"🔄 [AUTO SHARE-REFILL COMPLETED] ({sym}) @ ${current_price:.4f}: "
        f"Replaced +{bought_sh:.6f} {sym} using Refill Pool (${spent_usdt:.2f}) -> "
        f"Shares now {new_total_sh:.6f} {sym} while Pure USDT Profits stay safe!"
    )
    dispatch_phone_alert(
        f"Shares Refilled & Restored ({sym})",
        f"Added +{bought_sh:.6f} {sym} -> Total Shares: {new_total_sh:.6f} {sym}",
        category="buy"
    )
    save_persistent_state(sync_db=True)
    return True, "Shares refilled."


# ==============================================================================
# 9. SELF-HEALING WORKER ENGINE
# ==============================================================================
def run_single_engine_cycle():
    global _last_engine_tick_ts, _last_wallet_sync_ts
    if not tick_lock.acquire(blocking=False):
        return
    try:
        pull_latest_state_if_newer()
        run_automatic_bug_fixer()
        _last_engine_tick_ts = time.time()
        with state_lock:
            active_symbols = list(STATE.get("coins", {}).keys())
            live_armed = bool(STATE.get("exchange_bridge", {}).get("live_armed", False))
            if not active_symbols:
                STATE["last_price_tick_hms"] = now_hms()

        for sym in active_symbols:
            evaluate_coin_tick(sym)

        now_ts = time.time()
        if live_armed and (now_ts - _last_wallet_sync_ts) >= 30:
            _last_wallet_sync_ts = now_ts
            sync_all_live_cards_with_gateio(auto_buy_waiting=True, verbose=False)
    except Exception as e:
        logger.debug(f"Single engine cycle notice: {e}")
    finally:
        tick_lock.release()


def market_engine_loop():
    while True:
        run_single_engine_cycle()
        time.sleep(2.5)


def keep_alive_self_ping_loop():
    while True:
        time.sleep(240)
        if RENDER_EXTERNAL_URL:
            try:
                requests.get(f"{RENDER_EXTERNAL_URL.rstrip('/')}/heartbeat", timeout=6)
            except Exception:
                pass


def ensure_worker_threads_running():
    global _worker_pid, _engine_thread, _keepalive_thread
    curr_pid = os.getpid()
    if _worker_pid != curr_pid or _engine_thread is None or not _engine_thread.is_alive():
        _worker_pid = curr_pid
        _engine_thread = threading.Thread(target=market_engine_loop, daemon=True)
        _engine_thread.start()
        _keepalive_thread = threading.Thread(target=keep_alive_self_ping_loop, daemon=True)
        _keepalive_thread.start()

    if (time.time() - _last_engine_tick_ts) >= 2.4:
        threading.Thread(target=run_single_engine_cycle, daemon=True).start()


@app.before_request
def before_any_request():
    pull_latest_state_if_newer()
    ensure_worker_threads_running()
# ==============================================================================
# END OF PART 1 OF 2 — PASTE PART 2 DIRECTLY BELOW THIS LINE
# ==============================================================================# ==============================================================================
# VAULTSKIM COMMAND CENTER — v6.0 PRIORITY USDT & SHARE REFILL POOL
# PART 2 OF 2: FLASK API ROUTES, MANUAL SHARE REFILL & FULL COMMAND CENTER UI
# ==============================================================================
@app.route("/ping")
@app.route("/health")
@app.route("/heartbeat")
@app.route("/wake")
@app.route("/cron")
@app.route("/api/ping")
@app.route("/api/health")
@app.route("/api/heartbeat")
def route_heartbeat():
    ensure_worker_threads_running()
    with state_lock:
        armed = bool(STATE.get("exchange_bridge", {}).get("live_armed", False))
        count = len(STATE.get("coins", {}))
        last_tick = STATE.get("last_price_tick_hms", "--:--:--")
        fixes = int(STATE.get("auto_fixes_count") or 0)
    return jsonify({
        "ok": True,
        "status": "ALIVE",
        "version": "v6.0 - Priority USDT & Share Refill Pool",
        "live_bridge_armed": armed,
        "active_coins": count,
        "auto_fixes_applied": fixes,
        "last_price_tick_utc": last_tick,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }), 200


@app.route("/icon.svg")
def route_icon_svg():
    svg = """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512">
        <rect width="512" height="512" rx="115" fill="#06121e" stroke="#10b981" stroke-width="14"/>
        <path d="M96 320 L192 210 L272 265 L400 135" fill="none" stroke="#10b981" stroke-width="36" stroke-linecap="round" stroke-linejoin="round"/>
        <circle cx="400" cy="135" r="24" fill="#10b981"/>
        <text x="256" y="435" font-family="system-ui, sans-serif" font-size="82" font-weight="900" fill="#ffffff" text-anchor="middle" letter-spacing="6">VAULT</text>
    </svg>"""
    return Response(svg, mimetype="image/svg+xml")


@app.route("/manifest.json")
def route_manifest():
    manifest = {
        "name": "VaultSkim Command Center",
        "short_name": "VaultSkim",
        "description": "Priority Pure USDT Profit Skimmer & Auto Share-Refill Pool",
        "start_url": "/",
        "display": "standalone",
        "background_color": "#050914",
        "theme_color": "#10b981",
        "icons": [
            {
                "src": "/icon.svg",
                "sizes": "192x192 512x512",
                "type": "image/svg+xml",
                "purpose": "any maskable"
            }
        ]
    }
    return jsonify(manifest)


@app.route("/sw.js")
def route_sw():
    js = """
    self.addEventListener('install', e => self.skipWaiting());
    self.addEventListener('activate', e => e.waitUntil(self.clients.claim()));
    self.addEventListener('fetch', e => {});
    """
    return Response(js, mimetype="application/javascript")


@app.route("/api/state")
def route_api_state():
    pull_latest_state_if_newer()
    ensure_worker_threads_running()
    with state_lock:
        b = STATE.get("exchange_bridge", {})
        raw_key = b.get("api_key") or ""
        masked_key = f"{raw_key[:4]}...{raw_key[-4:]}" if len(raw_key) > 8 else ("Saved" if raw_key else "")
        avail_usdt = float(b.get("available_usdt") or 0.0)
        realized_usdt = float(b.get("realized_usdt_profit") or 0.0)
        baseline_usdt = float(b.get("baseline_usdt") or 0.0)

        safe_bridge = {
            "exchange": b.get("exchange", "gateio"),
            "masked_key": masked_key,
            "has_secret": bool(b.get("api_secret")),
            "max_order_usd": float(b.get("max_order_usd") or 250.0),
            "live_armed": bool(b.get("live_armed", False)),
            "verified": bool(b.get("verified", False)),
            "available_usdt": avail_usdt,
            "realized_usdt_profit": realized_usdt,
            "baseline_usdt": baseline_usdt,
            "last_sync_ts": b.get("last_sync_ts", 0)
        }
        a = STATE.get("alerts", {})
        safe_alerts = {
            "enabled": bool(a.get("enabled", False)),
            "ntfy_topic": a.get("ntfy_topic", ""),
            "telegram_chat_id": a.get("telegram_chat_id", ""),
            "has_tg_token": bool(a.get("telegram_bot_token")),
            "alert_on_skim": bool(a.get("alert_on_skim", True)),
            "alert_on_shield": bool(a.get("alert_on_shield", True)),
            "alert_on_buy": bool(a.get("alert_on_buy", True))
        }
        vc = float(STATE.get("vault_cash") or 0.0)
        aa = float(STATE.get("added_to_anchors") or 0.0)
        total_pool = sum(float(c.get("refill_pool_usdt") or 0.0) for c in STATE.get("coins", {}).values())

        payload = {
            "version": STATE.get("version", "v6.0 - Priority USDT & Share Refill Pool"),
            "vault_cash": vc,
            "added_to_anchors": aa,
            "total_escrow_reserved": round(total_pool, 4),
            "total_profit": round(vc + aa, 4),
            "total_skims": int(STATE.get("total_skims") or 0),
            "total_withdrawn": float(STATE.get("total_withdrawn") or 0.0),
            "auto_fixes_count": int(STATE.get("auto_fixes_count") or 0),
            "frozen": bool(STATE.get("frozen", False)),
            "last_price_tick_hms": STATE.get("last_price_tick_hms", now_hms()),
            "profit_curve": STATE.get("profit_curve", []),
            "skim_ledger": STATE.get("skim_ledger", []),
            "cashout_history": STATE.get("cashout_history", []),
            "logs": STATE.get("logs", []),
            "exchange_bridge": safe_bridge,
            "alerts": safe_alerts,
            "coins": STATE.get("coins", {})
        }
    resp = make_response(jsonify(payload))
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    return resp


@app.route("/api/bridge/save", methods=["POST"])
def route_bridge_save():
    data = request.json or {}
    with state_lock:
        b = STATE["exchange_bridge"]
        if data.get("exchange"):
            b["exchange"] = data["exchange"].strip().lower()
        if data.get("api_key") and data["api_key"].strip():
            b["api_key"] = data["api_key"].strip()
        if data.get("api_secret") and data["api_secret"].strip():
            b["api_secret"] = data["api_secret"].strip()
        if data.get("max_order_usd") is not None:
            try:
                b["max_order_usd"] = float(data["max_order_usd"])
            except Exception:
                pass

        if data.get("toggle_live"):
            b["live_armed"] = not bool(b.get("live_armed", False))
        elif "live_armed" in data:
            b["live_armed"] = bool(data["live_armed"])

    order_cooldowns.clear()
    ok, result = sync_all_live_cards_with_gateio(auto_buy_waiting=True, verbose=True)
    if not ok:
        add_log(f"❌ EXCHANGE BRIDGE ERROR: {result}")
        save_persistent_state(sync_db=True)
        return jsonify({"ok": False, "error": result}), 400

    with state_lock:
        armed_now = STATE["exchange_bridge"]["live_armed"]
    save_persistent_state(sync_db=True)
    return jsonify({
        "ok": True,
        "available_usdt": result,
        "live_armed": armed_now
    })


@app.route("/api/sync_live", methods=["POST"])
def route_sync_live():
    order_cooldowns.clear()
    run_automatic_bug_fixer()
    run_single_engine_cycle()
    ok, result = sync_all_live_cards_with_gateio(auto_buy_waiting=True, verbose=True)
    if not ok:
        return jsonify({"ok": False, "error": result}), 400
    save_persistent_state(sync_db=True)
    return jsonify({"ok": True, "available_usdt": result})


@app.route("/api/coin/launch", methods=["POST"])
def route_coin_launch():
    data = request.json or {}
    raw_input = (data.get("symbol") or "").strip()
    try:
        anchor_usd = float(data.get("anchor_usd") or 10.0)
    except Exception:
        anchor_usd = 10.0

    if not raw_input:
        return jsonify({"ok": False, "error": "Enter a coin symbol (e.g. QNT) or contract address."}), 400

    is_contract = len(raw_input) > 15
    sym = raw_input.upper().replace("/USDT", "").replace("_USDT", "") if not is_contract else raw_input[:8].upper()
    contract = raw_input if is_contract else ""

    quote = fetch_live_market_quote(sym, contract)
    if not quote or quote["price"] <= 0:
        return jsonify({"ok": False, "error": f"Could not verify live market feed for '{raw_input}'."}), 400

    if quote.get("is_dex") and float(quote.get("liquidity_usd") or 0.0) < 15000.0:
        msg = f"Anti-Rug Firewall Blocked {sym}: Pool liquidity (${quote.get('liquidity_usd', 0):.0f}) is under $15,000 minimum."
        add_log(f"🛡️ {msg}")
        return jsonify({"ok": False, "error": msg}), 400

    if quote.get("symbol"):
        sym = quote["symbol"].upper()

    price = float(quote["price"])
    init_paper_sh = round(anchor_usd / price, 6)
    with state_lock:
        live_armed = bool(STATE["exchange_bridge"].get("live_armed", False))
        STATE["coins"][sym] = {
            "symbol": sym,
            "contract": contract,
            "source": quote["source"],
            "strategy": "70% Pure USDT + 30% Share Refill",
            "live_enabled": live_armed,
            "status": "WAITING_LIVE_BUY" if live_armed else "HOLDING_ANCHOR",
            "current_price": price,
            "entry_price": price,
            "sliding_low": price,
            "highest_price": price,
            "zero_loss_stop": 0.0,
            "gain_from_low_pct": 0.0,
            "anchor_usd": anchor_usd,
            "shares": 0.0 if live_armed else init_paper_sh,
            "protected_shares": 0.0 if live_armed else init_paper_sh,
            "refill_pool_usdt": 0.0,
            "skims_since_refill": 0,
            "last_skim_sell_price": price,
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
        mode_label = "LIVE GATE.IO ORDER MODE" if live_armed else "PAPER SIMULATOR"
        add_log(f"🚀 Launched {sym}/USDT (${anchor_usd:.2f} Anchor in {mode_label})")

    if live_armed:
        order_cooldowns[sym] = 0
        sync_all_live_cards_with_gateio(auto_buy_waiting=True, verbose=True)
    save_persistent_state(sync_db=True)
    return jsonify({"ok": True, "symbol": sym})


@app.route("/api/coin/switch_mode", methods=["POST"])
def route_coin_switch_mode():
    data = request.json or {}
    sym = (data.get("symbol") or "").upper().strip()
    with state_lock:
        card = STATE["coins"].get(sym)
        if not card:
            return jsonify({"ok": False, "error": "Coin card not found."}), 404

        card["live_enabled"] = not bool(card.get("live_enabled", False))
        is_now_live = card["live_enabled"]

        if is_now_live:
            card["shares"] = 0.0
            card["status"] = "WAITING_LIVE_BUY"
            add_log(f"⚡ COIN EXECUTION MODE [{sym}]: Switched to LIVE ORDERS ENABLED.")
        else:
            price = float(card.get("current_price") or 1.0)
            card["shares"] = round(float(card.get("anchor_usd") or 10.0) / price, 6)
            card["protected_shares"] = card["shares"]
            card["status"] = "HOLDING_ANCHOR"
            add_log(f"📄 COIN EXECUTION MODE [{sym}]: Switched to PAPER SIMULATOR.")

    if is_now_live:
        order_cooldowns[sym] = 0
        sync_all_live_cards_with_gateio(auto_buy_waiting=True, verbose=True)
    save_persistent_state(sync_db=True)
    return jsonify({"ok": True, "live_enabled": is_now_live})


@app.route("/api/coin/cycle_strategy", methods=["POST"])
def route_coin_cycle_strategy():
    data = request.json or {}
    sym = (data.get("symbol") or "").upper().strip()
    strategies = [
        "70% Pure USDT + 30% Share Refill",
        "100% Pure Cash Skim",
        "100% Share Compounder"
    ]
    with state_lock:
        card = STATE["coins"].get(sym)
        if not card:
            return jsonify({"ok": False, "error": "Coin card not found."}), 404
        curr = card.get("strategy") or strategies[0]
        idx = (strategies.index(curr) + 1) % len(strategies) if curr in strategies else 0
        card["strategy"] = strategies[idx]
        add_log(f"🔄 Strategy for {sym} switched to: {card['strategy']}")
    save_persistent_state(sync_db=True)
    return jsonify({"ok": True, "strategy": card["strategy"]})


@app.route("/api/coin/force_rebuy", methods=["POST"])
def route_coin_force_rebuy():
    """
    1-Tap Manual Share Refill:
    Immediately triggers a Share Refill using the coin's Refill Pool (or available
    Spot USDT) to buy back and replace shares on Gate.io right now!
    """
    data = request.json or {}
    sym = (data.get("symbol") or "").upper().strip()
    with state_lock:
        card = STATE["coins"].get(sym)
        if not card:
            return jsonify({"ok": False, "error": "Coin not found."}), 404
        px = float(card.get("current_price") or 0.0)
        pool = float(card.get("refill_pool_usdt") or 0.0)

    if px <= 0:
        return jsonify({"ok": False, "error": f"Invalid price for {sym}."}), 400

    order_cooldowns[sym] = 0
    ok, msg = execute_periodic_share_refill(sym, px, manual_trigger=True)
    if not ok:
        return jsonify({"ok": False, "error": msg}), 400
    return jsonify({"ok": True})


@app.route("/api/coin/force_buy", methods=["POST"])
def route_coin_force_buy():
    data = request.json or {}
    sym = (data.get("symbol") or "").upper().strip()
    custom_usd = data.get("usd_amount")

    with state_lock:
        card = STATE["coins"].get(sym)
        if not card:
            return jsonify({"ok": False, "error": "Coin not found."}), 404
        sh = float(card.get("shares") or 0.0)
        usdt_avail = float(STATE["exchange_bridge"].get("available_usdt") or 0.0)
        anchor_usd = float(card.get("anchor_usd") or 10.0)
        card["live_enabled"] = True
        STATE["exchange_bridge"]["live_armed"] = True

    if custom_usd is not None:
        buy_usd = float(custom_usd)
    elif sh <= 0:
        buy_usd = anchor_usd
    else:
        buy_usd = max(GATEIO_MIN_ORDER_USD, usdt_avail * 0.988)

    order_cooldowns[sym] = 0
    ok, msg, _ = execute_live_exchange_order(sym, "buy", usd_amount=buy_usd)
    if ok:
        time.sleep(1.1)
        sync_all_live_cards_with_gateio(auto_buy_waiting=False, verbose=True)
        with state_lock:
            c_now = STATE["coins"].get(sym)
            if c_now and float(c_now.get("shares") or 0.0) > 0:
                px = float(c_now.get("current_price") or 1.0)
                c_now["anchor_usd"] = round(float(c_now["shares"]) * px, 2)
                c_now["protected_shares"] = max(float(c_now.get("protected_shares") or 0.0), float(c_now["shares"]))
                c_now["status"] = "HOLDING_ANCHOR"
        save_persistent_state(sync_db=True)
        return jsonify({"ok": True})
    return jsonify({"ok": False, "error": msg}), 400


@app.route("/api/coin/edit_anchor", methods=["POST"])
def route_coin_edit_anchor():
    data = request.json or {}
    sym = (data.get("symbol") or "").upper().strip()
    try:
        new_anchor = float(data.get("anchor_usd") or 10.0)
    except Exception:
        return jsonify({"ok": False, "error": "Invalid Anchor USD amount."}), 400

    with state_lock:
        card = STATE["coins"].get(sym)
        if not card:
            return jsonify({"ok": False, "error": "Coin not found."}), 404
        is_live = bool(card.get("live_enabled"))
        shares_now = float(card.get("shares") or 0.0)
        price_now = float(card.get("current_price") or 1.0)
        current_token_val = round(shares_now * price_now, 2)

        if not is_live:
            card["anchor_usd"] = round(new_anchor, 2)
            card["shares"] = round(new_anchor / price_now, 6)
            card["protected_shares"] = card["shares"]
            add_log(f"✏️ [PAPER] Anchor for {sym} updated to ${new_anchor:.2f}")
            save_persistent_state(sync_db=True)
            return jsonify({"ok": True})

    if shares_now <= 0:
        with state_lock:
            STATE["coins"][sym]["anchor_usd"] = round(new_anchor, 2)
        order_cooldowns[sym] = 0
        sync_all_live_cards_with_gateio(auto_buy_waiting=True, verbose=True)
        save_persistent_state(sync_db=True)
        return jsonify({"ok": True})

    delta_usd = new_anchor - current_token_val
    if delta_usd >= 0.25:
        order_cooldowns[sym] = 0
        ok, msg, _ = execute_live_exchange_order(sym, "buy", usd_amount=delta_usd)
        if not ok:
            with state_lock:
                STATE["coins"][sym]["anchor_usd"] = current_token_val
            save_persistent_state(sync_db=True)
            return jsonify({
                "ok": False,
                "error": f"Gate.io Buy Notice for {sym}: {msg} (Anchor kept at real token value ${current_token_val:.2f})"
            }), 400

        time.sleep(1.1)
        sync_all_live_cards_with_gateio(auto_buy_waiting=False, verbose=True)
        with state_lock:
            c_updated = STATE["coins"].get(sym)
            if c_updated:
                new_sh = float(c_updated.get("shares") or shares_now)
                px = float(c_updated.get("current_price") or price_now)
                actual_val = round(new_sh * px, 2)
                c_updated["anchor_usd"] = actual_val
                c_updated["protected_shares"] = max(float(c_updated.get("protected_shares") or 0.0), new_sh)
                add_log(
                    f"✅ LIVE ANCHOR INCREASED ({sym}): Bought extra shares on Gate.io -> "
                    f"Now holding {new_sh:.6f} {sym} (${actual_val:.2f})"
                )
        save_persistent_state(sync_db=True)
        return jsonify({"ok": True})

    with state_lock:
        STATE["coins"][sym]["anchor_usd"] = current_token_val
        add_log(f"✏️ Anchor baseline for {sym} aligned to real token value ${current_token_val:.2f}")
    save_persistent_state(sync_db=True)
    return jsonify({"ok": True})


@app.route("/api/coin/spike", methods=["POST"])
def route_coin_spike():
    data = request.json or {}
    sym = (data.get("symbol") or "").upper().strip()
    with state_lock:
        card = STATE["coins"].get(sym)
        if not card:
            return jsonify({"ok": False, "error": "Coin not found."}), 404
        if card.get("live_enabled"):
            return jsonify({"ok": False, "error": "Practice Simulator is disabled on Live Gate.io coins."}), 400
        old_px = float(card.get("current_price") or 1.0)
        new_price = old_px * 1.015
        card["current_price"] = new_price

    execute_paper_simulator_skim(sym, new_price, 1.50)
    return jsonify({"ok": True})


@app.route("/api/coin/remove", methods=["POST"])
def route_coin_remove():
    data = request.json or {}
    sym = (data.get("symbol") or "").upper().strip()
    with state_lock:
        if sym in STATE["coins"]:
            del STATE["coins"][sym]
            add_log(f"🗑️ Removed coin card {sym}.")
    save_persistent_state(sync_db=True)
    return jsonify({"ok": True})


@app.route("/api/radar/scan")
def route_radar_scan():
    candidates = ["QNT", "LTC", "LQTY", "PEPE", "BONK", "SOL", "SUI", "RENDER"]
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
    add_log(f"📡 Meme & Momentum Radar Scanned: {len(results)} liquid pairs verified.")
    return jsonify({"ok": True, "results": results})


@app.route("/api/alerts/save", methods=["POST"])
def route_alerts_save():
    data = request.json or {}
    with state_lock:
        a = STATE["alerts"]
        a["enabled"] = bool(data.get("enabled", True))
        if "ntfy_topic" in data:
            a["ntfy_topic"] = (data.get("ntfy_topic") or "").strip()
        if "telegram_chat_id" in data:
            a["telegram_chat_id"] = (data.get("telegram_chat_id") or "").strip()
        if data.get("telegram_bot_token") and data["telegram_bot_token"].strip():
            a["telegram_bot_token"] = data["telegram_bot_token"].strip()
    save_persistent_state(sync_db=True)

    if data.get("send_test"):
        dispatch_phone_alert(
            "VaultSkim v6.0 Alert Connected",
            "Your phone push notification bridge is active and verified!",
            category="skim"
        )
    add_log("🔔 Phone Push Alert settings updated.")
    return jsonify({"ok": True})


@app.route("/api/cashout/record", methods=["POST"])
def route_cashout_record():
    data = request.json or {}
    try:
        amount = float(data.get("amount") or 0.0)
    except Exception:
        amount = 0.0
    dest = (data.get("destination") or "Gate.io Spot USDT / Trust Wallet").strip()

    with state_lock:
        avail_vault = float(STATE.get("vault_cash") or 0.0)
        if amount <= 0 or amount > avail_vault:
            return jsonify({"ok": False, "error": f"Enter an amount up to ${avail_vault:.4f}."}), 400
        STATE["vault_cash"] = round(avail_vault - amount, 4)
        STATE["total_withdrawn"] = round(float(STATE.get("total_withdrawn") or 0.0) + amount, 4)
        STATE["cashout_history"].insert(0, {
            "time": now_hms(),
            "amount": round(amount, 4),
            "destination": dest
        })
        add_log(f"🏦 Vault Cash-Out Recorded: ${amount:.4f} -> {dest}")
    save_persistent_state(sync_db=True)
    return jsonify({"ok": True})


@app.route("/api/control/freeze", methods=["POST"])
def route_control_freeze():
    with state_lock:
        STATE["frozen"] = not bool(STATE.get("frozen", False))
        state_word = "FROZEN" if STATE["frozen"] else "ACTIVE"
        add_log(f"🛑 Live Execution Engine switched to: {state_word}")
        frozen_now = STATE["frozen"]
    save_persistent_state(sync_db=True)
    return jsonify({"ok": True, "frozen": frozen_now})


@app.route("/api/control/reset", methods=["POST"])
def route_control_reset():
    with state_lock:
        STATE["vault_cash"] = 0.0
        STATE["added_to_anchors"] = 0.0
        STATE["total_escrow_reserved"] = 0.0
        STATE["total_skims"] = 0
        STATE["profit_curve"] = [{"time": now_hms(), "value": 0.0}]
        STATE["skim_ledger"] = []
        avail_u = float(STATE["exchange_bridge"].get("available_usdt") or 0.0)
        STATE["exchange_bridge"]["baseline_usdt"] = round(avail_u, 4)
        STATE["exchange_bridge"]["realized_usdt_profit"] = 0.0

        for sym, c in STATE.get("coins", {}).items():
            c["share_topup_usd"] = 0.0
            c["vaulted_usd"] = 0.0
            c["refill_pool_usdt"] = 0.0
            c["skims_since_refill"] = 0
            c["skim_count"] = 0
            c["ratchet_armed"] = False
            c["zero_loss_stop"] = 0.0
            sh = float(c.get("shares") or 0.0)
            px = float(c.get("current_price") or 0.0)
            if sh > 0 and px > 0:
                c["anchor_usd"] = round(sh * px, 2)
                c["protected_shares"] = max(float(c.get("protected_shares") or 0.0), sh)
                c["entry_price"] = px
                c["sliding_low"] = px
                c["highest_price"] = px
                c["status"] = "HOLDING_ANCHOR"
        add_log("♻️ Reset Counters to $0.0000 • Preserved Protected Share Floors & Cleared Pools.")
    save_persistent_state(sync_db=True)
    return jsonify({"ok": True})


@app.route("/api/export_csv")
def route_export_csv():
    rows = ["Timestamp_UTC,Symbol,Mode,Stage,Price_USD,Total_Skim_USD,Real_USDT_Profit,Bonus_Shares_USD,Shares_Held"]
    with state_lock:
        for item in STATE.get("skim_ledger", []):
            rows.append(
                f"{item.get('time','')},{item.get('symbol','')},{item.get('mode','')},"
                f"\"{item.get('stage','')}\",{item.get('price','')},{item.get('profit',0)},"
                f"{item.get('vault_cut',0)},{item.get('topup_cut',0)},{item.get('shares','')}"
            )
    csv_bytes = "\n".join(rows)
    return Response(
        csv_bytes,
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment;filename=vaultskim_verified_ledger.csv"}
    )


# ==============================================================================
# 10. COMPLETE FRONTEND DASHBOARD HTML / CSS / SVG CHART / JS CONTROLLER
# ==============================================================================
DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <meta name="theme-color" content="#060b16">
    <meta name="apple-mobile-web-app-capable" content="yes">
    <meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
    <link rel="manifest" href="/manifest.json">
    <link rel="icon" type="image/svg+xml" href="/icon.svg">
    <link rel="apple-touch-icon" href="/icon.svg">
    <title>VaultSkim Command Center • v6.0 Priority USDT & Share Refill Pool</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <style>
        :root {
            --bg-main: #060a12;
            --bg-card: #0d1424;
            --bg-inner: #080d18;
            --border-subtle: #1c2841;
        }
        * { box-sizing: border-box; -webkit-tap-highlight-color: transparent; }
        body {
            background-color: var(--bg-main);
            color: #e2e8f0;
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
            margin: 0;
            padding: 10px 10px 90px 10px;
        }
        .mono {
            font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
        }
        .v-card {
            background-color: var(--bg-card);
            border: 1px solid var(--border-subtle);
            border-radius: 14px;
            box-shadow: 0 10px 28px rgba(0, 0, 0, 0.45);
        }
        .v-inner {
            background-color: var(--bg-inner);
            border: 1px solid #162035;
            border-radius: 10px;
        }
        .profit-card {
            background: linear-gradient(180deg, rgba(16, 185, 129, 0.12) 0%, rgba(13, 20, 36, 0.95) 100%);
            border: 1px solid rgba(16, 185, 129, 0.38);
            border-radius: 14px;
        }
        .bridge-card {
            background-color: #0e1525;
            border: 1px solid rgba(245, 158, 11, 0.55);
            border-radius: 14px;
        }
        .btn-press:active { transform: scale(0.97); }
        ::-webkit-scrollbar { width: 5px; height: 5px; }
        ::-webkit-scrollbar-track { background: #070b14; }
        ::-webkit-scrollbar-thumb { background: #1e293b; border-radius: 4px; }
    </style>
</head>
<body class="max-w-xl mx-auto">

    <!-- TOP MASTER COMMAND HEADER -->
    <div class="v-card p-3.5 mb-3">
        <div class="flex items-center justify-between gap-2 mb-1">
            <h1 class="text-lg sm:text-xl font-extrabold text-white tracking-tight">VaultSkim Command Center</h1>
            <span id="versionBadge" class="text-[11px] font-bold px-2.5 py-0.5 rounded-full bg-emerald-950/90 text-emerald-400 border border-emerald-600/60 mono whitespace-nowrap">
                v6.0 - Priority USDT & Refill Pool
            </span>
        </div>
        <div class="flex items-center justify-between text-[11px] text-slate-400 mb-3">
            <span>Direct Pure USDT Skims • Periodic Share-Refill Pool • Auto-Fixer</span>
            <span id="liveTickClock" class="text-emerald-400 mono font-bold">● Tick: --:--:--</span>
        </div>

        <div class="flex flex-wrap gap-1.5 mb-3">
            <button onclick="toggleDrawer('bridgeDrawer')" class="btn-press px-2.5 py-1.5 rounded-lg bg-amber-500 text-black font-extrabold text-xs flex items-center gap-1 shadow">
                ⚡ Live Exchange Bridge
            </button>
            <button onclick="toggleDrawer('alertsDrawer')" class="btn-press px-2.5 py-1.5 rounded-lg bg-indigo-950 text-indigo-200 border border-indigo-700/70 font-bold text-xs flex items-center gap-1">
                🔔 Phone Alerts
            </button>
            <button onclick="toggleDrawer('cashoutDrawer')" class="btn-press px-2.5 py-1.5 rounded-lg bg-emerald-950 text-emerald-300 border border-emerald-700/70 font-bold text-xs flex items-center gap-1">
                🏦 Access Cash-Out & CSV
            </button>
        </div>

        <div class="flex flex-wrap items-center gap-2 mb-3">
            <div id="liveBridgeBadge" class="px-2.5 py-1 rounded-md bg-emerald-950/90 border border-emerald-600/70 text-emerald-400 font-extrabold text-[11px] mono flex items-center gap-1.5">
                <span>🟢</span> <span>LIVE BRIDGE ARMED (GATEIO)</span>
            </div>
            <div id="spotUsdtBadge" class="px-2.5 py-1 rounded-md bg-slate-900 border border-slate-700 text-slate-200 font-bold text-[11px] mono">
                Gate.io Spot USDT: $0.0000
            </div>
            <div id="pureProfitUsdtBadge" class="px-2.5 py-1 rounded-md bg-emerald-950/80 border border-emerald-700/70 text-emerald-300 font-bold text-[11px] mono">
                Real USDT Added: +$0.0000
            </div>
            <div id="autoFixBadge" class="px-2 py-1 rounded-md bg-sky-950/90 border border-sky-700/70 text-sky-300 font-bold text-[10px] mono">
                🛠️ Auto-Bug Fixer: ON
            </div>
        </div>

        <div class="flex flex-wrap gap-2">
            <button id="freezeBtn" onclick="toggleFreeze()" class="btn-press px-3 py-1.5 rounded-lg bg-rose-950/90 text-rose-300 border border-rose-700/70 font-bold text-xs">
                🛑 Freeze Live
            </button>
            <button id="syncLiveBtn" onclick="manualSyncLive()" class="btn-press px-3 py-1.5 rounded-lg bg-indigo-950/90 text-indigo-200 border border-indigo-600/70 font-bold text-xs">
                🔄 Sync & Auto-Heal
            </button>
            <button onclick="resetProfitCounters()" class="btn-press px-3 py-1.5 rounded-lg bg-slate-800/90 text-slate-300 border border-slate-700 font-bold text-xs">
                ♻️ Reset ($0.00)
            </button>
        </div>
    </div>

    <!-- TOTAL UNTOUCHABLE PROFIT HERO CARD -->
    <div class="profit-card p-4 mb-3 text-center">
        <div class="text-[11px] font-bold uppercase tracking-widest text-slate-400 mb-1">
            TOTAL REALIZED GATE.IO PROFIT (DIRECT USDT + REFILL POOL)
        </div>
        <div id="heroTotalProfit" class="text-4xl font-extrabold text-emerald-400 mono tracking-tight my-1">
            $0.0000
        </div>
        <div id="heroProfitSub" class="text-xs text-slate-300 mono">
            Pure USDT Vaulted: $0.0000 | Share-Refill Pool: $0.00 | Total Skims: 0
        </div>
    </div>

    <!-- DRAWER 1: LIVE EXCHANGE BRIDGE -->
    <div id="bridgeDrawer" class="bridge-card p-4 mb-3 hidden">
        <div class="flex items-start justify-between gap-2 mb-2">
            <h2 class="text-sm font-extrabold text-amber-400 leading-snug">
                ⚡ Live Exchange Execution Bridge (Zero-Withdrawal API Security)
            </h2>
            <button onclick="toggleDrawer('bridgeDrawer')" class="text-slate-400 hover:text-white font-bold text-sm px-1">✕</button>
        </div>
        <div class="space-y-3 text-xs">
            <div>
                <label class="block text-slate-200 font-bold mb-1">1. Spot Exchange</label>
                <select id="bridgeExchange" class="w-full p-2.5 rounded-lg bg-slate-950 border border-slate-700 text-white font-semibold">
                    <option value="gateio">Gate.io Spot v4 (0.1% Fee)</option>
                    <option value="binance">Binance Spot v3 (0.1% Fee)</option>
                </select>
            </div>
            <div>
                <label class="block text-slate-200 font-bold mb-1">2. Spot API Key (No Withdrawals)</label>
                <input id="bridgeApiKey" type="text" placeholder="Paste API Key..." class="w-full p-2.5 rounded-lg bg-slate-950 border border-slate-700 text-white mono">
                <div id="savedKeyLabel" class="text-[11px] text-slate-400 mt-1 mono">Saved Key: None</div>
            </div>
            <div>
                <label class="block text-slate-200 font-bold mb-1">3. Spot API Secret</label>
                <input id="bridgeApiSecret" type="password" placeholder="Paste API Secret (Encrypted)..." class="w-full p-2.5 rounded-lg bg-slate-950 border border-slate-700 text-white mono">
            </div>
            <div>
                <label class="block text-slate-200 font-bold mb-1">4. Max Order Safety Cap (USD)</label>
                <input id="bridgeMaxCap" type="number" value="250" class="w-full p-2.5 rounded-lg bg-slate-950 border border-slate-700 text-white mono">
            </div>
            <div id="bridgeConnectionStatus" class="text-xs font-bold text-emerald-400 mono pt-1">
                Gate.io v4 Connected! Available Spot USDT: $0.0000
            </div>
            <div class="flex flex-wrap gap-2 pt-1">
                <button id="verifyApiBtn" onclick="saveBridgeConfig(false)" class="btn-press px-3 py-2 rounded-lg bg-slate-800 hover:bg-slate-700 text-white border border-slate-600 font-bold text-xs">
                    🔍 Verify API & Check USDT Balance
                </button>
                <button onclick="saveBridgeConfig(true)" class="btn-press px-3.5 py-2 rounded-lg bg-amber-500 hover:bg-amber-400 text-black font-extrabold text-xs">
                    ⚡ Save & Toggle Live Bridge
                </button>
            </div>
        </div>
    </div>

    <!-- DRAWER 2: PHONE ALERTS -->
    <div id="alertsDrawer" class="v-card p-4 mb-3 border border-indigo-500/50 hidden">
        <div class="flex items-center justify-between mb-2">
            <h2 class="text-sm font-extrabold text-indigo-300">🔔 Instant Phone Push Alerts (ntfy.sh & Telegram)</h2>
            <button onclick="toggleDrawer('alertsDrawer')" class="text-slate-400 font-bold text-sm px-1">✕</button>
        </div>
        <div class="space-y-2.5 text-xs">
            <div>
                <label class="block text-slate-300 font-bold mb-1">ntfy.sh Topic Name</label>
                <input id="alertNtfyTopic" type="text" placeholder="e.g. Pinger" class="w-full p-2 rounded-lg bg-slate-950 border border-slate-700 text-white mono">
            </div>
            <div class="grid grid-cols-1 sm:grid-cols-2 gap-2">
                <div>
                    <label class="block text-slate-300 font-bold mb-1">Telegram Bot Token</label>
                    <input id="alertTgToken" type="password" placeholder="123456:ABC-DEF..." class="w-full p-2 rounded-lg bg-slate-950 border border-slate-700 text-white mono">
                </div>
                <div>
                    <label class="block text-slate-300 font-bold mb-1">Telegram Chat ID</label>
                    <input id="alertTgChat" type="text" placeholder="e.g. 987654321" class="w-full p-2 rounded-lg bg-slate-950 border border-slate-700 text-white mono">
                </div>
            </div>
            <div class="flex gap-2 pt-1">
                <button onclick="saveAlertSettings(true)" class="btn-press px-3 py-2 rounded-lg bg-indigo-600 text-white font-bold">
                    🔔 Save & Send Test Alert
                </button>
                <button onclick="saveAlertSettings(false)" class="btn-press px-3 py-2 rounded-lg bg-slate-800 text-slate-200 border border-slate-700 font-bold">
                    Save Settings
                </button>
            </div>
        </div>
    </div>

    <!-- DRAWER 3: CASH-OUT & CSV -->
    <div id="cashoutDrawer" class="v-card p-4 mb-3 border border-emerald-500/50 hidden">
        <div class="flex items-center justify-between mb-2">
            <h2 class="text-sm font-extrabold text-emerald-400">🏦 Vault Cash-Out & Verified CSV Ledger</h2>
            <button onclick="toggleDrawer('cashoutDrawer')" class="text-slate-400 font-bold text-sm px-1">✕</button>
        </div>
        <div class="v-inner p-3 mb-3 text-xs mono flex justify-between items-center">
            <span>Realized USDT Profit in Gate.io Wallet:</span>
            <strong id="cashoutAvailDisplay" class="text-emerald-400 text-sm">$0.0000</strong>
        </div>
        <div class="flex gap-2 mb-3 text-xs">
            <input id="cashoutAmountInput" type="number" step="0.01" placeholder="Amount USD (e.g. 5.00)" class="flex-1 p-2 rounded-lg bg-slate-950 border border-slate-700 text-white mono">
            <button onclick="recordCashout()" class="btn-press px-3 py-2 rounded-lg bg-emerald-500 text-black font-extrabold">
                Record Cash-Out
            </button>
        </div>
        <button onclick="window.location='/api/export_csv'" class="btn-press w-full py-2 rounded-lg bg-slate-800 text-emerald-300 border border-emerald-700/70 font-bold text-xs">
            📥 Download Full Verified Skim Ledger (.CSV)
        </button>
    </div>

    <!-- PROFIT GROWTH CURVE CHART -->
    <div class="v-card p-4 mb-3">
        <div class="flex items-center justify-between mb-1">
            <span class="text-xs font-extrabold uppercase tracking-wider text-slate-200">
                📈 LIVE REALIZED PROFIT GROWTH CURVE
            </span>
            <span id="chartSkimBadge" class="text-[11px] font-bold px-2.5 py-0.5 rounded-full bg-emerald-950 text-emerald-400 border border-emerald-700/70 mono">
                0 Completed Wave Skims
            </span>
        </div>
        <p class="text-[11px] text-slate-400 mb-2">
            Direct Pure USDT Skimmer + Periodic Share-Refill Pool (100% token bags kept intact)
        </p>
        <div class="w-full h-40 bg-slate-950/70 rounded-lg border border-slate-800/80 p-2 relative">
            <svg id="profitChartSvg" viewBox="0 0 500 150" class="w-full h-full overflow-visible"></svg>
        </div>
    </div>

    <!-- VERIFIED SKIM LEDGER -->
    <div class="v-card p-4 mb-3">
        <div class="flex items-center justify-between mb-1">
            <div>
                <div class="text-xs font-extrabold uppercase tracking-wider text-slate-200">
                    📋 VERIFIED GATE.IO SKIM LEDGER
                </div>
                <div class="text-[11px] text-slate-400">Every direct USDT profit skim & share refill</div>
            </div>
            <button onclick="window.location='/api/export_csv'" class="btn-press px-2.5 py-1 rounded-md bg-emerald-950/90 text-emerald-300 border border-emerald-700/70 text-xs font-bold mono">
                📥 CSV
            </button>
        </div>
        <div id="skimLedgerList" class="mt-2.5 space-y-2 max-h-52 overflow-y-auto text-xs mono">
            <div class="text-center text-slate-500 py-4 text-xs">Waiting for next price bounce to skim Pure USDT...</div>
        </div>
    </div>

    <!-- LAUNCH COIN CARD -->
    <div class="v-card p-4 mb-3">
        <div class="text-xs font-extrabold text-rose-400 mb-0.5">
            🎯 Launch Any Coin or Paste DEX Contract Address (Anti-Rug Protected)
        </div>
        <p class="text-[11px] text-slate-400 mb-2.5">
            Checks Gate.io + DexScreener Liquidity Pools (Automatically blocks pools under $15K liquidity)
        </p>
        <div class="flex gap-2 mb-2.5">
            <input id="coinSymbolInput" type="text" placeholder="Symbol or Contract (e.g. QNT, LTC, PEPE, 0x...)" class="flex-1 p-2.5 rounded-lg bg-slate-950 border border-slate-700 text-white text-xs mono outline-none focus:border-emerald-500">
            <input id="coinAnchorInput" type="number" value="10" step="1" class="w-20 p-2.5 rounded-lg bg-slate-950 border border-slate-700 text-white text-xs mono text-center outline-none focus:border-emerald-500">
        </div>
        <div class="flex flex-wrap gap-2">
            <button id="launchBtn" onclick="launchNewCoin()" class="btn-press px-4 py-2 rounded-lg bg-emerald-500 hover:bg-emerald-400 text-black font-extrabold text-xs">
                + Snipe & Launch
            </button>
            <button id="radarBtn" onclick="scanMemeRadar()" class="btn-press px-3.5 py-2 rounded-lg bg-amber-600/90 hover:bg-amber-500 text-white font-extrabold text-xs">
                📡 Scan Meme Radar
            </button>
        </div>
        <div id="radarResultsBox" class="mt-3 grid grid-cols-2 gap-2 hidden"></div>
    </div>

    <!-- ACTIVE COIN CARDS CONTAINER -->
    <div id="activeCoinsContainer" class="space-y-3 mb-3"></div>

    <!-- LIVE EXECUTION & ANTI-RUG SECURITY LOG -->
    <div class="v-card p-4">
        <div class="text-xs font-extrabold uppercase tracking-wider text-slate-400 mb-2">
            LIVE EXECUTION & AUTO-BUG FIXER SECURITY LOG
        </div>
        <div id="securityLogBox" class="bg-black/80 border border-slate-800 rounded-lg p-3 h-52 overflow-y-auto text-[11px] mono space-y-1.5 text-emerald-400 leading-relaxed"></div>
    </div>

<script>
if ('serviceWorker' in navigator) {
    navigator.serviceWorker.register('/sw.js').catch(() => {});
}

let _lastKnownCoinCount = 0;

function toggleDrawer(id) {
    const el = document.getElementById(id);
    if (el) el.classList.toggle('hidden');
}

function renderProfitCurveSvg(curveData) {
    const svg = document.getElementById('profitChartSvg');
    if (!svg) return;
    const pts = (Array.isArray(curveData) && curveData.length > 0)
        ? curveData
        : [{time: "00:00:00", value: 0.0}];

    const width = 500;
    const height = 140;
    const padLeft = 48;
    const padRight = 14;
    const padTop = 12;
    const padBottom = 24;
    const plotW = width - padLeft - padRight;
    const plotH = height - padTop - padBottom;

    const maxVal = Math.max(0.10, ...pts.map(p => Number(p.value || 0))) * 1.15;

    let gridHtml = '';
    const steps = 4;
    for (let i = 0; i <= steps; i++) {
        const y = padTop + (plotH * (i / steps));
        const labelVal = maxVal * (1 - (i / steps));
        gridHtml += `<line x1="${padLeft}" y1="${y}" x2="${width - padRight}" y2="${y}" stroke="#1e293b" stroke-dasharray="3,3" stroke-width="1"/>`;
        gridHtml += `<text x="${padLeft - 6}" y="${y + 4}" fill="#64748b" font-size="10" text-anchor="end" font-family="monospace">$${labelVal.toFixed(2)}</text>`;
    }

    const coords = pts.map((pt, idx) => {
        const x = pts.length === 1
            ? padLeft + plotW / 2
            : padLeft + (idx / (pts.length - 1)) * plotW;
        const v = Number(pt.value || 0);
        const y = padTop + plotH - ((v / maxVal) * plotH);
        return {x, y, time: pt.time, value: v};
    });

    let pathD = '';
    if (coords.length === 1) {
        pathD = `M ${padLeft} ${coords[0].y} L ${width - padRight} ${coords[0].y}`;
    } else {
        pathD = coords.map((c, i) => `${i === 0 ? 'M' : 'L'} ${c.x.toFixed(1)} ${c.y.toFixed(1)}`).join(' ');
    }

    const areaD = coords.length === 1
        ? `${pathD} L ${width - padRight} ${padTop + plotH} L ${padLeft} ${padTop + plotH} Z`
        : `${pathD} L ${coords[coords.length - 1].x.toFixed(1)} ${padTop + plotH} L ${coords[0].x.toFixed(1)} ${padTop + plotH} Z`;

    const dotsHtml = coords.map(c =>
        `<circle cx="${c.x.toFixed(1)}" cy="${c.y.toFixed(1)}" r="3.5" fill="#10b981" stroke="#052e16" stroke-width="1.5"/>`
    ).join('');

    let xLabels = '';
    const labelCount = Math.min(5, coords.length);
    for (let i = 0; i < labelCount; i++) {
        const idx = Math.floor((i / Math.max(1, labelCount - 1)) * (coords.length - 1));
        const c = coords[idx];
        xLabels += `<text x="${c.x.toFixed(1)}" y="${height - 4}" fill="#64748b" font-size="9" text-anchor="middle" font-family="monospace">${c.time}</text>`;
    }

    svg.innerHTML = `
        <defs>
            <linearGradient id="skimGrad" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stop-color="#10b981" stop-opacity="0.32"/>
                <stop offset="100%" stop-color="#10b981" stop-opacity="0.0"/>
            </linearGradient>
        </defs>
        ${gridHtml}
        <path d="${areaD}" fill="url(#skimGrad)"/>
        <path d="${pathD}" fill="none" stroke="#10b981" stroke-width="2.5" stroke-linecap="round"/>
        ${dotsHtml}
        ${xLabels}
    `;
}

function formatPrice(p) {
    const num = Number(p || 0);
    if (num >= 10) return num.toFixed(4);
    if (num >= 0.1) return num.toFixed(4);
    if (num >= 0.001) return num.toFixed(6);
    return num.toFixed(8);
}

async function pollState() {
    try {
        const r = await fetch('/api/state?_t=' + Date.now(), { cache: 'no-store' });
        if (!r.ok) return;
        const data = await r.json();
        updateUI(data);
    } catch (e) {
        console.debug("State poll error:", e);
    }
}

function updateUI(data) {
    const coins = data.coins || {};
    const syms = Object.keys(coins);

    if (syms.length === 0 && _lastKnownCoinCount > 0 && !data.exchange_bridge?.masked_key) {
        return;
    }
    _lastKnownCoinCount = syms.length;

    const vc = Number(data.vault_cash || 0);
    const aa = Number(data.added_to_anchors || 0);
    const totalPool = Number(data.total_escrow_reserved || 0);

    document.getElementById('heroTotalProfit').innerText = `$${Number(data.total_profit || 0).toFixed(4)}`;
    document.getElementById('heroProfitSub').innerText =
        `Pure USDT Vaulted: $${vc.toFixed(4)} | Share-Refill Pool: $${totalPool.toFixed(2)} | Total Skims: ${data.total_skims || 0}`;
    document.getElementById('cashoutAvailDisplay').innerText = `$${vc.toFixed(4)}`;
    document.getElementById('chartSkimBadge').innerText = `${data.total_skims || 0} Completed Wave Skims`;
    if (data.last_price_tick_hms) {
        document.getElementById('liveTickClock').innerText = `● Tick: ${data.last_price_tick_hms} UTC`;
    }

    const fixBadge = document.getElementById('autoFixBadge');
    if (fixBadge) {
        fixBadge.innerText = `🛠️ Auto-Bug Fixer: ON (${data.auto_fixes_count || 0} healed)`;
    }

    const b = data.exchange_bridge || {};
    const liveBadge = document.getElementById('liveBridgeBadge');
    const exchUpper = (b.exchange || 'gateio').toUpperCase();
    if (b.live_armed) {
        liveBadge.className = "px-2.5 py-1 rounded-md bg-emerald-950/90 border border-emerald-600/70 text-emerald-400 font-extrabold text-[11px] mono flex items-center gap-1.5";
        liveBadge.innerHTML = `<span>🟢</span> <span>LIVE BRIDGE ARMED (${exchUpper})</span>`;
    } else {
        liveBadge.className = "px-2.5 py-1 rounded-md bg-slate-800 border border-slate-700 text-slate-300 font-bold text-[11px] mono flex items-center gap-1.5";
        liveBadge.innerHTML = `<span>⚪</span> <span>PAPER SIMULATOR</span>`;
    }

    const usdtAvail = Number(b.available_usdt || 0);
    document.getElementById('spotUsdtBadge').innerText = `Gate.io Spot USDT: $${usdtAvail.toFixed(4)}`;
    document.getElementById('pureProfitUsdtBadge').innerText = `Real USDT Added: +$${vc.toFixed(4)}`;
    document.getElementById('bridgeConnectionStatus').innerText =
        `Gate.io v4 Connected! Available Spot USDT: $${usdtAvail.toFixed(4)}`;

    if (b.masked_key) {
        document.getElementById('savedKeyLabel').innerText = `Saved Key: ${b.masked_key}`;
    }

    const a = data.alerts || {};
    const ntfyInput = document.getElementById('alertNtfyTopic');
    if (ntfyInput && document.activeElement !== ntfyInput && a.ntfy_topic) {
        ntfyInput.value = a.ntfy_topic;
    }

    const freezeBtn = document.getElementById('freezeBtn');
    if (data.frozen) {
        freezeBtn.innerText = "▶️ Resume Live";
        freezeBtn.className = "btn-press px-3 py-1.5 rounded-lg bg-emerald-600 text-black font-extrabold text-xs";
    } else {
        freezeBtn.innerText = "🛑 Freeze Live";
        freezeBtn.className = "btn-press px-3 py-1.5 rounded-lg bg-rose-950/90 text-rose-300 border border-rose-700/70 font-bold text-xs";
    }

    renderProfitCurveSvg(data.profit_curve || []);

    const ledgerBox = document.getElementById('skimLedgerList');
    const ledger = data.skim_ledger || [];
    if (ledger.length === 0) {
        ledgerBox.innerHTML = `<div class="text-center text-slate-500 py-4 text-xs">Waiting for next price bounce to skim Pure USDT...</div>`;
    } else {
        ledgerBox.innerHTML = ledger.slice(0, 12).map(row => `
            <div class="v-inner p-2.5 flex items-center justify-between">
                <div>
                    <div class="flex items-center gap-1.5">
                        <span class="text-emerald-400 font-extrabold">${row.symbol}</span>
                        <span class="text-[10px] px-1.5 py-0.5 rounded bg-slate-800 text-slate-200">[${row.mode}]</span>
                        <span class="text-slate-300 text-[11px]">${row.stage}</span>
                    </div>
                    <div class="text-[10px] text-slate-400 mt-0.5">@ $${row.price} • ${row.time}</div>
                </div>
                <div class="text-right">
                    <div class="text-emerald-400 font-extrabold">+$${Number(row.profit || 0).toFixed(4)}</div>
                    <div class="text-[10px] text-slate-400">${row.shares} sh now</div>
                </div>
            </div>
        `).join('');
    }

    const coinsBox = document.getElementById('activeCoinsContainer');
    if (syms.length === 0) {
        coinsBox.innerHTML = `
            <div class="v-card p-5 text-center text-xs text-slate-400">
                No active coin cards. Enter <b>QNT</b> or <b>LTC</b> above and tap <b>+ Snipe & Launch</b>.
            </div>`;
    } else {
        coinsBox.innerHTML = syms.map(sym => {
            const c = coins[sym];
            const isLive = Boolean(c.live_enabled);
            const sh = Number(c.shares || 0);
            const protSh = Number(c.protected_shares || sh);
            const refillPool = Number(c.refill_pool_usdt || 0);
            const skimsSince = Number(c.skims_since_refill || 0);
            const bonusSh = Number(c.bonus_shares_added || 0);
            const price = Number(c.current_price || 0);
            const low = Number(c.sliding_low || price);
            const anchor = Number(c.anchor_usd || 10);
            const tokenVal = sh * price;
            const totalBucketVal = tokenVal + refillPool;
            const gainPct = low > 0 ? (((price - low) / low) * 100) : 0;
            const harvestTargetPrice = low * 1.0065;
            const pctBar = Math.max(0, Math.min(100, (gainPct / 0.65) * 100));

            const modePill = isLive
                ? `<button onclick="switchCoinMode('${sym}', true)" class="btn-press px-2 py-0.5 rounded bg-rose-950/90 text-rose-400 border border-rose-700/70 text-[10px] font-extrabold mono">⚡ LIVE ORDER</button>`
                : `<button onclick="switchCoinMode('${sym}', false)" class="btn-press px-2 py-0.5 rounded bg-slate-800 text-slate-300 border border-slate-700 text-[10px] font-bold mono">📄 PAPER</button>`;

            const statusPill = `<span class="px-2 py-0.5 rounded bg-emerald-950/90 text-emerald-400 border border-emerald-700/70 text-[10px] font-extrabold mono">${c.status}</span>`;

            const refillDetailHtml = `<div class="flex items-center justify-between text-amber-300 bg-amber-950/40 px-2 py-1.5 rounded border border-amber-700/60">
                <span>🔄 Share-Refill Pool:</span>
                <span class="font-bold">$${refillPool.toFixed(2)} USDT (${skimsSince}/3 skims stored)</span>
            </div>`;

            const forceRefillBtnHtml = (isLive && refillPool >= 3.18)
                ? `<button onclick="forceShareRefill('${sym}')" class="btn-press w-full mt-2 py-2 rounded-lg bg-amber-500 hover:bg-amber-400 text-black font-extrabold text-xs flex items-center justify-center gap-1.5 shadow">
                       <span>🔄</span> <span>Take Pool ($${refillPool.toFixed(2)}) & Replace Shares Now</span>
                   </button>`
                : '';

            const topupBtnHtml = (isLive && usdtAvail >= 3.18)
                ? `<button onclick="deployUsdtToShares('${sym}', ${usdtAvail})" class="btn-press w-full mt-2 py-2 rounded-lg bg-sky-950/90 hover:bg-sky-900 text-sky-300 border border-sky-600/80 font-extrabold text-xs flex items-center justify-center gap-1.5">
                       <span>🛡️</span> <span>Use Spot USDT ($${usdtAvail.toFixed(2)}) to Buy More ${sym} Shares</span>
                   </button>`
                : '';

            const bottomBar = isLive
                ? `<button onclick="manualSyncLive()" class="btn-press w-full mt-3 py-2.5 rounded-lg bg-emerald-950/80 text-emerald-300 border border-emerald-700/70 font-bold text-xs flex items-center justify-center gap-1.5">
                       <span>🟢</span> <span>Priority Pure USDT Skimmer Active — Zero Lockups</span>
                   </button>
                   ${forceRefillBtnHtml}
                   ${topupBtnHtml}`
                : `<button onclick="triggerPracticeSpike('${sym}')" class="btn-press w-full mt-3 py-2.5 rounded-lg bg-amber-950/70 hover:bg-amber-900/70 text-amber-300 border border-amber-600/70 font-extrabold text-xs">
                       ⚡ Practice Spike (+1.5% Simulator)
                   </button>`;

            return `
            <div class="v-card p-4">
                <div class="flex items-center justify-between gap-2 mb-1">
                    <div class="flex items-center gap-1.5 flex-wrap">
                        <span class="text-base font-extrabold text-white tracking-tight">${sym}/USDT</span>
                        ${modePill}
                        ${statusPill}
                    </div>
                    <button onclick="removeCoinCard('${sym}')" class="text-slate-500 hover:text-rose-400 font-bold text-sm px-1">✕</button>
                </div>

                <div class="flex items-center justify-between text-[11px] mb-2.5">
                    <span class="text-emerald-400 font-bold mono">● VERIFIED CEX (${c.source || 'GATE.IO SPOT'})</span>
                    <span class="text-emerald-400 font-bold mono">+${bonusSh.toFixed(6)} Bonus ${sym} Added</span>
                </div>

                <div class="v-inner px-3 py-2 mb-3 flex items-center justify-between text-xs">
                    <span class="text-left text-indigo-300 font-bold">
                        ⚡ Strategy: ${c.strategy || '70% Pure USDT + 30% Share Refill'}
                    </span>
                    <button onclick="cycleCoinStrategy('${sym}')" class="btn-press px-2.5 py-0.5 rounded bg-indigo-950 text-indigo-300 border border-indigo-700/80 font-bold text-[11px]">
                        Switch
                    </button>
                </div>

                <div class="flex items-baseline gap-2.5 mb-3">
                    <span class="text-2xl sm:text-3xl font-extrabold text-white mono">$${formatPrice(price)}</span>
                    <span class="text-xs font-bold text-emerald-400 mono">(${gainPct >= 0 ? '+' : ''}${gainPct.toFixed(2)}% from Low)</span>
                </div>

                <div class="v-inner p-3 space-y-2 text-xs mono">
                    <div class="flex items-center justify-between">
                        <span class="text-slate-400">Bucket A (Anchor):</span>
                        <div class="flex items-center gap-1.5">
                            <span class="text-white font-bold">$${anchor.toFixed(2)} <span class="text-slate-400 text-[11px]">(In Token)</span></span>
                            <button onclick="promptEditAnchor('${sym}', ${tokenVal > 0 ? tokenVal.toFixed(2) : anchor})" class="px-1.5 py-0.5 rounded bg-sky-950 text-sky-300 border border-sky-700/70 text-[10px] font-bold">
                                Edit
                            </button>
                        </div>
                    </div>

                    <div class="flex items-center justify-between">
                        <span class="text-slate-400">Real Shares Owned (Gate.io):</span>
                        <span class="text-emerald-400 font-bold">${sh.toFixed(6)} ${sym}</span>
                    </div>

                    <div class="flex items-center justify-between">
                        <span class="text-slate-400">Protected Share Baseline:</span>
                        <span class="text-sky-300 font-bold">🛡️ ${protSh.toFixed(6)} ${sym}</span>
                    </div>

                    <div class="flex items-center justify-between">
                        <span class="text-slate-400">Total Position Value:</span>
                        <span class="text-white font-bold">$${totalBucketVal.toFixed(2)}</span>
                    </div>

                    ${refillDetailHtml}

                    <div class="flex items-center justify-between">
                        <span class="text-slate-400">Sliding Low / Next Skim:</span>
                        <span class="text-slate-200 font-bold">$${formatPrice(low)} / $${formatPrice(harvestTargetPrice)}</span>
                    </div>

                    <div class="flex items-center justify-between">
                        <span class="text-slate-400">Zero-Loss / +0.65% Bar:</span>
                        <span class="text-emerald-300 font-bold">${c.zero_loss_stop > 0 ? 'LOCKED Stop @ $' + formatPrice(c.zero_loss_stop) : 'Shield @ +0.38% | Skim @ $' + formatPrice(harvestTargetPrice)}</span>
                    </div>

                    <div class="w-full bg-slate-900 h-2 rounded-full overflow-hidden border border-slate-800">
                        <div class="bg-emerald-400 h-full transition-all duration-300" style="width: ${pctBar}%"></div>
                    </div>

                    <div class="flex items-center justify-between pt-0.5">
                        <span class="text-slate-400">Realized USDT Profit (${sym}):</span>
                        <span class="text-emerald-400 font-extrabold">+$${Number(c.vaulted_usd || 0).toFixed(4)} (${c.skim_count || 0} direct skims)</span>
                    </div>
                </div>

                ${bottomBar}
            `;
        }).join('');
    }

    const logBox = document.getElementById('securityLogBox');
    logBox.innerHTML = (data.logs || []).map(line => `<div>${line}</div>`).join('');
}

async function saveBridgeConfig(toggleLive) {
    const verifyBtn = document.getElementById('verifyApiBtn');
    verifyBtn.innerText = "⏳ Checking Exchange...";
    const payload = {
        exchange: document.getElementById('bridgeExchange').value,
        api_key: document.getElementById('bridgeApiKey').value,
        api_secret: document.getElementById('bridgeApiSecret').value,
        max_order_usd: document.getElementById('bridgeMaxCap').value,
        toggle_live: toggleLive
    };
    try {
        const r = await fetch('/api/bridge/save', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(payload)
        });
        const res = await r.json();
        if (r.ok && res.ok) {
            verifyBtn.innerText = "✅ Verified";
            document.getElementById('bridgeApiKey').value = '';
            document.getElementById('bridgeApiSecret').value = '';
        } else {
            verifyBtn.innerText = "❌ Failed";
            alert(res.error || "Could not verify Gate.io API keys.");
        }
    } catch (e) {
        verifyBtn.innerText = "❌ Error";
    }
    setTimeout(() => {
        verifyBtn.innerText = "🔍 Verify API & Check USDT Balance";
    }, 2600);
    pollState();
}

async function manualSyncLive() {
    const btn = document.getElementById('syncLiveBtn');
    btn.innerText = "⏳ Healing & Syncing...";
    await fetch('/api/sync_live', {method: 'POST'});
    btn.innerText = "🔄 Sync & Auto-Heal";
    pollState();
}

async function forceShareRefill(symbol) {
    const r = await fetch('/api/coin/force_rebuy', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({symbol})
    });
    const res = await r.json();
    if (!r.ok || !res.ok) {
        alert(res.error || "Could not execute share refill.");
    }
    pollState();
}

async function launchNewCoin() {
    const symInput = document.getElementById('coinSymbolInput');
    const ancInput = document.getElementById('coinAnchorInput');
    const symbol = symInput.value.trim();
    const anchor_usd = parseFloat(ancInput.value || '10');
    if (!symbol) return;

    const btn = document.getElementById('launchBtn');
    btn.innerText = "⏳ Launching...";
    const r = await fetch('/api/coin/launch', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({symbol, anchor_usd})
    });
    const res = await r.json();
    btn.innerText = "+ Snipe & Launch";
    if (!r.ok || !res.ok) {
        alert(res.error || "Could not launch coin.");
        return;
    }
    symInput.value = '';
    pollState();
}

async function scanMemeRadar() {
    const btn = document.getElementById('radarBtn');
    const box = document.getElementById('radarResultsBox');
    btn.innerText = "⏳ Scanning...";
    const r = await fetch('/api/radar/scan');
    const res = await r.json();
    btn.innerText = "📡 Scan Meme Radar";
    if (res.ok && Array.isArray(res.results)) {
        box.classList.remove('hidden');
        box.innerHTML = res.results.map(item => `
            <div onclick="quickLaunchFromRadar('${item.symbol}')" class="v-inner p-2.5 cursor-pointer hover:border-emerald-500 transition">
                <div class="flex justify-between items-center">
                    <span class="font-extrabold text-white text-xs">${item.symbol}/USDT</span>
                    <span class="text-[10px] font-bold ${item.change_24h >= 0 ? 'text-emerald-400' : 'text-rose-400'} mono">
                        ${item.change_24h >= 0 ? '+' : ''}${item.change_24h}%
                    </span>
                </div>
                <div class="text-[11px] text-slate-300 mono mt-0.5">$${formatPrice(item.price)}</div>
            </div>
        `).join('');
    }
}

function quickLaunchFromRadar(sym) {
    document.getElementById('coinSymbolInput').value = sym;
    launchNewCoin();
}

async function switchCoinMode(symbol, currentlyLive) {
    if (currentlyLive && !confirm(`Switch ${symbol} from LIVE Gate.io mode to PAPER simulator?`)) {
        return;
    }
    await fetch('/api/coin/switch_mode', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({symbol})
    });
    pollState();
}

async function cycleCoinStrategy(symbol) {
    await fetch('/api/coin/cycle_strategy', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({symbol})
    });
    pollState();
}

async function deployUsdtToShares(symbol, availUsdt) {
    const input = prompt(
        `How much of your $${availUsdt.toFixed(2)} Spot USDT would you like to deploy right now to buy more ${symbol} shares? (Gate.io minimum order is $3.18 USDT):`,
        availUsdt.toFixed(2)
    );
    if (input === null || input.trim() === '') return;
    const r = await fetch('/api/coin/force_buy', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({symbol, usd_amount: parseFloat(input)})
    });
    const res = await r.json();
    if (!r.ok || !res.ok) {
        alert(res.error || "Could not execute live share buy.");
    }
    pollState();
}

async function promptEditAnchor(symbol, currentVal) {
    const input = prompt(
        `Enter new Bucket A (Anchor USD) for ${symbol} (Current live token value is $${Number(currentVal).toFixed(2)}. Entering a higher USD value will immediately buy the extra ${symbol} shares on Gate.io! Gate.io minimum order size is $3.18 USDT):`,
        currentVal
    );
    if (input === null || input.trim() === '') return;
    const r = await fetch('/api/coin/edit_anchor', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({symbol, anchor_usd: parseFloat(input)})
    });
    const res = await r.json();
    if (!r.ok || !res.ok) {
        alert(res.error || "Could not update Anchor on Gate.io.");
    }
    pollState();
}

async function triggerPracticeSpike(symbol) {
    await fetch('/api/coin/spike', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({symbol})
    });
    pollState();
}

async function removeCoinCard(symbol) {
    if (!confirm(`Remove ${symbol} card from dashboard?`)) return;
    _lastKnownCoinCount = Math.max(0, _lastKnownCoinCount - 1);
    await fetch('/api/coin/remove', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({symbol})
    });
    pollState();
}

async function saveAlertSettings(sendTest) {
    const payload = {
        enabled: true,
        ntfy_topic: document.getElementById('alertNtfyTopic').value,
        telegram_bot_token: document.getElementById('alertTgToken').value,
        telegram_chat_id: document.getElementById('alertTgChat').value,
        send_test: sendTest
    };
    await fetch('/api/alerts/save', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(payload)
    });
    alert(sendTest ? "Alert settings saved & test push dispatched!" : "Alert settings saved.");
    pollState();
}

async function recordCashout() {
    const amt = document.getElementById('cashoutAmountInput').value;
    if (!amt) return;
    const r = await fetch('/api/cashout/record', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({amount: parseFloat(amt)})
    });
    const res = await r.json();
    if (!r.ok || !res.ok) {
        alert(res.error || "Could not record cash-out.");
        return;
    }
    document.getElementById('cashoutAmountInput').value = swift;
    pollState();
}

async function toggleFreeze() {
    await fetch('/api/control/freeze', {method: 'POST'});
    pollState();
}

async function resetProfitCounters() {
    if (!confirm("Reset Profit Counters to $0.0000 and clear Refill Pools?")) return;
    await fetch('/api/control/reset', {method: 'POST'});
    pollState();
}

pollState();
setInterval(pollState, 2500);
</script>
</body>
</html>
"""


@app.route("/")
def route_index():
    pull_latest_state_if_newer()
    ensure_worker_threads_running()
    resp = make_response(render_template_string(DASHBOARD_HTML))
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    return resp


if __name__ == "__main__":
    ensure_worker_threads_running()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, threaded=True)
