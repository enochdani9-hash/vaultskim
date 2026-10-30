# ==============================================================================
# VAULTSKIM QUANTITATIVE ENGINE — v8.0 WIN-BOTH-WAYS & PEAK COMMAND CENTER UI
# PART 1 OF 2: CORE QUANT ENGINE, GATE.IO V4 API & WALLET INTEGRITY GUARD
# ==============================================================================
import os
import sys
import time
import json
import hmac
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
logger = logging.getLogger("VaultSkimV80")

app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False

# ==============================================================================
# 1. QUANTITATIVE PARAMETERS & PERSISTENCE CONSTANTS
# ==============================================================================
STATE_FILE = os.environ.get("VAULTSKIM_STATE_FILE", "vaultskim_state_v80.json")
LEGACY_FILES = (
    "vaultskim_state_v71.json",
    "vaultskim_state_v70.json",
    "vaultskim_state_v60.json",
    "vaultskim_state_v510.json"
)
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
RENDER_EXTERNAL_URL = os.environ.get("RENDER_EXTERNAL_URL", "").strip()

GATEIO_MIN_ORDER_USD = 3.20
SELL_BOUNCE_TRIGGER_PCT = 0.75     # +0.75% from sliding low -> Sell 25% slice
SHIELD_ARM_TRIGGER_PCT = 0.42      # +0.42% from sliding low -> Arm trailing shield
DIP_REBUY_TARGET_PCT = 0.40        # -0.40% or lower from sell price -> Dip Re-Buy
UPWARD_SURPLUS_GUARD_PCT = 0.35    # +0.35% or higher from sell price -> Surplus Re-Buy
WORKING_SLICE_RATIO = 0.25         # 25% working slice, 75% untouched Moonbag

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
_last_watchdog_ts = 0.0

DEFAULT_STATE = {
    "version": "v8.0 - Peak Quant Win-Both-Ways",
    "state_updated_ts": 0.0,
    "vault_cash": 0.0,
    "added_to_anchors": 0.0,
    "total_skims": 0,
    "total_withdrawn": 0.0,
    "auto_heals": 0,
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
        f"VaultSkim v8.0 Quant Engine Online: +0.75% Sell Slice | -0.40% Dip Re-Buy | +0.35% Upward Surplus Guard."
    ],
    "exchange_bridge": {
        "exchange": "gateio",
        "api_key": "",
        "api_secret": "",
        "max_order_usd": 250.0,
        "live_armed": False,
        "verified": False,
        "available_usdt": 0.0,
        "locked_usdt": 0.0,
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
# 2. MULTI-WORKER DATABASE & STATE PERSISTENCE LAYER
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

    merged["version"] = "v8.0 - Peak Quant Win-Both-Ways"
    merged.setdefault("auto_heals", 0)
    merged["exchange_bridge"].setdefault("locked_usdt", 0.0)

    cleaned_ledger = []
    recalc_vault = 0.0
    recalc_added = 0.0
    for row in merged.get("skim_ledger", []):
        v_cut = float(row.get("vault_cut") or 0.0)
        t_cut = float(row.get("topup_cut") or 0.0)
        if v_cut > 5.0:
            v_cut = round(min(v_cut, 0.35), 4)
            row["vault_cut"] = v_cut
            row["profit"] = round(v_cut + t_cut, 4)
        cleaned_ledger.append(row)
        recalc_vault += v_cut
        recalc_added += t_cut

    if cleaned_ledger:
        merged["skim_ledger"] = cleaned_ledger
        merged["vault_cash"] = round(recalc_vault, 4)
        merged["added_to_anchors"] = round(recalc_added, 4)
        merged["exchange_bridge"]["realized_usdt_profit"] = round(recalc_vault, 4)

    for sym, c in merged.get("coins", {}).items():
        sh = float(c.get("shares") or 0.0)
        px = float(c.get("current_price") or 0.0)
        prot_sh = float(c.get("protected_shares") or 0.0)
        c["protected_shares"] = sh if prot_sh <= 0 else max(prot_sh, sh)
        if c.get("strategy") not in ("50/50 Moonbag + Cash Skim", "100% Pure Cash Skim"):
            c["strategy"] = "50/50 Moonbag + Cash Skim"

        c.setdefault("locked_shares", 0.0)
        c.setdefault("trade_usdt_proceeds", 0.0)
        c.setdefault("trade_sold_shares", 0.0)
        c.setdefault("trade_sell_price", 0.0)
        c.setdefault("trade_rebuy_price", 0.0)
        c.setdefault("trade_upward_rebuy_price", 0.0)
        c.setdefault("trade_started_ts", time.time())
        c.setdefault("bonus_shares_added", 0.0)
        c.setdefault("zero_loss_stop", 0.0)
        c.setdefault("share_topup_usd", 0.0)
        c.setdefault("vaulted_usd", 0.0)
        c.setdefault("skim_count", 0)

        if float(c.get("vaulted_usd") or 0.0) > 5.0 and int(c.get("skim_count") or 0) <= 5:
            c["vaulted_usd"] = round(min(float(c.get("vaulted_usd") or 0.0), 0.45), 4)

        sell_px = float(c.get("trade_sell_price") or px)
        if sell_px > 0 and float(c.get("trade_upward_rebuy_price") or 0.0) <= 0:
            c["trade_upward_rebuy_price"] = round(sell_px * (1.0 + UPWARD_SURPLUS_GUARD_PCT / 100.0), 8)
        if sell_px > 0 and float(c.get("trade_rebuy_price") or 0.0) <= 0:
            c["trade_rebuy_price"] = round(sell_px * (1.0 - DIP_REBUY_TARGET_PCT / 100.0), 8)

        if c.get("status") not in ("HOLDING_ANCHOR", "RATCHET_ARMED", "WAITING_DIP_REBUY", "WAITING_LIVE_BUY"):
            c["status"] = "HOLDING_ANCHOR"

        if sh > 0 and px > 0:
            c["anchor_usd"] = round(sh * px, 2)

    return merged


def load_persistent_state():
    if DATABASE_URL and HAS_PSYCOPG2:
        for key_name in (
            "master_state_v80",
            "master_state_v71",
            "master_state_v70",
            "master_state_v60"
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
                    ("master_state_v80",)
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
                        "master_state_v80",
                        "master_state_v71",
                        "master_state_v70"
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
# 3. LOGGING & PUSH ALERTS
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
        "User-Agent": f"VaultSkimQuant/8.0 ({int(now_ts)})"
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
# 6. WALLET INTEGRITY GUARD, OPEN ORDER SWEEPER & SPOT EXECUTION BRIDGE
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


def cancel_all_open_spot_orders(symbol=None):
    """
    Cancels open Gate.io Spot limit orders that are freezing USDT or token balances
    so locked funds immediately return to Available Spot Balance.
    """
    with state_lock:
        bridge = dict(STATE.get("exchange_bridge", {}))
        active_syms = [symbol.upper().strip()] if symbol else list(STATE.get("coins", {}).keys())
    if not active_syms:
        active_syms = ["QNT", "LTC"]

    api_key = (bridge.get("api_key") or "").strip()
    api_secret = (bridge.get("api_secret") or "").strip()
    if not api_key or not api_secret:
        return False, "Missing Spot API Key or Secret."

    canceled_total = 0
    url_path = "/api/v4/spot/orders"
    for sym in active_syms:
        query_str = f"currency_pair={sym}_USDT&account=spot"
        headers = gateio_v4_sign(api_key, api_secret, "DELETE", url_path, query_str, "")
        try:
            r = requests.delete(f"https://api.gateio.ws{url_path}?{query_str}", headers=headers, timeout=8)
            if r.status_code in (200, 201):
                arr = r.json()
                if isinstance(arr, list):
                    canceled_total += len(arr)
        except Exception:
            pass

    time.sleep(0.8)
    sync_all_live_cards_with_gateio(auto_buy_waiting=False, verbose=True)
    msg = f"Canceled {canceled_total} open Spot order(s) and unlocked frozen funds."
    add_log(f"🧹 {msg}")
    return True, msg


def execute_live_exchange_order(symbol, side, usd_amount=None, share_amount=None, min_required_shares=0.0):
    """
    STRICT WALLET INTEGRITY GUARD:
    - Never executes a partial re-buy if `usdt_avail` is less than what is needed
      to restore `min_required_shares` (prevents Idle Earn or Open Orders from causing share loss!).
    - Uses IOC (Immediate-Or-Cancel) so orders never sit open and freeze your USDT!
    """
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
    usdt_locked = balances.get("USDT", {}).get("locked", 0.0)
    coin_avail = balances.get(sym, {}).get("available", 0.0)

    with state_lock:
        STATE["exchange_bridge"]["available_usdt"] = round(usdt_avail, 4)
        STATE["exchange_bridge"]["locked_usdt"] = round(usdt_locked, 4)

    rules = get_exchange_pair_rules(sym)
    min_order_usd = max(float(rules.get("min_quote_amount") or 3.20), GATEIO_MIN_ORDER_USD)
    side_lower = side.lower()

    if side_lower == "buy":
        exec_price = quote["ask"] * 1.0005
        price_str = quantize_value(exec_price, rules["price_precision"], ROUND_HALF_UP)
        exec_price_float = float(price_str)

        if share_amount is not None and float(share_amount) > 0:
            raw_shares = float(share_amount) / 0.9988
            required_usdt = raw_shares * exec_price_float
        else:
            requested_usd = min(float(usd_amount or min_order_usd), max_cap)
            if requested_usd < min_order_usd:
                requested_usd = min_order_usd * 1.01
            required_usdt = requested_usd
            raw_shares = (requested_usd * 0.9985) / exec_price_float

        # STRICT ZERO-SHARE-LOSS CHECK: Never allow partial re-buys if USDT is locked in Earn/Orders!
        if min_required_shares > 0:
            min_cost_needed = (float(min_required_shares) / 0.9988) * exec_price_float
            if usdt_avail < min_cost_needed:
                warn_msg = (
                    f"WALLET GUARD BLOCKED PARTIAL RE-BUY ({sym}): Need ${min_cost_needed:.2f} USDT to restore "
                    f"100% of {min_required_shares:.6f} {sym}, but Spot Available USDT is only ${usdt_avail:.2f} "
                    f"(Locked/Earn USDT: ${usdt_locked:.2f}). Redeem Idle Earn or tap 'Cancel Open Orders'!"
                )
                add_log(f"🛡️ {warn_msg}")
                return False, warn_msg, None

        if usdt_avail < required_usdt:
            if min_required_shares <= 0 and usdt_avail >= min_order_usd:
                raw_shares = (usdt_avail * 0.996) / exec_price_float
            else:
                return False, (
                    f"Insufficient Available Spot USDT (${usdt_avail:.2f} available, ${usdt_locked:.2f} locked). "
                    f"Required: ${required_usdt:.2f}."
                ), None

        amount_str = quantize_value(raw_shares, rules["amount_precision"], ROUND_UP)
        amount_float = float(amount_str)

        if (amount_float * exec_price_float) > (usdt_avail * 0.9985):
            fitted = (usdt_avail * 0.997) / exec_price_float
            amount_str = quantize_value(fitted, rules["amount_precision"], ROUND_DOWN)
            amount_float = float(amount_str)

        if min_required_shares > 0 and (amount_float * 0.999) < (min_required_shares * 0.998):
            return False, f"Blocked partial re-buy: {amount_float:.6f} {sym} is below {min_required_shares:.6f} {sym}.", None

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
                return False, f"Bag (${coin_avail * exec_price_float:.2f}) under Gate.io $3.20 minimum.", None

        amount_str = quantize_value(raw_shares, rules["amount_precision"], ROUND_DOWN)
        amount_float = float(amount_str)

        if amount_float <= 0 or (amount_float * exec_price_float) < 3.00:
            return False, f"Sell order (${amount_float * exec_price_float:.2f}) below Gate.io $3.00 minimum.", None

    if exch == "gateio":
        url_path = "/api/v4/spot/orders"
        # Always use IOC ("Immediate-Or-Cancel") so unfilled orders NEVER sit open and freeze your USDT!
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
                    return False, f"IOC Order did not fill immediately at ${price_str} (Zero funds locked).", None

                order_id = resp_data.get("id", "OK")
                actual_filled_sh = filled_amt
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
    usdt_locked = balances.get("USDT", {}).get("locked", 0.0)
    with state_lock:
        STATE["exchange_bridge"]["available_usdt"] = round(usdt_avail, 4)
        STATE["exchange_bridge"]["locked_usdt"] = round(usdt_locked, 4)
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
            current_status = card.get("status", "HOLDING_ANCHOR")
            proceeds = float(card.get("trade_usdt_proceeds") or 0.0)

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

            with state_lock:
                card["locked_shares"] = round(locked_shares, 6)

            if current_status == "WAITING_DIP_REBUY" and proceeds > 0:
                with state_lock:
                    card["shares"] = real_shares if real_shares > 0 else total_real_shares
                    card["anchor_usd"] = round(card["shares"] * price, 2)
                sync_notes.append(f"{sym}: Win-Both-Ways Re-Buy Armed (${proceeds:.2f})")
                continue

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
                    if card.get("status") == "WAITING_LIVE_BUY":
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
                    add_log(f"🚀 AUTO-BUYING INITIAL POSITION ({sym}): ${buy_usd:.2f} on Gate.io...")
                    ok, msg, _ = execute_live_exchange_order(sym, "buy", usd_amount=buy_usd)
                    if ok:
                        time.sleep(1.0)
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
        locked_note = f" (Locked: ${usdt_locked:.2f})" if usdt_locked > 0.05 else ""
        add_log(f"REAL GATE.IO WALLET SYNCED: Available USDT = ${usdt_avail:.4f}{locked_note} | {summary_str}")
    save_persistent_state()
    return True, usdt_avail


# ==============================================================================
# 7. QUANTITATIVE WIN-BOTH-WAYS VOLATILITY SKIMMER ENGINE
# ==============================================================================
def run_auto_healing_watchdog():
    global _last_watchdog_ts
    now_ts = time.time()
    if (now_ts - _last_watchdog_ts) < 8.0:
        return
    _last_watchdog_ts = now_ts

    with state_lock:
        for sym, c in list(STATE.get("coins", {}).items()):
            sh = float(c.get("shares") or 0.0)
            prot_sh = float(c.get("protected_shares") or 0.0)
            px = float(c.get("current_price") or 0.0)
            proceeds = float(c.get("trade_usdt_proceeds") or 0.0)
            sold_sh = float(c.get("trade_sold_shares") or 0.0)
            sell_px = float(c.get("trade_sell_price") or 0.0)

            if sh > prot_sh:
                c["protected_shares"] = sh

            if sh > 0 and px > 0:
                c["anchor_usd"] = round(sh * px, 2)

            if c.get("status") == "WAITING_DIP_REBUY":
                if proceeds <= 0.01 or sold_sh <= 0.000001:
                    c["trade_usdt_proceeds"] = 0.0
                    c["trade_sold_shares"] = 0.0
                    c["status"] = "HOLDING_ANCHOR"
                    STATE["auto_heals"] = int(STATE.get("auto_heals") or 0) + 1
                    add_log(f"🛠️ [AUTO-HEAL] Cleared empty re-buy state on {sym}.")
                elif sell_px > 0:
                    if float(c.get("trade_upward_rebuy_price") or 0.0) <= 0:
                        c["trade_upward_rebuy_price"] = round(sell_px * (1.0 + UPWARD_SURPLUS_GUARD_PCT / 100.0), 8)
                    if float(c.get("trade_rebuy_price") or 0.0) <= 0:
                        c["trade_rebuy_price"] = round(sell_px * (1.0 - DIP_REBUY_TARGET_PCT / 100.0), 8)


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
    ask_price = float(quote.get("ask") or price)

    with state_lock:
        card = STATE["coins"].get(sym)
        if not card:
            return
        card["current_price"] = price
        card["source"] = quote["source"]
        STATE["last_price_tick_hms"] = now_hms()

        status = card.get("status", "HOLDING_ANCHOR")
        shares = float(card.get("shares") or 0.0)
        anchor_usd = float(card.get("anchor_usd") or 10.0)
        proceeds = float(card.get("trade_usdt_proceeds") or 0.0)
        sold_sh = float(card.get("trade_sold_shares") or 0.0)

        if shares > 0:
            card["anchor_usd"] = round(shares * price, 2)

        # ----------------------------------------------------------------------
        # STEP 2 OF CYCLE: WIN-BOTH-WAYS RE-BUY (DIP <= -0.40% OR PUMP >= +0.35%)
        # ----------------------------------------------------------------------
        if status == "WAITING_DIP_REBUY" and proceeds > 0 and sold_sh > 0:
            sell_px = float(card.get("trade_sell_price") or price)
            rebuy_px = float(card.get("trade_rebuy_price") or (sell_px * (1.0 - DIP_REBUY_TARGET_PCT / 100.0)))
            upward_px = float(card.get("trade_upward_rebuy_price") or (sell_px * (1.0 + UPWARD_SURPLUS_GUARD_PCT / 100.0)))
            move_pct = ((price - sell_px) / sell_px) * 100.0 if sell_px > 0 else 0.0
            card["gain_from_low_pct"] = round(move_pct, 3)

            # PATH A: Dip Re-Buy (-0.40% or lower below sell price)
            hit_dip_target = (price <= rebuy_px) or (ask_price <= rebuy_px * 1.0004)
            if not is_frozen and hit_dip_target:
                if not is_live or (time.time() - order_cooldowns.get(sym, 0)) > 8:
                    execute_smart_rebuy_and_skim(sym, price, upward_mode=False)
                return

            # PATH B: Upward Surplus Guard (+0.35% or higher above sell price)
            hit_upward_target = price >= upward_px
            if not is_frozen and hit_upward_target:
                if not is_live or (time.time() - order_cooldowns.get(sym, 0)) > 12:
                    execute_smart_rebuy_and_skim(sym, price, upward_mode=True)
                return
            return

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

    # --------------------------------------------------------------------------
    # STEP 1 OF CYCLE: HOLDING SHARES & WATCHING FOR +0.75% BOUNCE TO SELL 25%
    # --------------------------------------------------------------------------
    with state_lock:
        card = STATE["coins"].get(sym)
        if not card or float(card.get("shares") or 0.0) <= 0:
            return

        shares = float(card["shares"])
        if shares > float(card.get("protected_shares") or 0.0):
            card["protected_shares"] = shares

        sliding_low = float(card.get("sliding_low") or price)
        card["anchor_usd"] = round(shares * price, 2)

        if sliding_low <= 0 or price < sliding_low:
            old_low = sliding_low
            sliding_low = price
            card["sliding_low"] = price
            card["entry_price"] = price
            if old_low > 0 and ((old_low - price) / old_low) >= 0.004:
                add_log(
                    f"📉 SLIDING LOW ALIGNED ({sym}) @ ${price:.4f}: "
                    f"Holding 100% of {shares:.6f} {sym} — waiting for +{SELL_BOUNCE_TRIGGER_PCT:.2f}% bounce!"
                )

        gain_pct = ((price - sliding_low) / sliding_low) * 100.0 if sliding_low > 0 else 0.0
        card["gain_from_low_pct"] = round(gain_pct, 3)

        if gain_pct >= SHIELD_ARM_TRIGGER_PCT and not card.get("ratchet_armed"):
            card["ratchet_armed"] = True
            card["highest_price"] = price
            stop_floor = sliding_low * 1.0028
            card["zero_loss_stop"] = stop_floor
            card["status"] = "RATCHET_ARMED"
            add_log(f"🛡️ SHIELD ARMED ({sym}) @ ${price:.4f} (+{gain_pct:.2f}% from low)")

        if card.get("ratchet_armed"):
            if price > float(card.get("highest_price") or price):
                card["highest_price"] = price
                trail_stop = price * 0.9982
                if trail_stop > float(card.get("zero_loss_stop") or 0.0):
                    card["zero_loss_stop"] = trail_stop

        highest_p = float(card.get("highest_price") or price)
        target_hit = gain_pct >= SELL_BOUNCE_TRIGGER_PCT
        trailing_stop_hit = (
            card.get("ratchet_armed")
            and gain_pct >= 0.58
            and highest_p > 0
            and price <= (highest_p * 0.9982)
        )

    if not is_frozen and (target_hit or trailing_stop_hit):
        if is_live and live_armed:
            execute_smart_wave_sell(sym, price, gain_pct)
        elif not is_live:
            execute_paper_simulator_skim(sym, price, gain_pct)


def execute_paper_simulator_skim(sym, price, gain_pct):
    with state_lock:
        card = STATE["coins"].get(sym)
        if not card or card.get("live_enabled"):
            return
        shares = float(card.get("shares") or 0.0)
        low_px = float(card.get("sliding_low") or (price / 1.0075))
        strategy = card.get("strategy") or "50/50 Moonbag + Cash Skim"
        skim_usd = max(0.08, round(shares * max(0.0, price - low_px), 4))

        if "100% Pure Cash" in strategy:
            vault_cut = skim_usd
            topup_cut = 0.0
            bonus_sh = 0.0
        else:
            vault_cut = round(skim_usd * 0.50, 4)
            topup_cut = round(skim_usd * 0.50, 4)
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
            "stage": f"Moonbag Skim (+{bonus_sh:.5f} {sym} & +${vault_cut:.4f} USDT)",
            "price": f"{price:.4f}",
            "time": ts_now,
            "profit": skim_usd,
            "vault_cut": vault_cut,
            "topup_cut": topup_cut,
            "shares": f"{card['shares']:.6f}"
        })
    save_persistent_state()


def execute_smart_wave_sell(sym, price, gain_pct=0.75):
    """
    STEP 1 (SELL HIGH):
    When a coin bounces +0.75% from its sliding low, sells a 25% working slice
    (leaving 75% in the Moonbag) and locks the USDT proceeds for Step 2.
    """
    with state_lock:
        card = STATE["coins"].get(sym)
        if not card:
            return
        shares = float(card.get("shares") or 0.0)
        is_live = bool(card.get("live_enabled")) and bool(STATE["exchange_bridge"].get("live_armed"))

    if not is_live or shares <= 0 or price <= 0:
        return

    now_ts = time.time()
    if (now_ts - order_cooldowns.get(sym, 0)) <= 12:
        return

    bag_usd = shares * price
    if bag_usd < GATEIO_MIN_ORDER_USD:
        return

    tranche_usd = max(bag_usd * WORKING_SLICE_RATIO, 3.60)
    if (bag_usd - tranche_usd) < 3.20:
        shares_to_sell = shares
    else:
        shares_to_sell = tranche_usd / price

    order_cooldowns[sym] = now_ts
    ok, msg, resp_data = execute_live_exchange_order(sym, "sell", share_amount=shares_to_sell)
    if not ok or not resp_data:
        return

    sold_sh = float(resp_data.get("_filled_shares") or shares_to_sell)
    net_usdt_proceeds = float(resp_data.get("_filled_usdt") or (sold_sh * price)) * 0.999
    actual_sell_px = net_usdt_proceeds / sold_sh if sold_sh > 0 else price
    rebuy_target_px = round(actual_sell_px * (1.0 - DIP_REBUY_TARGET_PCT / 100.0), 8)
    upward_target_px = round(actual_sell_px * (1.0 + UPWARD_SURPLUS_GUARD_PCT / 100.0), 8)

    time.sleep(0.9)
    bals, _ = fetch_live_exchange_balances()
    with state_lock:
        if bals:
            STATE["exchange_bridge"]["available_usdt"] = round(bals.get("USDT", {}).get("available", 0.0), 4)
            STATE["exchange_bridge"]["locked_usdt"] = round(bals.get("USDT", {}).get("locked", 0.0), 4)
            card["shares"] = bals.get(sym, {}).get("available", max(0.0, shares - sold_sh))
        else:
            card["shares"] = max(0.0, shares - sold_sh)

        card["anchor_usd"] = round(float(card["shares"]) * price, 2)
        card["trade_usdt_proceeds"] = round(net_usdt_proceeds, 4)
        card["trade_sold_shares"] = round(sold_sh, 6)
        card["trade_sell_price"] = round(actual_sell_px, 6)
        card["trade_rebuy_price"] = rebuy_target_px
        card["trade_upward_rebuy_price"] = upward_target_px
        card["trade_started_ts"] = time.time()
        card["status"] = "WAITING_DIP_REBUY"
        card["ratchet_armed"] = False

    add_log(
        f"📈 STEP 1 SELL HIGH ({sym}) @ ${price:.4f} (+{gain_pct:.2f}%): "
        f"Sold {sold_sh:.6f} {sym} (25% Slice) for ${net_usdt_proceeds:.2f} USDT -> "
        f"Dip Buy <= ${rebuy_target_px:.4f} (-0.40%) | Upward Guard >= ${upward_target_px:.4f} (+0.35%)!"
    )
    save_persistent_state(sync_db=True)


def execute_smart_rebuy_and_skim(sym, current_price, upward_mode=False, force_manual=False):
    """
    STEP 2 (WIN-BOTH-WAYS RE-BUY):
    - Dip Mode (-0.40% or lower): Buys back 100% of `sold_sh` + bonus Moonbag shares,
      and banks Pure USDT Profit in Spot wallet.
    - Upward Surplus Guard (+0.35% or higher): Uses the 3x unrealized gain on your 75%
      un-sold Moonbag to fund a 100.05% share restoration so shares NEVER decrease!
      Strictly protected by `min_required_shares=sold_sh` so it NEVER executes a partial buy.
    """
    with state_lock:
        card = STATE["coins"].get(sym)
        if not card:
            return False, "Coin card not found."
        proceeds_usdt = float(card.get("trade_usdt_proceeds") or 0.0)
        sold_sh = float(card.get("trade_sold_shares") or 0.0)
        sell_px = float(card.get("trade_sell_price") or current_price)
        current_sh = float(card.get("shares") or 0.0)
        prot_sh = float(card.get("protected_shares") or (current_sh + sold_sh))
        strategy = card.get("strategy") or "50/50 Moonbag + Cash Skim"
        is_live = bool(card.get("live_enabled")) and bool(STATE["exchange_bridge"].get("live_armed"))

    if proceeds_usdt <= 0 or sold_sh <= 0 or current_price <= 0:
        return False, "No active sold slice waiting for re-buy."

    quote = fetch_live_market_quote(sym)
    ask_px = float((quote or {}).get("ask") or current_price) * 1.0005
    cost_to_restore_sold_shares = (sold_sh / 0.9988) * ask_px

    if upward_mode and cost_to_restore_sold_shares > proceeds_usdt:
        # Target 100.05% of sold shares so even on an upward re-buy your share count grows!
        target_shares_with_bonus = sold_sh * 1.0005
        usdt_to_spend_on_rebuy = (target_shares_with_bonus / 0.9988) * ask_px
        pure_usdt_profit = 0.0
        min_sh_guard = sold_sh
    else:
        raw_spread_profit = max(0.0, (sell_px - ask_px) * sold_sh)
        net_spread_profit = max(0.0, min(proceeds_usdt - cost_to_restore_sold_shares, raw_spread_profit))

        if "100% Pure Cash" in strategy:
            pure_usdt_profit = round(net_spread_profit, 4)
            usdt_to_spend_on_rebuy = max(GATEIO_MIN_ORDER_USD, proceeds_usdt - pure_usdt_profit)
        else:
            pure_usdt_profit = round(net_spread_profit * 0.50, 4)
            usdt_to_spend_on_rebuy = max(GATEIO_MIN_ORDER_USD, proceeds_usdt - pure_usdt_profit)
        min_sh_guard = sold_sh if not force_manual else 0.0

    order_cooldowns[sym] = time.time()
    if is_live:
        ok, msg, resp_data = execute_live_exchange_order(
            sym,
            "buy",
            usd_amount=usdt_to_spend_on_rebuy,
            min_required_shares=min_sh_guard
        )
        if not ok or not resp_data:
            return False, msg
        net_received_sh = float(resp_data.get("_net_received_shares") or (usdt_to_spend_on_rebuy / ask_px))
        actual_spent_usdt = float(resp_data.get("_filled_usdt") or usdt_to_spend_on_rebuy)
        max_possible_spread = max(0.0, (sell_px - current_price) * sold_sh)
        pure_usdt_profit = round(min(max(0.0, proceeds_usdt - actual_spent_usdt), max_possible_spread + 0.05), 4)
        time.sleep(0.9)
        bals, _ = fetch_live_exchange_balances()
        with state_lock:
            if bals:
                STATE["exchange_bridge"]["available_usdt"] = round(bals.get("USDT", {}).get("available", 0.0), 4)
                STATE["exchange_bridge"]["locked_usdt"] = round(bals.get("USDT", {}).get("locked", 0.0), 4)
                card["shares"] = bals.get(sym, {}).get("available", current_sh + net_received_sh)
            else:
                card["shares"] = current_sh + net_received_sh
    else:
        net_received_sh = (usdt_to_spend_on_rebuy * 0.999) / ask_px
        with state_lock:
            card["shares"] = current_sh + net_received_sh

    with state_lock:
        new_total_sh = float(card["shares"])
        bonus_sh = max(0.0, new_total_sh - prot_sh) if new_total_sh > prot_sh else max(0.0, net_received_sh - sold_sh)
        bonus_usd_val = round(bonus_sh * current_price, 4)
        upward_moonbag_gain = round(max(0.0, current_sh * (current_price - sell_px)) * 0.25, 4) if upward_mode else 0.0
        total_cycle_profit = round(pure_usdt_profit + bonus_usd_val + upward_moonbag_gain, 4)
        ts_now = now_hms()

        if new_total_sh > prot_sh:
            card["protected_shares"] = new_total_sh
        card["bonus_shares_added"] = round(float(card.get("bonus_shares_added") or 0.0) + bonus_sh, 6)
        card["share_topup_usd"] = round(float(card.get("share_topup_usd") or 0.0) + bonus_usd_val + upward_moonbag_gain, 4)
        card["vaulted_usd"] = round(float(card.get("vaulted_usd") or 0.0) + pure_usdt_profit, 4)
        card["skim_count"] = int(card.get("skim_count") or 0) + 1

        card["trade_usdt_proceeds"] = 0.0
        card["trade_sold_shares"] = 0.0
        card["trade_sell_price"] = 0.0
        card["trade_rebuy_price"] = 0.0
        card["trade_upward_rebuy_price"] = 0.0

        card["anchor_usd"] = round(new_total_sh * current_price, 2)
        card["entry_price"] = current_price
        card["sliding_low"] = current_price
        card["highest_price"] = current_price
        card["gain_from_low_pct"] = 0.0
        card["ratchet_armed"] = False
        card["zero_loss_stop"] = 0.0
        card["status"] = "HOLDING_ANCHOR"

        STATE["vault_cash"] = round(float(STATE.get("vault_cash", 0.0)) + pure_usdt_profit, 4)
        STATE["added_to_anchors"] = round(
            float(STATE.get("added_to_anchors", 0.0)) + bonus_usd_val + upward_moonbag_gain, 4
        )
        STATE["total_skims"] = int(STATE.get("total_skims", 0)) + 1
        STATE["exchange_bridge"]["realized_usdt_profit"] = round(
            float(STATE["exchange_bridge"].get("realized_usdt_profit") or 0.0) + pure_usdt_profit, 4
        )

        total_gen = round(STATE["vault_cash"] + STATE["added_to_anchors"], 4)
        STATE["profit_curve"].append({"time": ts_now, "value": total_gen})
        if len(STATE["profit_curve"]) > 50:
            STATE["profit_curve"] = STATE["profit_curve"][-50:]

        stage_label = (
            f"Upward +0.35% Guard (+{bonus_sh:.5f} {sym} & +${upward_moonbag_gain:.4f} Moonbag)"
            if upward_mode
            else f"Dip -0.40% Skim (+{bonus_sh:.5f} {sym} & +${pure_usdt_profit:.4f} USDT)"
        )
        STATE["skim_ledger"].insert(0, {
            "id": str(uuid.uuid4())[:8],
            "symbol": sym,
            "mode": "LIVE" if is_live else "PAPER",
            "stage": stage_label,
            "price": f"{current_price:.4f}",
            "time": ts_now,
            "profit": total_cycle_profit,
            "vault_cut": pure_usdt_profit,
            "topup_cut": round(bonus_usd_val + upward_moonbag_gain, 4),
            "shares": f"{new_total_sh:.6f}"
        })
        if len(STATE["skim_ledger"]) > 60:
            STATE["skim_ledger"] = STATE["skim_ledger"][:60]

    add_log(
        f"💰 [{'UPWARD SURPLUS RE-BUY' if upward_mode else 'DIP SKIM'} COMPLETE] ({sym}) @ ${current_price:.4f}: "
        f"Shares now {new_total_sh:.6f} {sym} (+{bonus_sh:.6f} Moonbag added) | "
        f"Pure USDT Skimmed: +${pure_usdt_profit:.4f}!"
    )
    dispatch_phone_alert(
        f"Wave Cycle Complete ({sym})",
        f"Shares Now: {new_total_sh:.6f} {sym} (+{bonus_sh:.6f} bonus)\n"
        f"Pure USDT Profit: +${pure_usdt_profit:.4f}",
        category="skim"
    )
    save_persistent_state(sync_db=True)
    return True, "Re-buy and skim completed."


# ==============================================================================
# 8. BACKGROUND QUANT WORKER LOOP
# ==============================================================================
def run_single_engine_cycle():
    global _last_engine_tick_ts, _last_wallet_sync_ts
    if not tick_lock.acquire(blocking=False):
        return
    try:
        pull_latest_state_if_newer()
        run_auto_healing_watchdog()
        _last_engine_tick_ts = time.time()
        with state_lock:
            active_symbols = list(STATE.get("coins", {}).keys())
            live_armed = bool(STATE.get("exchange_bridge", {}).get("live_armed", False))
            if not active_symbols:
                STATE["last_price_tick_hms"] = now_hms()

        for sym in active_symbols:
            evaluate_coin_tick(sym)

        now_ts = time.time()
        if live_armed and (now_ts - _last_wallet_sync_ts) >= 25:
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
# PART 2 OF 2: PEAK COMMAND CENTER UI, API ROUTES & 1-TAP ORDER SWEEPER
# PASTE DIRECTLY BELOW PART 1 IN app.py
# ==============================================================================

PEAK_UI_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no" />
<title>VaultSkim v8.0 — Peak Quant Command Center</title>
<script src="https://cdn.tailwindcss.com"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600;700&display=swap');
  :root {
    --bg-main: #060911;
    --bg-card: #0c1220;
    --bg-elevated: #111a2e;
    --border-dim: rgba(148, 163, 184, 0.14);
    --accent-emerald: #10b981;
    --accent-cyan: #06b6d4;
    --accent-amber: #f59e0b;
    --accent-rose: #f43f5e;
  }
  body {
    background: radial-gradient(circle at 50% 0%, #0f172a 0%, #060911 70%);
    color: #f8fafc;
    font-family: 'Inter', sans-serif;
    min-height: 100vh;
    overflow-x: hidden;
  }
  .mono { font-family: 'JetBrains Mono', monospace; }
  .peak-card {
    background: rgba(12, 18, 32, 0.88);
    backdrop-filter: blur(14px);
    border: 1px solid var(--border-dim);
    border-radius: 1rem;
    box-shadow: 0 12px 32px -8px rgba(0, 0, 0, 0.6);
  }
  .peak-card-glow {
    background: linear-gradient(145deg, rgba(16, 185, 129, 0.08) 0%, rgba(12, 18, 32, 0.94) 55%);
    border: 1px solid rgba(16, 185, 129, 0.32);
    border-radius: 1rem;
  }
  .pulse-dot {
    width: 8px;
    height: 8px;
    border-radius: 9999px;
    background: #10b981;
    box-shadow: 0 0 10px #10b981;
    animation: pulseGlow 1.8s infinite;
  }
  @keyframes pulseGlow {
    0%, 100% { transform: scale(1); opacity: 1; }
    50% { transform: scale(1.3); opacity: 0.55; }
  }
  ::-webkit-scrollbar { width: 6px; height: 6px; }
  ::-webkit-scrollbar-track { background: #060911; }
  ::-webkit-scrollbar-thumb { background: #1e293b; border-radius: 3px; }
</style>
</head>
<body class="pb-16">

<div class="max-w-5xl mx-auto px-3 sm:px-5 pt-4 space-y-4">

  <!-- TOP COMMAND HEADER -->
  <header class="peak-card p-4">
    <div class="flex flex-wrap items-center justify-between gap-2">
      <div>
        <div class="flex items-center gap-2">
          <span class="pulse-dot"></span>
          <h1 class="text-lg sm:text-xl font-extrabold tracking-tight text-white">
            VaultSkim <span class="text-emerald-400">Command Center</span>
          </h1>
          <span id="versionBadge" class="text-[11px] font-semibold px-2.5 py-0.5 rounded-full bg-emerald-500/15 text-emerald-300 border border-emerald-500/30">
            v8.0 • Win-Both-Ways Quant
          </span>
        </div>
        <p class="text-xs text-slate-400 mt-0.5">
          +0.75% Sell 25% Slice • -0.40% Dip Skim OR +0.35% Upward Surplus Guard • Strict Zero-Share-Loss Wallet Guard
        </p>
      </div>
      <div class="text-right">
        <div class="text-xs text-slate-400">Live Tick (UTC): <span id="tickClock" class="mono text-emerald-400 font-semibold">--:--:--</span></div>
        <div class="text-[11px] text-slate-500">IOC Spot Execution • Zero Locked Order Drift</div>
      </div>
    </div>

    <!-- QUICK CONTROL BAR -->
    <div class="flex flex-wrap items-center gap-2 mt-3 pt-3 border-t border-slate-800/80">
      <button onclick="toggleDrawer('bridgeDrawer')" class="px-3 py-1.5 rounded-lg text-xs font-bold bg-amber-500 hover:bg-amber-400 text-slate-950 transition">
        ⚡ Live Exchange Bridge
      </button>
      <button onclick="toggleDrawer('alertsDrawer')" class="px-3 py-1.5 rounded-lg text-xs font-semibold bg-slate-800 hover:bg-slate-700 text-slate-200 border border-slate-700 transition">
        🔔 Phone Alerts
      </button>
      <button onclick="toggleDrawer('cashoutDrawer')" class="px-3 py-1.5 rounded-lg text-xs font-semibold bg-emerald-500/20 hover:bg-emerald-500/30 text-emerald-300 border border-emerald-500/30 transition">
        💵 Cash-Out & CSV
      </button>
      <button onclick="syncAndHealWallet()" id="syncBtn" class="px-3 py-1.5 rounded-lg text-xs font-semibold bg-indigo-600 hover:bg-indigo-500 text-white transition">
        🔄 Sync & Auto-Heal
      </button>
      <button onclick="cancelOpenOrders('')" id="sweepOrdersBtn" class="px-3 py-1.5 rounded-lg text-xs font-semibold bg-cyan-500/20 hover:bg-cyan-500/30 text-cyan-300 border border-cyan-500/40 transition">
        🧹 Cancel Open Orders (Unfreeze USDT)
      </button>
      <button onclick="toggleFreeze()" id="freezeBtn" class="px-3 py-1.5 rounded-lg text-xs font-semibold bg-rose-500/20 hover:bg-rose-500/30 text-rose-300 border border-rose-500/30 transition ml-auto">
        🛑 Freeze Live
      </button>
    </div>

    <!-- LIVE WALLET INTEGRITY STATUS PILLS -->
    <div class="grid grid-cols-2 sm:grid-cols-4 gap-2 mt-3">
      <div class="bg-slate-900/90 border border-slate-800 rounded-lg p-2.5">
        <div class="text-[10px] uppercase tracking-wider text-slate-400">Bridge Status</div>
        <div id="bridgeStatusPill" class="text-xs font-bold text-emerald-400 mt-0.5 truncate">Checking...</div>
      </div>
      <div class="bg-slate-900/90 border border-slate-800 rounded-lg p-2.5">
        <div class="text-[10px] uppercase tracking-wider text-slate-400">Available Spot USDT</div>
        <div id="availUsdtPill" class="mono text-sm font-bold text-white mt-0.5">$0.0000</div>
      </div>
      <div id="lockedUsdtBox" class="bg-slate-900/90 border border-slate-800 rounded-lg p-2.5">
        <div class="text-[10px] uppercase tracking-wider text-slate-400">Frozen / Locked USDT</div>
        <div id="lockedUsdtPill" class="mono text-sm font-bold text-slate-300 mt-0.5">$0.0000</div>
      </div>
      <div class="bg-slate-900/90 border border-slate-800 rounded-lg p-2.5">
        <div class="text-[10px] uppercase tracking-wider text-slate-400">Pure USDT Skimmed</div>
        <div id="pureUsdtPill" class="mono text-sm font-bold text-emerald-400 mt-0.5">+$0.0000</div>
      </div>
    </div>

    <!-- LOCKED FUNDS / IDLE EARN WARNING BANNER -->
    <div id="lockedWarningBanner" class="hidden mt-3 p-3 rounded-lg bg-amber-500/15 border border-amber-500/40 text-xs text-amber-200 flex flex-wrap items-center justify-between gap-2">
      <div>
        <span class="font-bold text-amber-300">⚠️ Gate.io Locked USDT Detected:</span>
        <span id="lockedWarningText">Part of your USDT is frozen in an Open Limit Order or Gate.io Idle Earn.</span>
      </div>
      <button onclick="cancelOpenOrders('')" class="px-2.5 py-1 rounded bg-amber-400 text-slate-950 font-bold text-xs hover:bg-amber-300">
        🧹 Unfreeze Open Orders Now
      </button>
    </div>
  </header>

  <!-- DRAWER 1: LIVE EXCHANGE BRIDGE -->
  <section id="bridgeDrawer" class="hidden peak-card p-4 border-amber-500/40">
    <div class="flex items-center justify-between mb-3">
      <h2 class="text-sm font-bold text-amber-400">🔐 Gate.io v4 Spot Execution Bridge (Zero-Withdrawal Security)</h2>
      <button onclick="toggleDrawer('bridgeDrawer')" class="text-xs text-slate-400 hover:text-white">✕ Close</button>
    </div>
    <div class="grid grid-cols-1 sm:grid-cols-2 gap-3 text-xs">
      <div>
        <label class="block text-slate-400 mb-1">1. Spot Exchange</label>
        <select id="exchSelect" class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-white">
          <option value="gateio">Gate.io Spot v4 (0.1% Fee • IOC Guard)</option>
          <option value="binance">Binance Spot v3</option>
        </select>
      </div>
      <div>
        <label class="block text-slate-400 mb-1">2. Max Order Safety Cap (USD)</label>
        <input id="maxOrderInput" type="number" value="250" class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-white mono" />
      </div>
      <div>
        <label class="block text-slate-400 mb-1">3. Spot API Key (Spot Trade Only, No Withdrawals)</label>
        <input id="apiKeyInput" type="text" placeholder="Paste API Key..." class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-white mono" />
        <div id="savedKeyHint" class="text-[11px] text-slate-500 mt-1"></div>
      </div>
      <div>
        <label class="block text-slate-400 mb-1">4. Spot API Secret</label>
        <input id="apiSecretInput" type="password" placeholder="Paste API Secret..." class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-white mono" />
      </div>
    </div>
    <div class="flex flex-wrap items-center gap-2 mt-3">
      <button onclick="saveExchangeBridge(false)" class="px-3.5 py-2 rounded-lg text-xs font-bold bg-slate-800 hover:bg-slate-700 text-emerald-300 border border-emerald-500/30">
        ✅ Verify API & Check USDT
      </button>
      <button onclick="saveExchangeBridge(true)" class="px-3.5 py-2 rounded-lg text-xs font-bold bg-amber-500 hover:bg-amber-400 text-slate-950">
        🚀 Save & Toggle Live Bridge
      </button>
      <span id="bridgeMsg" class="text-xs text-slate-300 ml-2"></span>
    </div>
  </section>

  <!-- DRAWER 2: PHONE ALERTS -->
  <section id="alertsDrawer" class="hidden peak-card p-4">
    <div class="flex items-center justify-between mb-3">
      <h2 class="text-sm font-bold text-indigo-300">📲 Instant Push Alerts (ntfy.sh & Telegram)</h2>
      <button onclick="toggleDrawer('alertsDrawer')" class="text-xs text-slate-400 hover:text-white">✕ Close</button>
    </div>
    <div class="grid grid-cols-1 sm:grid-cols-3 gap-3 text-xs">
      <div>
        <label class="block text-slate-400 mb-1">ntfy.sh Topic Name</label>
        <input id="ntfyInput" type="text" placeholder="e.g. enoch_vaultskim_alerts" class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-white mono" />
      </div>
      <div>
        <label class="block text-slate-400 mb-1">Telegram Bot Token (Optional)</label>
        <input id="tgTokenInput" type="text" placeholder="123456:ABC..." class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-white mono" />
      </div>
      <div>
        <label class="block text-slate-400 mb-1">Telegram Chat ID (Optional)</label>
        <input id="tgChatInput" type="text" placeholder="987654321" class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-white mono" />
      </div>
    </div>
    <div class="flex items-center gap-3 mt-3">
      <label class="flex items-center gap-1.5 text-xs text-slate-300">
        <input id="alertsEnabledToggle" type="checkbox" class="rounded" /> Enable Push Notifications
      </label>
      <button onclick="saveAlertsConfig()" class="px-3 py-1.5 rounded-lg text-xs font-bold bg-indigo-600 hover:bg-indigo-500 text-white">
        Save Alert Settings
      </button>
      <button onclick="testPhoneAlert()" class="px-3 py-1.5 rounded-lg text-xs font-semibold bg-slate-800 hover:bg-slate-700 text-slate-200">
        Send Test Alert
      </button>
    </div>
  </section>

  <!-- DRAWER 3: CASH-OUT & CSV EXPORT -->
  <section id="cashoutDrawer" class="hidden peak-card p-4">
    <div class="flex items-center justify-between mb-3">
      <h2 class="text-sm font-bold text-emerald-300">💵 Realized Profit Ledger & CSV Export</h2>
      <button onclick="toggleDrawer('cashoutDrawer')" class="text-xs text-slate-400 hover:text-white">✕ Close</button>
    </div>
    <div class="flex flex-wrap items-center justify-between gap-2 text-xs">
      <div class="text-slate-300">
        Download your complete verified Gate.io wave-skim history as a spreadsheet or reset counters.
      </div>
      <div class="flex items-center gap-2">
        <a href="/export_csv" class="px-3 py-1.5 rounded-lg font-bold bg-emerald-500 hover:bg-emerald-400 text-slate-950">
          📥 Export Verified CSV
        </a>
        <button onclick="resetProfitCounters()" class="px-3 py-1.5 rounded-lg font-semibold bg-slate-800 hover:bg-slate-700 text-rose-300 border border-rose-500/30">
          Reset Profit Counters ($0.00)
        </button>
      </div>
    </div>
  </section>

  <!-- HERO REALIZED PROFIT + CURVE -->
  <div class="grid grid-cols-1 lg:grid-cols-12 gap-4">
    <div class="lg:col-span-5 peak-card-glow p-5 flex flex-col justify-between">
      <div>
        <div class="text-[11px] font-bold uppercase tracking-widest text-slate-400">
          TOTAL REALIZED PROFIT (PURE USDT + MOONBAG SHARES)
        </div>
        <div id="heroTotalProfit" class="mono text-3xl sm:text-4xl font-extrabold text-emerald-400 mt-1">
          $0.0000
        </div>
        <div class="mt-2 space-y-1 text-xs text-slate-300">
          <div class="flex justify-between">
            <span>Pure USDT Cash Skimmed:</span>
            <span id="heroVaultCash" class="mono font-bold text-emerald-300">$0.0000</span>
          </div>
          <div class="flex justify-between">
            <span>Moonbag Bonus Shares Added:</span>
            <span id="heroMoonbagAdded" class="mono font-bold text-cyan-300">+$0.0000</span>
          </div>
          <div class="flex justify-between">
            <span>Completed Win-Both-Ways Skims:</span>
            <span id="heroSkimCount" class="mono font-bold text-white">0</span>
          </div>
        </div>
      </div>
      <div class="mt-4 pt-3 border-t border-emerald-500/20 text-[11px] text-slate-400 flex items-center justify-between">
        <span>75% Moonbag Protected on Pumps</span>
        <span id="autoHealsBadge" class="mono text-emerald-300">Auto-Heals: 0</span>
      </div>
    </div>

    <div class="lg:col-span-7 peak-card p-4">
      <div class="flex items-center justify-between mb-2">
        <span class="text-xs font-bold uppercase tracking-wider text-slate-300">📈 Live Realized Profit Growth Curve</span>
        <span id="curveCompletedBadge" class="text-[11px] px-2 py-0.5 rounded bg-emerald-500/15 text-emerald-300 font-semibold">
          0 Completed Wave Skims
        </span>
      </div>
      <div class="h-36">
        <canvas id="profitChart"></canvas>
      </div>
    </div>
  </div>

  <!-- LAUNCH COIN OR QUICK QNT/LTC LAUNCHER -->
  <section class="peak-card p-4">
    <div class="flex flex-wrap items-center justify-between gap-2 mb-2">
      <div class="text-xs font-bold text-amber-300">
        🚀 Launch Any Spot Coin or Paste DEX Contract (Anti-Rug Protected)
      </div>
      <div class="flex items-center gap-1.5">
        <button onclick="quickLaunchCoin('QNT', 110)" class="px-2.5 py-1 rounded text-[11px] font-bold bg-emerald-500/20 hover:bg-emerald-500/30 text-emerald-300 border border-emerald-500/30">
          + Quick Sync QNT
        </button>
        <button onclick="quickLaunchCoin('LTC', 55)" class="px-2.5 py-1 rounded text-[11px] font-bold bg-cyan-500/20 hover:bg-cyan-500/30 text-cyan-300 border border-cyan-500/30">
          + Quick Sync LTC
        </button>
      </div>
    </div>
    <div class="grid grid-cols-1 sm:grid-cols-12 gap-2">
      <input id="launchSymbolInput" type="text" placeholder="Symbol or Contract (e.g. QNT, LTC, SOL)..." class="sm:col-span-6 bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-xs text-white mono" />
      <input id="launchAnchorInput" type="number" value="50" placeholder="Anchor USD" class="sm:col-span-3 bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-xs text-white mono" />
      <button onclick="launchCoinCard()" class="sm:col-span-3 px-4 py-2 rounded-lg text-xs font-extrabold bg-emerald-500 hover:bg-emerald-400 text-slate-950 transition">
        + Snipe & Launch
      </button>
    </div>
  </section>

  <!-- ACTIVE QUANT CARDS CONTAINER -->
  <section id="coinsGrid" class="grid grid-cols-1 md:grid-cols-2 gap-4"></section>

  <!-- VERIFIED GATE.IO SKIM LEDGER & LIVE EXECUTION LOGS -->
  <div class="grid grid-cols-1 lg:grid-cols-12 gap-4">
    <div class="lg:col-span-6 peak-card p-4">
      <div class="flex items-center justify-between mb-3">
        <div>
          <h3 class="text-xs font-bold uppercase tracking-wider text-white">🧾 Verified Gate.io Skim Ledger</h3>
          <p class="text-[11px] text-slate-400">Every completed Dip Skim & Upward Surplus Re-Buy</p>
        </div>
        <a href="/export_csv" class="px-2.5 py-1 rounded text-xs font-bold bg-emerald-500/20 text-emerald-300 border border-emerald-500/30">
          📥 CSV
        </a>
      </div>
      <div id="skimLedgerList" class="space-y-2 max-h-72 overflow-y-auto pr-1"></div>
    </div>

    <div class="lg:col-span-6 peak-card p-4">
      <div class="flex items-center justify-between mb-3">
        <div>
          <h3 class="text-xs font-bold uppercase tracking-wider text-white">🛡️ Live Execution & Wallet Guard Log</h3>
          <p class="text-[11px] text-slate-400">Real-time IOC fills, sliding lows & wallet locks</p>
        </div>
      </div>
      <div id="liveLogsBox" class="mono text-[11px] space-y-1.5 max-h-72 overflow-y-auto bg-slate-950/90 p-3 rounded-lg border border-slate-800 text-slate-300"></div>
    </div>
  </div>

</div>

<script>
let profitChartInstance = null;

function toggleDrawer(id) {
  const el = document.getElementById(id);
  if (el) el.classList.toggle('hidden');
}

function initProfitChart() {
  const ctx = document.getElementById('profitChart').getContext('2d');
  profitChartInstance = new Chart(ctx, {
    type: 'line',
    data: {
      labels: ['Start'],
      datasets: [{
        label: 'Realized Profit ($)',
        data: [0],
        borderColor: '#10b981',
        backgroundColor: 'rgba(16, 185, 129, 0.14)',
        borderWidth: 2.2,
        pointRadius: 2.5,
        fill: true,
        tension: 0.32
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { ticks: { color: '#64748b', font: { size: 10 } }, grid: { display: false } },
        y: {
          ticks: {
            color: '#94a3b8',
            font: { size: 10 },
            callback: (v) => '$' + Number(v).toFixed(2)
          },
          grid: { color: 'rgba(148, 163, 184, 0.08)' }
        }
      }
    }
  });
}

function renderCoinCard(sym, c, availUsdt) {
  const px = Number(c.current_price || 0);
  const sh = Number(c.shares || 0);
  const protSh = Number(c.protected_shares || sh);
  const bonusSh = Number(c.bonus_shares_added || 0);
  const lockedSh = Number(c.locked_shares || 0);
  const liveTokenUsd = sh * px;
  const proceedsUsd = Number(c.trade_usdt_proceeds || 0);
  const totalPosUsd = liveTokenUsd + proceedsUsd;
  const slidingLow = Number(c.sliding_low || px);
  const sellTargetPx = slidingLow * 1.0075;
  const estMoonbagGain = (sh * (sellTargetPx - slidingLow)) * 0.5;
  const gainPct = Number(c.gain_from_low_pct || 0);
  const vaultedUsd = Number(c.vaulted_usd || 0);
  const skimCount = Number(c.skim_count || 0);
  const status = c.status || 'HOLDING_ANCHOR';
  const strategy = c.strategy || '50/50 Moonbag + Cash Skim';
  const isWaitingRebuy = (status === 'WAITING_DIP_REBUY' && proceedsUsd > 0);

  const progressPct = Math.max(4, Math.min(100, ((gainPct + 0.45) / 1.25) * 100));

  let rebuyBannerHtml = '';
  if (isWaitingRebuy) {
    const soldSh = Number(c.trade_sold_shares || 0);
    const dipPx = Number(c.trade_rebuy_price || (px * 0.996));
    const upPx = Number(c.trade_upward_rebuy_price || (px * 1.0035));
    rebuyBannerHtml = `
      <div class="mt-3 p-3 rounded-lg bg-amber-500/10 border border-amber-500/40 text-xs space-y-1.5">
        <div class="flex items-center justify-between">
          <span class="font-bold text-amber-300">🎯 Sold ${soldSh.toFixed(4)} ${sym} ($${proceedsUsd.toFixed(2)}):</span>
          <span class="mono font-bold text-emerald-300">Dip Buy &lt;= $${dipPx.toFixed(4)}</span>
        </div>
        <div class="flex items-center justify-between text-[11px]">
          <span class="text-cyan-300">🚀 75% Moonbag Upward Guard (+0.35%):</span>
          <span class="mono font-bold text-cyan-300">Re-Buy &gt;= $${upPx.toFixed(4)}</span>
        </div>
      </div>
    `;
  }

  let topupBtnHtml = '';
  if (availUsdt >= 3.25) {
    topupBtnHtml = `
      <button onclick="topupCoinWithSpotUsdt('${sym}')" class="w-full mt-2 py-2 px-3 rounded-lg text-xs font-bold bg-indigo-500/20 hover:bg-indigo-500/30 text-indigo-200 border border-indigo-500/40 transition">
        🛡️ Use Spot USDT ($${availUsdt.toFixed(2)}) to Buy More ${sym} Shares
      </button>
    `;
  }

  let manualRebuyBtnHtml = '';
  if (isWaitingRebuy) {
    manualRebuyBtnHtml = `
      <button onclick="manualRebuyNow('${sym}')" class="w-full mt-2 py-2 px-3 rounded-lg text-xs font-extrabold bg-amber-400 hover:bg-amber-300 text-slate-950 transition">
        ⚡ Buy Back ${sym} Shares Right Now ($${proceedsUsd.toFixed(2)})
      </button>
    `;
  }

  let lockedSharesAlert = '';
  if (lockedSh > 0.0001) {
    lockedSharesAlert = `
      <div class="mt-2 p-2 rounded bg-rose-500/15 border border-rose-500/40 text-[11px] text-rose-200 flex items-center justify-between">
        <span>🔒 ${lockedSh.toFixed(5)} ${sym} locked in Open Order</span>
        <button onclick="cancelOpenOrders('${sym}')" class="px-2 py-0.5 rounded bg-rose-500 text-white font-bold">Unlock</button>
      </div>
    `;
  }

  return `
    <div class="peak-card p-4 flex flex-col justify-between">
      <div>
        <!-- CARD HEADER -->
        <div class="flex items-center justify-between gap-2">
          <div class="flex items-center gap-2">
            <span class="text-base font-extrabold text-white">${sym}/USDT</span>
            <span class="text-[10px] font-bold px-2 py-0.5 rounded bg-rose-500/20 text-rose-300 border border-rose-500/30">
              ${c.live_enabled ? '⚡ LIVE ORDER' : '🧪 PAPER'}
            </span>
            <span class="text-[10px] font-bold px-2 py-0.5 rounded bg-slate-800 text-cyan-300 border border-slate-700">
              ${status}
            </span>
          </div>
          <button onclick="removeCoinCard('${sym}')" class="text-xs text-slate-500 hover:text-rose-400">✕</button>
        </div>

        <div class="flex items-center justify-between text-[11px] mt-1">
          <span class="text-emerald-400 font-medium">● VERIFIED CEX (${c.source || 'GATE.IO SPOT'})</span>
          <span class="mono text-emerald-300 font-semibold">+${bonusSh.toFixed(6)} Moonbag ${sym} Added</span>
        </div>

        <!-- STRATEGY SWITCHER -->
        <div class="flex items-center justify-between mt-2.5 p-2 rounded-lg bg-slate-900/90 border border-slate-800 text-xs">
          <span class="text-slate-300 font-medium">⚡ Mode: <strong class="text-white">${strategy}</strong></span>
          <button onclick="switchStrategy('${sym}')" class="px-2.5 py-0.5 rounded bg-indigo-500/20 hover:bg-indigo-500/30 text-indigo-300 border border-indigo-500/30 font-semibold">
            Switch
          </button>
        </div>

        <!-- PRICE HERO -->
        <div class="mt-3 flex items-baseline gap-2">
          <span class="mono text-2xl sm:text-3xl font-extrabold text-white">$${px.toFixed(4)}</span>
          <span class="mono text-xs font-bold ${gainPct >= 0 ? 'text-emerald-400' : 'text-amber-400'}">
            (${gainPct >= 0 ? '+' : ''}${gainPct.toFixed(2)}% from Low)
          </span>
        </div>

        ${lockedSharesAlert}

        <!-- METRICS LIST -->
        <div class="mt-3 space-y-1.5 text-xs border-t border-slate-800/80 pt-3">
          <div class="flex justify-between">
            <span class="text-slate-400">Live Token Value (In ${sym}):</span>
            <div class="flex items-center gap-1.5">
              <span class="mono font-bold text-white">$${liveTokenUsd.toFixed(2)}</span>
              <button onclick="editAnchorPrompt('${sym}', ${liveTokenUsd.toFixed(2)})" class="text-[10px] px-1.5 py-0.5 rounded bg-slate-800 text-cyan-300">Edit</button>
            </div>
          </div>
          <div class="flex justify-between">
            <span class="text-slate-400">Real Shares Owned (Gate.io):</span>
            <span class="mono font-bold text-emerald-400">${sh.toFixed(6)} ${sym}</span>
          </div>
          <div class="flex justify-between">
            <span class="text-slate-400">Protected Share Baseline:</span>
            <span class="mono font-semibold text-cyan-300">🛡️ ${protSh.toFixed(6)} ${sym} (Grows on Re-Buy)</span>
          </div>
          <div class="flex justify-between">
            <span class="text-slate-400">Total Position Value (Tokens + Cash):</span>
            <span class="mono font-bold text-white">$${totalPosUsd.toFixed(2)}</span>
          </div>
          <div class="flex justify-between">
            <span class="text-slate-400">Next Sell-High Trigger (+0.75%):</span>
            <span class="mono font-bold text-amber-300">$${sellTargetPx.toFixed(4)} (+$${estMoonbagGain.toFixed(4)} Moonbag)</span>
          </div>
          <div class="flex justify-between">
            <span class="text-slate-400">Sliding Low / Sell-High Target:</span>
            <span class="mono text-slate-200">$${slidingLow.toFixed(4)} / $${sellTargetPx.toFixed(4)}</span>
          </div>
        </div>

        ${rebuyBannerHtml}

        <!-- PROGRESS BAR -->
        <div class="w-full h-2 bg-slate-900 rounded-full overflow-hidden mt-3 border border-slate-800">
          <div class="h-full bg-gradient-to-r from-emerald-500 to-amber-400 transition-all duration-300" style="width: ${progressPct}%"></div>
        </div>

        <div class="flex justify-between items-center text-xs mt-2.5 pt-2 border-t border-slate-800/60">
          <span class="text-slate-400">Pure USDT Skimmed (${sym}):</span>
          <span class="mono font-bold text-emerald-400">+$${vaultedUsd.toFixed(4)} (${skimCount} completed cycles)</span>
        </div>
      </div>

      <!-- ACTION FOOTER -->
      <div class="mt-3 pt-2 border-t border-slate-800/80">
        <div class="w-full py-2 px-3 rounded-lg text-center text-xs font-bold bg-emerald-500/15 text-emerald-300 border border-emerald-500/30">
          ● Win-Both-Ways (Dip -0.40% OR +0.35% Upward) Skimmer Active
        </div>
        ${manualRebuyBtnHtml}
        ${topupBtnHtml}
      </div>
    </div>
  `;
}

async function pollState() {
  try {
    const r = await fetch('/state?_t=' + Date.now());
    if (!r.ok) return;
    const s = await r.json();

    document.getElementById('tickClock').textContent = s.last_price_tick_hms || '--:--:--';
    const vaultCash = Number(s.vault_cash || 0);
    const addedAnchors = Number(s.added_to_anchors || 0);
    const totalProfit = vaultCash + addedAnchors;

    document.getElementById('heroTotalProfit').textContent = '$' + totalProfit.toFixed(4);
    document.getElementById('heroVaultCash').textContent = '$' + vaultCash.toFixed(4);
    document.getElementById('heroMoonbagAdded').textContent = '+$' + addedAnchors.toFixed(4);
    document.getElementById('heroSkimCount').textContent = s.total_skims || 0;
    document.getElementById('curveCompletedBadge').textContent = (s.total_skims || 0) + ' Completed Wave Skims';
    document.getElementById('autoHealsBadge').textContent = 'Auto-Heals: ' + (s.auto_heals || 0);

    const b = s.exchange_bridge || {};
    const availUsdt = Number(b.available_usdt || 0);
    const lockedUsdt = Number(b.locked_usdt || 0);

    document.getElementById('availUsdtPill').textContent = '$' + availUsdt.toFixed(4);
    document.getElementById('lockedUsdtPill').textContent = '$' + lockedUsdt.toFixed(4);
    document.getElementById('pureUsdtPill').textContent = '+$' + vaultCash.toFixed(4);

    const pill = document.getElementById('bridgeStatusPill');
    if (b.live_armed) {
      pill.textContent = '🟢 LIVE ARMED (' + (b.exchange || 'GATEIO').toUpperCase() + ')';
      pill.className = 'text-xs font-bold text-emerald-400 mt-0.5 truncate';
    } else {
      pill.textContent = '🟡 PAPER / STANDBY';
      pill.className = 'text-xs font-bold text-amber-300 mt-0.5 truncate';
    }

    const lockedBanner = document.getElementById('lockedWarningBanner');
    const lockedBox = document.getElementById('lockedUsdtBox');
    if (lockedUsdt > 0.50) {
      lockedBanner.classList.remove('hidden');
      lockedBox.className = 'bg-amber-500/15 border border-amber-500/50 rounded-lg p-2.5';
      document.getElementById('lockedWarningText').textContent =
        `$${lockedUsdt.toFixed(2)} USDT is currently Frozen in an Open Order or Idle Earn. Tap "Unfreeze Open Orders Now" or redeem Idle Earn on Gate.io!`;
    } else {
      lockedBanner.classList.add('hidden');
      lockedBox.className = 'bg-slate-900/90 border border-slate-800 rounded-lg p-2.5';
    }

    const freezeBtn = document.getElementById('freezeBtn');
    if (s.frozen) {
      freezeBtn.textContent = '▶️ Unfreeze Bot';
      freezeBtn.className = 'px-3 py-1.5 rounded-lg text-xs font-bold bg-emerald-500 text-slate-950 ml-auto';
    } else {
      freezeBtn.textContent = '🛑 Freeze Live';
      freezeBtn.className = 'px-3 py-1.5 rounded-lg text-xs font-semibold bg-rose-500/20 text-rose-300 border border-rose-500/30 ml-auto';
    }

    if (b.api_key_masked) {
      document.getElementById('savedKeyHint').textContent = 'Saved key: ' + b.api_key_masked;
    }

    // Update Chart
    if (profitChartInstance && Array.isArray(s.profit_curve)) {
      profitChartInstance.data.labels = s.profit_curve.map(pt => pt.time);
      profitChartInstance.data.datasets[0].data = s.profit_curve.map(pt => pt.value);
      profitChartInstance.update('none');
    }

    // Render Coin Cards
    const coins = s.coins || {};
    const keys = Object.keys(coins);
    const grid = document.getElementById('coinsGrid');
    if (keys.length === 0) {
      grid.innerHTML = `
        <div class="col-span-2 peak-card p-6 text-center text-xs text-slate-400">
          No active coin cards yet. Tap <strong>+ Quick Sync QNT</strong> or <strong>+ Quick Sync LTC</strong> above to launch!
        </div>
      `;
    } else {
      grid.innerHTML = keys.map(k => renderCoinCard(k, coins[k], availUsdt)).join('');
    }

    // Render Skim Ledger
    const ledger = s.skim_ledger || [];
    const ledgerEl = document.getElementById('skimLedgerList');
    if (ledger.length === 0) {
      ledgerEl.innerHTML = '<div class="text-xs text-slate-500 py-4 text-center">Waiting for next completed wave cycle...</div>';
    } else {
      ledgerEl.innerHTML = ledger.slice(0, 20).map(item => `
        <div class="p-2.5 rounded-lg bg-slate-900/90 border border-slate-800 flex items-center justify-between text-xs">
          <div>
            <div class="flex items-center gap-1.5">
              <span class="font-extrabold text-white">${item.symbol}</span>
              <span class="text-[10px] px-1.5 py-0.2 rounded bg-emerald-500/20 text-emerald-300 font-bold">${item.mode || 'LIVE'}</span>
              <span class="text-slate-300 text-[11px]">${item.stage || ''}</span>
            </div>
            <div class="mono text-[10px] text-slate-400 mt-0.5">@ $${item.price} • ${item.time}</div>
          </div>
          <div class="text-right mono">
            <div class="font-bold text-emerald-400">+$${Number(item.profit || 0).toFixed(4)}</div>
            <div class="text-[10px] text-slate-400">${item.shares || ''} sh</div>
          </div>
        </div>
      `).join('');
    }

    // Render Logs
    const logs = s.logs || [];
    document.getElementById('liveLogsBox').innerHTML = logs.slice(0, 40).map(l => `<div>${l}</div>`).join('');
  } catch (e) {
    console.error('State poll error:', e);
  }
}

async function saveExchangeBridge(toggleLive) {
  const payload = {
    exchange: document.getElementById('exchSelect').value,
    api_key: document.getElementById('apiKeyInput').value.trim(),
    api_secret: document.getElementById('apiSecretInput').value.trim(),
    max_order_usd: parseFloat(document.getElementById('maxOrderInput').value || '250'),
    toggle_live: toggleLive
  };
  const msgEl = document.getElementById('bridgeMsg');
  msgEl.textContent = 'Checking Gate.io Spot v4...';
  const r = await fetch('/configure_exchange', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });
  const d = await r.json();
  msgEl.textContent = d.message || (d.ok ? 'Verified!' : 'Error');
  pollState();
}

async function syncAndHealWallet() {
  const btn = document.getElementById('syncBtn');
  btn.textContent = '🔄 Syncing...';
  await fetch('/sync_exchange_wallet', { method: 'POST' });
  btn.textContent = '🔄 Sync & Auto-Heal';
  pollState();
}

async function cancelOpenOrders(symbol) {
  const btn = document.getElementById('sweepOrdersBtn');
  btn.textContent = '🧹 Unfreezing...';
  await fetch('/cancel_open_orders', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ symbol: symbol || '' })
  });
  btn.textContent = '🧹 Cancel Open Orders (Unfreeze USDT)';
  pollState();
}

async function toggleFreeze() {
  await fetch('/toggle_freeze', { method: 'POST' });
  pollState();
}

async function launchCoinCard() {
  const sym = document.getElementById('launchSymbolInput').value.trim();
  const anchor = parseFloat(document.getElementById('launchAnchorInput').value || '50');
  if (!sym) return;
  await fetch('/launch_coin', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ symbol: sym, anchor_usd: anchor, live_enabled: true })
  });
  document.getElementById('launchSymbolInput').value = '';
  pollState();
}

async function quickLaunchCoin(sym, anchor) {
  await fetch('/launch_coin', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ symbol: sym, anchor_usd: anchor, live_enabled: true })
  });
  pollState();
}

async function switchStrategy(sym) {
  await fetch('/switch_strategy', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ symbol: sym })
  });
  pollState();
}

async function manualRebuyNow(sym) {
  await fetch('/manual_rebuy_now', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ symbol: sym })
  });
  pollState();
}

async function topupCoinWithSpotUsdt(sym) {
  await fetch('/topup_coin_with_spot_usdt', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ symbol: sym })
  });
  pollState();
}

async function editAnchorPrompt(sym, currentVal) {
  const val = prompt(`Enter new target Anchor USD for ${sym}:`, currentVal);
  if (!val) return;
  await fetch('/edit_anchor', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ symbol: sym, anchor_usd: parseFloat(val) })
  });
  pollState();
}

async function removeCoinCard(sym) {
  if (!confirm(`Remove ${sym} card from dashboard? (Your Gate.io Spot tokens remain untouched)`)) return;
  await fetch('/remove_coin', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ symbol: sym })
  });
  pollState();
}

async function resetProfitCounters() {
  if (!confirm('Reset profit counters and skim ledger to $0.00?')) return;
  await fetch('/reset_profits', { method: 'POST' });
  pollState();
}

async function saveAlertsConfig() {
  await fetch('/configure_alerts', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      enabled: document.getElementById('alertsEnabledToggle').checked,
      ntfy_topic: document.getElementById('ntfyInput').value.trim(),
      telegram_bot_token: document.getElementById('tgTokenInput').value.trim(),
      telegram_chat_id: document.getElementById('tgChatInput').value.trim()
    })
  });
  pollState();
}

async function testPhoneAlert() {
  await fetch('/test_alert', { method: 'POST' });
}

window.addEventListener('DOMContentLoaded', () => {
  initProfitChart();
  pollState();
  setInterval(pollState, 2500);
});
</script>
</body>
</html>
"""


# ==============================================================================
# 9. FLASK HTTP ROUTES & JSON CONTROLLERS
# ==============================================================================
@app.route("/")
def index_route():
    return render_template_string(PEAK_UI_HTML)


@app.route("/heartbeat")
def heartbeat_route():
    return jsonify({"ok": True, "version": STATE.get("version"), "ts": int(time.time())})


@app.route("/state")
def state_route():
    with state_lock:
        resp = json.loads(json.dumps(STATE))
        raw_key = resp.get("exchange_bridge", {}).get("api_key") or ""
        resp["exchange_bridge"]["api_key_masked"] = (
            f"{raw_key[:4]}...{raw_key[-4:]}" if len(raw_key) > 8 else ""
        )
        resp["exchange_bridge"]["api_key"] = ""
        resp["exchange_bridge"]["api_secret"] = ""
    return jsonify(resp)


@app.route("/configure_exchange", methods=["POST"])
def configure_exchange_route():
    data = request.get_json(force=True) or {}
    with state_lock:
        b = STATE["exchange_bridge"]
        if data.get("exchange"):
            b["exchange"] = data["exchange"].strip().lower()
        if data.get("api_key"):
            b["api_key"] = data["api_key"].strip()
        if data.get("api_secret"):
            b["api_secret"] = data["api_secret"].strip()
        if data.get("max_order_usd"):
            b["max_order_usd"] = float(data["max_order_usd"])
        if data.get("toggle_live"):
            b["live_armed"] = not bool(b.get("live_armed", False))

    ok, res = sync_all_live_cards_with_gateio(auto_buy_waiting=True, verbose=True)
    save_persistent_state(sync_db=True)
    if ok:
        with state_lock:
            avail = STATE["exchange_bridge"].get("available_usdt", 0.0)
            locked = STATE["exchange_bridge"].get("locked_usdt", 0.0)
        return jsonify({
            "ok": True,
            "message": f"Connected! Available USDT: ${avail:.4f} (Locked: ${locked:.2f})"
        })
    return jsonify({"ok": False, "message": str(res)})


@app.route("/sync_exchange_wallet", methods=["POST"])
def sync_exchange_wallet_route():
    run_auto_healing_watchdog()
    ok, res = sync_all_live_cards_with_gateio(auto_buy_waiting=True, verbose=True)
    save_persistent_state(sync_db=True)
    return jsonify({"ok": ok, "result": res})


@app.route("/cancel_open_orders", methods=["POST"])
def cancel_open_orders_route():
    data = request.get_json(force=True) or {}
    sym = (data.get("symbol") or "").strip().upper() or None
    ok, msg = cancel_all_open_spot_orders(sym)
    save_persistent_state(sync_db=True)
    return jsonify({"ok": ok, "message": msg})


@app.route("/launch_coin", methods=["POST"])
def launch_coin_route():
    data = request.get_json(force=True) or {}
    raw_input = (data.get("symbol") or "QNT").strip()
    anchor_usd = max(GATEIO_MIN_ORDER_USD, float(data.get("anchor_usd") or 50.0))
    live_enabled = bool(data.get("live_enabled", True))

    contract = raw_input if len(raw_input) > 15 else ""
    sym = raw_input[:6].upper() if contract else raw_input.upper().replace("/USDT", "").replace("_USDT", "")

    quote = fetch_live_market_quote(sym, contract)
    if not quote or quote["price"] <= 0:
        return jsonify({"ok": False, "message": f"Could not fetch live quote for {raw_input}."}), 400

    if quote.get("is_dex") and float(quote.get("liquidity_usd") or 0.0) < 15000.0:
        return jsonify({"ok": False, "message": "Anti-Rug Block: DEX liquidity under $15,000 USD."}), 400

    sym = (quote.get("symbol") or sym).upper()
    price = float(quote["price"])

    with state_lock:
        STATE["coins"][sym] = {
            "symbol": sym,
            "contract": contract,
            "anchor_usd": round(anchor_usd, 2),
            "current_price": price,
            "entry_price": price,
            "sliding_low": price,
            "highest_price": price,
            "shares": 0.0 if live_enabled else round(anchor_usd / price, 6),
            "protected_shares": 0.0 if live_enabled else round(anchor_usd / price, 6),
            "locked_shares": 0.0,
            "bonus_shares_added": 0.0,
            "gain_from_low_pct": 0.0,
            "ratchet_armed": False,
            "zero_loss_stop": 0.0,
            "status": "WAITING_LIVE_BUY" if live_enabled else "HOLDING_ANCHOR",
            "strategy": "50/50 Moonbag + Cash Skim",
            "live_enabled": live_enabled,
            "source": quote["source"],
            "vaulted_usd": 0.0,
            "share_topup_usd": 0.0,
            "skim_count": 0,
            "trade_usdt_proceeds": 0.0,
            "trade_sold_shares": 0.0,
            "trade_sell_price": 0.0,
            "trade_rebuy_price": 0.0,
            "trade_upward_rebuy_price": 0.0
        }

    add_log(f"🚀 LAUNCHED {sym}/USDT @ ${price:.4f} ({quote['source']})")
    if live_enabled:
        sync_all_live_cards_with_gateio(auto_buy_waiting=True, verbose=True)
    save_persistent_state(sync_db=True)
    return jsonify({"ok": True, "symbol": sym})


@app.route("/switch_strategy", methods=["POST"])
def switch_strategy_route():
    data = request.get_json(force=True) or {}
    sym = (data.get("symbol") or "").upper().strip()
    with state_lock:
        c = STATE["coins"].get(sym)
        if not c:
            return jsonify({"ok": False}), 404
        curr = c.get("strategy") or "50/50 Moonbag + Cash Skim"
        c["strategy"] = (
            "100% Pure Cash Skim"
            if "50/50" in curr
            else "50/50 Moonbag + Cash Skim"
        )
        new_mode = c["strategy"]
    add_log(f"⚡ STRATEGY SWITCHED ({sym}): {new_mode}")
    save_persistent_state(sync_db=True)
    return jsonify({"ok": True, "strategy": new_mode})


@app.route("/manual_rebuy_now", methods=["POST"])
def manual_rebuy_now_route():
    data = request.get_json(force=True) or {}
    sym = (data.get("symbol") or "").upper().strip()
    with state_lock:
        c = STATE["coins"].get(sym)
        if not c:
            return jsonify({"ok": False, "message": "Card not found."}), 404
        px = float(c.get("current_price") or 0.0)
    ok, msg = execute_smart_rebuy_and_skim(sym, px, upward_mode=False, force_manual=True)
    return jsonify({"ok": ok, "message": msg})


@app.route("/topup_coin_with_spot_usdt", methods=["POST"])
def topup_coin_with_spot_usdt_route():
    data = request.get_json(force=True) or {}
    sym = (data.get("symbol") or "").upper().strip()
    balances, err = fetch_live_exchange_balances()
    if balances is None:
        return jsonify({"ok": False, "message": err}), 400

    usdt_avail = balances.get("USDT", {}).get("available", 0.0)
    if usdt_avail < GATEIO_MIN_ORDER_USD:
        return jsonify({"ok": False, "message": f"Available Spot USDT (${usdt_avail:.2f}) below $3.20 minimum."}), 400

    spend_usd = usdt_avail * 0.985
    ok, msg, resp_data = execute_live_exchange_order(sym, "buy", usd_amount=spend_usd)
    if ok:
        time.sleep(0.9)
        sync_all_live_cards_with_gateio(auto_buy_waiting=False, verbose=True)
        with state_lock:
            c = STATE["coins"].get(sym)
            if c:
                c["trade_usdt_proceeds"] = 0.0
                c["trade_sold_shares"] = 0.0
                c["status"] = "HOLDING_ANCHOR"
                c["protected_shares"] = max(float(c.get("protected_shares") or 0.0), float(c.get("shares") or 0.0))
        save_persistent_state(sync_db=True)
    return jsonify({"ok": ok, "message": msg})


@app.route("/edit_anchor", methods=["POST"])
def edit_anchor_route():
    data = request.get_json(force=True) or {}
    sym = (data.get("symbol") or "").upper().strip()
    new_anchor = max(GATEIO_MIN_ORDER_USD, float(data.get("anchor_usd") or 10.0))
    with state_lock:
        c = STATE["coins"].get(sym)
        if not c:
            return jsonify({"ok": False}), 404
        c["anchor_usd"] = round(new_anchor, 2)
    save_persistent_state(sync_db=True)
    return jsonify({"ok": True})


@app.route("/remove_coin", methods=["POST"])
def remove_coin_route():
    data = request.get_json(force=True) or {}
    sym = (data.get("symbol") or "").upper().strip()
    with state_lock:
        STATE["coins"].pop(sym, None)
    add_log(f"🗑️ Removed {sym} card from dashboard.")
    save_persistent_state(sync_db=True)
    return jsonify({"ok": True})


@app.route("/toggle_freeze", methods=["POST"])
def toggle_freeze_route():
    with state_lock:
        STATE["frozen"] = not bool(STATE.get("frozen", False))
        st = "FROZEN" if STATE["frozen"] else "ACTIVE"
    add_log(f"🛑 Engine state set to: {st}")
    save_persistent_state(sync_db=True)
    return jsonify({"ok": True, "frozen": STATE["frozen"]})


@app.route("/reset_profits", methods=["POST"])
def reset_profits_route():
    with state_lock:
        STATE["vault_cash"] = 0.0
        STATE["added_to_anchors"] = 0.0
        STATE["total_skims"] = 0
        STATE["skim_ledger"] = []
        STATE["profit_curve"] = [{"time": now_hms(), "value": 0.0}]
        for sym, c in STATE.get("coins", {}).items():
            c["vaulted_usd"] = 0.0
            c["share_topup_usd"] = 0.0
            c["bonus_shares_added"] = 0.0
            c["skim_count"] = 0
    add_log("🔄 Realized profit counters and ledger reset to $0.00.")
    save_persistent_state(sync_db=True)
    return jsonify({"ok": True})


@app.route("/configure_alerts", methods=["POST"])
def configure_alerts_route():
    data = request.get_json(force=True) or {}
    with state_lock:
        a = STATE["alerts"]
        a["enabled"] = bool(data.get("enabled", False))
        a["ntfy_topic"] = (data.get("ntfy_topic") or "").strip()
        a["telegram_bot_token"] = (data.get("telegram_bot_token") or "").strip()
        a["telegram_chat_id"] = (data.get("telegram_chat_id") or "").strip()
    save_persistent_state(sync_db=True)
    return jsonify({"ok": True})


@app.route("/test_alert", methods=["POST"])
def test_alert_route():
    dispatch_phone_alert(
        "VaultSkim v8.0 Test Alert",
        "Your Win-Both-Ways Quant Engine is connected and sending live alerts!",
        category="skim"
    )
    return jsonify({"ok": True})


@app.route("/export_csv")
def export_csv_route():
    with state_lock:
        rows = list(STATE.get("skim_ledger", []))
    lines = ["ID,Symbol,Mode,Stage,Price_USD,Time_UTC,Total_Profit_USD,Pure_USDT_Cut,Moonbag_Cut,Total_Shares"]
    for r in rows:
        stage_clean = str(r.get("stage") or "").replace(",", " ")
        lines.append(
            f"{r.get('id','')},{r.get('symbol','')},{r.get('mode','')},"
            f"\"{stage_clean}\",{r.get('price','')},{r.get('time','')},"
            f"{r.get('profit',0)},{r.get('vault_cut',0)},{r.get('topup_cut',0)},{r.get('shares','')}"
        )
    csv_body = "\n".join(lines)
    resp = make_response(csv_body)
    resp.headers["Content-Disposition"] = "attachment; filename=vaultskim_verified_ledger.csv"
    resp.headers["Content-Type"] = "text/csv"
    return resp


if __name__ == "__main__":
    ensure_worker_threads_running()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
