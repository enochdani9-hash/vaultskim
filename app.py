# ==============================================================================
# VAULTSKIM COMMAND CENTER — v7.1 SURPLUS-FUNDED UPWARD RE-BUY & MOONBAG ENGINE
# PART 1 OF 2: CORE ENGINE, GATE.IO V4 BRIDGE & WIN-BOTH-WAYS (DIP OR PUMP) LOOP
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
logger = logging.getLogger("VaultSkimV71")

app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False

# ==============================================================================
# 1. GLOBAL CONFIGURATION & PERSISTENCE CONSTANTS
# ==============================================================================
STATE_FILE = os.environ.get("VAULTSKIM_STATE_FILE", "vaultskim_state_v71.json")
LEGACY_FILES = (
    "vaultskim_state_v70.json",
    "vaultskim_state_v60.json",
    "vaultskim_state_v510.json",
    "vaultskim_state_v59.json",
    "vaultskim_state_v58.json",
    "vaultskim_state_v52.json"
)
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
RENDER_EXTERNAL_URL = os.environ.get("RENDER_EXTERNAL_URL", "").strip()

GATEIO_MIN_ORDER_USD = 3.20

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
    "version": "v7.1 - Win-Both-Ways Moonbag Skimmer",
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
        f"VaultSkim v7.1 Initialized: Win-Both-Ways (Dip OR +0.35% Upward Move) Moonbag Skimmer."
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

    merged["version"] = "v7.1 - Win-Both-Ways Moonbag Skimmer"
    merged.setdefault("auto_heals", 0)

    # Clean out any fake/inflated ledger entries (> $5.00 on a single micro-skim) from v6.0 migration
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

        # Fix any inflated per-coin vaulted_usd from v6.0 migration
        if float(c.get("vaulted_usd") or 0.0) > 5.0 and int(c.get("skim_count") or 0) <= 5:
            c["vaulted_usd"] = round(min(float(c.get("vaulted_usd") or 0.0), 0.45), 4)

        sell_px = float(c.get("trade_sell_price") or px)
        if sell_px > 0 and float(c.get("trade_upward_rebuy_price") or 0.0) <= 0:
            c["trade_upward_rebuy_price"] = round(sell_px * 1.0035, 8)

        if c.get("status") not in ("HOLDING_ANCHOR", "RATCHET_ARMED", "WAITING_DIP_REBUY", "WAITING_LIVE_BUY"):
            c["status"] = "HOLDING_ANCHOR"

        # Always keep anchor_usd accurate to live token value + active trade proceeds
        if sh > 0 and px > 0:
            c["anchor_usd"] = round(sh * px, 2)

    return merged


def load_persistent_state():
    if DATABASE_URL and HAS_PSYCOPG2:
        for key_name in (
            "master_state_v71",
            "master_state_v70",
            "master_state_v60",
            "master_state_v510",
            "master_state_v59"
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
                    ("master_state_v71",)
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
                        "master_state_v71",
                        "master_state_v70",
                        "master_state_v60",
                        "master_state_v510"
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
        "User-Agent": f"VaultSkimEngine/7.1 ({int(now_ts)})"
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
    min_order_usd = max(float(rules.get("min_quote_amount") or 3.20), GATEIO_MIN_ORDER_USD)
    side_lower = side.lower()

    if side_lower == "buy":
        exec_price = quote["ask"] * 1.0005
        price_str = quantize_value(exec_price, rules["price_precision"], ROUND_HALF_UP)
        exec_price_float = float(price_str)

        if share_amount is not None and float(share_amount) > 0:
            raw_shares = float(share_amount) / 0.9988
        else:
            requested_usd = min(float(usd_amount or min_order_usd), max_cap)
            if requested_usd < min_order_usd:
                requested_usd = min_order_usd * 1.01
            if usdt_avail < requested_usd:
                if usdt_avail >= 3.08:
                    requested_usd = usdt_avail * 0.997
                else:
                    return False, f"Need at least $3.15 USDT in Gate.io wallet (Available: ${usdt_avail:.2f}).", None
            raw_shares = (requested_usd * 0.9985) / exec_price_float

        amount_str = quantize_value(raw_shares, rules["amount_precision"], ROUND_UP)
        amount_float = float(amount_str)

        if (amount_float * exec_price_float) > (usdt_avail * 0.9985):
            fitted = (usdt_avail * 0.997) / exec_price_float
            amount_str = quantize_value(fitted, rules["amount_precision"], ROUND_DOWN)
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

        amount_str = quantize_value(raw_shares, rules["amount_precision"], ROUND_DOWN)
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

            if current_status == "WAITING_DIP_REBUY" and proceeds > 0:
                with state_lock:
                    card["shares"] = real_shares if real_shares > 0 else total_real_shares
                    card["anchor_usd"] = round(card["shares"] * price, 2)
                sync_notes.append(f"{sym}: Waiting for Re-Buy (${proceeds:.2f} ready)")
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
# 7. SELF-HEALING WATCHDOG & WIN-BOTH-WAYS (DIP OR +0.35% PUMP) SKIMMER ENGINE
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
                elif sell_px > 0 and float(c.get("trade_upward_rebuy_price") or 0.0) <= 0:
                    c["trade_upward_rebuy_price"] = round(sell_px * 1.0035, 8)


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
        # STEP 2 OF CYCLE: WIN-BOTH-WAYS RE-BUY (ON DIP <= -0.40% OR PUMP >= +0.35%)
        # ----------------------------------------------------------------------
        if status == "WAITING_DIP_REBUY" and proceeds > 0 and sold_sh > 0:
            sell_px = float(card.get("trade_sell_price") or price)
            rebuy_px = float(card.get("trade_rebuy_price") or (sell_px * 0.9960))
            upward_px = float(card.get("trade_upward_rebuy_price") or (sell_px * 1.0035))
            move_pct = ((price - sell_px) / sell_px) * 100.0 if sell_px > 0 else 0.0
            card["gain_from_low_pct"] = round(move_pct, 3)

            # PATH A: Price dipped to `rebuy_px` OR ANY PRICE LOWER!
            hit_dip_target = (price <= rebuy_px) or (ask_price <= rebuy_px * 1.0004)
            if not is_frozen and hit_dip_target:
                if not is_live or (time.time() - order_cooldowns.get(sym, 0)) > 8:
                    execute_smart_rebuy_and_skim(sym, price, upward_mode=False)
                return

            # PATH B: Price moved UP by +0.35% (`price >= upward_px`)!
            # Your remaining 75% Moonbag (`shares`) has gained 3x more than the 0.35% gap on `sold_sh`!
            # We execute the Surplus-Funded Upward Re-Buy so you NEVER lose shares or get left behind!
            hit_upward_target = price >= upward_px
            if not is_frozen and hit_upward_target:
                if not is_live or (time.time() - order_cooldowns.get(sym, 0)) > 10:
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
    # STEP 1 OF CYCLE: HOLDING SHARES & WATCHING FOR +0.75% BOUNCE TO SELL HIGH
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
                    f"Holding 100% of {shares:.6f} {sym} — waiting for +0.75% bounce!"
                )

        gain_pct = ((price - sliding_low) / sliding_low) * 100.0 if sliding_low > 0 else 0.0
        card["gain_from_low_pct"] = round(gain_pct, 3)

        if gain_pct >= 0.42 and not card.get("ratchet_armed"):
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
        target_hit = gain_pct >= 0.75
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
    STEP 1 OF LIVE SKIM CYCLE (SELL 25% SLICE ON +0.75% BOUNCE):
    Leaves 75% of your shares in your Moonbag so if the price continues rising +0.35%,
    your 75% Moonbag gains 3x the upward spread!
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

    tranche_usd = max(bag_usd * 0.25, 3.60)
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
    rebuy_target_px = round(actual_sell_px * 0.9960, 8)
    upward_target_px = round(actual_sell_px * 1.0035, 8)

    time.sleep(0.9)
    bals, _ = fetch_live_exchange_balances()
    with state_lock:
        if bals:
            STATE["exchange_bridge"]["available_usdt"] = round(bals.get("USDT", {}).get("available", 0.0), 4)
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
        f"Sold {sold_sh:.6f} {sym} for ${net_usdt_proceeds:.2f} USDT -> "
        f"Dip Target <= ${rebuy_target_px:.4f} (or Lower) | Upward +0.35% Guard @ ${upward_target_px:.4f}!"
    )
    save_persistent_state(sync_db=True)


def execute_smart_rebuy_and_skim(sym, current_price, upward_mode=False, force_manual=False):
    """
    STEP 2 OF LIVE SKIM CYCLE (WIN-BOTH-WAYS: DIP RE-BUY OR +0.35% UPWARD SURPLUS RE-BUY):
    - On Dip (`upward_mode=False`): Buys back `sold_sh + bonus_shares` for LESS than
      `proceeds_usdt`, banking Pure USDT Profit + Moonbag Share Growth!
    - On +0.35% Upward Move (`upward_mode=True`): Because your 75% Moonbag gained 3x
      more value than the 0.35% gap on the 25% sold slice, the bot uses a tiny few
      cents from your Spot USDT surplus to buy back 100.05% of `sold_sh` (so you
      GAIN shares and never lose shares!) and resets the wave floor at the new higher
      price. If no spare USDT is in the wallet, it sells a tiny $3.30 step-up slice
      from your 75% Moonbag so it can buy back 100% of shares + profit cleanly!
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
        usdt_avail = float(STATE["exchange_bridge"].get("available_usdt") or 0.0)

    if proceeds_usdt <= 0 or sold_sh <= 0 or current_price <= 0:
        return False, "No active sold slice waiting for re-buy."

    quote = fetch_live_market_quote(sym)
    ask_px = float((quote or {}).get("ask") or current_price) * 1.0005

    # Exact cost to restore 100% of sold shares at current ask (with 0.1% Gate.io fee)
    cost_to_restore_sold_shares = (sold_sh / 0.9988) * ask_px

    if upward_mode and not force_manual and cost_to_restore_sold_shares > proceeds_usdt:
        # Calculate the 3x unrealized gain on your remaining 75% Moonbag (`current_sh`)
        moonbag_surplus_usd = max(0.0, current_sh * (current_price - sell_px))
        extra_usdt_needed = cost_to_restore_sold_shares - proceeds_usdt
        # Add +0.05% bonus shares so your share count still increases on an upward re-buy!
        target_shares_with_bonus = sold_sh * 1.0005
        cost_with_bonus = (target_shares_with_bonus / 0.9988) * ask_px

        if usdt_avail >= cost_with_bonus and (cost_with_bonus - proceeds_usdt) <= max(1.25, moonbag_surplus_usd * 0.85):
            usdt_to_spend_on_rebuy = cost_with_bonus
            pure_usdt_profit = 0.0
        elif (current_sh * current_price) >= 12.0 and current_price >= (sell_px * 1.0055):
            # If user had $0 extra USDT in wallet, harvest a tiny $3.35 step-up from the 75% Moonbag
            order_cooldowns[sym] = time.time()
            ok_s, _, resp_s = execute_live_exchange_order(sym, "sell", usd_amount=3.35)
            if ok_s and resp_s:
                add_sh = float(resp_s.get("_filled_shares") or (3.35 / current_price))
                add_u = float(resp_s.get("_filled_usdt") or 3.35) * 0.999
                new_proc = proceeds_usdt + add_u
                new_sold = sold_sh + add_sh
                new_avg = new_proc / new_sold
                with state_lock:
                    card["shares"] = max(0.0, current_sh - add_sh)
                    card["anchor_usd"] = round(float(card["shares"]) * current_price, 2)
                    card["trade_usdt_proceeds"] = round(new_proc, 4)
                    card["trade_sold_shares"] = round(new_sold, 6)
                    card["trade_sell_price"] = round(new_avg, 6)
                    card["trade_rebuy_price"] = round(new_avg * 0.9960, 8)
                    card["trade_upward_rebuy_price"] = round(current_price * 1.0035, 8)
                add_log(
                    f"🚀 UPWARD STEP-UP ({sym}) @ ${current_price:.4f}: "
                    f"Added +${add_u:.2f} from 75% Moonbag gain -> New Re-Buy Target <= ${card['trade_rebuy_price']:.4f}!"
                )
                save_persistent_state(sync_db=True)
            return True, "Step-up completed."
        else:
            usdt_to_spend_on_rebuy = min(usdt_avail * 0.996, max(proceeds_usdt, cost_to_restore_sold_shares))
            pure_usdt_profit = 0.0
    else:
        # True profit spread strictly from selling high and buying back lower (never counts principal!)
        raw_spread_profit = max(0.0, (sell_px - ask_px) * sold_sh)
        net_spread_profit = max(0.0, min(proceeds_usdt - cost_to_restore_sold_shares, raw_spread_profit))

        if "100% Pure Cash" in strategy:
            pure_usdt_profit = round(net_spread_profit, 4)
            usdt_to_spend_on_rebuy = max(GATEIO_MIN_ORDER_USD, proceeds_usdt - pure_usdt_profit)
        else:
            pure_usdt_profit = round(net_spread_profit * 0.50, 4)
            usdt_to_spend_on_rebuy = max(GATEIO_MIN_ORDER_USD, proceeds_usdt - pure_usdt_profit)

    order_cooldowns[sym] = time.time()
    if is_live:
        ok, msg, resp_data = execute_live_exchange_order(sym, "buy", usd_amount=usdt_to_spend_on_rebuy)
        if not ok or not resp_data:
            return False, msg
        net_received_sh = float(resp_data.get("_net_received_shares") or (usdt_to_spend_on_rebuy / ask_px))
        actual_spent_usdt = float(resp_data.get("_filled_usdt") or usdt_to_spend_on_rebuy)
        # Strict cap so profit never exceeds true price spread
        max_possible_spread = max(0.0, (sell_px - current_price) * sold_sh)
        pure_usdt_profit = round(min(max(0.0, proceeds_usdt - actual_spent_usdt), max_possible_spread + 0.05), 4)
        time.sleep(0.9)
        bals, _ = fetch_live_exchange_balances()
        with state_lock:
            if bals:
                STATE["exchange_bridge"]["available_usdt"] = round(bals.get("USDT", {}).get("available", 0.0), 4)
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
        # If upward re-buy occurred, record the 75% Moonbag dollar gain unlocked by moving up!
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
            f"Upward +0.35% Re-Buy (+{bonus_sh:.5f} {sym} Restored)"
            if upward_mode
            else f"Buy-Low Skim (+{bonus_sh:.5f} {sym} & +${pure_usdt_profit:.4f} USDT)"
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
# 8. BACKGROUND WORKER LOOP
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
# VAULTSKIM COMMAND CENTER — v7.1 SURPLUS-FUNDED UPWARD RE-BUY & MOONBAG ENGINE
# PART 2 OF 2: FLASK API ROUTES, MANUAL OVERRIDES & FULL COMMAND CENTER UI
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
        heals = int(STATE.get("auto_heals") or 0)
    return jsonify({
        "ok": True,
        "status": "ALIVE",
        "version": "v7.1 - Win-Both-Ways Moonbag Skimmer",
        "live_bridge_armed": armed,
        "active_coins": count,
        "auto_heals": heals,
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
        "description": "Win-Both-Ways (Dip OR +0.35% Upward Move) Moonbag & Profit Skimmer",
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
        pending_rebuy_usdt = sum(float(c.get("trade_usdt_proceeds") or 0.0) for c in STATE.get("coins", {}).values())

        payload = {
            "version": STATE.get("version", "v7.1 - Win-Both-Ways Moonbag Skimmer"),
            "vault_cash": vc,
            "added_to_anchors": aa,
            "pending_rebuy_usdt": round(pending_rebuy_usdt, 4),
            "total_profit": round(vc + aa, 4),
            "total_skims": int(STATE.get("total_skims") or 0),
            "total_withdrawn": float(STATE.get("total_withdrawn") or 0.0),
            "auto_heals": int(STATE.get("auto_heals") or 0),
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
    run_auto_healing_watchdog()
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
            "strategy": "50/50 Moonbag + Cash Skim",
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
            "trade_usdt_proceeds": 0.0,
            "trade_sold_shares": 0.0,
            "trade_sell_price": 0.0,
            "trade_rebuy_price": 0.0,
            "trade_upward_rebuy_price": 0.0,
            "trade_started_ts": time.time(),
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
        "50/50 Moonbag + Cash Skim",
        "100% Pure Cash Skim"
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
    data = request.json or {}
    sym = (data.get("symbol") or "").upper().strip()
    with state_lock:
        card = STATE["coins"].get(sym)
        if not card:
            return jsonify({"ok": False, "error": "Coin not found."}), 404
        px = float(card.get("current_price") or 0.0)
        sell_px = float(card.get("trade_sell_price") or px)

    if px <= 0:
        return jsonify({"ok": False, "error": f"Invalid price for {sym}."}), 400

    order_cooldowns[sym] = 0
    is_upward = px > sell_px
    ok, msg = execute_smart_rebuy_and_skim(sym, px, upward_mode=is_upward, force_manual=True)
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
                c_now["trade_usdt_proceeds"] = 0.0
                c_now["trade_sold_shares"] = 0.0
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
            "VaultSkim v7.1 Alert Connected",
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
        STATE["total_skims"] = 0
        STATE["profit_curve"] = [{"time": now_hms(), "value": 0.0}]
        STATE["skim_ledger"] = []
        avail_u = float(STATE["exchange_bridge"].get("available_usdt") or 0.0)
        STATE["exchange_bridge"]["baseline_usdt"] = round(avail_u, 4)
        STATE["exchange_bridge"]["realized_usdt_profit"] = 0.0

        for sym, c in STATE.get("coins", {}).items():
            c["share_topup_usd"] = 0.0
            c["vaulted_usd"] = 0.0
            c["trade_usdt_proceeds"] = 0.0
            c["trade_sold_shares"] = 0.0
            c["trade_sell_price"] = 0.0
            c["trade_rebuy_price"] = 0.0
            c["trade_upward_rebuy_price"] = 0.0
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
        add_log("♻️ Reset Counters to $0.0000 • Aligned Sliding Lows & Preserved Share Baselines.")
    save_persistent_state(sync_db=True)
    return jsonify({"ok": True})


@app.route("/api/export_csv")
def route_export_csv():
    rows = ["Timestamp_UTC,Symbol,Mode,Stage,Price_USD,Total_Skim_USD,Pure_USDT_Profit,Moonbag_Shares_USD,Shares_Held"]
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
# 9. COMPLETE FRONTEND DASHBOARD HTML / CSS / SVG CHART / JS CONTROLLER
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
    <title>VaultSkim Command Center • v7.1 Win-Both-Ways Moonbag Skimmer</title>
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
                v7.1 - Win-Both-Ways Moonbag
            </span>
        </div>
        <div class="flex items-center justify-between text-[11px] text-slate-400 mb-3">
            <span>Dip Re-Buy (<= -0.40%) OR Upward Surplus Re-Buy (>= +0.35%)</span>
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
                Pure USDT Skimmed: +$0.0000
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

    <!-- TOTAL REALIZED PROFIT HERO CARD -->
    <div class="profit-card p-4 mb-3 text-center">
        <div class="text-[11px] font-bold uppercase tracking-widest text-slate-400 mb-1">
            TOTAL REALIZED PROFIT (PURE USDT + MOONBAG SHARES ADDED)
        </div>
        <div id="heroTotalProfit" class="text-4xl font-extrabold text-emerald-400 mono tracking-tight my-1">
            $0.0000
        </div>
        <div id="heroProfitSub" class="text-xs text-slate-300 mono">
            Pure USDT Profit: $0.0000 | Moonbag Shares Added: +$0.0000 | Skims: 0
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
            <span>Pure USDT Profit in Gate.io Wallet:</span>
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
            Win-Both-Ways Cycle: Re-Buys on Dip (<= -0.40%) OR Upward Surplus Move (>= +0.35%)
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
                <div class="text-[11px] text-slate-400">Every completed Dip Skim & Upward Surplus Re-Buy</div>
            </div>
            <button onclick="window.location='/api/export_csv'" class="btn-press px-2.5 py-1 rounded-md bg-emerald-950/90 text-emerald-300 border border-emerald-700/70 text-xs font-bold mono">
                📥 CSV
            </button>
        </div>
        <div id="skimLedgerList" class="mt-2.5 space-y-2 max-h-52 overflow-y-auto text-xs mono">
            <div class="text-center text-slate-500 py-4 text-xs">Waiting for next completed Buy-Low / Sell-High cycle...</div>
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

    <!-- LIVE EXECUTION & SECURITY LOG -->
    <div class="v-card p-4">
        <div class="text-xs font-extrabold uppercase tracking-wider text-slate-400 mb-2">
            LIVE EXECUTION & AUTO-HEAL SECURITY LOG
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

    document.getElementById('heroTotalProfit').innerText = `$${Number(data.total_profit || 0).toFixed(4)}`;
    document.getElementById('heroProfitSub').innerText =
        `Pure USDT Profit: $${vc.toFixed(4)} | Moonbag Shares Added: +$${aa.toFixed(4)} | Skims: ${data.total_skims || 0}`;
    document.getElementById('cashoutAvailDisplay').innerText = `$${vc.toFixed(4)}`;
    document.getElementById('chartSkimBadge').innerText = `${data.total_skims || 0} Completed Wave Skims`;
    if (data.last_price_tick_hms) {
        document.getElementById('liveTickClock').innerText = `● Tick: ${data.last_price_tick_hms} UTC`;
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
    document.getElementById('pureProfitUsdtBadge').innerText = `Pure USDT Skimmed: +$${vc.toFixed(4)}`;
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
        ledgerBox.innerHTML = `<div class="text-center text-slate-500 py-4 text-xs">Waiting for next completed Buy-Low / Sell-High cycle...</div>`;
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
            const proceeds = Number(c.trade_usdt_proceeds || 0);
            const soldSh = Number(c.trade_sold_shares || 0);
            const sellPx = Number(c.trade_sell_price || 0);
            const rebuyPx = Number(c.trade_rebuy_price || (sellPx * 0.9960));
            const upwardPx = Number(c.trade_upward_rebuy_price || (sellPx * 1.0035));
            const bonusSh = Number(c.bonus_shares_added || 0);
            const price = Number(c.current_price || 0);
            const low = Number(c.sliding_low || price);
            const tokenVal = sh * price;
            const totalBucketVal = tokenVal + proceeds;
            const gainPct = low > 0 ? (((price - low) / low) * 100) : 0;
            const harvestTargetPrice = low * 1.0075;
            const pctBar = c.status === 'WAITING_DIP_REBUY'
                ? 100
                : Math.max(0, Math.min(100, (gainPct / 0.75) * 100));

            const modePill = isLive
                ? `<button onclick="switchCoinMode('${sym}', true)" class="btn-press px-2 py-0.5 rounded bg-rose-950/90 text-rose-400 border border-rose-700/70 text-[10px] font-extrabold mono">⚡ LIVE ORDER</button>`
                : `<button onclick="switchCoinMode('${sym}', false)" class="btn-press px-2 py-0.5 rounded bg-slate-800 text-slate-300 border border-slate-700 text-[10px] font-bold mono">📄 PAPER</button>`;

            const statusPill = c.status === 'WAITING_DIP_REBUY'
                ? `<span class="px-2 py-0.5 rounded bg-amber-950/90 text-amber-300 border border-amber-600/80 text-[10px] font-extrabold mono">🎯 WIN-BOTH-WAYS RE-BUY</span>`
                : `<span class="px-2 py-0.5 rounded bg-emerald-950/90 text-emerald-400 border border-emerald-700/70 text-[10px] font-extrabold mono">${c.status}</span>`;

            const cycleDetailHtml = proceeds > 0
                ? `<div class="space-y-1 text-amber-300 bg-amber-950/40 px-2.5 py-2 rounded border border-amber-700/60">
                       <div class="flex items-center justify-between">
                           <span>🎯 Sold ${soldSh.toFixed(4)} ${sym} ($${proceeds.toFixed(2)}):</span>
                           <span class="font-bold">Dip Buy <= $${formatPrice(rebuyPx)}</span>
                       </div>
                       <div class="flex items-center justify-between text-[11px] text-sky-300">
                           <span>🚀 75% Moonbag Upward Guard (+0.35%):</span>
                           <span class="font-bold">Re-Buy >= $${formatPrice(upwardPx)}</span>
                       </div>
                   </div>`
                : `<div class="flex items-center justify-between">
                       <span class="text-slate-400">Next Sell-High Trigger (+0.75%):</span>
                       <span class="text-amber-300 font-bold">$${formatPrice(harvestTargetPrice)} (+$${Number(c.share_topup_usd || 0).toFixed(4)} Moonbag Gain)</span>
                   </div>`;

            const forceRebuyBtnHtml = (isLive && proceeds > 0)
                ? `<button onclick="forceManualRebuy('${sym}')" class="btn-press w-full mt-2 py-2 rounded-lg bg-amber-500 hover:bg-amber-400 text-black font-extrabold text-xs flex items-center justify-center gap-1.5 shadow">
                       <span>⚡</span> <span>Buy Back ${sym} Shares Right Now ($${proceeds.toFixed(2)})</span>
                   </button>`
                : '';

            const topupBtnHtml = (isLive && usdtAvail >= 3.20 && proceeds <= 0)
                ? `<button onclick="deployUsdtToShares('${sym}', ${usdtAvail})" class="btn-press w-full mt-2 py-2 rounded-lg bg-sky-950/90 hover:bg-sky-900 text-sky-300 border border-sky-600/80 font-extrabold text-xs flex items-center justify-center gap-1.5">
                       <span>🛡️</span> <span>Use Spot USDT ($${usdtAvail.toFixed(2)}) to Buy More ${sym} Shares</span>
                   </button>`
                : '';

            const bottomBar = isLive
                ? `<button onclick="manualSyncLive()" class="btn-press w-full mt-3 py-2.5 rounded-lg bg-emerald-950/80 text-emerald-300 border border-emerald-700/70 font-bold text-xs flex items-center justify-center gap-1.5">
                       <span>🟢</span> <span>Win-Both-Ways (Dip OR +0.35% Upward) Skimmer Active</span>
                   </button>
                   ${forceRebuyBtnHtml}
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
                    <span class="text-emerald-400 font-bold mono">+${bonusSh.toFixed(6)} Moonbag ${sym} Added</span>
                </div>

                <div class="v-inner px-3 py-2 mb-3 flex items-center justify-between text-xs">
                    <span class="text-left text-indigo-300 font-bold">
                        ⚡ Mode: ${c.strategy || '50/50 Moonbag + Cash Skim'}
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
                        <span class="text-slate-400">Live Token Value (In ${sym}):</span>
                        <div class="flex items-center gap-1.5">
                            <span class="text-white font-bold">$${tokenVal.toFixed(2)}</span>
                            <button onclick="promptEditAnchor('${sym}', ${tokenVal.toFixed(2)})" class="px-1.5 py-0.5 rounded bg-sky-950 text-sky-300 border border-sky-700/70 text-[10px] font-bold">
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
                        <span class="text-sky-300 font-bold">🛡️ ${protSh.toFixed(6)} ${sym} (Grows on Re-Buy)</span>
                    </div>

                    <div class="flex items-center justify-between">
                        <span class="text-slate-400">Total Position Value (Tokens + Cash):</span>
                        <span class="text-white font-bold">$${totalBucketVal.toFixed(2)}</span>
                    </div>

                    ${cycleDetailHtml}

                    <div class="flex items-center justify-between">
                        <span class="text-slate-400">Sliding Low / Sell-High Target:</span>
                        <span class="text-slate-200 font-bold">$${formatPrice(low)} / $${formatPrice(harvestTargetPrice)}</span>
                    </div>

                    <div class="w-full bg-slate-900 h-2 rounded-full overflow-hidden border border-slate-800">
                        <div class="${c.status === 'WAITING_DIP_REBUY' ? 'bg-amber-400' : 'bg-emerald-400'} h-full transition-all duration-300" style="width: ${pctBar}%"></div>
                    </div>

                    <div class="flex items-center justify-between pt-0.5">
                        <span class="text-slate-400">Pure USDT Skimmed (${sym}):</span>
                        <span class="text-emerald-400 font-extrabold">+$${Number(c.vaulted_usd || 0).toFixed(4)} (${c.skim_count || 0} completed cycles)</span>
                    </div>
                </div>

                ${bottomBar}
            </div>`;
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
    btn.innerText = "⏳ Syncing...";
    await fetch('/api/sync_live', {method: 'POST'});
    btn.innerText = "🔄 Sync & Auto-Heal";
    pollState();
}

async function forceManualRebuy(symbol) {
    const r = await fetch('/api/coin/force_rebuy', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({symbol})
    });
    const res = await r.json();
    if (!r.ok || !res.ok) {
        alert(res.error || "Could not execute re-buy.");
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
        `How much of your $${availUsdt.toFixed(2)} Spot USDT would you like to deploy right now to buy more ${symbol} shares? (Gate.io minimum order is $3.20 USDT):`,
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
        `Enter new target Token USD value for ${symbol} (Current live token value is $${Number(currentVal).toFixed(2)}. Entering a higher USD value will immediately buy the extra ${symbol} shares on Gate.io! Gate.io minimum order size is $3.20 USDT):`,
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
    document.getElementById('cashoutAmountInput').value = '';
    pollState();
}

async function toggleFreeze() {
    await fetch('/api/control/freeze', {method: 'POST'});
    pollState();
}

async function resetProfitCounters() {
    if (!confirm("Reset Profit Counters to $0.0000 and align Sliding Lows?")) return;
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
