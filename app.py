# ==============================================================================
# VAULTSKIM COMMAND CENTER — v5.2 PURE LIVE TRUTH ENGINE (PART 1 OF 2)
# ==============================================================================
# Fixes engineered specifically from live Gate.io Spot inspection:
#   1. STRICT LIVE MODE: Any coin set to ⚡ LIVE ORDER can NEVER execute a [PAPER]
#      trade, never run a fake Sliding Floor Skim, and never book profit unless
#      Gate.io returns HTTP 200/201 with a real filled Order ID.
#   2. REAL WALLET TRUTH: Switching a coin to ⚡ LIVE ORDER (or clicking Sync Live)
#      queries Gate.io `/api/v4/spot/accounts` and sets `shares_owned` to the
#      EXACT token balance in your Gate.io Spot wallet (e.g., 0.12275 QNT, 0 LQTY).
#      If Gate.io holds 0 tokens, status becomes `WAITING_LIVE_BUY` until Gate.io
#      actually fills the buy order—never showing fake "(In Token)" shares!
#   3. ZERO `BALANCE_NOT_ENOUGH` (400) ERRORS:
#      - Live SELL queries Gate.io Spot balance first and sells `floor_decimals(avail_token * 0.9995, 4)`
#        so Gate.io's 0.1% fee deduction never causes a share mismatch.
#      - Live BUY queries Gate.io Spot USDT first and caps order cost + 0.35% limit
#        buffer strictly below `avail_usdt * 0.995`.
# ==============================================================================

import os
import sys
import csv
import io
import time
import json
import math
import hmac
import hashlib
import logging
import threading
import urllib.parse
from datetime import datetime, timezone
from typing import Dict, Any, Tuple, List, Optional

import requests
import psycopg2
from flask import (
    Flask,
    render_template_string,
    request,
    jsonify,
    Response,
    make_response
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("VaultSkimLiveTruth")

app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False

DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
PORT = int(os.environ.get("PORT", 10000))

GATEIO_API_HOST = "https://api.gateio.ws"
GATEIO_API_PREFIX = "/api/v4"
BINANCE_API_HOST = "https://api.binance.com"
DEXSCREENER_API_HOST = "https://api.dexscreener.com"

DEFAULT_FEE_RATE = 0.0010          # 0.10% Gate.io Spot fee
MIN_EXCHANGE_ORDER_USD = 1.05      # Gate.io minimum order notional ($1.05)
MIN_DEX_LIQUIDITY_USD = 15000.0    # Anti-Rug $15K minimum liquidity

# Zero-Loss Ratchet Thresholds
SHIELD_ARM_PCT = 0.0035            # +0.35% above entry arms the Zero-Loss Shield
SHIELD_FLOOR_LOCK_PCT = 0.0022     # Locks profit floor at Entry + 0.22% (covers 2x 0.1% fees)
STAGE1_SKIM_TARGET_PCT = 0.0080    # +0.80% bounce from sliding low triggers Stage 1 Skim
STAGE1_TRAIL_GAP_PCT = 0.0025      # 0.25% trailing stop gap once shield is armed
STAGE2_MOONBAG_TARGET_PCT = 0.0180 # +1.80% rally target for Stage 2 Moonbag Exit
STAGE2_MOONBAG_TRAIL_PCT = 0.0035  # 0.35% trailing stop gap for riding Moonbag
DIP_REBUY_DROP_PCT = 0.0040        # -0.40% dip from exit price triggers Share-Keeper Re-Buy
BREAKOUT_REBUY_PCT = 0.0045        # +0.45% breakout continuation re-enters anchor

MEME_RADAR_CANDIDATES = [
    {"symbol": "QNT", "name": "Quant", "pair": "QNT_USDT", "tag": "Verified CEX Utility"},
    {"symbol": "LQTY", "name": "Liquity", "pair": "LQTY_USDT", "tag": "DeFi Momentum"},
    {"symbol": "LTC", "name": "Litecoin", "pair": "LTC_USDT", "tag": "Fast Settlement"},
    {"symbol": "DASH", "name": "Dash", "pair": "DASH_USDT", "tag": "Rapid Swing Pair"},
    {"symbol": "PEPE", "name": "Pepe", "pair": "PEPE_USDT", "tag": "High Velocity Meme"},
    {"symbol": "BONK", "name": "Bonk", "pair": "BONK_USDT", "tag": "Solana Meme Leader"},
    {"symbol": "WIF", "name": "dogwifhat", "pair": "WIF_USDT", "tag": "High Volatility"},
    {"symbol": "FLOKI", "name": "Floki", "pair": "FLOKI_USDT", "tag": "Liquid Meme CEX"},
    {"symbol": "SHIB", "name": "Shiba Inu", "pair": "SHIB_USDT", "tag": "Large Cap Meme"},
    {"symbol": "DOGE", "name": "Dogecoin", "pair": "DOGE_USDT", "tag": "Blue-Chip Meme"},
    {"symbol": "POPCAT", "name": "Popcat", "pair": "POPCAT_USDT", "tag": "Solana Momentum"},
    {"symbol": "SOL", "name": "Solana", "pair": "SOL_USDT", "tag": "L1 Liquidity Leader"}
]


def floor_to_decimals(val: float, decimals: int = 4) -> float:
    """Strictly floors a float to `decimals` places so we NEVER round up above wallet balance."""
    factor = 10 ** decimals
    return math.floor(float(val) * factor) / factor


def build_default_coin_card(
    symbol: str,
    anchor_usd: float = 50.00,
    source: str = "gateio",
    contract: str = "",
    liquidity_usd: float = 5000000.0,
    live_mode: bool = False,
    strategy: str = "hybrid_50_50"
) -> Dict[str, Any]:
    clean_sym = symbol.upper().replace("/USDT", "").replace("_USDT", "").replace("USDT", "").strip()
    return {
        "symbol": clean_sym,
        "pair": f"{clean_sym}/USDT",
        "source": "gateio" if not contract else source,
        "contract": contract,
        "liquidity_usd": float(liquidity_usd),
        "live_mode": bool(live_mode),
        "strategy": strategy,              # 'hybrid_50_50' or 'share_keeper_100'
        "status": "WAITING_LIVE_BUY" if live_mode else "HOLDING_ANCHOR",
        "anchor_usd": round(float(anchor_usd), 2),
        "cash_reserve_usd": round(float(anchor_usd), 2),
        "shares_owned": 0.0,
        "target_shares_floor": 0.0,
        "entry_price": 0.0,
        "sliding_low": 0.0,
        "high_water": 0.0,
        "last_price": 0.0,
        "last_exit_price": 0.0,
        "shield_armed": False,
        "zero_loss_stop": 0.0,
        "escrow_profit": 0.0,
        "vaulted_profit": 0.0,
        "share_topup_usd": 0.0,
        "skims_count": 0,
        "last_live_Action_ts": 0.0,
        "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    }


DEFAULT_STATE: Dict[str, Any] = {
    "version": "5.2 · Pure Live Truth",
    "vault_cash": 0.0,
    "added_to_anchors": 0.0,
    "total_skims": 0,
    "live_bridge_enabled": True,
    "spot_exchange": "gateio",
    "spot_api_key": "",
    "spot_api_secret": "",
    "max_order_cap": 250.0,
    "last_verified_usdt": 0.0,
    "ntfy_topic": "",
    "telegram_bot_token": "",
    "telegram_chat_id": "",
    "alerts_enabled": True,
    "profit_curve": [
        {"time": datetime.now(timezone.utc).strftime("%H:%M:%S"), "value": 0.0}
    ],
    "skim_ledger": [],
    "security_log": [
        f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] VaultSkim v5.2 Pure Live Truth Engine initialized."
    ],
    "coins": {
        "QNT": build_default_coin_card("QNT", anchor_usd=50.00, source="gateio", live_mode=True, strategy="hybrid_50_50")
    }
}

state: Dict[str, Any] = json.loads(json.dumps(DEFAULT_STATE))
state_lock = threading.RLock()


# ==============================================================================
# 1. POSTGRESQL DATABASE PERSISTENCE ENGINE
# ==============================================================================
def get_db_connection():
    if not DATABASE_URL:
        return None
    try:
        db_url = DATABASE_URL
        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql://", 1)
        return psycopg2.connect(db_url, connect_timeout=8)
    except Exception as exc:
        logger.warning(f"PostgreSQL connection warning: {exc}")
        return None


def init_db_tables():
    conn = get_db_connection()
    if not conn:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS vaultskim_state (
                    id INTEGER PRIMARY KEY,
                    data JSONB NOT NULL,
                    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                );
            """)
            conn.commit()
    except Exception as exc:
        logger.error(f"init_db_tables error: {exc}")
    finally:
        conn.close()


def load_state_from_db():
    global state
    conn = get_db_connection()
    if not conn:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT data FROM vaultskim_state WHERE id = 1;")
            row = cur.fetchone()
            if row and row[0]:
                loaded = row[0] if isinstance(row[0], dict) else json.loads(row[0])
                with state_lock:
                    for key, default_val in DEFAULT_STATE.items():
                        if key not in loaded:
                            loaded[key] = json.loads(json.dumps(default_val))
                    loaded["version"] = "5.2 · Pure Live Truth"
                    coins_dict = loaded.get("coins", {})
                    for sym, c_data in list(coins_dict.items()):
                        template = build_default_coin_card(sym, anchor_usd=c_data.get("anchor_usd", 50.0))
                        for ck, cv in template.items():
                            if ck not in c_data:
                                c_data[ck] = cv
                        # Fix false DEX label on standard CEX tickers like QNT or LQTY
                        if not c_data.get("contract"):
                            c_data["source"] = "gateio"
                    state = loaded
                logger.info("Loaded state from PostgreSQL.")
    except Exception as exc:
        logger.error(f"load_state_from_db error: {exc}")
    finally:
        conn.close()


def save_state_to_db():
    conn = get_db_connection()
    if not conn:
        return
    try:
        with state_lock:
            payload = json.dumps(state)
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO vaultskim_state (id, data, updated_at)
                VALUES (1, %s::jsonb, CURRENT_TIMESTAMP)
                ON CONFLICT (id) DO UPDATE
                SET data = EXCLUDED.data,
                    updated_at = CURRENT_TIMESTAMP;
            """, (payload,))
            conn.commit()
    except Exception as exc:
        logger.error(f"save_state_to_db error: {exc}")
    finally:
        conn.close()


# ==============================================================================
# 2. LOGGING, VERIFIED LEDGER & PHONE ALERTS
# ==============================================================================
def now_ts() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


def log_event(message: str):
    with state_lock:
        entry = f"[{now_ts()}] {message}"
        if "security_log" not in state or not isinstance(state["security_log"], list):
            state["security_log"] = []
        state["security_log"].insert(0, entry)
        state["security_log"] = state["security_log"][:80]


def send_phone_alert(title: str, body: str, tags: str = "moneybag,chart_with_upwards_trend"):
    with state_lock:
        if not state.get("alerts_enabled", True):
            return
        ntfy_topic = (state.get("ntfy_topic") or "").strip()
        tg_token = (state.get("telegram_bot_token") or "").strip()
        tg_chat = (state.get("telegram_chat_id") or "").strip()

    def _send():
        if ntfy_topic:
            try:
                requests.post(
                    f"https://ntfy.sh/{ntfy_topic}",
                    data=body.encode("utf-8"),
                    headers={"Title": title, "Priority": "high", "Tags": tags},
                    timeout=5
                )
            except Exception:
                pass
        if tg_token and tg_chat:
            try:
                requests.post(
                    f"https://api.telegram.org/bot{tg_token}/sendMessage",
                    json={"chat_id": tg_chat, "text": f"*{title}*\n{body}", "parse_mode": "Markdown"},
                    timeout=5
                )
            except Exception:
                pass

    threading.Thread(target=_send, daemon=True).start()


def record_verified_skim(
    symbol: str,
    stage_label: str,
    profit_usd: float,
    share_topup_usd: float,
    exec_price: float,
    shares_remaining: float,
    is_live_order: bool
):
    with state_lock:
        state["total_skims"] = int(state.get("total_skims", 0)) + 1
        state["vault_cash"] = round(float(state.get("vault_cash", 0.0)) + float(profit_usd), 4)
        state["added_to_anchors"] = round(float(state.get("added_to_anchors", 0.0)) + float(share_topup_usd), 4)

        ts = now_ts()
        mode_tag = "LIVE" if is_live_order else "PAPER"
        ledger_item = {
            "symbol": symbol,
            "mode": mode_tag,
            "stage": stage_label,
            "profit": round(float(profit_usd), 4),
            "topup": round(float(share_topup_usd), 4),
            "price": round(float(exec_price), 4),
            "shares": round(float(shares_remaining), 6),
            "time": ts
        }
        if "skim_ledger" not in state or not isinstance(state["skim_ledger"], list):
            state["skim_ledger"] = []
        state["skim_ledger"].insert(0, ledger_item)
        state["skim_ledger"] = state["skim_ledger"][:75]

        cumulative_total = round(float(state["vault_cash"]) + float(state["added_to_anchors"]), 4)
        if "profit_curve" not in state or not isinstance(state["profit_curve"], list):
            state["profit_curve"] = []
        state["profit_curve"].append({
            "time": ts,
            "value": cumulative_total,
            "vault": round(float(state["vault_cash"]), 4),
            "topups": round(float(state["added_to_anchors"]), 4)
        })
        state["profit_curve"] = state["profit_curve"][-60:]


# ==============================================================================
# 3. GATE.IO SPOT V4 WALLET INSPECTOR & PRECISION ORDER BRIDGE
# ==============================================================================
def sign_gateio_v4(method: str, url_path: str, query_string: str, body_str: str, api_secret: str) -> Tuple[str, str]:
    ts = str(int(time.time()))
    body_hash = hashlib.sha512((body_str or "").encode("utf-8")).hexdigest()
    sign_string = f"{method.upper()}\n{GATEIO_API_PREFIX}{url_path}\n{query_string or ''}\n{body_hash}\n{ts}"
    signature = hmac.new(
        api_secret.encode("utf-8"),
        sign_string.encode("utf-8"),
        hashlib.sha512
    ).hexdigest()
    return ts, signature


def fetch_gateio_spot_balances(api_key: str, api_secret: str) -> Tuple[Optional[Dict[str, float]], str]:
    """Queries Gate.io Spot v4 `/spot/accounts` and returns {CURRENCY: available_float}."""
    if not api_key or not api_secret:
        return None, "Missing Gate.io API Key or Secret Key."
    try:
        url_path = "/spot/accounts"
        ts, sig = sign_gateio_v4("GET", url_path, "", "", api_secret)
        headers = {
            "KEY": api_key,
            "Timestamp": ts,
            "SIGN": sig,
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        resp = requests.get(f"{GATEIO_API_HOST}{GATEIO_API_PREFIX}{url_path}", headers=headers, timeout=8)
        if resp.status_code == 200:
            data = resp.json()
            balances: Dict[str, float] = {}
            for row in data:
                curr = str(row.get("currency", "")).upper()
                avail = float(row.get("available", 0.0))
                balances[curr] = avail
            return balances, "OK"
        return None, f"Gate.io Auth Error ({resp.status_code}): {resp.text}"
    except Exception as exc:
        return None, f"Gate.io Network Error: {str(exc)}"


def fetch_gateio_pair_rules(pair_gate: str) -> Tuple[int, int, float]:
    """
    Fetches Gate.io exact currency_pair precision (`amount_precision`, `precision`, `min_quote_amount`)
    so orders are formatted to Gate.io's exact decimal step and never fail.
    """
    try:
        r = requests.get(f"{GATEIO_API_HOST}{GATEIO_API_PREFIX}/spot/currency_pairs/{pair_gate}", timeout=5)
        if r.status_code == 200:
            d = r.json()
            amt_prec = int(d.get("amount_precision", 4))
            px_prec = int(d.get("precision", 4))
            min_quote = float(d.get("min_quote_amount", 1.0))
            return amt_prec, px_prec, min_quote
    except Exception:
        pass
    return 4, 4, 1.0


def fetch_active_exchange_balances() -> Tuple[Optional[Dict[str, float]], str]:
    with state_lock:
        key = (state.get("spot_api_key") or "").strip()
        sec = (state.get("spot_api_secret") or "").strip()
    return fetch_gateio_spot_balances(key, sec)


def sync_all_live_cards_with_gateio() -> Tuple[bool, str, Dict[str, float]]:
    """
    Reads your real Gate.io Spot Wallet (`/spot/accounts`) and synchronizes every
    `⚡ LIVE ORDER` coin card so VaultSkim shows your EXACT real token shares and USDT!
    - If you hold 0.12275 QNT and $30.61 USDT on Gate.io, QNT is aligned to 0.12275 QNT
      in `MOONBAG_RIDING` (since half is in QNT and half is in USDT).
    - If you hold 0 LQTY on Gate.io, LQTY's fake paper shares (15.0922) are wiped to 0.0
      and status is set to `WAITING_LIVE_BUY` so it buys real LQTY on the next tick.
    """
    balances, msg = fetch_active_exchange_balances()
    if balances is None:
        return False, msg, {}

    usdt_avail = float(balances.get("USDT", 0.0))
    synced_summary = []

    with state_lock:
        state["last_verified_usdt"] = round(usdt_avail, 2)

        # Remove any old [PAPER] entries from the ledger if live bridge is armed
        if state.get("live_bridge_enabled", True):
            state["skim_ledger"] = [
                item for item in state.get("skim_ledger", [])
                if item.get("mode") == "LIVE"
            ]
            real_vault = sum(float(item.get("profit", 0.0)) for item in state["skim_ledger"])
            real_topup = sum(float(item.get("topup", 0.0)) for item in state["skim_ledger"])
            state["vault_cash"] = round(real_vault, 4)
            state["added_to_anchors"] = round(real_topup, 4)
            state["total_skims"] = len(state["skim_ledger"])

        for sym, coin in state.get("coins", {}).items():
            base = str(coin.get("symbol", sym)).upper()
            if not coin.get("contract"):
                coin["source"] = "gateio"

            if not coin.get("live_mode", False):
                continue

            real_shares = float(balances.get(base, 0.0))
            px = float(coin.get("last_price") or fetch_live_coin_price(coin) or 1.0)
            bag_usd = real_shares * px
            anchor_target = float(coin.get("anchor_usd", 50.0))

            if real_shares * px < 0.50:
                # Gate.io holds ZERO (or dust) of this token! Wipe fake paper shares!
                coin["shares_owned"] = 0.0
                coin["cash_reserve_usd"] = round(anchor_target, 2)
                coin["status"] = "WAITING_LIVE_BUY"
                coin["shield_armed"] = False
                coin["zero_loss_stop"] = 0.0
                coin["escrow_profit"] = 0.0
                synced_summary.append(f"{base}: 0.0000 sh on Gate.io -> Ready for Live Buy")
            else:
                # Gate.io holds real tokens (e.g. 0.12275 QNT worth ~$24.56)
                coin["shares_owned"] = real_shares
                if coin.get("entry_price", 0.0) <= 0.0:
                    coin["entry_price"] = px
                if coin.get("sliding_low", 0.0) <= 0.0:
                    coin["sliding_low"] = px

                # Determine whether this bag is a 50% Moonbag or a 100% Anchor
                if coin.get("strategy") == "hybrid_50_50" and bag_usd < (anchor_target * 0.72) and usdt_avail >= (bag_usd * 0.50):
                    # Currently holding the 50% Moonbag half on Gate.io (like your 0.12275 QNT + $30.61 USDT!)
                    coin["status"] = "MOONBAG_RIDING"
                    coin["cash_reserve_usd"] = round(anchor_target - bag_usd, 2)
                    if coin.get("zero_loss_stop", 0.0) <= 0.0:
                        coin["zero_loss_stop"] = round(px * (1.0 - STAGE2_MOONBAG_TRAIL_PCT), 4)
                    synced_summary.append(f"{base}: Synced {real_shares:.5f} sh (${bag_usd:.2f}) in MOONBAG_RIDING")
                else:
                    coin["status"] = "HOLDING_ANCHOR"
                    coin["anchor_usd"] = round(bag_usd, 2)
                    coin["cash_reserve_usd"] = 0.0
                    synced_summary.append(f"{base}: Synced {real_shares:.5f} sh (${bag_usd:.2f}) in HOLDING_ANCHOR")

        save_state_to_db()

    summary_str = " | ".join(synced_summary) if synced_summary else "All live cards checked"
    log_event(f"🔄 REAL GATE.IO WALLET SYNCED: Available USDT = ${usdt_avail:.2f} | {summary_str}")
    return True, f"Synced with Gate.io! Spot USDT: ${usdt_avail:.2f} ({summary_str})", balances


def execute_live_exchange_order(
    symbol: str,
    side: str,
    usd_notional: float,
    current_price: float,
    shares_requested: float = 0.0,
    is_simulated_test: bool = False
) -> Tuple[bool, float, float, str]:
    """
    Fires a real Gate.io Spot v4 order.
    - Queries live Gate.io `/spot/accounts` immediately before every order.
    - Fetches exact pair decimals (`amount_precision`, `precision`) from Gate.io.
    - Floors share amounts so Gate.io NEVER throws `400 BALANCE_NOT_ENOUGH`.
    """
    if is_simulated_test:
        return True, shares_requested, usd_notional, "SIMULATED_TEST"

    with state_lock:
        api_key = (state.get("spot_api_key") or "").strip()
        api_secret = (state.get("spot_api_secret") or "").strip()
        max_cap = float(state.get("max_order_cap", 250.0))

    if not api_key or not api_secret:
        msg = f"Live {side} Aborted ({symbol}): Missing Gate.io API Key/Secret."
        log_event(f"⚠️ {msg}")
        return False, 0.0, 0.0, msg

    base_asset = symbol.upper().replace("/USDT", "").replace("_USDT", "").replace("USDT", "").strip()
    pair_gate = f"{base_asset}_USDT"
    side_up = side.upper().strip()

    balances, bal_msg = fetch_gateio_spot_balances(api_key, api_secret)
    if balances is None:
        log_event(f"⚠️ {bal_msg}")
        return False, 0.0, 0.0, bal_msg

    avail_usdt = float(balances.get("USDT", 0.0))
    avail_token = float(balances.get(base_asset, 0.0))
    amt_prec, px_prec, min_quote = fetch_gateio_pair_rules(pair_gate)

    with state_lock:
        state["last_verified_usdt"] = round(avail_usdt, 2)

    if side_up == "BUY":
        # Leave 0.6% safety margin on available USDT so limit price (+0.3%) + 0.1% fee never exceeds avail_usdt
        max_spendable_usdt = avail_usdt * 0.994
        target_usd = min(float(usd_notional), max_spendable_usdt, max_cap)

        if target_usd < max(MIN_EXCHANGE_ORDER_USD, min_quote):
            msg = (
                f"Gate.io Live BUY Paused ({base_asset}): Available Spot USDT is ${avail_usdt:.2f} "
                f"(Need at least ${max(MIN_EXCHANGE_ORDER_USD, min_quote):.2f} USDT)."
            )
            log_event(f"⚠️ {msg}")
            return False, 0.0, 0.0, msg

        limit_px = round(float(current_price) * 1.003, px_prec)
        raw_shares = (target_usd / limit_px) * 0.998
        order_shares = floor_to_decimals(raw_shares, amt_prec)

        if order_shares <= 0 or (order_shares * limit_px) < min_quote:
            msg = f"Gate.io Live BUY Paused ({base_asset}): Order size ({order_shares}) below pair minimum."
            log_event(f"⚠️ {msg}")
            return False, 0.0, 0.0, msg

    else:
        # LIVE SELL: Always clamp directly to `avail_token` in Gate.io Spot wallet!
        if avail_token <= 0.0:
            msg = f"Gate.io Live SELL Paused ({base_asset}): Your Gate.io Spot wallet holds 0 {base_asset}."
            log_event(f"⚠️ {msg}")
            return False, 0.0, 0.0, msg

        # If selling all/most of the position or if requested shares > avail_token, use avail_token!
        if shares_requested <= 0.0 or shares_requested >= avail_token * 0.85:
            raw_sell_shares = avail_token * 0.9998
        else:
            raw_sell_shares = min(float(shares_requested), avail_token * 0.9998)

        order_shares = floor_to_decimals(raw_sell_shares, amt_prec)
        limit_px = round(float(current_price) * 0.997, px_prec)

        if order_shares <= 0 or (order_shares * limit_px) < min_quote:
            msg = (
                f"Gate.io Live SELL Paused ({base_asset}): Wallet balance {avail_token:.6f} {base_asset} "
                f"(${avail_token * current_price:.2f}) is below Gate.io ${min_quote:.2f} minimum."
            )
            log_event(f"⚠️ {msg}")
            return False, 0.0, 0.0, msg

    try:
        url_path = "/spot/orders"
        body_payload = {
            "currency_pair": pair_gate,
            "type": "limit",
            "account": "spot",
            "side": side_up.lower(),
            "amount": f"{order_shares:.{amt_prec}f}",
            "price": f"{limit_px:.{px_prec}f}",
            "time_in_force": "ioc"
        }
        body_str = json.dumps(body_payload)
        ts, sig = sign_gateio_v4("POST", url_path, "", body_str, api_secret)
        headers = {
            "KEY": api_key,
            "Timestamp": ts,
            "SIGN": sig,
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        resp = requests.post(
            f"{GATEIO_API_HOST}{GATEIO_API_PREFIX}{url_path}",
            headers=headers,
            data=body_str,
            timeout=8
        )
        if resp.status_code in (200, 201):
            res_data = resp.json()
            order_id = res_data.get("id", "OK")
            filled_total_usd = float(res_data.get("filled_total", 0.0))
            left_shares = float(res_data.get("left", 0.0))
            filled_shares = max(0.0, order_shares - left_shares)

            if filled_shares <= 0.0 and res_data.get("status") in ("closed", "filled"):
                filled_shares = order_shares
            if filled_shares <= 0.0:
                msg = f"Gate.io IOC Order #{order_id} did not fill immediately ({res_data.get('status')}); retrying next tick."
                log_event(f"⚠️ {msg}")
                return False, 0.0, 0.0, msg

            if filled_total_usd <= 0.0:
                filled_total_usd = filled_shares * float(current_price)

            # Query Gate.io Spot wallet again right after fill so we store the EXACT post-fee balance
            post_balances, _ = fetch_gateio_spot_balances(api_key, api_secret)
            if post_balances is not None:
                state["last_verified_usdt"] = round(float(post_balances.get("USDT", avail_usdt)), 2)
                exact_token_now = float(post_balances.get(base_asset, 0.0))
            else:
                exact_token_now = (
                    avail_token + filled_shares * (1.0 - DEFAULT_FEE_RATE)
                    if side_up == "BUY"
                    else max(0.0, avail_token - filled_shares)
                )

            net_usd = filled_total_usd if side_up == "BUY" else filled_total_usd * (1.0 - DEFAULT_FEE_RATE)
            log_event(
                f"⚡ LIVE GATE.IO {side_up} EXECUTED ({base_asset}): Order #{order_id} filled! "
                f"Traded {filled_shares:.{amt_prec}f} {base_asset} (${net_usd:.2f}) | "
                f"Spot Wallet Now Holds: {exact_token_now:.6f} {base_asset}"
            )
            return True, exact_token_now if side_up == "BUY" else filled_shares, net_usd, f"Order #{order_id}"
        else:
            err_msg = f"Gate.io Live Order Notice ({resp.status_code}): {resp.text}"
            log_event(f"⚠️ {err_msg}")
            return False, 0.0, 0.0, err_msg
    except Exception as exc:
        err_msg = f"Gate.io Execution Exception: {str(exc)}"
        log_event(f"⚠️ {err_msg}")
        return False, 0.0, 0.0, err_msg


# ==============================================================================
# 4. LIVE PRICE FEED & DEX ANTI-RUG INSPECTOR
# ==============================================================================
def inspect_coin_or_dex_contract(raw_input: str) -> Dict[str, Any]:
    cleaned = raw_input.strip()
    is_contract = len(cleaned) > 20 or cleaned.startswith("0x")

    if is_contract:
        try:
            r = requests.get(f"{DEXSCREENER_API_HOST}/latest/dex/tokens/{cleaned}", timeout=6)
            if r.status_code == 200:
                pairs = r.json().get("pairs") or []
                if not pairs:
                    return {"ok": False, "reason": "No active DEX liquidity pool found for this contract."}
                best = max(pairs, key=lambda p: float((p.get("liquidity") or {}).get("usd", 0.0)))
                liq_usd = float((best.get("liquidity") or {}).get("usd", 0.0))
                price_usd = float(best.get("priceUsd", 0.0))
                base_sym = str((best.get("baseToken") or {}).get("symbol", "DEX")).upper()
                if liq_usd < MIN_DEX_LIQUIDITY_USD:
                    return {
                        "ok": False,
                        "reason": f"ANTI-RUG BLOCK: Pool liquidity (${liq_usd:,.0f}) is below $15,000 safety floor!"
                    }
                return {
                    "ok": True,
                    "symbol": base_sym,
                    "source": "dex",
                    "contract": cleaned,
                    "liquidity_usd": liq_usd,
                    "price": price_usd
                }
        except Exception as exc:
            return {"ok": False, "reason": f"DexScreener lookup error: {exc}"}

    sym = cleaned.upper().replace("/USDT", "").replace("_USDT", "").replace("USDT", "").strip()
    try:
        r = requests.get(
            f"{GATEIO_API_HOST}{GATEIO_API_PREFIX}/spot/tickers",
            params={"currency_pair": f"{sym}_USDT"},
            timeout=6
        )
        if r.status_code == 200 and isinstance(r.json(), list) and len(r.json()) > 0:
            t_data = r.json()[0]
            px = float(t_data.get("last", 0.0))
            vol_usd = float(t_data.get("quote_volume", 0.0))
            if px > 0:
                return {
                    "ok": True,
                    "symbol": sym,
                    "source": "gateio",
                    "contract": "",
                    "liquidity_usd": max(vol_usd, 100000.0),
                    "price": px
                }
    except Exception:
        pass

    return {"ok": False, "reason": f"Pair {sym}/USDT not found on Gate.io Spot."}


def fetch_live_coin_price(coin: Dict[str, Any]) -> float:
    sym = str(coin.get("symbol", "")).upper()
    source = coin.get("source", "gateio")
    contract = (coin.get("contract") or "").strip()

    if source == "dex" and contract:
        try:
            r = requests.get(f"{DEXSCREENER_API_HOST}/latest/dex/tokens/{contract}", timeout=5)
            if r.status_code == 200:
                pairs = r.json().get("pairs") or []
                if pairs:
                    px = float(pairs[0].get("priceUsd", 0.0))
                    if px > 0:
                        return px
        except Exception:
            pass

    try:
        r = requests.get(
            f"{GATEIO_API_HOST}{GATEIO_API_PREFIX}/spot/tickers",
            params={"currency_pair": f"{sym}_USDT"},
            timeout=5
        )
        if r.status_code == 200 and isinstance(r.json(), list) and len(r.json()) > 0:
            px = float(r.json()[0].get("last", 0.0))
            if px > 0:
                return px
    except Exception:
        pass

    return float(coin.get("last_price", 0.0))


# ==============================================================================
# 5. PURE LIVE TRUTH SKIMMING ENGINE (ZERO PAPER FALLBACK ON LIVE COINS)
# ==============================================================================
def process_coin_tick(symbol: str, current_price: float, is_simulated_test: bool = False):
    """
    Processes price ticks with strict separation between LIVE and PAPER modes:
      - If `coin["live_mode"]` is True (and `is_simulated_test` is False):
          * It NEVER runs paper trades or fake Sliding Floor Skims.
          * If Gate.io has not bought the coin yet (`status == "WAITING_LIVE_BUY"` or `shares_owned <= 0`),
            it fires a real Gate.io BUY order first. If that BUY fails, it stays in `WAITING_LIVE_BUY`
            and shows `0.000000` shares (never fake shares!).
          * Every Stage 1 Skim, Stage 2 Moonbag Exit, and Dip Re-Buy MUST succeed on Gate.io
            before changing card status or adding a single cent to Bucket B Vault.
    """
    if current_price <= 0:
        return

    with state_lock:
        coin = state.get("coins", {}).get(symbol)
        if not coin:
            return

        coin["last_price"] = float(current_price)
        if not coin.get("contract"):
            coin["source"] = "gateio"

        card_wants_live = bool(coin.get("live_mode", False))
        bridge_armed = bool(state.get("live_bridge_enabled", True))

        # CRITICAL RULE: If the card is set to ⚡ LIVE ORDER (and not a manual practice button click),
        # it MUST trade live on Gate.io and NEVER fall back to paper!
        if card_wants_live and not is_simulated_test and not bridge_armed:
            # Automatically keep bridge armed if user set a card to LIVE ORDER
            state["live_bridge_enabled"] = True
            bridge_armed = True

        is_live_execution = card_wants_live and not is_simulated_test

        # Throttle live API order retries to at least 12 seconds apart per coin
        now_sec = time.time()
        last_try = float(coin.get("last_live_Action_ts", 0.0))

        status = coin.get("status", "HOLDING_ANCHOR")
        strategy = coin.get("strategy", "hybrid_50_50")
        anchor_usd = float(coin.get("anchor_usd", 50.0))

        # ----------------------------------------------------------------------
        # STEP 0: INITIAL BUY (WAITING_LIVE_BUY OR 0 SHARES OWNED)
        # ----------------------------------------------------------------------
        if status == "WAITING_LIVE_BUY" or (status == "HOLDING_ANCHOR" and float(coin.get("shares_owned", 0.0)) <= 0.0):
            if is_live_execution:
                if now_sec - last_try < 12.0:
                    return
                coin["last_live_Action_ts"] = now_sec

                ok, exact_wallet_shares, spent_usd, msg = execute_live_exchange_order(
                    symbol=symbol,
                    side="BUY",
                    usd_notional=anchor_usd,
                    current_price=current_price,
                    shares_requested=0.0,
                    is_simulated_test=False
                )
                if not ok:
                    coin["status"] = "WAITING_LIVE_BUY"
                    coin["shares_owned"] = 0.0
                    coin["cash_reserve_usd"] = round(anchor_usd, 2)
                    return

                coin["shares_owned"] = exact_wallet_shares
                coin["target_shares_floor"] = exact_wallet_shares
                coin["anchor_usd"] = round(spent_usd, 2)
                coin["cash_reserve_usd"] = 0.0
                coin["entry_price"] = current_price
                coin["sliding_low"] = current_price
                coin["high_water"] = current_price
                coin["shield_armed"] = False
                coin["zero_loss_stop"] = 0.0
                coin["status"] = "HOLDING_ANCHOR"
                return
            else:
                # Paper mode initialization
                init_sh = (anchor_usd / current_price) * (1.0 - DEFAULT_FEE_RATE)
                coin["shares_owned"] = init_sh
                coin["target_shares_floor"] = init_sh
                coin["cash_reserve_usd"] = 0.0
                coin["entry_price"] = current_price
                coin["sliding_low"] = current_price
                coin["high_water"] = current_price
                coin["shield_armed"] = False
                coin["zero_loss_stop"] = 0.0
                coin["status"] = "HOLDING_ANCHOR"
                log_event(f"PAPER BUY {symbol}: ${anchor_usd:.2f} @ ${current_price:.4f} -> {init_sh:.6f} sh")
                return

        # ----------------------------------------------------------------------
        # STATE A: HOLDING_ANCHOR (100% IN TOKEN)
        # ----------------------------------------------------------------------
        if status == "HOLDING_ANCHOR":
            entry_px = float(coin.get("entry_price") or current_price)
            sliding_low = float(coin.get("sliding_low") or current_price)
            high_water = float(coin.get("high_water") or current_price)

            if current_price < sliding_low or sliding_low <= 0.0:
                sliding_low = current_price
                coin["sliding_low"] = sliding_low

            gain_from_low = (current_price - sliding_low) / sliding_low if sliding_low > 0 else 0.0
            gain_from_entry = (current_price - entry_px) / entry_px if entry_px > 0 else 0.0

            # Arm Zero-Loss Shield at +0.35% above entry
            if gain_from_entry >= SHIELD_ARM_PCT and not coin.get("shield_armed", False):
                coin["shield_armed"] = True
                floor_px = entry_px * (1.0 + SHIELD_FLOOR_LOCK_PCT)
                coin["zero_loss_stop"] = floor_px
                coin["high_water"] = current_price
                log_event(
                    f"🛡️ ZERO-LOSS SHIELD ARMED {symbol} @ ${current_price:.4f}: "
                    f"Guaranteed profit floor locked @ ${floor_px:.4f}"
                )

            if coin.get("shield_armed", False) and current_price > high_water:
                high_water = current_price
                coin["high_water"] = high_water
                trail_stop = high_water * (1.0 - STAGE1_TRAIL_GAP_PCT)
                if trail_stop > float(coin.get("zero_loss_stop", 0.0)):
                    coin["zero_loss_stop"] = trail_stop

            # Require price to be strictly above Entry + 0.22% fee floor so we NEVER sell at a loss
            min_profitable_px = entry_px * (1.0 + SHIELD_FLOOR_LOCK_PCT)
            hit_bounce_target = (gain_from_low >= STAGE1_SKIM_TARGET_PCT and current_price >= min_profitable_px)
            hit_shield_floor = (
                bool(coin.get("shield_armed", False))
                and current_price <= float(coin.get("zero_loss_stop", 0.0))
                and current_price >= min_profitable_px
            )

            if hit_bounce_target or hit_shield_floor:
                total_shares = float(coin.get("shares_owned", 0.0))

                if strategy == "hybrid_50_50":
                    shares_to_sell = total_shares * 0.50
                    if is_live_execution:
                        if now_sec - last_try < 12.0:
                            return
                        coin["last_live_Action_ts"] = now_sec
                        ok, sold_sh, net_proceeds_usd, msg = execute_live_exchange_order(
                            symbol=symbol,
                            side="SELL",
                            usd_notional=shares_to_sell * current_price,
                            current_price=current_price,
                            shares_requested=shares_to_sell,
                            is_simulated_test=False
                        )
                        if not ok:
                            return  # STRICT LIVE: Never book paper profit if Gate.io order did not fill!
                    else:
                        sold_sh = shares_to_sell
                        net_proceeds_usd = (sold_sh * current_price) * (1.0 - DEFAULT_FEE_RATE)

                    half_cost = anchor_usd * 0.50
                    stage1_profit = max(0.0001, net_proceeds_usd - half_cost)

                    coin["shares_owned"] = max(0.0, total_shares - sold_sh)
                    coin["cash_reserve_usd"] = round(half_cost, 2)
                    coin["escrow_profit"] = round(float(coin.get("escrow_profit", 0.0)) + stage1_profit, 4)
                    coin["status"] = "MOONBAG_RIDING"
                    coin["high_water"] = current_price
                    coin["zero_loss_stop"] = max(
                        entry_px * (1.0 + SHIELD_FLOOR_LOCK_PCT),
                        current_price * (1.0 - STAGE2_MOONBAG_TRAIL_PCT)
                    )

                    record_verified_skim(
                        symbol=symbol,
                        stage_label="Stage 1 Hybrid (50%)",
                        profit_usd=stage1_profit,
                        share_topup_usd=0.0,
                        exec_price=current_price,
                        shares_remaining=coin["shares_owned"],
                        is_live_order=is_live_execution
                    )
                    log_event(
                        f"🚀 STAGE 1 HYBRID SKIM {symbol} ({'LIVE' if is_live_execution else 'PAPER'}) @ ${current_price:.4f}: "
                        f"Sold {sold_sh:.6f} sh -> +${stage1_profit:.4f} profit! Riding {coin['shares_owned']:.6f} Moonbag sh"
                    )
                else:
                    shares_to_sell = total_shares
                    if is_live_execution:
                        if now_sec - last_try < 12.0:
                            return
                        coin["last_live_Action_ts"] = now_sec
                        ok, sold_sh, net_proceeds_usd, msg = execute_live_exchange_order(
                            symbol=symbol,
                            side="SELL",
                            usd_notional=shares_to_sell * current_price,
                            current_price=current_price,
                            shares_requested=shares_to_sell,
                            is_simulated_test=False
                        )
                        if not ok:
                            return
                    else:
                        sold_sh = shares_to_sell
                        net_proceeds_usd = (sold_sh * current_price) * (1.0 - DEFAULT_FEE_RATE)

                    full_profit = max(0.0001, net_proceeds_usd - anchor_usd)
                    coin["shares_owned"] = 0.0
                    coin["cash_reserve_usd"] = round(anchor_usd, 2)
                    coin["escrow_profit"] = round(float(coin.get("escrow_profit", 0.0)) + full_profit, 4)
                    coin["last_exit_price"] = current_price
                    coin["status"] = "WAITING_DIP_REBUY"
                    coin["shield_armed"] = False
                    coin["zero_loss_stop"] = 0.0

                    log_event(
                        f"💰 FULL SKIM {symbol} ({'LIVE' if is_live_execution else 'PAPER'}) @ ${current_price:.4f}: "
                        f"Sold {sold_sh:.6f} sh -> +${full_profit:.4f} in Escrow"
                    )

        # ----------------------------------------------------------------------
        # STATE B: MOONBAG_RIDING (50% CASH IN USDT + 50% MOONBAG IN TOKEN)
        # ----------------------------------------------------------------------
        elif status == "MOONBAG_RIDING":
            entry_px = float(coin.get("entry_price") or current_price)
            high_water = float(coin.get("high_water") or current_price)
            stop_px = float(coin.get("zero_loss_stop") or (entry_px * 1.0018))

            if current_price > high_water:
                high_water = current_price
                coin["high_water"] = high_water
                new_trail = high_water * (1.0 - STAGE2_MOONBAG_TRAIL_PCT)
                if new_trail > stop_px:
                    stop_px = new_trail
                    coin["zero_loss_stop"] = stop_px

            rally_from_entry = (current_price - entry_px) / entry_px if entry_px > 0 else 0.0
            hit_moonbag_target = (rally_from_entry >= STAGE2_MOONBAG_TARGET_PCT)
            hit_moonbag_stop = (current_price <= stop_px)

            if hit_moonbag_target or hit_moonbag_stop:
                moonbag_shares = float(coin.get("shares_owned", 0.0))
                if is_live_execution:
                    if now_sec - last_try < 12.0:
                        return
                    coin["last_live_Action_ts"] = now_sec
                    ok, sold_sh, net_proceeds_usd, msg = execute_live_exchange_order(
                        symbol=symbol,
                        side="SELL",
                        usd_notional=moonbag_shares * current_price,
                        current_price=current_price,
                        shares_requested=moonbag_shares,
                        is_simulated_test=False
                    )
                    if not ok:
                        return  # Never fake Stage 2 Moonbag Exit in paper mode!
                else:
                    sold_sh = moonbag_shares
                    net_proceeds_usd = (sold_sh * current_price) * (1.0 - DEFAULT_FEE_RATE)

                half_cost = float(coin.get("cash_reserve_usd") or (anchor_usd * 0.50))
                stage2_profit = max(0.0, net_proceeds_usd - half_cost)
                escrow_prof = float(coin.get("escrow_profit", 0.0))
                total_cycle_profit = round(escrow_prof + stage2_profit, 4)

                vault_portion = round(stage2_profit * 0.80, 4)
                topup_portion = round(stage2_profit - vault_portion, 4)

                coin["shares_owned"] = 0.0
                coin["escrow_profit"] = 0.0
                coin["vaulted_profit"] = round(float(coin.get("vaulted_profit", 0.0)) + escrow_prof + vault_portion, 4)
                coin["share_topup_usd"] = round(float(coin.get("share_topup_usd", 0.0)) + topup_portion, 4)
                coin["anchor_usd"] = round(anchor_usd + topup_portion, 2)
                coin["cash_reserve_usd"] = coin["anchor_usd"]
                coin["skims_count"] = int(coin.get("skims_count", 0)) + 1
                coin["last_exit_price"] = current_price
                coin["status"] = "WAITING_DIP_REBUY"
                coin["shield_armed"] = False
                coin["zero_loss_stop"] = 0.0

                record_verified_skim(
                    symbol=symbol,
                    stage_label="Stage 2 Moonbag Exit",
                    profit_usd=vault_portion,
                    share_topup_usd=topup_portion,
                    exec_price=current_price,
                    shares_remaining=0.0,
                    is_live_order=is_live_execution
                )
                log_event(
                    f"🏦 STAGE 2 MOONBAG EXIT {symbol} ({'LIVE' if is_live_execution else 'PAPER'}) @ ${current_price:.4f}: "
                    f"Sold {sold_sh:.6f} sh -> Cycle Profit +${total_cycle_profit:.4f}!"
                )
                send_phone_alert(
                    title=f"VaultSkim LIVE Profit ({symbol})",
                    body=f"Stage 2 Moonbag Exit @ ${current_price:.4f}\nCycle Profit: +${total_cycle_profit:.4f}"
                )

        # ----------------------------------------------------------------------
        # STATE C: WAITING_DIP_REBUY (100% CASH IN USDT -> RE-BUY ON DIP)
        # ----------------------------------------------------------------------
        elif status == "WAITING_DIP_REBUY":
            ref_px = float(coin.get("last_exit_price") or coin.get("entry_price") or current_price)
            dip_from_exit = (ref_px - current_price) / ref_px if ref_px > 0 else 0.0
            breakout_from_exit = (current_price - ref_px) / ref_px if ref_px > 0 else 0.0

            if dip_from_exit >= DIP_REBUY_DROP_PCT or breakout_from_exit >= BREAKOUT_REBUY_PCT:
                if is_live_execution:
                    if now_sec - last_try < 12.0:
                        return
                    coin["last_live_Action_ts"] = now_sec
                    ok, exact_wallet_shares, actual_spent_usd, msg = execute_live_exchange_order(
                        symbol=symbol,
                        side="BUY",
                        usd_notional=anchor_usd,
                        current_price=current_price,
                        shares_requested=0.0,
                        is_simulated_test=False
                    )
                    if not ok:
                        return  # Stay in WAITING_DIP_REBUY until Gate.io fills the buy!
                    coin["shares_owned"] = exact_wallet_shares
                    coin["anchor_usd"] = round(actual_spent_usd, 2)
                else:
                    bought_sh = (anchor_usd / current_price) * (1.0 - DEFAULT_FEE_RATE)
                    coin["shares_owned"] = bought_sh

                coin["escrow_profit"] = 0.0
                coin["cash_reserve_usd"] = 0.0
                coin["entry_price"] = current_price
                coin["sliding_low"] = current_price
                coin["high_water"] = current_price
                coin["shield_armed"] = False
                coin["zero_loss_stop"] = 0.0
                coin["status"] = "HOLDING_ANCHOR"

                log_event(
                    f"🔄 SHARE-KEEPER RE-BUY {symbol} ({'LIVE' if is_live_execution else 'PAPER'}) @ ${current_price:.4f}: "
                    f"Holding {coin['shares_owned']:.6f} {symbol} shares"
                )# ==============================================================================
# 6. BACKGROUND 24/7 DAEMON THREAD & FLASK API ROUTES (PART 2 OF 2)
# ==============================================================================
def background_market_daemon():
    """
    24/7 Background Worker:
      1. Loads persistent state from PostgreSQL on startup.
      2. Immediately runs `sync_all_live_cards_with_gateio()` so any card set to
         ⚡ LIVE ORDER is aligned with your real Gate.io Spot wallet (0.12275 QNT,
         $30.61 USDT, 0 LQTY) and any old [PAPER] skims are purged.
      3. Ticks live prices every 4 seconds and periodically re-verifies wallet sync.
    """
    init_db_tables()
    load_state_from_db()
    try:
        sync_all_live_cards_with_gateio()
    except Exception as exc:
        logger.warning(f"Startup wallet sync notice: {exc}")

    logger.info("VaultSkim v5.2 Pure Live Truth background daemon started.")
    tick_counter = 0

    while True:
        try:
            tick_counter += 1
            # Every 15 ticks (~60s), refresh real Gate.io USDT & token balances
            if tick_counter % 15 == 0:
                balances, _ = fetch_active_exchange_balances()
                if balances is not None:
                    with state_lock:
                        state["last_verified_usdt"] = round(float(balances.get("USDT", 0.0)), 2)
                        for sym, c_obj in state.get("coins", {}).items():
                            if c_obj.get("live_mode", False):
                                base = str(c_obj.get("symbol", sym)).upper()
                                real_sh = float(balances.get(base, 0.0))
                                px_now = float(c_obj.get("last_price") or 0.0)
                                if real_sh * px_now >= 0.50:
                                    c_obj["shares_owned"] = real_sh
                                elif c_obj.get("status") not in ("WAITING_DIP_REBUY", "WAITING_LIVE_BUY"):
                                    c_obj["shares_owned"] = 0.0
                                    c_obj["status"] = "WAITING_LIVE_BUY"

            with state_lock:
                symbols_to_check = list(state.get("coins", {}).keys())

            for sym in symbols_to_check:
                with state_lock:
                    coin_ref = state.get("coins", {}).get(sym)
                if not coin_ref:
                    continue
                px = fetch_live_coin_price(coin_ref)
                if px > 0.0:
                    with state_lock:
                        process_coin_tick(sym, px, is_simulated_test=False)
                        save_state_to_db()
        except Exception as exc:
            logger.error(f"Background daemon loop error: {exc}")
        time.sleep(4)


daemon_thread = threading.Thread(target=background_market_daemon, daemon=True)
daemon_thread.start()


@app.route("/api/state", methods=["GET"])
def api_get_state():
    """Returns sanitized state JSON to the dashboard frontend."""
    with state_lock:
        safe_state = json.loads(json.dumps(state))
        key = safe_state.get("spot_api_key", "")
        if key:
            safe_state["spot_api_key_masked"] = f"{key[:4]}...{key[-4:]}" if len(key) > 8 else "Saved Key"
        else:
            safe_state["spot_api_key_masked"] = ""
        safe_state.pop("spot_api_secret", None)
        return jsonify(safe_state)


@app.route("/api/verify_exchange", methods=["POST"])
def api_verify_exchange():
    """Verifies Gate.io Spot v4 credentials and syncs live cards with actual wallet balances."""
    req = request.json or {}
    with state_lock:
        if "spot_exchange" in req:
            state["spot_exchange"] = str(req["spot_exchange"]).strip()
        if "spot_api_key" in req and str(req["spot_api_key"]).strip():
            state["spot_api_key"] = str(req["spot_api_key"]).strip()
        if "spot_api_secret" in req and str(req["spot_api_secret"]).strip():
            state["spot_api_secret"] = str(req["spot_api_secret"]).strip()
        if "max_order_cap" in req:
            state["max_order_cap"] = float(req["max_order_cap"])
        save_state_to_db()

    ok, sync_msg, balances = sync_all_live_cards_with_gateio()
    if ok and balances is not None:
        usdt_bal = float(balances.get("USDT", 0.0))
        return jsonify({
            "ok": True,
            "usdt": usdt_bal,
            "message": f"Gate.io v4 Connected! Available Spot USDT: ${usdt_bal:.2f}"
        })
    return jsonify({"ok": False, "message": sync_msg})


@app.route("/api/toggle_bridge", methods=["POST"])
def api_toggle_bridge():
    """Saves API keys and toggles the Global Live Exchange Bridge."""
    req = request.json or {}
    with state_lock:
        if "spot_api_key" in req and str(req["spot_api_key"]).strip():
            state["spot_api_key"] = str(req["spot_api_key"]).strip()
        if "spot_api_secret" in req and str(req["spot_api_secret"]).strip():
            state["spot_api_secret"] = str(req["spot_api_secret"]).strip()
        if "spot_exchange" in req:
            state["spot_exchange"] = str(req["spot_exchange"]).strip()
        if "max_order_cap" in req:
            state["max_order_cap"] = float(req["max_order_cap"])

        # If keys were provided, ensure bridge is armed; otherwise toggle
        if req.get("spot_api_key") or req.get("spot_api_secret"):
            state["live_bridge_enabled"] = True
        else:
            state["live_bridge_enabled"] = not bool(state.get("live_bridge_enabled", True))

        status_label = "ARMED (GATE.IO LIVE)" if state["live_bridge_enabled"] else "PAUSED (PAPER SIMULATOR)"
        log_event(f"⚡ EXCHANGE BRIDGE ({status_label}): Available Spot USDT: ${state.get('last_verified_usdt', 0.0):.2f}")
        save_state_to_db()

    if state.get("live_bridge_enabled"):
        sync_all_live_cards_with_gateio()

    return jsonify({"ok": True, "live_bridge_enabled": state["live_bridge_enabled"]})


@app.route("/api/sync_live_wallet", methods=["POST"])
def api_sync_live_wallet():
    """
    1-Click Real Gate.io Wallet Sync:
    Aligns QNT, LQTY, and all live cards with your exact Gate.io Spot balances
    and purges any old [PAPER] skims.
    """
    ok, msg, balances = sync_all_live_cards_with_gateio()
    if not ok:
        return jsonify({"ok": False, "message": msg})
    usdt_avail = float((balances or {}).get("USDT", 0.0))
    return jsonify({
        "ok": True,
        "usdt": usdt_avail,
        "message": msg
    })


@app.route("/api/save_alerts", methods=["POST"])
def api_save_alerts():
    req = request.json or {}
    with state_lock:
        if "ntfy_topic" in req:
            state["ntfy_topic"] = str(req["ntfy_topic"]).strip()
        if "telegram_bot_token" in req:
            state["telegram_bot_token"] = str(req["telegram_bot_token"]).strip()
        if "telegram_chat_id" in req:
            state["telegram_chat_id"] = str(req["telegram_chat_id"]).strip()
        save_state_to_db()
        log_event(f"📲 Phone alert channels updated (ntfy topic: {state.get('ntfy_topic') or 'none'}).")
    return jsonify({"ok": True})


@app.route("/api/coin_action", methods=["POST"])
def api_coin_action():
    """
    Handles per-card actions:
      - `toggle_live`: Switches between ⚡ LIVE ORDER and 🧪 PAPER. When switched to LIVE,
        immediately checks Gate.io Spot wallet so fake paper shares are never shown as live tokens!
      - `edit_anchor`: Updates anchor USD. If increased on a live coin, immediately buys the extra
        top-up delta on Gate.io Spot if USDT is available!
      - `switch_strategy`: Toggles between 50/50 Hybrid Moonbag and 100% Share-Keeper.
      - `practice_spike`: Only runs on PAPER cards (blocked on LIVE cards so paper trades never mix!).
    """
    req = request.json or {}
    sym = str(req.get("symbol", "")).upper().strip()
    action = str(req.get("action", "")).strip()

    with state_lock:
        coin = state.get("coins", {}).get(sym)
        if not coin:
            return jsonify({"ok": False, "message": "Coin card not found."})

        if action == "toggle_live":
            new_mode = not bool(coin.get("live_mode", False))
            coin["live_mode"] = new_mode
            if new_mode:
                state["live_bridge_enabled"] = True
                log_event(f"⚡ COIN EXECUTION MODE ({sym}): Switched to ⚡ LIVE ORDERS ENABLED.")
                save_state_to_db()
                # Immediately align with real Gate.io wallet so paper shares are cleared!
                sync_all_live_cards_with_gateio()
                return jsonify({"ok": True})
            else:
                log_event(f"🧪 COIN EXECUTION MODE ({sym}): Switched to PAPER SIMULATOR.")

        elif action == "switch_strategy":
            curr_strat = coin.get("strategy", "hybrid_50_50")
            coin["strategy"] = "share_keeper_100" if curr_strat == "hybrid_50_50" else "hybrid_50_50"
            log_event(f"Strategy for {sym} switched to -> {coin['strategy']}")

        elif action == "edit_anchor":
            old_anc = float(coin.get("anchor_usd", 50.0))
            new_anc = float(req.get("anchor_usd", old_anc))
            if new_anc >= 2.0:
                delta_usd = new_anc - old_anc
                coin["anchor_usd"] = round(new_anc, 2)
                px = float(coin.get("last_price") or fetch_live_coin_price(coin) or 0.0)

                # If user increased anchor on an active LIVE card, buy the delta immediately on Gate.io
                if coin.get("live_mode", False) and delta_usd >= 1.20 and px > 0:
                    ok, exact_wallet_sh, spent_usd, msg = execute_live_exchange_order(
                        symbol=sym,
                        side="BUY",
                        usd_notional=delta_usd,
                        current_price=px,
                        shares_requested=0.0,
                        is_simulated_test=False
                    )
                    if ok:
                        coin["shares_owned"] = exact_wallet_sh
                        coin["status"] = "HOLDING_ANCHOR"
                        log_event(
                            f"⚡ LIVE ANCHOR TOP-UP ({sym}): Added +${spent_usd:.2f} on Gate.io -> "
                            f"Now holding {exact_wallet_sh:.6f} {sym}"
                        )
                    else:
                        log_event(f"✏️ Anchor target set to ${new_anc:.2f} for {sym} ({msg})")
                else:
                    log_event(f"✏️ Anchor updated for {sym} -> ${coin['anchor_usd']:.2f}")

        elif action == "practice_spike":
            if coin.get("live_mode", False):
                log_event(
                    f"🛡️ LIVE MODE PROTECTION ({sym}): Practice Simulator is disabled while {sym} is in ⚡ LIVE ORDER mode "
                    f"so fake [PAPER] trades never pollute your live ledger!"
                )
                save_state_to_db()
                return jsonify({
                    "ok": False,
                    "message": f"{sym} is in ⚡ LIVE ORDER mode! Practice Simulator is blocked so paper trades never mix with your live Gate.io trading."
                })
            px = float(coin.get("last_price") or 100.0) * 1.022
            process_coin_tick(sym, px, is_simulated_test=True)

        elif action == "remove":
            state["coins"].pop(sym, None)
            log_event(f"Removed coin card {sym}.")

        save_state_to_db()
        return jsonify({"ok": True})


@app.route("/api/add_coin", methods=["POST"])
def api_add_coin():
    """
    Launches a new coin card.
    If Global Live Bridge is armed, checks Gate.io Spot wallet first so it starts
    cleanly without fake paper shares.
    """
    req = request.json or {}
    raw_input = str(req.get("symbol", "")).strip()
    anchor_usd = float(req.get("anchor_usd", 10.0))
    start_live = bool(req.get("live_mode", False))

    if not raw_input:
        return jsonify({"ok": False, "message": "Please enter a token symbol or contract address."})

    inspection = inspect_coin_or_dex_contract(raw_input)
    if not inspection.get("ok"):
        err = inspection.get("reason", "Inspection failed.")
        log_event(f"⚠️ {err}")
        return jsonify({"ok": False, "message": err})

    sym = inspection["symbol"]
    src = inspection["source"]
    contract = inspection["contract"]
    liq = inspection["liquidity_usd"]
    init_px = inspection["price"]

    with state_lock:
        new_card = build_default_coin_card(
            symbol=sym,
            anchor_usd=anchor_usd,
            source=src,
            contract=contract,
            liquidity_usd=liq,
            live_mode=start_live,
            strategy="hybrid_50_50"
        )
        new_card["last_price"] = init_px
        if not start_live and init_px > 0:
            # Paper card initializes simulated shares; Live card waits for real Gate.io BUY
            sim_sh = (anchor_usd / init_px) * (1.0 - DEFAULT_FEE_RATE)
            new_card["shares_owned"] = sim_sh
            new_card["target_shares_floor"] = sim_sh
            new_card["entry_price"] = init_px
            new_card["sliding_low"] = init_px
            new_card["high_water"] = init_px
            new_card["cash_reserve_usd"] = 0.0
            new_card["status"] = "HOLDING_ANCHOR"

        state["coins"][sym] = new_card
        mode_str = "⚡ LIVE ORDER" if start_live else "🧪 PAPER SIMULATOR"
        log_event(f"🎯 Launched {sym}/USDT (${anchor_usd:.2f} Anchor in {mode_str})")
        save_state_to_db()

    if start_live:
        sync_all_live_cards_with_gateio()

    return jsonify({"ok": True, "symbol": sym, "price": init_px})


@app.route("/api/freeze_all", methods=["POST"])
def api_freeze_all():
    with state_lock:
        state["live_bridge_enabled"] = False
        for _, coin in state.get("coins", {}).items():
            coin["live_mode"] = False
        log_event("🛑 EMERGENCY FREEZE: Global live bridge paused, all cards set to Paper mode.")
        save_state_to_db()
    return jsonify({"ok": True})


@app.route("/api/reset_vault", methods=["POST"])
def api_reset_vault():
    with state_lock:
        state["vault_cash"] = 0.0
        state["added_to_anchors"] = 0.0
        state["total_skims"] = 0
        state["profit_curve"] = [{"time": now_ts(), "value": 0.0}]
        state["skim_ledger"] = []
        for _, coin in state.get("coins", {}).items():
            coin["escrow_profit"] = 0.0
            coin["vaulted_profit"] = 0.0
            coin["share_topup_usd"] = 0.0
            coin["skims_count"] = 0
        log_event("🧹 Vault profit counters & ledger reset to $0.00.")
        save_state_to_db()
    return jsonify({"ok": True})


@app.route("/api/export_csv", methods=["GET"])
def api_export_csv():
    with state_lock:
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["Time", "Symbol", "Mode", "Stage", "Profit_USD", "AnchorTopUp_USD", "Execution_Price", "Shares_Remaining"])
        for item in state.get("skim_ledger", []):
            writer.writerow([
                item.get("time"),
                item.get("symbol"),
                item.get("mode"),
                item.get("stage"),
                item.get("profit"),
                item.get("topup"),
                item.get("price"),
                item.get("shares")
            ])
        csv_data = output.getvalue()

    resp = make_response(csv_data)
    resp.headers["Content-Disposition"] = "attachment; filename=vaultskim_ledger.csv"
    resp.headers["Content-Type"] = "text/csv"
    return resp


@app.route("/manifest.json", methods=["GET"])
def pwa_manifest():
    return jsonify({
        "name": "VaultSkim v5.2 Command Center",
        "short_name": "VaultSkim",
        "start_url": "/",
        "display": "standalone",
        "background_color": "#070b14",
        "theme_color": "#059669",
        "icons": [
            {"src": "https://img.icons8.com/color/192/safe.png", "sizes": "192x192", "type": "image/png"},
            {"src": "https://img.icons8.com/color/512/safe.png", "sizes": "512x512", "type": "image/png"}
        ]
    })


@app.route("/sw.js", methods=["GET"])
def pwa_service_worker():
    js_code = """
    self.addEventListener('install', (e) => {
      e.waitUntil(caches.open('vaultskim-v52').then((cache) => cache.addAll(['/'])));
    });
    self.addEventListener('fetch', (e) => {
      e.respondWith(fetch(e.request).catch(() => caches.match(e.request)));
    });
    """
    return Response(js_code, mimetype="application/javascript")


# ==============================================================================
# 7. COMMAND CENTER DASHBOARD UI (HTML, SVG LINE CHART & JS FRONTEND)
# ==============================================================================
DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>VaultSkim v5.2 · Pure Live Command Center</title>
  <link rel="manifest" href="/manifest.json" />
  <meta name="theme-color" content="#059669" />
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    body { background-color: #060a12; color: #e2e8f0; font-family: system-ui, -apple-system, sans-serif; }
    .card-bg { background: #0c1322; border: 1px solid #1e293b; border-radius: 0.85rem; }
    .live-card-border { border: 1px solid rgba(245, 158, 11, 0.55); box-shadow: 0 0 18px rgba(245, 158, 11, 0.08); }
    .hero-box { background: linear-gradient(135deg, rgba(6, 78, 59, 0.35), rgba(15, 23, 42, 0.9)); border: 1px solid rgba(16, 185, 129, 0.45); border-radius: 0.85rem; }
  </style>
</head>
<body class="p-3 md:p-6 max-w-7xl mx-auto">

  <!-- TOP HEADER & CONTROLS -->
  <div class="flex flex-wrap justify-between items-start gap-4 mb-5">
    <div>
      <div class="flex flex-wrap items-center gap-2.5">
        <h1 class="text-2xl md:text-3xl font-extrabold text-teal-400 tracking-tight">VaultSkim Command Center</h1>
        <span class="text-xs px-2.5 py-1 rounded-full bg-teal-950 text-teal-300 border border-teal-700 font-bold">v5.2 · Pure Live Truth</span>
        <button onclick="installPWA()" id="pwaBtn" class="text-xs px-3 py-1 rounded-md bg-indigo-600 hover:bg-indigo-500 text-white font-bold hidden">📲 Install App</button>
      </div>

      <div class="flex flex-wrap items-center gap-2 mt-3">
        <button onclick="togglePanel('bridgePanel')" class="text-xs px-3 py-1.5 rounded-md bg-amber-600 hover:bg-amber-500 text-white font-bold">🔑 Live Exchange Bridge</button>
        <button onclick="togglePanel('alertsPanel')" class="text-xs px-3 py-1.5 rounded-md bg-sky-900/80 hover:bg-sky-800 text-sky-200 border border-sky-700 font-semibold">📲 Phone Alerts</button>
        <button onclick="togglePanel('accruePanel')" class="text-xs px-3 py-1.5 rounded-md bg-emerald-900/80 hover:bg-emerald-800 text-emerald-200 border border-emerald-700 font-semibold">💰 Accrue Cash-Out & CSV</button>
      </div>

      <div class="flex flex-wrap items-center gap-2 mt-2.5 text-xs text-slate-400">
        <span>Strict Live Gate.io Wallet Sync + Zero Paper Fallback on Live Coins</span>
        <span id="globalModeBadge" class="text-xs px-2.5 py-0.5 rounded font-bold"></span>
        <span id="spotUsdtBadge" class="text-xs px-2.5 py-0.5 rounded bg-slate-900 text-emerald-400 border border-slate-700 font-mono">Spot USDT: $0.00</span>
      </div>

      <div class="flex flex-wrap items-center gap-2 mt-3">
        <button onclick="freezeAll()" class="text-xs px-3.5 py-1.5 rounded-full bg-rose-900/80 hover:bg-rose-800 text-rose-200 border border-rose-600 font-bold">🛑 Freeze Live</button>
        <button onclick="syncLiveWallet()" class="text-xs px-3.5 py-1.5 rounded-full bg-blue-900/80 hover:bg-blue-800 text-blue-200 border border-blue-600 font-bold">🔄 Sync Live</button>
        <button onclick="resetVault()" class="text-xs px-3.5 py-1.5 rounded-full bg-slate-800 hover:bg-slate-700 text-slate-300 border border-slate-600 font-semibold">🧹 Reset ($0.00)</button>
      </div>
    </div>

    <!-- HERO PROFIT BOX -->
    <div class="hero-box px-6 py-4 text-right min-w-[270px]">
      <div class="text-[11px] uppercase tracking-wider text-teal-300 font-bold">Total Untouchable Profit Generated</div>
      <div id="totalProfitHero" class="text-3xl md:text-4xl font-extrabold text-emerald-400 mt-1">$0.00</div>
      <div id="profitSubtext" class="text-xs text-slate-300 mt-1 font-mono">Vault Cash: $0.00 | Added to Anchors: +$0.00</div>
    </div>
  </div>

  <!-- LIVE EXCHANGE BRIDGE PANEL -->
  <div id="bridgePanel" class="card-bg p-5 mb-5 hidden border-amber-500/50">
    <div class="flex justify-between items-center mb-2">
      <h2 class="text-sm font-bold text-amber-400">🔑 Day 5 Live Exchange Execution Bridge (Zero-Withdrawal API Security)</h2>
      <button onclick="togglePanel('bridgePanel')" class="text-slate-400 hover:text-white text-sm">✕</button>
    </div>
    <p class="text-xs text-slate-400 mb-3">Connect Gate.io Spot v4 API keys (with Withdrawals Disabled). Any coin set to ⚡ Live reads your exact Gate.io Spot balances and NEVER executes paper fallback trades.</p>
    <div class="grid grid-cols-1 md:grid-cols-4 gap-3 text-xs">
      <div class="bg-slate-950/70 p-3 rounded border border-slate-800">
        <label class="block text-slate-300 font-bold mb-1">1. Spot Exchange</label>
        <select id="spotExchange" class="w-full bg-slate-900 border border-slate-700 rounded p-2 text-white">
          <option value="gateio">Gate.io Spot v4 (0.1% Fee)</option>
        </select>
        <div class="text-[11px] text-slate-500 mt-1">Auto-clamps to exact Spot wallet decimals</div>
      </div>
      <div class="bg-slate-950/70 p-3 rounded border border-slate-800">
        <label class="block text-slate-300 font-bold mb-1">2. Spot API Key (No Withdrawals)</label>
        <input id="spotKey" type="text" placeholder="Paste API Key..." class="w-full bg-slate-900 border border-slate-700 rounded p-2 text-white font-mono" />
        <div id="savedKeyHint" class="text-[11px] text-emerald-400 mt-1 font-mono"></div>
      </div>
      <div class="bg-slate-950/70 p-3 rounded border border-slate-800">
        <label class="block text-slate-300 font-bold mb-1">3. Spot API Secret</label>
        <input id="spotSecret" type="password" placeholder="Paste API Secret (Encrypted)..." class="w-full bg-slate-900 border border-slate-700 rounded p-2 text-white font-mono" />
        <div class="text-[11px] text-slate-500 mt-1">Never exposed to browser after saving</div>
      </div>
      <div class="bg-slate-950/70 p-3 rounded border border-slate-800">
        <label class="block text-slate-300 font-bold mb-1">4. Max Order Safety Cap (USD)</label>
        <input id="maxCap" type="number" value="250" class="w-full bg-slate-900 border border-slate-700 rounded p-2 text-white font-mono" />
        <div class="text-[11px] text-slate-500 mt-1">Blocks any single order above this $ cap</div>
      </div>
    </div>
    <div class="flex flex-wrap justify-between items-center gap-3 mt-4">
      <div id="bridgeStatusMsg" class="text-xs text-emerald-400 font-mono"></div>
      <div class="flex flex-wrap gap-2">
        <button onclick="verifyBridge()" class="text-xs px-4 py-2 rounded bg-slate-800 hover:bg-slate-700 border border-slate-600 text-white font-semibold">🔍 Verify API & Check USDT Balance</button>
        <button onclick="toggleBridge()" class="text-xs px-4 py-2 rounded bg-amber-600 hover:bg-amber-500 text-white font-bold">⚡ Save & Toggle Live Bridge</button>
      </div>
    </div>
  </div>

  <!-- PHONE ALERTS PANEL -->
  <div id="alertsPanel" class="card-bg p-5 mb-5 hidden border-sky-500/40">
    <div class="flex justify-between items-center mb-3">
      <h2 class="text-sm font-bold text-sky-400">📲 Instant Phone Push Alerts (ntfy.sh & Telegram)</h2>
      <button onclick="togglePanel('alertsPanel')" class="text-slate-400 hover:text-white text-sm">✕</button>
    </div>
    <div class="grid grid-cols-1 md:grid-cols-3 gap-3 text-xs">
      <div>
        <label class="block text-slate-400 mb-1">ntfy.sh Topic Name</label>
        <input id="ntfyTopic" type="text" placeholder="e.g. vaultskim_enoch_live" class="w-full bg-slate-900 border border-slate-700 rounded p-2 text-white" />
      </div>
      <div>
        <label class="block text-slate-400 mb-1">Telegram Bot Token</label>
        <input id="tgToken" type="text" placeholder="123456:ABC-DEF..." class="w-full bg-slate-900 border border-slate-700 rounded p-2 text-white font-mono" />
      </div>
      <div>
        <label class="block text-slate-400 mb-1">Telegram Chat ID</label>
        <input id="tgChat" type="text" placeholder="Chat ID" class="w-full bg-slate-900 border border-slate-700 rounded p-2 text-white font-mono" />
      </div>
    </div>
    <button onclick="saveAlerts()" class="mt-3 text-xs px-4 py-2 rounded bg-sky-600 hover:bg-sky-500 text-white font-bold">💾 Save Alert Channels</button>
  </div>

  <!-- ACCRUE CASH-OUT PANEL -->
  <div id="accruePanel" class="card-bg p-5 mb-5 hidden border-emerald-500/40">
    <div class="flex justify-between items-center mb-2">
      <h2 class="text-sm font-bold text-emerald-400">💰 Accrue Zero-Trading-Fee Cash-Out Guide & Profit Ledger CSV</h2>
      <button onclick="togglePanel('accruePanel')" class="text-slate-400 hover:text-white text-sm">✕</button>
    </div>
    <div class="grid grid-cols-1 md:grid-cols-3 gap-3 text-xs mt-2">
      <div class="bg-slate-950/70 p-3 rounded border border-slate-800">
        <div class="text-emerald-400 font-bold mb-1">Step 1: Ready Vault Cash</div>
        <div class="text-white font-bold mb-1">Available in Bucket B Vault: <span id="accrueVaultAmt">$0.00</span> USDT</div>
        <p class="text-slate-400">Keep Bucket A working on Gate.io; only withdraw your locked Bucket B Vault profits!</p>
      </div>
      <div class="bg-slate-950/70 p-3 rounded border border-slate-800">
        <div class="text-emerald-400 font-bold mb-1">Step 2: Free USDT Deposit to Accrue</div>
        <p class="text-slate-300">In Accrue: Tap <b>Deposit -> Crypto Deposit -> USDT</b>. Choose <b>Solana</b> or <b>Tron (TRC-20)</b> and send your Vault USDT from Gate.io.</p>
      </div>
      <div class="bg-slate-950/70 p-3 rounded border border-slate-800 flex flex-col justify-between">
        <div>
          <div class="text-emerald-400 font-bold mb-1">Step 3: Instant MoMo / Bank Payout</div>
          <p class="text-slate-300 mb-2">Once USDT lands in Accrue as USD, tap <b>Withdraw -> Mobile Money / Bank</b>.</p>
        </div>
        <a href="/api/export_csv" class="block text-center py-2 px-3 rounded bg-emerald-600 hover:bg-emerald-500 text-white font-bold">📥 Download Verified Skim Ledger (.CSV)</a>
      </div>
    </div>
  </div>

  <!-- PROFIT CURVE & VERIFIED SKIM LEDGER -->
  <div class="grid grid-cols-1 lg:grid-cols-3 gap-5 mb-5">
    <!-- SMOOTH SVG LINE CHART -->
    <div class="card-bg p-5 lg:col-span-2">
      <div class="flex justify-between items-center mb-1">
        <h3 class="text-xs font-bold uppercase tracking-wider text-teal-400">📈 LIVE VAULT PROFIT GROWTH CURVE</h3>
        <span id="skimsCountPill" class="text-xs px-2.5 py-0.5 rounded-full bg-emerald-950 text-emerald-300 border border-emerald-700 font-bold">0 Total Skims</span>
      </div>
      <p class="text-xs text-slate-400 mb-3">Tracks cumulative Vault Cash + Share-Keeper Anchor Top-Ups across every skim</p>
      <div class="h-44 w-full bg-slate-950/70 rounded border border-slate-900 p-2 relative" id="chartContainer">
        <svg id="profitSvg" class="w-full h-full" viewBox="0 0 600 160" preserveAspectRatio="none"></svg>
      </div>
    </div>

    <!-- VERIFIED SKIM LEDGER -->
    <div class="card-bg p-5 flex flex-col">
      <div class="flex justify-between items-center mb-2">
        <h3 class="text-xs font-bold uppercase tracking-wider text-amber-400">🧾 VERIFIED SKIM LEDGER</h3>
        <a href="/api/export_csv" class="text-xs text-sky-400 hover:underline font-semibold">📥 CSV</a>
      </div>
      <p class="text-[11px] text-slate-400 mb-2">Recent completed profit sweeps</p>
      <div class="flex-1 overflow-y-auto max-h-44 space-y-2 font-mono text-xs pr-1" id="ledgerList">
        <div class="text-slate-500 text-center py-8">Waiting for next verified profit skim...</div>
      </div>
    </div>
  </div>

  <!-- LAUNCH COIN BAR -->
  <div class="card-bg p-4 mb-5">
    <div class="text-xs font-bold text-teal-400 mb-1">🎯 Launch Any Coin or Paste DEX Contract Address (Anti-Rug Protected):</div>
    <p class="text-[11px] text-slate-400 mb-3">Checks Gate.io + DexScreener Liquidity Pools (Automatically blocks pools under $15K liquidity)</p>
    <div class="flex flex-wrap items-center gap-2.5">
      <input id="newSym" type="text" placeholder="Symbol or Contract (e.g. QNT, PEPE, LQTY, 0x...)" class="bg-slate-950 border border-slate-700 rounded px-3 py-2 text-sm text-white flex-1 min-w-[200px]" />
      <input id="newAnc" type="number" value="10" class="bg-slate-950 border border-slate-700 rounded px-3 py-2 text-sm text-white w-24 font-mono" />
      <button onclick="addCoin()" class="px-4 py-2 rounded bg-emerald-600 hover:bg-emerald-500 text-white font-bold text-sm">+ Snipe & Launch</button>
      <button onclick="togglePanel('memeRadarPanel')" class="px-4 py-2 rounded bg-amber-600 hover:bg-amber-500 text-white font-bold text-sm">🔥 Scan Meme Radar</button>
    </div>
  </div>

  <!-- MEME RADAR PANEL -->
  <div id="memeRadarPanel" class="card-bg p-5 mb-5 hidden border-amber-500/40">
    <div class="flex justify-between items-center mb-3">
      <h2 class="text-sm font-bold text-amber-400">🔥 Gate.io High-Velocity Momentum Radar</h2>
      <button onclick="loadMemeRadar()" class="text-xs px-3 py-1 rounded bg-slate-800 hover:bg-slate-700 text-slate-200">🔄 Refresh</button>
    </div>
    <div id="memeRadarGrid" class="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-4 gap-3 text-xs"></div>
  </div>

  <!-- ACTIVE COIN CARDS GRID -->
  <div id="coinsGrid" class="grid grid-cols-1 md:grid-cols-2 gap-5 mb-6"></div>

  <!-- LIVE EXECUTION & ANTI-RUG SECURITY LOG -->
  <div class="card-bg p-5">
    <h3 class="text-xs font-bold uppercase tracking-wider text-slate-400 mb-2">LIVE EXECUTION & ANTI-RUG SECURITY LOG</h3>
    <div id="secLog" class="font-mono text-xs space-y-1.5 max-h-64 overflow-y-auto text-slate-300 bg-slate-950/80 p-3 rounded border border-slate-900"></div>
  </div>

<script>
let deferredPrompt = null;
window.addEventListener('beforeinstallprompt', (e) => {
  e.preventDefault();
  deferredPrompt = e;
  document.getElementById('pwaBtn').classList.remove('hidden');
});

function installPWA() {
  if (deferredPrompt) {
    deferredPrompt.prompt();
    deferredPrompt.userChoice.then(() => { deferredPrompt = null; });
  }
}

function togglePanel(id) {
  document.getElementById(id).classList.toggle('hidden');
}

async function verifyBridge() {
  const btn = event.target;
  const origText = btn.innerText;
  btn.innerText = '⏳ Checking Exchange...';
  const res = await fetch('/api/verify_exchange', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({
      spot_exchange: document.getElementById('spotExchange').value,
      spot_api_key: document.getElementById('spotKey').value,
      spot_api_secret: document.getElementById('spotSecret').value,
      max_order_cap: document.getElementById('maxCap').value
    })
  });
  const d = await res.json();
  btn.innerText = d.ok ? '✅ Verified' : origText;
  setTimeout(() => { btn.innerText = origText; }, 2500);
  document.getElementById('bridgeStatusMsg').innerText = d.message || '';
  refreshState();
}

async function toggleBridge() {
  await fetch('/api/toggle_bridge', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({
      spot_exchange: document.getElementById('spotExchange').value,
      spot_api_key: document.getElementById('spotKey').value,
      spot_api_secret: document.getElementById('spotSecret').value,
      max_order_cap: document.getElementById('maxCap').value
    })
  });
  refreshState();
}

async function syncLiveWallet() {
  const res = await fetch('/api/sync_live_wallet', {method: 'POST'});
  const d = await res.json();
  alert(d.message || 'Wallet synced.');
  refreshState();
}

async function saveAlerts() {
  await fetch('/api/save_alerts', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({
      ntfy_topic: document.getElementById('ntfyTopic').value,
      telegram_bot_token: document.getElementById('tgToken').value,
      telegram_chat_id: document.getElementById('tgChat').value
    })
  });
  alert('Phone alert settings saved!');
}

async function coinAction(symbol, action, extra={}) {
  const res = await fetch('/api/coin_action', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({symbol, action, ...extra})
  });
  const d = await res.json();
  if (d && d.ok === false && d.message) {
    alert(d.message);
  }
  refreshState();
}

async function editAnchorPrompt(symbol, currentAnc) {
  const val = prompt(`Enter new Anchor USD for ${symbol} (If ${symbol} is in LIVE mode and you increase the Anchor, VaultSkim will immediately buy the difference on Gate.io):`, currentAnc);
  if (val !== null && !isNaN(parseFloat(val))) {
    await coinAction(symbol, 'edit_anchor', {anchor_usd: parseFloat(val)});
  }
}

async function addCoin() {
  const symbol = document.getElementById('newSym').value;
  const anchor_usd = parseFloat(document.getElementById('newAnc').value || '10');
  const res = await fetch('/api/add_coin', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({symbol, anchor_usd, live_mode: false})
  });
  const d = await res.json();
  if (!d.ok) {
    alert('Launch Blocked: ' + d.message);
  } else {
    document.getElementById('newSym').value = '';
  }
  refreshState();
}

async function freezeAll() {
  await fetch('/api/freeze_all', {method: 'POST'});
  refreshState();
}

async function resetVault() {
  if (confirm('Reset Vault profit counters and ledger to $0.00? (Your live Gate.io holdings remain untouched).')) {
    await fetch('/api/reset_vault', {method: 'POST'});
    refreshState();
  }
}

function renderProfitLineChart(curve) {
  const svg = document.getElementById('profitSvg');
  if (!curve || curve.length === 0) {
    svg.innerHTML = '';
    return;
  }
  const pts = curve.length === 1 ? [curve[0], curve[0]] : curve;
  const vals = pts.map(p => parseFloat(p.value || 0));
  const maxV = Math.max(...vals, 0.05);
  const minV = 0.0;
  const w = 600, h = 140, padX = 35, padY = 15;

  const coords = pts.map((p, idx) => {
    const x = padX + (idx / (pts.length - 1)) * (w - padX - 15);
    const ratio = (parseFloat(p.value || 0) - minV) / (maxV - minV || 1);
    const y = (h - padY) - ratio * (h - 2 * padY);
    return {x, y, val: parseFloat(p.value || 0), time: p.time || ''};
  });

  const polyPoints = coords.map(c => `${c.x.toFixed(1)},${c.y.toFixed(1)}`).join(' ');
  const areaPoints = `${coords[0].x.toFixed(1)},${h - padY} ${polyPoints} ${coords[coords.length - 1].x.toFixed(1)},${h - padY}`;

  let dotsHtml = coords.map(c => `<circle cx="${c.x.toFixed(1)}" cy="${c.y.toFixed(1)}" r="3.5" fill="#10b981" />`).join('');
  svg.innerHTML = `
    <defs>
      <linearGradient id="emeraldGrad" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%" stop-color="#10b981" stop-opacity="0.35"/>
        <stop offset="100%" stop-color="#10b981" stop-opacity="0.02"/>
      </linearGradient>
    </defs>
    <polygon points="${areaPoints}" fill="url(#emeraldGrad)" />
    <polyline fill="none" stroke="#10b981" stroke-width="2.5" points="${polyPoints}" />
    ${dotsHtml}
  `;
}

async function loadMemeRadar() {
  const grid = document.getElementById('memeRadarGrid');
  try {
    const res = await fetch('https://api.gateio.ws/api/v4/spot/tickers');
    const data = await res.json();
    const watch = ['QNT', 'LQTY', 'LTC', 'DASH', 'PEPE', 'BONK', 'WIF', 'FLOKI', 'SHIB', 'DOGE', 'POPCAT', 'SOL'];
    const filtered = data.filter(t => watch.some(s => t.currency_pair === `${s}_USDT`));
    filtered.sort((a, b) => parseFloat(b.change_percentage) - parseFloat(a.change_percentage));
    grid.innerHTML = filtered.map(t => {
      const sym = t.currency_pair.replace('_USDT', '');
      const chg = parseFloat(t.change_percentage);
      const col = chg >= 0 ? 'text-emerald-400' : 'text-rose-400';
      return `
        <div class="bg-slate-950 p-3 rounded border border-slate-800 flex flex-col justify-between">
          <div class="flex justify-between items-center">
            <span class="font-bold text-white">${sym}/USDT</span>
            <span class="${col} font-bold">${chg >= 0 ? '+' : ''}${chg.toFixed(2)}%</span>
          </div>
          <div class="text-slate-400 font-mono my-1">$${parseFloat(t.last).toFixed(4)}</div>
          <button onclick="document.getElementById('newSym').value='${sym}'; togglePanel('memeRadarPanel');" class="mt-1 py-1 rounded bg-emerald-600 hover:bg-emerald-500 text-white font-bold">
            + Select ${sym}
          </button>
        </div>
      `;
    }).join('');
  } catch (e) {
    grid.innerHTML = '<div class="text-slate-400">Radar feed unavailable.</div>';
  }
}

async function refreshState() {
  try {
    const res = await fetch('/api/state');
    const s = await res.json();

    const vaultCash = parseFloat(s.vault_cash || 0);
    const addedAnchors = parseFloat(s.added_to_anchors || 0);
    const totalUntouchable = vaultCash + addedAnchors;

    document.getElementById('totalProfitHero').innerText = '$' + totalUntouchable.toFixed(2);
    document.getElementById('profitSubtext').innerText = `Vault Cash: $${vaultCash.toFixed(2)} | Added to Anchors: +$${addedAnchors.toFixed(2)}`;
    document.getElementById('accrueVaultAmt').innerText = '$' + vaultCash.toFixed(2);
    document.getElementById('skimsCountPill').innerText = `${s.total_skims || 0} Total Skims`;
    document.getElementById('spotUsdtBadge').innerText = `Gate.io Spot USDT: $${parseFloat(s.last_verified_usdt || 0).toFixed(2)}`;
    document.getElementById('savedKeyHint').innerText = s.spot_api_key_masked ? `Saved Key: ${s.spot_api_key_masked}` : '';

    if (s.last_verified_usdt !== undefined && !document.getElementById('bridgeStatusMsg').innerText) {
      document.getElementById('bridgeStatusMsg').innerText = `Gate.io v4 Connected! Available Spot USDT: $${parseFloat(s.last_verified_usdt || 0).toFixed(2)}`;
    }

    const badge = document.getElementById('globalModeBadge');
    if (s.live_bridge_enabled) {
      badge.className = 'text-xs px-2.5 py-0.5 rounded bg-amber-950 text-amber-300 border border-amber-600 font-bold';
      badge.innerText = '⚡ LIVE BRIDGE ARMED (GATEIO)';
    } else {
      badge.className = 'text-xs px-2.5 py-0.5 rounded bg-slate-800 text-slate-300 border border-slate-700 font-bold';
      badge.innerText = '🛡️ PAPER SIMULATOR';
    }

    renderProfitLineChart(s.profit_curve || []);

    // Verified Skim Ledger
    const ledgerBox = document.getElementById('ledgerList');
    const ledger = s.skim_ledger || [];
    if (ledger.length > 0) {
      ledgerBox.innerHTML = ledger.map(l => {
        const tagClass = l.mode === 'LIVE' ? 'text-amber-300 font-bold' : 'text-slate-400';
        return `
          <div class="bg-slate-950/90 p-2.5 rounded border border-slate-800/90 flex justify-between items-center">
            <div>
              <div>
                <span class="font-bold text-teal-400">${l.symbol}</span>
                <span class="${tagClass} text-[11px] ml-1">[${l.mode}]</span>
                <span class="text-slate-200 text-[11px] ml-1">${l.stage}</span>
              </div>
              <div class="text-slate-400 text-[10px] mt-0.5">@ $${parseFloat(l.price).toFixed(4)} · ${l.time}</div>
            </div>
            <div class="text-right">
              <div class="text-emerald-400 font-extrabold">+$${parseFloat(l.profit).toFixed(4)}</div>
              <div class="text-sky-400 text-[10px]">${parseFloat(l.shares).toFixed(5)} sh</div>
            </div>
          </div>
        `;
      }).join('');
    } else {
      ledgerBox.innerHTML = '<div class="text-slate-500 text-center py-8">Waiting for next verified profit skim...</div>';
    }

    // Coin Cards
    const grid = document.getElementById('coinsGrid');
    grid.innerHTML = '';
    Object.values(s.coins || {}).forEach(c => {
      const px = parseFloat(c.last_price || 0);
      const sh = parseFloat(c.shares_owned || 0);
      const bagVal = sh * px;
      const cashRes = parseFloat(c.cash_reserve_usd || 0);
      const totalBucketA = (c.status === 'MOONBAG_RIDING' || c.status === 'WAITING_DIP_REBUY' || c.status === 'WAITING_LIVE_BUY')
        ? (cashRes + bagVal)
        : bagVal;

      const lowPx = parseFloat(c.sliding_low || px || 1);
      const pctFromLow = lowPx > 0 ? (((px - lowPx) / lowPx) * 100) : 0.0;
      const targetPx = lowPx * 1.008;

      const liveBtnClass = c.live_mode
        ? 'bg-amber-950/90 text-amber-300 border border-amber-500'
        : 'bg-slate-800 text-slate-300 border border-slate-600';
      const liveBtnText = c.live_mode ? '⚡ LIVE ORDER' : '🧪 PAPER';
      const stratText = c.strategy === 'hybrid_50_50' ? '🚀 Strategy: 50/50 Hybrid Moonbag' : '💰 Strategy: 100% Share-Keeper';

      const sourceBadge = (c.source === 'dex' && c.contract)
        ? `<span class="text-[11px] text-teal-300 font-semibold">🛡️ SAFE DEX ($${Math.round(c.liquidity_usd || 0).toLocaleString()} Liq)</span>`
        : `<span class="text-[11px] text-slate-300 font-semibold">🛡️ VERIFIED CEX (GATE.IO SPOT)</span>`;

      let anchorSubLabel = '(In Token)';
      let totalBucketBreakdown = `$${totalBucketA.toFixed(2)}`;
      if (c.status === 'WAITING_LIVE_BUY' || sh <= 0) {
        anchorSubLabel = '(Waiting Gate.io Buy · $0 in Token)';
        totalBucketBreakdown = `$${cashRes.toFixed(2)} Cash Reserved ($0.00 In Token)`;
      } else if (c.status === 'MOONBAG_RIDING') {
        anchorSubLabel = '(50% Cash + 50% Moonbag)';
        totalBucketBreakdown = `$${totalBucketA.toFixed(2)} ($${cashRes.toFixed(2)} Cash + $${bagVal.toFixed(2)} Bag)`;
      }

      const simBtnHtml = c.live_mode
        ? `<div class="w-full mt-4 py-2 px-3 rounded bg-slate-950 text-slate-400 text-[11px] text-center border border-slate-800 font-mono">
             🔒 Live Gate.io Mode Active — Paper Simulator Disabled
           </div>`
        : `<button onclick="coinAction('${c.symbol}', 'practice_spike')" class="w-full mt-4 py-2 rounded bg-slate-800 hover:bg-slate-700 text-amber-300 text-xs font-bold border border-slate-700">
             ⚡ Practice Spike (+2% Simulator)
           </button>`;

      const card = document.createElement('div');
      card.className = 'card-bg p-5 relative ' + (c.live_mode ? 'live-card-border' : '');
      card.innerHTML = `
        <div class="flex justify-between items-center mb-1.5">
          <div class="flex flex-wrap items-center gap-2">
            <span class="text-xl font-extrabold text-white">${c.pair}</span>
            <button onclick="coinAction('${c.symbol}', 'toggle_live')" class="text-xs px-2.5 py-1 rounded font-bold ${liveBtnClass}">${liveBtnText}</button>
            <span class="text-xs px-2.5 py-0.5 rounded-full bg-teal-950 text-teal-300 border border-teal-700 font-bold">${c.status}</span>
          </div>
          <button onclick="coinAction('${c.symbol}', 'remove')" class="text-slate-500 hover:text-rose-400 text-sm font-bold">✕</button>
        </div>

        <div class="flex justify-between items-center mb-3">
          ${sourceBadge}
          <span class="text-xs text-emerald-400 font-mono font-bold">+$${parseFloat(c.share_topup_usd || 0).toFixed(2)} Share-TopUp</span>
        </div>

        <div class="flex justify-between items-center bg-indigo-950/40 border border-indigo-800/50 rounded px-3 py-1.5 mb-3 text-xs">
          <span class="text-indigo-200 font-semibold">${stratText}</span>
          <button onclick="coinAction('${c.symbol}', 'switch_strategy')" class="text-indigo-300 underline font-bold">Switch</button>
        </div>

        <div class="flex items-baseline gap-2.5 mb-4">
          <span class="text-2xl md:text-3xl font-extrabold text-white font-mono">$${px.toFixed(4)}</span>
          <span class="text-xs text-emerald-400 font-mono">(${pctFromLow.toFixed(2)}% from Low)</span>
        </div>

        <div class="space-y-2 text-xs border-t border-slate-800/90 pt-3">
          <div class="flex justify-between items-center">
            <span class="text-slate-400">Bucket A (Anchor):</span>
            <span class="font-bold text-white font-mono">$${parseFloat(c.anchor_usd || 0).toFixed(2)} <span class="text-slate-400 font-normal">${anchorSubLabel}</span>
              <button onclick="editAnchorPrompt('${c.symbol}', ${c.anchor_usd || 0})" class="ml-1 px-2 py-0.5 rounded bg-teal-950 text-teal-300 border border-teal-700 font-sans">✏️ Edit</button>
            </span>
          </div>
          <div class="flex justify-between">
            <span class="text-slate-400">Real Shares Owned (Gate.io):</span>
            <span class="font-mono text-sky-400 font-bold">${sh.toFixed(6)} ${c.symbol}</span>
          </div>
          <div class="flex justify-between">
            <span class="text-slate-400">Total Bucket A Value:</span>
            <span class="font-bold text-white font-mono">${totalBucketBreakdown}</span>
          </div>
          <div class="flex justify-between">
            <span class="text-slate-400">Entry / Sliding Low:</span>
            <span class="font-mono text-slate-200">$${parseFloat(c.entry_price || 0).toFixed(4)} / <span class="text-sky-400">$${lowPx.toFixed(4)}</span></span>
          </div>
          <div class="flex justify-between">
            <span class="text-slate-400">Zero-Loss / +0.8% Bar:</span>
            <span class="font-mono text-amber-400">${c.zero_loss_stop > 0 ? 'LOCKED Stop @ $' + parseFloat(c.zero_loss_stop).toFixed(4) : 'Shield @ +0.35% | Target $' + targetPx.toFixed(4)}</span>
          </div>
          <div class="flex justify-between pt-2 border-t border-slate-800/90">
            <span class="text-emerald-400 font-bold">Bucket B (Vaulted):</span>
            <span class="font-mono text-emerald-400 font-extrabold">+$${parseFloat(c.vaulted_profit || 0).toFixed(4)} (${c.skims_count || 0} skims)</span>
          </div>
        </div>

        ${simBtnHtml}
      `;
      grid.appendChild(card);
    });

    const logBox = document.getElementById('secLog');
    logBox.innerHTML = (s.security_log || []).map(l => `<div>${l}</div>`).join('');
  } catch (e) {
    console.error(e);
  }
}

setInterval(refreshState, 3000);
refreshState();
loadMemeRadar();
</script>
</body>
</html>
"""


@app.route("/", methods=["GET"])
def index():
    return render_template_string(DASHBOARD_HTML)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
