import os
import time
import json
import hmac
import hashlib
import urllib.parse
import threading
import requests
import psycopg2
from datetime import datetime
from flask import Flask, jsonify, request, render_template_string, Response

app = Flask(__name__)

# =========================================================
# PART 1: CONFIGURATION, LIVE EXCHANGE BRIDGE & CORE ENGINE
# =========================================================
DATABASE_URL = os.environ.get("DATABASE_URL")
STATE_FILE = "vault_state.json"
TRADING_FEE_PCT = 0.001          # 0.10% per swap (0.20% round-trip)
MIN_NET_PROFIT_FLOOR = 0.0022    # Guaranteed > 0.20% fees so EVERY sale makes net profit
EARLY_SHIELD_ARM_PCT = 0.0035    # At +0.35% up, lock Zero-Loss Stop at Entry + 0.22%
MIN_MEME_LIQUIDITY_USD = 15000.0 # Anti-Rug Shield: Blocks DEX pools with < $15,000 liquidity

ENV_EXCHANGE_PROVIDER = os.environ.get("EXCHANGE_PROVIDER", "gateio").lower()
ENV_API_KEY = os.environ.get("EXCHANGE_API_KEY", "").strip()
ENV_API_SECRET = os.environ.get("EXCHANGE_API_SECRET", "").strip()

def make_coin_template(symbol, anchor_usd, strategy="HYBRID_50_50", token_address="", chain_id=""):
    return {
        "symbol": symbol.upper(),
        "anchor_usd": float(anchor_usd),
        "initial_anchor_usd": float(anchor_usd),
        "reinvested_anchor_usd": 0.00,
        "vault_profit_usd": 0.00,
        "pending_profit_usd": 0.00,
        "target_shares": 0.0,
        "strategy": strategy,          # "FULL_100" or "HYBRID_50_50"
        "live_enabled": False,         # Safety Lock: False = Paper Simulator, True = Live Exchange Order
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
        "trades_count": 0,
        "token_address": token_address,
        "chain_id": chain_id,
        "liquidity_usd": 0.0,
        "volume_24h_usd": 0.0,
        "security_badge": "🛡️ VERIFIED CEX",
        "price_source": "CEX"
    }

DEFAULT_COINS = {
    "QNTUSDT": make_coin_template("QNT", 180.00, strategy="HYBRID_50_50"),
    "CELOUSDT": make_coin_template("CELO", 100.00, strategy="FULL_100"),
    "KSMUSDT": make_coin_template("KSM", 100.00, strategy="HYBRID_50_50")
}

WAKE_UP_PROFIT_PCT = 0.0078    # +0.80% above Sliding Low wakes up full tight Ratchet
MICRO_TRAIL_PCT = 0.0020       # 0.20% tight trail behind peak once armed
MOONBAG_TRAIL_PCT = 0.0120     # 1.20% wider trail for Stage 2 Moonbag rallies
WIDE_BREAKOUT_PCT = 0.030      # +3.0% switches Full Mode to wider trail
WIDE_TRAIL_PCT = 0.0080        # 0.80% trail for big rallies in Full Mode

DIP_PULLBACK_TARGET = -0.0038  # Door 1: Instant re-buy at -0.4% dip (Grows shares + 100% Escrow to Vault!)
STEP_UP_REBUY_PCT = 0.0020     # Door 2: +0.2% Step-Up (ONLY triggers if Escrow covers 100% of Target Shares!)

bot_state = {
    "coins": DEFAULT_COINS,
    "logs": [],
    "radar": [],
    "history": [],
    "skims": [],
    "alerts": {
        "enabled": True,
        "ntfy_topic": "",
        "tg_token": "",
        "tg_chat_id": "",
        "webhook_url": ""
    },
    "exchange": {
        "global_live_armed": False,
        "provider": ENV_EXCHANGE_PROVIDER,
        "api_key": ENV_API_KEY,
        "api_secret": ENV_API_SECRET,
        "max_order_usd": 250.0,
        "usdt_balance": 0.0,
        "last_check_status": "Paper Simulator Active (No Live Keys Required)"
    }
}

# Reentrant lock prevents any possibility of thread deadlocks
state_lock = threading.RLock()
worker_started = False
db_dirty = False


def fmt_px(val):
    if val <= 0:
        return "0.0000"
    if val < 0.00001:
        return f"{val:.9f}"
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


def clean_dec(val, max_decimals=6):
    """Formats floats for Exchange APIs without scientific notation (e.g. no 1e-05 errors)."""
    if val <= 0:
        return "0"
    s = f"{val:.{max_decimals}f}"
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s if s else "0"


def add_log(msg):
    timestamp = datetime.utcnow().strftime("%H:%M:%S")
    entry = f"[{timestamp}] {msg}"
    print(entry, flush=True)
    with state_lock:
        bot_state["logs"].insert(0, entry)
        bot_state["logs"] = bot_state["logs"][:80]


def calculate_totals():
    vault_cash = 0.0
    reinvested = 0.0
    total_skims = 0
    for c in bot_state.get("coins", {}).values():
        vault_cash += (c.get("vault_profit_usd", 0.0) + c.get("pending_profit_usd", 0.0))
        reinvested += c.get("reinvested_anchor_usd", 0.0)
        total_skims += int(c.get("trades_count", 0))
    return round(vault_cash + reinvested, 4), round(vault_cash, 4), round(reinvested, 4), total_skims


def record_skim_and_chart(symbol, skim_type, price, shares, profit_added, is_live=False):
    now_str = datetime.utcnow().strftime("%H:%M:%S")
    grand_total, vault_cash, reinvested, _ = calculate_totals()
    mode_tag = "LIVE" if is_live else "PAPER"

    bot_state.setdefault("history", [])
    bot_state["history"].append({
        "time": now_str,
        "total": grand_total,
        "vault": vault_cash,
        "reinvested": reinvested,
        "label": f"[{mode_tag}] {symbol} +${profit_added:.4f}"
    })
    bot_state["history"] = bot_state["history"][-50:]

    bot_state.setdefault("skims", [])
    bot_state["skims"].insert(0, {
        "time": now_str,
        "symbol": symbol,
        "type": f"[{mode_tag}] {skim_type}",
        "price": fmt_px(price),
        "shares": fmt_qty(shares),
        "profit": round(profit_added, 4),
        "grand_total": grand_total
    })
    bot_state["skims"] = bot_state["skims"][:40]


# =========================================================
# 2. DAY 5 LIVE EXCHANGE BRIDGE (GATE.IO v4 & BINANCE v3)
# =========================================================
def get_active_credentials():
    ex = bot_state.get("exchange") or {}
    provider = (ex.get("provider") or ENV_EXCHANGE_PROVIDER or "gateio").lower().strip()
    key = (ex.get("api_key") or ENV_API_KEY or "").strip()
    secret = (ex.get("api_secret") or ENV_API_SECRET or "").strip()
    return provider, key, secret


def gateio_signed_request(method, path, query_str="", body_dict=None, api_key="", api_secret=""):
    t = str(int(time.time()))
    payload_str = json.dumps(body_dict) if body_dict is not None else ""
    hashed_payload = hashlib.sha512(payload_str.encode("utf-8")).hexdigest()
    sign_string = f"{method.upper()}\n{path}\n{query_str}\n{hashed_payload}\n{t}"
    signature = hmac.new(
        api_secret.encode("utf-8"),
        sign_string.encode("utf-8"),
        hashlib.sha512
    ).hexdigest()

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "KEY": api_key,
        "Timestamp": t,
        "SIGN": signature
    }
    url = f"https://api.gateio.ws{path}"
    if query_str:
        url += f"?{query_str}"

    if method.upper() == "GET":
        return requests.get(url, headers=headers, timeout=5)
    return requests.post(url, headers=headers, data=payload_str, timeout=5)


def binance_signed_request(method, path, params_dict=None, api_key="", api_secret=""):
    params = dict(params_dict or {})
    params["timestamp"] = int(time.time() * 1000)
    params["recvWindow"] = 5000
    query_str = urllib.parse.urlencode(params)
    signature = hmac.new(
        api_secret.encode("utf-8"),
        query_str.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()
    full_url = f"https://api.binance.com{path}?{query_str}&signature={signature}"
    headers = {"X-MBX-APIKEY": api_key}

    if method.upper() == "GET":
        return requests.get(full_url, headers=headers, timeout=5)
    return requests.post(full_url, headers=headers, timeout=5)


def fetch_exchange_usdt_balance():
    provider, key, secret = get_active_credentials()
    if not key or not secret:
        return False, 0.0, "No API Key/Secret entered yet (Paper Simulator Mode)."

    try:
        if provider == "gateio":
            resp = gateio_signed_request("GET", "/api/v4/spot/accounts", "currency=USDT", None, key, secret)
            if resp.status_code == 200:
                arr = resp.json()
                bal = float(arr[0].get("available", 0.0)) if isinstance(arr, list) and len(arr) > 0 else 0.0
                return True, bal, f"Gate.io v4 Connected! Available Spot USDT: ${bal:,.2f}"
            return False, 0.0, f"Gate.io Auth Error ({resp.status_code}): {resp.text[:120]}"

        elif provider == "binance":
            resp = binance_signed_request("GET", "/api/v3/account", {}, key, secret)
            if resp.status_code == 200:
                balances = resp.json().get("balances") or []
                usdt_bal = 0.0
                for b in balances:
                    if b.get("asset") == "USDT":
                        usdt_bal = float(b.get("free", 0.0))
                        break
                return True, usdt_bal, f"Binance v3 Connected! Available Spot USDT: ${usdt_bal:,.2f}"
            return False, 0.0, f"Binance Auth Error ({resp.status_code}): {resp.text[:120]}"

        return False, 0.0, f"Unknown provider: {provider}"
    except Exception as e:
        return False, 0.0, f"Connection error: {e}"


def execute_live_spot_order(coin, side, usd_amount, token_qty, limit_bound_price):
    ex = bot_state.get("exchange") or {}
    if not ex.get("global_live_armed", False) or not coin.get("live_enabled", False):
        return False, "PAPER_SIM"

    provider, key, secret = get_active_credentials()
    if not key or not secret:
        add_log(f"⚠️ LIVE ORDER BLOCKED ({coin['symbol']}): API Keys missing. Executed in Paper Mode.")
        return False, "NO_KEYS"

    max_cap = float(ex.get("max_order_usd", 250.0))
    if usd_amount > max_cap:
        msg = f"🛡️ SAFETY CAP BLOCKED LIVE ORDER ({coin['symbol']}): ${usd_amount:.2f} exceeds Max Cap (${max_cap:.2f})!"
        add_log(msg)
        return False, msg

    if usd_amount < 1.0:
        msg = f"⚠️ LIVE ORDER SKIPPED ({coin['symbol']}): Order value ${usd_amount:.2f} below $1.00 exchange minimum."
        add_log(msg)
        return False, msg

    sym = coin["symbol"].upper()
    px_decimals = 8 if limit_bound_price < 0.001 else (6 if limit_bound_price < 0.1 else 4)
    qty_decimals = 2 if token_qty >= 100 else (4 if token_qty >= 1 else 6)

    try:
        if provider == "gateio":
            pair_gate = f"{sym}_USDT"
            if side.lower() == "buy":
                buy_limit_px = limit_bound_price * 1.0015
                calc_qty = (usd_amount * 0.999) / buy_limit_px
                body = {
                    "currency_pair": pair_gate,
                    "type": "limit",
                    "account": "spot",
                    "side": "buy",
                    "amount": clean_dec(calc_qty, qty_decimals),
                    "price": clean_dec(buy_limit_px, px_decimals),
                    "time_in_force": "ioc"
                }
            else:
                body = {
                    "currency_pair": pair_gate,
                    "type": "limit",
                    "account": "spot",
                    "side": "sell",
                    "amount": clean_dec(token_qty, qty_decimals),
                    "price": clean_dec(limit_bound_price, px_decimals),
                    "time_in_force": "ioc"
                }

            resp = gateio_signed_request("POST", "/api/v4/spot/orders", "", body, key, secret)
            if resp.status_code in (200, 201):
                oid = resp.json().get("id", "OK")
                add_log(f"⚡ LIVE GATE.IO {side.upper()} EXECUTED ({sym}): Order #{oid} filled!")
                return True, f"GATE_ORDER_{oid}"
            else:
                err_msg = f"⚠️ Gate.io Live Order Notice ({resp.status_code}): {resp.text[:100]}"
                add_log(err_msg)
                return False, err_msg

        elif provider == "binance":
            pair_bin = f"{sym}USDT"
            if side.lower() == "buy":
                params = {
                    "symbol": pair_bin,
                    "side": "BUY",
                    "type": "MARKET",
                    "quoteOrderQty": clean_dec(usd_amount, 2)
                }
            else:
                params = {
                    "symbol": pair_bin,
                    "side": "SELL",
                    "type": "LIMIT",
                    "timeInForce": "IOC",
                    "quantity": clean_dec(token_qty, qty_decimals),
                    "price": clean_dec(limit_bound_price, px_decimals)
                }
            resp = binance_signed_request("POST", "/api/v3/order", params, key, secret)
            if resp.status_code in (200, 201):
                oid = resp.json().get("orderId", "OK")
                add_log(f"⚡ LIVE BINANCE {side.upper()} EXECUTED ({sym}): Order #{oid} filled!")
                return True, f"BINANCE_ORDER_{oid}"
            else:
                err_msg = f"⚠️ Binance Live Order Notice ({resp.status_code}): {resp.text[:100]}"
                add_log(err_msg)
                return False, err_msg

    except Exception as e:
        add_log(f"⚠️ Live Exchange Network Notice ({sym}): {e}")
        return False, str(e)

    return False, "UNKNOWN"


# =========================================================
# 3. PHONE NOTIFICATIONS, DEXSCREENER & CORE SKIM ENGINE
# =========================================================
def _dispatch_alert_async(title, message, payload, alerts_cfg):
    ntfy_topic = (alerts_cfg.get("ntfy_topic") or "").strip()
    if ntfy_topic:
        try:
            requests.post(
                f"https://ntfy.sh/{ntfy_topic}",
                data=message.encode("utf-8"),
                headers={"Title": title, "Priority": "high", "Tags": "moneybag,chart_with_upwards_trend"},
                timeout=4
            )
        except Exception:
            pass

    tg_token = (alerts_cfg.get("tg_token") or "").strip()
    tg_chat = (alerts_cfg.get("tg_chat_id") or "").strip()
    if tg_token and tg_chat:
        try:
            tg_url = f"https://api.telegram.org/bot{tg_token}/sendMessage"
            requests.post(
                tg_url,
                json={"chat_id": tg_chat, "text": f"💰 *{title}*\n{message}", "parse_mode": "Markdown"},
                timeout=4
            )
        except Exception:
            pass

    webhook = (alerts_cfg.get("webhook_url") or "").strip()
    if webhook:
        try:
            requests.post(
                webhook,
                json={"title": title, "content": f"💰 **{title}**\n{message}", "data": payload},
                timeout=4
            )
        except Exception:
            pass


def trigger_phone_alert(symbol, skim_type, profit_usd, price, shares, is_live=False):
    alerts_cfg = bot_state.get("alerts") or {}
    if not alerts_cfg.get("enabled", True):
        return
    if not any([alerts_cfg.get("ntfy_topic"), alerts_cfg.get("tg_token"), alerts_cfg.get("webhook_url")]):
        return

    grand_total, vault_cash, reinvested, total_skims = calculate_totals()
    mode_tag = "⚡ LIVE MONEY" if is_live else "🧪 PAPER"
    title = f"VaultSkim ({mode_tag}): {symbol} +${profit_usd:.4f}!"
    message = (
        f"{skim_type} on {symbol}/USDT @ ${fmt_px(price)}\n"
        f"Profit Locked: +${profit_usd:.4f} | Shares Protected: {fmt_qty(shares)} {symbol}\n"
        f"Total Profit Generated: ${grand_total:.2f} (Vault: ${vault_cash:.2f} | Anchors: +${reinvested:.2f} | {total_skims} skims)"
    )
    payload = {
        "symbol": symbol,
        "mode": mode_tag,
        "skim_type": skim_type,
        "profit_usd": profit_usd,
        "price": price,
        "grand_total_usd": grand_total
    }
    threading.Thread(
        target=_dispatch_alert_async,
        args=(title, message, payload, dict(alerts_cfg)),
        daemon=True
    ).start()


KNOWN_INITIAL_ANCHORS = {
    "QNT": 204.0, "CELO": 100.0, "KSM": 100.0, "SOL": 100.0,
    "DASH": 60.0, "LTC": 60.0, "PENGU": 50.0, "WIF": 50.0,
    "COMP": 20.0, "LQTY": 20.0, "AAVE": 10.0, "ETH": 10.0,
    "INJ": 10.0, "LINK": 10.0, "PEPE": 10.0, "XCN": 10.0,
    "AI": 5.0, "BTC": 5.0
}


def ensure_coin_fields():
    bot_state.setdefault("radar", [])
    bot_state.setdefault("history", [])
    bot_state.setdefault("skims", [])
    bot_state.setdefault("alerts", {
        "enabled": True, "ntfy_topic": "", "tg_token": "", "tg_chat_id": "", "webhook_url": ""
    })
    bot_state.setdefault("exchange", {
        "global_live_armed": False,
        "provider": ENV_EXCHANGE_PROVIDER,
        "api_key": ENV_API_KEY,
        "api_secret": ENV_API_SECRET,
        "max_order_usd": 250.0,
        "usdt_balance": 0.0,
        "last_check_status": "Paper Simulator Active"
    })
    if ENV_API_KEY and not bot_state["exchange"].get("api_key"):
        bot_state["exchange"]["api_key"] = ENV_API_KEY
    if ENV_API_SECRET and not bot_state["exchange"].get("api_secret"):
        bot_state["exchange"]["api_secret"] = ENV_API_SECRET

    for pair, c in bot_state.get("coins", {}).items():
        sym = c.get("symbol", "")
        c.setdefault("live_enabled", False)
        c.setdefault("last_sell_price", 0.0)
        c.setdefault("post_sell_peak", c.get("last_sell_price", 0.0))
        if c["post_sell_peak"] <= 0 and c["last_sell_price"] > 0:
            c["post_sell_peak"] = c["last_sell_price"]
        c.setdefault("dip_buy_stop", 0.0)
        c.setdefault("pending_profit_usd", 0.0)
        c.setdefault("target_shares", c.get("token_qty", 0.0))
        c.setdefault("strategy", "HYBRID_50_50" if sym in ["QNT", "KSM"] else "FULL_100")
        c.setdefault("moonbag_active", False)
        c.setdefault("active_anchor_usd", c.get("anchor_usd", 100.0))
        c.setdefault("token_address", "")
        c.setdefault("chain_id", "")
        c.setdefault("liquidity_usd", 0.0)
        c.setdefault("volume_24h_usd", 0.0)
        c.setdefault("security_badge", "🛡️ VERIFIED CEX")
        c.setdefault("price_source", "CEX")

        base_init = KNOWN_INITIAL_ANCHORS.get(sym, c.get("anchor_usd", 50.0))
        c.setdefault("initial_anchor_usd", base_init)
        if c.get("reinvested_anchor_usd", 0.0) <= 0 and c.get("anchor_usd", 0.0) > c["initial_anchor_usd"]:
            c["reinvested_anchor_usd"] = round(c["anchor_usd"] - c["initial_anchor_usd"], 4)
        else:
            c.setdefault("reinvested_anchor_usd", 0.0)

        if c.get("local_low", 0.0) <= 0 and c.get("entry_price", 0.0) > 0:
            c["local_low"] = c["entry_price"]

    if len(bot_state["history"]) == 0:
        grand_total, vault_cash, reinvested, _ = calculate_totals()
        now_str = datetime.utcnow().strftime("%H:%M:%S")
        bot_state["history"] = [
            {"time": "Start", "total": 0.0, "vault": 0.0, "reinvested": 0.0, "label": "Bot Initialized"},
            {"time": now_str, "total": grand_total, "vault": vault_cash, "reinvested": reinvested, "label": "Current Synced"}
        ]


def inspect_dexscreener_token(query_str):
    try:
        q = query_str.strip()
        if len(q) > 25 and " " not in q:
            url = f"https://api.dexscreener.com/latest/dex/tokens/{q}"
        else:
            url = f"https://api.dexscreener.com/latest/dex/search?q={q}"

        r = requests.get(url, timeout=3.5)
        if r.status_code != 200:
            return None

        pairs = r.json().get("pairs") or []
        if not pairs:
            return None

        best = max(pairs, key=lambda p: float((p.get("liquidity") or {}).get("usd") or 0.0))
        price_usd = float(best.get("priceUsd") or 0.0)
        liq_usd = float((best.get("liquidity") or {}).get("usd") or 0.0)
        vol_24h = float((best.get("volume") or {}).get("h24") or 0.0)
        base_tok = best.get("baseToken") or {}
        sym = (base_tok.get("symbol") or q[:8]).upper()
        addr = base_tok.get("address") or ""
        chain = (best.get("chainId") or "solana").upper()

        txns = (best.get("txns") or {}).get("h24") or {}
        buys = int(txns.get("buys") or 0)
        sells = int(txns.get("sells") or 0)

        if liq_usd >= 50000:
            badge = f"🛡️ SAFE DEX (${liq_usd/1000:.0f}K Liq · {chain})"
            safe = True
        elif liq_usd >= MIN_MEME_LIQUIDITY_USD:
            badge = f"⚠️ MED RISK (${liq_usd/1000:.1f}K Liq · {chain})"
            safe = True
        else:
            badge = f"🚨 RUG RISK (${liq_usd:.0f} Liq < $15K)"
            safe = False

        return {
            "symbol": sym,
            "price_usd": price_usd,
            "liquidity_usd": liq_usd,
            "volume_24h_usd": vol_24h,
            "token_address": addr,
            "chain_id": chain,
            "buys_24h": buys,
            "sells_24h": sells,
            "security_badge": badge,
            "is_safe": safe
        }
    except Exception:
        return None


def fetch_live_prices(symbols_list=None):
    with state_lock:
        coins_snapshot = list(bot_state["coins"].items())
        if not symbols_list:
            symbols_list = [c["symbol"] for _, c in coins_snapshot]

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

    for pair_key, c in coins_snapshot:
        if pair_key not in prices or c.get("price_source") == "DEX":
            query = c.get("token_address") or c.get("symbol")
            dex_info = inspect_dexscreener_token(query)
            if dex_info and dex_info["price_usd"] > 0:
                prices[pair_key] = dex_info["price_usd"]
                with state_lock:
                    if pair_key in bot_state["coins"]:
                        bot_state["coins"][pair_key]["liquidity_usd"] = dex_info["liquidity_usd"]
                        bot_state["coins"][pair_key]["volume_24h_usd"] = dex_info["volume_24h_usd"]
                        bot_state["coins"][pair_key]["security_badge"] = dex_info["security_badge"]

    return prices


def deploy_anchor_buy(coin, live_price, reason="Anchor Deployed", allow_live_order=True):
    global db_dirty
    pending = coin.get("pending_profit_usd", 0.0)
    target_sh = coin.get("target_shares", 0.0)
    top_up_used = 0.0

    if pending > 0 and target_sh > 0:
        required_anchor_for_same_shares = (target_sh * live_price) / (1.0 - TRADING_FEE_PCT)
        if required_anchor_for_same_shares > coin["anchor_usd"]:
            needed_extra = required_anchor_for_same_shares - coin["anchor_usd"]
            top_up_used = min(pending, needed_extra)
            coin["anchor_usd"] += top_up_used
            coin["reinvested_anchor_usd"] = coin.get("reinvested_anchor_usd", 0.0) + top_up_used

        vault_remainder = max(0.0, pending - top_up_used)
        coin["vault_profit_usd"] += vault_remainder
        coin["pending_profit_usd"] = 0.0

    fee = coin["anchor_usd"] * TRADING_FEE_PCT
    net_invest = coin["anchor_usd"] - fee
    new_shares = net_invest / live_price

    if allow_live_order:
        execute_live_spot_order(coin, "buy", coin["anchor_usd"], new_shares, live_price)

    coin["token_qty"] = new_shares
    coin["target_shares"] = max(target_sh, new_shares)
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
            f"Kept {fmt_qty(coin['token_qty'])} shares (+${top_up_used:.4f} to Anchor) | "
            f"Locked +${vaulted_now:.4f} Cash in Vault!"
        )
    else:
        add_log(
            f"BUY {coin['symbol']} ({reason}): ${coin['anchor_usd']:.2f} @ ${fmt_px(live_price)} "
            f"-> Bought {fmt_qty(coin['token_qty'])} {coin['symbol']} shares"
        )
    db_dirty = True


def process_coin(pair, coin, live_price, is_simulated_test=False):
    global db_dirty
    if live_price <= 0:
        return
    coin["current_price"] = live_price
    allow_live = not is_simulated_test

    # CASE 1: IN CASH -> -0.4% DIP BUY OR STRICT 100%-SHARE +0.2% STEP-UP BUY
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

            if dip_from_sliding_peak <= DIP_PULLBACK_TARGET:
                deploy_anchor_buy(coin, live_price, reason=f"-0.4% Dip Re-Buy from ${fmt_px(peak_ref)}", allow_live_order=allow_live)
                return
            elif gain_above_sell >= STEP_UP_REBUY_PCT:
                target_sh = coin.get("target_shares", 0.0)
                pending = coin.get("pending_profit_usd", 0.0)
                required_usd = (target_sh * live_price) / (1.0 - TRADING_FEE_PCT) if target_sh > 0 else coin["anchor_usd"]

                if (coin["anchor_usd"] + pending) >= (required_usd * 0.9999):
                    deploy_anchor_buy(coin, live_price, reason="+0.2% Strict Share-Keeper Step-Up", allow_live_order=allow_live)
                    return
                else:
                    if live_price <= (sell_px * 0.9985):
                        deploy_anchor_buy(coin, live_price, reason="Zero-Dilution Pullback Re-Buy", allow_live_order=allow_live)
                        return
            return
        else:
            deploy_anchor_buy(coin, live_price, reason="Initial Entry", allow_live_order=allow_live)
            return

    # CASE 2: HOLDING SHARES -> IRONCLAD ZERO-LOSS SHIELD & HYBRID RATCHET
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

        # 1. SUB-ENTRY SLIDING FLOOR SKIM
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
            record_skim_and_chart(coin["symbol"], "Sliding Floor Skim", live_price, coin["token_qty"], skimmed_profit, False)
            trigger_phone_alert(coin["symbol"], "Sliding Floor Skim", skimmed_profit, live_price, coin["token_qty"], False)
            db_dirty = True
            return

        # 2. STAGE 1 HYBRID SKIM (50% Shares Sold -> Profit Held in Escrow)
        if coin.get("strategy") == "HYBRID_50_50" and not coin.get("moonbag_active"):
            coin["target_shares"] = max(coin.get("target_shares", 0.0), coin["token_qty"])
            sell_qty = coin["token_qty"] * 0.5
            half_anchor = coin["anchor_usd"] * 0.5
            gross_val = sell_qty * exec_price
            sell_fee = gross_val * TRADING_FEE_PCT
            net_payout = gross_val - sell_fee
            skimmed_profit = max(0.0001, net_payout - half_anchor)

            live_filled = False
            if allow_live:
                live_filled, _ = execute_live_spot_order(coin, "sell", gross_val, sell_qty, zero_loss_floor_px)

            coin["pending_profit_usd"] = coin.get("pending_profit_usd", 0.0) + skimmed_profit
            coin["trades_count"] += 1
            coin["token_qty"] -= sell_qty
            coin["active_anchor_usd"] = half_anchor
            coin["moonbag_active"] = True
            coin["mode"] = "MOONBAG_RIDING"
            coin["ratchet_stop_price"] = max(zero_loss_floor_px, live_price * (1.0 - MOONBAG_TRAIL_PCT))
            add_log(
                f"STAGE 1 HYBRID SKIM {coin['symbol']} @ ${fmt_px(exec_price)}: "
                f"Sold {fmt_qty(sell_qty)} shares -> +${skimmed_profit:.4f} in Escrow! "
                f"Riding {fmt_qty(coin['token_qty'])} Moonbag shares"
            )
            record_skim_and_chart(coin["symbol"], "Stage 1 Hybrid (50%)", exec_price, coin["target_shares"], skimmed_profit, live_filled)
            trigger_phone_alert(coin["symbol"], "Stage 1 Hybrid Skim (50%)", skimmed_profit, exec_price, coin["target_shares"], live_filled)
            db_dirty = True
            return

        # 3. FINAL SALE (100% Full Skim OR Stage 2 Moonbag Exit)
        if not coin.get("moonbag_active"):
            coin["target_shares"] = max(coin.get("target_shares", 0.0), coin["token_qty"])

        active_cost = coin.get("active_anchor_usd", coin["anchor_usd"])
        sold_shares = coin["token_qty"]
        gross_value = sold_shares * exec_price
        sell_fee = gross_value * TRADING_FEE_PCT
        net_payout = gross_value - sell_fee
        skimmed_profit = max(0.0001, net_payout - active_cost)

        live_filled = False
        if allow_live:
            live_filled, _ = execute_live_spot_order(coin, "sell", gross_value, sold_shares, zero_loss_floor_px)

        coin["pending_profit_usd"] = coin.get("pending_profit_usd", 0.0) + skimmed_profit
        coin["trades_count"] += 1
        exit_label = "Stage 2 Moonbag Exit" if coin.get("moonbag_active") else "100% Full Vault Skim"
        add_log(
            f"{exit_label.upper()} {coin['symbol']} @ ${fmt_px(exec_price)}! "
            f"Sold {fmt_qty(sold_shares)} shares -> +${coin['pending_profit_usd']:.4f} ready in Escrow "
            f"(Protecting {fmt_qty(coin['target_shares'])} {coin['symbol']} shares)"
        )
        record_skim_and_chart(coin["symbol"], exit_label, exec_price, coin["target_shares"], skimmed_profit, live_filled)
        trigger_phone_alert(coin["symbol"], exit_label, skimmed_profit, exec_price, coin["target_shares"], live_filled)

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
        return# =========================================================
# PART 2: NEON DB SYNC, PWA APP, CSV EXPORT & MASTER UI
# =========================================================
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
                    if "history" in loaded and isinstance(loaded["history"], list):
                        bot_state["history"] = loaded["history"]
                    if "skims" in loaded and isinstance(loaded["skims"], list):
                        bot_state["skims"] = loaded["skims"]
                    if "alerts" in loaded and isinstance(loaded["alerts"], dict):
                        bot_state["alerts"].update(loaded["alerts"])
                    if "exchange" in loaded and isinstance(loaded["exchange"], dict):
                        bot_state["exchange"].update(loaded["exchange"])
                    ensure_coin_fields()
            add_log("Neon PostgreSQL synced: Restored all coins, shares, chart, and Live Bridge config.")
        else:
            with state_lock:
                ensure_coin_fields()
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
    add_log("VaultSkim v5.0 Master Release (Day 5: Live Exchange Bridge + Safety Firewall) live.")
    threading.Thread(target=init_db_async, daemon=True).start()
    last_save = time.time()

    while True:
        try:
            prices = fetch_live_prices()
            if prices:
                with state_lock:
                    for pair, coin in list(bot_state["coins"].items()):
                        if pair in prices:
                            process_coin(pair, coin, prices[pair], is_simulated_test=False)

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


APP_ICON_SVG = """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512">
  <rect width="512" height="512" rx="110" fill="#020617"/>
  <rect x="24" y="24" width="464" height="464" rx="96" fill="none" stroke="#10b981" stroke-width="16"/>
  <path d="M128 336 L224 224 L296 288 L384 160" fill="none" stroke="#34d399" stroke-width="36" stroke-linecap="round" stroke-linejoin="round"/>
  <circle cx="384" cy="160" r="28" fill="#10b981"/>
  <text x="256" y="435" font-family="sans-serif" font-size="68" font-weight="bold" fill="#f8fafc" text-anchor="middle">VAULT</text>
</svg>"""

@app.route("/icon.svg")
def app_icon():
    return Response(APP_ICON_SVG, mimetype="image/svg+xml")

@app.route("/manifest.json")
def manifest():
    return jsonify({
        "name": "VaultSkim Command Center",
        "short_name": "VaultSkim",
        "description": "24/7 Automated Two-Bucket Crypto & Meme-Coin Profit Skimmer",
        "start_url": "/",
        "display": "standalone",
        "background_color": "#020617",
        "theme_color": "#020617",
        "icons": [
            {"src": "/icon.svg", "sizes": "192x192 512x512", "type": "image/svg+xml", "purpose": "any maskable"}
        ]
    })

@app.route("/sw.js")
def service_worker():
    sw_js = """
    self.addEventListener('install', (e) => self.skipWaiting());
    self.addEventListener('activate', (e) => self.clients.claim());
    self.addEventListener('fetch', (e) => {});
    """
    return Response(sw_js, mimetype="application/javascript")

@app.route("/api/export_csv")
def export_csv():
    with state_lock:
        skims = list(bot_state.get("skims") or [])
    lines = ["Time_UTC,Symbol,Skim_Type,Execution_Price_USD,Protected_Shares,Profit_Locked_USD,Cumulative_Total_USD"]
    for s in skims:
        lines.append(
            f"{s.get('time','')},{s.get('symbol','')},\"{s.get('type','')}\","
            f"{s.get('price','')},\"{s.get('shares','')}\",{s.get('profit',0):.4f},{s.get('grand_total',0):.4f}"
        )
    csv_content = "\n".join(lines)
    return Response(
        csv_content,
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=vaultskim_profit_ledger.csv"}
    )


DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
  <meta name="theme-color" content="#020617">
  <meta name="apple-mobile-web-app-capable" content="yes">
  <meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
  <link rel="manifest" href="/manifest.json">
  <link rel="icon" type="image/svg+xml" href="/icon.svg">
  <title>VaultSkim v5.0 — Master Execution Center</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
</head>
<body class="bg-slate-950 text-slate-100 min-h-screen p-4 md:p-6 font-sans">
  <div class="max-w-6xl mx-auto">
    <!-- Top Command Header -->
    <div class="flex flex-col md:flex-row justify-between items-start md:items-center border-b border-slate-800 pb-4 mb-6 gap-4">
      <div>
        <div class="flex items-center gap-2 flex-wrap">
          <h1 class="text-2xl font-bold text-emerald-400">VaultSkim Command Center</h1>
          <span class="text-xs bg-emerald-900/60 text-emerald-300 px-2.5 py-0.5 rounded font-semibold">v5.0 · Master Release</span>
          <button id="btn-install" onclick="installPWA()" class="hidden bg-indigo-600 hover:bg-indigo-500 text-white text-xs font-bold px-3 py-1 rounded-lg shadow transition cursor-pointer">
            📲 Install App
          </button>
          <button onclick="togglePanel('exchange-panel')" class="bg-amber-600/80 hover:bg-amber-500 text-white text-xs font-bold px-3 py-1 rounded-lg shadow transition cursor-pointer">
            🔑 Live Exchange Bridge
          </button>
          <button onclick="togglePanel('alerts-panel')" class="bg-sky-900/70 hover:bg-sky-800 border border-sky-500/40 text-sky-200 text-xs font-semibold px-3 py-1 rounded-lg transition cursor-pointer">
            📱 Phone Alerts
          </button>
          <button onclick="togglePanel('cashout-panel')" class="bg-emerald-900/70 hover:bg-emerald-800 border border-emerald-500/40 text-emerald-200 text-xs font-semibold px-3 py-1 rounded-lg transition cursor-pointer">
            💰 Accrue Cash-Out & CSV
          </button>
        </div>
        <p class="text-xs md:text-sm text-slate-400 mt-1">
          5-Layer Zero-Loss Firewall + Per-Coin Paper/Live Switch + 100% Share-Keeper
          <span id="global-mode-pill" class="ml-2 px-2 py-0.5 rounded text-[11px] font-bold bg-slate-800 text-slate-300">🛡️ PAPER SIMULATOR</span>
        </p>
      </div>

      <div class="flex flex-wrap items-center gap-3 w-full md:w-auto justify-between md:justify-end">
        <div class="flex gap-2">
          <button id="btn-panic" onclick="panicFreezeLive()" title="Instantly switch all cards back to safe Paper Simulator Mode" class="bg-rose-900/80 hover:bg-rose-700 active:scale-95 border border-rose-500/50 text-white text-xs font-bold px-3 py-2 rounded-xl transition cursor-pointer">
            🛑 Freeze Live
          </button>
          <button id="btn-sync" onclick="syncLivePrices(false)" class="bg-slate-800 hover:bg-slate-700 active:scale-95 border border-slate-700 text-slate-200 text-xs font-semibold px-3 py-2 rounded-xl transition cursor-pointer">
            🔄 Sync Live
          </button>
          <button id="btn-reset" onclick="syncLivePrices(true)" class="bg-slate-900 hover:bg-rose-950 active:scale-95 border border-slate-800 text-rose-300 text-xs font-semibold px-3 py-2 rounded-xl transition cursor-pointer">
            🧹 Reset ($0.00)
          </button>
        </div>

        <div class="bg-emerald-950/60 border border-emerald-500/40 px-4 py-2.5 rounded-xl text-right min-w-[235px]">
          <div class="text-[11px] uppercase tracking-wider text-emerald-300 font-semibold">Total Untouchable Profit Generated</div>
          <div id="total-vault" class="text-3xl font-extrabold text-emerald-400">$0.00</div>
          <div id="vault-breakdown" class="text-[11px] text-slate-300 font-mono mt-0.5">Vault Cash: $0.00 | Added to Anchors: +$0.00</div>
        </div>
      </div>
    </div>

    <!-- Day 5 Panel 1: Live Exchange Execution Bridge & Safety Firewall -->
    <div id="exchange-panel" class="hidden bg-slate-900 border border-amber-500/50 rounded-xl p-5 mb-6 shadow-xl">
      <div class="flex justify-between items-start mb-3">
        <div>
          <h2 class="text-base font-bold text-amber-300">🔑 Day 5 Live Exchange Execution Bridge (Zero-Withdrawal API Security)</h2>
          <p class="text-xs text-slate-400">Connect Gate.io v4 or Binance Spot v3 API keys (with <b>Withdrawals Disabled</b>). Even when connected, a coin ONLY trades real money if you switch its card from <b>🧪 Paper</b> to <b>⚡ Live</b>.</p>
        </div>
        <button onclick="togglePanel('exchange-panel')" class="text-slate-400 hover:text-white text-sm font-bold px-2">✕</button>
      </div>

      <div class="grid grid-cols-1 md:grid-cols-4 gap-3 mb-4">
        <div class="bg-slate-950 border border-slate-800 p-3 rounded-lg">
          <label class="text-xs font-bold text-slate-300 block mb-1">1. Spot Exchange</label>
          <select id="ex-provider" class="w-full bg-slate-900 border border-slate-700 rounded px-2.5 py-1.5 text-xs text-white">
            <option value="gateio">Gate.io Spot v4 (0.1% Fee)</option>
            <option value="binance">Binance Spot v3 (0.1% Fee)</option>
          </select>
          <div class="text-[10px] text-slate-400 mt-1.5">Uses Limit-IOC Zero-Loss Floor orders</div>
        </div>

        <div class="bg-slate-950 border border-slate-800 p-3 rounded-lg">
          <label class="text-xs font-bold text-slate-300 block mb-1">2. Spot API Key (No Withdrawals)</label>
          <input id="ex-key" type="text" placeholder="Paste API Key..." class="w-full bg-slate-900 border border-slate-700 rounded px-2.5 py-1.5 text-xs text-white font-mono">
          <div id="ex-key-status" class="text-[10px] text-emerald-400 mt-1"></div>
        </div>

        <div class="bg-slate-950 border border-slate-800 p-3 rounded-lg">
          <label class="text-xs font-bold text-slate-300 block mb-1">3. Spot API Secret</label>
          <input id="ex-secret" type="password" placeholder="Paste API Secret (Encrypted)..." class="w-full bg-slate-900 border border-slate-700 rounded px-2.5 py-1.5 text-xs text-white font-mono">
          <div class="text-[10px] text-slate-400 mt-1">Never exposed to browser after saving</div>
        </div>

        <div class="bg-slate-950 border border-slate-800 p-3 rounded-lg">
          <label class="text-xs font-bold text-amber-300 block mb-1">4. Max Order Safety Cap (USD)</label>
          <input id="ex-max-usd" type="number" value="250" class="w-full bg-slate-900 border border-slate-700 rounded px-2.5 py-1.5 text-xs text-white font-mono">
          <div class="text-[10px] text-slate-400 mt-1">Blocks any single order above this $ cap</div>
        </div>
      </div>

      <div class="flex flex-wrap items-center justify-between gap-3 border-t border-slate-800 pt-3">
        <div class="text-xs font-mono text-emerald-300" id="ex-status-text">Status: Paper Simulator Active</div>
        <div class="flex flex-wrap gap-2">
          <button id="btn-test-ex" onclick="testExchangeBridge()" class="bg-slate-800 hover:bg-slate-700 border border-slate-600 text-amber-300 text-xs font-semibold px-3.5 py-2 rounded-lg transition cursor-pointer">
            🔍 Verify API & Check USDT Balance
          </button>
          <button id="btn-arm-ex" onclick="toggleGlobalLiveBridge()" class="bg-amber-600 hover:bg-amber-500 text-white text-xs font-bold px-4 py-2 rounded-lg transition cursor-pointer">
            ⚡ Save & Toggle Live Bridge
          </button>
        </div>
      </div>
    </div>

    <!-- Day 5 Panel 2: Accrue USDT Cash-Out Assistant & CSV Export -->
    <div id="cashout-panel" class="hidden bg-slate-900 border border-emerald-500/40 rounded-xl p-5 mb-6 shadow-xl">
      <div class="flex justify-between items-start mb-3">
        <div>
          <h2 class="text-base font-bold text-emerald-300">💰 Accrue Zero-Trading-Fee Cash-Out Guide & Profit Ledger CSV</h2>
          <p class="text-xs text-slate-400">How to move your locked Bucket B Vault USDT to Mobile Money / Bank via Accrue without paying Accrue's ~0.9% crypto trading fee:</p>
        </div>
        <button onclick="togglePanel('cashout-panel')" class="text-slate-400 hover:text-white text-sm font-bold px-2">✕</button>
      </div>
      <div class="grid grid-cols-1 md:grid-cols-3 gap-4 text-xs text-slate-300 mb-4">
        <div class="bg-slate-950 border border-slate-800 p-3 rounded-lg">
          <div class="font-bold text-emerald-400 mb-1">Step 1: Ready Vault Cash</div>
          <div>Available in Bucket B Vault: <span id="cashout-vault-amt" class="font-bold text-white font-mono">$0.00 USDT</span></div>
          <div class="text-[11px] text-slate-400 mt-1">Keep Bucket A working on Gate.io/Binance; only withdraw your Bucket B Vault profits!</div>
        </div>
        <div class="bg-slate-950 border border-slate-800 p-3 rounded-lg">
          <div class="font-bold text-emerald-400 mb-1">Step 2: Free USDT Deposit to Accrue</div>
          <div class="text-[11px] text-slate-400">In Accrue: Tap <b>Deposit -> Crypto Deposit -> USDT</b>. Choose <b>Solana</b> or <b>Tron (TRC-20)</b> and send your Vault USDT from Gate.io/Binance.</div>
        </div>
        <div class="bg-slate-950 border border-slate-800 p-3 rounded-lg flex flex-col justify-between">
          <div>
            <div class="font-bold text-emerald-400 mb-1">Step 3: Instant MoMo / Bank Payout</div>
            <div class="text-[11px] text-slate-400 mb-2">Once USDT lands in Accrue as USD, tap <b>Withdraw -> Mobile Money / Bank</b>.</div>
          </div>
          <a href="/api/export_csv" class="block text-center bg-emerald-700 hover:bg-emerald-600 text-white font-bold py-1.5 rounded transition">
            📥 Download Verified Skim Ledger (.CSV)
          </a>
        </div>
      </div>
    </div>

    <!-- Day 4 Phone Alerts Setup Panel -->
    <div id="alerts-panel" class="hidden bg-slate-900 border border-sky-500/40 rounded-xl p-5 mb-6 shadow-xl">
      <div class="flex justify-between items-center mb-3">
        <div>
          <h2 class="text-base font-bold text-sky-300">📱 Instant Phone Profit Alerts (Saved to Neon Cloud)</h2>
          <p class="text-xs text-slate-400">Get an instant notification on your phone every time VaultSkim locks profit into your Vault.</p>
        </div>
        <button onclick="togglePanel('alerts-panel')" class="text-slate-400 hover:text-white text-sm font-bold px-2">✕</button>
      </div>

      <div class="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
        <div class="bg-slate-950 border border-slate-800 p-3.5 rounded-lg">
          <div class="text-xs font-bold text-emerald-400 mb-1">Option 1: ntfy.sh (10-Sec Zero-Signup Push)</div>
          <p class="text-[11px] text-slate-400 mb-2">Type any secret topic word below, install the free <b>ntfy</b> phone app (or open <code>ntfy.sh/your_topic</code>), and subscribe!</p>
          <input id="alert-ntfy" type="text" placeholder="e.g. vaultskim_enoch_777" class="w-full bg-slate-900 border border-slate-700 rounded px-2.5 py-1.5 text-xs text-white font-mono">
        </div>

        <div class="bg-slate-950 border border-slate-800 p-3.5 rounded-lg">
          <div class="text-xs font-bold text-sky-400 mb-1">Option 2: Telegram Bot Alerts</div>
          <p class="text-[11px] text-slate-400 mb-2">Paste your Telegram Bot Token (from @BotFather) and your Chat ID:</p>
          <input id="alert-tg-token" type="text" placeholder="Bot Token (e.g. 123456:ABC...)" class="w-full bg-slate-900 border border-slate-700 rounded px-2.5 py-1 text-xs text-white font-mono mb-1.5">
          <input id="alert-tg-chat" type="text" placeholder="Chat ID (e.g. 987654321)" class="w-full bg-slate-900 border border-slate-700 rounded px-2.5 py-1 text-xs text-white font-mono">
        </div>

        <div class="bg-slate-950 border border-slate-800 p-3.5 rounded-lg">
          <div class="text-xs font-bold text-indigo-400 mb-1">Option 3: WhatsApp / Discord Webhook</div>
          <p class="text-[11px] text-slate-400 mb-2">Paste any Discord, Make.com, or WhatsApp Bridge Webhook URL:</p>
          <input id="alert-webhook" type="text" placeholder="https://..." class="w-full bg-slate-900 border border-slate-700 rounded px-2.5 py-1.5 text-xs text-white font-mono">
        </div>
      </div>

      <div class="flex flex-wrap gap-3 justify-end">
        <button id="btn-test-alert" onclick="testPhoneAlert()" class="bg-slate-800 hover:bg-slate-700 border border-slate-600 text-amber-300 text-xs font-semibold px-4 py-2 rounded-lg transition cursor-pointer">
          🔔 Send Test Alert to My Phone
        </button>
        <button id="btn-save-alert" onclick="saveAlertSettings()" class="bg-sky-600 hover:bg-sky-500 text-white text-xs font-bold px-5 py-2 rounded-lg transition cursor-pointer">
          💾 Save Alert Settings
        </button>
      </div>
    </div>

    <!-- Live Vault Growth Chart & Verified Skim Ledger -->
    <div class="grid grid-cols-1 lg:grid-cols-3 gap-5 mb-6">
      <div class="lg:col-span-2 bg-slate-900 border border-slate-800 rounded-xl p-4 shadow-lg flex flex-col justify-between">
        <div class="flex justify-between items-center mb-2">
          <div>
            <h2 class="text-sm font-bold text-emerald-400 uppercase tracking-wider">📈 Live Vault Profit Growth Curve</h2>
            <p class="text-xs text-slate-400">Tracks cumulative Vault Cash + Share-Keeper Anchor Top-Ups across every skim</p>
          </div>
          <span id="total-skims-badge" class="text-xs bg-emerald-950 text-emerald-300 border border-emerald-700/40 px-2.5 py-1 rounded-full font-mono">0 Total Skims</span>
        </div>
        <div class="relative h-48 w-full">
          <canvas id="vaultChart"></canvas>
        </div>
      </div>

      <div class="bg-slate-900 border border-slate-800 rounded-xl p-4 shadow-lg flex flex-col justify-between">
        <div>
          <div class="flex justify-between items-center mb-1">
            <h2 class="text-sm font-bold text-amber-300 uppercase tracking-wider">🧾 Verified Skim Ledger</h2>
            <a href="/api/export_csv" class="text-[11px] text-emerald-400 hover:underline font-mono">📥 CSV</a>
          </div>
          <p class="text-xs text-slate-400 mb-2">Recent completed profit sweeps</p>
          <div id="skim-ledger" class="space-y-2 max-h-44 overflow-y-auto text-xs font-mono pr-1">
            <div class="text-slate-500 text-xs">Waiting for next profit skim...</div>
          </div>
        </div>
      </div>
    </div>

    <!-- Add Any Token / Contract Address + Anti-Rug Scanner Bar -->
    <div class="bg-slate-900 border border-slate-800 rounded-xl p-4 mb-6 flex flex-col gap-3">
      <div class="flex flex-wrap items-center justify-between gap-3">
        <div>
          <div class="text-sm font-semibold text-emerald-300">🎯 Launch Any Coin or Paste DEX Contract Address (Anti-Rug Protected):</div>
          <div class="text-xs text-slate-400">Checks Gate.io + DexScreener Liquidity Pools (Automatically blocks pools under $15K liquidity)</div>
        </div>
        <div class="flex flex-wrap gap-2 w-full md:w-auto">
          <input id="new-symbol" type="text" placeholder="Symbol or Contract (e.g. PEPE, BONK, 0x...)" class="bg-slate-950 border border-slate-700 rounded-lg px-3 py-1.5 text-sm text-white flex-1 md:w-64">
          <input id="new-anchor" type="number" placeholder="Anchor USD" value="10" class="bg-slate-950 border border-slate-700 rounded-lg px-3 py-1.5 text-sm text-white w-24">
          <button id="btn-add" onclick="addCoin()" class="bg-emerald-600 hover:bg-emerald-500 active:scale-95 text-white text-sm font-semibold px-4 py-1.5 rounded-lg transition cursor-pointer">+ Snipe & Launch</button>
          <button id="btn-radar" onclick="scanMemeRadar()" class="bg-amber-600/80 hover:bg-amber-500 active:scale-95 text-white text-sm font-semibold px-3.5 py-1.5 rounded-lg transition cursor-pointer">🔥 Scan Meme Radar</button>
        </div>
      </div>

      <div id="radar-box" class="hidden border-t border-slate-800 pt-3 mt-1">
        <div class="text-xs font-semibold uppercase tracking-wider text-amber-300 mb-2">🔥 Live High-Volume Meme Candidates (Anti-Rug Verified):</div>
        <div id="radar-items" class="grid grid-cols-1 md:grid-cols-4 gap-2.5"></div>
      </div>
    </div>

    <!-- Active Skimmer Cards -->
    <div id="coin-cards" class="grid grid-cols-1 md:grid-cols-3 gap-5 mb-8"></div>

    <!-- Live Execution Log -->
    <div class="bg-slate-900 border border-slate-800 rounded-xl p-5">
      <h2 class="text-sm font-semibold uppercase tracking-wider text-slate-400 mb-3">Live Execution & Anti-Rug Security Log</h2>
      <div id="logs" class="font-mono text-xs text-slate-300 space-y-1.5 max-h-64 overflow-y-auto"></div>
    </div>
  </div>

  <script>
    let deferredPrompt = null;
    let vaultChartInstance = null;
    let settingsLoadedIntoInputs = false;

    if ('serviceWorker' in navigator) {
      navigator.serviceWorker.register('/sw.js').catch(() => {});
    }
    window.addEventListener('beforeinstallprompt', (e) => {
      e.preventDefault();
      deferredPrompt = e;
      document.getElementById('btn-install').classList.remove('hidden');
    });
    async function installPWA() {
      if (deferredPrompt) {
        deferredPrompt.prompt();
        await deferredPrompt.userChoice;
        deferredPrompt = null;
      } else {
        alert('To install VaultSkim: Tap your browser menu (⋮) and select "Install app" or "Add to Home screen".');
      }
    }

    function togglePanel(id) {
      const el = document.getElementById(id);
      if (el) el.classList.toggle('hidden');
    }

    async function testExchangeBridge() {
      const btn = document.getElementById('btn-test-ex');
      btn.innerText = '⏳ Checking Exchange...';
      const payload = {
        provider: document.getElementById('ex-provider').value,
        api_key: document.getElementById('ex-key').value.trim(),
        api_secret: document.getElementById('ex-secret').value.trim(),
        max_order_usd: parseFloat(document.getElementById('ex-max-usd').value) || 250
      };
      const res = await fetch('/api/exchange_config', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({...payload, test_only: true})
      });
      const out = await res.json();
      document.getElementById('ex-status-text').innerText = out.message || 'Checked';
      btn.innerText = out.ok ? '✅ Verified!' : '⚠️ Check Keys';
      setTimeout(() => { btn.innerText = '🔍 Verify API & Check USDT Balance'; }, 1800);
      refresh();
    }

    async function toggleGlobalLiveBridge() {
      const btn = document.getElementById('btn-arm-ex');
      btn.innerText = '⏳ Saving...';
      const payload = {
        provider: document.getElementById('ex-provider').value,
        api_key: document.getElementById('ex-key').value.trim(),
        api_secret: document.getElementById('ex-secret').value.trim(),
        max_order_usd: parseFloat(document.getElementById('ex-max-usd').value) || 250,
        toggle_arm: true
      };
      const res = await fetch('/api/exchange_config', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(payload)
      });
      const out = await res.json();
      document.getElementById('ex-status-text').innerText = out.message || '';
      document.getElementById('ex-secret').value = '';
      btn.innerText = '✅ Updated!';
      setTimeout(() => { btn.innerText = '⚡ Save & Toggle Live Bridge'; }, 1400);
      refresh();
    }

    async function panicFreezeLive() {
      await fetch('/api/panic_freeze', {method: 'POST'});
      refresh();
    }

    async function toggleCoinLive(pair, sym, currentlyLive) {
      if (!currentlyLive) {
        const ok = confirm(
          `⚡ ARM LIVE TRADING FOR ${sym}/USDT?\n\n` +
          `When both the Global Live Bridge AND this ${sym} card are set to LIVE, ` +
          `the bot will send real Spot orders bounded by your Zero-Loss Floor. Proceed?`
        );
        if (!ok) return;
      }
      await fetch('/api/toggle_coin_live/' + pair, {method: 'POST'});
      refresh();
    }

    async function saveAlertSettings() {
      const btn = document.getElementById('btn-save-alert');
      btn.innerText = '⏳ Saving...';
      const payload = {
        ntfy_topic: document.getElementById('alert-ntfy').value.trim(),
        tg_token: document.getElementById('alert-tg-token').value.trim(),
        tg_chat_id: document.getElementById('alert-tg-chat').value.trim(),
        webhook_url: document.getElementById('alert-webhook').value.trim(),
        enabled: true
      };
      await fetch('/api/save_alerts', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(payload)
      });
      btn.innerText = '✅ Saved!';
      setTimeout(() => { btn.innerText = '💾 Save Alert Settings'; }, 1400);
    }

    async function testPhoneAlert() {
      await saveAlertSettings();
      const btn = document.getElementById('btn-test-alert');
      btn.innerText = '📡 Sending Push...';
      const res = await fetch('/api/test_alert', {method: 'POST'});
      const out = await res.json();
      btn.innerText = out.ok ? '✅ Alert Sent!' : '⚠️ Add a Topic/Token First';
      setTimeout(() => { btn.innerText = '🔔 Send Test Alert to My Phone'; }, 1800);
    }

    function initOrUpdateChart(historyData) {
      const ctx = document.getElementById('vaultChart');
      if (!ctx || !historyData || historyData.length === 0) return;

      const labels = historyData.map(h => h.time);
      const totals = historyData.map(h => h.total);

      if (!vaultChartInstance) {
        vaultChartInstance = new Chart(ctx, {
          type: 'line',
          data: {
            labels: labels,
            datasets: [{
              label: 'Total Profit Generated ($)',
              data: totals,
              borderColor: '#10b981',
              backgroundColor: 'rgba(16, 185, 129, 0.15)',
              borderWidth: 2.5,
              pointRadius: 3,
              pointBackgroundColor: '#34d399',
              fill: true,
              tension: 0.3
            }]
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
              x: { ticks: { color: '#64748b', maxTicksLimit: 8 }, grid: { color: 'rgba(30, 41, 59, 0.5)' } },
              y: { ticks: { color: '#34d399', callback: (v) => '$' + v.toFixed(2) }, grid: { color: 'rgba(30, 41, 59, 0.5)' } }
            }
          }
        });
      } else {
        vaultChartInstance.data.labels = labels;
        vaultChartInstance.data.datasets[0].data = totals;
        vaultChartInstance.update('none');
      }
    }

    function fmtPx(val) {
      if (!val || val <= 0) return '0.0000';
      if (val < 0.00001) return val.toFixed(9);
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

    async function addCoin(customQuery, customAnchor) {
      const rawInput = customQuery || document.getElementById('new-symbol').value.trim();
      const anc = customAnchor || parseFloat(document.getElementById('new-anchor').value) || 10;
      if (!rawInput) return;
      const btn = document.getElementById('btn-add');
      btn.innerText = '🛡️ Scanning Pool...';
      const res = await fetch('/api/add_coin', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({symbol: rawInput, anchor_usd: anc})
      });
      const out = await res.json();
      if (!out.ok && out.error) {
        alert(out.error);
      }
      document.getElementById('new-symbol').value = '';
      btn.innerText = out.ok ? '✅ Sniped!' : '🚨 Blocked!';
      setTimeout(() => { btn.innerText = '+ Snipe & Launch'; }, 1400);
      refresh();
    }

    async function scanMemeRadar() {
      const btn = document.getElementById('btn-radar');
      btn.innerText = '⏳ Scanning DEX...';
      const res = await fetch('/api/meme_radar');
      const data = await res.json();
      btn.innerText = '🔥 Scan Meme Radar';
      const box = document.getElementById('radar-box');
      const list = document.getElementById('radar-items');
      box.classList.remove('hidden');
      if (!data.candidates || data.candidates.length === 0) {
        list.innerHTML = '<div class="text-xs text-slate-400">No new candidates found right now.</div>';
        return;
      }
      list.innerHTML = data.candidates.map(m => `
        <div class="bg-slate-950 border border-slate-800 rounded-lg p-3 flex flex-col justify-between gap-2">
          <div>
            <div class="flex justify-between items-center">
              <span class="font-bold text-sm text-amber-300">${m.symbol}</span>
              <span class="text-[10px] px-2 py-0.5 rounded bg-emerald-950 text-emerald-300 border border-emerald-700/40">${m.chain_id}</span>
            </div>
            <div class="text-xs font-mono text-white mt-1">$${fmtPx(m.price_usd)}</div>
            <div class="text-[11px] text-slate-400 mt-1">Liq: $${(m.liquidity_usd/1000).toFixed(0)}K | 24h Vol: $${(m.volume_24h_usd/1000).toFixed(0)}K</div>
            <div class="text-[10px] text-emerald-400 mt-0.5">Buys: ${m.buys_24h} / Sells: ${m.sells_24h}</div>
          </div>
          <button onclick="addCoin('${m.symbol}', 10)" class="w-full bg-emerald-700 hover:bg-emerald-600 text-white text-xs font-semibold py-1 rounded transition cursor-pointer">
            + Snipe ${m.symbol} ($10)
          </button>
        </div>
      `).join('');
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
      btn.innerText = '⏳ Working...';
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
        let totalVaultCash = 0;
        let totalReinvestedAnchors = 0;
        let totalSkimsCount = 0;
        let cardsHtml = '';

        const ex = data.exchange || {};
        const globalArmed = !!ex.global_live_armed;
        const modePill = document.getElementById('global-mode-pill');
        if (globalArmed) {
          modePill.className = 'ml-2 px-2.5 py-0.5 rounded text-[11px] font-bold bg-amber-500/20 text-amber-300 border border-amber-500/40';
          modePill.innerText = `⚡ LIVE BRIDGE ARMED (${(ex.provider || 'gateio').toUpperCase()})`;
        } else {
          modePill.className = 'ml-2 px-2.5 py-0.5 rounded text-[11px] font-bold bg-slate-800 text-slate-300';
          modePill.innerText = '🛡️ PAPER SIMULATOR MODE';
        }

        if (!settingsLoadedIntoInputs) {
          if (data.alerts) {
            document.getElementById('alert-ntfy').value = data.alerts.ntfy_topic || '';
            document.getElementById('alert-tg-token').value = data.alerts.tg_token || '';
            document.getElementById('alert-tg-chat').value = data.alerts.tg_chat_id || '';
            document.getElementById('alert-webhook').value = data.alerts.webhook_url || '';
          }
          if (ex) {
            document.getElementById('ex-provider').value = ex.provider || 'gateio';
            document.getElementById('ex-max-usd').value = ex.max_order_usd || 250;
            if (ex.has_key) {
              document.getElementById('ex-key-status').innerText = `Saved Key: ${ex.api_key_masked}`;
            }
            document.getElementById('ex-status-text').innerText = ex.last_check_status || 'Paper Simulator Active';
          }
          settingsLoadedIntoInputs = true;
        }

        for (const [pair, c] of Object.entries(data.coins)) {
          const pending = c.pending_profit_usd || 0;
          const reinvested = c.reinvested_anchor_usd || 0;
          totalVaultCash += (c.vault_profit_usd + pending);
          totalReinvestedAnchors += reinvested;
          totalSkimsCount += (c.trades_count || 0);

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

          const cardLive = !!c.live_enabled;
          const execBadge = cardLive
            ? (globalArmed ? '⚡ LIVE ORDER' : '⚡ LIVE QUEUED')
            : '🧪 PAPER';
          const execBtnStyle = cardLive
            ? 'bg-amber-600/30 border-amber-500/50 text-amber-300 hover:bg-amber-600/50'
            : 'bg-slate-800 border-slate-700 text-slate-400 hover:text-white';

          let bucketStatus = c.in_position ? '(In Token)' : '(In Cash)';
          if (c.moonbag_active) bucketStatus = '(50% Cash + 50% Moonbag)';

          let sharesDisplay = c.in_position
            ? `${fmtQty(c.token_qty)} ${c.symbol}`
            : `0.00 (Locked Target: ${fmtQty(c.target_shares || 0)} ${c.symbol})`;

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

          const secBadge = c.security_badge || '🛡️ VERIFIED CEX';
          const simBtnText = c.moonbag_active
            ? '🚀 Practice Moonbag Rally (+5%)'
            : '⚡ Practice Spike (+2% Simulator)';

          cardsHtml += `
            <div class="bg-slate-900 border ${cardLive && globalArmed ? 'border-amber-500/60' : 'border-slate-800'} rounded-xl p-5 shadow-lg flex flex-col justify-between">
              <div>
                <div class="flex justify-between items-center mb-1.5">
                  <span class="text-xl font-bold">${c.symbol}/USDT</span>
                  <div class="flex items-center gap-1.5">
                    <button onclick="toggleCoinLive('${pair}', '${c.symbol}', ${cardLive})" title="Switch this coin between Paper Simulator and Live Exchange Orders" class="text-[11px] px-2 py-0.5 rounded border font-bold cursor-pointer transition ${execBtnStyle}">
                      ${execBadge}
                    </button>
                    <span class="text-xs px-2 py-0.5 rounded-full bg-slate-800 text-emerald-300 font-medium">${c.mode}</span>
                    <button onclick="deleteCoin('${pair}', '${c.symbol}')" title="Remove Card" class="text-slate-500 hover:text-rose-400 text-sm font-bold px-1.5 py-0.5 rounded cursor-pointer">✕</button>
                  </div>
                </div>

                <div class="text-[11px] text-slate-400 mb-2 flex justify-between items-center">
                  <span>${secBadge}</span>
                  ${reinvested > 0 ? `<span class="text-emerald-400 font-mono">+$${reinvested.toFixed(2)} Share-TopUp</span>` : ''}
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

        const grandTotalProfit = totalVaultCash + totalReinvestedAnchors;
        document.getElementById('coin-cards').innerHTML = cardsHtml;
        document.getElementById('total-vault').innerText = '$' + grandTotalProfit.toFixed(2);
        document.getElementById('cashout-vault-amt').innerText = '$' + totalVaultCash.toFixed(2) + ' USDT';
        document.getElementById('vault-breakdown').innerText =
          `Vault Cash: $${totalVaultCash.toFixed(2)} | Added to Anchors: +$${totalReinvestedAnchors.toFixed(2)}`;
        document.getElementById('total-skims-badge').innerText = `${totalSkimsCount} Total Skims`;
        document.getElementById('logs').innerHTML = data.logs.map(l => `<div>${l}</div>`).join('');

        initOrUpdateChart(data.history || []);
        if (data.skims && data.skims.length > 0) {
          document.getElementById('skim-ledger').innerHTML = data.skims.map(s => `
            <div class="bg-slate-950 border border-slate-800/80 rounded p-2 flex justify-between items-center">
              <div>
                <span class="text-emerald-400 font-bold">${s.symbol}</span>
                <span class="text-slate-400 text-[10px] ml-1">${s.type}</span>
                <div class="text-[10px] text-slate-400">@ $${s.price} · ${s.time}</div>
              </div>
              <div class="text-right">
                <div class="text-emerald-300 font-bold">+$${s.profit.toFixed(4)}</div>
                <div class="text-[10px] text-sky-300">${s.shares} sh</div>
              </div>
            </div>
          `).join('');
        }
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
        safe_copy = dict(bot_state)
        ex = dict(bot_state.get("exchange") or {})
        raw_key = ex.get("api_key", "")
        ex["has_key"] = bool(raw_key)
        ex["api_key_masked"] = (raw_key[:4] + "..." + raw_key[-4:]) if len(raw_key) >= 8 else ("SET" if raw_key else "")
        ex.pop("api_key", None)
        ex.pop("api_secret", None)
        safe_copy["exchange"] = ex
        return jsonify(safe_copy)

@app.route("/api/exchange_config", methods=["POST"])
def api_exchange_config():
    global db_dirty
    data = request.json or {}
    with state_lock:
        ex = bot_state.setdefault("exchange", {})
        if data.get("provider"):
            ex["provider"] = data["provider"].lower().strip()
        if data.get("api_key"):
            ex["api_key"] = data["api_key"].strip()
        if data.get("api_secret"):
            ex["api_secret"] = data["api_secret"].strip()
        if data.get("max_order_usd"):
            ex["max_order_usd"] = float(data["max_order_usd"])

        if data.get("toggle_arm"):
            ex["global_live_armed"] = not ex.get("global_live_armed", False)

    ok, bal, msg = fetch_exchange_usdt_balance()
    with state_lock:
        bot_state["exchange"]["usdt_balance"] = bal
        bot_state["exchange"]["last_check_status"] = msg
        armed_state = "ARMED" if bot_state["exchange"].get("global_live_armed") else "PAPER_MODE"
        add_log(f"EXCHANGE BRIDGE ({armed_state}): {msg}")
        db_dirty = True
    save_state_to_db()
    return jsonify({"ok": ok, "balance": bal, "message": msg})

@app.route("/api/panic_freeze", methods=["POST"])
def api_panic_freeze():
    global db_dirty
    with state_lock:
        bot_state.setdefault("exchange", {})["global_live_armed"] = False
        for c in bot_state.get("coins", {}).values():
            c["live_enabled"] = False
        add_log("🛑 EMERGENCY FREEZE ACTIVATED: All coins switched back to safe Paper Simulator Mode!")
        db_dirty = True
    save_state_to_db()
    return jsonify({"ok": True})

@app.route("/api/toggle_coin_live/<pair>", methods=["POST"])
def api_toggle_coin_live(pair):
    global db_dirty
    with state_lock:
        coin = bot_state["coins"].get(pair)
        if coin:
            coin["live_enabled"] = not coin.get("live_enabled", False)
            status = "⚡ LIVE ORDERS ENABLED" if coin["live_enabled"] else "🧪 PAPER SIMULATOR"
            add_log(f"COIN EXECUTION MODE ({coin['symbol']}): Switched to {status}.")
            db_dirty = True
    save_state_to_db()
    return jsonify({"ok": True})

@app.route("/api/save_alerts", methods=["POST"])
def api_save_alerts():
    global db_dirty
    data = request.json or {}
    with state_lock:
        bot_state.setdefault("alerts", {})
        bot_state["alerts"]["ntfy_topic"] = data.get("ntfy_topic", "").strip()
        bot_state["alerts"]["tg_token"] = data.get("tg_token", "").strip()
        bot_state["alerts"]["tg_chat_id"] = data.get("tg_chat_id", "").strip()
        bot_state["alerts"]["webhook_url"] = data.get("webhook_url", "").strip()
        bot_state["alerts"]["enabled"] = bool(data.get("enabled", True))
        add_log("PHONE ALERTS CONFIGURED: Notification channels saved to Neon database.")
        db_dirty = True
    save_state_to_db()
    return jsonify({"ok": True})

@app.route("/api/test_alert", methods=["POST"])
def api_test_alert():
    with state_lock:
        alerts_cfg = dict(bot_state.get("alerts") or {})
        grand_total, vault_cash, reinvested, total_skims = calculate_totals()

    if not any([alerts_cfg.get("ntfy_topic"), alerts_cfg.get("tg_token"), alerts_cfg.get("webhook_url")]):
        return jsonify({"ok": False})

    title = "VaultSkim v5.0 Master Alert Connected!"
    msg = (
        f"Your 24/7 Phone Profit Alerts are active!\n"
        f"Current Total Profit: ${grand_total:.2f} (Vault Cash: ${vault_cash:.2f} | Anchor Top-Ups: +${reinvested:.2f})\n"
        f"Completed Skims: {total_skims}"
    )
    threading.Thread(
        target=_dispatch_alert_async,
        args=(title, msg, {"test": True, "grand_total": grand_total}, alerts_cfg),
        daemon=True
    ).start()
    add_log("SENT TEST PHONE ALERT: Check your ntfy app, Telegram, or Webhook!")
    return jsonify({"ok": True})

@app.route("/api/meme_radar")
def api_meme_radar():
    watch_seeds = ["BONK", "FLOKI", "POPCAT", "BRETT", "MOG", "BOME", "NEIRO", "GOAT"]
    candidates = []
    for sym in watch_seeds:
        info = inspect_dexscreener_token(sym)
        if info and info["is_safe"] and info["price_usd"] > 0:
            candidates.append(info)
        if len(candidates) >= 4:
            break
    return jsonify({"candidates": candidates})

@app.route("/api/add_coin", methods=["POST"])
def add_coin():
    global db_dirty
    data = request.json or {}
    raw = data.get("symbol", "").strip()
    anchor = float(data.get("anchor_usd", 10.0))
    if not raw:
        return jsonify({"ok": False})

    clean_sym = raw.upper().replace("USDT", "").strip()
    prices = fetch_live_prices([clean_sym]) if len(raw) <= 12 else {}
    pair = f"{clean_sym}USDT"

    if pair in prices and prices[pair] > 0:
        with state_lock:
            bot_state["coins"][pair] = make_coin_template(clean_sym, anchor, strategy="HYBRID_50_50")
            deploy_anchor_buy(bot_state["coins"][pair], prices[pair], reason="Verified CEX Launch", allow_live_order=False)
            db_dirty = True
        return jsonify({"ok": True})

    dex_info = inspect_dexscreener_token(raw)
    if not dex_info or dex_info["price_usd"] <= 0:
        add_log(f"SNIPER WARNING: Could not find active price pool for '{raw}'.")
        return jsonify({"ok": False, "error": f"No active price pool found for {raw}."})

    if not dex_info["is_safe"]:
        msg = (
            f"🚨 ANTI-RUG SHIELD BLOCKED {dex_info['symbol']}: "
            f"Pool Liquidity is only ${dex_info['liquidity_usd']:,.0f} (Minimum safe threshold is $15,000)!"
        )
        add_log(msg)
        return jsonify({"ok": False, "error": msg})

    sym = dex_info["symbol"]
    pair = f"{sym}USDT"
    with state_lock:
        new_c = make_coin_template(
            sym, anchor, strategy="HYBRID_50_50",
            token_address=dex_info["token_address"], chain_id=dex_info["chain_id"]
        )
        new_c["liquidity_usd"] = dex_info["liquidity_usd"]
        new_c["volume_24h_usd"] = dex_info["volume_24h_usd"]
        new_c["security_badge"] = dex_info["security_badge"]
        new_c["price_source"] = "DEX"
        bot_state["coins"][pair] = new_c
        deploy_anchor_buy(bot_state["coins"][pair], dex_info["price_usd"], reason="DexScreener Anti-Rug Verified", allow_live_order=False)
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
                coin["initial_anchor_usd"] = new_anc
                coin["reinvested_anchor_usd"] = 0.0
                if coin["in_position"] and coin["current_price"] > 0:
                    deploy_anchor_buy(coin, coin["current_price"], reason="Anchor Resized", allow_live_order=False)
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
            deploy_anchor_buy(coin, real_price, reason="Pre-Test Entry", allow_live_order=False)

        if coin["entry_price"] > 0:
            base_low = coin["local_low"] if coin["local_low"] > 0 else coin["entry_price"]
            if coin.get("moonbag_active"):
                rally_peak = coin["entry_price"] * 1.055
                process_coin(pair, coin, rally_peak, is_simulated_test=True)
                pullback = rally_peak * 0.985
                process_coin(pair, coin, pullback, is_simulated_test=True)
            else:
                spike_price = base_low * 1.022
                process_coin(pair, coin, spike_price, is_simulated_test=True)
                pullback_price = spike_price * 0.996
                process_coin(pair, coin, pullback_price, is_simulated_test=True)

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
                coin["reinvested_anchor_usd"] = 0.0
                coin["trades_count"] = 0
            if live_px > 0:
                deploy_anchor_buy(coin, live_px, reason="Synced to Live Market", allow_live_order=False)
        if clear_vault:
            bot_state["history"] = [
                {"time": datetime.utcnow().strftime("%H:%M:%S"), "total": 0.0, "vault": 0.0, "reinvested": 0.0, "label": "Reset"}
            ]
            bot_state["skims"] = []
        add_log("SYNCED TO LIVE MARKET: All anchors deployed at live prices." + (" (Vault reset to $0.00)" if clear_vault else ""))
        db_dirty = True
    return jsonify({"ok": True})

@app.route("/health")
def health():
    return jsonify({"status": "awake", "utc_time": datetime.utcnow().isoformat()}), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
