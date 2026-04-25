import requests
import time
import math
import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime

# ─── CONFIG ───────────────────────────────────────────────────────────────────
TG_TOKEN       = os.environ.get("TG_TOKEN")
TG_CHAT_ID     = os.environ.get("TG_CHAT_ID")
ODDS_API_KEY   = os.environ.get("ODDS_API_KEY", "")
DATABASE_URL   = os.environ.get("DATABASE_URL")
EDGE_THRESHOLD = 0.04
POLL_INTERVAL  = 30
# ──────────────────────────────────────────────────────────────────────────────

GAMMA_API    = "https://gamma-api.polymarket.com"
ODDS_API     = "https://api.the-odds-api.com/v4"
NOAA_API     = "https://api.weather.gov"
BYBIT_API    = "https://api.bybit.com/v5/market"
KRAKEN_API   = "https://api.kraken.com/0/public"

KRAKEN_SYMBOLS = {
    "BTC": "XXBTZUSD", "ETH": "XETHZUSD", "SOL": "SOLUSD",
    "XRP": "XXRPZUSD", "DOGE": "XDGUSD"
}

SPORTS_LEAGUES = [
    "basketball_nba", "icehockey_nhl", "americanfootball_nfl",
    "baseball_mlb", "soccer_epl", "soccer_mls",
]
CRYPTO_PAIRS = ["BTC", "ETH", "SOL", "XRP", "DOGE"]

alerted = set()
vegas_cache = {}
vegas_cache_time = {}
VEGAS_CACHE_TTL = 3600  # refresh odds once per hour

# ─── DATABASE ─────────────────────────────────────────────────────────────────
def db_connect():
    return psycopg2.connect(DATABASE_URL, sslmode="require")

def db_init():
    """Create the signals table if it doesn't exist."""
    if not DATABASE_URL:
        print("⚠️  No DATABASE_URL — logging disabled.")
        return
    try:
        with db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS signals (
                        id              SERIAL PRIMARY KEY,
                        signal_type     TEXT NOT NULL,
                        market_question TEXT NOT NULL,
                        market_slug     TEXT NOT NULL,
                        event_slug      TEXT,
                        condition_id    TEXT,
                        direction       TEXT NOT NULL,
                        entry_price     NUMERIC(6,4) NOT NULL,
                        implied_price   NUMERIC(6,4) NOT NULL,
                        edge            NUMERIC(6,4) NOT NULL,
                        current_price   NUMERIC(6,4),
                        resolved        BOOLEAN DEFAULT FALSE,
                        outcome_won     BOOLEAN,
                        pnl_pct         NUMERIC(8,4),
                        created_at      TIMESTAMPTZ DEFAULT NOW(),
                        updated_at      TIMESTAMPTZ DEFAULT NOW()
                    );
                    CREATE INDEX IF NOT EXISTS idx_signals_resolved
                        ON signals(resolved);
                    CREATE INDEX IF NOT EXISTS idx_signals_slug
                        ON signals(market_slug);
                """)
        print("✅ Database initialized.")
    except Exception as e:
        print(f"DB init error: {e}")

def db_log_signal(signal_type, market, direction, entry_price, implied_price, edge):
    """Insert a new signal row."""
    if not DATABASE_URL:
        return
    try:
        events = market.get("events") or []
        event_slug = events[0].get("slug", "") if events else ""
        with db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO signals
                    (signal_type, market_question, market_slug, event_slug,
                     condition_id, direction, entry_price, implied_price, edge)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    signal_type,
                    market.get("question", ""),
                    market.get("slug", ""),
                    event_slug,
                    market.get("conditionId", ""),
                    direction,
                    entry_price,
                    implied_price,
                    edge,
                ))
    except Exception as e:
        print(f"DB log error: {e}")

def db_update_open_signals():
    """Refresh current_price + check for resolutions on open signals."""
    if not DATABASE_URL:
        return
    try:
        with db_connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT id, market_slug, direction, entry_price "
                    "FROM signals WHERE resolved = FALSE"
                )
                open_signals = cur.fetchall()

            for sig in open_signals:
                fresh = fetch_market_by_slug(sig["market_slug"])
                if not fresh:
                    continue

                # Parse current YES price
                current_price = None
                try:
                    outcome_str = fresh.get("outcomePrices") or fresh.get("outcomes_prices")
                    parsed = json.loads(outcome_str) if isinstance(outcome_str, str) else outcome_str
                    current_price = float(parsed[0])
                except:
                    pass

                if current_price is None:
                    continue

                # Calculate unrealized PnL
                entry = float(sig["entry_price"])
                if sig["direction"] == "YES":
                    pnl_pct = ((current_price - entry) / entry) * 100
                else:
                    pnl_pct = ((entry - current_price) / entry) * 100

                # Check if market is resolved
                is_closed = fresh.get("closed") or False
                outcome_won = None
                resolved = False
                if is_closed:
                    resolved = True
                    # Resolution: YES wins if final price ~1, NO wins if ~0
                    if sig["direction"] == "YES":
                        outcome_won = current_price > 0.5
                    else:
                        outcome_won = current_price < 0.5
                    pnl_pct = 100.0 if outcome_won else -100.0

                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE signals
                        SET current_price = %s,
                            resolved = %s,
                            outcome_won = %s,
                            pnl_pct = %s,
                            updated_at = NOW()
                        WHERE id = %s
                    """, (current_price, resolved, outcome_won, pnl_pct, sig["id"]))

                # Notify on resolution
                if resolved:
                    emoji = "✅" if outcome_won else "❌"
                    word  = "WON" if outcome_won else "LOST"
                    send_telegram(
                        f"{emoji} <b>SIGNAL RESOLVED — {word}</b>\n\n"
                        f"📋 {fresh.get('question','')[:80]}\n"
                        f"🎯 Bet: {sig['direction']}\n"
                        f"📊 Entry: {entry*100:.1f}¢\n"
                        f"📈 Final: {current_price*100:.1f}¢"
                    )
    except Exception as e:
        print(f"DB update error: {e}")

def fetch_market_by_slug(slug):
    try:
        r = requests.get(f"{GAMMA_API}/markets",
            params={"slug": slug, "limit": 1}, timeout=10)
        if not r.ok:
            return None
        data = r.json()
        markets = data.get("markets", data) if isinstance(data, dict) else data
        return markets[0] if markets else None
    except:
        return None


# ─── TELEGRAM ─────────────────────────────────────────────────────────────────
def send_telegram(message):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": message, "parse_mode": "HTML"},
            timeout=10
        )
    except Exception as e:
        print(f"Telegram error: {e}")

def poly_link(m):
    events = m.get("events")
    if events and len(events) > 0:
        event_slug = events[0].get("slug", "")
        if event_slug:
            return f"https://polymarket.com/event/{event_slug}"
    return f"https://polymarket.com/event/{m.get('slug','')}"


# ─── CRYPTO SIGNAL ────────────────────────────────────────────────────────────
def sigmoid(x):
    return 1 / (1 + math.exp(-max(-20, min(20, x))))

def derive_implied_prob(klines, funding_rate):
    if not klines or len(klines) < 10:
        return None
    closes  = [float(k[4]) for k in klines]
    returns = [(closes[i] - closes[i-1]) / closes[i-1] for i in range(1, len(closes))]
    recent  = returns[-5:]
    avg_ret = sum(recent) / len(recent)
    vol     = math.sqrt(sum(r**2 for r in recent) / len(recent))
    signal  = avg_ret / vol if vol > 0 else 0
    momentum     = (closes[-1] - closes[-2]) / closes[-2]
    funding_bias = float(funding_rate or 0) * 100
    raw = sigmoid(signal * 2 + funding_bias * 0.5 + momentum * 10)
    return max(0.05, min(0.95, raw))

def fetch_klines_bybit(symbol):
    try:
        r = requests.get(f"{BYBIT_API}/kline",
            params={"category": "spot", "symbol": f"{symbol}USDT",
                    "interval": "1", "limit": 15},
            timeout=10)
        if not r.ok:
            return None
        data = r.json()
        rows = data.get("result", {}).get("list", [])
        if not rows:
            return None
        # Bybit returns [time, open, high, low, close, volume, turnover]
        # Reformat to match Binance structure [0]=time [4]=close
        return [[row[0], row[1], row[2], row[3], row[4]] for row in rows]
    except Exception as e:
        print(f"  Bybit {symbol} error: {e}")
        return None

def fetch_klines_kraken(symbol):
    try:
        pair = KRAKEN_SYMBOLS.get(symbol)
        if not pair:
            return None
        r = requests.get(f"{KRAKEN_API}/OHLC",
            params={"pair": pair, "interval": 1},
            timeout=10)
        if not r.ok:
            return None
        data = r.json()
        result = data.get("result", {})
        rows = result.get(pair) or result.get(list(result.keys())[0])
        if not rows:
            return None
        # Kraken returns [time, open, high, low, close, vwap, volume, count]
        return [[row[0], row[1], row[2], row[3], row[4]] for row in rows[-15:]]
    except Exception as e:
        print(f"  Kraken {symbol} error: {e}")
        return None

def fetch_klines(symbol):
    data = fetch_klines_bybit(symbol)
    if data:
        print(f"  {symbol}: Bybit ✅")
        return data
    print(f"  {symbol}: Bybit failed, trying Kraken...")
    data = fetch_klines_kraken(symbol)
    if data:
        print(f"  {symbol}: Kraken ✅")
        return data
    print(f"  {symbol}: both sources failed")
    return None

def fetch_funding(symbol):
    try:
        r = requests.get(f"{BYBIT_API}/funding/history",
            params={"category": "linear", "symbol": f"{symbol}USDT", "limit": 1},
            timeout=10)
        if not r.ok:
            return None
        rows = r.json().get("result", {}).get("list", [])
        return rows[0].get("fundingRate") if rows else None
    except:
        return None

def fetch_polymarkets_crypto():
    try:
        r = requests.get(f"{GAMMA_API}/markets",
            params={"active": "true", "limit": 100,
                    "order": "volume", "ascending": "false"}, timeout=10)
        if not r.ok:
            return []
        data = r.json()
        markets = data.get("markets", data) if isinstance(data, dict) else data
        assets = ["btc", "bitcoin", "eth", "ethereum", "sol", "solana",
                  "xrp", "ripple", "doge", "dogecoin"]
        dirs = ["up", "down", "higher", "above", "below", "reach", "hit", "exceed"]
        return [m for m in markets if
                any(a in m.get("question", "").lower() for a in assets) and
                any(d in m.get("question", "").lower() for d in dirs)]
    except Exception as e:
        print(f"Polymarket crypto fetch error: {e}")
        return []

def get_crypto_symbol(question):
    q = question.lower()
    if "sol" in q or "solana" in q:    return "SOL"
    if "xrp" in q or "ripple" in q:    return "XRP"
    if "doge" in q or "dogecoin" in q: return "DOGE"
    if "eth" in q or "ethereum" in q:  return "ETH"
    return "BTC"

def run_crypto_scan(implied_probs):
    markets = fetch_polymarkets_crypto()
    signals = 0
    for m in markets:
        question = m.get("question","")
        slug     = m.get("slug","")
        poly_price = 0.5
        try:
            outcome_str = m.get("outcomePrices") or m.get("outcomes_prices")
            parsed = json.loads(outcome_str) if isinstance(outcome_str, str) else outcome_str
            poly_price = float(parsed[0])
        except:
            pass

        symbol  = get_crypto_symbol(question)
        implied = implied_probs.get(symbol)
        if implied is None:
            continue

        edge = implied - poly_price
        abs_edge = abs(edge)
        if abs_edge < EDGE_THRESHOLD:
            continue

        direction = "YES" if edge > 0 else "NO"
        key = f"crypto:{slug}:{direction}:{round(poly_price,2)}"
        if key in alerted:
            continue
        alerted.add(key)

        db_log_signal("crypto", m, direction, poly_price, implied, abs_edge)

        volume  = m.get("volume24hr") or m.get("volumeNum") or "n/a"
        vol_str = f"${float(volume):,.0f}" if volume != "n/a" else "n/a"
        emoji   = "🟢" if direction == "YES" else "🔴"

        print(f"  ⚡ CRYPTO — {question[:50]} | {direction} | +{abs_edge*100:.1f}%")
        send_telegram(
            f"{emoji} <b>CRYPTO SIGNAL</b>\n\n"
            f"📋 <b>Market:</b> {question}\n"
            f"🎯 <b>Bet:</b> {direction}\n"
            f"📊 <b>Poly price:</b> {poly_price*100:.1f}¢\n"
            f"📈 <b>Binance implied:</b> {implied*100:.1f}¢\n"
            f"⚡ <b>Edge:</b> +{abs_edge*100:.1f}%\n"
            f"💧 <b>24h volume:</b> {vol_str}\n"
            f"🔗 <a href='{poly_link(m)}'>Bet on Polymarket</a>\n\n"
            f"<i>Signal only — not financial advice.</i>"
        )
        signals += 1
    return signals


# ─── SPORTS SIGNAL ────────────────────────────────────────────────────────────
def moneyline_to_prob(american):
    return (100/(american+100)) if american > 0 else (abs(american)/(abs(american)+100))

def fetch_vegas_odds_for_league(league):
    if not ODDS_API_KEY:
        return []
    try:
        r = requests.get(f"{ODDS_API}/sports/{league}/odds",
            params={"apiKey": ODDS_API_KEY, "regions": "us",
                    "markets": "h2h", "oddsFormat": "american"},
            timeout=10)
        return r.json() if r.ok else []
    except:
        return []

def fetch_polymarkets_sports():
    try:
        r = requests.get(f"{GAMMA_API}/markets",
            params={"active": "true", "tag_slug": "sports", "limit": 100,
                    "order": "volume", "ascending": "false"}, timeout=10)
        if not r.ok:
            return []
        data = r.json()
        markets = data.get("markets", data) if isinstance(data, dict) else data
        return [m for m in markets if m.get("question")]
    except:
        return []

def run_sports_scan():
    poly_sports = fetch_polymarkets_sports()
    if not poly_sports:
        return 0

    signals = 0
    now = time.time()

    for league in SPORTS_LEAGUES:
        # Only fetch fresh odds if cache is stale
        if league not in vegas_cache or (now - vegas_cache_time.get(league, 0)) > VEGAS_CACHE_TTL:
            fresh = fetch_vegas_odds_for_league(league)
            if fresh:
                vegas_cache[league] = fresh
                vegas_cache_time[league] = now
                print(f"  Refreshed odds: {league}")

        vegas_games = vegas_cache.get(league, [])
        if not vegas_games:
            continue

        for game in vegas_games:
            home = game.get("home_team", "")
            away = game.get("away_team", "")
            if not home or not away:
                continue

            best_home_odds = None
            for bm in game.get("bookmakers", []):
                for mk in bm.get("markets", []):
                    if mk.get("key") != "h2h":
                        continue
                    for o in mk.get("outcomes", []):
                        if o["name"] == home:
                            if best_home_odds is None or o["price"] > best_home_odds:
                                best_home_odds = o["price"]

            if best_home_odds is None:
                continue

            vegas_prob = moneyline_to_prob(best_home_odds)

            for m in poly_sports:
                question = m.get("question", "")
                if home.split()[-1].lower() not in question.lower():
                    continue

                poly_price = 0.5
                try:
                    outcome_str = m.get("outcomePrices") or m.get("outcomes_prices")
                    parsed = json.loads(outcome_str) if isinstance(outcome_str, str) else outcome_str
                    poly_price = float(parsed[0])
                except:
                    pass

                edge = vegas_prob - poly_price
                abs_edge = abs(edge)
                if abs_edge < EDGE_THRESHOLD:
                    continue

                direction = "YES" if edge > 0 else "NO"
                key = f"sports:{m.get('slug', '')}:{direction}:{round(poly_price, 2)}"
                if key in alerted:
                    continue
                alerted.add(key)

                db_log_signal("sports", m, direction, poly_price, vegas_prob, abs_edge)

                emoji = "🟢" if direction == "YES" else "🔴"
                league_emoji = {"basketball_nba": "🏀", "icehockey_nhl": "🏒",
                                "americanfootball_nfl": "🏈", "baseball_mlb": "⚾",
                                "soccer_epl": "⚽", "soccer_mls": "⚽"}.get(league, "🏆")

                print(f"  ⚡ SPORTS — {question[:50]} | {direction} | +{abs_edge*100:.1f}%")
                send_telegram(
                    f"{emoji} <b>SPORTS SIGNAL</b>\n\n"
                    f"📋 <b>Market:</b> {question}\n"
                    f"🎯 <b>Bet:</b> {direction}\n"
                    f"📊 <b>Poly price:</b> {poly_price*100:.1f}¢\n"
                    f"{league_emoji} <b>Vegas implied:</b> {vegas_prob*100:.1f}¢\n"
                    f"⚡ <b>Edge:</b> +{abs_edge*100:.1f}%\n"
                    f"🔗 <a href='{poly_link(m)}'>Bet on Polymarket</a>\n\n"
                    f"<i>Signal only — not financial advice.</i>"
                )
                signals += 1
    return signals


# ─── WEATHER SIGNAL ───────────────────────────────────────────────────────────
WEATHER_CITIES = [
    {"name":"New York","office":"OKX","gridX":33,"gridY":37},
    {"name":"Los Angeles","office":"LOX","gridX":149,"gridY":48},
    {"name":"Chicago","office":"LOT","gridX":76,"gridY":73},
    {"name":"Houston","office":"HGX","gridX":66,"gridY":98},
    {"name":"Miami","office":"MFL","gridX":110,"gridY":38},
]

def fetch_noaa_forecast(office, grid_x, grid_y):
    try:
        r = requests.get(
            f"{NOAA_API}/gridpoints/{office}/{grid_x},{grid_y}/forecast",
            headers={"User-Agent":"PolySignalBot/1.0"}, timeout=10)
        if not r.ok:
            return None
        periods = r.json().get("properties",{}).get("periods",[])
        return periods[:4] if periods else None
    except:
        return None

def parse_rain_prob(period):
    prob = period.get("probabilityOfPrecipitation",{})
    if prob and prob.get("value") is not None:
        return prob["value"]/100.0
    text = period.get("detailedForecast","").lower()
    for phrase, val in [("slight chance",0.2),("chance of rain",0.4),
                        ("likely",0.65),("definitely",0.85)]:
        if phrase in text:
            return val
    return None

def fetch_polymarkets_weather():
    try:
        r = requests.get(f"{GAMMA_API}/markets",
            params={"active":"true","limit":100,"order":"volume",
                    "ascending":"false"}, timeout=10)
        if not r.ok:
            return []
        data = r.json()
        markets = data.get("markets", data) if isinstance(data, dict) else data
        words = ["rain","temperature","snow","storm","weather",
                 "degrees","fahrenheit","celsius","precipitation"]
        return [m for m in markets if m.get("question") and
                any(w in m.get("question","").lower() for w in words)]
    except:
        return []

def run_weather_scan():
    poly_weather = fetch_polymarkets_weather()
    if not poly_weather:
        return 0

    signals = 0
    for city in WEATHER_CITIES:
        forecast = fetch_noaa_forecast(city["office"], city["gridX"], city["gridY"])
        if not forecast:
            continue

        for period in forecast:
            rain_prob = parse_rain_prob(period)
            if rain_prob is None:
                continue

            for m in poly_weather:
                question = m.get("question","")
                if city["name"].lower() not in question.lower():
                    continue
                if "rain" not in question.lower() and "precipitation" not in question.lower():
                    continue

                poly_price = 0.5
                try:
                    outcome_str = m.get("outcomePrices") or m.get("outcomes_prices")
                    parsed = json.loads(outcome_str) if isinstance(outcome_str, str) else outcome_str
                    poly_price = float(parsed[0])
                except:
                    pass

                edge = rain_prob - poly_price
                abs_edge = abs(edge)
                if abs_edge < EDGE_THRESHOLD:
                    continue

                direction = "YES" if edge > 0 else "NO"
                key = f"weather:{m.get('slug','')}:{direction}:{round(poly_price,2)}"
                if key in alerted:
                    continue
                alerted.add(key)

                db_log_signal("weather", m, direction, poly_price, rain_prob, abs_edge)

                emoji = "🟢" if direction == "YES" else "🔴"
                print(f"  ⚡ WEATHER — {question[:50]} | {direction} | +{abs_edge*100:.1f}%")
                send_telegram(
                    f"{emoji} <b>WEATHER SIGNAL</b>\n\n"
                    f"📋 <b>Market:</b> {question}\n"
                    f"🎯 <b>Bet:</b> {direction}\n"
                    f"📊 <b>Poly price:</b> {poly_price*100:.1f}¢\n"
                    f"🌧 <b>NOAA forecast:</b> {rain_prob*100:.0f}% chance\n"
                    f"⚡ <b>Edge:</b> +{abs_edge*100:.1f}%\n"
                    f"🔗 <a href='{poly_link(m)}'>Bet on Polymarket</a>\n\n"
                    f"<i>Signal only — not financial advice.</i>"
                )
                signals += 1
    return signals


# ─── MAIN LOOP ────────────────────────────────────────────────────────────────
RESOLUTION_CHECK_EVERY = 10  # cycles (every 5 min at 30s interval)

def run_scan(cycle):
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Scanning... (cycle #{cycle})")

    implied = {}
    for sym in CRYPTO_PAIRS:
        p = derive_implied_prob(fetch_klines(sym), fetch_funding(sym))
        if p:
            implied[sym] = p
    print(f"  Crypto implied: {implied}")
    print(f"  Crypto markets found: {len(fetch_polymarkets_crypto())}")
    print(f"  Sports markets found: {len(fetch_polymarkets_sports())}")
    print(f"  Weather markets found: {len(fetch_polymarkets_weather())}")
    print(f"  Vegas odds (NBA): {len(fetch_vegas_odds_for_league('basketball_nba'))}")

    cs = run_crypto_scan(implied)
    ss = run_sports_scan()
    ws = run_weather_scan()

    if cs+ss+ws == 0:
        print("  No signals this cycle.")

    if cycle % RESOLUTION_CHECK_EVERY == 0:
        print("  Checking open signals for updates...")
        db_update_open_signals()

    if len(alerted) > 1000:
        alerted.clear()


if __name__ == "__main__":
    print("=" * 50)
    print("  POLY SIGNAL ENGINE")
    print(f"  Threshold: {EDGE_THRESHOLD*100:.0f}%  |  Interval: {POLL_INTERVAL}s")
    print(f"  DB: {'✅' if DATABASE_URL else '❌'}")
    print("=" * 50)

    db_init()

    send_telegram(
        f"⚡ <b>Poly Signal Engine started</b>\n\n"
        f"Scanning every {POLL_INTERVAL}s\n"
        f"Edge threshold: {EDGE_THRESHOLD*100:.0f}%\n"
        f"Crypto: ✅ ({', '.join(CRYPTO_PAIRS)})\n"
        f"Sports: {'✅' if ODDS_API_KEY else '❌'} ({len(SPORTS_LEAGUES)} leagues)\n"
        f"Weather: ✅\n"
        f"Logging: {'✅ Postgres' if DATABASE_URL else '❌'}"
    )

    cycle = 0
    while True:
        cycle += 1
        try:
            run_scan(cycle)
        except Exception as e:
            print(f"Error: {e}")
            send_telegram(f"⚠️ <b>Error:</b> {e}")
        time.sleep(POLL_INTERVAL)
