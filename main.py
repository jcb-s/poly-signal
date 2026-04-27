import re
import requests
import time
import math
import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timezone

# ─── CONFIG ───────────────────────────────────────────────────────────────────
TG_TOKEN       = os.environ.get("TG_TOKEN")
TG_CHAT_ID     = os.environ.get("TG_CHAT_ID")
_raw_odds_keys = os.environ.get("ODDS_API_KEYS") or os.environ.get("ODDS_API_KEY", "")
ODDS_API_KEYS  = [k.strip() for k in _raw_odds_keys.split(",") if k.strip()]
_odds_key_idx      = [0]   # mutable so fetch_vegas_odds_for_league can rotate without global
_odds_request_count = [0]  # incremented on every live HTTP request to the Odds API
DATABASE_URL   = os.environ.get("DATABASE_URL")
EDGE_THRESHOLD = 0.04
POLL_INTERVAL  = 30
BOT_VERSION    = os.environ.get("BOT_VERSION", "2.0.0")
ENABLE_CRYPTO_SIGNALS  = False
ENABLE_SPORTS_SIGNALS  = False
ENABLE_WEATHER_SIGNALS = True
# ──────────────────────────────────────────────────────────────────────────────

GAMMA_API            = "https://gamma-api.polymarket.com"
POLYMARKET_DATA_API  = "https://data-api.polymarket.com"
ODDS_API             = "https://api.the-odds-api.com/v4"
NOAA_API             = "https://api.weather.gov"
KRAKEN_API           = "https://api.kraken.com/0/public"

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

def parse_aware_dt(s):
    """Parse any ISO date/datetime string and always return a timezone-aware datetime.
    Handles date-only strings (e.g. '2026-04-26') that have no Z to replace."""
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None

def is_market_expired(market):
    end = market.get("endDate") or market.get("endDateIso")
    dt = parse_aware_dt(end)
    if dt is None:
        return False
    return dt < datetime.now(timezone.utc)

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
                    ALTER TABLE signals ADD COLUMN IF NOT EXISTS bot_version TEXT DEFAULT '1.0.0';
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
                     condition_id, direction, entry_price, implied_price, edge,
                     bot_version)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
                    BOT_VERSION,
                ))
    except Exception as e:
        print(f"DB log error: {e}")

def db_load_alerted():
    """Seed the in-memory alerted set from open DB signals so restarts don't re-fire the same slug."""
    if not DATABASE_URL:
        return
    try:
        with db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT signal_type, market_slug FROM signals WHERE resolved = FALSE"
                )
                rows = cur.fetchall()
        for signal_type, slug in rows:
            alerted.add(f"{signal_type}:{slug}")
        print(f"  Seeded {len(rows)} slugs into alerted from DB.")
    except Exception as e:
        print(f"DB load alerted error: {e}")

def db_init_wallets():
    if not DATABASE_URL:
        return
    try:
        with db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS tracked_wallets (
                        address          TEXT PRIMARY KEY,
                        win_rate         NUMERIC(5,4)  NOT NULL,
                        markets_resolved INTEGER       NOT NULL,
                        total_pnl        NUMERIC(12,2),
                        first_seen       TIMESTAMPTZ   DEFAULT NOW(),
                        last_seen        TIMESTAMPTZ   DEFAULT NOW()
                    );
                    CREATE TABLE IF NOT EXISTS wallet_positions (
                        id           SERIAL PRIMARY KEY,
                        address      TEXT        NOT NULL,
                        condition_id TEXT        NOT NULL,
                        outcome      TEXT        NOT NULL,
                        slug         TEXT,
                        title        TEXT,
                        avg_price    NUMERIC(6,4),
                        size         NUMERIC(12,4),
                        alerted_at   TIMESTAMPTZ DEFAULT NOW(),
                        UNIQUE(address, condition_id, outcome)
                    );
                """)
        print("✅ Wallet tables initialized.")
    except Exception as e:
        print(f"Wallet DB init error: {e}")

def db_upsert_wallet(address, win_rate, markets_resolved, total_pnl):
    if not DATABASE_URL:
        return
    try:
        with db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO tracked_wallets
                        (address, win_rate, markets_resolved, total_pnl)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (address) DO UPDATE SET
                        win_rate         = EXCLUDED.win_rate,
                        markets_resolved = EXCLUDED.markets_resolved,
                        total_pnl        = EXCLUDED.total_pnl,
                        last_seen        = NOW()
                """, (address, win_rate, markets_resolved, total_pnl))
    except Exception as e:
        print(f"DB upsert wallet error: {e}")

def db_get_tracked_wallets():
    if not DATABASE_URL:
        return []
    try:
        with db_connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM tracked_wallets ORDER BY win_rate DESC")
                return cur.fetchall()
    except Exception as e:
        print(f"DB get wallets error: {e}")
        return []

def db_log_wallet_position(address, position):
    """Insert a new wallet position row. Returns True if it was genuinely new."""
    if not DATABASE_URL:
        return True  # assume new so we still alert when DB is unavailable
    try:
        with db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO wallet_positions
                        (address, condition_id, outcome, slug, title, avg_price, size)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (address, condition_id, outcome) DO NOTHING
                    RETURNING id
                """, (
                    address,
                    position.get("conditionId", ""),
                    position.get("outcome", ""),
                    position.get("slug", ""),
                    position.get("title", "")[:200],
                    position.get("avgPrice") or 0,
                    position.get("size") or 0,
                ))
                return cur.fetchone() is not None
    except Exception as e:
        print(f"DB log wallet position error: {e}")
        return False

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
                    no_entry   = 1 - entry
                    no_current = 1 - current_price
                    pnl_pct = ((no_current - no_entry) / no_entry) * 100 if no_entry > 0 else 0

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
    """Fetch a market by slug, falling back to closed=true for resolved markets."""
    try:
        for extra in [{}, {"closed": "true"}]:
            r = requests.get(f"{GAMMA_API}/markets",
                params={"slug": slug, "limit": 1, **extra}, timeout=10)
            if not r.ok:
                return None
            data = r.json()
            markets = data.get("markets", data) if isinstance(data, dict) else data
            if markets:
                return markets[0]
        return None
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


# ─── KELLY CRITERION ──────────────────────────────────────────────────────────
def kelly_str(edge, poly_price, direction):
    """Return a formatted Kelly sizing string for a Telegram message."""
    denom = (1 - poly_price) if direction == "YES" else poly_price
    if denom <= 0:
        return ""
    full_kelly = (edge / denom) * 100
    half_kelly = full_kelly / 2
    return f"🎲 <b>Kelly sizing:</b> {full_kelly:.1f}% (½ Kelly: {half_kelly:.1f}%)\n"


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
    data = fetch_klines_kraken(symbol)
    if data:
        print(f"  {symbol}: Kraken ✅")
        return data
    print(f"  {symbol}: Kraken failed")
    return None

def fetch_polymarkets_crypto():
    try:
        r = requests.get(f"{GAMMA_API}/markets",
            params={"active": "true", "limit": 100,
                    "order": "volume", "ascending": "false"}, timeout=10)
        if not r.ok:
            print(f"  Polymarket fetch failed: {r.status_code}")
            return []
        data = r.json()
        markets = data.get("markets", data) if isinstance(data, dict) else data
        print(f"  Raw markets returned: {len(markets)}")
        print(f"  Sample questions: {[m.get('question','') for m in markets[:5]]}")
        assets = ["btc", "bitcoin", "eth", "ethereum", "sol", "solana",
                  "xrp", "ripple", "doge", "dogecoin"]
        dirs = ["up", "down", "higher", "above", "below", "reach", "hit", "exceed"]
        filtered = [m for m in markets if
                any(a in m.get("question", "").lower() for a in assets) and
                any(d in m.get("question", "").lower() for d in dirs)]
        print(f"  After filter: {len(filtered)}")
        return filtered
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

        if is_market_expired(m):
            continue
        key = f"crypto:{slug}"
        if key in alerted:
            continue

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
            f"📈 <b>Implied:</b> {implied*100:.1f}¢\n"
            f"⚡ <b>Edge:</b> +{abs_edge*100:.1f}%\n"
            f"{kelly_str(abs_edge, poly_price, direction)}"
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
    if not ODDS_API_KEYS:
        return []
    n = len(ODDS_API_KEYS)
    for attempt in range(n):
        key = ODDS_API_KEYS[_odds_key_idx[0]]
        try:
            _odds_request_count[0] += 1
            r = requests.get(f"{ODDS_API}/sports/{league}/odds",
                params={"apiKey": key, "regions": "us",
                        "markets": "h2h", "oddsFormat": "american"},
                timeout=10)
            if r.ok:
                return r.json()
            if r.status_code in (401, 429):
                next_idx = (_odds_key_idx[0] + 1) % n
                print(f"  Odds API key {_odds_key_idx[0]+1}/{n} got {r.status_code} on {league}, rotating to key {next_idx+1}")
                _odds_key_idx[0] = next_idx
                continue
            return []
        except Exception as e:
            print(f"  Odds API error (key {_odds_key_idx[0]+1}/{n}): {e}")
            return []
    print(f"  Odds API: all {n} keys returned 401/429 for {league}")
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
            # Always stamp the time so an empty/failed response doesn't trigger
            # a live fetch on every cycle — retry after TTL like everything else.
            vegas_cache_time[league] = now
            vegas_cache[league] = fresh  # always write, even [] so cache hit fires next cycle
            print(f"  Odds API [{league}]: fetched {len(fresh)} games")
        else:
            age = int(now - vegas_cache_time[league])
            print(f"  Odds API [{league}]: cache hit ({age}s old, TTL {VEGAS_CACHE_TTL}s)")

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

                if is_market_expired(m):
                    continue
                key = f"sports:{m.get('slug', '')}"
                if key in alerted:
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
                    f"{kelly_str(abs_edge, poly_price, direction)}"
                    f"🔗 <a href='{poly_link(m)}'>Bet on Polymarket</a>\n\n"
                    f"<i>Signal only — not financial advice.</i>"
                )
                signals += 1
    return signals


# ─── WEATHER SIGNAL ───────────────────────────────────────────────────────────
WEATHER_CITIES = [
    {"name": "New York",      "lat": 40.7128,  "lon": -74.0060},
    {"name": "Los Angeles",   "lat": 34.0522,  "lon": -118.2437},
    {"name": "Chicago",       "lat": 41.8781,  "lon": -87.6298},
    {"name": "Houston",       "lat": 29.7604,  "lon": -95.3698},
    {"name": "Miami",         "lat": 25.7617,  "lon": -80.1918},
    {"name": "Atlanta",       "lat": 33.7490,  "lon": -84.3880},
    {"name": "San Francisco", "lat": 37.7749,  "lon": -122.4194},
    {"name": "Dallas",        "lat": 32.7767,  "lon": -96.7970},
    {"name": "Seattle",       "lat": 47.6062,  "lon": -122.3321},
    {"name": "Phoenix",       "lat": 33.4484,  "lon": -112.0740},
]

_noaa_grid_cache = {}    # (lat,lon) -> (office, grid_x, grid_y, stations_url)
_noaa_station_cache = {} # stations_url -> station_id

def _noaa_points(lat, lon):
    """Populate _noaa_grid_cache for (lat, lon). Returns True on success."""
    cache_key = (lat, lon)
    if cache_key in _noaa_grid_cache:
        return True
    try:
        pts = requests.get(
            f"{NOAA_API}/points/{lat},{lon}",
            headers={"User-Agent": "DUB/1.0"}, timeout=10)
        if not pts.ok:
            print(f"  NOAA /points {lat},{lon} failed: {pts.status_code}")
            return False
        props = pts.json().get("properties", {})
        _noaa_grid_cache[cache_key] = (
            props.get("gridId"),
            props.get("gridX"),
            props.get("gridY"),
            props.get("observationStations"),
        )
        return True
    except Exception as e:
        print(f"  NOAA /points error ({lat},{lon}): {e}")
        return False

def fetch_noaa_forecast(lat, lon):
    """Fetch NOAA forecast, resolving gridpoints dynamically from lat/lon."""
    try:
        if not _noaa_points(lat, lon):
            return None
        office, grid_x, grid_y, _ = _noaa_grid_cache[(lat, lon)]
        if not office:
            print(f"  NOAA /points returned no gridId for {lat},{lon}")
            return None
        r = requests.get(
            f"{NOAA_API}/gridpoints/{office}/{grid_x},{grid_y}/forecast",
            headers={"User-Agent": "DUB/1.0"}, timeout=10)
        if not r.ok:
            print(f"  NOAA forecast {office}/{grid_x},{grid_y} failed: {r.status_code}")
            return None
        periods = r.json().get("properties", {}).get("periods", [])
        return periods[:4] if periods else None
    except Exception as e:
        print(f"  NOAA fetch error ({lat},{lon}): {e}")
        return None

def fetch_noaa_current_temp(lat, lon):
    """Return current observed temperature in °F from the nearest NOAA station."""
    try:
        if not _noaa_points(lat, lon):
            return None
        _, _, _, stations_url = _noaa_grid_cache[(lat, lon)]
        if not stations_url:
            return None

        if stations_url not in _noaa_station_cache:
            sr = requests.get(stations_url,
                              headers={"User-Agent": "DUB/1.0"}, timeout=10)
            if not sr.ok:
                return None
            features = sr.json().get("features", [])
            if not features:
                return None
            sid = features[0].get("properties", {}).get("stationIdentifier")
            _noaa_station_cache[stations_url] = sid

        station_id = _noaa_station_cache.get(stations_url)
        if not station_id:
            return None

        obs = requests.get(
            f"{NOAA_API}/stations/{station_id}/observations/latest",
            headers={"User-Agent": "DUB/1.0"}, timeout=10)
        if not obs.ok:
            return None
        temp_c = obs.json().get("properties", {}).get("temperature", {}).get("value")
        if temp_c is None:
            return None
        return temp_c * 9 / 5 + 32
    except Exception as e:
        print(f"  NOAA current temp error ({lat},{lon}): {e}")
        return None

def parse_rain_prob(period):
    prob = period.get("probabilityOfPrecipitation", {})
    if prob and prob.get("value") is not None:
        return prob["value"] / 100.0
    text = period.get("detailedForecast", "").lower()
    for phrase, val in [("slight chance", 0.2), ("chance of", 0.4),
                        ("likely", 0.65), ("rain and thunderstorms", 0.8)]:
        if phrase in text:
            return val
    return None

def parse_temp_from_forecast(period):
    """Returns forecast temperature in °F."""
    temp = period.get("temperature")
    unit = period.get("temperatureUnit", "F")
    if temp is None:
        return None
    if unit == "C":
        temp = temp * 9 / 5 + 32
    return float(temp)

def implied_prob_for_temp_market(question, forecast_temp_f):
    """Derive implied probability from NOAA temp vs market question threshold."""
    q = question.lower()

    # "between X-Y°F" or "between X–Y°F"
    m = re.search(r'between\s+(\d+)[–\-](\d+)\s*°?f', q)
    if m:
        low, high = float(m.group(1)), float(m.group(2))
        if low <= forecast_temp_f <= high:
            return 0.72
        diff = min(abs(forecast_temp_f - low), abs(forecast_temp_f - high))
        return max(0.05, 0.72 - diff * 0.06)

    # "X°F or higher/above"
    m = re.search(r'(\d+)\s*°?f\s+or\s+(?:higher|above)', q)
    if m:
        threshold = float(m.group(1))
        return min(0.95, max(0.05, 0.5 + (forecast_temp_f - threshold) * 0.05))

    # "X°F or below/lower"
    m = re.search(r'(\d+)\s*°?f\s+or\s+(?:below|lower)', q)
    if m:
        threshold = float(m.group(1))
        return min(0.95, max(0.05, 0.5 + (threshold - forecast_temp_f) * 0.05))

    # "between X-Y°C"
    m = re.search(r'between\s+(\d+)[–\-](\d+)\s*°?c', q)
    if m:
        low_f = float(m.group(1)) * 9 / 5 + 32
        high_f = float(m.group(2)) * 9 / 5 + 32
        if low_f <= forecast_temp_f <= high_f:
            return 0.72
        diff = min(abs(forecast_temp_f - low_f), abs(forecast_temp_f - high_f))
        return max(0.05, 0.72 - diff * 0.06)

    # "X°C or higher/above"
    m = re.search(r'(\d+)\s*°?c\s+or\s+(?:higher|above)', q)
    if m:
        threshold_f = float(m.group(1)) * 9 / 5 + 32
        return min(0.95, max(0.05, 0.5 + (forecast_temp_f - threshold_f) * 0.05))

    # "X°C or below/lower"
    m = re.search(r'(\d+)\s*°?c\s+or\s+(?:below|lower)', q)
    if m:
        threshold_f = float(m.group(1)) * 9 / 5 + 32
        return min(0.95, max(0.05, 0.5 + (threshold_f - forecast_temp_f) * 0.05))

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
        forecast = fetch_noaa_forecast(city["lat"], city["lon"])
        matched = sum(1 for m in poly_weather if city["name"].lower() in m.get("question", "").lower())
        print(f"  Weather {city['name']}: forecast={'ok' if forecast else 'failed'}, markets matched={matched}")
        if not forecast:
            continue

        for m in poly_weather:
            question = m.get("question", "")
            if city["name"].lower() not in question.lower():
                continue

            if is_market_expired(m):
                continue
            key = f"weather:{m.get('slug', '')}"
            if key in alerted:
                continue

            poly_price = 0.5
            try:
                outcome_str = m.get("outcomePrices") or m.get("outcomes_prices")
                parsed = json.loads(outcome_str) if isinstance(outcome_str, str) else outcome_str
                poly_price = float(parsed[0])
            except:
                pass

            # Determine market type and derive implied probability
            implied = None
            signal_label = ""
            q_lower = question.lower()

            is_temp_market = any(w in q_lower for w in
                                 ["temperature", "°f", "°c", "fahrenheit", "celsius", "degrees"])
            is_precip_market = any(w in q_lower for w in ["rain", "precipitation", "snow", "storm"])

            if is_temp_market:
                # If the current observed temp already exceeds the market's upper
                # bound, the range cannot resolve YES — skip before doing anything else.
                upper_bound_f = None
                m_ub = re.search(r'between\s+(\d+)[–\-](\d+)\s*°?f', q_lower)
                if m_ub:
                    upper_bound_f = float(m_ub.group(2))
                else:
                    m_ub_c = re.search(r'between\s+(\d+)[–\-](\d+)\s*°?c', q_lower)
                    if m_ub_c:
                        upper_bound_f = float(m_ub_c.group(2)) * 9 / 5 + 32

                if upper_bound_f is not None:
                    current_temp_f = fetch_noaa_current_temp(city["lat"], city["lon"])
                    if current_temp_f is not None and current_temp_f > upper_bound_f:
                        print(f"    Skip (obs {current_temp_f:.1f}°F > ceiling {upper_bound_f:.0f}°F): {question[:50]!r}")
                        continue

                daytime = next((p for p in forecast if p.get("isDaytime", True)), forecast[0])
                temp_f = parse_temp_from_forecast(daytime)
                if temp_f is not None:
                    implied = implied_prob_for_temp_market(question, temp_f)
                    signal_label = f"🌡️ NOAA temp: {temp_f:.0f}°F → implied"

            if implied is None and is_precip_market:
                for period in forecast:
                    rain_prob = parse_rain_prob(period)
                    if rain_prob is not None:
                        implied = rain_prob
                        signal_label = f"🌧 NOAA precip: {rain_prob*100:.0f}% → implied"
                        break

            if implied is None:
                continue

            edge = implied - poly_price
            abs_edge = abs(edge)
            if abs_edge < EDGE_THRESHOLD:
                continue

            direction = "YES" if edge > 0 else "NO"

            # Skip YES signals where the market is already nearly worthless
            if direction == "YES" and poly_price < 0.05:
                continue

            alerted.add(key)

            db_log_signal("weather", m, direction, poly_price, implied, abs_edge)

            emoji = "🟢" if direction == "YES" else "🔴"
            print(f"  ⚡ WEATHER — {question[:50]} | {direction} | +{abs_edge*100:.1f}%")
            send_telegram(
                f"{emoji} <b>WEATHER SIGNAL</b>\n\n"
                f"📋 <b>Market:</b> {question}\n"
                f"🎯 <b>Bet:</b> {direction}\n"
                f"📊 <b>Poly price:</b> {poly_price*100:.1f}¢\n"
                f"{signal_label}: {implied*100:.0f}¢\n"
                f"⚡ <b>Edge:</b> +{abs_edge*100:.1f}%\n"
                f"{kelly_str(abs_edge, poly_price, direction)}"
                f"🔗 <a href='{poly_link(m)}'>Bet on Polymarket</a>\n\n"
                f"<i>Signal only — not financial advice.</i>"
            )
            signals += 1
    return signals


# ─── WALLET TRACKER ───────────────────────────────────────────────────────────
WALLET_WIN_RATE_THRESHOLD = 0.70
WALLET_MIN_MARKETS        = 100
WALLET_EVAL_EVERY         = 20   # cycles between evaluation batches (~10 min at 30s interval)
WALLET_EVAL_BATCH         = 10   # addresses evaluated per batch cycle
WALLET_MIN_POSITION_SIZE  = 10   # ignore dust positions below $10

_wallet_candidates = set()   # seen in trades feed, not yet evaluated
_wallet_evaluated  = set()   # already evaluated (good or bad)

def fetch_recent_trades(limit=100):
    try:
        r = requests.get(f"{POLYMARKET_DATA_API}/trades",
            params={"limit": limit}, timeout=10)
        return r.json() if r.ok else []
    except Exception as e:
        print(f"  Wallet trades fetch error: {e}")
        return []

def fetch_wallet_positions(address, limit=500):
    try:
        r = requests.get(f"{POLYMARKET_DATA_API}/positions",
            params={"user": address, "limit": limit, "sizeThreshold": "0"},
            timeout=15)
        return r.json() if r.ok else []
    except Exception as e:
        print(f"  Wallet positions fetch error ({address[:10]}...): {e}")
        return []

def evaluate_wallet(address):
    """
    Compute win rate from resolved positions.
    Returns (win_rate, markets_resolved, total_pnl) or None if insufficient data.
    Resolved won  = redeemable AND curPrice >= 0.98
    Resolved lost = curPrice <= 0.02 AND endDate past
    """
    positions = fetch_wallet_positions(address, limit=500)
    if not positions:
        return None

    now = datetime.now(timezone.utc)
    won = lost = 0
    total_pnl = 0.0

    for p in positions:
        cur_price  = float(p.get("curPrice") or 0)
        redeemable = p.get("redeemable", False)
        pnl        = float(p.get("cashPnl") or p.get("realizedPnl") or 0)

        if redeemable and cur_price >= 0.98:
            won += 1
            total_pnl += pnl
        elif cur_price <= 0.02:
            end_str = p.get("endDate")
            end_dt = parse_aware_dt(end_str)
            if end_dt is not None and end_dt < now:
                lost += 1
                total_pnl += pnl

    total = won + lost
    if total < WALLET_MIN_MARKETS:
        return None
    return won / total, total, total_pnl

def run_wallet_scan(cycle):
    # ── Harvest addresses from the global trades feed ──
    trades = fetch_recent_trades(limit=100)
    added = 0
    for t in trades:
        addr = (t.get("proxyWallet") or "").lower()
        if addr and addr not in _wallet_evaluated and addr not in _wallet_candidates:
            _wallet_candidates.add(addr)
            added += 1
    if added:
        print(f"  Wallets: +{added} new candidates (pool={len(_wallet_candidates)})")

    # ── Evaluate a batch of candidates every N cycles ──
    if cycle % WALLET_EVAL_EVERY == 0 and _wallet_candidates:
        batch = list(_wallet_candidates)[:WALLET_EVAL_BATCH]
        for addr in batch:
            _wallet_candidates.discard(addr)
            _wallet_evaluated.add(addr)
            result = evaluate_wallet(addr)
            if result is None:
                continue
            win_rate, resolved, total_pnl = result
            if win_rate < WALLET_WIN_RATE_THRESHOLD:
                continue
            db_upsert_wallet(addr, win_rate, resolved, total_pnl)
            print(f"  ⭐ Sharp wallet: {addr[:10]}... {win_rate*100:.0f}% over {resolved} markets")
            send_telegram(
                f"⭐ <b>SHARP WALLET DETECTED</b>\n\n"
                f"🔑 <b>Address:</b> <code>{addr}</code>\n"
                f"📊 <b>Win rate:</b> {win_rate*100:.0f}%\n"
                f"🏆 <b>Resolved markets:</b> {resolved}\n"
                f"💰 <b>Total PnL:</b> ${total_pnl:+,.0f}\n"
                f"🔗 <a href='https://polymarket.com/profile/{addr}'>View profile</a>"
            )

    # ── Monitor tracked wallets for new open positions ──
    tracked = db_get_tracked_wallets()
    if not tracked:
        return

    now = datetime.now(timezone.utc)
    for wallet in tracked:
        addr     = wallet["address"]
        win_rate = float(wallet["win_rate"])
        positions = fetch_wallet_positions(addr, limit=50)

        for p in positions:
            size = float(p.get("size") or 0)
            if size < WALLET_MIN_POSITION_SIZE:
                continue

            # Skip expired markets
            end_str = p.get("endDate")
            if end_str:
                end_dt = parse_aware_dt(end_str)
                if end_dt is not None and end_dt < now:
                    continue

            if not db_log_wallet_position(addr, p):
                continue  # already alerted

            avg_price = float(p.get("avgPrice") or 0)
            title     = (p.get("title") or "")[:80]
            outcome   = p.get("outcome", "")
            slug      = p.get("slug", "")

            print(f"  👁 WALLET SIGNAL — {addr[:10]}... | {outcome} | {title[:40]}")
            send_telegram(
                f"👁 <b>SHARP WALLET SIGNAL</b>\n\n"
                f"🔑 <b>Wallet:</b> <code>{addr[:10]}...{addr[-4:]}</code> "
                f"({win_rate*100:.0f}% win rate)\n"
                f"📋 <b>Market:</b> {title}\n"
                f"🎯 <b>Bet:</b> {outcome}\n"
                f"📊 <b>Avg price:</b> {avg_price*100:.1f}¢\n"
                f"💰 <b>Position size:</b> ${size:,.0f}\n"
                f"🔗 <a href='https://polymarket.com/event/{slug}'>View market</a>\n\n"
                f"<i>Signal only — not financial advice.</i>"
            )


# ─── MAIN LOOP ────────────────────────────────────────────────────────────────
RESOLUTION_CHECK_EVERY = 10  # cycles (every 5 min at 30s interval)

def run_scan(cycle):
    print(f"\n[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] Scanning... (cycle #{cycle})")
    _odds_request_count[0] = 0

    implied = {}
    if ENABLE_CRYPTO_SIGNALS:
        for sym in CRYPTO_PAIRS:
            p = derive_implied_prob(fetch_klines(sym), None)
            if p:
                implied[sym] = p
        print(f"  Crypto implied: {implied}")
        print(f"  Crypto markets found: {len(fetch_polymarkets_crypto())}")
    if ENABLE_SPORTS_SIGNALS:
        print(f"  Sports markets found: {len(fetch_polymarkets_sports())}")
    if ENABLE_WEATHER_SIGNALS:
        print(f"  Weather markets found: {len(fetch_polymarkets_weather())}")
    print(f"  Vegas odds cached: {sum(len(v) for v in vegas_cache.values())} games across {len(vegas_cache)} leagues")

    cs = run_crypto_scan(implied) if ENABLE_CRYPTO_SIGNALS else 0
    ss = run_sports_scan()       if ENABLE_SPORTS_SIGNALS else 0
    ws = run_weather_scan()      if ENABLE_WEATHER_SIGNALS else 0
    run_wallet_scan(cycle)

    print(f"  Odds API requests this cycle: {_odds_request_count[0]}")
    if cs+ss+ws == 0:
        print("  No signals this cycle.")

    if cycle % RESOLUTION_CHECK_EVERY == 0:
        print("  Checking open signals for updates...")
        db_update_open_signals()

    if len(alerted) > 1000:
        alerted.clear()


if __name__ == "__main__":
    print("=" * 50)
    print("  DUB TRADING BOT")
    print(f"  Threshold: {EDGE_THRESHOLD*100:.0f}%  |  Interval: {POLL_INTERVAL}s")
    print(f"  DB: {'✅' if DATABASE_URL else '❌'}")
    print("=" * 50)

    db_init()
    db_init_wallets()
    db_load_alerted()

    send_telegram(
        f"⚡ <b>DUB Trading Bot started</b>\n\n"
        f"Version: {BOT_VERSION}\n"
        f"Scanning every {POLL_INTERVAL}s\n"
        f"Edge threshold: {EDGE_THRESHOLD*100:.0f}%\n"
        f"Crypto: {'✅' if ENABLE_CRYPTO_SIGNALS else '⏸'} ({', '.join(CRYPTO_PAIRS)})\n"
        f"Sports: {'✅' if ENABLE_SPORTS_SIGNALS and ODDS_API_KEYS else ('⏸' if not ENABLE_SPORTS_SIGNALS else '❌')} ({len(SPORTS_LEAGUES)} leagues, {len(ODDS_API_KEYS)} key(s))\n"
        f"Weather: {'✅' if ENABLE_WEATHER_SIGNALS else '⏸'}\n"
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
