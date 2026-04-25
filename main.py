import requests
import time
import math
import os
import json
from datetime import datetime

# ─── CONFIG ───────────────────────────────────────────────────────────────────
TG_TOKEN       = os.environ.get("TG_TOKEN")
TG_CHAT_ID     = os.environ.get("TG_CHAT_ID")
ODDS_API_KEY   = os.environ.get("ODDS_API_KEY", "")
EDGE_THRESHOLD = 0.06  # 6% minimum edge
POLL_INTERVAL  = 30    # seconds between scans
# ──────────────────────────────────────────────────────────────────────────────

GAMMA_API    = "https://gamma-api.polymarket.com"
BINANCE_API  = "https://api.binance.com/api/v3"
BINANCE_FAPI = "https://fapi.binance.com/fapi/v1"
ODDS_API     = "https://api.the-odds-api.com/v4"
NOAA_API     = "https://api.weather.gov"

alerted = set()


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

def fetch_klines(symbol):
    try:
        r = requests.get(f"{BINANCE_API}/klines",
            params={"symbol": f"{symbol}USDT", "interval": "1m", "limit": 15},
            timeout=10)
        return r.json() if r.ok else None
    except:
        return None

def fetch_funding(symbol):
    try:
        r = requests.get(f"{BINANCE_FAPI}/fundingRate",
            params={"symbol": f"{symbol}USDT", "limit": 1}, timeout=10)
        return r.json()[0]["fundingRate"] if r.ok else None
    except:
        return None

def fetch_polymarkets_crypto():
    try:
        r = requests.get(f"{GAMMA_API}/markets",
            params={"active": "true", "tag_slug": "crypto", "limit": 30,
                    "order": "volume", "ascending": "false"}, timeout=10)
        if not r.ok:
            return []
        data    = r.json()
        markets = data.get("markets", data) if isinstance(data, dict) else data
        assets  = ["btc", "bitcoin", "eth", "ethereum"]
        dirs    = ["up", "down", "higher", "above", "below"]
        return [m for m in markets if
                any(a in m.get("question","").lower() for a in assets) and
                any(d in m.get("question","").lower() for d in dirs)]
    except Exception as e:
        print(f"Polymarket crypto fetch error: {e}")
        return []

def run_crypto_scan(btc_prob, eth_prob):
    markets = fetch_polymarkets_crypto()
    signals = 0
    for m in markets:
        question = m.get("question", "")
        slug     = m.get("slug", "")
        poly_price = 0.5
        try:
            outcome_str = m.get("outcomePrices") or m.get("outcomes_prices")
            parsed = json.loads(outcome_str) if isinstance(outcome_str, str) else outcome_str
            poly_price = float(parsed[0])
        except:
            pass

        is_btc  = "btc" in question.lower() or "bitcoin" in question.lower()
        implied = btc_prob if is_btc else eth_prob
        if implied is None:
            continue

        edge     = implied - poly_price
        abs_edge = abs(edge)
        if abs_edge < EDGE_THRESHOLD:
            continue

        direction = "YES" if edge > 0 else "NO"
        key = f"crypto:{slug}:{direction}:{round(poly_price,2)}"
        if key in alerted:
            continue
        alerted.add(key)

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
            f"💧 <b>24h volume:</b> {vol_str}\n\n"
            f"🔗 <a href='https://polymarket.com/event/{m.get('slug','')}'}>Bet on Polymarket</a>\n\n"
            f"<i>Signal only — not financial advice.</i>"
        )
        signals += 1
    return signals


# ─── SPORTS SIGNAL ────────────────────────────────────────────────────────────
def moneyline_to_prob(american):
    if american > 0:
        return 100 / (american + 100)
    else:
        return abs(american) / (abs(american) + 100)

def fetch_vegas_odds():
    if not ODDS_API_KEY:
        return []
    try:
        r = requests.get(f"{ODDS_API}/sports/basketball_nba/odds",
            params={"apiKey": ODDS_API_KEY, "regions": "us",
                    "markets": "h2h", "oddsFormat": "american"},
            timeout=10)
        return r.json() if r.ok else []
    except:
        return []

def fetch_polymarkets_sports():
    try:
        r = requests.get(f"{GAMMA_API}/markets",
            params={"active": "true", "tag_slug": "sports", "limit": 50,
                    "order": "volume", "ascending": "false"}, timeout=10)
        if not r.ok:
            return []
        data    = r.json()
        markets = data.get("markets", data) if isinstance(data, dict) else data
        return [m for m in markets if m.get("question")]
    except:
        return []

def run_sports_scan():
    vegas_games = fetch_vegas_odds()
    poly_sports = fetch_polymarkets_sports()
    if not vegas_games or not poly_sports:
        return 0

    signals = 0
    for game in vegas_games:
        home = game.get("home_team", "")
        away = game.get("away_team", "")
        if not home or not away:
            continue

        best_home_odds = None
        for bookmaker in game.get("bookmakers", []):
            for market in bookmaker.get("markets", []):
                if market.get("key") != "h2h":
                    continue
                for outcome in market.get("outcomes", []):
                    if outcome["name"] == home:
                        if best_home_odds is None or outcome["price"] > best_home_odds:
                            best_home_odds = outcome["price"]

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

            edge     = vegas_prob - poly_price
            abs_edge = abs(edge)
            if abs_edge < EDGE_THRESHOLD:
                continue

            direction = "YES" if edge > 0 else "NO"
            key = f"sports:{m.get('slug','')}:{direction}:{round(poly_price,2)}"
            if key in alerted:
                continue
            alerted.add(key)

            emoji = "🟢" if direction == "YES" else "🔴"
            print(f"  ⚡ SPORTS — {question[:50]} | {direction} | +{abs_edge*100:.1f}%")
            send_telegram(
                f"{emoji} <b>SPORTS SIGNAL</b>\n\n"
                f"📋 <b>Market:</b> {question}\n"
                f"🎯 <b>Bet:</b> {direction}\n"
                f"📊 <b>Poly price:</b> {poly_price*100:.1f}¢\n"
                f"🏀 <b>Vegas implied:</b> {vegas_prob*100:.1f}¢\n"
                f"⚡ <b>Edge:</b> +{abs_edge*100:.1f}%\n\n"
                f"🔗 <a href='https://polymarket.com/event/{m.get('slug','')}'}>Bet on Polymarket</a>\n\n"
                f"<i>Signal only — not financial advice.</i>"
            )
            signals += 1
    return signals


# ─── WEATHER SIGNAL ───────────────────────────────────────────────────────────
WEATHER_CITIES = [
    {"name": "New York",    "office": "OKX", "gridX": 33,  "gridY": 37},
    {"name": "Los Angeles", "office": "LOX", "gridX": 149, "gridY": 48},
    {"name": "Chicago",     "office": "LOT", "gridX": 76,  "gridY": 73},
    {"name": "Houston",     "office": "HGX", "gridX": 66,  "gridY": 98},
    {"name": "Miami",       "office": "MFL", "gridX": 110, "gridY": 38},
]

def fetch_noaa_forecast(office, grid_x, grid_y):
    try:
        r = requests.get(
            f"{NOAA_API}/gridpoints/{office}/{grid_x},{grid_y}/forecast",
            headers={"User-Agent": "PolySignalBot/1.0"},
            timeout=10
        )
        if not r.ok:
            return None
        periods = r.json().get("properties", {}).get("periods", [])
        return periods[:4] if periods else None
    except:
        return None

def parse_rain_prob(period):
    prob = period.get("probabilityOfPrecipitation", {})
    if prob and prob.get("value") is not None:
        return prob["value"] / 100.0
    text = period.get("detailedForecast", "").lower()
    for phrase, val in [("slight chance", 0.2), ("chance of rain", 0.4),
                        ("likely", 0.65), ("definitely", 0.85)]:
        if phrase in text:
            return val
    return None

def fetch_polymarkets_weather():
    try:
        r = requests.get(f"{GAMMA_API}/markets",
            params={"active": "true", "limit": 50, "order": "volume",
                    "ascending": "false"}, timeout=10)
        if not r.ok:
            return []
        data    = r.json()
        markets = data.get("markets", data) if isinstance(data, dict) else data
        weather_words = ["rain", "temperature", "snow", "storm", "weather",
                         "degrees", "fahrenheit", "celsius", "precipitation"]
        return [m for m in markets if m.get("question") and
                any(w in m.get("question","").lower() for w in weather_words)]
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
                question = m.get("question", "")
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

                edge     = rain_prob - poly_price
                abs_edge = abs(edge)
                if abs_edge < EDGE_THRESHOLD:
                    continue

                direction = "YES" if edge > 0 else "NO"
                key = f"weather:{m.get('slug','')}:{direction}:{round(poly_price,2)}"
                if key in alerted:
                    continue
                alerted.add(key)

                emoji = "🟢" if direction == "YES" else "🔴"
                print(f"  ⚡ WEATHER — {question[:50]} | {direction} | +{abs_edge*100:.1f}%")
                send_telegram(
                    f"{emoji} <b>WEATHER SIGNAL</b>\n\n"
                    f"📋 <b>Market:</b> {question}\n"
                    f"🎯 <b>Bet:</b> {direction}\n"
                    f"📊 <b>Poly price:</b> {poly_price*100:.1f}¢\n"
                    f"🌧 <b>NOAA forecast:</b> {rain_prob*100:.0f}% chance\n"
                    f"⚡ <b>Edge:</b> +{abs_edge*100:.1f}%\n\n"
                    f"🔗 <a href='https://polymarket.com/event/{m.get('slug','')}'}>Bet on Polymarket</a>\n\n"
                    f"<i>Signal only — not financial advice.</i>"
                )
                signals += 1
    return signals


# ─── MAIN LOOP ────────────────────────────────────────────────────────────────
def run_scan():
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Scanning...")

    btc_prob = derive_implied_prob(fetch_klines("BTC"), fetch_funding("BTC"))
    eth_prob = derive_implied_prob(fetch_klines("ETH"), fetch_funding("ETH"))
    if btc_prob and eth_prob:
        print(f"  BTC: {btc_prob*100:.1f}%  ETH: {eth_prob*100:.1f}%")

    crypto_signals  = run_crypto_scan(btc_prob, eth_prob)
    sports_signals  = run_sports_scan()
    weather_signals = run_weather_scan()

    total = crypto_signals + sports_signals + weather_signals
    if total == 0:
        print("  No signals this cycle.")

    if len(alerted) > 500:
        alerted.clear()


if __name__ == "__main__":
    print("=" * 50)
    print("  POLY SIGNAL ENGINE")
    print(f"  Threshold: {EDGE_THRESHOLD*100:.0f}%  |  Interval: {POLL_INTERVAL}s")
    print(f"  Sports: {'ON' if ODDS_API_KEY else 'OFF'}")
    print("=" * 50)

    send_telegram(
        f"⚡ <b>Poly Signal Engine started</b>\n\n"
        f"Scanning every {POLL_INTERVAL}s\n"
        f"Edge threshold: {EDGE_THRESHOLD*100:.0f}%\n"
        f"Crypto: ✅\n"
        f"Sports (NBA): {'✅' if ODDS_API_KEY else '❌'}\n"
        f"Weather (NOAA): ✅"
    )

    while True:
        try:
            run_scan()
        except Exception as e:
            print(f"Error: {e}")
            send_telegram(f"⚠️ <b>Error:</b> {e}")
        time.sleep(POLL_INTERVAL)
