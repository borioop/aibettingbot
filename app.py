import os
import requests
from flask import Flask, render_template, abort, jsonify, request, session, redirect, url_for
from datetime import datetime, timedelta
from functools import wraps
import json
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
import queue

# === API KEYS ===
API_FOOTBALL_KEY = "77e4c25d5460c378f6331d7d33e74482"
API_BASE_URL = "https://v3.football.api-sports.io"
HEADERS = {"x-apisports-key": API_FOOTBALL_KEY}

# === BET IDs ===
GOALS_BET_ID = 5
DOUBLE_CHANCE_BET_ID = 12
MATCH_WINNER_BET_ID = 1

app = Flask(__name__, static_folder='static', template_folder='templates')
app.secret_key = os.environ.get("SECRET_KEY", "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")

# === PROGRESSIVE CACHE SYSTEM ===
master_cache = {}  # {date: {'upcoming': [...], 'finished': [...], 'ready': True/False}}
cache_lock = threading.Lock()
cache_status = {}  # Track cache building progress

# === SUPPORTING CACHES ===
predictions_cache = {}
odds_cache = {}
fixtures_raw_cache = {}
CACHE_DURATION = 7200

# === RATE LIMITING ===
request_times = []
rate_limit_lock = threading.Lock()
MAX_REQUESTS_PER_SECOND = 6
RETRY_DELAY = 3

executor = ThreadPoolExecutor(max_workers=4)

DOMESTIC_LEAGUES = {
    "England": 39, "Spain": 140, "Germany": 78, "Italy": 135, "France": 61,
    "Portugal": 94, "Netherlands": 88, "Belgium": 144, "Turkey": 203, "Scotland": 179,
    "Austria": 218, "Switzerland": 207, "Greece": 197, "Poland": 106, "Ukraine": 333,
    "Russia": 235, "Brazil": 71, "Argentina": 128, "Mexico": 262, "USA": 253,
    "Japan": 98, "China": 169, "Saudi Arabia": 307
}

def wait_for_rate_limit():
    """Smart rate limiting with cleanup"""
    with rate_limit_lock:
        now = time.time()
        # Clean old requests
        while request_times and now - request_times[0] > 1.0:
            request_times.pop(0)
        
        # Wait if at limit
        if len(request_times) >= MAX_REQUESTS_PER_SECOND:
            sleep_time = 1.0 - (now - request_times[0])
            if sleep_time > 0:
                time.sleep(sleep_time)
                now = time.time()
                while request_times and now - request_times[0] > 1.0:
                    request_times.pop(0)
        
        request_times.append(now)

def get_cached_data(cache_dict, key, duration):
    """Get from cache if fresh"""
    if key in cache_dict:
        data, timestamp = cache_dict[key]
        if time.time() - timestamp < duration:
            return data
    return None

def set_cached_data(cache_dict, key, data):
    """Save to cache"""
    cache_dict[key] = (data, time.time())

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login_page'))
        return f(*args, **kwargs)
    return decorated_function

def api_get(path, params=None, max_retries=3):
    """API call with retry and rate limiting"""
    for attempt in range(max_retries):
        try:
            wait_for_rate_limit()
            r = requests.get(API_BASE_URL + path, headers=HEADERS, params=(params or {}), timeout=15)
            
            if r.status_code == 200:
                return r.json().get("response", None)
            elif r.status_code == 429:
                if attempt < max_retries - 1:
                    time.sleep(RETRY_DELAY * (attempt + 1))
                    continue
            else:
                app.logger.error(f"API {path} -> {r.status_code}")
                return None
        except Exception as e:
            app.logger.error(f"API error {path}: {e}")
            if attempt < max_retries - 1:
                time.sleep(1)
                continue
    return None

# === API WRAPPERS ===
def get_fixtures_by_date(date):
    cache_key = f"raw_{date}"
    cached = get_cached_data(fixtures_raw_cache, cache_key, 3600)
    if cached:
        return cached
    result = api_get("/fixtures", {"date": date})
    if result:
        set_cached_data(fixtures_raw_cache, cache_key, result)
    return result

def get_predictions(fixture_id):
    cache_key = f"pred_{fixture_id}"
    cached = get_cached_data(predictions_cache, cache_key, CACHE_DURATION)
    if cached:
        return cached
    result = api_get("/predictions", {"fixture": fixture_id})
    if result:
        set_cached_data(predictions_cache, cache_key, result)
    return result

def get_odds(fixture_id):
    return api_get("/odds", {"fixture": fixture_id})

def get_odds_by_bet(fixture_id, bet_id):
    cache_key = f"odds_{fixture_id}_{bet_id}"
    cached = get_cached_data(odds_cache, cache_key, CACHE_DURATION)
    if cached:
        return cached
    result = api_get("/odds", {"fixture": fixture_id, "bet": bet_id})
    if result:
        set_cached_data(odds_cache, cache_key, result)
    return result

def get_fixture(fixture_id): 
    return api_get("/fixtures", {"id": fixture_id})

def get_h2h(home_id, away_id): 
    return api_get("/fixtures/headtohead", {"h2h": f"{home_id}-{away_id}"})

def get_standings(league_id, season): 
    return api_get("/standings", {"league": league_id, "season": season})

def get_lineups(fixture_id): 
    return api_get("/fixtures/lineups", {"fixture": fixture_id})

def get_team_fixtures_by_league(team_id, league_id, season): 
    return api_get("/fixtures", {"team": team_id, "league": league_id, "season": season})

def get_team_info(team_id): 
    return api_get("/teams", {"id": team_id})

def get_team_recent_fixtures(team_id, limit=10): 
    return api_get("/fixtures", {"team": team_id, "last": limit})

def get_team_fixtures(team_id, season): 
    return api_get("/fixtures", {"team": team_id, "season": season})

# === ODDS CALCULATION ===
def get_advice_odd(fixture, advice):
    """Calculate odds for advice"""
    if not advice or not isinstance(advice, str):
        return None

    fixture_id = fixture.get("fixture", {}).get("id")
    home_name = fixture.get("teams", {}).get("home", {}).get("name", "").strip()
    away_name = fixture.get("teams", {}).get("away", {}).get("name", "").strip()
    advice_lower = advice.lower()

    # Combo Bet
    if "combo double chance" in advice_lower:
        m = re.search(r'combo double chance\s*:\s*(.+?)\s+and\s+([+-]\d+(\.\d+)?)\s*goals', advice_lower)
        if not m:
            return None

        dc_part = m.group(1).strip()
        goal_value = m.group(2).strip()
        
        # Determine double chance option
        dc_option = None
        if home_name.lower() in dc_part and 'draw' in dc_part:
            dc_option = "home/draw"
        elif away_name.lower() in dc_part and 'draw' in dc_part:
            dc_option = "draw/away"
        elif home_name.lower() in dc_part and away_name.lower() in dc_part:
            dc_option = "home/away"
        else:
            return None

        # Determine goals option
        if goal_value.startswith('-'):
            goal_option = "under " + goal_value.lstrip('-')
        elif goal_value.startswith('+'):
            goal_option = "over " + goal_value.lstrip('+')
        else:
            return None

        # Get Double Chance odds
        dc_odds_raw = get_odds_by_bet(fixture_id, DOUBLE_CHANCE_BET_ID)
        dc_odd = None
        if dc_odds_raw:
            for market in dc_odds_raw:
                for bookmaker in market.get("bookmakers", []):
                    for bet in bookmaker.get("bets", []):
                        if bet.get("id") == DOUBLE_CHANCE_BET_ID:
                            for opt in bet.get("values", []):
                                if opt.get("value", "").strip().lower() == dc_option:
                                    dc_odd = float(opt.get("odd"))
                                    break
                        if dc_odd: break
                if dc_odd: break

        # Get Goals odds
        goals_odds_raw = get_odds_by_bet(fixture_id, GOALS_BET_ID)
        goals_odd = None
        if goals_odds_raw:
            for market in goals_odds_raw:
                for bookmaker in market.get("bookmakers", []):
                    for bet in bookmaker.get("bets", []):
                        if bet.get("id") == GOALS_BET_ID:
                            for opt in bet.get("values", []):
                                if goal_option in opt.get("value", "").strip().lower():
                                    goals_odd = float(opt.get("odd"))
                                    break
                        if goals_odd: break
                if goals_odd: break

        if dc_odd and goals_odd:
            return dc_odd * goals_odd
        return None

    # Simple Bets
    bet_id = None
    option = ""

    if "double chance" in advice_lower:
        bet_id = DOUBLE_CHANCE_BET_ID
        if home_name.lower() in advice_lower and "draw" in advice_lower:
            option = "home/draw"
        elif away_name.lower() in advice_lower and "draw" in advice_lower:
            option = "draw/away"
        elif home_name.lower() in advice_lower and away_name.lower() in advice_lower:
            option = "home/away"

    elif "winner" in advice_lower:
        bet_id = MATCH_WINNER_BET_ID
        if home_name.lower() in advice_lower:
            option = "home"
        elif away_name.lower() in advice_lower:
            option = "away"

    if bet_id and option:
        odds_raw = get_odds_by_bet(fixture_id, bet_id)
        if odds_raw:
            for market in odds_raw:
                for bookmaker in market.get("bookmakers", []):
                    for bet in bookmaker.get("bets", []):
                        if bet.get("id") == bet_id:
                            for opt in bet.get("values", []):
                                if opt.get("value", "").strip().lower() == option:
                                    try:
                                        return float(opt.get("odd"))
                                    except:
                                        return None
    return None

def check_prediction_result(fixture, advice):
    """Check if prediction won"""
    if not advice:
        return None
        
    goals = fixture.get("goals", {})
    home_goals = goals.get("home")
    away_goals = goals.get("away")
    
    if home_goals is None or away_goals is None:
        return None
    
    home_name = fixture.get("teams", {}).get("home", {}).get("name", "").strip().lower()
    away_name = fixture.get("teams", {}).get("away", {}).get("name", "").strip().lower()
    advice_lower = advice.lower()
    
    if home_goals > away_goals:
        actual = "home"
    elif away_goals > home_goals:
        actual = "away"
    else:
        actual = "draw"
    
    # Winner
    if "winner" in advice_lower:
        if home_name in advice_lower:
            return actual == "home"
        elif away_name in advice_lower:
            return actual == "away"
    
    # Double Chance
    if "double chance" in advice_lower:
        if home_name in advice_lower and "draw" in advice_lower:
            return actual in ["home", "draw"]
        elif away_name in advice_lower and "draw" in advice_lower:
            return actual in ["away", "draw"]
        elif home_name in advice_lower and away_name in advice_lower:
            return actual in ["home", "away"]
    
    # Combo
    if "combo" in advice_lower:
        dc_ok = False
        if home_name in advice_lower and "draw" in advice_lower:
            dc_ok = actual in ["home", "draw"]
        elif away_name in advice_lower and "draw" in advice_lower:
            dc_ok = actual in ["away", "draw"]
        elif home_name in advice_lower and away_name in advice_lower:
            dc_ok = actual in ["home", "away"]
        
        goals_match = re.search(r'([+-]\d+(\.\d+)?)\s*goals', advice_lower)
        goals_ok = False
        
        if goals_match:
            goal_value = goals_match.group(1)
            total = home_goals + away_goals
            
            if goal_value.startswith('-'):
                threshold = float(goal_value.lstrip('-'))
                goals_ok = total < threshold
            elif goal_value.startswith('+'):
                threshold = float(goal_value.lstrip('+'))
                goals_ok = total > threshold
        
        return dc_ok and goals_ok
    
    return None

# === PROCESS SINGLE FIXTURE ===
def process_fixture(fixture):
    """Process one fixture - get prediction and odds"""
    try:
        fixture_id = fixture.get("fixture", {}).get("id")
        if not fixture_id:
            return None

        status = fixture.get("fixture", {}).get("status", {}).get("short", "")
        
        # Get prediction
        pred_raw = get_predictions(fixture_id)
        if not pred_raw:
            return None

        advice = None
        if isinstance(pred_raw, list) and len(pred_raw) > 0:
            advice = pred_raw[0].get("predictions", {}).get("advice")

        if not advice or advice == "—":
            return None

        # Get odds
        advice_odd = get_advice_odd(fixture, advice)
        if not advice_odd:
            return None

        # Add data to fixture
        fixture["advice"] = advice
        fixture["advice_odd"] = advice_odd

        # Check result if finished
        if status in ["FT", "AET", "PEN"]:
            fixture["prediction_won"] = check_prediction_result(fixture, advice)
        else:
            fixture["prediction_won"] = None

        return fixture

    except Exception as e:
        app.logger.error(f"Error processing {fixture_id}: {e}")
        return None

# === BACKGROUND CACHE BUILDER ===
def build_cache_for_date(date_str):
    """Build cache for one date"""
    try:
        app.logger.info(f"🔨 Building cache: {date_str}")
        
        # Update status
        with cache_lock:
            cache_status[date_str] = {'status': 'building', 'progress': 0, 'total': 0}

        # Get all fixtures
        all_fixtures = get_fixtures_by_date(date_str)
        if not all_fixtures:
            with cache_lock:
                cache_status[date_str] = {'status': 'empty', 'progress': 0, 'total': 0}
                master_cache[date_str] = {'upcoming': [], 'finished': [], 'ready': True}
            return

        total = len(all_fixtures)
        with cache_lock:
            cache_status[date_str]['total'] = total

        app.logger.info(f"📊 Processing {total} fixtures for {date_str}")

        # Process in parallel
        futures = {executor.submit(process_fixture, f): f for f in all_fixtures}
        
        upcoming = []
        finished = []
        processed = 0

        for future in as_completed(futures):
            processed += 1
            
            # Update progress
            with cache_lock:
                cache_status[date_str]['progress'] = processed

            if processed % max(1, total // 5) == 0:
                app.logger.info(f"  ⏳ {date_str}: {processed}/{total} ({int(processed/total*100)}%)")

            result = future.result()
            if result:
                status = result.get("fixture", {}).get("status", {}).get("short", "")
                if status in ["FT", "AET", "PEN"]:
                    finished.append(result)
                else:
                    upcoming.append(result)

        # Save to cache
        with cache_lock:
            master_cache[date_str] = {
                'upcoming': upcoming,
                'finished': finished,
                'ready': True,
                'timestamp': time.time()
            }
            cache_status[date_str] = {
                'status': 'ready',
                'progress': processed,
                'total': total,
                'upcoming_count': len(upcoming),
                'finished_count': len(finished)
            }

        app.logger.info(f"✅ {date_str}: {len(upcoming)} upcoming, {len(finished)} finished")

    except Exception as e:
        app.logger.error(f"Error building cache for {date_str}: {e}")
        with cache_lock:
            cache_status[date_str] = {'status': 'error', 'error': str(e)}

def cache_builder_worker():
    """Background worker - builds cache progressively"""
    while True:
        try:
            # Dates to cache
            dates = []
            for offset in range(-2, 3):  # -2, -1, 0, 1, 2
                date = datetime.now() + timedelta(days=offset)
                dates.append(date.strftime('%Y-%m-%d'))

            for date_str in dates:
                app.logger.info(f"📅 Updating cache: {date_str}")
                build_cache_for_date(date_str)
                time.sleep(3)  # Small delay between dates

            app.logger.info("✅ Cache cycle completed. Sleeping 30 minutes...")
            time.sleep(1800)  # 30 minutes

        except Exception as e:
            app.logger.error(f"Cache builder error: {e}")
            time.sleep(300)  # 5 min on error

def start_cache_builder():
    """Start background cache builder"""
    thread = threading.Thread(target=cache_builder_worker, daemon=True)
    thread.start()
    app.logger.info("🔄 Cache builder started")

# === HELPER FUNCTIONS ===
def clamp(v):
    try: 
        v = int(float(v))
    except: 
        v = 0
    return max(0, min(100, v))

def extract_team_position_from_standings(standings_list, team_name_or_id):
    if not standings_list: 
        return None
    for r in standings_list:
        if 'id' in r and r['id'] == team_name_or_id: 
            return r.get('rank')
        if r.get('name') and team_name_or_id and r.get('name').lower() == str(team_name_or_id).lower(): 
            return r.get('rank')
    return None

def detect_zone(description):
    if not description: 
        return None
    d = description.lower()
    if "relegation" in d: 
        return "relegation"
    if "champions" in d or ("promotion" in d and "champions" in d): 
        return "promotion"
    if "europa" in d or "conference" in d or "uefa" in d: 
        return "europe"
    if "promotion" in d: 
        return "promotion"
    return None

def calculate_form_from_matches(team_id, league_id, season, limit=5, current_fixture_date=None):
    try:
        fixtures = get_team_fixtures_by_league(team_id, league_id, season)
        if not fixtures: 
            return ["?"] * 6
        finished = []
        for match in fixtures:
            status = match.get("fixture", {}).get("status", {}).get("short", "")
            date = match.get("fixture", {}).get("date", "")
            if current_fixture_date and date >= current_fixture_date: 
                continue
            if status in ["FT", "AET", "PEN"]:
                home_goals = match.get("goals", {}).get("home")
                away_goals = match.get("goals", {}).get("away")
                if home_goals is None or away_goals is None: 
                    continue
                is_home = (match.get("teams", {}).get("home", {}).get("id") == team_id)
                if home_goals == away_goals: 
                    result = "D"
                elif (is_home and home_goals > away_goals) or (not is_home and away_goals > home_goals): 
                    result = "W"
                else: 
                    result = "L"
                finished.append({"date": date, "result": result})
        finished.sort(key=lambda x: x["date"], reverse=True)
        recent_5 = finished[:5][::-1] if len(finished) >= 5 else finished[::-1]
        form_list = [m["result"] for m in recent_5]
        while len(form_list) < 5: 
            form_list.insert(0, "?")
        form_list.append("?")
        return form_list
    except Exception as e:
        app.logger.exception("Error calculating form: %s", e)
        return ["?"] * 6

def prepare_comparison_single(fixture_raw):
    """Prepare match details"""
    fixture = fixture_raw.get("fixture", {})
    league = fixture_raw.get("league", {})
    teams = fixture_raw.get("teams", {})
    home = teams.get("home", {})
    away = teams.get("away", {})
    goals = fixture_raw.get("goals", {})

    fixture_id = fixture.get("id")
    league_id = league.get("id")
    season = league.get("season")
    home_id = home.get("id")
    away_id = away.get("id")

    home_logo = home.get("logo") or home.get("image") or None
    away_logo = away.get("logo") or away.get("image") or None

    date_iso = fixture.get("date")
    try:
        dt = datetime.fromisoformat(date_iso.replace("Z","+00:00"))
        date_fmt = dt.strftime("%Y-%m-%d %H:%M UTC")
    except:
        date_fmt = date_iso

    status_info = {
        "short": fixture.get("status", {}).get("short"),
        "elapsed": fixture.get("status", {}).get("elapsed"),
        "home_goals": goals.get("home"),
        "away_goals": goals.get("away")
    }

    venue_name = fixture.get("venue", {}).get("name") or None
    venue_city = fixture.get("venue", {}).get("city") or None
    venue_display = f"{venue_name} ({venue_city})" if venue_name and venue_city else venue_name or venue_city

    pred_raw = get_predictions(fixture_id)

    advice = "—"
    percent = {}
    if isinstance(pred_raw, list) and len(pred_raw) > 0:
        p = pred_raw[0].get("predictions", {})
        advice = p.get("advice", "—")
        percent = p.get("percent", {}) or {}
    elif isinstance(pred_raw, dict):
        p = pred_raw.get("predictions", {}) or {}
        advice = p.get("advice", "—")
        percent = p.get("percent", {}) or {}

    advice_odd_val = get_advice_odd(fixture_raw, advice)

    advice_short = advice
    if ":" in advice:
        try:
            advice_short = advice.split(":", 1)[1].strip()
        except:
            advice_short = advice

    try:
        home_pct = clamp(str(percent.get("home", "0")).replace("%",""))
        away_pct = clamp(str(percent.get("away", "0")).replace("%",""))
    except:
        home_pct=away_pct=0

    radar = {
        "labels": ["Strength", "Attacking", "Defensive", "Wins", "GoalsFor"],
        "home": [clamp(home_pct), clamp(home_pct - 5), clamp(home_pct + 5), clamp(home_pct + 10), clamp(home_pct)],
        "away": [clamp(away_pct), clamp(away_pct + 5), clamp(away_pct - 5), clamp(away_pct - 10), clamp(away_pct)],
    }

    bars = [
        {"label": "Strength", "home": radar["home"][0], "away": radar["away"][0]},
        {"label": "Attacking Potential", "home": radar["home"][1], "away": radar["away"][1]},
        {"label": "Defensive Potential", "home": radar["home"][2], "away": radar["away"][2]},
        {"label": "Poisson Distribution (approx.)", "home": max(0, home_pct+10), "away": max(0, away_pct+10)},
        {"label": "Strength H2H", "home": radar["home"][3], "away": radar["away"][3]},
        {"label": "Goals H2H", "home": radar["home"][4], "away": radar["away"][4]},
        {"label": "Win Probability", "home": home_pct, "away": away_pct, "show_for_logged_in": True},
    ]

    h2h_raw = get_h2h(home.get("id"), away.get("id")) or []
    h2h_list = []
    for m in h2h_raw:
        try:
            status_short_h2h = (m.get("fixture", {}).get("status", {}).get("short") or "").upper()
            if status_short_h2h not in ("FT","AET","PEN"): continue
            g = m.get("goals", {}) or {}; gh = g.get("home"); ga = g.get("away")
            if gh is None or ga is None: 
                score = m.get("score", {}).get("fulltime", {}) or {}
                gh = score.get("home"); ga = score.get("away")
            winner = None
            if gh is not None and ga is not None:
                if gh > ga: winner = "home"
                elif ga > gh: winner = "away"
            h2h_list.append({
                "fixture_id": m.get("fixture", {}).get("id"), 
                "date": (m.get("fixture", {}).get("date") or "")[:10],
                "home_name": m.get("teams", {}).get("home", {}).get("name"), 
                "away_name": m.get("teams", {}).get("away", {}).get("name"),
                "home_logo": m.get("teams", {}).get("home", {}).get("logo"), 
                "away_logo": m.get("teams", {}).get("away", {}).get("logo"),
                "home_goals": gh, "away_goals": ga, "winner": winner, 
                "raw_date": m.get("fixture", {}).get("date"),
                "league": m.get("league", {}).get("name") or ""
            })
        except Exception: 
            continue

    h2h_list = sorted(h2h_list, key=lambda x: x.get("raw_date") or "", reverse=True)[:8]

    standings = None
    if league_id and season:
        standings_raw = get_standings(league_id, season)
        if standings_raw and isinstance(standings_raw, list) and len(standings_raw) > 0:
            try:
                first = standings_raw[0].get("league", {})
                rows = first.get("standings", [])
                if rows and isinstance(rows, list) and len(rows) > 0:
                    flat = rows[0]
                    standings = []
                    for r in flat:
                        team_id = r.get("team", {}).get("id")
                        standings.append({
                            "rank": r.get("rank"), "id": team_id, 
                            "name": r.get("team", {}).get("name"),
                            "logo": r.get("team", {}).get("logo"), 
                            "played": r.get("all", {}).get("played"),
                            "win": r.get("all", {}).get("win"), 
                            "draw": r.get("all", {}).get("draw"),
                            "loss": r.get("all", {}).get("lose"), 
                            "goals_for": r.get("all", {}).get("goals", {}).get("for"),
                            "goals_against": r.get("all", {}).get("goals", {}).get("against"), 
                            "points": r.get("points"),
                            "zone": detect_zone(r.get("description") or r.get("group")), 
                            "highlight": team_id in (home_id, away_id)
                        })
            except Exception as e:
                app.logger.exception("Parsing standings error: %s", e)

    home_position = None
    away_position = None
    if standings:
        home_position = extract_team_position_from_standings(standings, home.get("id")) or extract_team_position_from_standings(standings, home.get("name"))
        away_position = extract_team_position_from_standings(standings, away.get("id")) or extract_team_position_from_standings(standings, away.get("name"))

    home_form_list = calculate_form_from_matches(home_id, league_id, season, limit=5)
    away_form_list = calculate_form_from_matches(away_id, league_id, season, limit=5)

    lineups_raw = get_lineups(fixture_id) or []
    lineups = {"home": None, "away": None}
    if isinstance(lineups_raw, list) and len(lineups_raw) > 0:
        for entry in lineups_raw:
            try:
                if entry.get("team", {}).get("id") == home.get("id"): 
                    lineups["home"] = entry
                elif entry.get("team", {}).get("id") == away.get("id"): 
                    lineups["away"] = entry
            except: 
                continue

    def prepare_recent_for_team(team_id):
        recent_raw = get_team_recent_fixtures(team_id, limit=15) or []
        lst = []
        for m in recent_raw:
            try:
                status_short_rec = (m.get("fixture", {}).get("status", {}).get("short") or "").upper()
                if status_short_rec not in ("FT","AET","PEN"): 
                    continue
                g = m.get("goals", {}) or {}
                gh = g.get("home")
                ga = g.get("away")
                if gh is None or ga is None: 
                    score = m.get("score", {}).get("fulltime", {}) or {}
                    gh = score.get("home")
                    ga = score.get("away")
                home_team_recent = m.get("teams", {}).get("home", {})
                away_team_recent = m.get("teams", {}).get("away", {})
                is_home = (home_team_recent.get("id") == team_id)
                winner = None
                if gh is not None and ga is not None:
                    if gh > ga: 
                        winner = "home"
                    elif ga > gh: 
                        winner = "away"
                lst.append({
                    "fixture_id": m.get("fixture", {}).get("id"), 
                    "date": (m.get("fixture", {}).get("date") or "")[:10],
                    "league": m.get("league", {}).get("name") or "",
                    "opponent_name": away_team_recent.get("name") if is_home else home_team_recent.get("name"),
                    "opponent_logo": away_team_recent.get("logo") if is_home else home_team_recent.get("logo"),
                    "is_home": is_home, "home_goals": gh, "away_goals": ga, 
                    "winner": winner, "raw_date": m.get("fixture", {}).get("date")
                })
            except: 
                continue
        return sorted(lst, key=lambda x: x.get("raw_date") or "", reverse=True)[:8]

    home_recent_all = prepare_recent_for_team(home.get("id"))
    away_recent_all = prepare_recent_for_team(away.get("id"))
    home_recent = [r for r in home_recent_all if r.get("is_home")][:8]
    away_recent = [r for r in away_recent_all if not r.get("is_home")][:8]

    odds = {"home": "—", "draw": "—", "away": "—", "home_won": False, "draw_won": False, "away_won": False}
    try:
        odds_raw = get_odds(fixture_id)
        if odds_raw and isinstance(odds_raw, list) and len(odds_raw) > 0:
            for odds_entry in odds_raw:
                for bookmaker in odds_entry.get('bookmakers', []):
                    for bet in bookmaker.get('bets', []):
                        if bet.get('name', '') in ['Match Winner', 'Home/Away']:
                            for v in bet.get('values', []):
                                if v.get('value') == 'Home': 
                                    odds['home'] = v.get('odd', "—")
                                elif v.get('value') == 'Draw': 
                                    odds['draw'] = v.get('odd', "—")
                                elif v.get('value') == 'Away': 
                                    odds['away'] = v.get('odd', "—")
                            if odds['home'] != "—": 
                                break
                    if odds['home'] != "—": 
                        break
                if odds['home'] != "—": 
                    break
        if status_info['short'] in ['FT', 'AET', 'PEN']:
            if status_info['home_goals'] is not None and status_info['away_goals'] is not None:
                if status_info['home_goals'] > status_info['away_goals']: 
                    odds['home_won'] = True
                elif status_info['away_goals'] > status_info['home_goals']: 
                    odds['away_won'] = True
                else: 
                    odds['draw_won'] = True
    except Exception as e: 
        app.logger.error("Error fetching odds: %s", e)

    return {
        "fixture_id": fixture_id, 
        "league": {"name": league.get("name"), "id": league_id}, 
        "date": date_iso,
        "date_fmt": f"{date_fmt} — {league.get('name') or ''}",
        "home": {"id": home.get("id"), "name": home.get("name"), "logo": home_logo, 
                 "form_list": home_form_list, "position": home_position},
        "away": {"id": away.get("id"), "name": away.get("name"), "logo": away_logo, 
                 "form_list": away_form_list, "position": away_position},
        "advice_full": advice,
        "advice_short": advice_short,
        "advice_odd": advice_odd_val,
        "percent": {"home": home_pct, "draw": "N/A", "away": away_pct},
        "radar": radar, "bars": bars, "h2h": h2h_list, "standings": standings,
        "venue": venue_display, "lineups": lineups, "home_recent": home_recent,
        "away_recent": away_recent, "status_info": status_info, "odds": odds,
        "is_logged_in": 'user_id' in session,
    }

def prepare_team_data(team_id):
    """Prepare team page data"""
    team_info_raw = get_team_info(team_id)
    if not team_info_raw or not isinstance(team_info_raw, list) or len(team_info_raw) == 0: 
        return None
    team_data = team_info_raw[0].get("team", {})
    venue_data = team_info_raw[0].get("venue", {})
    team = {
        "id": team_data.get("id"), "name": team_data.get("name"), 
        "logo": team_data.get("logo"),
        "country": team_data.get("country"), "founded": team_data.get("founded"),
        "venue_name": venue_data.get("name"), "venue_capacity": venue_data.get("capacity")
    }
    domestic_league_id = DOMESTIC_LEAGUES.get(team.get("country"))
    current_season = datetime.now().year
    try:
        current_fixtures = api_get("/fixtures", {"team": team_id, "next": 5}) or []
        if current_fixtures: 
            current_season = current_fixtures[0].get("league", {}).get("season", current_season)
    except: 
        pass

    upcoming_fixtures = api_get("/fixtures", {"team": team_id, "next": 10}) or []
    recent_fixtures = api_get("/fixtures", {"team": team_id, "last": 40}) or []
    upcoming_matches, recent_matches, leagues_set = [], [], set()

    for fixture in upcoming_fixtures + recent_fixtures:
        try:
            date_iso = fixture.get("fixture", {}).get("date", "")
            dt = datetime.fromisoformat(date_iso.replace("Z", "+00:00"))
            match_data = {
                "fixture_id": fixture.get("fixture", {}).get("id"), 
                "date": dt.strftime("%Y-%m-%d"), 
                "time": dt.strftime("%H:%M"),
                "home_id": fixture.get("teams", {}).get("home", {}).get("id"), 
                "home_name": fixture.get("teams", {}).get("home", {}).get("name"),
                "home_logo": fixture.get("teams", {}).get("home", {}).get("logo"), 
                "away_id": fixture.get("teams", {}).get("away", {}).get("id"),
                "away_name": fixture.get("teams", {}).get("away", {}).get("name"), 
                "away_logo": fixture.get("teams", {}).get("away", {}).get("logo"),
                "home_goals": fixture.get("goals", {}).get("home"), 
                "away_goals": fixture.get("goals", {}).get("away"),
                "league_name": fixture.get("league", {}).get("name"), 
                "league_id": fixture.get("league", {}).get("id"),
                "season": fixture.get("league", {}).get("season"), 
                "status": fixture.get("fixture", {}).get("status", {}).get("short", "")
            }
            if match_data["league_id"] and match_data["season"]: 
                leagues_set.add((match_data["league_id"], match_data["league_name"], match_data["season"]))
            if match_data["status"] in ["FT", "AET", "PEN"]: 
                recent_matches.append(match_data)
            else: 
                upcoming_matches.append(match_data)
        except: 
            continue

    upcoming_matches.sort(key=lambda x: f"{x['date']} {x['time']}")
    recent_matches.sort(key=lambda x: f"{x['date']} {x['time']}", reverse=True)
    leagues_by_season = {}
    for lid, lname, season in leagues_set:
        if season not in leagues_by_season: 
            leagues_by_season[season] = []
        if not any(l["id"] == lid for l in leagues_by_season[season]): 
            leagues_by_season[season].append({"id": lid, "name": lname})
    for season in leagues_by_season: 
        leagues_by_season[season].sort(key=lambda x: x["name"])

    return {
        "team": team, "upcoming_matches": upcoming_matches, 
        "recent_matches": recent_matches,
        "current_season": current_season, 
        "available_seasons": list(range(current_season, current_season - 5, -1)),
        "leagues_by_season": leagues_by_season, 
        "domestic_league_id": domestic_league_id
    }

# === ROUTES ===
@app.route("/")
def index():
    return redirect(url_for('leagues_page'))

@app.route("/leagues")
def leagues_page():
    return render_template("leagues.html", is_logged_in='user_id' in session)

@app.route("/history")
def history_page():
    return render_template("history.html", is_logged_in='user_id' in session)

@app.route("/match/<int:fixture_id>")
def match_page(fixture_id):
    fr = get_fixture(fixture_id)
    if not fr: 
        abort(404, description="Fixture not found")
    fixture_raw = fr[0] if isinstance(fr, list) and len(fr) > 0 else fr
    data = prepare_comparison_single(fixture_raw)
    return render_template("match.html", data=data)

@app.route("/team/<int:team_id>")
def team_page(team_id):
    data = prepare_team_data(team_id)
    if not data: 
        abort(404, description="Team not found")
    return render_template("team.html", data=data)

@app.route("/login")
def login_page():
    return render_template("login.html")

@app.route("/logout")
def logout():
    session.pop('user_id', None)
    session.pop('user_email', None)
    return redirect(url_for('index'))

@app.route("/api/auth/demo-login", methods=["POST"])
def demo_login():
    email = request.get_json().get('email')
    if email:
        session['user_id'] = 'demo_user_' + email
        session['user_email'] = email
        return jsonify({"success": True, "message": "Logged in successfully"})
    return jsonify({"success": False, "message": "Invalid credentials"}), 401

@app.route("/api/match/<int:fixture_id>")
def api_match_json(fixture_id):
    fr = get_fixture(fixture_id)
    if not fr: 
        return jsonify({"error":"not found"}), 404
    fixture_raw = fr[0] if isinstance(fr, list) and len(fr) > 0 else fr
    data = prepare_comparison_single(fixture_raw)
    return jsonify(data)

@app.route("/api/fixtures")
def api_fixtures():
    """Get upcoming fixtures - INSTANT from cache"""
    date = request.args.get('date')
    if not date:
        date = datetime.now().strftime('%Y-%m-%d')

    app.logger.info(f"📥 Request: upcoming {date}")

    with cache_lock:
        if date in master_cache and master_cache[date].get('ready'):
            fixtures = master_cache[date].get('upcoming', [])
            app.logger.info(f"✅ Cache HIT: {len(fixtures)} upcoming")
            return jsonify({"fixtures": fixtures})

    # Cache not ready - return partial or empty
    app.logger.warning(f"⚠️ Cache MISS: {date}")
    return jsonify({"fixtures": [], "building": True})

@app.route("/api/fixtures/finished")
def api_fixtures_finished():
    """Get finished fixtures - INSTANT from cache"""
    date = request.args.get('date')
    if not date:
        date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    app.logger.info(f"📥 Request: finished {date}")

    with cache_lock:
        if date in master_cache and master_cache[date].get('ready'):
            fixtures = master_cache[date].get('finished', [])
            app.logger.info(f"✅ Cache HIT: {len(fixtures)} finished")
            return jsonify({"fixtures": fixtures})

    # Cache not ready
    app.logger.warning(f"⚠️ Cache MISS: {date}")
    return jsonify({"fixtures": [], "building": True})

@app.route("/api/team/<int:team_id>/standings")
def api_team_standings(team_id):
    season = request.args.get('season', type=int)
    league_id = request.args.get('league', type=int)
    if not season: 
        return jsonify({"error": "Season parameter required"}), 400

    leagues_to_check = []
    if league_id:
        leagues_to_check.append(league_id)
    else:
        fixtures = get_team_fixtures(team_id, season)
        if fixtures:
            leagues_to_check = list(set(f.get("league", {}).get("id") for f in fixtures if f.get("league", {}).get("id")))

    result = {"standings": []}
    for lid in leagues_to_check:
        standings_raw = get_standings(lid, season)
        if standings_raw and isinstance(standings_raw, list) and standings_raw:
            try:
                league_data = standings_raw[0].get("league", {})
                rows = league_data.get("standings", [[]])[0]
                standings_list = []
                for r in rows:
                    team_id_row = r.get("team", {}).get("id")
                    standings_list.append({
                        "rank": r.get("rank"), "id": team_id_row, 
                        "name": r.get("team", {}).get("name"),
                        "logo": r.get("team", {}).get("logo"), 
                        "played": r.get("all", {}).get("played"),
                        "win": r.get("all", {}).get("win"), 
                        "draw": r.get("all", {}).get("draw"), 
                        "loss": r.get("all", {}).get("lose"),
                        "goals_for": r.get("all", {}).get("goals", {}).get("for"), 
                        "goals_against": r.get("all", {}).get("goals", {}).get("against"),
                        "points": r.get("points"), 
                        "zone": detect_zone(r.get("description")), 
                        "highlight": team_id_row == team_id
                    })
                result["standings"].append({
                    "league_name": league_data.get("name"), 
                    "league_id": league_data.get("id"), 
                    "rows": standings_list
                })
            except Exception as e:
                app.logger.exception(f"Error parsing standings for league {lid}: {e}")
    return jsonify(result)

@app.route("/api/cache/status")
def cache_status_endpoint():
    """Cache status - for debugging"""
    with cache_lock:
        status = {}
        for date_str, data in master_cache.items():
            status[date_str] = {
                'ready': data.get('ready', False),
                'upcoming_count': len(data.get('upcoming', [])),
                'finished_count': len(data.get('finished', [])),
                'timestamp': datetime.fromtimestamp(data.get('timestamp', 0)).strftime('%Y-%m-%d %H:%M:%S') if data.get('timestamp') else 'N/A'
            }
        
        progress = {}
        for date_str, info in cache_status.items():
            progress[date_str] = info

    return jsonify({
        'cache': status,
        'progress': progress,
        'current_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'predictions_cache_size': len(predictions_cache),
        'odds_cache_size': len(odds_cache)
    })

# === STARTUP ===
if __name__ == "__main__":
    app.logger.info("=" * 80)
    app.logger.info("🚀 FOOTBALL APP - FAST START MODE")
    app.logger.info("=" * 80)
    
    # Start cache builder in background IMMEDIATELY
    app.logger.info("🔄 Starting background cache builder...")
    start_cache_builder()
    
    app.logger.info("=" * 80)
    app.logger.info("✅ SERVER READY - Cache building in background")
    app.logger.info("💡 Visit /api/cache/status to check progress")
    app.logger.info("⚡ Users get data as soon as it's ready")
    app.logger.info("=" * 80)

    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False)
