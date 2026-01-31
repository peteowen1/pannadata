"""
Complete Opta/TheAnalyst Data Scraper

Scrapes match data from TheAnalyst/Opta API and extracts player-level stats
for building xG models and augmenting panna ratings.

API Endpoints:
- Match list: /soccerdata/match/{provider_id}?tmcl={season_id}&...
- Match stats: /soccerdata/matchstats/{provider_id}/{match_id} - Player stats, lineups
- Match events: /soccerdata/matchevent/{provider_id}/{match_id} - Event-level data with x/y
- Possession: /soccerdata/possession/{provider_id}/{match_id} - Possession stats
- Pass matrix: /soccerdata/passmatrix/{provider_id}/{match_id} - Pass networks
- Tournament calendar: /soccerdata/tournamentcalendar/{provider_id}/active?comp={comp_id}

Data extracted:
- player_stats: 263+ columns per player-match
- events: Goal/card/sub timing for splint boundaries
- shots: Individual shots with x/y coords for xG modeling
- lineups: Starting XI with positions and minutes played
"""

import requests
import json
import re
import time
import random
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
import pandas as pd


@dataclass
class Shot:
    """Represents a shot event extracted from player stats (aggregated per player-match)"""
    match_id: str
    player_id: str
    player_name: str
    team_id: str
    team_name: str
    position: str
    minutes_played: int
    # Shot counts by type
    total_shots: int = 0
    shots_on_target: int = 0
    shots_off_target: int = 0
    shots_blocked: int = 0
    # Location
    shots_inside_box: int = 0
    shots_outside_box: int = 0
    shots_box_centre: int = 0
    shots_box_left: int = 0
    shots_box_right: int = 0
    # Body part
    shots_right_foot: int = 0
    shots_left_foot: int = 0
    shots_header: int = 0
    # Outcomes
    goals: int = 0
    goals_inside_box: int = 0
    # Shot type
    shots_open_play: int = 0
    shots_corner: int = 0
    shots_penalty: int = 0
    # Big chances
    big_chance_created: int = 0
    big_chance_missed: int = 0
    big_chance_scored: int = 0


@dataclass
class ShotEvent:
    """Represents an individual shot event with x/y coordinates"""
    match_id: str
    event_id: int
    player_id: str
    player_name: str
    team_id: str
    minute: int
    second: int
    x: float
    y: float
    outcome: int  # 1=on target, 0=off target
    is_goal: bool
    type_id: int  # 13=attempt saved, 14=post, 15=miss, 16=goal
    # Qualifiers (extracted from qualifier list)
    body_part: str = ""  # Head, RightFoot, LeftFoot
    situation: str = ""  # OpenPlay, SetPiece, Corner, Penalty, etc.
    big_chance: bool = False


@dataclass
class AllMatchEvent:
    """Represents any match event with x/y coordinates from matchevent API.

    This captures ALL events (passes, tackles, aerials, dribbles, shots, etc.)
    with their x/y coordinates for advanced analysis like passing networks,
    defensive action maps, and positional data.

    Opta Event Type IDs (common):
    - 1: Pass
    - 2: Offside pass
    - 3: Take on (dribble)
    - 4: Foul
    - 5: Out
    - 6: Corner awarded
    - 7: Tackle
    - 8: Interception
    - 9: Turnover
    - 10: Save
    - 11: Claim
    - 12: Clearance
    - 13: Miss (shot)
    - 14: Post (shot)
    - 15: Attempt saved (shot)
    - 16: Goal
    - 17: Card
    - 18: Player off
    - 19: Player on
    - 27: Start delay
    - 30: End (half/match)
    - 32: Blocked pass
    - 34: Resume
    - 35: Ball touch
    - 37: Shield ball opp
    - 41: Ball recovery
    - 42: Blocked pass
    - 44: Aerial
    - 45: Challenge
    - 49: Ball touch
    - 50: Second yellow
    - 51: Temp off
    - 52: Temp on
    - 53: Tackle lost
    - 54: Offside provoked
    - 55: Shield ball opp
    - 56: Ball touch
    - 57: Temp injury
    - 58: Resume from injury
    - 59: Tackle
    - 60: Cross not claimed
    - 61: Keeper pick-up
    - 63: Cross not claimed
    - 64: Error
    - 65: Good skill
    - 66: Punch
    - 67: Dispossessed
    - 68: Tackle
    - 69: Big chance
    - 70: Chance missed
    - 71: Foul won
    - 72: Pass
    - 73: Keeper sweeper
    - 74: Offside
    - 77: Key pass
    """
    match_id: str
    event_id: int
    type_id: int
    player_id: str
    player_name: str
    team_id: str
    minute: int
    second: int
    x: float
    y: float
    end_x: float = 0.0  # For passes/carries - where the ball ended
    end_y: float = 0.0
    outcome: int = 0  # 1=successful, 0=unsuccessful
    period_id: int = 1  # 1=first half, 2=second half, etc.
    # Key qualifiers stored as separate fields for convenience
    qualifier_json: str = ""  # Full qualifiers as JSON string for advanced analysis


@dataclass
class MatchEvent:
    """Represents a match event (goal, card, substitution) with timing"""
    match_id: str
    event_type: str  # goal, yellow_card, red_card, substitution
    minute: int
    second: int = 0
    team_id: str = ""
    player_id: str = ""
    player_name: str = ""
    # For substitutions
    player_on_id: str = ""
    player_on_name: str = ""
    player_off_id: str = ""
    player_off_name: str = ""
    # For goals
    assist_player_id: str = ""
    assist_player_name: str = ""


@dataclass
class PlayerLineup:
    """Represents a player's lineup info with minutes played"""
    match_id: str
    match_date: str
    player_id: str
    player_name: str
    team_id: str
    team_name: str
    team_position: str  # home/away
    position: str
    position_side: str
    formation_place: str
    shirt_number: int
    is_starter: bool
    minutes_played: int = 0
    sub_on_minute: int = 0
    sub_off_minute: int = 0


class OptaScraper:
    """Scrapes Opta data from TheAnalyst API"""

    BASE_URL = "https://api.performfeeds.com/soccerdata"
    PROVIDER_ID = "1mjq6w6ezkxe611ykkj8rgz7f1"

    # Rotating User-Agent pool to help prevent rate limiting
    # Mimics various browsers on Windows/Mac for realistic traffic patterns
    USER_AGENTS = [
        # Chrome on Windows
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        # Chrome on Mac
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        # Firefox on Windows
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
        # Firefox on Mac
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0",
        # Edge on Windows
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0",
        # Safari on Mac
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    ]

    # Competition IDs - Organized by importance/priority
    COMPETITIONS = {
        # ===========================================
        # TIER 1: Big 5 European Leagues
        # ===========================================
        "EPL": "2kwbbcootiqqgmrzs6o5inle5",
        "La_Liga": "34pl8szyvrbwcmfkuocjm3r6t",
        "Bundesliga": "6by3h89i2eykc341oz7lv1ddd",
        "Serie_A": "1r097lpxe0xn03ihb7wi98kao",
        "Ligue_1": "dm5ka0os1e3dxcp3vh05kmp33",

        # ===========================================
        # TIER 2: Major International Tournaments
        # ===========================================
        "World_Cup": "70excpe1synn9kadnbppahdn7",
        "UEFA_Euros": "8tddm56zbasf57jkkay4kbf11",
        "Copa_America": "45db8orh1qttbsqq9hqapmbit",
        "CONCACAF_Gold_Cup": "f51991ex45qhp1p3iu74u4d4e",
        "AFCON": "68zplepppndhl8bfdvgy9vgu1",
        "UEFA_Nations_League": "595nsvo7ykvoe690b1e4u5n56",
        "UEFA_Euro_Qualifiers": "gfskxsdituog2kqp9yiu7bzi",
        "UEFA_WC_Qualifiers": "39q1hq42hxjfylxb7xpe9bvf9",

        # ===========================================
        # TIER 3: Major Club Competitions
        # ===========================================
        "UCL": "4oogyu6o156iphvdvphwpck10",
        "UEL": "4c1nfi2j1m731hcay25fcgndq",
        "Conference_League": "c7b8o53flg36wbuevfzy3lb10",
        "Club_World_Cup": "dc4k1xh2984zbypbnunk7ncic",
        "UEFA_Super_Cup": "a0f4gtru0oyxmpvty4thc5qkc",

        # ===========================================
        # TIER 4: Big 5 Domestic Cups
        # ===========================================
        "FA_Cup": "2hj3286pqov1g1g59k2t2qcgm",
        "League_Cup": "725gd73msyt08xm76v7gkxj7u",
        "Copa_del_Rey": "apdwh753fupxheygs8seahh7x",
        "Supercopa": "sd8z02fe455z2fjvlxvxh0zo",
        "DFB_Pokal": "486rhdgz7yc0sygziht7hje65",
        "Coppa_Italia": "6694fff47wqxl10lrd9tb91f8",
        "Coupe_de_France": "3n9mk5b2mxmq831wfmv6pu86i",
        "Trophee_Champions": "1nsu863daf68kns4l7ou69orf",

        # ===========================================
        # TIER 5: Secondary European Leagues
        # ===========================================
        "Championship": "7ntvbsyq31jnzoqoa8850b9b8",
        "League_One": "3frp1zxrqulrlrnk503n6l4l",
        "League_Two": "bgen5kjer2ytfp7lo9949t72g",
        "WSL": "6vq8j5p3av14nr3iuyi4okhjt",
        "Primeira_Liga": "8yi6ejjd1zudcqtbn07haahg6",
        "Eredivisie": "akmkihra9ruad09ljapsm84b3",
        "Scottish_Premiership": "e21cf135btr8t3upw0vl6n6x0",
        "Super_Lig": "482ofyysbdbeoxauk19yg7tdt",
        "Belgian_First_Division": "4zwgbb66rif2spcoeeol2motx",
        "Swiss_Super_League": "e0lck99w8meo9qoalfrxgo33o",
        "Austrian_Bundesliga": "5c96g1zm7vo5ons9c42uy2w3r",
        "Greek_Super_League": "c0r21rtokgnbtc0o2rldjmkxu",

        # ===========================================
        # TIER 6: Secondary European Domestic Cups
        # ===========================================
        "Taca_de_Portugal": "5jd0k2txwnq69frs79eulba8j",
        "Taca_da_Liga": "bqvy41un7sf86rbse9tv810x7",
        "KNVB_Beker": "cbdbziaqczfuyuwqsylqi26zd",
        "Scottish_Cup": "8kt53kt3mfo29gldhkl05u25b",
        "Scottish_League_Cup": "eog6knrkfei68si736fpquyzc",
        "Turkish_Cup": "7af85xa75vozt2l4hzi6ryts7",
        "Belgian_Cup": "1qt9bfl6dhydf4tpano6n1p7s",
        "Schweizer_Pokal": "8cit3whr514nnd4zkaovsnqn",
        "Austrian_Cup": "1ncmha8yglhyyhg6gtaujymqf",
        "Greek_Cup": "10x5pvhifwo4y7hs3fz9hf245",

        # ===========================================
        # TIER 7: Americas Top Leagues & Cups
        # ===========================================
        "MLS": "287tckirbfj9nb2ar2k9r60vn",
        "Liga_MX": "5vwz4siguym0udhj3cr4l2sz3",
        "Brazilian_Serie_A": "scf9p4y91yjvqvg5jndxzhxj",
        "Argentine_Liga_Profesional": "ecpu6zdp8s0l2zwrx0zprpqzl",
        "CONMEBOL_Libertadores": "86rw2b2ml7rydq74bng6pzwbo",
        "Copa_do_Brasil": "16bnz0wt7mzzrn92p2pza2k9n",

        # ===========================================
        # TIER 8: African Leagues & Competitions
        # ===========================================
        "CAF_CL": "cse5oqqt2pzfcy8uz6yz3tkbj",
        "CAF_Confederation_Cup": "bx57cmq1edfq53ckfk791supi",
        "Egyptian_Premier_League": "12ixvd3k8vfqf10qbfcqitgzo",
        "South_African_PSL": "xvddvdgd2g40p82e3ztylmrqn",
        "Botola_Pro": "1eruend45vd20g9hbrpiggs5u",
        "Moroccan_Cup": "d1d1wnseo0ao8ojqtpxbirh2b",
        "Tunisian_Ligue_1": "f4jc2cc5nq7flaoptpi5ua4k4",
        "Tunisian_Cup": "138n7rt9ngbmktlhtwfeefqqp",
        "Tunisian_Super_Cup": "8wmp2ym78qoluhmxpfe0o73mc",

        # ===========================================
        # TIER 9: Asian Leagues & Cups
        # ===========================================
        "J1_League": "ctms5njkdpwkl1lnopx1d0lxp",
        "K_League_1": "xoxb38hl9k1l9e0yw6xm8qe",
        "Saudi_League": "blmkbxq7l6dufmuzqv831y6w9",
        "UAE_Pro_League": "f39uq10c8xhg5e6rwwcf6lhgc",
        "UAE_League_Cup": "89v3ukjpui1gashsz3i1vphfa",
        "UAE_Presidents_Cup": "2smaq6vx7pgwmkfkn15kp7ib",
        "Gulf_Champions_League": "4gyhjrol8ycf1taamo21fvfh2",
        "AFC_Champions_League_Elite": "3v16kc92h6xfv10c2b19f5owj",
        "A_League": "xwnjb1az11zffwty3m6vn8y6",

        # ===========================================
        # TIER 9B: Oceania Leagues & Cups
        # ===========================================
        "NZ_National_League": "1vyghvhuy6abu4htoemdi79bd",
        "OFC_Champions_League": "4y9msam43q5ddjdrhsvd7fo85",
        "NZ_Chatham_Cup": "c9n0iioc66668md31jzkpmfmj",
        "FIFA_Intercontinental_Cup": "cmvff99i4w10udooqckzt8c2x",

        # ===========================================
        # TIER 10: Nordic & Baltic Leagues
        # ===========================================
        "Danish_Superliga": "29actv1ohj8r10kd9hu0jnb0n",
        "Eliteserien": "9ynnnx1qmkizq1o3qr3v0nsuk",
        "Allsvenskan": "b60nisd3qn427jm0hrg9kvmab",
        "Veikkausliiga": "dvstmwnvw0mt5p38twn9yttyb",
        "DBU_Pokalen": "8ztsv3pzrsyq5w1r3a0nfk1y5",
        "Svenska_Cupen": "d9eaigzyfnfiraqc3ius757tl",
        "Suomen_Cup": "6hlw7rhrpe9garwmfoxu4lebc",

        # ===========================================
        # TIER 11: Eastern European Leagues
        # ===========================================
        "Ukrainian_Premier_League": "6wubmo7di3kdpflluf6s8c7vs",
        "Ekstraklasa": "7hl0svs2hg225i2zud0g3xzp2",
        "Czech_Liga": "bu1l7ckihyr0errxw61p0m05",
        "Romanian_Liga_I": "89ovpy1rarewwzqvi30bfdr8b",
        "Serbian_Super_Liga": "3ww12jab49q8q8mk9avdwjqgk",
        "Croatian_HNL": "1b70m6qtxrp75b4vtk8hxh8c3",
        "NB_I": "47s2kt0e8m444ftqvsrqa3bvq",
        "Bulgarian_First_League": "c0yqkbilbbg70ij2473xymmqv",
        "Slovak_Liga": "1mpjd0vbxbtu9zw89yj09xk3z",
        "Slovenian_Liga": "7nmz249q89qg5ezcvzlheljji",

        # ===========================================
        # TIER 12: Eastern European Cups
        # ===========================================
        "Ukrainian_Cup": "2kuyfkulm5lsgjxynrgh3vz70",
        "Polish_Cup": "b3ufcd24wfnnd5j98ped6irfu",
        "Czech_Cup": "193wqkyb0v5jnsblhvd2ocmyo",
        "Cupa_Romaniei": "65q4uwm6ol1rkf5dp89m8omny",
        "Serbian_Cup": "29lni33vxqrl1tqhadrnfid6t",
        "Croatian_Cup": "3z6xfyd3ovi5x09orlo4rmskx",
        "Magyar_Kupa": "chfah95whw2m0sbdq6cvfac7q",
        "Bulgarian_Cup": "22euhl6zy56cp651ipq99rooq",
        "Slovak_Cup": "ahl3vljaignq9ebaos4uqkrvo",
        "Slovenian_Cup": "ggsjtgoapnah61wu939ni8js",

        # ===========================================
        # TIER 13: Middle East & Israel
        # ===========================================
        "Ligat_Haal": "2xg0qvif1rh7du6wmk2eleku3",
        "Israeli_Cup": "481owj46t50pkw9k6qz1c4jm6",

        # ===========================================
        # TIER 14: Minor European Leagues & Cups
        # ===========================================
        "Irish_Premier": "4mbfidy8zum5u0aqjqo0vuqs2",
        "Bosnian_Premier": "4yngyfinzd6bb1k7anqtqs0wt",
        "Icelandic_Premier": "zilopfej2h0n3vpan5tcynpo",
        "Macedonian_First": "dl5v19e4466jn4fuufhlae7jm",
        "Kosovo_Superliga": "dwiqifdd70ytjytcs09vzosb3",
        "Maltese_Premier": "5taraea6mqjjldg9zxswo825y",
        "Gibraltar_Premier": "dzu6b07r25e3pk2uahxi5mzsz",
        "Armenian_Premier": "dn9kmfdbz74lzt3m8e9g0vwjq",
        "Cyprus_First": "btouq8t23agr62ij7e0ju5p6n",
        "Azerbaijan_Premier": "3428tckxcirwwh3o3jgc1m8ji",
        "Kazakhstan_Premier": "9ikchyu9fb8bvx0s673jofj6s",
        "Azerbaijan_Cup": "9gvvndi7vk9fzvpe65pv5x2ir",
    }

    # Known season IDs (will be populated by discover_seasons)
    SEASONS = {
        # EPL
        "EPL_2025-2026": "51r6ph2woavlbbpk8f29nynf8",
        "EPL_2024-2025": "9n12waklv005j8r32sfjj2eqc",
        "EPL_2023-2024": "1jt5mxgn4q5r6mknmlqv5qjh0",
        "EPL_2022-2023": "80foo89mm28qjvyhjzlpwj28k",
        # La Liga
        "La_Liga_2025-2026": "80zg2v1cuqcfhphn56u4qpyqc",
        # Bundesliga
        "Bundesliga_2025-2026": "2bchmrj23l9u42d68ntcekob8",
        # Serie A
        "Serie_A_2025-2026": "emdmtfr1v8rey2qru3xzfwges",
        # Ligue 1
        "Ligue_1_2025-2026": "dbxs75cag7zyip5re0ppsanmc",
    }

    def __init__(self, data_dir: str = "data"):
        self.session = requests.Session()
        # Set comprehensive browser headers to mimic real browser traffic
        self._set_random_headers()
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self._request_count = 0
        self._last_request_time = 0

    def _set_random_headers(self):
        """Set randomized browser headers to help prevent rate limiting"""
        ua = random.choice(self.USER_AGENTS)

        # Determine browser type from User-Agent for sec-ch-ua
        if "Edg/" in ua:
            sec_ch_ua = '"Microsoft Edge";v="120", "Chromium";v="120", "Not-A.Brand";v="24"'
        elif "Firefox" in ua:
            sec_ch_ua = None  # Firefox doesn't send sec-ch-ua
        elif "Safari" in ua and "Chrome" not in ua:
            sec_ch_ua = None  # Safari doesn't send sec-ch-ua
        else:
            # Chrome
            sec_ch_ua = '"Google Chrome";v="120", "Chromium";v="120", "Not-A.Brand";v="24"'

        headers = {
            "User-Agent": ua,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer": "https://theanalyst.com/",
            "Origin": "https://theanalyst.com",
            "Connection": "keep-alive",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }

        # Add Chromium-specific headers
        if sec_ch_ua:
            headers["sec-ch-ua"] = sec_ch_ua
            headers["sec-ch-ua-mobile"] = "?0"
            headers["sec-ch-ua-platform"] = '"Windows"' if "Windows" in ua else '"macOS"'
            headers["Sec-Fetch-Dest"] = "empty"
            headers["Sec-Fetch-Mode"] = "cors"
            headers["Sec-Fetch-Site"] = "cross-site"

        self.session.headers.update(headers)

    def _rate_limit(self, min_delay: float = 1.0, max_delay: float = 2.0):
        """Rate limiting with User-Agent rotation and randomized delays.

        Args:
            min_delay: Minimum delay between requests in seconds
            max_delay: Maximum delay between requests in seconds
        """
        # Add random jitter to delay to make request pattern less predictable
        delay = random.uniform(min_delay, max_delay)
        elapsed = time.time() - self._last_request_time
        if elapsed < delay:
            time.sleep(delay - elapsed)
        self._last_request_time = time.time()
        self._request_count += 1

        # Rotate User-Agent every 10 requests to vary traffic pattern
        if self._request_count % 10 == 0:
            self._set_random_headers()

    def _fetch(self, endpoint: str, params: Dict[str, str]) -> Optional[Dict]:
        """Fetch from API with JSON format"""
        self._rate_limit()
        url = f"{self.BASE_URL}/{endpoint}"
        default_params = {"_rt": "c", "_fmt": "json"}
        all_params = {**default_params, **params}

        try:
            resp = self.session.get(url, params=all_params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            print(f"Request failed for {endpoint}: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"JSON parse failed: {e}")
            return None

    def discover_seasons(self, competition: str) -> Dict[str, str]:
        """
        Discover all available seasons for a competition via tournament calendar API

        Args:
            competition: Competition key (e.g., "EPL", "La_Liga")

        Returns:
            Dict mapping season names to season IDs
        """
        if competition not in self.COMPETITIONS:
            print(f"Unknown competition: {competition}")
            return {}

        comp_id = self.COMPETITIONS[competition]
        endpoint = f"tournamentcalendar/{self.PROVIDER_ID}"
        params = {"comp": comp_id}

        data = self._fetch(endpoint, params)
        if not data or "competition" not in data:
            print(f"Failed to fetch tournament calendar for {competition}")
            return {}

        seasons = {}
        # Seasons are nested inside competition[0].tournamentCalendar
        for comp in data.get("competition", []):
            for tc in comp.get("tournamentCalendar", []):
                season_id = tc.get("id", "")
                season_name = tc.get("name", "")

                # Normalize season name: "2025/2026" -> "2025-2026"
                if season_name:
                    season_name = season_name.replace("/", "-")
                    seasons[season_name] = season_id

        return seasons

    def get_season_matches(self, season_id: str, start_date: str, end_date: str) -> List[Dict]:
        """
        Get all matches for a season within date range

        Args:
            season_id: Tournament calendar ID (e.g., "51r6ph2woavlbbpk8f29nynf8")
            start_date: ISO date string (e.g., "2025-08-01")
            end_date: ISO date string (e.g., "2026-05-31")

        Returns:
            List of match info dicts with match IDs
        """
        endpoint = f"match/{self.PROVIDER_ID}"
        params = {
            "tmcl": season_id,
            "live": "yes",
            "_pgSz": "100",
            "mt.mDt": f"[{start_date}T00:00:00Z TO {end_date}T23:59:59Z]",
        }
        data = self._fetch(endpoint, params)
        if data and "match" in data:
            return data["match"]
        return []

    def get_match_stats(self, match_id: str) -> Optional[Dict]:
        """Get detailed match statistics including player stats"""
        endpoint = f"matchstats/{self.PROVIDER_ID}/{match_id}"
        params = {"detailed": "yes", "_lcl": "en"}
        return self._fetch(endpoint, params)

    def get_match_events(self, match_id: str) -> Optional[Dict]:
        """Get event-level data with x/y coordinates for all match events"""
        endpoint = f"matchevent/{self.PROVIDER_ID}/{match_id}"
        params = {}
        return self._fetch(endpoint, params)

    def get_possession(self, match_id: str) -> Optional[Dict]:
        """Get possession statistics"""
        endpoint = f"possession/{self.PROVIDER_ID}/{match_id}"
        params = {}
        return self._fetch(endpoint, params)

    def get_pass_matrix(self, match_id: str) -> Optional[Dict]:
        """Get pass matrix (passing network) data"""
        endpoint = f"passmatrix/{self.PROVIDER_ID}/{match_id}"
        params = {}
        return self._fetch(endpoint, params)

    def extract_player_shots(self, match_data: Dict) -> List[Shot]:
        """Extract shot data from match stats for xG model"""
        shots = []

        match_id = match_data.get("matchInfo", {}).get("id", "")

        # Get team info
        contestants = match_data.get("matchInfo", {}).get("contestant", [])
        team_map = {c["id"]: c["name"] for c in contestants}

        # Process lineups
        lineups = match_data.get("liveData", {}).get("lineUp", [])

        for lineup in lineups:
            team_id = lineup.get("contestantId", "")
            team_name = team_map.get(team_id, "Unknown")

            for player in lineup.get("player", []):
                player_id = player.get("playerId", "")
                player_name = player.get("matchName", "")
                position = player.get("position", "")

                # Convert stats list to dict for easier access
                stats = {}
                for s in player.get("stat", []):
                    stats[s["type"]] = int(s.get("value", 0))

                # Only include players who took shots
                total_shots = stats.get("totalScoringAtt", 0)
                if total_shots == 0:
                    continue

                shot = Shot(
                    match_id=match_id,
                    player_id=player_id,
                    player_name=player_name,
                    team_id=team_id,
                    team_name=team_name,
                    position=position,
                    minutes_played=stats.get("minsPlayed", 0),
                    total_shots=total_shots,
                    shots_on_target=stats.get("ontargetScoringAtt", 0),
                    shots_off_target=stats.get("shotOffTarget", 0),
                    shots_blocked=stats.get("blockedScoringAtt", 0),
                    shots_inside_box=stats.get("attemptsIbox", 0),
                    shots_outside_box=stats.get("attemptsObox", 0),
                    shots_box_centre=stats.get("attBxCentre", 0),
                    shots_box_left=stats.get("attBxLeft", 0),
                    shots_box_right=stats.get("attBxRight", 0),
                    shots_right_foot=stats.get("attRfTotal", 0),
                    shots_left_foot=stats.get("attLfTotal", 0),
                    shots_header=stats.get("attHdTotal", 0),
                    goals=stats.get("goals", 0),
                    # Note: goalsIbox or attIboxGoal both work for goals inside box
                    goals_inside_box=stats.get("goalsIbox", stats.get("attIboxGoal", 0)),
                    shots_open_play=stats.get("attOpenplay", 0),
                    shots_corner=stats.get("attCorner", 0),
                    shots_penalty=stats.get("attPenGoal", 0) + stats.get("attPenMiss", 0),
                    big_chance_created=stats.get("bigChanceCreated", 0),
                    big_chance_missed=stats.get("bigChanceMissed", 0),
                    big_chance_scored=stats.get("bigChanceScored", 0),
                )
                shots.append(shot)

        return shots

    def extract_all_player_stats(self, match_data: Dict) -> pd.DataFrame:
        """Extract full player stats from match data as DataFrame"""
        rows = []

        match_id = match_data.get("matchInfo", {}).get("id", "")
        match_date = match_data.get("matchInfo", {}).get("date", "")
        match_desc = match_data.get("matchInfo", {}).get("description", "")

        # Get team info
        contestants = match_data.get("matchInfo", {}).get("contestant", [])
        team_map = {c["id"]: c for c in contestants}

        # Get scores
        scores = match_data.get("liveData", {}).get("matchDetails", {}).get("scores", {})
        ft_score = scores.get("ft", {})

        # Process lineups
        lineups = match_data.get("liveData", {}).get("lineUp", [])

        for lineup in lineups:
            team_id = lineup.get("contestantId", "")
            team_info = team_map.get(team_id, {})
            team_name = team_info.get("name", "Unknown")
            team_position = team_info.get("position", "")  # home/away
            formation = lineup.get("formationUsed", "")

            for player in lineup.get("player", []):
                # Base player info
                row = {
                    "match_id": match_id,
                    "match_date": match_date,
                    "match_description": match_desc,
                    "player_id": player.get("playerId", ""),
                    "player_name": player.get("matchName", ""),
                    "first_name": player.get("firstName", ""),
                    "last_name": player.get("lastName", ""),
                    "shirt_number": player.get("shirtNumber", 0),
                    "position": player.get("position", ""),
                    "position_side": player.get("positionSide", ""),
                    "formation_place": player.get("formationPlace", ""),
                    "team_id": team_id,
                    "team_name": team_name,
                    "team_position": team_position,
                    "team_formation": formation,
                    "home_score": ft_score.get("home", 0),
                    "away_score": ft_score.get("away", 0),
                }

                # Add all stats
                for s in player.get("stat", []):
                    stat_name = s["type"]
                    stat_value = s.get("value", 0)
                    try:
                        row[stat_name] = int(stat_value)
                    except ValueError:
                        row[stat_name] = stat_value

                rows.append(row)

        return pd.DataFrame(rows)

    def extract_match_events(self, match_data: Dict) -> List[MatchEvent]:
        """Extract goals, cards, and substitutions with timing from matchstats data"""
        events = []
        match_id = match_data.get("matchInfo", {}).get("id", "")
        live_data = match_data.get("liveData", {})

        # Extract goals
        for goal in live_data.get("goal", []):
            minute_str = goal.get("timeMinSec", "0:0")
            parts = minute_str.split(":")
            minute = int(parts[0]) if parts else 0
            second = int(parts[1].split(".")[0]) if len(parts) > 1 else 0

            events.append(MatchEvent(
                match_id=match_id,
                event_type="goal",
                minute=minute,
                second=second,
                team_id=goal.get("contestantId", ""),
                player_id=goal.get("scorerId", ""),
                player_name=goal.get("scorerName", ""),
                assist_player_id=goal.get("assistPlayerId", ""),
                assist_player_name=goal.get("assistPlayerName", ""),
            ))

        # Extract cards
        for card in live_data.get("card", []):
            minute_str = card.get("timeMinSec", "0:0")
            parts = minute_str.split(":")
            minute = int(parts[0]) if parts else 0
            second = int(parts[1].split(".")[0]) if len(parts) > 1 else 0

            card_type = card.get("type", "")
            if card_type == "YC":
                event_type = "yellow_card"
            elif card_type == "RC":
                event_type = "red_card"
            elif card_type == "Y2C":
                event_type = "second_yellow"
            else:
                event_type = f"card_{card_type}"

            events.append(MatchEvent(
                match_id=match_id,
                event_type=event_type,
                minute=minute,
                second=second,
                team_id=card.get("contestantId", ""),
                player_id=card.get("playerId", ""),
                player_name=card.get("playerName", ""),
            ))

        # Extract substitutions
        for sub in live_data.get("substitute", []):
            minute_str = sub.get("timeMinSec", "0:0")
            parts = minute_str.split(":")
            minute = int(parts[0]) if parts else 0
            second = int(parts[1].split(".")[0]) if len(parts) > 1 else 0

            events.append(MatchEvent(
                match_id=match_id,
                event_type="substitution",
                minute=minute,
                second=second,
                team_id=sub.get("contestantId", ""),
                player_on_id=sub.get("playerOnId", ""),
                player_on_name=sub.get("playerOnName", ""),
                player_off_id=sub.get("playerOffId", ""),
                player_off_name=sub.get("playerOffName", ""),
            ))

        return events

    def extract_shot_events(self, event_data: Dict) -> List[ShotEvent]:
        """Extract individual shot events with x/y coordinates from matchevent data"""
        shots = []
        match_id = event_data.get("matchInfo", {}).get("id", "")

        # Shot type IDs in Opta: 13=attempt saved, 14=post, 15=miss, 16=goal
        shot_type_ids = {13, 14, 15, 16}

        for event in event_data.get("liveData", {}).get("event", []):
            type_id = event.get("typeId")
            if type_id not in shot_type_ids:
                continue

            # Extract qualifiers
            qualifiers = {q.get("qualifierId"): q.get("value")
                         for q in event.get("qualifier", [])}

            # Body part from qualifiers (15=Head, 72=LeftFoot, 72=RightFoot)
            body_part = ""
            if 15 in qualifiers:
                body_part = "Head"
            elif 72 in qualifiers:
                body_part = "LeftFoot"
            elif qualifiers.get(72) is None and 15 not in qualifiers:
                body_part = "RightFoot"  # Default for non-header shots

            # Situation from qualifiers
            situation = "OpenPlay"
            if 22 in qualifiers:
                situation = "SetPiece"
            if 24 in qualifiers:
                situation = "Corner"
            if 9 in qualifiers:
                situation = "Penalty"

            # Big chance (qualifier 214)
            big_chance = 214 in qualifiers

            shots.append(ShotEvent(
                match_id=match_id,
                event_id=event.get("id", 0),
                player_id=event.get("playerId", ""),
                player_name=event.get("playerName", ""),
                team_id=event.get("contestantId", ""),
                minute=event.get("timeMin", 0),
                second=event.get("timeSec", 0),
                x=float(event.get("x", 0)),
                y=float(event.get("y", 0)),
                outcome=event.get("outcome", 0),
                is_goal=(type_id == 16),
                type_id=type_id,
                body_part=body_part,
                situation=situation,
                big_chance=big_chance,
            ))

        return shots

    def extract_all_match_events(self, event_data: Dict) -> List[AllMatchEvent]:
        """Extract ALL events with x/y coordinates from matchevent data.

        This extracts every event type (passes, tackles, aerials, dribbles, shots, etc.)
        for comprehensive analysis. Each match typically has ~2000 events.

        Args:
            event_data: Raw response from matchevent API endpoint

        Returns:
            List of AllMatchEvent dataclass objects with x/y coordinates
        """
        events = []
        match_id = event_data.get("matchInfo", {}).get("id", "")

        for event in event_data.get("liveData", {}).get("event", []):
            type_id = event.get("typeId", 0)
            if type_id is None:
                continue

            # Extract qualifiers for end coordinates (passEndX=140, passEndY=141)
            qualifiers = {q.get("qualifierId"): q.get("value")
                         for q in event.get("qualifier", [])}

            # End coordinates for passes/carries
            end_x = float(qualifiers.get(140, 0) or 0)  # passEndX
            end_y = float(qualifiers.get(141, 0) or 0)  # passEndY

            # Store qualifiers as JSON string for advanced analysis
            qualifier_json = json.dumps(
                {str(k): v for k, v in qualifiers.items()},
                separators=(',', ':')
            ) if qualifiers else ""

            events.append(AllMatchEvent(
                match_id=match_id,
                event_id=event.get("id", 0),
                type_id=type_id,
                player_id=event.get("playerId", "") or "",
                player_name=event.get("playerName", "") or "",
                team_id=event.get("contestantId", "") or "",
                minute=event.get("timeMin", 0) or 0,
                second=event.get("timeSec", 0) or 0,
                x=float(event.get("x", 0) or 0),
                y=float(event.get("y", 0) or 0),
                end_x=end_x,
                end_y=end_y,
                outcome=event.get("outcome", 0) or 0,
                period_id=event.get("periodId", 1) or 1,
                qualifier_json=qualifier_json,
            ))

        return events

    def extract_lineups(self, match_data: Dict) -> List[PlayerLineup]:
        """Extract lineup data with minutes played"""
        lineups = []
        match_id = match_data.get("matchInfo", {}).get("id", "")
        match_date = match_data.get("matchInfo", {}).get("date", "")

        # Get team info
        contestants = match_data.get("matchInfo", {}).get("contestant", [])
        team_map = {c["id"]: c for c in contestants}

        # Get substitution info for calculating minutes
        subs = match_data.get("liveData", {}).get("substitute", [])
        sub_on_times = {}  # player_id -> minute
        sub_off_times = {}  # player_id -> minute

        for sub in subs:
            minute_str = sub.get("timeMinSec", "0:0")
            minute = int(minute_str.split(":")[0]) if minute_str else 0
            if sub.get("playerOnId"):
                sub_on_times[sub["playerOnId"]] = minute
            if sub.get("playerOffId"):
                sub_off_times[sub["playerOffId"]] = minute

        # Process lineups
        for lineup in match_data.get("liveData", {}).get("lineUp", []):
            team_id = lineup.get("contestantId", "")
            team_info = team_map.get(team_id, {})
            team_name = team_info.get("name", "Unknown")
            team_position = team_info.get("position", "")

            for player in lineup.get("player", []):
                player_id = player.get("playerId", "")

                # Get minutes played from stats
                stats = {s["type"]: s.get("value", 0) for s in player.get("stat", [])}
                mins_played = int(stats.get("minsPlayed", 0))

                # Determine if starter (formation_place 1-11 or gameStarted stat)
                formation_place = player.get("formationPlace", "")
                is_starter = (
                    formation_place.isdigit() and int(formation_place) <= 11
                ) or int(stats.get("gameStarted", 0)) == 1

                # Get sub times
                sub_on = sub_on_times.get(player_id, 0)
                sub_off = sub_off_times.get(player_id, 0)

                lineups.append(PlayerLineup(
                    match_id=match_id,
                    match_date=match_date,
                    player_id=player_id,
                    player_name=player.get("matchName", ""),
                    team_id=team_id,
                    team_name=team_name,
                    team_position=team_position,
                    position=player.get("position", ""),
                    position_side=player.get("positionSide", ""),
                    formation_place=formation_place,
                    shirt_number=int(player.get("shirtNumber", 0)),
                    is_starter=is_starter,
                    minutes_played=mins_played,
                    sub_on_minute=sub_on,
                    sub_off_minute=sub_off,
                ))

        return lineups

    def scrape_season(self, competition: str, season: str,
                      start_date: str, end_date: str,
                      max_matches: Optional[int] = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Scrape all matches for a season (legacy method for backwards compatibility)

        Returns:
            Tuple of (player_stats_df, shots_df)
        """
        result = self.scrape_season_full(competition, season, start_date, end_date, max_matches)
        return result.get("player_stats", pd.DataFrame()), result.get("shots", pd.DataFrame())

    def scrape_season_full(self, competition: str, season: str,
                           start_date: str, end_date: str,
                           max_matches: Optional[int] = None) -> Dict[str, pd.DataFrame]:
        """
        Scrape all matches for a season with full data extraction.

        Collects from multiple API endpoints:
        - matchstats: Player stats (263+ columns), lineups, goals, cards, subs
        - matchevent: Event-level data with x/y coords (shots, passes, etc.)

        Returns:
            Dict with keys: player_stats, shots, shot_events, match_events, events, lineups
        """
        season_key = f"{competition}_{season}"
        if season_key not in self.SEASONS:
            print(f"Unknown season: {season_key}")
            return {}

        season_id = self.SEASONS[season_key]

        print(f"Fetching matches for {season_key}...")
        matches = self.get_season_matches(season_id, start_date, end_date)
        print(f"Found {len(matches)} matches")

        # Filter to played matches only
        played_matches = [
            m for m in matches
            if m.get("liveData", {}).get("matchDetails", {}).get("matchStatus") == "Played"
        ]
        print(f"{len(played_matches)} matches have been played")

        if max_matches:
            played_matches = played_matches[:max_matches]

        # Collectors for all data types
        all_player_stats = []
        all_shots = []
        all_shot_events = []
        all_match_events = []  # ALL events with x/y coords (~2000/match)
        all_events = []
        all_lineups = []

        for i, match in enumerate(played_matches):
            match_id = match["matchInfo"]["id"]
            match_desc = match["matchInfo"]["description"]
            print(f"  [{i+1}/{len(played_matches)}] {match_desc}...", end=" ", flush=True)

            # Get matchstats data (player stats, lineups, goals, cards, subs)
            stats = self.get_match_stats(match_id)
            if not stats:
                print("FAILED (stats)")
                continue

            # Extract from matchstats
            player_df = self.extract_all_player_stats(stats)
            all_player_stats.append(player_df)

            shots = self.extract_player_shots(stats)
            all_shots.extend([asdict(s) for s in shots])

            events = self.extract_match_events(stats)
            all_events.extend([asdict(e) for e in events])

            lineups = self.extract_lineups(stats)
            all_lineups.extend([asdict(l) for l in lineups])

            # Get matchevent data (event-level with x/y coords)
            event_data = self.get_match_events(match_id)
            if event_data:
                shot_events = self.extract_shot_events(event_data)
                all_shot_events.extend([asdict(s) for s in shot_events])

                # Extract ALL events with x/y coords (passes, tackles, aerials, etc.)
                match_events = self.extract_all_match_events(event_data)
                all_match_events.extend([asdict(e) for e in match_events])

            # Save raw JSON files
            raw_dir = self.data_dir / "raw" / competition / season
            raw_dir.mkdir(parents=True, exist_ok=True)
            with open(raw_dir / f"{match_id}_stats.json", "w") as f:
                json.dump(stats, f)
            if event_data:
                with open(raw_dir / f"{match_id}_events.json", "w") as f:
                    json.dump(event_data, f)

            print("OK")

        # Combine all data into DataFrames
        result = {}

        if all_player_stats:
            result["player_stats"] = pd.concat(all_player_stats, ignore_index=True)
        else:
            result["player_stats"] = pd.DataFrame()

        if all_shots:
            result["shots"] = pd.DataFrame(all_shots)
        else:
            result["shots"] = pd.DataFrame()

        if all_shot_events:
            result["shot_events"] = pd.DataFrame(all_shot_events)
        else:
            result["shot_events"] = pd.DataFrame()

        if all_match_events:
            result["match_events"] = pd.DataFrame(all_match_events)
        else:
            result["match_events"] = pd.DataFrame()

        if all_events:
            result["events"] = pd.DataFrame(all_events)
        else:
            result["events"] = pd.DataFrame()

        if all_lineups:
            result["lineups"] = pd.DataFrame(all_lineups)
        else:
            result["lineups"] = pd.DataFrame()

        # Save processed data
        processed_dir = self.data_dir / "processed" / competition / season
        processed_dir.mkdir(parents=True, exist_ok=True)

        for name, df in result.items():
            if not df.empty:
                df.to_parquet(processed_dir / f"{name}.parquet", index=False)
                print(f"Saved {name}: {len(df)} rows")

        return result


def main():
    """Scrape sample data for testing"""
    scraper = OptaScraper()

    print("=" * 60)
    print("Opta Data Scraper - Full Data Test")
    print("=" * 60)

    # Scrape a few recent matches with full data extraction
    result = scraper.scrape_season_full(
        competition="EPL",
        season="2024-2025",
        start_date="2025-01-01",
        end_date="2025-01-28",
        max_matches=3  # Limit for testing
    )

    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)

    # Player stats summary
    player_df = result.get("player_stats", pd.DataFrame())
    if not player_df.empty:
        print(f"\nPlayer Stats: {len(player_df)} player-match records")
        print(f"  Unique players: {player_df['player_id'].nunique()}")
        print(f"  Unique matches: {player_df['match_id'].nunique()}")
        stat_cols = [c for c in player_df.columns if c not in [
            'match_id', 'match_date', 'match_description', 'player_id',
            'player_name', 'first_name', 'last_name', 'shirt_number',
            'position', 'position_side', 'formation_place', 'team_id',
            'team_name', 'team_position', 'team_formation', 'home_score', 'away_score'
        ]]
        print(f"  Stat columns: {len(stat_cols)}")

    # Shot events summary (individual shots with x/y)
    shot_events_df = result.get("shot_events", pd.DataFrame())
    if not shot_events_df.empty:
        print(f"\nShot Events (with x/y coords): {len(shot_events_df)} shots")
        print(f"  Goals: {shot_events_df['is_goal'].sum()}")
        print(f"  Big chances: {shot_events_df['big_chance'].sum()}")
        print(f"  Sample coords: x={shot_events_df['x'].mean():.1f}, y={shot_events_df['y'].mean():.1f}")

    # All match events summary (with x/y coords)
    match_events_df = result.get("match_events", pd.DataFrame())
    if not match_events_df.empty:
        print(f"\nAll Match Events (with x/y coords): {len(match_events_df)} events")
        print(f"  Unique event types: {match_events_df['type_id'].nunique()}")
        type_counts = match_events_df['type_id'].value_counts().head(10).to_dict()
        print(f"  Top 10 event types: {type_counts}")
        print(f"  Events with end coords: {(match_events_df['end_x'] > 0).sum()}")

    # Match events summary (goals, cards, subs from matchstats)
    events_df = result.get("events", pd.DataFrame())
    if not events_df.empty:
        print(f"\nMatch Events (goals/cards/subs): {len(events_df)} events")
        event_counts = events_df['event_type'].value_counts().to_dict()
        for etype, count in event_counts.items():
            print(f"  {etype}: {count}")

    # Lineups summary
    lineups_df = result.get("lineups", pd.DataFrame())
    if not lineups_df.empty:
        print(f"\nLineups: {len(lineups_df)} player records")
        print(f"  Starters: {lineups_df['is_starter'].sum()}")
        print(f"  Avg minutes played: {lineups_df['minutes_played'].mean():.1f}")


if __name__ == "__main__":
    main()
