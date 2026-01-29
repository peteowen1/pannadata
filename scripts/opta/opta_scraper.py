"""
Complete Opta/TheAnalyst Data Scraper

Scrapes match data from TheAnalyst/Opta API and extracts player-level stats
for building xG models and augmenting panna ratings.

API Endpoints:
- Match list: /soccerdata/match/{provider_id}?tmcl={season_id}&...
- Match stats: /soccerdata/matchstats/{provider_id}/{match_id}
- Tournament calendar: /soccerdata/tournamentcalendar/{provider_id}/active?comp={comp_id}
"""

import requests
import json
import re
import time
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
import pandas as pd


@dataclass
class Shot:
    """Represents a shot event extracted from player stats"""
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


class OptaScraper:
    """Scrapes Opta data from TheAnalyst API"""

    BASE_URL = "https://api.performfeeds.com/soccerdata"
    PROVIDER_ID = "1mjq6w6ezkxe611ykkj8rgz7f1"

    # Big 5 league competition IDs
    COMPETITIONS = {
        "EPL": "2kwbbcootiqqgmrzs6o5inle5",
        "La_Liga": "34pl8szyvrbwcmfkuocjm3r6t",
        "Bundesliga": "6by3h89i2eykc341oz7lv1ddd",
        "Serie_A": "1r097lpxe0xn03ihb7wi98kao",
        "Ligue_1": "dm5ka0os1e3dxcp3vh05kmp33",
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
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://theanalyst.com/",
        })
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self._request_count = 0
        self._last_request_time = 0

    def _rate_limit(self, min_delay: float = 1.0):
        """Simple rate limiting"""
        elapsed = time.time() - self._last_request_time
        if elapsed < min_delay:
            time.sleep(min_delay - elapsed)
        self._last_request_time = time.time()
        self._request_count += 1

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

    def scrape_season(self, competition: str, season: str,
                      start_date: str, end_date: str,
                      max_matches: Optional[int] = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Scrape all matches for a season

        Returns:
            Tuple of (player_stats_df, shots_df)
        """
        season_key = f"{competition}_{season}"
        if season_key not in self.SEASONS:
            print(f"Unknown season: {season_key}")
            return pd.DataFrame(), pd.DataFrame()

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

        all_player_stats = []
        all_shots = []

        for i, match in enumerate(played_matches):
            match_id = match["matchInfo"]["id"]
            match_desc = match["matchInfo"]["description"]
            print(f"  [{i+1}/{len(played_matches)}] {match_desc}...")

            # Get detailed stats
            stats = self.get_match_stats(match_id)
            if not stats:
                print(f"    Failed to get stats")
                continue

            # Extract player stats
            player_df = self.extract_all_player_stats(stats)
            all_player_stats.append(player_df)

            # Extract shots
            shots = self.extract_player_shots(stats)
            shot_dicts = [asdict(s) for s in shots]
            all_shots.extend(shot_dicts)

            # Save raw JSON
            raw_dir = self.data_dir / "raw" / competition / season
            raw_dir.mkdir(parents=True, exist_ok=True)
            with open(raw_dir / f"{match_id}.json", "w") as f:
                json.dump(stats, f)

        # Combine all data
        player_stats_df = pd.concat(all_player_stats, ignore_index=True) if all_player_stats else pd.DataFrame()
        shots_df = pd.DataFrame(all_shots)

        # Save processed data
        processed_dir = self.data_dir / "processed" / competition / season
        processed_dir.mkdir(parents=True, exist_ok=True)

        if not player_stats_df.empty:
            player_stats_df.to_parquet(processed_dir / "player_stats.parquet", index=False)
            print(f"Saved player stats: {len(player_stats_df)} rows")

        if not shots_df.empty:
            shots_df.to_parquet(processed_dir / "shots.parquet", index=False)
            print(f"Saved shots: {len(shots_df)} rows")

        return player_stats_df, shots_df


def main():
    """Scrape sample data for testing"""
    scraper = OptaScraper()

    print("=" * 60)
    print("Opta Data Scraper - Test Run")
    print("=" * 60)

    # Scrape a few recent matches
    player_df, shots_df = scraper.scrape_season(
        competition="EPL",
        season="2025-2026",
        start_date="2026-01-01",
        end_date="2026-01-21",
        max_matches=5  # Limit for testing
    )

    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)

    if not player_df.empty:
        print(f"\nPlayer Stats: {len(player_df)} player-match records")
        print(f"Unique players: {player_df['player_id'].nunique()}")
        print(f"Unique matches: {player_df['match_id'].nunique()}")

        # Show available stats
        stat_cols = [c for c in player_df.columns if c not in [
            'match_id', 'match_date', 'match_description', 'player_id',
            'player_name', 'first_name', 'last_name', 'shirt_number',
            'position', 'position_side', 'formation_place', 'team_id',
            'team_name', 'team_position', 'team_formation', 'home_score', 'away_score'
        ]]
        print(f"Available stats ({len(stat_cols)}): {stat_cols[:10]}...")

    if not shots_df.empty:
        print(f"\nShots Data: {len(shots_df)} player-match records with shots")
        print(f"Total shots: {shots_df['total_shots'].sum()}")
        print(f"Total goals: {shots_df['goals'].sum()}")
        print(f"Conversion rate: {shots_df['goals'].sum() / shots_df['total_shots'].sum():.1%}")


if __name__ == "__main__":
    main()
