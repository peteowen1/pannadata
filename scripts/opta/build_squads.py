#!/usr/bin/env python3
"""Build blog/squads.parquet -- CURRENT club membership from Opta's squads feed.

Why this exists (pannadata#123): football/ratings.parquet carries a `team`
column that is end-of-last-season membership and does not refresh with the
ratings. A player's club comes from wherever he was last rated, so a summer
transfer only appears once he has played for the new club -- weeks into the
season, which is exactly when it is most wrong. Verified on the live file after
its 2026-08-22 rebuild: Mohamed Salah listed at Liverpool despite no longer
being at a Premier League club, Bruno Guimaraes at Newcastle despite being at
Arsenal.

The squads feed answers "who is at this club right now" directly, is licensed,
and keys on the Opta player id -- the same `player_id` ratings.parquet uses, so
the join needs no name matching.

Deliberately NOT a Wikipedia scrape. panna's 01b_refresh_wc2026_squads.R does
that for the World Cup and is a good script, but the feed is authoritative,
uses credentials the pipeline already holds, and returns stable ids.

Output: blog/squads.parquet -- player_id, team, team_id, league, position,
season, build_id
"""
import argparse
import collections
import datetime as dt
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from opta_scraper import OptaScraper  # noqa: E402

# Competitions to fetch. The blog rates players across ~15 leagues; the feed is
# verified for these. Anything not here simply has no squad row, and the blog
# must fall back to ratings.parquet's `team` rather than showing the player as
# clubless -- see the join note in docs/plans/OPTA-SQUADS-2026-08-22.md.
DEFAULT_COMPS = ["EPL", "La_Liga", "Bundesliga", "Serie_A", "Ligue_1"]

# Guards. A squad file with a club missing, or a club with almost nobody in it,
# silently empties that club's panels on the site -- worse than not publishing.
# Observed player-only squad sizes on EPL 2026/2027 (2026-08-22): 23 to 40, so
# 15 is a floor with real headroom, not a value tuned to today's data.
MIN_SQUAD = 15
MIN_CLUBS = 10


def fetch_competition(scraper, comp):
    """Return (rows, problems) for one competition."""
    cal = scraper.get_active_tournament_calendar(comp)
    if not cal:
        return [], [f"{comp}: no active tournament calendar"]
    data = scraper.get_squads(cal["id"])
    if not data:
        return [], [f"{comp}: squads feed returned nothing for tmcl={cal['id']}"]

    squads = data.get("squad", [])
    if len(squads) < MIN_CLUBS:
        return [], [f"{comp}: only {len(squads)} club(s) returned (min {MIN_CLUBS})"]

    season = (cal.get("name") or "").replace("/", "-")
    rows, problems = [], []
    for club in squads:
        # contestantClubName, NOT contestantName. Measured against
        # ratings.parquet's `team` on the Bundesliga 2026/2027 calendar:
        # contestantClubName matched 17/18 clubs, contestantName 2/18
        # ("BV Borussia 09 Dortmund" vs "Borussia Dortmund", "1. FC Koln" vs
        # "Koln", and so on).
        team = club.get("contestantClubName") or club.get("contestantName")
        team_id = club.get("contestantId")
        # type: the feed includes coaching staff. On EPL 2026/2027 that was 20
        # coach + 22 assistant coach against 592 players, and those 42 are
        # exactly the entries with a null position. Publishing them unfiltered
        # would put coaches into the ratings join as clubless players.
        #
        # NOTE (Pete, 2026-08-22): the staff rows are WANTED eventually --
        # coach and assistant-coach effects are a candidate RAPM input. They
        # arrive on the same person id ratings.parquet keys on, and carry a
        # startDate, so a tenure is datable, which is what a decay-weighted or
        # as-of design needs. This filter exists because THIS export feeds the
        # player ratings join, not because the data is unwanted. If you want
        # staff, add a separate export rather than relaxing this.
        people = [p for p in club.get("person", [])
                  if p.get("type") == "player" and p.get("active") != "no"]
        if len(people) < MIN_SQUAD:
            problems.append(
                f"{comp}: {team} has {len(people)} player(s) (min {MIN_SQUAD})")
            continue
        for p in people:
            if not p.get("id"):
                problems.append(f"{comp}: {team} has a player with no id")
                continue
            rows.append({
                "player_id": p["id"],
                "team": team,
                # Exported even though ratings.parquet has no team id to join
                # on today -- it only carries a `team` NAME. Four club names
                # differ between the two sources (measured 2026-08-22 across
                # the big five: "AFC Bournemouth"/"Bournemouth",
                # "Bayer Leverkusen"/"Bayer 04 Leverkusen", "Angers SCO"/
                # "Angers", "Deportivo de La Coruna"/"Deportivo de A Coruna"),
                # 92 of 96 agreeing. Any consumer that GROUPS a club's players
                # across the two files will split those four unless it joins on
                # this id or normalises the names. Shipping the id costs one
                # column and is the durable fix.
                "team_id": team_id,
                "league": comp,
                "position": p.get("position"),
                "season": season,
            })
    return rows, problems


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--comps", nargs="*", default=DEFAULT_COMPS)
    ap.add_argument("--out", default=os.path.join("blog", "squads.parquet"))
    ap.add_argument("--allow-partial", action="store_true",
                    help="write even if a competition failed its guards "
                         "(default: refuse, so a half file never reaches the site)")
    args = ap.parse_args()

    scraper = OptaScraper()
    all_rows, all_problems = [], []
    for comp in args.comps:
        rows, problems = fetch_competition(scraper, comp)
        print(f"{comp}: {len(rows)} player-rows, {len(problems)} problem(s)")
        all_rows.extend(rows)
        all_problems.extend(problems)

    if all_problems:
        print("\nGUARD FAILURES:")
        for p in all_problems:
            print(f"  - {p}")
        if not args.allow_partial:
            print("\nRefusing to write a partial squad file. A club missing here "
                  "silently empties its panels on the site; rerun, or pass "
                  "--allow-partial deliberately.")
            return 1

    if not all_rows:
        print("No rows built -- nothing to write.")
        return 1

    df = pd.DataFrame(all_rows)

    # A player in two squads means the join to ratings.parquet would multiply
    # his rows. Observed 0 on EPL 2026/2027, but a mid-season loan across two
    # covered leagues is exactly how it would appear, so assert rather than
    # assume.
    dupes = df[df.duplicated("player_id", keep=False)]
    if not dupes.empty:
        n = dupes["player_id"].nunique()
        print(f"\n{n} player_id(s) appear in more than one squad, e.g.:")
        print(dupes.sort_values("player_id").head(6).to_string(index=False))
        print("Refusing to write: this would multiply rows on the ratings join.")
        return 1

    df["build_id"] = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    df.to_parquet(args.out, index=False)

    print(f"\nWritten: {args.out} ({len(df)} rows, {df['team'].nunique()} clubs, "
          f"{df['league'].nunique()} league(s))")
    by_league = collections.Counter(df["league"])
    for lg, n in sorted(by_league.items()):
        clubs = df[df["league"] == lg]["team"].nunique()
        print(f"   {lg}: {n} players across {clubs} clubs")
    return 0


if __name__ == "__main__":
    sys.exit(main())
