# fetchers/fetch_doubles_rankings.py
import requests
from config import BASE_URL, FORMAT, API_KEY
from db_handler import SessionLocal
from models import Competitor, CompetitorRanking
from tqdm import tqdm

def fetch_doubles_rankings():
    """Fetch raw JSON from the doubles rankings endpoint."""
    url = f"{BASE_URL}/double_competitors_rankings.{FORMAT}"
    params = {"api_key": API_KEY}
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()

def process_and_store_rankings(json_data):
    """
    Parse returned JSON and insert competitor + ranking rows.
    Supports Sportradar shape where:
    json_data['rankings'] -> list of ranking groups,
    each group has 'competitor_rankings' -> list of ranking entries.
    """
    session = SessionLocal()
    try:
        # find the list of ranking groups
        ranking_groups = []
        if isinstance(json_data, dict) and "rankings" in json_data and isinstance(json_data["rankings"], list):
            ranking_groups = json_data["rankings"]
        elif isinstance(json_data, list):
            ranking_groups = json_data
        else:
            # try to find the first list inside the dict
            for v in (json_data.values() if isinstance(json_data, dict) else []):
                if isinstance(v, list):
                    ranking_groups = v
                    break

        if not ranking_groups:
            print("No ranking groups found in JSON.")
            return

        inserted = 0
        for group in tqdm(ranking_groups, desc="ranking_groups"):
            # each group may contain 'competitor_rankings' or similar
            entries = None
            if isinstance(group, dict):
                entries = group.get("competitor_rankings") or group.get("competitor_rankings_list") or group.get("rankings") or []
            elif isinstance(group, list):
                entries = group
            else:
                entries = []

            if not entries:
                continue

            for r in entries:
                # r expected to be a dict like:
                # { "rank":1, "movement":0, "points":8300, "competitions_played":26, "competitor": { ... } }
                if not isinstance(r, dict):
                    continue

                # competitor object is nested under 'competitor'
                competitor = r.get("competitor") or r.get("player") or r.get("team") or r.get("participant")
                if not isinstance(competitor, dict):
                    continue

                comp_id = competitor.get("id") or competitor.get("competitor_id") or competitor.get("player_id")
                name = competitor.get("name") or competitor.get("full_name") or competitor.get("display_name")
                country = competitor.get("country") or competitor.get("country_name") or "Unknown"
                country_code = competitor.get("country_code") or competitor.get("countryCode") or (country and country[:3])
                abbr = competitor.get("abbreviation") or competitor.get("abbr")

                # ranking fields
                rank = r.get("rank") or r.get("position")
                movement = r.get("movement") or r.get("change") or 0
                points = r.get("points") or 0
                competitions_played = r.get("competitions_played") or r.get("events") or 0

                if not comp_id:
                    # fallback: make an id from name
                    if name:
                        comp_id = name.replace(" ", "_")[:48]
                    else:
                        continue

                # Upsert competitor
                cobj = Competitor(
                    competitor_id=str(comp_id),
                    name=str(name or ""),
                    country=str(country or ""),
                    country_code=(str(country_code)[:3] if country_code else ""),
                    abbreviation=str(abbr) if abbr else None
                )
                session.merge(cobj)
                session.commit()  # ensure FK exists

                # Insert ranking row
                rost = CompetitorRanking(
                    rank=int(rank) if rank is not None else 999999,
                    movement=int(movement) if movement is not None else 0,
                    points=int(points) if points is not None else 0,
                    competitions_played=int(competitions_played) if competitions_played is not None else 0,
                    competitor_id=str(comp_id)
                )
                session.add(rost)
                inserted += 1

        session.commit()
        print(f"Inserted {inserted} competitor ranking rows.")
    except Exception as e:
        session.rollback()
        print("ERROR processing rankings:", e)
        raise
    finally:
        session.close()

if __name__ == "__main__":
    # convenience runner for manual testing
    j = fetch_doubles_rankings()
    process_and_store_rankings(j)
