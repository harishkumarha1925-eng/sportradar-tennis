# etl_run.py
from db_handler import init_db
from fetchers.fetch_competitions import fetch_competitions, process_and_store_competitions
from fetchers.fetch_complexes import fetch_complexes, process_and_store_complexes
from fetchers.fetch_doubles_rankings import fetch_doubles_rankings, process_and_store_rankings

def main():
    print("Init DB...")
    init_db()
    print("Fetching competitions...")
    comp_json = fetch_competitions()
    process_and_store_competitions(comp_json)
    print("Fetching complexes...")
    complexes_json = fetch_complexes()
    process_and_store_complexes(complexes_json)
    print("Fetching doubles rankings...")
    rank_json = fetch_doubles_rankings()
    process_and_store_rankings(rank_json)
    print("ETL complete.")

if __name__ == "__main__":
    main()
