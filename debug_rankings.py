# debug_rankings.py
import os, json, textwrap
import requests
from sqlalchemy import text
from db_handler import engine
from config import API_KEY, BASE_URL, FORMAT

def db_counts_and_samples():
    print("=== DB TABLE COUNTS ===")
    with engine.connect() as conn:
        for t in ["competitors", "competitor_rankings"]:
            try:
                res = conn.execute(text(f"SELECT COUNT(*) AS cnt FROM {t};"))
                cnt = res.mappings().first()["cnt"]
            except Exception as e:
                cnt = f"ERROR: {e}"
            print(f"{t}: {cnt}")
    print()

    print("=== SAMPLE rows (competitors LIMIT 5) ===")
    with engine.connect() as conn:
        try:
            res = conn.execute(text("SELECT * FROM competitors LIMIT 5;"))
            rows = res.mappings().all()
            print(rows or "NO ROWS")
        except Exception as e:
            print("ERROR:", e)
    print()

    print("=== SAMPLE rows (competitor_rankings LIMIT 5) ===")
    with engine.connect() as conn:
        try:
            res = conn.execute(text("SELECT * FROM competitor_rankings LIMIT 5;"))
            rows = res.mappings().all()
            print(rows or "NO ROWS")
        except Exception as e:
            print("ERROR:", e)
    print()

def call_double_rankings_api():
    print("=== CALLING doubles rankings API ===")
    # Construct same endpoint used in ETL
    endpoint = f"{BASE_URL}/double_competitors_rankings.{FORMAT}"
    print("Request URL:", endpoint)
    params = {"api_key": API_KEY}
    try:
        resp = requests.get(endpoint, params=params, timeout=30)
    except Exception as e:
        print("ERROR making request:", e)
        return
    print("HTTP status:", resp.status_code)
    # Print small snippet of response body (first 2000 chars)
    text_snip = resp.text[:2000]
    # If JSON, pretty print top-level keys
    try:
        j = resp.json()
        print("Top-level keys in JSON:", list(j.keys()) if isinstance(j, dict) else f"type={type(j)}")
        # pretty-print first element(s) if it's a list or dictionary
        if isinstance(j, dict):
            print("Preview of JSON (pretty, truncated):")
            pretty = json.dumps(j, indent=2)[:4000]
            print(pretty)
        else:
            print("Preview (first item):")
            print(json.dumps(j[:3], indent=2) if len(j) else "[]")
    except Exception:
        print("Response not JSON (raw snippet):")
        print(textwrap.fill(text_snip, width=120))
    print()

def main():
    print("CONFIG CHECK")
    print("API_KEY present:", bool(API_KEY))
    print("BASE_URL:", BASE_URL)
    print("FORMAT:", FORMAT)
    print()
    db_counts_and_samples()
    call_double_rankings_api()

if __name__ == "__main__":
    main()
