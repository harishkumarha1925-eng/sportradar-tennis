# fetchers/fetch_competitions.py
import requests
from config import BASE_URL, FORMAT, API_KEY
from sqlalchemy.exc import IntegrityError
from db_handler import SessionLocal
from models import Category, Competition
from tqdm import tqdm

def fetch_competitions():
    url = f"{BASE_URL}/competitions.{FORMAT}"
    params = {"api_key": API_KEY}
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()

def process_and_store_competitions(json_data):
    session = SessionLocal()
    try:
        # Attempt flexible parsing: look for categories and competitions
        # Many Sportradar endpoints include a top-level "categories" and "competitions" lists.
        categories = json_data.get("categories") or []
        competitions = json_data.get("competitions") or json_data.get("tournaments") or []

        # Insert categories
        for c in tqdm(categories, desc="categories"):
            cat_id = c.get("id") or c.get("category_id") or c.get("categoryId")
            name = c.get("name") or c.get("category_name")
            if not cat_id or not name:
                continue
            obj = Category(category_id=str(cat_id), category_name=name)
            session.merge(obj)  # merge avoids duplicates
        session.commit()

        # Insert competitions
        for comp in tqdm(competitions, desc="competitions"):
            comp_id = comp.get("id") or comp.get("competition_id") or comp.get("id")
            name = comp.get("name") or comp.get("competition_name") or comp.get("title")
            parent = comp.get("parent") or comp.get("parent_id") or comp.get("parentId")
            ctype = comp.get("type") or comp.get("competition_type") or comp.get("event_type") or "unknown"
            gender = comp.get("gender") or comp.get("competition_gender") or "unknown"
            category_id = None
            # category may be nested
            if comp.get("category"):
                category_id = comp.get("category").get("id")
            else:
                category_id = comp.get("category_id") or comp.get("categoryId")

            if not comp_id or not name:
                continue

            obj = Competition(
                competition_id=str(comp_id),
                competition_name=name,
                parent_id=str(parent) if parent else None,
                type=str(ctype),
                gender=str(gender),
                category_id=str(category_id) if category_id else None
            )
            session.merge(obj)
        session.commit()

    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
