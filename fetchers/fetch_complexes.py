# fetchers/fetch_complexes.py
import requests
from config import BASE_URL, FORMAT, API_KEY
from db_handler import SessionLocal
from models import Complex, Venue
from tqdm import tqdm

def fetch_complexes():
    url = f"{BASE_URL}/complexes.{FORMAT}"
    params = {"api_key": API_KEY}
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()

def process_and_store_complexes(json_data):
    session = SessionLocal()
    try:
        complexes = json_data.get("complexes") or []
        for comp in tqdm(complexes, desc="complexes"):
            comp_id = comp.get("id") or comp.get("complex_id")
            comp_name = comp.get("name") or comp.get("complex_name")
            if not comp_id or not comp_name:
                continue
            cobj = Complex(complex_id=str(comp_id), complex_name=comp_name)
            session.merge(cobj)
            # venues under a complex
            for v in comp.get("venues", []) or []:
                venue_id = v.get("id") or v.get("venue_id")
                venue_name = v.get("name") or v.get("venue_name")
                city = v.get("city", {}).get("name") or v.get("city_name") or v.get("city")
                country = v.get("country", {}).get("name") or v.get("country_name") or v.get("country")
                country_code = v.get("country", {}).get("code") or v.get("country_code") or (country and country[:3])
                timezone = v.get("timezone") or v.get("tz") or "unknown"
                if not venue_id or not venue_name:
                    continue
                vobj = Venue(
                    venue_id=str(venue_id),
                    venue_name=venue_name,
                    city_name=str(city or ""),
                    country_name=str(country or ""),
                    country_code=str(country_code or "")[:3],
                    timezone=str(timezone),
                    complex_id=str(comp_id)
                )
                session.merge(vobj)
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
