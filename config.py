# config.py
import os
from dotenv import load_dotenv
load_dotenv()

# REQUIRED: set SPORT_RADAR_API_KEY in environment or .env
API_KEY = os.getenv("SPORT_RADAR_API_KEY")
if not API_KEY:
    raise RuntimeError("Set SPORT_RADAR_API_KEY in your environment (.env recommended).")

# API URL building pieces
ACCESS_LEVEL = os.getenv("SPORTRADAR_ACCESS_LEVEL", "trial")  # e.g., 'trial' or your prod level
LANG = os.getenv("SPORTRADAR_LANG", "en")
FORMAT = os.getenv("SPORTRADAR_FORMAT", "json")
BASE_URL = f"https://api.sportradar.com/tennis/{ACCESS_LEVEL}/v3/{LANG}"

# DB
DB_URL = os.getenv("DATABASE_URL", "sqlite:///sportradar.db")  # default local sqlite file
