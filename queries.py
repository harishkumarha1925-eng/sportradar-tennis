# queries.py
from sqlalchemy import text
import pandas as pd
from db_handler import engine

def run_query(sql, params=None):
    with engine.connect() as conn:
        result = conn.execute(text(sql), params or {})
        df = pd.DataFrame(result.mappings().all())
    return df

# 1. List all competitions along with their category name
def competitions_with_category():
    sql = """
    SELECT c.competition_id, c.competition_name, c.type, c.gender, c.parent_id, cat.category_name
    FROM competitions c
    LEFT JOIN categories cat ON c.category_id = cat.category_id;
    """
    return run_query(sql)

# 2. Count the number of competitions in each category
def count_competitions_by_category():
    sql = """
    SELECT cat.category_name, COUNT(c.competition_id) as competition_count
    FROM categories cat
    LEFT JOIN competitions c ON c.category_id = cat.category_id
    GROUP BY cat.category_name;
    """
    return run_query(sql)

# 3. Find all competitions of type 'doubles'
def find_doubles():
    sql = "SELECT * FROM competitions WHERE lower(type) LIKE '%double%' OR lower(type) LIKE '%doubles%';"
    return run_query(sql)

# 4. Get competitions that belong to a specific category (parameterized)
def competitions_in_category(category_name):
    sql = """
    SELECT c.*
    FROM competitions c
    JOIN categories cat ON c.category_id = cat.category_id
    WHERE lower(cat.category_name) = lower(:category_name);
    """
    return run_query(sql, {"category_name": category_name})

# 5. Identify parent competitions and their sub-competitions
def parent_and_subcompetitions():
    sql = """
    SELECT parent.competition_id as parent_id, parent.competition_name as parent_name,
           child.competition_id as child_id, child.competition_name as child_name
    FROM competitions child
    JOIN competitions parent ON child.parent_id = parent.competition_id
    ORDER BY parent_name;
    """
    return run_query(sql)

# 6. Analyze distribution of competition types by category
def type_distribution_by_category():
    sql = """
    SELECT cat.category_name, c.type, COUNT(*) as count
    FROM competitions c
    LEFT JOIN categories cat ON c.category_id = cat.category_id
    GROUP BY cat.category_name, c.type
    ORDER BY cat.category_name;
    """
    return run_query(sql)

# 7. List all competitions with no parent (top-level competitions)
def top_level_competitions():
    sql = "SELECT * FROM competitions WHERE parent_id IS NULL OR parent_id = '';"
    return run_query(sql)

# --- Complexes / Venues queries
def venues_with_complex_name():
    sql = """
    SELECT v.*, cx.complex_name
    FROM venues v
    LEFT JOIN complexes cx ON v.complex_id = cx.complex_id;
    """
    return run_query(sql)

def count_venues_by_complex():
    sql = """
    SELECT cx.complex_name, COUNT(v.venue_id) as venue_count
    FROM complexes cx
    LEFT JOIN venues v ON v.complex_id = cx.complex_id
    GROUP BY cx.complex_name;
    """
    return run_query(sql)

def venues_in_country(country_name):
    sql = "SELECT * FROM venues WHERE lower(country_name) = lower(:country_name);"
    return run_query(sql, {"country_name": country_name})

def venues_timezones():
    sql = "SELECT venue_name, timezone FROM venues;"
    return run_query(sql)

def complexes_with_multiple_venues():
    sql = """
    SELECT cx.complex_name, COUNT(v.venue_id) as venue_count
    FROM complexes cx
    JOIN venues v ON v.complex_id = cx.complex_id
    GROUP BY cx.complex_name
    HAVING COUNT(v.venue_id) > 1;
    """
    return run_query(sql)

def venues_grouped_by_country():
    sql = "SELECT country_name, COUNT(*) as venue_count FROM venues GROUP BY country_name;"
    return run_query(sql)

def venues_for_complex(complex_name):
    sql = """
    SELECT v.*
    FROM venues v
    JOIN complexes cx ON v.complex_id = cx.complex_id
    WHERE lower(cx.complex_name) = lower(:complex_name);
    """
    return run_query(sql, {"complex_name": complex_name})

# --- Competitors & Rankings queries
def competitors_with_rank_and_points():
    sql = """
    SELECT comp.*, cr.rank, cr.points, cr.movement, cr.competitions_played
    FROM competitors comp
    LEFT JOIN competitor_rankings cr ON comp.competitor_id = cr.competitor_id
    ORDER BY cr.rank;
    """
    return run_query(sql)

def top5_competitors():
    sql = """
    SELECT comp.*, cr.rank, cr.points
    FROM competitors comp
    JOIN competitor_rankings cr ON comp.competitor_id = cr.competitor_id
    WHERE cr.rank <= 5
    ORDER BY cr.rank;
    """
    return run_query(sql)

def stable_rank_competitors():
    sql = """
    SELECT comp.*, cr.rank, cr.movement
    FROM competitors comp
    JOIN competitor_rankings cr ON comp.competitor_id = cr.competitor_id
    WHERE cr.movement = 0;
    """
    return run_query(sql)

def total_points_by_country(country_name):
    sql = """
    SELECT comp.country, SUM(cr.points) as total_points
    FROM competitors comp
    JOIN competitor_rankings cr ON comp.competitor_id = cr.competitor_id
    WHERE lower(comp.country) = lower(:country_name)
    GROUP BY comp.country;
    """
    return run_query(sql, {"country_name": country_name})

def count_competitors_per_country():
    sql = "SELECT country, COUNT(*) as competitor_count FROM competitors GROUP BY country;"
    return run_query(sql)

def highest_points_current_week():
    sql = """
    SELECT comp.*, cr.rank, cr.points
    FROM competitors comp
    JOIN competitor_rankings cr ON comp.competitor_id = cr.competitor_id
    ORDER BY cr.points DESC
    LIMIT 1;
    """
    return run_query(sql)
