# streamlit_app.py
import streamlit as st

# ========== DB init & core imports ==========
from db_handler import init_db, SessionLocal
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError, OperationalError

# Initialize DB (create tables if missing)
init_db()

# Admin ETL import (kept after init to reduce startup race)
from fetchers.fetch_competitions import fetch_competitions, process_and_store_competitions
from fetchers.fetch_complexes import fetch_complexes, process_and_store_complexes
from fetchers.fetch_doubles_rankings import fetch_doubles_rankings, process_and_store_rankings

# Queries import (after db init)
# If a DB error happens here, we'll catch later and show a friendly message.
try:
    from queries import (
        competitions_with_category, count_competitions_by_category, find_doubles,
        competitions_in_category, parent_and_subcompetitions, type_distribution_by_category,
        top_level_competitions, venues_with_complex_name, count_venues_by_complex,
        venues_in_country, venues_timezones, complexes_with_multiple_venues,
        venues_grouped_by_country, venues_for_complex,
        competitors_with_rank_and_points, top5_competitors, stable_rank_competitors,
        total_points_by_country, count_competitors_per_country, highest_points_current_week,
        run_query
    )
except Exception as e:
    # If import fails due to DB operational issues, we allow the app to start but show message later.
    queries_import_error = e
else:
    queries_import_error = None

# ========== Helper functions ==========
def db_counts():
    """Return (competitions_count, venues_count, ranking_rows_count)."""
    session = None
    try:
        session = SessionLocal()
        q = session.execute(text("SELECT COUNT(*) AS c FROM competitions")).mappings().first()
        r = session.execute(text("SELECT COUNT(*) AS c FROM venues")).mappings().first()
        p = session.execute(text("SELECT COUNT(*) AS c FROM competitor_rankings")).mappings().first()
        qc = int(q["c"]) if q and q.get("c") is not None else 0
        rc = int(r["c"]) if r and r.get("c") is not None else 0
        pc = int(p["c"]) if p and p.get("c") is not None else 0
        return qc, rc, pc
    except SQLAlchemyError as e:
        st.sidebar.error(f"DB error while reading counts: {str(e)}")
        return 0, 0, 0
    except Exception as e:
        st.sidebar.error(f"Unexpected error while reading DB counts: {str(e)}")
        return 0, 0, 0
    finally:
        if session:
            session.close()

# ========== Admin sidebar (ETL) ==========
st.set_page_config(layout="wide", page_title="Sportradar Tennis Explorer")

st.sidebar.header("Admin")
try:
    c_comp, c_ven, c_rank = db_counts()
except Exception:
    c_comp, c_ven, c_rank = 0, 0, 0

st.sidebar.markdown(f"**DB Status:**\n- Competitions: {c_comp}\n- Venues: {c_ven}\n- Rankings: {c_rank}")

if st.sidebar.button("Run initial ETL (fetch & populate DB)"):
    # Run ETL sequentially and show success/errors
    with st.spinner("Running ETL — fetching competitions..."):
        try:
            j = fetch_competitions()
            process_and_store_competitions(j)
            st.success("Competitions fetched & stored.")
        except Exception as e:
            st.error(f"Competitions ETL failed: {e}")
            st.stop()

    with st.spinner("Fetching complexes..."):
        try:
            j = fetch_complexes()
            process_and_store_complexes(j)
            st.success("Complexes fetched & stored.")
        except Exception as e:
            st.error(f"Complexes ETL failed: {e}")
            st.stop()

    with st.spinner("Fetching doubles rankings..."):
        try:
            j = fetch_doubles_rankings()
            process_and_store_rankings(j)
            st.success("Rankings fetched & stored.")
        except Exception as e:
            st.error(f"Rankings ETL failed: {e}")
            st.stop()

    st.balloons()
    st.experimental_rerun()

# ========== App pages / UI ==========
st.title("Game Analytics — Tennis (Sportradar)")

# Show friendly message if queries import failed (likely DB not ready)
if queries_import_error:
    st.error(
        "App could not load query helpers. This usually means the database isn't ready yet. "
        "If you just deployed, please open the Admin sidebar and click 'Run initial ETL', wait for completion, then refresh."
    )
    st.write("Detailed error (for debugging):", str(queries_import_error))
    st.stop()

# Load data for home page
try:
    comps = competitions_with_category()
    ven = venues_with_complex_name()
    competitors = competitors_with_rank_and_points()
except OperationalError as e:
    st.error("Database operational error. Try running the ETL from the Admin sidebar or check your database connection.")
    st.write(str(e))
    st.stop()
except Exception as e:
    st.error("Unexpected error loading data.")
    st.write(str(e))
    st.stop()

# ---------- Home page ----------
menu = st.sidebar.selectbox("Page", ["Home", "Competitions", "Complexes & Venues", "Rankings", "Run SQL"])

if menu == "Home":
    st.header("Summary")
    comps_count = len(comps)
    venues_count = len(ven)
    competitors_count = len(competitors)

    col1, col2, col3 = st.columns(3)
    col1.metric("Total competitions", comps_count)
    col2.metric("Total venues", venues_count)
    col3.metric("Total competitors (rank entries)", competitors_count)

    st.subheader("Competitions by Category")
    df_cat = count_competitions_by_category()
    st.dataframe(df_cat)
    if not df_cat.empty:
        try:
            import plotly.express as px
            fig = px.bar(df_cat, x="category_name", y="competition_count", title="Competitions per Category")
            st.plotly_chart(fig, use_container_width=True)
        except Exception:
            st.write("Could not render chart (plotly error). Showing table instead.")

# ---------- Competitions page ----------
elif menu == "Competitions":
    st.header("Competitions Explorer")
    df = competitions_with_category()
    st.dataframe(df)

    st.subheader("Doubles competitions")
    st.dataframe(find_doubles())

    st.subheader("Parent & sub-competitions")
    st.dataframe(parent_and_subcompetitions())

# ---------- Complexes & Venues ----------
elif menu == "Complexes & Venues":
    st.header("Complexes & Venues")
    vdf = venues_with_complex_name()
    st.dataframe(vdf)

    st.subheader("Venues by country")
    st.dataframe(venues_grouped_by_country())

    st.subheader("Find venues for specific complex")
    complex_name = st.text_input("Complex name (exact)", "")
    if complex_name:
        st.dataframe(venues_for_complex(complex_name))

# ---------- Rankings ----------
elif menu == "Rankings":
    st.header("Doubles Competitor Rankings")
    df = competitors_with_rank_and_points()
    st.dataframe(df)

    st.subheader("Top 5")
    st.dataframe(top5_competitors())

    st.subheader("Stable ranks (movement = 0)")
    st.dataframe(stable_rank_competitors())

    st.subheader("Country-wise total points")
    country = st.text_input("Country name (e.g., Croatia)", "")
    if country:
        st.dataframe(total_points_by_country(country))

    st.subheader("Leaderboard (highest points)")
    st.dataframe(highest_points_current_week())

# ---------- Run SQL ----------
elif menu == "Run SQL":
    st.header("Run arbitrary SQL (read-only)")
    sql = st.text_area("SQL Query", value="SELECT * FROM competitions LIMIT 50;")
    if st.button("Run"):
        try:
            res = run_query(sql)
            st.dataframe(res)
        except Exception as e:
            st.error(f"Error: {e}")

# End of file

