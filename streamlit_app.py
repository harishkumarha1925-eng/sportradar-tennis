# streamlit_app.py
import streamlit as st
from db_handler import init_db
from sqlalchemy.exc import OperationalError

# Initialize DB (create tables if missing)
init_db()

# Now import the query functions (after init_db to prevent race)
try:
    from queries import (
        competitions_with_category, count_competitions_by_category, find_doubles,
        competitions_in_category, parent_and_subcompetitions, type_distribution_by_category,
        top_level_competitions, venues_with_complex_name, count_venues_by_complex,
        venues_in_country, venues_timezones, complexes_with_multiple_venues,
        venues_grouped_by_country, venues_for_complex,
        competitors_with_rank_and_points, top5_competitors, stable_rank_competitors,
        total_points_by_country, count_competitors_per_country, highest_points_current_week
    )
except OperationalError as e:
    st.error("Database not ready. Initializing database — please wait and refresh the app.")
    # attempt to initialize DB again and stop further execution
    init_db()
    st.stop()

st.set_page_config(layout="wide", page_title="Sportradar Tennis Explorer")

st.title("Game Analytics — Tennis (Sportradar)")

menu = st.sidebar.selectbox("Page", ["Home", "Competitions", "Complexes & Venues", "Rankings", "Run SQL"])

if menu == "Home":
    st.header("Summary")
    comps = competitions_with_category()
    ven = venues_with_complex_name()
    comps_count = len(comps)
    venues_count = len(ven)
    competitors = competitors_with_rank_and_points()

    col1, col2, col3 = st.columns(3)
    col1.metric("Total competitions", comps_count)
    col2.metric("Total venues", venues_count)
    col3.metric("Total competitors (rank entries)", len(competitors))

    st.subheader("Competitions by Category")
    df_cat = count_competitions_by_category()
    st.dataframe(df_cat)
    if not df_cat.empty:
        fig = px.bar(df_cat, x="category_name", y="competition_count", title="Competitions per Category")
        st.plotly_chart(fig, use_container_width=True)

elif menu == "Competitions":
    st.header("Competitions Explorer")
    df = competitions_with_category()
    st.dataframe(df)
    st.subheader("Filters")
    cat = st.selectbox("Category (or All)", options=["All"] + sorted(df['category_name'].dropna().unique().tolist()))
    if cat and cat != "All":
        filtered = df[df['category_name'] == cat]
        st.dataframe(filtered)
    st.subheader("Doubles competitions")
    st.dataframe(find_doubles())

    st.subheader("Parent & sub-competitions")
    st.dataframe(parent_and_subcompetitions())

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

elif menu == "Run SQL":
    st.header("Run arbitrary SQL (read-only)")
    sql = st.text_area("SQL Query", value="SELECT * FROM competitions LIMIT 50;")
    if st.button("Run"):
        try:
            from queries import run_query
            res = run_query(sql)
            st.dataframe(res)
        except Exception as e:
            st.error(f"Error: {e}")
