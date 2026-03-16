import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit.components.v1 as components
import os

DB_PASSWORD = "Abbaji1940!"
DB_NAME = "dubai_property"
DB_USER = "postgres"
DB_HOST = "localhost"
DB_PORT = "5432"

engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

def run_query(sql, params=None):
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)

def get_listing_type_badge(reg_type):
    if reg_type == 'Off-Plan Properties':
        return "<span style='background:rgba(200,121,65,0.12);color:#E8985A;padding:3px 10px;border-radius:50px;font-size:14px;font-weight:600;border:1px solid rgba(200,121,65,0.3);font-family:Courier Prime,monospace'>OFF-PLAN</span>"
    elif reg_type == 'Existing Properties':
        return "<span style='background:rgba(46,184,122,0.12);color:#2EB87A;padding:3px 10px;border-radius:50px;font-size:14px;font-weight:600;border:1px solid rgba(46,184,122,0.3);font-family:Courier Prime,monospace'>READY</span>"
    else:
        return "<span style='background:rgba(255,255,255,0.05);color:#5C7070;padding:3px 10px;border-radius:50px;font-size:14px;font-weight:600;border:1px solid rgba(255,255,255,0.08);font-family:Courier Prime,monospace'>UNKNOWN</span>"

def get_liquidity_grade(count_12m, in_dld):
    if not in_dld:
        return 'N/A', 'No building-level resale history', '#5C7070'
    if count_12m == 0:
        return 'E', 'Illiquid — no resale activity', '#E05C5C'
    if count_12m >= 50:
        return 'A+', 'Exceptionally liquid', '#22C4B0'
    if count_12m >= 20:
        return 'A', 'Very liquid', '#2EB87A'
    if count_12m >= 10:
        return 'B', 'Liquid', '#1A9B8C'
    if count_12m >= 5:
        return 'C', 'Moderate liquidity', '#C87941'
    return 'D', 'Low liquidity', '#E05C5C'

def get_building_liquidity(building, bedrooms, match_tier):
    tier1_query = run_query("""
        SELECT
            COUNT(CASE WHEN instance_date >= CURRENT_DATE - INTERVAL '12 months' THEN 1 END) as count_12m,
            COUNT(*) as count_total
        FROM transactions_clean t
        JOIN (
            SELECT DISTINCT match_field, building_name_en, project_name_en
            FROM bayut_matched_clean
            WHERE building_name = :building AND match_tier = 'Tier 1'
        ) m ON
            LOWER(TRIM(
                CASE m.match_field
                    WHEN 'building_name_en' THEN t.building_name_en
                    WHEN 'project_name_en' THEN t.project_name_en
                    ELSE NULL
                END
            )) = LOWER(TRIM(
                CASE m.match_field
                    WHEN 'building_name_en' THEN m.building_name_en
                    WHEN 'project_name_en' THEN m.project_name_en
                    ELSE NULL
                END
            ))
        WHERE t.bedrooms = :bedrooms
    """, params={"building": building, "bedrooms": bedrooms})

    if len(tier1_query) > 0 and int(tier1_query.iloc[0]['count_total']) > 0:
        count_12m = int(tier1_query.iloc[0]['count_12m'])
        return get_liquidity_grade(count_12m, True)

    dld_check = run_query("""
        SELECT COUNT(*) as cnt
        FROM transactions_clean t
        JOIN (
            SELECT DISTINCT building_name_en, project_name_en
            FROM bayut_matched_clean
            WHERE building_name = :building
        ) m ON
            LOWER(TRIM(t.building_name_en)) = LOWER(TRIM(m.building_name_en))
            OR LOWER(TRIM(t.project_name_en)) = LOWER(TRIM(m.project_name_en))
        WHERE t.bedrooms = :bedrooms
    """, params={"building": building, "bedrooms": bedrooms})

    count_total = int(dld_check.iloc[0]['cnt']) if len(dld_check) > 0 else 0
    return get_liquidity_grade(0, count_total > 0)

def get_building_liquidity_batch(building, bedroom_list):
    """Fetch liquidity for all bedroom types in one query. Returns dict {bedrooms: (grade, desc, color)}."""
    if not bedroom_list:
        return {}
    bed_list_sql = "','".join(str(b) for b in bedroom_list)
    tier1 = run_query(f"""
        SELECT
            t.bedrooms,
            COUNT(CASE WHEN t.instance_date >= CURRENT_DATE - INTERVAL '12 months' THEN 1 END) as count_12m,
            COUNT(*) as count_total
        FROM transactions_clean t
        JOIN (
            SELECT DISTINCT match_field, building_name_en, project_name_en
            FROM bayut_matched_clean
            WHERE building_name = :building AND match_tier = 'Tier 1'
        ) m ON
            LOWER(TRIM(
                CASE m.match_field
                    WHEN 'building_name_en' THEN t.building_name_en
                    WHEN 'project_name_en' THEN t.project_name_en
                    ELSE NULL
                END
            )) = LOWER(TRIM(
                CASE m.match_field
                    WHEN 'building_name_en' THEN m.building_name_en
                    WHEN 'project_name_en' THEN m.project_name_en
                    ELSE NULL
                END
            ))
        WHERE t.bedrooms IN ('{bed_list_sql}')
        GROUP BY t.bedrooms
    """, params={"building": building})

    result = {}
    tier1_found = set()
    if len(tier1) > 0:
        for _, r in tier1.iterrows():
            beds = str(r['bedrooms'])
            if int(r['count_total']) > 0:
                result[beds] = get_liquidity_grade(int(r['count_12m']), True)
                tier1_found.add(beds)

    # For any not found in tier1, do a single fallback query
    missing = [b for b in bedroom_list if str(b) not in tier1_found]
    if missing:
        miss_sql = "','".join(str(b) for b in missing)
        fallback = run_query(f"""
            SELECT t.bedrooms, COUNT(*) as cnt
            FROM transactions_clean t
            JOIN (
                SELECT DISTINCT building_name_en, project_name_en
                FROM bayut_matched_clean
                WHERE building_name = :building
            ) m ON
                LOWER(TRIM(t.building_name_en)) = LOWER(TRIM(m.building_name_en))
                OR LOWER(TRIM(t.project_name_en)) = LOWER(TRIM(m.project_name_en))
            WHERE t.bedrooms IN ('{miss_sql}')
            GROUP BY t.bedrooms
        """, params={"building": building})
        found_in_fallback = set()
        if len(fallback) > 0:
            for _, r in fallback.iterrows():
                beds = str(r['bedrooms'])
                found_in_fallback.add(beds)
                result[beds] = get_liquidity_grade(0, int(r['cnt']) > 0)
        for b in missing:
            if str(b) not in found_in_fallback:
                result[str(b)] = get_liquidity_grade(0, False)
    return result

def get_chart_data(building):
    return run_query("""
        SELECT
            DATE_TRUNC('quarter', instance_date) as quarter,
            bedrooms,
            ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_per_sqft)::numeric, 0) as median_ppsf,
            COUNT(*) as transaction_count
        FROM transactions_clean t
        JOIN (
            SELECT DISTINCT ON (match_field) match_field, building_name_en, project_name_en, master_project_en, area_name_en
            FROM bayut_matched_clean
            WHERE building_name = :building
            ORDER BY match_field
        ) m ON (
            (m.match_field = 'building_name_en' AND (
                LOWER(TRIM(t.building_name_en)) = LOWER(TRIM(m.building_name_en))
                OR LOWER(TRIM(t.project_name_en)) = LOWER(TRIM(m.project_name_en))
            ))
            OR (m.match_field = 'project_name_en' AND
                LOWER(TRIM(t.project_name_en)) = LOWER(TRIM(m.project_name_en)))
            OR (m.match_field = 'master_project_en' AND
                LOWER(TRIM(t.master_project_en)) = LOWER(TRIM(m.master_project_en)))
            OR (m.match_field = 'area_name_en' AND
                LOWER(TRIM(t.area_name_en)) = LOWER(TRIM(m.area_name_en)))
        )
        WHERE instance_date >= CURRENT_DATE - INTERVAL '5 years'
        GROUP BY DATE_TRUNC('quarter', instance_date), bedrooms
        ORDER BY quarter, bedrooms
    """, params={"building": building})

# ── CONFIG ────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="PropVestIQ — Dubai Property Intelligence",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ── CSS ───────────────────────────────────────────────────────────────────────
css_path = os.path.join(os.path.dirname(__file__), 'style.css')
with open(css_path) as f:
    css = f.read()
st.markdown(f"""
<link href="https://fonts.googleapis.com/css2?family=Tenor+Sans&family=Nunito+Sans:wght@300;400;500;600;700&family=Courier+Prime:wght@400;700&display=swap" rel="stylesheet">
<style>{css}</style>
""", unsafe_allow_html=True)

# ── SESSION STATE ─────────────────────────────────────────────────────────────
if 'page' not in st.session_state:
    st.session_state.page = 'search'
if 'selected_building' not in st.session_state:
    st.session_state.selected_building = None
if 'selected_unit' not in st.session_state:
    st.session_state.selected_unit = None
# Compare page state
if 'compare_units' not in st.session_state:
    st.session_state.compare_units = []          # list of dicts, max 5
if 'compare_search' not in st.session_state:
    st.session_state.compare_search = ''
if 'compare_building_selected' not in st.session_state:
    st.session_state.compare_building_selected = None

# Tokenisation page state
if 'token_units' not in st.session_state:
    st.session_state.token_units = []
if 'token_building_selected' not in st.session_state:
    st.session_state.token_building_selected = None
if 'token_capital' not in st.session_state:
    st.session_state.token_capital = 1000000
if 'token_alloc_mode' not in st.session_state:
    st.session_state.token_alloc_mode = 'equal'
if 'token_manual_pct' not in st.session_state:
    st.session_state.token_manual_pct = {}

# My Portfolio state
if 'portfolio_properties' not in st.session_state:
    st.session_state.portfolio_properties = []   # list of dicts
if 'portfolio_submitted' not in st.session_state:
    st.session_state.portfolio_submitted = False

# Chart bedroom filter state
if 'detail_chart_beds' not in st.session_state:
    st.session_state.detail_chart_beds = None   # None = All
if 'cmp_chart_beds' not in st.session_state:
    st.session_state.cmp_chart_beds = {}        # key=ci -> set of selected beds (None=All)

# Recover from full-page reload caused by JS setting ?view_row=N&building=...
_vr = st.query_params.get("view_row", None)
_vb = st.query_params.get("building", None)
if _vr is not None and _vb is not None:
    st.session_state.selected_building = _vb
    st.session_state.page = 'listings'

CONF_COLORS = {
    'Very High': ('#2EB87A', 'rgba(46,184,122,0.12)', 'rgba(46,184,122,0.3)'),
    'High':      ('#22C4B0', 'rgba(26,155,140,0.12)', 'rgba(26,155,140,0.3)'),
    'Medium':    ('#C87941', 'rgba(200,121,65,0.12)',  'rgba(200,121,65,0.3)'),
    'Low':       ('#E05C5C', 'rgba(224,92,92,0.12)',   'rgba(224,92,92,0.3)'),
    'No Result': ('#5C7070', 'rgba(255,255,255,0.05)', 'rgba(255,255,255,0.1)'),
}

LIQ_COLORS = {
    'A+': ('#1DB954', 'rgba(29,185,84,0.12)',   'rgba(29,185,84,0.3)'),
    'A':  ('#2EB87A', 'rgba(46,184,122,0.12)',  'rgba(46,184,122,0.3)'),
    'B':  ('#7EC87A', 'rgba(126,200,122,0.12)', 'rgba(126,200,122,0.3)'),
    'C':  ('#C87941', 'rgba(200,121,65,0.12)',  'rgba(200,121,65,0.3)'),
    'D':  ('#E05C5C', 'rgba(224,92,92,0.12)',   'rgba(224,92,92,0.3)'),
    'E':  ('#B02020', 'rgba(176,32,32,0.15)',   'rgba(176,32,32,0.4)'),
    'N/A':('#5C7070', 'rgba(92,112,112,0.08)',  'rgba(92,112,112,0.2)'),
}

CHART_COLORS = ['#22C4B0', '#E8985A', '#2EB87A', '#E05C5C', '#A98FD0', '#C87941']

# Bedroom type → fixed colour
BED_COLORS = {
    'Studio': '#22C4B0',   # teal
    '1':      '#A98FD0',   # purple
    '2':      '#2EB87A',   # green
    '3':      '#E05C5C',   # red
    '4':      '#E8985A',   # orange
    '5+':     '#F472B6',   # pink
}
def bed_color(bed):
    return BED_COLORS.get(str(bed), '#9BA8A6')


def demand_colour(label, score=None):
    """Returns (text_colour, bg_colour, border_colour) for a demand label/score."""
    if label == 'A+ Elite':
        return '#2EB87A', 'rgba(46,184,122,0.12)', 'rgba(46,184,122,0.3)'
    elif label == 'A High':
        return '#7DC97A', 'rgba(125,201,122,0.12)', 'rgba(125,201,122,0.3)'
    elif label == 'B Moderate':
        return '#E8C84A', 'rgba(232,200,74,0.12)', 'rgba(232,200,74,0.3)'
    elif label == 'C Weak':
        return '#E8985A', 'rgba(232,152,90,0.12)', 'rgba(232,152,90,0.3)'
    elif label == 'D Poor':
        return '#E05C5C', 'rgba(224,92,92,0.12)', 'rgba(224,92,92,0.3)'
    # Fallback: colour by raw score (handles old labels still in session state)
    try:
        s = float(score)
        if s >= 80: return '#2EB87A', 'rgba(46,184,122,0.12)', 'rgba(46,184,122,0.3)'
        if s >= 68: return '#7DC97A', 'rgba(125,201,122,0.12)', 'rgba(125,201,122,0.3)'
        if s >= 50: return '#E8C84A', 'rgba(232,200,74,0.12)', 'rgba(232,200,74,0.3)'
        if s >= 35: return '#E8985A', 'rgba(232,152,90,0.12)', 'rgba(232,152,90,0.3)'
        return '#E05C5C', 'rgba(224,92,92,0.12)', 'rgba(224,92,92,0.3)'
    except: pass
    return '#5C7070', 'rgba(92,112,112,0.12)', 'rgba(92,112,112,0.3)'

def get_recommendation(listed_price, fair_value_aed, listed_yield_pct, demand_score, liquidity_grade=None):
    """Returns (label, emoji, text_colour, bg_colour, border_colour) for the entry recommendation.
    4 signals: price (≤10% above FV), yield (≥6.5%), demand (≥65), liquidity (A+ or A).
    Scoring:
      4 bullish → Strong Entry
      3 bullish → Good Entry
      2 bullish, price is one of them → OK Entry
      2 bullish, price is not one → Weak Entry
      0-1 bullish → Poor Entry
    """
    price_bull    = 0
    yield_bull    = 0
    demand_bull   = 0
    liq_bull      = 0

    # Signal 1: Price vs Fair Value (Bullish = ≤10% above FV)
    try:
        if fair_value_aed and listed_price and float(fair_value_aed) > 0:
            pct_over = (float(listed_price) - float(fair_value_aed)) / float(fair_value_aed) * 100
            if pct_over <= 10:
                price_bull = 1
    except: pass

    # Signal 2: Listed Yield (Bullish = ≥6.5%)
    try:
        if listed_yield_pct is not None and str(listed_yield_pct) not in ('', 'nan', 'None'):
            y = float(listed_yield_pct)
            if y >= 6.5:
                yield_bull = 1
    except: pass

    # Signal 3: Demand Score (Bullish = ≥65)
    try:
        if demand_score is not None and str(demand_score) not in ('', 'nan', 'None'):
            d = float(demand_score)
            if d >= 65:
                demand_bull = 1
    except: pass

    # Signal 4: Liquidity (Bullish = A+ or A)
    try:
        if liquidity_grade is not None and liquidity_grade in ('A+', 'A'):
            liq_bull = 1
    except: pass

    bullish = price_bull + yield_bull + demand_bull + liq_bull

    if bullish == 4:
        return "Strong Entry", "🟢", "#2EB87A", "rgba(46,184,122,0.12)", "rgba(46,184,122,0.3)"
    elif bullish == 3:
        return "Good Entry",   "🔵", "#4A9FD4", "rgba(74,159,212,0.12)", "rgba(74,159,212,0.3)"
    elif bullish == 2 and price_bull == 1:
        return "OK Entry",     "🟡", "#E8C84A", "rgba(232,200,74,0.12)",  "rgba(232,200,74,0.3)"
    elif bullish == 2:
        return "Weak Entry",   "🟠", "#E8985A", "rgba(232,152,90,0.12)",  "rgba(232,152,90,0.3)"
    else:
        return "Poor Entry",   "🔴", "#E05C5C", "rgba(224,92,92,0.12)",   "rgba(224,92,92,0.3)"

def pill(text, color, bg, border):
    return f"<span style='background:{bg};color:{color};border:1px solid {border};padding:2px 8px;border-radius:50px;font-size:14px;font-weight:600;font-family:Courier Prime,monospace;white-space:nowrap'>{text}</span>"

def conf_badge(conf):
    c, bg, bd = CONF_COLORS.get(conf, CONF_COLORS['No Result'])
    return f"<span style='color:{c};background:{bg};border:1px solid {bd};padding:3px 9px;border-radius:50px;font-size:11px;font-weight:600;font-family:Courier Prime,monospace'>{conf}</span>"

def section_label(text):
    st.markdown(f"""
    <div style='font-family:Courier Prime,monospace;font-size:11px;letter-spacing:2.5px;
    text-transform:uppercase;color:#1A9B8C;margin:16px 0 10px;opacity:.85;
    display:flex;align-items:center;gap:10px'>
        {text}
        <span style='flex:1;height:1px;background:rgba(255,255,255,0.07);display:inline-block'></span>
    </div>
    """, unsafe_allow_html=True)

def fin_card(label, value, value_color="#EAE6DF", accent=False):
    border = "rgba(26,155,140,0.22)" if accent else "rgba(255,255,255,0.07)"
    bg = "linear-gradient(135deg,rgba(26,155,140,0.08),#162840)" if accent else "#112030"
    top = "background:linear-gradient(90deg,#22C4B0,#1A9B8C,transparent)" if accent else "background:transparent"
    return f"""
    <div style='background:{bg};border:1px solid {border};border-radius:8px;
    padding:13px 16px;position:relative;overflow:hidden;min-height:72px'>
        <div style='position:absolute;top:0;left:0;right:0;height:2px;{top}'></div>
        <div style='font-family:Courier Prime,monospace;font-size:9.5px;font-weight:500;
        letter-spacing:1px;text-transform:uppercase;color:#5C7070;margin-bottom:7px'>{label}</div>
        <div style='font-family:Tenor Sans,serif;font-size:20px;font-weight:600;
        color:{value_color};line-height:1.2'>{value}</div>
    </div>"""

def nav_bar(active=None):
    compare_count = len(st.session_state.compare_units)
    badge = f" ({compare_count})" if compare_count > 0 else ""

    st.markdown("""
    <style>
    /* st.pills — dark bg with white text (avoids Streamlit theme override on white) */
    [data-testid="stPillsInput"] button,
    [data-testid="stPillsInput"] li,
    [data-testid="stPillsInput"] [role="option"],
    [data-testid="stPillsInput"] [role="checkbox"],
    .stPills button, .stPills li {
        background-color: #1E3448 !important;
        color: #FFFFFF !important;
        border: 1px solid rgba(26,155,140,0.4) !important;
        font-weight: 700 !important;
    }
    [data-testid="stPillsInput"] button *,
    [data-testid="stPillsInput"] button p,
    [data-testid="stPillsInput"] button span,
    [data-testid="stPillsInput"] li *,
    [data-testid="stPillsInput"] [role="option"] *,
    .stPills button *, .stPills button p, .stPills button span {
        color: #FFFFFF !important;
        fill: #FFFFFF !important;
    }
    /* selected = red */
    [data-testid="stPillsInput"] button[aria-pressed="true"],
    [data-testid="stPillsInput"] button[aria-selected="true"],
    [data-testid="stPillsInput"] button[aria-checked="true"],
    [data-testid="stPillsInput"] button[data-selected="true"],
    [data-testid="stPillsInput"] [role="option"][aria-selected="true"],
    [data-testid="stPillsInput"] .stPill--selected,
    .stPills button[aria-pressed="true"] {
        background-color: #E05C5C !important;
        color: #FFFFFF !important;
        border-color: #E05C5C !important;
    }
    [data-testid="stPillsInput"] button[aria-pressed="true"] *,
    [data-testid="stPillsInput"] button[aria-selected="true"] *,
    [data-testid="stPillsInput"] button[aria-checked="true"] *,
    [data-testid="stPillsInput"] button[data-selected="true"] *,
    [data-testid="stPillsInput"] [role="option"][aria-selected="true"] * {
        color: #FFFFFF !important;
    }
    /* hover = slightly lighter navy */
    [data-testid="stPillsInput"] button:hover,
    [data-testid="stPillsInput"] li:hover,
    .stPills button:hover {
        background-color: #2A4860 !important;
        color: #FFFFFF !important;
        border-color: #22C4B0 !important;
    }
    [data-testid="stPillsInput"] button:hover *,
    .stPills button:hover * {
        color: #FFFFFF !important;
    }
    /* Nav buttons */
    div[data-testid="stHorizontalBlock"] div[data-testid="column"]:last-child .stButton > button,
    div[data-testid="stHorizontalBlock"] div[data-testid="column"]:nth-last-child(2) .stButton > button {
        background: transparent !important;
        border: 1px solid rgba(26,155,140,0.35) !important;
        color: #22C4B0 !important;
        font-family: 'Courier Prime', monospace !important;
        font-size: 11px !important;
        letter-spacing: 1.5px !important;
        text-transform: uppercase !important;
        border-radius: 6px !important;
        padding: 6px 16px !important;
        width: auto !important;
        margin-top: 10px !important;
    }
    div[data-testid="stHorizontalBlock"] div[data-testid="column"]:last-child .stButton > button:hover,
    div[data-testid="stHorizontalBlock"] div[data-testid="column"]:nth-last-child(2) .stButton > button:hover {
        background: rgba(26,155,140,0.12) !important;
        border-color: #22C4B0 !important;
    }
    </style>
    """, unsafe_allow_html=True)

    nav_indent, nav_logo_col, nav_cmp, nav_tok, nav_port, nav_right = st.columns([0.4, 2.2, 1.4, 1.4, 1.4, 5])

    with nav_logo_col:
        st.markdown(f"""
        <style>
        /* PropVestIQ home button — no box, just styled text */
        div[data-testid="stHorizontalBlock"] div[data-testid="column"]:nth-child(2) .stButton>button {{
            background: transparent !important; border: none !important;
            padding: 8px 0 2px 0 !important; margin: 0 !important;
            text-align: left !important; width: auto !important;
            cursor: pointer !important;
        }}
        div[data-testid="stHorizontalBlock"] div[data-testid="column"]:nth-child(2) .stButton>button:hover {{
            background: transparent !important; border: none !important; opacity: 0.8 !important;
        }}
        div[data-testid="stHorizontalBlock"] div[data-testid="column"]:nth-child(2) .stButton>button p {{
            font-family: 'Tenor Sans', serif !important; font-size: 22px !important;
            font-weight: 700 !important; color: #22C4B0 !important;
            margin: 0 !important; line-height: 1.2 !important;
        }}
        </style>
        """, unsafe_allow_html=True)
        if st.button("PropVestIQ.", key=f"nav_home_{st.session_state.page}"):
            st.session_state.page = 'search'
            st.rerun()
        st.markdown("""
        <div style='font-family:Courier Prime,monospace;font-size:10px;font-weight:700;
        letter-spacing:1.6px;text-transform:uppercase;color:#FFFFFF;
        margin-top:-6px;text-align:center'>Dubai Property Intelligence</div>
        """, unsafe_allow_html=True)

    with nav_cmp:
        if st.button(f"⇄  Compare{badge}", key=f"nav_cmp_{st.session_state.page}"):
            st.session_state.page = 'compare'
            st.session_state.compare_building_selected = None
            st.rerun()
    with nav_tok:
        if st.button("◈  Tokenise", key=f"nav_tok_{st.session_state.page}"):
            st.session_state.page = 'tokenise'
            st.session_state.token_building_selected = None
            st.rerun()
    with nav_port:
        if st.button("⌂  My Portfolio", key=f"nav_port_{st.session_state.page}"):
            st.session_state.page = 'my_portfolio'
            st.rerun()

    st.markdown("<hr style='border:none;border-top:1px solid rgba(26,155,140,0.15);margin:0 0 0 0'>", unsafe_allow_html=True)

def prop_header(bname, barea, extra_meta="", unit_count=None):
    count_html = f"<span style='font-family:Courier Prime,monospace;font-size:13px;letter-spacing:1.5px;text-transform:uppercase;color:#ffffff;margin-top:4px;display:block'>{unit_count} units listed</span>" if unit_count else ""
    st.markdown(f"""
    <div style='padding:18px 48px 14px;border-bottom:1px solid rgba(26,155,140,0.15);
    background:linear-gradient(160deg,#0D1520 0%,#081628 100%)'>
        <div style='display:flex;align-items:center;gap:16px;margin-bottom:4px'>
            <div style='font-family:Tenor Sans,serif;font-size:32px;font-weight:700;
            color:#EAE6DF;letter-spacing:-.3px'>{bname}</div>
        </div>
        <div style='display:flex;gap:14px;align-items:center;flex-wrap:wrap;font-size:13px;color:#9BA8A6'>
            <span>📍 {barea}</span>{extra_meta}
        </div>
        {count_html}
    </div>
    """, unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 1 — SEARCH
# ═══════════════════════════════════════════════════════════════════════════════
if st.session_state.page == 'search':
    nav_bar()

    _, centre, _ = st.columns([1, 2, 1])
    with centre:
        st.markdown("""
        <div style='text-align:center;padding:12vh 0 0'>
            <div style='font-family:Tenor Sans,serif;font-size:52px;font-weight:700;
            line-height:1.15;margin-bottom:14px;letter-spacing:-.3px;color:#EAE6DF'>
                To Invest or
                <em style='font-style:italic;color:#22C4B0'>Not To Invest?</em>
            </div>
            <div style='color:#FFFFFF;font-size:20px;font-weight:700;line-height:1.6;
            letter-spacing:.2px;margin-bottom:40px'>
                Compare Property Listings v Fair Value Prices
            </div>
        </div>
        """, unsafe_allow_html=True)

        search_term = st.text_input("search", placeholder="🔍  Type a building or area name...", label_visibility="collapsed")

    if search_term and len(search_term) >= 2:
        buildings = run_query("""
            SELECT DISTINCT building_name FROM dashboard_listings
            WHERE building_name ILIKE :search
            ORDER BY building_name LIMIT 20
        """, params={"search": f"%{search_term}%"})

        _, res_col, _ = st.columns([1, 2, 1])
        with res_col:
            if len(buildings) == 0:
                st.markdown("<p style='color:#5C7070;font-size:15px;text-align:center;margin-top:20px'>No buildings found.</p>", unsafe_allow_html=True)
            else:
                st.markdown(f"<p style='color:#5C7070;font-size:12px;letter-spacing:1px;text-transform:uppercase;font-family:Courier Prime,monospace;margin:24px 0 14px'>{len(buildings)} result(s)</p>", unsafe_allow_html=True)
                for _, row in buildings.iterrows():
                    parts = row['building_name'].split(',')
                    name = parts[0].strip()
                    area = ', '.join(p.strip() for p in parts[1:]) if len(parts) > 1 else ''
                    col_info, col_btn = st.columns([11, 1])
                    with col_info:
                        st.markdown(f"""
                        <div style='background:#112030;border:1px solid rgba(255,255,255,0.07);
                        border-radius:10px;padding:14px 20px;margin-bottom:7px;
                        display:flex;align-items:center;justify-content:space-between'>
                            <div>
                                <div style='font-family:Nunito Sans,sans-serif;font-size:15px;
                                font-weight:600;color:#EAE6DF'>{name}</div>
                                <div style='font-size:13px;color:#5C7070;margin-top:3px'>{area}</div>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
                    with col_btn:
                        st.markdown("""
                        <style>
                        div[data-testid="stVerticalBlock"] div[data-testid="stVerticalBlock"] .stButton > button {
                            height: 54px !important;
                            margin-top: 0 !important;
                        }
                        </style>""", unsafe_allow_html=True)
                        if st.button("→", key=f"btn_{row['building_name']}"):
                            st.session_state.selected_building = row['building_name']
                            st.session_state.page = 'listings'
                            st.rerun()

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 2 — LISTINGS
# ═══════════════════════════════════════════════════════════════════════════════
elif st.session_state.page == 'listings':
    building = st.session_state.selected_building

    parts = building.split(',')
    bname = parts[0].strip()
    barea = ', '.join(p.strip() for p in parts[1:]) if len(parts) > 1 else ''

    listings = run_query("""
        SELECT d.property_id, d.number_of_bedrooms as bedrooms, d.sqft, d.listed_price,
               d.fair_value_aed, d.premium_discount_pct, d.confidence_score,
               d.median_ppsf, d.trans_count_used, d.window_used,
               d.match_tier, d.match_field, d.reg_type_en, d.url,
               d.demand_score, d.demand_label,
               y.listed_yield_pct, y.estimated_annual_rent, y.fair_value_yield_pct, y.yield_confidence
        FROM dashboard_listings d
        LEFT JOIN yield_results y ON y.property_id = d.property_id
            AND y.number_of_bedrooms = d.number_of_bedrooms
            AND y.sqft = d.sqft
            AND y.listed_price = d.listed_price
        WHERE d.building_name = :building
        ORDER BY
            CASE d.number_of_bedrooms
                WHEN 'Studio' THEN 0 WHEN '1' THEN 1 WHEN '2' THEN 2
                WHEN '3' THEN 3 WHEN '4' THEN 4 WHEN '5+' THEN 5 ELSE 6
            END, d.sqft
    """, params={"building": building})

    _vr = st.query_params.get("view_row", None)
    if _vr is not None:
        try:
            _idx = int(_vr)
            if 0 <= _idx < len(listings):
                st.session_state.selected_unit = listings.iloc[_idx].to_dict()
                st.session_state.page = 'detail'
                st.query_params.clear()
                st.rerun()
        except (ValueError, KeyError):
            pass
        st.query_params.clear()

    nav_bar()
    st.markdown("<div style='border-bottom:1px solid rgba(26,155,140,0.15);background:linear-gradient(160deg,#0D1520 0%,#081628 100%);padding:8px 0'></div>", unsafe_allow_html=True)
    st.markdown("<div style='padding:8px 48px 60px'>", unsafe_allow_html=True)

    if st.button("← Back to Search"):
        st.session_state.page = 'search'
        st.session_state.selected_building = None
        st.rerun()

    st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)

    if len(listings) == 0:
        st.markdown("<p style='color:#5C7070'>No listings found.</p>", unsafe_allow_html=True)
    else:
        liq_cache = get_building_liquidity_batch(building, listings['bedrooms'].unique().tolist())

        TD = "padding:6px 10px;border-bottom:1px solid rgba(255,255,255,0.04);vertical-align:middle;text-align:center;white-space:nowrap;"

        pdf_rows = ""
        rows_html = ""
        row_index_map = {}
        display_idx = 0
        for i, row in listings.iterrows():
            beds  = row['bedrooms']
            grade, _, _ = liq_cache.get(beds, ('N/A', '', '#5C7070'))
            lc, lbg, lbd = LIQ_COLORS.get(grade, LIQ_COLORS['N/A'])
            reg = row.get('reg_type_en', '')

            type_pill = pill("OFF-PLAN", "#E8985A", "rgba(200,121,65,0.12)", "rgba(200,121,65,0.3)") if reg == 'Off-Plan Properties' else \
                        pill("READY",    "#2EB87A", "rgba(46,184,122,0.12)", "rgba(46,184,122,0.3)") if reg == 'Existing Properties' else \
                        pill("UNKNOWN",  "#5C7070", "rgba(255,255,255,0.05)", "rgba(255,255,255,0.08)")

            beds_pill   = pill(str(beds), "#7EB8D4", "rgba(126,184,212,0.12)", "rgba(126,184,212,0.3)")
            sqft_pill   = pill(f"{int(row['sqft']):,}", "#A98FD0", "rgba(169,143,208,0.12)", "rgba(169,143,208,0.3)")
            listed_pill = pill(f"AED {int(row['listed_price']):,}", "#22C4B0", "rgba(26,155,140,0.12)", "rgba(26,155,140,0.3)")
            liq_pill    = pill(grade, lc, lbg, lbd)
            _dscore = row.get('demand_score')
            _dlabel = row.get('demand_label', '—') or '—'
            if _dscore is not None and str(_dscore) not in ('', 'nan'):
                _dval = f"{float(_dscore):.1f} · {_dlabel}"
                _dc, _dbg, _dbd = demand_colour(_dlabel, row.get('demand_score'))
                demand_pill = pill(_dval, _dc, _dbg, _dbd)
            else:
                demand_pill = pill("—", "#5C7070", "rgba(92,112,112,0.08)", "rgba(92,112,112,0.2)")
            rec_label, rec_emoji, _rc, _rbg, _rbd = get_recommendation(
                row.get('listed_price'), row.get('fair_value_aed'),
                row.get('listed_yield_pct'), row.get('demand_score'), grade
            )
            rec_pill = pill(f"{rec_emoji} {rec_label}", _rc, _rbg, _rbd)

            # Yield pill
            _yield_val = row.get('listed_yield_pct')
            if _yield_val is not None and str(_yield_val) not in ('', 'nan'):
                _yv = float(_yield_val)
                _yc  = "#2EB87A" if _yv >= 6.5 else "#E8C84A" if _yv >= 4 else "#E05C5C"
                _ybg = "rgba(46,184,122,0.12)" if _yv >= 6.5 else "rgba(232,200,74,0.12)" if _yv >= 4 else "rgba(224,92,92,0.12)"
                _ybd = "rgba(46,184,122,0.3)"  if _yv >= 6.5 else "rgba(232,200,74,0.3)"  if _yv >= 4 else "rgba(224,92,92,0.3)"
                yield_pill = pill(f"{_yv:.1f}%", _yc, _ybg, _ybd)
            else:
                yield_pill = pill("—", "#5C7070", "rgba(92,112,112,0.08)", "rgba(92,112,112,0.2)")

            if pd.notna(row['fair_value_aed']):
                pct  = float(row['premium_discount_pct'])
                pc   = "#E05C5C" if pct > 0 else "#2EB87A"
                pbg  = "rgba(224,92,92,0.12)" if pct > 0 else "rgba(46,184,122,0.12)"
                pbd  = "rgba(224,92,92,0.3)"  if pct > 0 else "rgba(46,184,122,0.3)"
                plbl = f"+{pct:.1f}%" if pct > 0 else f"{pct:.1f}%"
                conf = str(row['confidence_score']) if row['confidence_score'] else 'No Result'
                cc, cbg, cbd = CONF_COLORS.get(conf, CONF_COLORS['No Result'])
                fv_pill   = pill(f"AED {int(row['fair_value_aed']):,}", "#22C4B0", "rgba(26,155,140,0.12)", "rgba(26,155,140,0.3)")
                pct_pill  = pill(plbl, pc, pbg, pbd)
                conf_pill = pill(conf, cc, cbg, cbd)
                fv_pdf, pct_pdf, conf_pdf = f"AED {int(row['fair_value_aed']):,}", plbl, conf
            else:
                fv_pill = pct_pill = conf_pill = pill("—", "#5C7070", "rgba(255,255,255,0.05)", "rgba(255,255,255,0.08)")
                fv_pdf, pct_pdf, conf_pdf = "—", "—", "—"

            view_pill = f"<span style='background:rgba(26,155,140,0.1);color:#22C4B0;border:1px solid rgba(26,155,140,0.3);padding:2px 10px;border-radius:50px;font-size:13px;font-weight:600;font-family:Courier Prime,monospace;white-space:nowrap;cursor:pointer'>View →</span>"

            rows_html += (
                f"<tr class='clickable-row' data-idx='{display_idx}'>"
                f"<td style='{TD}'>{beds_pill}</td><td style='{TD}'>{sqft_pill}</td>"
                f"<td style='{TD}'>{listed_pill}</td><td style='{TD}'>{fv_pill}</td>"
                f"<td style='{TD}'>{pct_pill}</td><td style='{TD}'>{conf_pill}</td>"
                f"<td style='{TD}'>{liq_pill}</td><td style='{TD}'>{demand_pill}</td>"
                f"<td style='{TD}'>{yield_pill}</td>"
                f"<td style='{TD}'>{type_pill}</td><td style='{TD}'>{rec_pill}</td>"
                f"<td style='{TD}'>{view_pill}</td>"
                f"</tr>"
            )
            row_index_map[display_idx] = (i, row)
            _demand_pdf = f"{float(row['demand_score']):.1f} · {row.get('demand_label','—')}" if row.get('demand_score') is not None and str(row.get('demand_score','')) not in ('','nan') else "—"
            _yield_pdf  = f"{float(row['listed_yield_pct']):.1f}%" if row.get('listed_yield_pct') is not None and str(row.get('listed_yield_pct','')) not in ('','nan') else "—"
            pdf_rows += f"<tr><td>{beds}</td><td>{int(row['sqft']):,}</td><td>AED {int(row['listed_price']):,}</td><td>{fv_pdf}</td><td>{pct_pdf}</td><td>{conf_pdf}</td><td>{grade}</td><td>{_demand_pdf}</td><td>{_yield_pdf}</td><td>{reg}</td><td>{rec_emoji} {rec_label}</td></tr>"
            display_idx += 1

        pdf_html = f"""<html><head><style>
        body{{font-family:Arial,sans-serif;padding:32px;color:#111;}}
        h1{{font-size:24px;margin-bottom:4px;}}.meta{{color:#555;font-size:13px;margin-bottom:24px;}}
        table{{border-collapse:collapse;width:100%;font-size:12px;}}
        th{{background:#1A9B8C;color:#fff;padding:8px 10px;text-align:left;}}
        td{{padding:7px 10px;border-bottom:1px solid #eee;}}
        </style></head><body>
        <h1>{bname}</h1><div class="meta">📍 {barea} · {len(listings)} listings</div>
        <table><thead><tr><th>Beds</th><th>Sqft</th><th>Listed Price</th><th>Fair Value</th>
        <th>± FV</th><th>Confidence</th><th>Liquidity</th><th>Demand Score</th><th>Yield</th><th>Type</th><th>Recommendation</th>
        </tr></thead><tbody>{pdf_rows}</tbody></table></body></html>"""

        left_pad, hdr_center, pdf_area = st.columns([1, 8, 1])
        with hdr_center:
            st.markdown(f"""
            <div style='text-align:center;padding:16px 0 8px'>
                <span style='font-family:Tenor Sans,serif;font-size:28px;font-weight:700;color:#EAE6DF'>{bname}</span>
                <span style='font-family:Courier Prime,monospace;font-size:13px;color:#9BA8A6;margin-left:14px'>📍 {barea}</span>
                <span style='font-family:Courier Prime,monospace;font-size:13px;color:#ffffff;margin-left:14px'>{len(listings)} units listed</span>
            </div>
            """, unsafe_allow_html=True)
        with pdf_area:
            st.markdown("""
            <style>
            div[data-testid="stDownloadButton"] > button {
                background: #C0392B !important; color: #ffffff !important;
                border: none !important; border-radius: 6px !important;
                font-size: 12px !important; font-weight: 700 !important;
                padding: 6px 12px !important;
            }
            div[data-testid="stDownloadButton"] > button:hover { background: #A93226 !important; color: #ffffff !important; }
            </style>
            """, unsafe_allow_html=True)
            st.download_button("⬇ PDF", data=pdf_html.encode(), file_name=f"{bname}.html", mime="text/html", key="pdf_dl")

        st.markdown(f"""
        <style>
        .tbl-outer {{ display:flex; justify-content:center; margin-top:8px; overflow-x:auto; }}
        .lt2 {{ border-collapse:collapse; }}
        .lt2 th {{ font-family:'Courier Prime',monospace; font-size:13px; font-weight:700; color:#fff;
            letter-spacing:1px; text-transform:uppercase; padding:8px 10px;
            border-bottom:1px solid rgba(255,255,255,0.12); white-space:nowrap; text-align:center; }}
        .lt2 td {{ padding:6px 10px; border-bottom:1px solid rgba(255,255,255,0.04);
            vertical-align:middle; text-align:center; white-space:nowrap; }}
        .lt2 tbody tr:hover td {{ background:rgba(26,155,140,0.12); cursor:pointer; }}
        </style>
        <div class="tbl-outer"><table class="lt2" id="listings-table">
          <thead><tr>
            <th>Beds</th><th>Sqft</th><th>Listed Price</th><th>Fair Value</th>
            <th>± FV</th><th>Confidence</th><th>Liquidity</th><th>Demand Score</th>
            <th>Yield</th><th>Type</th><th>Recommendation</th><th></th>
          </tr></thead>
          <tbody>{rows_html}</tbody>
        </table></div>
        """, unsafe_allow_html=True)

        for d_idx, (orig_idx, row) in row_index_map.items():
            if st.button("→", key=f"vrow_{d_idx}"):
                st.session_state.selected_unit = row.to_dict()
                st.session_state.page = 'detail'
                st.rerun()

        components.html(f"""
        <script>
        (function() {{
            function init() {{
                var doc = window.parent.document;
                doc.querySelectorAll('button').forEach(function(b) {{
                    if (b.innerText.trim() === '→') {{
                        b.parentElement.style.cssText = 'position:fixed;top:-9999px;left:-9999px;';
                    }}
                }});
                var rows = doc.querySelectorAll('tr.clickable-row');
                if (!rows.length) {{ setTimeout(init, 300); return; }}
                rows.forEach(function(tr) {{
                    if (tr._wired) return;
                    tr._wired = true;
                    tr.addEventListener('click', function() {{
                        var idx = parseInt(this.getAttribute('data-idx'));
                        var btns = [];
                        doc.querySelectorAll('button').forEach(function(b) {{
                            if (b.innerText.trim() === '→') btns.push(b);
                        }});
                        if (btns[idx]) btns[idx].click();
                    }});
                }});
            }}
            init();
            setTimeout(init, 500);
        }})();
        </script>
        """, height=0)

    st.markdown("</div>", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 3 — DETAIL
# ═══════════════════════════════════════════════════════════════════════════════
elif st.session_state.page == 'detail':
    unit = st.session_state.selected_unit
    building = st.session_state.selected_building
    bedrooms = unit['bedrooms']
    match_tier = unit.get('match_tier', '')
    reg_type = unit.get('reg_type_en', '')

    parts = building.split(',')
    bname = parts[0].strip()
    barea = ', '.join(p.strip() for p in parts[1:]) if len(parts) > 1 else ''
    badge = get_listing_type_badge(reg_type)

    has_fair_value = unit.get('fair_value_aed') is not None and pd.notna(unit.get('fair_value_aed'))
    if has_fair_value:
        pct = float(unit['premium_discount_pct'])
        pct_str = f"{'+' if pct > 0 else ''}{pct:.1f}%"
        pct_color = "#E05C5C" if pct > 0 else "#2EB87A"
        conf = str(unit['confidence_score'])
        cc, _, _ = CONF_COLORS.get(conf, CONF_COLORS['No Result'])
    grade, grade_desc, grade_color = get_building_liquidity(building, bedrooms, match_tier)

    # Fetch yield data for this unit
    _yield_row = run_query("""
        SELECT listed_yield_pct, fair_value_yield_pct, estimated_annual_rent, yield_confidence
        FROM yield_results
        WHERE property_id = :pid 
          AND number_of_bedrooms = :beds
          AND sqft = :sqft
          AND listed_price = :price
        LIMIT 1
    """, params={
        "pid":   unit.get('property_id',''),
        "beds":  bedrooms,
        "sqft":  unit.get('sqft'),
        "price": unit.get('listed_price')
    })
    _has_yield = len(_yield_row) > 0 and _yield_row.iloc[0]['listed_yield_pct'] is not None
    if _has_yield:
        _yr = _yield_row.iloc[0]
        _listed_yield = float(_yr['listed_yield_pct'])
        _fv_yield     = float(_yr['fair_value_yield_pct']) if _yr['fair_value_yield_pct'] is not None else None
        _est_rent     = float(_yr['estimated_annual_rent']) if _yr['estimated_annual_rent'] is not None else None
        _yield_conf   = _yr['yield_confidence']
        _yield_dot    = "●" if _yield_conf == 'High' else "◐" if _yield_conf == 'Medium' else "○"
        _yield_dot_color = "#2EB87A" if _yield_conf == 'High' else "#E8C84A" if _yield_conf == 'Medium' else "#9BA8A6"
        _listed_yield_str = f"{_listed_yield:.2f}% <span style='color:{_yield_dot_color}'>{_yield_dot}</span>"
        _fv_yield_str     = f"{_fv_yield:.2f}% <span style='color:{_yield_dot_color}'>{_yield_dot}</span>" if _fv_yield else "—"
        _est_rent_str     = f"AED {int(_est_rent):,} p.a." if _est_rent else "—"
    else:
        _listed_yield     = None
        _listed_yield_str = "—"
        _fv_yield_str     = "—"
        _est_rent_str     = "—"

    # Compute recommendation
    rec_label, rec_emoji, rec_c, rec_bg, rec_bd = get_recommendation(
        unit.get('listed_price'), unit.get('fair_value_aed'),
        _listed_yield, unit.get('demand_score'), grade
    )

    # PDF is built after demand data is fetched — store template vars now
    _pdf_financial_rows = f"""
    <tr><td>Listed Price</td><td>AED {int(unit['listed_price']):,}</td></tr>
    <tr><td>Price per Sqft</td><td>{'AED ' + str(int(unit['listed_price'] / unit['sqft'])) if unit.get('sqft') else '—'}</td></tr>
    <tr><td>Listed Yield</td><td>{f"{_listed_yield:.2f}% ({_yield_conf})" if _has_yield else '—'}</td></tr>
    <tr><td>Fair Value</td><td>{'AED ' + f"{int(unit['fair_value_aed']):,}" if has_fair_value else '—'}</td></tr>
    <tr><td>Fair Value per Sqft</td><td>{'AED ' + str(int(unit['median_ppsf'])) if has_fair_value and unit.get('median_ppsf') else '—'}</td></tr>
    <tr><td>Estimated Rent</td><td>{f"AED {int(_est_rent):,} p.a." if _has_yield and _est_rent else '—'}</td></tr>
    <tr><td>Liquidity Grade</td><td>{grade}</td></tr>
    <tr><td>Demand Score</td><td>{f"{float(unit['demand_score']):.1f} · {unit.get('demand_label','—')}" if unit.get('demand_score') is not None and str(unit.get('demand_score','')) not in ('','nan') else '—'}</td></tr>
    <tr><td>Recommendation</td><td>{rec_emoji} {rec_label}</td></tr>
    <tr><td>Premium / Discount</td><td>{pct_str if has_fair_value else '—'}</td></tr>
    <tr><td>Confidence</td><td>{conf if has_fair_value else '—'}</td></tr>"""
    _pdf_demand_rows = '<tr><td colspan="3" style="color:#888">Demand data not available</td></tr>'

    nav_bar()
    st.markdown("<div style='border-bottom:1px solid rgba(26,155,140,0.15);background:linear-gradient(160deg,#0D1520 0%,#081628 100%);padding:8px 0'></div>", unsafe_allow_html=True)
    st.markdown("<div style='padding:8px 48px 60px'>", unsafe_allow_html=True)

    left_pad, hdr_center, pdf_area = st.columns([1, 8, 1])
    with hdr_center:
        st.markdown(f"""
        <div style='text-align:center;padding:16px 0 8px'>
            <div style='font-family:Tenor Sans,serif;font-size:28px;font-weight:700;color:#EAE6DF;margin-bottom:6px'>{bname}</div>
            <span style='font-family:Courier Prime,monospace;font-size:13px;color:#9BA8A6'>📍 {barea}</span>
            <span style='font-family:Courier Prime,monospace;font-size:13px;color:#9BA8A6;margin-left:12px'>🛏 {bedrooms} Bedroom</span>
            <span style='font-family:Courier Prime,monospace;font-size:13px;color:#9BA8A6;margin-left:12px'>📐 {int(unit['sqft']):,} sqft</span>
            <span style='margin-left:12px'>{badge}</span>
        </div>
        """, unsafe_allow_html=True)
    with pdf_area:
        st.markdown("""
        <style>
        div[data-testid="stDownloadButton"] > button {
            background: #C0392B !important; color: #ffffff !important;
            border: none !important; border-radius: 6px !important;
            font-size: 12px !important; font-weight: 700 !important;
            padding: 6px 12px !important;
        }
        div[data-testid="stDownloadButton"] > button:hover { background: #A93226 !important; }
        </style>""", unsafe_allow_html=True)
        _pdf_btn_placeholder = st.empty()

    if st.button("← Back to Listings"):
        st.session_state.page = 'listings'
        st.session_state.selected_unit = None
        st.rerun()

    if reg_type == 'Off-Plan Properties':
        st.markdown("""
        <div style='background:rgba(200,121,65,0.1);border:1px solid rgba(200,121,65,0.3);
        border-radius:10px;padding:14px 18px;color:#E8985A;font-size:13.5px;margin:12px 0'>
            ⚠️ <strong>Off-plan listing.</strong> Premium/discount comparison should be interpreted with caution.
        </div>""", unsafe_allow_html=True)

    section_label("Financial Summary")

    fv_val   = f"AED {int(unit['fair_value_aed']):,}" if has_fair_value else "—"
    fv_col   = "#22C4B0" if has_fair_value else "#5C7070"
    pct_val  = pct_str if has_fair_value else "—"
    pct_c    = pct_color if has_fair_value else "#5C7070"
    conf_val = conf if has_fair_value else "—"
    conf_c   = cc if has_fair_value else "#5C7070"

    def fcard(label, value, color="#EAE6DF", accent=False):
        top = "background:linear-gradient(90deg,#22C4B0,#1A9B8C,transparent)" if accent else "background:transparent"
        bg  = "linear-gradient(135deg,rgba(26,155,140,0.08),#162840)" if accent else "#112030"
        bdr = "rgba(26,155,140,0.22)" if accent else "rgba(255,255,255,0.07)"
        return f"""
        <div style='background:{bg};border:1px solid {bdr};border-radius:8px;
        padding:12px 10px;position:relative;overflow:hidden;text-align:center;'>
            <div style='position:absolute;top:0;left:0;right:0;height:2px;{top}'></div>
            <div style='font-family:Courier Prime,monospace;font-size:9px;font-weight:500;
            letter-spacing:1px;text-transform:uppercase;color:#5C7070;margin-bottom:6px'>{label}</div>
            <div style='font-family:Tenor Sans,serif;font-size:17px;font-weight:600;
            color:{color};line-height:1.2'>{value}</div>
        </div>"""

    cards = [
        fcard("Listed Price",       f"AED {int(unit['listed_price']):,}"),
        fcard("Price per Sqft",     f"AED {int(unit['listed_price'] / unit['sqft']):,}" if unit['sqft'] else "—"),
        fcard("Listed Yield",       _listed_yield_str, _yield_dot_color if _has_yield else "#5C7070"),
        fcard("Fair Value",         fv_val, fv_col, accent=has_fair_value),
        fcard("Fair Value / Sqft",  f"AED {int(unit['median_ppsf']):,}" if has_fair_value and unit.get('median_ppsf') else "—", "#22C4B0" if has_fair_value else "#5C7070"),
        fcard("Estimated Rent",     _est_rent_str, "#22C4B0" if _has_yield else "#5C7070"),
        fcard("Liquidity Grade",    grade, grade_color),
        fcard("Demand Score",       f"{float(unit['demand_score']):.1f} · {unit.get('demand_label','—')}" if unit.get('demand_score') is not None and str(unit.get('demand_score','')) not in ('','nan') else "—",
              demand_colour(unit.get('demand_label'), unit.get('demand_score'))[0]),
        fcard("Recommendation",     f"{rec_emoji} {rec_label}", rec_c),
        fcard("Premium / Discount", pct_val, pct_c),
        fcard("Confidence",         conf_val, conf_c),
        fcard("Match Tier",         unit.get('match_tier','—'), "#9BA8A6"),
    ]

    rows_html3 = ""
    for i in range(0, 12, 3):
        inner = "".join(f"<div style='flex:1'>{cards[j]}</div>" for j in range(i, i+3))
        rows_html3 += f"<div style='display:flex;gap:10px;margin-bottom:10px'>{inner}</div>"

    st.markdown(f"""
    <div style='display:flex;justify-content:center;margin:8px 0 20px'>
        <div style='width:100%;max-width:720px'>
            {rows_html3}
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Demand Score Breakdown ────────────────────────────────────────────────
    import math
    _raw_ds = unit.get('demand_score')
    _has_demand = _raw_ds is not None and str(_raw_ds) not in ('', 'nan', 'None') and not (isinstance(_raw_ds, float) and math.isnan(_raw_ds))
    if _has_demand:
        _pid = unit.get('property_id','')
        _ds_inputs = run_query("""
            SELECT
                nearest_gym, gym_distance_km, gym_duration_min,
                nearest_grocery, grocery_distance_km, grocery_duration_min,
                nearest_medical, medical_distance_km, medical_duration_min,
                nearest_pharmacy, pharmacy_distance_km, pharmacy_duration_min,
                nearest_waterfront, waterfront_distance_km, waterfront_label, waterfront_score,
                dxb_km, dxb_min, dubaimall_km, dubaimall_min,
                businessbay_min, difc_min, downtowndubai_min, dubaiinternetcity_min, dubaimediacity_min,
                gems_min, jess_min, jumeirah_college_min, kingsdubai_min, nordanglia_min,
                existing_competing_stock, future_supply_risk,
                population_growth_area_level, road_transit_connectivity
            FROM demand_score_inputs WHERE property_id = :pid
        """, params={"pid": _pid})

        _ds_view = run_query("""
            SELECT gym_score, grocery_score, medical_score, pharmacy_score,
                   workhub_score, school_score, mall_score, airport_score,
                   waterfront_score, stock_score, supply_score, popgrowth_score, road_score
            FROM demand_score_view WHERE property_id = :pid
        """, params={"pid": _pid})

        if len(_ds_inputs) > 0 and len(_ds_view) > 0:
            di = _ds_inputs.iloc[0]
            dv = _ds_view.iloc[0]

            def _dcard(icon, title, place, score, max_score, detail):
                pct = (score / max_score) * 100
                score_disp = f"{score}/{max_score}" if score == int(score) else f"{score:.1f}/{max_score}"
                return f"""
                <div style='background:#112030;border:1px solid rgba(255,255,255,0.07);border-radius:10px;
                padding:16px;transition:border-color .2s;'>
                    <div style='display:flex;justify-content:space-between;align-items:center;margin-bottom:10px'>
                        <div style='width:32px;height:32px;border-radius:8px;background:rgba(26,155,140,0.12);
                        display:flex;align-items:center;justify-content:center;font-size:15px'>{icon}</div>
                        <span style='font-family:Courier Prime,monospace;font-size:17px;font-weight:500;color:#22C4B0'>{score_disp}</span>
                    </div>
                    <div style='font-size:13px;font-weight:600;color:#EAE6DF;margin-bottom:3px'>{title}</div>
                    <div style='font-size:11.5px;color:#7A8A88;margin-bottom:10px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis'>{place}</div>
                    <div style='height:3px;background:rgba(255,255,255,0.06);border-radius:2px;overflow:hidden;margin-bottom:7px'>
                        <div style='height:100%;width:{pct:.0f}%;background:linear-gradient(90deg,#1A9B8C,#22C4B0);border-radius:2px'></div>
                    </div>
                    <div style='display:flex;justify-content:space-between;font-size:11px;color:#5C7070'>
                        <span>Detail</span><strong style='color:#9BA8A6;font-weight:500'>{detail}</strong>
                    </div>
                </div>"""

            def _mins(v):
                try: return f"{float(v):.0f} min"
                except: return "—"
            def _km(v):
                try: return f"{float(v):.1f} km"
                except: return "—"

            # Work hubs count
            _hub_names = ['businessbay_min','difc_min','downtowndubai_min','dubaiinternetcity_min','dubaimediacity_min']
            _hub_labels = ['Business Bay','DIFC','Downtown','DIC','DMC']
            _hub_in = [l for col,l in zip(_hub_names,_hub_labels) if di.get(col) is not None and float(di.get(col,999)) <= 20]
            _hub_count = len(_hub_in)
            _hub_detail = f"{_hub_count}/5 hubs ≤20 min" + (f" · {', '.join(_hub_in[:2])}" if _hub_in else "")

            # Schools count
            _sch_names = ['gems_min','jess_min','jumeirah_college_min','kingsdubai_min','nordanglia_min']
            _sch_labels = ['GEMS','JESS','JC','Kings','Nord Anglia']
            _sch_in = [l for col,l in zip(_sch_names,_sch_labels) if di.get(col) is not None and float(di.get(col,999)) <= 20]
            _sch_count = len(_sch_in)
            _sch_detail = f"{_sch_count}/5 schools ≤20 min" + (f" · {', '.join(_sch_in[:2])}" if _sch_in else "")

            section_label("Demand Score Breakdown")
            _all_cards = [
                _dcard("🏋️", "Nearest Gym",      di.get('nearest_gym','—') or '—',     float(dv['gym_score']),       5, f"{_km(di.get('gym_distance_km'))} · {_mins(di.get('gym_duration_min'))}"),
                _dcard("🛒", "Nearest Grocery",   di.get('nearest_grocery','—') or '—',  float(dv['grocery_score']),   5, f"{_km(di.get('grocery_distance_km'))} · {_mins(di.get('grocery_duration_min'))}"),
                _dcard("🏥", "Nearest Medical",   di.get('nearest_medical','—') or '—',  float(dv['medical_score']),   5, f"{_km(di.get('medical_distance_km'))} · {_mins(di.get('medical_duration_min'))}"),
                _dcard("💊", "Nearest Pharmacy",  di.get('nearest_pharmacy','—') or '—', float(dv['pharmacy_score']),  5, f"{_km(di.get('pharmacy_distance_km'))} · {_mins(di.get('pharmacy_duration_min'))}"),
                _dcard("🏙️", "Work Hubs",         _hub_detail,                            float(dv['workhub_score']),   5, f"{_hub_count} of 5 hubs within 20 min"),
                _dcard("🎓", "Schools",            _sch_detail,                            float(dv['school_score']),    5, f"{_sch_count} of 5 schools within 20 min"),
                _dcard("🏬", "Dubai Mall",         f"{_km(di.get('dubaimall_km'))} away",  float(dv['mall_score']),      5, f"{_km(di.get('dubaimall_km'))} · {_mins(di.get('dubaimall_min'))}"),
                _dcard("✈️", "DXB Airport",        f"{_km(di.get('dxb_km'))} away",        float(dv['airport_score']),   5, f"{_km(di.get('dxb_km'))} · {_mins(di.get('dxb_min'))}"),
                _dcard("🌊", "Waterfront",         di.get('waterfront_label','—') or '—',  float(dv['waterfront_score']),5, f"{_km(di.get('waterfront_distance_km'))} · Score {di.get('waterfront_score','—')}/5"),
                _dcard("🏗️", "Future Supply Risk", "Area-level assessment",                float(dv['supply_score']),    5, f"Score {di.get('future_supply_risk','—')}/5 · lower risk = higher score"),
                _dcard("📈", "Population Growth",  "Area-level assessment",                float(dv['popgrowth_score']), 5, f"Score {di.get('population_growth_area_level','—')}/5"),
                _dcard("🏢", "Existing Stock",     "Competing inventory",                  float(dv['stock_score']),     5, f"Score {di.get('existing_competing_stock','—')}/5 · lower stock = higher score"),
                _dcard("🚇", "Road & Transit",     "Connectivity assessment",              float(dv['road_score']),      5, f"Score {di.get('road_transit_connectivity','—')}/5"),
            ]
            for _row_start in range(0, 13, 4):
                _row_cards = _all_cards[_row_start:_row_start+4]
                # Always use 4 columns, pad with empty divs so Road & Transit stays same size
                _cols = st.columns(4)
                for _ci in range(4):
                    with _cols[_ci]:
                        if _ci < len(_row_cards):
                            st.markdown(_row_cards[_ci], unsafe_allow_html=True)
                        else:
                            st.markdown("<div></div>", unsafe_allow_html=True)
            st.markdown("<div style='margin-bottom:32px'></div>", unsafe_allow_html=True)

            # Build demand rows for PDF
            def _km_pdf(v):
                try: return f"{float(v):.1f} km"
                except: return "—"
            def _min_pdf(v):
                try: return f"{float(v):.0f} min"
                except: return "—"
            _pdf_demand_rows = (
                f"<tr><td>🏋️ Gym</td><td>{float(dv['gym_score']):.1f}/5</td><td>{di.get('nearest_gym','—')} · {_km_pdf(di.get('gym_distance_km'))} · {_min_pdf(di.get('gym_duration_min'))}</td></tr>"
                f"<tr><td>🛒 Grocery</td><td>{float(dv['grocery_score']):.1f}/5</td><td>{di.get('nearest_grocery','—')} · {_km_pdf(di.get('grocery_distance_km'))} · {_min_pdf(di.get('grocery_duration_min'))}</td></tr>"
                f"<tr><td>🏥 Medical</td><td>{float(dv['medical_score']):.1f}/5</td><td>{di.get('nearest_medical','—')} · {_km_pdf(di.get('medical_distance_km'))} · {_min_pdf(di.get('medical_duration_min'))}</td></tr>"
                f"<tr><td>💊 Pharmacy</td><td>{float(dv['pharmacy_score']):.1f}/5</td><td>{di.get('nearest_pharmacy','—')} · {_km_pdf(di.get('pharmacy_distance_km'))} · {_min_pdf(di.get('pharmacy_duration_min'))}</td></tr>"
                f"<tr><td>🏙️ Work Hubs</td><td>{float(dv['workhub_score']):.1f}/5</td><td>{_hub_count} of 5 hubs within 20 min</td></tr>"
                f"<tr><td>🎓 Schools</td><td>{float(dv['school_score']):.1f}/5</td><td>{_sch_count} of 5 schools within 20 min</td></tr>"
                f"<tr><td>🏬 Dubai Mall</td><td>{float(dv['mall_score']):.1f}/5</td><td>{_km_pdf(di.get('dubaimall_km'))} · {_min_pdf(di.get('dubaimall_min'))}</td></tr>"
                f"<tr><td>✈️ DXB Airport</td><td>{float(dv['airport_score']):.1f}/5</td><td>{_km_pdf(di.get('dxb_km'))} · {_min_pdf(di.get('dxb_min'))}</td></tr>"
                f"<tr><td>🌊 Waterfront</td><td>{float(dv['waterfront_score']):.1f}/5</td><td>{di.get('waterfront_label','—')}</td></tr>"
                f"<tr><td>🏗️ Supply Risk</td><td>{float(dv['supply_score']):.1f}/5</td><td>Score {di.get('future_supply_risk','—')}/5</td></tr>"
                f"<tr><td>📈 Pop. Growth</td><td>{float(dv['popgrowth_score']):.1f}/5</td><td>Score {di.get('population_growth_area_level','—')}/5</td></tr>"
                f"<tr><td>🏢 Existing Stock</td><td>{float(dv['stock_score']):.1f}/5</td><td>Score {di.get('existing_competing_stock','—')}/5</td></tr>"
                f"<tr><td>🚇 Road &amp; Transit</td><td>{float(dv['road_score']):.1f}/5</td><td>Score {di.get('road_transit_connectivity','—')}/5</td></tr>"
            )

    section_label("Performance Analysis")

    chart_data = get_chart_data(building)
    ch_left, ch_right = st.columns(2)

    with ch_left:
        # Yield vs Demand Score scatter
        _yield_val  = _listed_yield if _has_yield else None
        _demand_val = unit.get('demand_score')
        _has_both   = _yield_val is not None and _demand_val is not None and str(_demand_val) not in ('','nan')
        if _has_both:
            _dy = float(_demand_val)
            _yy = float(_yield_val)
            _rec_label_s, _rec_emoji_s, _rc_s, _, _ = get_recommendation(
                unit.get('listed_price'), unit.get('fair_value_aed'), _yield_val, _demand_val, grade
            )
            fig_scatter = go.Figure()
            fig_scatter.add_shape(type="rect", x0=28, x1=65, y0=0, y1=6.5,
                fillcolor="rgba(220,60,60,0.15)", line_width=0, layer="below")
            fig_scatter.add_shape(type="rect", x0=65, x1=90, y0=6.5, y1=20,
                fillcolor="rgba(46,184,122,0.18)", line_width=0, layer="below")
            fig_scatter.add_shape(type="rect", x0=65, x1=90, y0=0, y1=6.5,
                fillcolor="rgba(232,180,50,0.15)", line_width=0, layer="below")
            fig_scatter.add_shape(type="rect", x0=28, x1=65, y0=6.5, y1=20,
                fillcolor="rgba(232,180,50,0.15)", line_width=0, layer="below")
            fig_scatter.add_shape(type="line", x0=65, x1=65, y0=0, y1=20,
                line=dict(color="rgba(255,255,255,0.2)", width=1, dash="dot"))
            fig_scatter.add_shape(type="line", x0=28, x1=90, y0=6.5, y1=6.5,
                line=dict(color="rgba(255,255,255,0.2)", width=1, dash="dot"))
            fig_scatter.add_trace(go.Scatter(
                x=[_dy], y=[_yy],
                mode='markers',
                marker=dict(size=14, color='#000000', line=dict(color='white', width=2)),
                hovertemplate=f"Demand: {_dy:.1f}<br>Yield: {_yy:.2f}%<extra></extra>",
            ))
            fig_scatter.add_annotation(
                x=_dy, y=_yy,
                text=f"{_rec_emoji_s} {_rec_label_s}",
                showarrow=False,
                yshift=18,
                font=dict(color='#EAE6DF', size=12, family='Courier Prime'),
            )
            fig_scatter.update_layout(
                title=dict(text="Yield vs Demand Score", font=dict(color='#9BA8A6', size=14, family="Tenor Sans"), x=0),
                height=340, plot_bgcolor='rgba(17,32,48,1)', paper_bgcolor='rgba(17,32,48,1)',
                font=dict(family='Nunito Sans', color='#9BA8A6', size=11),
                margin=dict(l=40, r=10, t=74, b=40),
                xaxis=dict(title="Demand Score", range=[28, 90], gridcolor='rgba(255,255,255,0.04)',
                           tickfont=dict(color='#5C7070'), title_font=dict(color='#5C7070')),
                yaxis=dict(title="Listed Yield (%)", gridcolor='rgba(255,255,255,0.04)',
                           tickfont=dict(color='#5C7070'), title_font=dict(color='#5C7070')),
                showlegend=False,
            )
            st.plotly_chart(fig_scatter, use_container_width=True, key="yield_demand_chart")
        else:
            st.markdown("""
            <div style='background:#112030;border:1px solid rgba(255,255,255,0.07);border-radius:10px;
            padding:20px;height:340px;display:flex;align-items:center;justify-content:center;flex-direction:column'>
                <div style='font-family:Courier Prime,monospace;font-size:11px;letter-spacing:1px;
                text-transform:uppercase;color:#5C7070;margin-bottom:8px'>Yield vs Demand Score</div>
                <div style='color:#5C7070;font-size:13px'>Yield or demand data not available</div>
            </div>""", unsafe_allow_html=True)

    with ch_right:
        if len(chart_data) > 0:
            chart_data['quarter'] = pd.to_datetime(chart_data['quarter'], utc=True).dt.tz_convert(None)
            all_beds = sorted(chart_data['bedrooms'].unique())

            # ── Bedroom filter pills (compact, single row) ────────────────
            _bed_labels = {"Studio": "S"}
            _all_opts = ["All"] + [_bed_labels.get(b, str(b)) for b in all_beds]
            _cur_sel = st.session_state.detail_chart_beds  # None=All, else set of raw bed values

            _sel_display = ["All"] if _cur_sel is None else [_bed_labels.get(b, str(b)) for b in _cur_sel]
            _new_sel = st.pills("Bedrooms", _all_opts, selection_mode="multi",
                                default=_sel_display, key="detail_bed_pills",
                                label_visibility="collapsed")
            # Translate back to raw bed values
            if _new_sel is None or "All" in _new_sel or len(_new_sel) == 0:
                st.session_state.detail_chart_beds = None
            else:
                _rev = {_bed_labels.get(b, str(b)): b for b in all_beds}
                st.session_state.detail_chart_beds = {_rev[x] for x in _new_sel if x in _rev}

            # ── Build chart ───────────────────────────────────────────────
            active_beds = all_beds if st.session_state.detail_chart_beds is None else [
                b for b in all_beds if b in st.session_state.detail_chart_beds]
            fig2 = go.Figure()
            for idx, bed in enumerate(all_beds):
                if bed not in active_beds:
                    continue
                bed_data = chart_data[chart_data['bedrooms'] == bed]
                is_sel = (bed == bedrooms)
                fig2.add_trace(go.Scatter(
                    x=bed_data['quarter'], y=bed_data['median_ppsf'],
                    name=f"{bed} BR", mode='lines+markers',
                    line=dict(color=bed_color(bed), width=3),
                    marker=dict(size=5 if is_sel else 3),
                ))
            fig2.update_layout(
                title=dict(text="Historical Price History — Median Building Price Per SQFT", font=dict(color='#9BA8A6', size=14, family="Tenor Sans"), x=0),
                height=340, plot_bgcolor='rgba(17,32,48,1)', paper_bgcolor='rgba(17,32,48,1)',
                font=dict(family='Nunito Sans', color='#9BA8A6', size=11),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(color='#9BA8A6', size=10)),
                margin=dict(l=0, r=0, t=40, b=0), hovermode="x unified",
                xaxis=dict(gridcolor='rgba(255,255,255,0.04)', showline=False),
                yaxis=dict(gridcolor='rgba(255,255,255,0.04)', tickfont=dict(color='#5C7070')),
            )
            st.plotly_chart(fig2, width='stretch', key="detail_price_chart")
        else:
            st.markdown("""
            <div style='background:#112030;border:1px solid rgba(255,255,255,0.07);border-radius:10px;
            padding:20px;height:340px;display:flex;align-items:center;justify-content:center;flex-direction:column'>
                <div style='font-family:Courier Prime,monospace;font-size:11px;letter-spacing:1px;
                text-transform:uppercase;color:#5C7070;margin-bottom:8px'>Historical Price History</div>
                <div style='color:#5C7070;font-size:13px'>No data available</div>
            </div>""", unsafe_allow_html=True)

    # ── Build final PDF now that all data (demand rows) is available ─────────
    # Build chart data for PDF
    _chart_data_pdf = get_chart_data(building)
    _chart_js = ""
    if len(_chart_data_pdf) > 0:
        try:
            _chart_data_pdf['quarter'] = pd.to_datetime(_chart_data_pdf['quarter'], utc=True).dt.tz_convert(None)
            _all_beds_pdf = sorted(_chart_data_pdf['bedrooms'].unique())
            _chart_colors = ['#22C4B0','#E8985A','#2EB87A','#E05C5C','#A98FD0','#C87941']
            _datasets = []
            for _bi, _b in enumerate(_all_beds_pdf):
                _bd = _chart_data_pdf[_chart_data_pdf['bedrooms'] == _b].sort_values('quarter')
                _labels = [str(q)[:7] for q in _bd['quarter']]
                _vals   = [int(v) if not pd.isna(v) else None for v in _bd['median_ppsf']]
                _col = _chart_colors[_bi % len(_chart_colors)]
                _datasets.append(f"{{'label':'{_b} BR','data':{_vals},'borderColor':'{_col}','backgroundColor':'transparent','tension':0.3,'pointRadius':3}}")
            _labels_json = str([str(q)[:7] for q in _chart_data_pdf[_chart_data_pdf['bedrooms']==_all_beds_pdf[0]].sort_values('quarter')['quarter']])
            _datasets_json = "[" + ",".join(_datasets) + "]"
            _chart_js = f"""
            <h2>Historical Price Per Sqft</h2>
            <canvas id="priceChart" height="120"></canvas>
            <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
            <script>
            new Chart(document.getElementById('priceChart'), {{
                type: 'line',
                data: {{ labels: {_labels_json}, datasets: {_datasets_json} }},
                options: {{ responsive: true, plugins: {{ legend: {{ position: 'top' }} }},
                    scales: {{ y: {{ title: {{ display: true, text: 'AED / sqft' }} }} }} }}
            }});
            </script>"""
        except: pass

    # Yield vs Demand chart
    _scatter_js = ""
    if _has_yield and unit.get('demand_score') is not None and str(unit.get('demand_score','')) not in ('','nan'):
        try:
            _dy_pdf = float(unit['demand_score'])
            _yy_pdf = float(_listed_yield)
            _scatter_js = f"""
            <h2>Yield vs Demand Score</h2>
            <canvas id="scatterChart" height="120"></canvas>
            <script>
            new Chart(document.getElementById('scatterChart'), {{
                type: 'scatter',
                data: {{ datasets: [{{
                    label: 'This Property',
                    data: [{{ x: {_dy_pdf}, y: {_yy_pdf} }}],
                    backgroundColor: '#000000',
                    borderColor: '#ffffff',
                    borderWidth: 2,
                    pointRadius: 10
                }}] }},
                options: {{ responsive: true,
                    scales: {{
                        x: {{ title: {{ display: true, text: 'Demand Score' }}, min: 28, max: 90 }},
                        y: {{ title: {{ display: true, text: 'Listed Yield (%)' }} }}
                    }}
                }}
            }});
            </script>"""
        except: pass

    _pdf_html3_final = f"""<html><head><style>
    body{{font-family:Arial,sans-serif;padding:32px;color:#111;}}
    h1{{font-size:22px;margin-bottom:4px;}} .meta{{color:#555;font-size:12px;margin-bottom:20px;}}
    h2{{font-size:15px;margin:20px 0 8px;color:#1A9B8C;border-bottom:1px solid #eee;padding-bottom:4px;}}
    table{{border-collapse:collapse;width:100%;font-size:12px;margin-bottom:16px;}}
    th{{background:#1A9B8C;color:#fff;padding:7px 10px;text-align:left;}}
    td{{padding:6px 10px;border-bottom:1px solid #eee;}}
    </style></head><body>
    <h1>{bname}</h1>
    <div class="meta">📍 {barea} · 🛏 {bedrooms} Bedroom · 📐 {int(unit['sqft']):,} sqft</div>
    <h2>Financial Summary</h2>
    <table><tr><th>Field</th><th>Value</th></tr>
    {_pdf_financial_rows}
    </table>
    <h2>Demand Score Breakdown</h2>
    <table><tr><th>Factor</th><th>Score</th><th>Detail</th></tr>
    {_pdf_demand_rows}
    </table>
    {_chart_js}
    {_scatter_js}
    </body></html>"""
    _pdf_btn_placeholder.download_button(
        "⬇ PDF",
        data=_pdf_html3_final.encode(),
        file_name=f"{bname}_{bedrooms}br.html",
        mime="text/html",
        key="pdf_dl3"
    )

    if unit.get('url') and pd.notna(unit.get('url')):
        st.markdown(f"""
        <div style='margin-top:20px'>
            <a href="{unit['url']}" target="_blank"
            style='display:inline-flex;align-items:center;gap:8px;padding:10px 22px;
            background:rgba(26,155,140,0.13);border:1px solid rgba(26,155,140,0.22);
            border-radius:8px;color:#22C4B0;font-size:13px;font-weight:600;
            text-decoration:none;font-family:Nunito Sans,sans-serif'>
                View on Bayut →
            </a>
        </div>""", unsafe_allow_html=True)

    st.markdown("</div>", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 4 — COMPARE
# ═══════════════════════════════════════════════════════════════════════════════
elif st.session_state.page == 'compare':

    nav_bar()
    st.markdown("<div style='padding:24px 48px 80px'>", unsafe_allow_html=True)

    compare_units = st.session_state.compare_units
    filled = len(compare_units)

    # ── Page header — centred, large ──────────────────────────────────────────
    st.markdown("""
    <div style='text-align:center;padding:28px 0 36px'>
        <div style='font-family:Tenor Sans,serif;font-size:44px;font-weight:700;
        color:#EAE6DF;letter-spacing:-.5px;margin-bottom:10px;line-height:1.15'>
            Compare <em style='font-style:italic;color:#22C4B0'>Properties</em>
        </div>
        <div style='color:#9BA8A6;font-size:16px;font-weight:300;letter-spacing:.3px'>
            Add up to 5 properties · compare side by side
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Split layout: indented from edges ─────────────────────────────────────
    _, left_col, right_col, _ = st.columns([0.5, 3, 4, 1.5], gap="large")

    with left_col:

        # Search input — always on top
        if filled < 5:
            section_label("Search a Building")
            cmp_search = st.text_input(
                "cmp_search",
                placeholder="🔍  Building name...",
                label_visibility="collapsed",
                key="cmp_search_input"
            )
        else:
            cmp_search = ""
            st.markdown("<div style='font-family:Courier Prime,monospace;font-size:10px;letter-spacing:1.5px;text-transform:uppercase;color:#5C7070;margin-bottom:8px'>Maximum 5 properties selected</div>", unsafe_allow_html=True)

        if cmp_search and len(cmp_search) >= 2:
            cmp_buildings = run_query("""
                SELECT DISTINCT building_name FROM dashboard_listings
                WHERE building_name ILIKE :search
                ORDER BY building_name LIMIT 12
            """, params={"search": f"%{cmp_search}%"})

            if len(cmp_buildings) == 0:
                st.markdown("<p style='color:#5C7070;font-size:13px'>No buildings found.</p>", unsafe_allow_html=True)
            else:
                for _, brow in cmp_buildings.iterrows():
                    bparts = brow['building_name'].split(',')
                    bn = bparts[0].strip()
                    ba = bparts[1].strip() if len(bparts) > 1 else ''
                    bl, br = st.columns([5, 2])
                    with bl:
                        st.markdown(f"""
                        <div style='padding:9px 2px 3px'>
                            <div style='font-family:Nunito Sans,sans-serif;font-size:13px;
                            font-weight:600;color:#EAE6DF'>{bn}</div>
                            <div style='font-size:11px;color:#5C7070'>{ba}</div>
                        </div>
                        """, unsafe_allow_html=True)
                    with br:
                        if st.button("View Units", key=f"cmpb_{brow['building_name']}"):
                            st.session_state.compare_building_selected = brow['building_name']
                            st.rerun()


    # ── Right column: unit picker ─────────────────────────────────────────────
    with right_col:

        # Selected tray + buttons at top of right col
        if filled > 0:
            section_label(f"Selected — {filled} of 5")
            for i, u in enumerate(compare_units):
                uparts = u['_building'].split(',')
                uname = uparts[0].strip()
                c1, c2 = st.columns([5, 1])
                with c1:
                    st.markdown(f"""
                    <div style='background:#112030;border:1px solid rgba(26,155,140,0.2);
                    border-left:3px solid #22C4B0;border-radius:8px;
                    padding:10px 14px;margin-bottom:6px'>
                        <div style='font-family:Nunito Sans,sans-serif;font-size:13px;
                        font-weight:700;color:#EAE6DF;margin-bottom:3px;
                        white-space:nowrap;overflow:hidden;text-overflow:ellipsis'>{uname}</div>
                        <div style='font-family:Courier Prime,monospace;font-size:10px;color:#5C7070'>
                            {u["bedrooms"]} BR · {int(u["sqft"]):,} sqft
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                with c2:
                    if st.button("\u2715", key=f"rm_{i}"):
                        st.session_state.compare_units.pop(i)
                        st.rerun()

            st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
            st.markdown("""
            <style>
            div[data-testid="stButton"] button[kind="primary"] {
                background: linear-gradient(135deg, #1A9B8C, #22C4B0) !important;
                color: #080E14 !important; border: none !important;
                font-weight: 700 !important; font-size: 12px !important;
                letter-spacing: .8px !important; border-radius: 8px !important;
                padding: 8px 16px !important;
            }
            </style>
            """, unsafe_allow_html=True)
            btn_c1, btn_c2 = st.columns([1, 2])
            with btn_c1:
                if st.button("Clear all", key="clear_all_cmp"):
                    st.session_state.compare_units = []
                    st.session_state.compare_building_selected = None
                    st.rerun()
            with btn_c2:
                if filled >= 2:
                    if st.button(f"View Comparison ({filled}) \u2192", key="view_cmp_btn", type="primary"):
                        st.session_state.page = 'compare_results'
                        st.rerun()
            st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)

        # Unit picker
        if st.session_state.compare_building_selected and filled < 5:
            sel_building = st.session_state.compare_building_selected
            sel_parts = sel_building.split(',')
            sel_bname = sel_parts[0].strip()
            sel_barea = sel_parts[1].strip() if len(sel_parts) > 1 else ''

            cmp_listings = run_query("""
                SELECT d.property_id, d.number_of_bedrooms as bedrooms, d.sqft, d.listed_price,
                       d.fair_value_aed, d.premium_discount_pct, d.confidence_score,
                       d.median_ppsf, d.match_tier, d.match_field, d.reg_type_en, d.url,
                       d.demand_score, d.demand_label,
                       y.listed_yield_pct, y.estimated_annual_rent, y.fair_value_yield_pct, y.yield_confidence
                FROM dashboard_listings d
                LEFT JOIN yield_results y ON y.property_id = d.property_id
                    AND y.number_of_bedrooms = d.number_of_bedrooms
                    AND y.sqft = d.sqft
                    AND y.listed_price = d.listed_price
                WHERE d.building_name = :building
                ORDER BY
                    CASE d.number_of_bedrooms
                        WHEN 'Studio' THEN 0 WHEN '1' THEN 1 WHEN '2' THEN 2
                        WHEN '3' THEN 3 WHEN '4' THEN 4 WHEN '5+' THEN 5 ELSE 6
                    END, d.sqft
            """, params={"building": sel_building})

            section_label(sel_bname)
            st.markdown(f"<div style='font-size:11px;color:#5C7070;margin-bottom:12px;font-family:Courier Prime,monospace'>📍 {sel_barea} · {len(cmp_listings)} units</div>", unsafe_allow_html=True)

            TH3 = "padding:8px 12px;border-bottom:1px solid rgba(255,255,255,0.1);text-align:center;font-family:Courier Prime,monospace;font-size:10px;font-weight:700;color:#9BA8A6;letter-spacing:1px;text-transform:uppercase;white-space:nowrap;background:#0D1520;"
            TD3 = "padding:9px 12px;border-bottom:1px solid rgba(255,255,255,0.04);vertical-align:middle;text-align:center;white-space:nowrap;"

            rows_html = ""
            add_btns = []

            for pi, prow in cmp_listings.iterrows():
                already = any(
                    u['_building'] == sel_building and
                    int(u['sqft']) == int(prow['sqft']) and
                    u['bedrooms'] == prow['bedrooms'] and
                    int(u['listed_price']) == int(prow['listed_price'])
                    for u in compare_units
                )

                beds_p  = pill(str(prow['bedrooms']), "#7EB8D4", "rgba(126,184,212,0.12)", "rgba(126,184,212,0.3)")
                sqft_p  = pill(f"{int(prow['sqft']):,} sqft", "#A98FD0", "rgba(169,143,208,0.12)", "rgba(169,143,208,0.3)")
                price_p = pill(f"AED {int(prow['listed_price']):,}", "#22C4B0", "rgba(26,155,140,0.12)", "rgba(26,155,140,0.3)")

                if already:
                    action = f"<span style='color:#3a5a4a;font-family:Courier Prime,monospace;font-size:10px;letter-spacing:1px'>✓ ADDED</span>"
                else:
                    action = f"<span class='add-btn' data-idx='{pi}' style='background:rgba(26,155,140,0.12);color:#22C4B0;border:1px solid rgba(26,155,140,0.3);padding:4px 14px;border-radius:50px;font-size:11px;font-weight:600;font-family:Courier Prime,monospace;cursor:pointer;letter-spacing:.5px'>+ Add</span>"

                rows_html += f"""<tr>
                    <td style='{TD3}'>{beds_p}</td>
                    <td style='{TD3}'>{sqft_p}</td>
                    <td style='{TD3}'>{price_p}</td>
                    <td style='{TD3}'>{action}</td>
                </tr>"""
                add_btns.append((pi, prow))

            st.markdown(f"""
            <div style='border:1px solid rgba(26,155,140,0.15);border-radius:10px;overflow:hidden'>
            <table style='border-collapse:collapse;width:100%'>
                <thead><tr>
                    <th style='{TH3}'>Beds</th>
                    <th style='{TH3}'>Sqft</th>
                    <th style='{TH3}'>Listed Price</th>
                    <th style='{TH3}'></th>
                </tr></thead>
                <tbody style='background:#112030'>{rows_html}</tbody>
            </table></div>
            """, unsafe_allow_html=True)

            # Hidden st.buttons wired to + Add spans via JS
            for pi, prow in add_btns:
                if st.button(f"__add_{pi}", key=f"addunit_{pi}"):
                    if len(st.session_state.compare_units) < 5:
                        unit_dict = prow.to_dict()
                        unit_dict['_building'] = sel_building
                        # Sanitise NaN yield values so they don't silently break comparisons
                        for _yf in ('listed_yield_pct','fair_value_yield_pct','estimated_annual_rent','yield_confidence'):
                            v = unit_dict.get(_yf)
                            if v is not None:
                                try:
                                    import math
                                    if math.isnan(float(v)):
                                        unit_dict[_yf] = None
                                except: pass
                        st.session_state.compare_units.append(unit_dict)
                        st.session_state.compare_building_selected = None
                        st.rerun()

            components.html("""
            <script>
            (function() {
                function wire() {
                    var doc = window.parent.document;
                    doc.querySelectorAll('button').forEach(function(b) {
                        if (b.innerText.trim().startsWith('__add_')) {
                            b.parentElement.style.cssText = 'position:fixed;top:-9999px;left:-9999px;';
                        }
                    });
                    doc.querySelectorAll('.add-btn').forEach(function(span) {
                        if (span._wired) return;
                        span._wired = true;
                        span.addEventListener('click', function() {
                            var idx = this.getAttribute('data-idx');
                            doc.querySelectorAll('button').forEach(function(b) {
                                if (b.innerText.trim() === '__add_' + idx) b.click();
                            });
                        });
                    });
                }
                wire(); setTimeout(wire, 400); setTimeout(wire, 1000);
            })();
            </script>
            """, height=0)

        elif not st.session_state.compare_building_selected:
            st.markdown("""
            <div style='height:200px;display:flex;align-items:center;justify-content:center;
            border:1px dashed rgba(255,255,255,0.06);border-radius:12px;margin-top:40px'>
                <div style='text-align:center'>
                    <div style='font-family:Courier Prime,monospace;font-size:11px;
                    letter-spacing:2px;text-transform:uppercase;color:#2a3a3a;margin-bottom:8px'>
                        Units appear here
                    </div>
                    <div style='font-size:12px;color:#2a3a3a'>
                        Search a building on the left and click View Units
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("</div>", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 5 — COMPARE RESULTS
# ═══════════════════════════════════════════════════════════════════════════════
elif st.session_state.page == 'compare_results':

    nav_bar()
    st.markdown("<div style='padding:24px 48px 80px'>", unsafe_allow_html=True)

    compare_units = st.session_state.compare_units
    filled = len(compare_units)

    # Header row: back | title | PDF
    hdr_back, hdr_title, hdr_pdf = st.columns([2, 5, 2])
    with hdr_back:
        if st.button("← Edit Selection", key="back_to_compare"):
            st.session_state.page = 'compare'
            st.rerun()
    with hdr_title:
        st.markdown(
            "<div style='padding:10px 0 4px;text-align:center'>"
            "<div style='font-family:Tenor Sans,serif;font-size:32px;font-weight:700;"
            "color:#EAE6DF;letter-spacing:-.3px'>Comparison</div></div>",
            unsafe_allow_html=True
        )

    if filled < 2:
        st.markdown("<p style='color:#5C7070'>Add at least 2 properties to compare.</p>", unsafe_allow_html=True)
    else:
        # Enrich — also fetch fresh yield data per unit
        enriched = []
        for u in compare_units:
            grade_u, _, grade_color_u = get_building_liquidity(u['_building'], u['bedrooms'], u.get('match_tier',''))
            has_fv_u = u.get('fair_value_aed') is not None and pd.notna(u.get('fair_value_aed'))
            # Fetch yield fresh from DB matching exact listing
            _yr = run_query("""
                SELECT listed_yield_pct, fair_value_yield_pct, estimated_annual_rent, yield_confidence
                FROM yield_results
                WHERE property_id = :pid 
                  AND number_of_bedrooms = :beds
                  AND sqft = :sqft
                  AND listed_price = :price
                LIMIT 1
            """, params={
                "pid":   u.get('property_id',''),
                "beds":  u.get('bedrooms',''),
                "sqft":  u.get('sqft'),
                "price": u.get('listed_price')
            })
            if len(_yr) > 0:
                u['listed_yield_pct']      = _yr.iloc[0]['listed_yield_pct']
                u['fair_value_yield_pct']  = _yr.iloc[0]['fair_value_yield_pct']
                u['estimated_annual_rent'] = _yr.iloc[0]['estimated_annual_rent']
                u['yield_confidence']      = _yr.iloc[0]['yield_confidence']
            enriched.append({'unit': u, 'grade': grade_u, 'grade_color': grade_color_u, 'has_fv': has_fv_u})

        # ── Pre-compute summary texts (used in both PDF and on-screen) ─────────
        def _prop_name(e):
            base = e['unit']['_building'].split(',')[0].strip()
            beds = e['unit'].get('bedrooms', '')
            sqft = e['unit'].get('sqft', '')
            price = e['unit'].get('listed_price', '')
            bed_label = 'Studio' if beds == 'Studio' else f"{beds} BR"
            try:
                price_label = f"AED {int(float(price)):,}"
            except: price_label = ''
            try:
                sqft_label = f"{int(float(sqft)):,} sqft"
            except: sqft_label = ''
            return f"{base} ({bed_label}, {sqft_label}, {price_label})"

        _value_candidates = [e for e in enriched if e['has_fv'] and e['unit'].get('premium_discount_pct') is not None]
        if _value_candidates:
            _best_value = min(_value_candidates, key=lambda e: float(e['unit']['premium_discount_pct']))
            _bv_pct = float(_best_value['unit']['premium_discount_pct'])
            _bv_name = _prop_name(_best_value)
            if _bv_pct <= 0:
                _bv_txt = f"<b>{_bv_name}</b> offers the best price entry, trading at <b>{abs(_bv_pct):.1f}% below fair value</b> — you're buying at a discount to what the data says it's worth."
            else:
                _bv_txt = f"<b>{_bv_name}</b> is the closest to fair value at <b>{_bv_pct:.1f}% above</b> — no property in this comparison is at a discount, so this is the least overpriced option."
        else:
            _bv_txt = "Fair value data is not available for these properties, so a price comparison cannot be made."

        _yield_candidates = [e for e in enriched if e['unit'].get('listed_yield_pct') is not None and str(e['unit'].get('listed_yield_pct','')) not in ('','nan')]
        if _yield_candidates:
            _best_yield = max(_yield_candidates, key=lambda e: float(e['unit']['listed_yield_pct']))
            _by_pct = float(_best_yield['unit']['listed_yield_pct'])
            _by_name = _prop_name(_best_yield)
            if len(_yield_candidates) > 1:
                _worst_yield = min(_yield_candidates, key=lambda e: float(e['unit']['listed_yield_pct']))
                _wy_pct = float(_worst_yield['unit']['listed_yield_pct'])
                _wy_name = _prop_name(_worst_yield)
                if _by_name != _wy_name:
                    _by_txt = f"<b>{_by_name}</b> leads on rental income with a listed yield of <b>{_by_pct:.1f}%</b>, compared to {_wy_pct:.1f}% for {_wy_name}. {'That\'s a strong income return.' if _by_pct >= 7 else 'A solid yield for this market.' if _by_pct >= 5 else 'Yields are relatively modest across this comparison.'}"
                else:
                    _by_txt = f"All properties show similar yields. <b>{_by_name}</b> edges ahead at <b>{_by_pct:.1f}%</b>."
            else:
                _by_txt = f"<b>{_by_name}</b> is the only property with yield data, showing <b>{_by_pct:.1f}%</b> listed yield."
        else:
            _by_txt = "Yield data is not available for these properties."

        _demand_candidates = [e for e in enriched if e['unit'].get('demand_score') is not None and str(e['unit'].get('demand_score','')) not in ('','nan')]
        if _demand_candidates:
            _best_demand = max(_demand_candidates, key=lambda e: float(e['unit']['demand_score']))
            _bd_score = float(_best_demand['unit']['demand_score'])
            _bd_label = _best_demand['unit'].get('demand_label','—')
            _bd_name = _prop_name(_best_demand)
            if len(_demand_candidates) > 1:
                _worst_demand = min(_demand_candidates, key=lambda e: float(e['unit']['demand_score']))
                _wd_score = float(_worst_demand['unit']['demand_score'])
                _wd_name = _prop_name(_worst_demand)
                if _bd_name != _wd_name:
                    _bd_txt = f"<b>{_bd_name}</b> scores highest on location and amenities with a demand score of <b>{_bd_score:.1f}</b> ({_bd_label}), reflecting stronger proximity to schools, retail, transport and lifestyle infrastructure. {_wd_name} scores {_wd_score:.1f} by comparison."
                else:
                    _bd_txt = f"Demand scores are evenly matched. <b>{_bd_name}</b> leads with <b>{_bd_score:.1f}</b> ({_bd_label})."
            else:
                _bd_txt = f"<b>{_bd_name}</b> has a demand score of <b>{_bd_score:.1f}</b> ({_bd_label})."
        else:
            _bd_txt = "Demand score data is not available for these properties."

        def _bullish_count(e):
            label, _, _, _, _ = get_recommendation(
                e['unit'].get('listed_price'), e['unit'].get('fair_value_aed'),
                e['unit'].get('listed_yield_pct'), e['unit'].get('demand_score'), e['grade']
            )
            return {"Strong Entry": 4, "Good Entry": 3, "OK Entry": 2, "Weak Entry": 1, "Poor Entry": 0}.get(label, 0)

        _scored = sorted(enriched, key=lambda e: (
            _bullish_count(e),
            float(e['unit'].get('listed_yield_pct') or 0),
            -float(e['unit'].get('premium_discount_pct') or 0)
        ), reverse=True)

        _overall = _scored[0]
        _ov_name = _prop_name(_overall)
        _ov_label, _ov_emoji, _, _, _ = get_recommendation(
            _overall['unit'].get('listed_price'), _overall['unit'].get('fair_value_aed'),
            _overall['unit'].get('listed_yield_pct'), _overall['unit'].get('demand_score'), _overall['grade']
        )
        _ov_reasons = []
        if _overall['has_fv'] and _overall['unit'].get('premium_discount_pct') is not None:
            _ov_p = float(_overall['unit']['premium_discount_pct'])
            if _ov_p <= 0:
                _ov_reasons.append(f"priced {abs(_ov_p):.1f}% below fair value")
            elif _ov_p <= 10:
                _ov_reasons.append("close to fair value")
        if _overall['unit'].get('listed_yield_pct') is not None and str(_overall['unit'].get('listed_yield_pct','')) not in ('','nan'):
            _ov_y = float(_overall['unit']['listed_yield_pct'])
            if _ov_y >= 6.5:
                _ov_reasons.append(f"strong yield of {_ov_y:.1f}%")
            elif _ov_y >= 5:
                _ov_reasons.append(f"solid yield of {_ov_y:.1f}%")
        if _overall['unit'].get('demand_score') is not None and str(_overall['unit'].get('demand_score','')) not in ('','nan'):
            _ov_d = float(_overall['unit']['demand_score'])
            if _ov_d >= 65:
                _ov_reasons.append(f"demand score of {_ov_d:.1f}")
        if _overall['grade'] in ('A+', 'A'):
            _ov_reasons.append(f"strong liquidity ({_overall['grade']})")
        if _ov_reasons:
            _ov_reason_txt = ", ".join(_ov_reasons[:-1]) + (" and " + _ov_reasons[-1] if len(_ov_reasons) > 1 else _ov_reasons[0])
            _ov_txt = f"Taking all four factors together, <b>{_ov_name}</b> comes out on top — {_ov_reason_txt}."
        else:
            _ov_txt = f"<b>{_ov_name}</b> ranks highest overall across the properties in this comparison."

        col_widths = [1] + [3] * filled

        # ── PDF export (landscape A4) ─────────────────────────────────────────
        from datetime import datetime as _dt
        _today = _dt.now().strftime("%d %B %Y")

        _prop_ths = "".join(
            "<th style='background:#1A9B8C;color:#fff;padding:10px 14px;"
            "text-align:center;border-right:1px solid #0d7a6e'>"
            + e['unit']['_building'].split(',')[0].strip()
            + "<br><span style='font-size:10px;font-weight:400;opacity:.85'>"
            + str(e['unit']['bedrooms']) + " BR &middot; "
            + "{:,}".format(int(e['unit']['sqft'])) + " sqft</span></th>"
            for e in enriched
        )
        _rows_spec = [
            ("Listed Price",       [("AED {:,}".format(int(e['unit']['listed_price']))) if e['unit'].get('listed_price') else "—" for e in enriched]),
            ("Price / Sqft",       [("AED {:,}".format(int(float(e['unit']['listed_price'])/float(e['unit']['sqft'])))) if e['unit'].get('sqft') and float(e['unit'].get('sqft',0))>0 else "—" for e in enriched]),
            ("Fair Value",         [("AED {:,}".format(int(e['unit']['fair_value_aed']))) if e['has_fv'] else "—" for e in enriched]),
            ("Premium/Discount",   [("{:.1f}%".format(float(e['unit']['premium_discount_pct']))) if e['has_fv'] and e['unit'].get('premium_discount_pct') is not None else "—" for e in enriched]),
            ("Listed Yield",       [("{:.1f}%".format(float(e['unit']['listed_yield_pct']))) if e['unit'].get('listed_yield_pct') is not None and str(e['unit'].get('listed_yield_pct','')) not in ('','nan') else "—" for e in enriched]),
            ("Est. Annual Rent",   [("AED {:,}".format(int(float(e['unit']['estimated_annual_rent'])))) if e['unit'].get('estimated_annual_rent') is not None and str(e['unit'].get('estimated_annual_rent','')) not in ('','nan') else "—" for e in enriched]),
            ("Fair Value Yield",   [("{:.1f}%".format(float(e['unit']['fair_value_yield_pct']))) if e['unit'].get('fair_value_yield_pct') is not None and str(e['unit'].get('fair_value_yield_pct','')) not in ('','nan') else "—" for e in enriched]),
            ("Confidence",         [str(e['unit'].get('confidence_score','—') or '—') for e in enriched]),
            ("Liquidity Grade",    [e['grade'] for e in enriched]),
            ("Demand Score",       [f"{float(e['unit']['demand_score']):.1f} · {e['unit'].get('demand_label','—')}" if e['unit'].get('demand_score') is not None and str(e['unit'].get('demand_score','')) not in ('','nan') else "—" for e in enriched]),
            ("Recommendation",     ["{} {}".format(*get_recommendation(e['unit'].get('listed_price'), e['unit'].get('fair_value_aed'), e['unit'].get('listed_yield_pct'), e['unit'].get('demand_score'), e['grade'])[:2]) for e in enriched]),
        ]
        _tbody = ""
        for _i, (_lbl, _vals) in enumerate(_rows_spec):
            _bg = "#f7f9fb" if _i % 2 == 0 else "#ffffff"
            _cells = "".join(
                "<td style='padding:9px 14px;text-align:center;"
                "border-right:1px solid #e8eaed;font-size:11px'>" + _v + "</td>"
                for _v in _vals
            )
            _tbody += (
                "<tr style='background:" + _bg + "'>"
                "<td style='padding:9px 14px;font-weight:600;color:#333;"
                "border-right:2px solid #1A9B8C;font-size:11px'>" + _lbl + "</td>"
                + _cells + "</tr>"
            )
        # PDF is built after the demand table below (where _pdf_demand_section is available)

        with hdr_pdf:
            st.markdown(
                "<style>div[data-testid='stDownloadButton']>button{"
                "background:#C0392B!important;color:#fff!important;"
                "border:none!important;border-radius:6px!important;"
                "font-size:13px!important;font-weight:700!important;"
                "padding:8px 18px!important;margin-top:10px!important;width:100%!important;}"
                "div[data-testid='stDownloadButton']>button:hover{"
                "background:#A93226!important;}</style>",
                unsafe_allow_html=True
            )
            st.empty()  # placeholder — final PDF download button is rendered after demand table

        # ── PropVestIQ Summary (shown ABOVE the table) ───────────────────────
        st.markdown("<div style='height:24px'></div>", unsafe_allow_html=True)
        st.markdown("""
        <div style='text-align:center;margin-bottom:20px'>
            <span style='font-family:Tenor Sans,serif;font-size:28px;font-weight:700;
            color:#EAE6DF;letter-spacing:-.2px'>PropVestIQ Summary</span>
        </div>
        """, unsafe_allow_html=True)

        def _summary_card(icon, heading, color, body_html):
            return f"""
            <div style='background:#112030;border:1px solid rgba(255,255,255,0.07);
            border-left:3px solid {color};border-radius:10px;padding:20px 24px;margin-bottom:14px'>
                <div style='font-family:Courier Prime,monospace;font-size:14px;font-weight:700;
                letter-spacing:1.2px;text-transform:uppercase;color:{color};margin-bottom:10px'>{icon} {heading}</div>
                <div style='font-family:Nunito Sans,sans-serif;font-size:16px;color:#C8D0CE;
                line-height:1.7'>{body_html}</div>
            </div>"""

        _sum_cols = st.columns([1, 6, 1])
        with _sum_cols[1]:
            st.markdown(
                _summary_card("💰", "Best Price Entry",   "#2EB87A", _bv_txt) +
                _summary_card("📈", "Best Yield",          "#4A9FD4", _by_txt) +
                _summary_card("📍", "Demand Score",        "#A98FD0", _bd_txt) +
                _summary_card("🏆", "Overall Pick",        "#E8C84A", _ov_txt),
                unsafe_allow_html=True
            )

        st.markdown("<div style='height:32px'></div>", unsafe_allow_html=True)

        # ── Property header row ───────────────────────────────────────────────
        hdr_cols = st.columns(col_widths)
        with hdr_cols[0]:
            st.markdown("<div style='height:72px'></div>", unsafe_allow_html=True)
        for ci, e in enumerate(enriched):
            uparts = e['unit']['_building'].split(',')
            uname = uparts[0].strip()
            uarea = uparts[1].strip() if len(uparts) > 1 else ''
            u = e['unit']
            reg = u.get('reg_type_en','')
            type_col = "#E8985A" if reg == 'Off-Plan Properties' else "#2EB87A" if reg == 'Existing Properties' else "#5C7070"
            type_lbl = "OFF-PLAN" if reg == 'Off-Plan Properties' else "READY" if reg == 'Existing Properties' else "—"
            with hdr_cols[ci + 1]:
                st.markdown(f"""
                <div style='background:#0D1520;border:1px solid rgba(26,155,140,0.2);
                border-top:3px solid #22C4B0;border-radius:10px 10px 0 0;
                padding:18px 16px;text-align:center'>
                    <div style='font-family:Tenor Sans,serif;font-size:18px;font-weight:700;
                    color:#EAE6DF;margin-bottom:4px'>{uname}</div>
                    <div style='font-size:13px;color:#5C7070;margin-bottom:6px'>{uarea}</div>
                    <div style='font-family:Courier Prime,monospace;font-size:12px;color:#9BA8A6'>
                        {u['bedrooms']} BR · {int(u['sqft']):,} sqft
                        <span style='color:{type_col};margin-left:8px'>{type_lbl}</span>
                    </div>
                </div>
                """, unsafe_allow_html=True)

        st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

        def cmp_val(label, values):
            row_cols = st.columns(col_widths)
            with row_cols[0]:
                st.markdown(f"""
                <div style='font-family:Courier Prime,monospace;font-size:13px;letter-spacing:.5px;
                text-transform:uppercase;color:#9BA8A6;font-weight:600;padding:14px 8px 14px 12px;
                border-bottom:1px solid rgba(255,255,255,0.07);
                background:rgba(13,21,32,0.6)'>{label}</div>
                """, unsafe_allow_html=True)
            for ci, val_html in enumerate(values):
                with row_cols[ci + 1]:
                    st.markdown(f"""
                    <div style='padding:12px 16px;border-bottom:1px solid rgba(255,255,255,0.07);
                    text-align:center;background:#112030'>{val_html}</div>
                    """, unsafe_allow_html=True)

        def cpill(text, color, bg="transparent", border="rgba(255,255,255,0.1)"):
            return f"<span style='background:{bg};color:{color};border:1px solid {border};padding:5px 14px;border-radius:50px;font-size:15px;font-weight:700;font-family:Courier Prime,monospace;white-space:nowrap'>{text}</span>"

        cmp_val("Listed Price", [
            cpill(f"AED {int(e['unit']['listed_price']):,}", "#22C4B0", "rgba(26,155,140,0.1)", "rgba(26,155,140,0.3)")
            for e in enriched
        ])

        cmp_val("Price / Sqft", [
            cpill(f"AED {int(e['unit']['listed_price'] / e['unit']['sqft']):,}", "#A98FD0", "rgba(169,143,208,0.1)", "rgba(169,143,208,0.3)")
            if e['unit'].get('sqft') and e['unit']['sqft'] > 0 else cpill("—", "#5C7070")
            for e in enriched
        ])

        cmp_val("Fair Value", [
            cpill(f"AED {int(e['unit']['fair_value_aed']):,}", "#22C4B0", "rgba(26,155,140,0.1)", "rgba(26,155,140,0.3)")
            if e['has_fv'] else cpill("—", "#5C7070")
            for e in enriched
        ])

        cmp_val("FV / Sqft", [
            cpill(f"AED {int(e['unit']['median_ppsf']):,}", "#22C4B0", "rgba(26,155,140,0.1)", "rgba(26,155,140,0.3)")
            if e['has_fv'] and e['unit'].get('median_ppsf') else cpill("—", "#5C7070")
            for e in enriched
        ])

        cmp_val("Listed Yield", [
            cpill(f"{float(e['unit']['listed_yield_pct']):.1f}%", "#2EB87A", "rgba(46,184,122,0.1)", "rgba(46,184,122,0.3)")
            if e['unit'].get('listed_yield_pct') is not None and str(e['unit'].get('listed_yield_pct','')) not in ('','nan')
            else cpill("—", "#5C7070")
            for e in enriched
        ])

        cmp_val("Est. Annual Rent", [
            cpill(f"AED {int(float(e['unit']['estimated_annual_rent'])):,}", "#22C4B0", "rgba(26,155,140,0.1)", "rgba(26,155,140,0.3)")
            if e['unit'].get('estimated_annual_rent') is not None and str(e['unit'].get('estimated_annual_rent','')) not in ('','nan')
            else cpill("—", "#5C7070")
            for e in enriched
        ])

        cmp_val("Fair Value Yield", [
            cpill(f"{float(e['unit']['fair_value_yield_pct']):.1f}%", "#4A9FD4", "rgba(74,159,212,0.1)", "rgba(74,159,212,0.3)")
            if e['unit'].get('fair_value_yield_pct') is not None and str(e['unit'].get('fair_value_yield_pct','')) not in ('','nan')
            else cpill("—", "#5C7070")
            for e in enriched
        ])

        pct_vals = []
        for e in enriched:
            if e['has_fv']:
                pv = float(e['unit']['premium_discount_pct'])
                pl = f"+{pv:.1f}%" if pv > 0 else f"{pv:.1f}%"
                pc = "#E05C5C" if pv > 0 else "#2EB87A"
                pb = "rgba(224,92,92,0.12)" if pv > 0 else "rgba(46,184,122,0.12)"
                pd_ = "rgba(224,92,92,0.3)" if pv > 0 else "rgba(46,184,122,0.3)"
                pct_vals.append(cpill(pl, pc, pb, pd_))
            else:
                pct_vals.append(cpill("—", "#5C7070"))
        cmp_val("Premium / Discount", pct_vals)

        conf_vals = []
        for e in enriched:
            if e['has_fv']:
                cv = str(e['unit'].get('confidence_score',''))
                cc_v, cbg_v, cbd_v = CONF_COLORS.get(cv, CONF_COLORS['No Result'])
                conf_vals.append(cpill(cv, cc_v, cbg_v, cbd_v))
            else:
                conf_vals.append(cpill("No Result", "#5C7070"))
        cmp_val("Confidence", conf_vals)

        liq_vals = []
        for e in enriched:
            lc_v, lbg_v, lbd_v = LIQ_COLORS.get(e['grade'], LIQ_COLORS['N/A'])
            liq_vals.append(cpill(e['grade'], lc_v, lbg_v, lbd_v))
        cmp_val("Liquidity Grade", liq_vals)

        cmp_val("Demand Score", [
            cpill(
                f"{float(e['unit']['demand_score']):.1f} · {e['unit'].get('demand_label','—')}",
                demand_colour(e['unit'].get('demand_label'), e['unit'].get('demand_score'))[0]
            ) if e['unit'].get('demand_score') is not None and str(e['unit'].get('demand_score','')) not in ('','nan')
            else cpill("—", "#5C7070")
            for e in enriched
        ])

        _rec_vals = []
        for e in enriched:
            _rl, _re, _rc, _rbg, _rbd = get_recommendation(
                e['unit'].get('listed_price'), e['unit'].get('fair_value_aed'),
                e['unit'].get('listed_yield_pct'), e['unit'].get('demand_score'), e['grade']
            )
            _rec_vals.append(cpill(f"{_re} {_rl}", _rc, _rbg, _rbd))
        cmp_val("Recommendation", _rec_vals)

        # ── Demand Score Breakdown (table format) ─────────────────────────────
        st.markdown("<div style='height:48px'></div>", unsafe_allow_html=True)
        st.markdown("""
        <div style='text-align:center;margin-bottom:24px'>
            <span style='font-family:Tenor Sans,serif;font-size:28px;font-weight:700;
            color:#EAE6DF;letter-spacing:-.2px'>Demand Score Breakdown</span>
        </div>
        """, unsafe_allow_html=True)

        def _mins_c(v):
            try: return f"{float(v):.0f} min"
            except: return "—"
        def _km_c(v):
            try: return f"{float(v):.1f} km"
            except: return "—"

        # Fetch demand inputs for each enriched property
        _cmp_inputs = {}
        _cmp_scores = {}
        for e in enriched:
            _pid = e['unit'].get('property_id','')
            if _pid:
                _di = run_query("SELECT * FROM demand_score_inputs WHERE property_id = :pid", params={"pid": _pid})
                _dv = run_query("""SELECT gym_score, grocery_score, medical_score, pharmacy_score,
                    workhub_score, school_score, mall_score, airport_score,
                    waterfront_score, stock_score, supply_score, popgrowth_score, road_score
                    FROM demand_score_view WHERE property_id = :pid""", params={"pid": _pid})
                if len(_di) > 0 and len(_dv) > 0:
                    _cmp_inputs[_pid] = _di.iloc[0]
                    _cmp_scores[_pid] = _dv.iloc[0]

        _breakdown_rows = [
            ("🏋️", "Gym",           lambda di,dv: (di.get('nearest_gym','—') or '—',     float(dv['gym_score']),       f"{_km_c(di.get('gym_distance_km'))} · {_mins_c(di.get('gym_duration_min'))}")),
            ("🛒", "Grocery",        lambda di,dv: (di.get('nearest_grocery','—') or '—',  float(dv['grocery_score']),   f"{_km_c(di.get('grocery_distance_km'))} · {_mins_c(di.get('grocery_duration_min'))}")),
            ("🏥", "Medical",        lambda di,dv: (di.get('nearest_medical','—') or '—',  float(dv['medical_score']),   f"{_km_c(di.get('medical_distance_km'))} · {_mins_c(di.get('medical_duration_min'))}")),
            ("💊", "Pharmacy",       lambda di,dv: (di.get('nearest_pharmacy','—') or '—', float(dv['pharmacy_score']),  f"{_km_c(di.get('pharmacy_distance_km'))} · {_mins_c(di.get('pharmacy_duration_min'))}")),
            ("🏙️", "Work Hubs",      lambda di,dv: (f"{sum(1 for c in ['businessbay_min','difc_min','downtowndubai_min','dubaiinternetcity_min','dubaimediacity_min'] if di.get(c) is not None and float(di.get(c,999))<=20)}/5 within 20 min", float(dv['workhub_score']), f"Score {float(dv['workhub_score']):.1f}/5")),
            ("🎓", "Schools",        lambda di,dv: (f"{sum(1 for c in ['gems_min','jess_min','jumeirah_college_min','kingsdubai_min','nordanglia_min'] if di.get(c) is not None and float(di.get(c,999))<=20)}/5 within 20 min", float(dv['school_score']), f"Score {float(dv['school_score']):.1f}/5")),
            ("🏬", "Dubai Mall",     lambda di,dv: (f"{_km_c(di.get('dubaimall_km'))} away", float(dv['mall_score']),     f"{_km_c(di.get('dubaimall_km'))} · {_mins_c(di.get('dubaimall_min'))}")),
            ("✈️", "DXB Airport",    lambda di,dv: (f"{_km_c(di.get('dxb_km'))} away",       float(dv['airport_score']),  f"{_km_c(di.get('dxb_km'))} · {_mins_c(di.get('dxb_min'))}")),
            ("🌊", "Waterfront",     lambda di,dv: (di.get('waterfront_label','—') or '—',    float(dv['waterfront_score']), f"Score {di.get('waterfront_score','—')}/5")),
            ("🏗️", "Supply Risk",    lambda di,dv: ("Area-level assessment",                  float(dv['supply_score']),   f"Score {di.get('future_supply_risk','—')}/5")),
            ("📈", "Pop. Growth",    lambda di,dv: ("Area-level assessment",                  float(dv['popgrowth_score']),f"Score {di.get('population_growth_area_level','—')}/5")),
            ("🏢", "Existing Stock", lambda di,dv: ("Competing inventory",                    float(dv['stock_score']),    f"Score {di.get('existing_competing_stock','—')}/5")),
            ("🚇", "Road & Transit", lambda di,dv: ("Connectivity",                           float(dv['road_score']),     f"Score {di.get('road_transit_connectivity','—')}/5")),
        ]

        # Build table headers
        _prop_names_list = [_prop_name(e) for e in enriched]
        _pids_list       = [e['unit'].get('property_id','') for e in enriched]

        TH_D = "padding:9px 12px;font-family:Courier Prime,monospace;font-size:11px;font-weight:700;color:#fff;background:#0D1520;border-bottom:1px solid rgba(255,255,255,0.12);text-align:center;white-space:nowrap;letter-spacing:.8px;text-transform:uppercase;"
        TD_D = "padding:9px 12px;border-bottom:1px solid rgba(255,255,255,0.05);font-size:13px;vertical-align:middle;text-align:center;"
        TD_LABEL = "padding:9px 12px;border-bottom:1px solid rgba(255,255,255,0.05);font-family:Courier Prime,monospace;font-size:12px;color:#9BA8A6;font-weight:600;white-space:nowrap;text-transform:uppercase;letter-spacing:.5px;"

        _th_props = "".join(f"<th style='{TH_D}'>{n}</th>" for n in _prop_names_list)
        _demand_table_rows = ""

        # Also build PDF demand rows at same time
        _pdf_demand_rows = "".join(f"<th style='background:#1A9B8C;color:#fff;padding:8px 10px;text-align:center;font-size:11px'>{n}</th>" for n in _prop_names_list)
        _pdf_demand_body = ""

        for icon, title, getter in _breakdown_rows:
            _cells = ""
            _pdf_cells = ""
            for _pid in _pids_list:
                if _pid in _cmp_inputs:
                    _place, _score, _detail = getter(_cmp_inputs[_pid], _cmp_scores[_pid])
                    _pct = min(int(_score / 5 * 100), 100)
                    _bar_col = "#2EB87A" if _score >= 4 else "#E8C84A" if _score >= 2.5 else "#E05C5C"
                    _cells += f"""<td style='{TD_D}'>
                        <div style='font-size:12px;color:#EAE6DF;margin-bottom:4px'>{_place}</div>
                        <div style='height:4px;background:rgba(255,255,255,0.06);border-radius:2px;overflow:hidden;margin-bottom:3px'>
                            <div style='height:100%;width:{_pct}%;background:{_bar_col};border-radius:2px'></div>
                        </div>
                        <div style='font-size:10px;color:#5C7070'>{_detail}</div>
                    </td>"""
                    _pdf_cells += f"<td style='padding:7px 10px;border-bottom:1px solid #eee;font-size:10px;text-align:center'>{_place}<br><span style='color:#888'>{_detail}</span></td>"
                else:
                    _cells += f"<td style='{TD_D}'><span style='color:#5C7070'>—</span></td>"
                    _pdf_cells += "<td style='padding:7px 10px;border-bottom:1px solid #eee;font-size:10px;text-align:center'>—</td>"

            _demand_table_rows += f"<tr><td style='{TD_LABEL}'>{icon} {title}</td>{_cells}</tr>"
            _pdf_demand_body   += f"<tr><td style='padding:7px 10px;border-bottom:1px solid #eee;font-weight:600;font-size:10px'>{icon} {title}</td>{_pdf_cells}</tr>"

        _demand_table_html = f"""
        <div style='overflow-x:auto;margin:0 auto;max-width:100%'>
        <table style='border-collapse:collapse;width:100%'>
            <thead><tr>
                <th style='{TH_D}text-align:left;min-width:120px'>Factor</th>
                {_th_props}
            </tr></thead>
            <tbody style='background:#112030'>{_demand_table_rows}</tbody>
        </table></div>"""

        st.markdown(_demand_table_html, unsafe_allow_html=True)

        # Store PDF demand table for use in PDF export (reassemble _pdf below)
        _pdf_demand_section = (
            "<div class='section-title'>Demand Score Breakdown</div>"
            "<table><thead><tr>"
            "<th style='background:#0d7a6e;color:#fff;padding:8px 10px;text-align:left;font-size:11px'>Factor</th>"
            + _pdf_demand_rows +
            "</tr></thead><tbody>" + _pdf_demand_body + "</tbody></table>"
        )

        # Rebuild PDF with demand breakdown included
        _pdf = (
            "<!DOCTYPE html><html><head><meta charset='UTF-8'><style>"
            "@page{size:A4 landscape;margin:18mm 14mm;}"
            "@media print{body{-webkit-print-color-adjust:exact;print-color-adjust:exact;}}"
            "body{font-family:Arial,sans-serif;color:#111;font-size:12px;}"
            ".hdr{display:flex;align-items:flex-end;justify-content:space-between;"
            "margin-bottom:18px;border-bottom:2px solid #1A9B8C;padding-bottom:10px;}"
            ".logo{font-size:22px;font-weight:700;color:#1A9B8C;}"
            ".sub{font-size:11px;color:#555;margin-top:3px;letter-spacing:1px;text-transform:uppercase;}"
            ".date{font-size:11px;color:#888;}"
            "table{border-collapse:collapse;width:100%;margin-top:8px;}"
            ".section-title{font-size:14px;font-weight:700;color:#1A9B8C;margin:24px 0 10px;border-bottom:1px solid #ddd;padding-bottom:5px;}"
            ".summary-card{border-left:4px solid #1A9B8C;padding:9px 13px;margin-bottom:9px;background:#f7f9fb;}"
            ".summary-card-heading{font-size:11px;font-weight:700;letter-spacing:1px;text-transform:uppercase;margin-bottom:4px;}"
            ".summary-card-body{font-size:11px;color:#333;line-height:1.5;}"
            ".disc{margin-top:20px;font-size:9px;color:#999;border-top:1px solid #ddd;padding-top:8px;}"
            "</style></head><body>"
            "<div class='hdr'>"
            "<div><div class='logo'>PropVestIQ.</div>"
            "<div class='sub'>Dubai Property Intelligence &mdash; Property Comparison Report</div></div>"
            "<div class='date'>Generated: " + _today + "</div>"
            "</div>"
            "<div class='section-title'>PropVestIQ Summary</div>"
            "<div class='summary-card'><div class='summary-card-heading' style='color:#1a7a4a'>&#x1F4B0; Best Price Entry</div><div class='summary-card-body'>" + _bv_txt.replace("<b>","<strong>").replace("</b>","</strong>") + "</div></div>"
            "<div class='summary-card'><div class='summary-card-heading' style='color:#1a5fa0'>&#x1F4C8; Best Yield</div><div class='summary-card-body'>" + _by_txt.replace("<b>","<strong>").replace("</b>","</strong>") + "</div></div>"
            "<div class='summary-card'><div class='summary-card-heading' style='color:#6a3a9a'>&#x1F4CD; Demand Score</div><div class='summary-card-body'>" + _bd_txt.replace("<b>","<strong>").replace("</b>","</strong>") + "</div></div>"
            "<div class='summary-card'><div class='summary-card-heading' style='color:#9a7a00'>&#x1F3C6; Overall Pick</div><div class='summary-card-body'>" + _ov_txt.replace("<b>","<strong>").replace("</b>","</strong>") + "</div></div>"
            "<div class='section-title'>Comparison Table</div>"
            "<table><thead><tr>"
            "<th style='background:#0d7a6e;color:#fff;padding:10px 14px;text-align:left;min-width:130px'>Metric</th>"
            + _prop_ths +
            "</tr></thead><tbody>" + _tbody + "</tbody></table>"
            + _pdf_demand_section +
            "</body></html>"
        )

        # Update the download button with the new PDF
        with hdr_pdf:
            st.markdown(
                "<style>div[data-testid='stDownloadButton']>button{"
                "background:#C0392B!important;color:#fff!important;"
                "border:none!important;border-radius:6px!important;"
                "font-size:13px!important;font-weight:700!important;"
                "padding:8px 18px!important;margin-top:10px!important;width:100%!important;}"
                "div[data-testid='stDownloadButton']>button:hover{"
                "background:#A93226!important;}</style>",
                unsafe_allow_html=True
            )
            st.download_button(
                "⬇ Export PDF",
                data=_pdf.encode(),
                file_name="PropVestIQ_Comparison.html",
                mime="text/html",
                key="cmp_pdf_dl2"
            )

        # ── 5-Year Price Charts ───────────────────────────────────────────────
        st.markdown("<div style='height:40px'></div>", unsafe_allow_html=True)
        st.markdown("""
        <div style='text-align:center;margin-bottom:24px'>
            <span style='font-family:Tenor Sans,serif;font-size:28px;font-weight:700;
            color:#EAE6DF;letter-spacing:-.2px'>
            Historical Price History — Median Building Price Per SQFT</span>
        </div>
        """, unsafe_allow_html=True)

        # Charts use same col layout as the comparison table above
        chart_hdr_cols = st.columns(col_widths)
        with chart_hdr_cols[0]:
            st.markdown("<div></div>", unsafe_allow_html=True)
        chart_data_cols = [chart_hdr_cols[i+1] for i in range(filled)]
        for ci, e in enumerate(enriched):
            with chart_data_cols[ci]:
                uparts = e['unit']['_building'].split(',')
                uname_c = uparts[0].strip()
                cdata = get_chart_data(e['unit']['_building'])
                if len(cdata) > 0:
                    cdata['quarter'] = pd.to_datetime(cdata['quarter'], utc=True).dt.tz_convert(None)
                    all_beds_c = sorted(cdata['bedrooms'].unique())

                    # ── Bedroom filter pills (compact, single row) ────────
                    _blabels = {"Studio": "S"}
                    _opts_c = ["All"] + [_blabels.get(b, str(b)) for b in all_beds_c]
                    _cur_c = st.session_state.cmp_chart_beds.get(ci, None)
                    _disp_c = ["All"] if _cur_c is None else [_blabels.get(b, str(b)) for b in _cur_c]
                    _new_c = st.pills("Beds", _opts_c, selection_mode="multi",
                                      default=_disp_c, key=f"cmp_bed_pills_{ci}",
                                      label_visibility="collapsed")
                    if _new_c is None or "All" in _new_c or len(_new_c) == 0:
                        st.session_state.cmp_chart_beds[ci] = None
                    else:
                        _rev_c = {_blabels.get(b, str(b)): b for b in all_beds_c}
                        st.session_state.cmp_chart_beds[ci] = {_rev_c[x] for x in _new_c if x in _rev_c}
                    _cur_c = st.session_state.cmp_chart_beds.get(ci, None)

                    # ── Build chart ───────────────────────────────────────
                    active_c = all_beds_c if _cur_c is None else [b for b in all_beds_c if b in _cur_c]
                    fig_c = go.Figure()
                    for cidx, bed in enumerate(all_beds_c):
                        if bed not in active_c:
                            continue
                        bd = cdata[cdata['bedrooms'] == bed]
                        is_sel = (bed == e['unit']['bedrooms'])
                        fig_c.add_trace(go.Scatter(
                            x=bd['quarter'], y=bd['median_ppsf'],
                            name=f"{bed} BR", mode='lines+markers',
                            line=dict(color=bed_color(bed), width=3),
                            marker=dict(size=4 if is_sel else 3),
                            opacity=1.0,
                        ))
                    fig_c.update_layout(
                        title=dict(text=uname_c, font=dict(color='#EAE6DF', size=11), x=0),
                        height=280, plot_bgcolor='rgba(17,32,48,1)', paper_bgcolor='rgba(17,32,48,1)',
                        font=dict(family='Nunito Sans', color='#9BA8A6', size=10),
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
                                    font=dict(color='#9BA8A6', size=9)),
                        margin=dict(l=0, r=0, t=36, b=0), hovermode="x unified",
                        xaxis=dict(gridcolor='rgba(255,255,255,0.04)', showline=False),
                        yaxis=dict(gridcolor='rgba(255,255,255,0.04)', tickfont=dict(color='#5C7070')),
                    )
                    st.plotly_chart(fig_c, width='stretch', key=f"cmp_chart_{ci}")
                else:
                    st.markdown(f"""
                    <div style='background:#112030;border:1px solid rgba(255,255,255,0.07);
                    border-radius:10px;padding:20px;height:280px;display:flex;
                    align-items:center;justify-content:center;flex-direction:column'>
                        <div style='font-family:Courier Prime,monospace;font-size:10px;
                        letter-spacing:1px;text-transform:uppercase;color:#5C7070;
                        margin-bottom:6px'>{uname_c}</div>
                        <div style='color:#5C7070;font-size:12px'>No chart data</div>
                    </div>""", unsafe_allow_html=True)

        # ── Add historical charts to PDF ──────────────────────────────────────
        _pdf_chart_colors = ['#1A9B8C','#E8985A','#2EB87A','#E05C5C','#A98FD0','#C87941']
        _pdf_charts_html  = "<div class='section-title'>Historical Price Per Sqft</div>"
        _pdf_charts_html += "<script src='https://cdn.jsdelivr.net/npm/chart.js'></script>"
        _prop_grid_style  = "display:grid;grid-template-columns:" + " ".join(["1fr"] * min(filled, 3)) + ";gap:20px;margin-bottom:20px;"
        _pdf_charts_html += f"<div style='{_prop_grid_style}'>"
        for ci, e in enumerate(enriched):
            _cn = _prop_name(e)
            _cdata = get_chart_data(e['unit']['_building'])
            if len(_cdata) > 0:
                try:
                    _cdata['quarter'] = pd.to_datetime(_cdata['quarter'], utc=True).dt.tz_convert(None)
                    _all_b = sorted(_cdata['bedrooms'].unique())
                    _lbs   = [str(q)[:7] for q in _cdata[_cdata['bedrooms']==_all_b[0]].sort_values('quarter')['quarter']]
                    _ds    = []
                    for _bi, _b in enumerate(_all_b):
                        _bd2  = _cdata[_cdata['bedrooms']==_b].sort_values('quarter')
                        _vals = [int(v) if not pd.isna(v) else None for v in _bd2['median_ppsf']]
                        _c2   = _pdf_chart_colors[_bi % len(_pdf_chart_colors)]
                        _ds.append(f"{{'label':'{_b} BR','data':{_vals},'borderColor':'{_c2}','backgroundColor':'transparent','tension':0.3,'pointRadius':2,'borderWidth':2}}")
                    _ds_json   = "[" + ",".join(_ds) + "]"
                    _lbs_json  = str(_lbs)
                    _chart_opts = "responsive:true,plugins:{legend:{position:'top',labels:{font:{size:9}}}},scales:{y:{title:{display:true,text:'AED/sqft',font:{size:9}},ticks:{font:{size:9}}},x:{ticks:{font:{size:8},maxRotation:45}}}"
                    _pdf_charts_html += (
                        f"<div><div style='font-size:11px;font-weight:700;color:#1A9B8C;margin-bottom:6px'>{_cn.split('(')[0].strip()}</div>"
                        f"<canvas id='chart_{ci}' height='180'></canvas>"
                        f"<script>new Chart(document.getElementById('chart_{ci}'),"
                        + "{"
                        + f"type:'line',data:{{labels:{_lbs_json},datasets:{_ds_json}}},"
                        + f"options:{{{_chart_opts}}}"
                        + "}"
                        + ");</script></div>"
                    )
                except: pass
        _pdf_charts_html += "</div>"
        _pdf = _pdf.replace("</body></html>", _pdf_charts_html + "<div class='disc'>This report is generated by PropVestIQ for informational purposes only and does not constitute financial or investment advice. Fair values are model-derived estimates based on historical transaction data. Always conduct independent due diligence.</div></body></html>")

    st.markdown("</div>", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 6 — TOKENISATION STUDIO
# ═══════════════════════════════════════════════════════════════════════════════
elif st.session_state.page == 'tokenise':

    MIN_INVESTMENT = 4000  # AED

    nav_bar()
    st.markdown("<div style='padding:24px 48px 80px'>", unsafe_allow_html=True)

    token_units = st.session_state.token_units
    n_props = len(token_units)

    # ── Page header ───────────────────────────────────────────────────────────
    st.markdown("""
    <div style='text-align:center;padding:28px 0 36px'>
        <div style='font-family:Tenor Sans,serif;font-size:44px;font-weight:700;
        color:#EAE6DF;letter-spacing:-.5px;margin-bottom:10px;line-height:1.15'>
            Tokenisation <em style='font-style:italic;color:#22C4B0'>Studio</em>
        </div>
        <div style='color:#9BA8A6;font-size:16px;font-weight:300;letter-spacing:.3px'>
            Fractional property investing · see what your capital earns across a portfolio
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Split layout ──────────────────────────────────────────────────────────
    _, left_col, right_col, _ = st.columns([0.5, 3, 4, 1.5], gap="large")

    with left_col:

        # Search
        section_label("Add a Property")
        tok_search = st.text_input(
            "tok_search",
            placeholder="🔍  Building name...",
            label_visibility="collapsed",
            key="tok_search_input"
        )

        if tok_search and len(tok_search) >= 2:
            tok_buildings = run_query("""
                SELECT DISTINCT building_name FROM dashboard_listings
                WHERE building_name ILIKE :search
                ORDER BY building_name LIMIT 12
            """, params={"search": f"%{tok_search}%"})

            if len(tok_buildings) == 0:
                st.markdown("<p style='color:#5C7070;font-size:13px'>No buildings found.</p>", unsafe_allow_html=True)
            else:
                for _, brow in tok_buildings.iterrows():
                    bparts = brow['building_name'].split(',')
                    bn = bparts[0].strip()
                    ba = bparts[1].strip() if len(bparts) > 1 else ''
                    bl, br = st.columns([5, 2])
                    with bl:
                        st.markdown(f"""
                        <div style='padding:9px 2px 3px'>
                            <div style='font-family:Nunito Sans,sans-serif;font-size:13px;
                            font-weight:600;color:#EAE6DF'>{bn}</div>
                            <div style='font-size:11px;color:#5C7070'>{ba}</div>
                        </div>
                        """, unsafe_allow_html=True)
                    with br:
                        if st.button("Units", key=f"tokb_{brow['building_name']}"):
                            st.session_state.token_building_selected = brow['building_name']
                            st.rerun()

        # Portfolio list
        if n_props > 0:
            st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)
            section_label(f"Portfolio — {n_props} {'property' if n_props == 1 else 'properties'}")
            for i, u in enumerate(token_units):
                uparts = u['_building'].split(',')
                uname = uparts[0].strip()
                c1, c2 = st.columns([5, 1])
                with c1:
                    st.markdown(f"""
                    <div style='background:#112030;border:1px solid rgba(26,155,140,0.2);
                    border-left:3px solid #E8985A;border-radius:8px;
                    padding:10px 14px;margin-bottom:6px'>
                        <div style='font-family:Nunito Sans,sans-serif;font-size:13px;
                        font-weight:700;color:#EAE6DF;margin-bottom:3px;
                        white-space:nowrap;overflow:hidden;text-overflow:ellipsis'>{uname}</div>
                        <div style='font-family:Courier Prime,monospace;font-size:10px;color:#5C7070'>
                            {u["bedrooms"]} BR · {int(u["sqft"]):,} sqft
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                with c2:
                    if st.button("✕", key=f"tok_rm_{i}"):
                        st.session_state.token_units.pop(i)
                        # Clean up manual pct for removed property
                        pkey = f"{u['_building']}_{i}"
                        st.session_state.token_manual_pct.pop(pkey, None)
                        st.rerun()

            if st.button("Clear portfolio", key="tok_clear"):
                st.session_state.token_units = []
                st.session_state.token_manual_pct = {}
                st.session_state.token_building_selected = None
                st.rerun()

    # ── Right column: unit picker + capital inputs ────────────────────────────
    with right_col:

        # Unit picker
        if st.session_state.token_building_selected:
            sel_building = st.session_state.token_building_selected
            sel_parts = sel_building.split(',')
            sel_bname = sel_parts[0].strip()
            sel_barea = sel_parts[1].strip() if len(sel_parts) > 1 else ''

            tok_listings = run_query("""
                SELECT number_of_bedrooms as bedrooms, sqft, listed_price,
                       fair_value_aed, premium_discount_pct, confidence_score,
                       median_ppsf, match_tier, reg_type_en, url
                FROM dashboard_listings
                WHERE building_name = :building
                ORDER BY
                    CASE number_of_bedrooms
                        WHEN 'Studio' THEN 0 WHEN '1' THEN 1 WHEN '2' THEN 2
                        WHEN '3' THEN 3 WHEN '4' THEN 4 WHEN '5+' THEN 5 ELSE 6
                    END, sqft
            """, params={"building": sel_building})

            section_label(sel_bname)
            st.markdown(f"<div style='font-size:11px;color:#5C7070;margin-bottom:12px;font-family:Courier Prime,monospace'>📍 {sel_barea} · {len(tok_listings)} units</div>", unsafe_allow_html=True)

            TH_T = "padding:8px 12px;border-bottom:1px solid rgba(255,255,255,0.1);text-align:center;font-family:Courier Prime,monospace;font-size:10px;font-weight:700;color:#9BA8A6;letter-spacing:1px;text-transform:uppercase;white-space:nowrap;background:#0D1520;"
            TD_T = "padding:9px 12px;border-bottom:1px solid rgba(255,255,255,0.04);vertical-align:middle;text-align:center;white-space:nowrap;"

            tok_rows_html = ""
            tok_add_btns = []

            for pi, prow in tok_listings.iterrows():
                already = any(
                    u['_building'] == sel_building and
                    int(u['sqft']) == int(prow['sqft']) and
                    u['bedrooms'] == prow['bedrooms'] and
                    int(u['listed_price']) == int(prow['listed_price'])
                    for u in token_units
                )
                beds_p  = pill(str(prow['bedrooms']), "#7EB8D4", "rgba(126,184,212,0.12)", "rgba(126,184,212,0.3)")
                sqft_p  = pill(f"{int(prow['sqft']):,} sqft", "#A98FD0", "rgba(169,143,208,0.12)", "rgba(169,143,208,0.3)")
                price_p = pill(f"AED {int(prow['listed_price']):,}", "#22C4B0", "rgba(26,155,140,0.12)", "rgba(26,155,140,0.3)")

                if already:
                    action = "<span style='color:#3a5a4a;font-family:Courier Prime,monospace;font-size:10px;letter-spacing:1px'>✓ ADDED</span>"
                else:
                    action = f"<span class='tok-add-btn' data-idx='{pi}' style='background:rgba(232,152,90,0.12);color:#E8985A;border:1px solid rgba(232,152,90,0.3);padding:4px 14px;border-radius:50px;font-size:11px;font-weight:600;font-family:Courier Prime,monospace;cursor:pointer;letter-spacing:.5px'>+ Add</span>"

                tok_rows_html += f"""<tr>
                    <td style='{TD_T}'>{beds_p}</td>
                    <td style='{TD_T}'>{sqft_p}</td>
                    <td style='{TD_T}'>{price_p}</td>
                    <td style='{TD_T}'>{action}</td>
                </tr>"""
                tok_add_btns.append((pi, prow))

            st.markdown(f"""
            <div style='border:1px solid rgba(232,152,90,0.15);border-radius:10px;overflow:hidden;margin-bottom:16px'>
            <table style='border-collapse:collapse;width:100%'>
                <thead><tr>
                    <th style='{TH_T}'>Beds</th>
                    <th style='{TH_T}'>Sqft</th>
                    <th style='{TH_T}'>Listed Price</th>
                    <th style='{TH_T}'></th>
                </tr></thead>
                <tbody style='background:#112030'>{tok_rows_html}</tbody>
            </table></div>
            """, unsafe_allow_html=True)

            for pi, prow in tok_add_btns:
                if st.button(f"__tokadd_{pi}", key=f"tokaddunit_{pi}"):
                    unit_dict = prow.to_dict()
                    unit_dict['_building'] = sel_building
                    st.session_state.token_units.append(unit_dict)
                    st.session_state.token_building_selected = None
                    st.rerun()

            components.html("""
            <script>
            (function() {
                function wire() {
                    var doc = window.parent.document;
                    doc.querySelectorAll('button').forEach(function(b) {
                        if (b.innerText.trim().startsWith('__tokadd_')) {
                            b.parentElement.style.cssText = 'position:fixed;top:-9999px;left:-9999px;';
                        }
                    });
                    doc.querySelectorAll('.tok-add-btn').forEach(function(span) {
                        if (span._wired) return;
                        span._wired = true;
                        span.addEventListener('click', function() {
                            var idx = this.getAttribute('data-idx');
                            doc.querySelectorAll('button').forEach(function(b) {
                                if (b.innerText.trim() === '__tokadd_' + idx) b.click();
                            });
                        });
                    });
                }
                wire(); setTimeout(wire, 400); setTimeout(wire, 1000);
            })();
            </script>
            """, height=0)

            col_back, _ = st.columns([2, 5])
            with col_back:
                if st.button("← Back to search", key="tok_back_search"):
                    st.session_state.token_building_selected = None
                    st.rerun()

    # ═══════════════════════════════════════════════════════════════════════
    # ═══════════════════════════════════════════════════════════════════════
    # SUBMIT — shown once at least 1 property added
    # ═══════════════════════════════════════════════════════════════════════
    if n_props > 0:
        st.markdown("<div style='height:32px'></div>", unsafe_allow_html=True)
        st.markdown("<hr style='border:none;border-top:1px solid rgba(232,152,90,0.15);margin-bottom:24px'>", unsafe_allow_html=True)

        section_label("Investment Settings")

        cap_col, mode_col, submit_col = st.columns([2, 2, 2])
        with cap_col:
            st.markdown("<div style='font-family:Courier Prime,monospace;font-size:10px;letter-spacing:1.5px;text-transform:uppercase;color:#5C7070;margin-bottom:6px'>Total Capital (AED)</div>", unsafe_allow_html=True)
            capital = st.number_input(
                "capital",
                min_value=4000,
                max_value=500_000_000,
                value=st.session_state.token_capital,
                step=10000,
                label_visibility="collapsed",
                key="tok_capital_input"
            )
            st.session_state.token_capital = capital

        with mode_col:
            st.markdown("<div style='font-family:Courier Prime,monospace;font-size:10px;letter-spacing:1.5px;text-transform:uppercase;color:#5C7070;margin-bottom:6px'>Allocation Mode</div>", unsafe_allow_html=True)
            mode = st.radio(
                "mode",
                options=["Equal split", "Manual split"],
                index=0 if st.session_state.token_alloc_mode == 'equal' else 1,
                horizontal=True,
                label_visibility="collapsed",
                key="tok_mode_radio"
            )
            st.session_state.token_alloc_mode = 'equal' if mode == "Equal split" else 'manual'

        with submit_col:
            st.markdown("<div style='height:24px'></div>", unsafe_allow_html=True)
            st.markdown("""
            <style>
            div[data-testid="stButton"] button[kind="primary"] {
                background: linear-gradient(135deg, #C87941, #E8985A) !important;
                color: #080E14 !important; border: none !important;
                font-weight: 700 !important; font-size: 13px !important;
                letter-spacing: .8px !important; border-radius: 8px !important;
                padding: 10px 20px !important;
            }
            </style>
            """, unsafe_allow_html=True)
            if st.button(f"View Portfolio Results ({n_props}) →", key="tok_submit", type="primary"):
                st.session_state.page = 'tokenise_results'
                st.rerun()

    else:
        st.markdown("""
        <div style='text-align:center;padding:60px 0;color:#2a3a3a'>
            <div style='font-size:40px;margin-bottom:16px;opacity:.4'>◈</div>
            <div style='font-family:Courier Prime,monospace;font-size:12px;letter-spacing:2px;
            text-transform:uppercase;color:#2a3a3a'>
                Search a building on the left and add properties to get started
            </div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("</div>", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 7 — TOKENISATION RESULTS
# ═══════════════════════════════════════════════════════════════════════════════
elif st.session_state.page == 'tokenise_results':

    MIN_INVESTMENT = 4000

    nav_bar()
    st.markdown("<div style='padding:24px 48px 80px'>", unsafe_allow_html=True)

    if st.button("← Edit Portfolio", key="tok_back_edit"):
        st.session_state.page = 'tokenise'
        st.rerun()

    token_units = st.session_state.token_units
    n_props = len(token_units)
    capital = st.session_state.token_capital
    alloc_mode = st.session_state.token_alloc_mode

    st.markdown(f"""
    <div style='padding:16px 0 28px'>
        <div style='font-family:Tenor Sans,serif;font-size:36px;font-weight:700;
        color:#EAE6DF;letter-spacing:-.3px;margin-bottom:8px'>
            Portfolio <em style='font-style:italic;color:#E8985A'>Results</em>
        </div>
        <div style='font-family:Courier Prime,monospace;font-size:11px;color:#5C7070;
        letter-spacing:1.5px;text-transform:uppercase'>
            {n_props} {'property' if n_props==1 else 'properties'} · AED {int(capital):,} capital · {'Equal split' if alloc_mode=='equal' else 'Manual split'}
        </div>
    </div>
    """, unsafe_allow_html=True)

    if n_props == 0:
        st.markdown("<p style='color:#5C7070'>No properties in portfolio. Go back and add some.</p>", unsafe_allow_html=True)
    else:
        # ── Compute allocations ───────────────────────────────────────────
        if alloc_mode == 'equal':
            per_prop = capital / n_props
            alloc_amts = [per_prop] * n_props
        else:
            # Use equal as fallback if manual not configured
            per_prop = capital / n_props
            alloc_amts = [per_prop] * n_props

        alloc_pcts = [(alloc_amts[i] / u['listed_price']) * 100 for i, u in enumerate(token_units)]

        # ── Validation warning ────────────────────────────────────────────
        below_min = [i for i, a in enumerate(alloc_amts) if a < MIN_INVESTMENT]
        if below_min:
            names = ', '.join(token_units[i]['_building'].split(',')[0].strip() for i in below_min)
            st.markdown(f"<div style='background:rgba(224,92,92,0.1);border:1px solid rgba(224,92,92,0.3);border-radius:8px;padding:10px 16px;color:#E05C5C;font-family:Courier Prime,monospace;font-size:12px;margin-bottom:16px'>⚠ Minimum investment is AED {MIN_INVESTMENT:,}. Affected: {names}</div>", unsafe_allow_html=True)

        # ── Summary cards ─────────────────────────────────────────────────
        section_label("Portfolio Summary")

        total_invested = sum(alloc_amts)
        wtd_own = sum(alloc_pcts[i] * (alloc_amts[i] / total_invested) for i in range(n_props)) if total_invested > 0 else 0

        def scard(label, value, color="#EAE6DF", accent=False):
            top = "background:linear-gradient(90deg,#E8985A,rgba(232,152,90,0.3),transparent)" if accent else "background:transparent"
            bg  = "linear-gradient(135deg,rgba(232,152,90,0.07),#162840)" if accent else "#112030"
            bdr = "rgba(232,152,90,0.25)" if accent else "rgba(255,255,255,0.07)"
            return f"""<div style='background:{bg};border:1px solid {bdr};border-radius:10px;
            padding:18px 20px;position:relative;overflow:hidden;text-align:center'>
                <div style='position:absolute;top:0;left:0;right:0;height:2px;{top}'></div>
                <div style='font-family:Courier Prime,monospace;font-size:9px;letter-spacing:1.5px;
                text-transform:uppercase;color:#5C7070;margin-bottom:8px'>{label}</div>
                <div style='font-family:Tenor Sans,serif;font-size:22px;font-weight:700;
                color:{color};line-height:1.2'>{value}</div>
            </div>"""

        s1, s2, s3, s4, s5 = st.columns(5)
        with s1:
            st.markdown(scard("Properties", str(n_props)), unsafe_allow_html=True)
        with s2:
            st.markdown(scard("Total Invested", f"AED {int(total_invested):,}", "#E8985A", accent=True), unsafe_allow_html=True)
        with s3:
            st.markdown(scard("Avg Ownership", f"{wtd_own:.2f}%", "#22C4B0"), unsafe_allow_html=True)
        with s4:
            st.markdown(scard("Total Annual Rent", "— pending", "#5C7070"), unsafe_allow_html=True)
        with s5:
            st.markdown(scard("Portfolio Yield", "— pending", "#5C7070"), unsafe_allow_html=True)

        # ── Per-property table ────────────────────────────────────────────
        st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
        section_label("Per-Property Breakdown")

        TH_R = "padding:10px 14px;border-bottom:1px solid rgba(255,255,255,0.1);text-align:center;font-family:Courier Prime,monospace;font-size:10px;font-weight:700;color:#9BA8A6;letter-spacing:1px;text-transform:uppercase;white-space:nowrap;background:#0D1520;"
        TD_R = "padding:10px 14px;border-bottom:1px solid rgba(255,255,255,0.05);vertical-align:middle;text-align:center;white-space:nowrap;"
        TD_L = "padding:10px 14px;border-bottom:1px solid rgba(255,255,255,0.05);vertical-align:middle;text-align:left;"

        result_rows = ""
        pdf_result_rows = ""

        for i, u in enumerate(token_units):
            uparts = u['_building'].split(',')
            uname = uparts[0].strip()
            uarea = uparts[1].strip() if len(uparts) > 1 else ''
            invest_amt = alloc_amts[i]
            own_pct = alloc_pcts[i]
            listed = int(u['listed_price'])

            name_cell = f"""<td style='{TD_L}'>
                <div style='font-family:Nunito Sans,sans-serif;font-size:13px;font-weight:700;color:#EAE6DF'>{uname}</div>
                <div style='font-size:11px;color:#5C7070;font-family:Courier Prime,monospace'>{uarea} · {u['bedrooms']} BR · {int(u['sqft']):,} sqft</div>
            </td>"""
            price_cell = f"<td style='{TD_R}'>{pill(f'AED {listed:,}', '#22C4B0', 'rgba(26,155,140,0.1)', 'rgba(26,155,140,0.3)')}</td>"
            invest_cell = f"<td style='{TD_R}'>{pill(f'AED {int(invest_amt):,}', '#E8985A', 'rgba(232,152,90,0.12)', 'rgba(232,152,90,0.3)')}</td>"

            own_color = "#2EB87A" if own_pct >= 5 else "#C87941" if own_pct >= 1 else "#E05C5C"
            own_bg    = "rgba(46,184,122,0.1)" if own_pct >= 5 else "rgba(200,121,65,0.1)" if own_pct >= 1 else "rgba(224,92,92,0.1)"
            own_bd    = "rgba(46,184,122,0.3)" if own_pct >= 5 else "rgba(200,121,65,0.3)" if own_pct >= 1 else "rgba(224,92,92,0.3)"
            own_cell  = f"<td style='{TD_R}'>{pill(f'{own_pct:.2f}%', own_color, own_bg, own_bd)}</td>"

            pending = "<span style='color:#3a5a4a;font-family:Courier Prime,monospace;font-size:11px'>— pending</span>"
            result_rows += f"<tr>{name_cell}{price_cell}{invest_cell}{own_cell}<td style='{TD_R}'>{pending}</td><td style='{TD_R}'>{pending}</td><td style='{TD_R}'>{pending}</td></tr>"
            pdf_result_rows += f"<tr><td>{uname}</td><td>{u['bedrooms']} BR / {int(u['sqft']):,} sqft</td><td>AED {listed:,}</td><td>AED {int(invest_amt):,}</td><td>{own_pct:.2f}%</td><td>—</td><td>—</td><td>—</td></tr>"

        st.markdown(f"""
        <div style='overflow-x:auto;border:1px solid rgba(232,152,90,0.15);border-radius:12px;overflow:hidden'>
        <table style='border-collapse:collapse;width:100%'>
            <thead><tr style='background:#0D1520'>
                <th style='{TH_R};text-align:left'>Property</th>
                <th style='{TH_R}'>Listed Price</th>
                <th style='{TH_R}'>Your Investment</th>
                <th style='{TH_R}'>Ownership %</th>
                <th style='{TH_R}'>Est. Annual Rent</th>
                <th style='{TH_R}'>Your Rental Share</th>
                <th style='{TH_R}'>Gross Yield</th>
            </tr></thead>
            <tbody style='background:#112030'>{result_rows}</tbody>
        </table></div>
        """, unsafe_allow_html=True)

        # ── Capital allocation bar chart ──────────────────────────────────
        st.markdown("<div style='height:32px'></div>", unsafe_allow_html=True)
        section_label("Capital Allocation")

        bar_labels = [u['_building'].split(',')[0].strip()[:22] for u in token_units]
        bar_values = [int(a) for a in alloc_amts]
        bar_pcts   = alloc_pcts

        fig_bar = go.Figure()
        for i in range(n_props):
            fig_bar.add_trace(go.Bar(
                x=[bar_labels[i]],
                y=[bar_values[i]],
                name=bar_labels[i],
                marker_color=CHART_COLORS[i % len(CHART_COLORS)],
                text=[f"AED {bar_values[i]:,}<br>{bar_pcts[i]:.2f}% ownership"],
                textposition='outside',
                textfont=dict(color='#9BA8A6', size=10, family='Courier Prime'),
            ))
        fig_bar.update_layout(
            height=340,
            plot_bgcolor='rgba(17,32,48,1)',
            paper_bgcolor='rgba(17,32,48,1)',
            font=dict(family='Nunito Sans', color='#9BA8A6', size=11),
            showlegend=False,
            margin=dict(l=0, r=0, t=40, b=0),
            xaxis=dict(gridcolor='rgba(255,255,255,0.04)', tickfont=dict(color='#9BA8A6', size=10)),
            yaxis=dict(gridcolor='rgba(255,255,255,0.04)', tickfont=dict(color='#5C7070'),
                       tickprefix='AED ', tickformat=',.0f'),
            bargap=0.3,
        )
        st.plotly_chart(fig_bar, width='stretch', key="tok_results_bar")

        # ── PDF Export ────────────────────────────────────────────────────
        st.markdown("<div style='height:24px'></div>", unsafe_allow_html=True)
        import datetime
        today = datetime.date.today().strftime("%d %B %Y")

        pdf_html = f"""<html><head><style>
        body{{font-family:Arial,sans-serif;padding:32px;color:#111;}}
        h1{{font-size:22px;margin-bottom:4px;color:#1A9B8C;}}
        .meta{{color:#555;font-size:12px;margin-bottom:24px;}}
        h2{{font-size:14px;margin:24px 0 8px;color:#1A9B8C;border-bottom:1px solid #eee;padding-bottom:4px;}}
        table{{border-collapse:collapse;width:100%;font-size:11px;margin-bottom:16px;}}
        th{{background:#1A9B8C;color:#fff;padding:8px 10px;text-align:left;}}
        td{{padding:6px 10px;border-bottom:1px solid #eee;}}
        .summary-grid{{display:flex;gap:16px;flex-wrap:wrap;margin-bottom:24px;}}
        .scard{{background:#f5f5f5;border-radius:8px;padding:12px 16px;min-width:140px;}}
        .scard .lbl{{font-size:10px;color:#888;text-transform:uppercase;letter-spacing:1px;margin-bottom:4px;}}
        .scard .val{{font-size:18px;font-weight:700;color:#C87941;}}
        </style></head><body>
        <h1>Tokenisation Studio — Portfolio Report</h1>
        <div class="meta">Generated {today} · PropVestIQ Dubai Property Intelligence</div>
        <h2>Portfolio Summary</h2>
        <div class="summary-grid">
            <div class="scard"><div class="lbl">Properties</div><div class="val">{n_props}</div></div>
            <div class="scard"><div class="lbl">Total Capital</div><div class="val">AED {int(total_invested):,}</div></div>
            <div class="scard"><div class="lbl">Avg Ownership</div><div class="val">{wtd_own:.2f}%</div></div>
            <div class="scard"><div class="lbl">Allocation</div><div class="val">{'Equal' if alloc_mode=='equal' else 'Manual'}</div></div>
        </div>
        <h2>Per-Property Breakdown</h2>
        <table><thead><tr>
            <th>Property</th><th>Beds / Sqft</th><th>Listed Price</th>
            <th>Investment</th><th>Ownership %</th>
            <th>Est. Annual Rent</th><th>Rental Share</th><th>Gross Yield</th>
        </tr></thead><tbody>{pdf_result_rows}</tbody></table>
        <div class="meta" style="margin-top:32px;font-size:10px;color:#aaa">
        Rental income and yield figures are pending rental data integration.
        Ownership % = Your Investment ÷ Listed Price. Min investment: AED {MIN_INVESTMENT:,}.
        For informational purposes only.
        </div></body></html>"""

        dl_col, _ = st.columns([2, 5])
        with dl_col:
            st.markdown("""
            <style>
            div[data-testid="stDownloadButton"] > button {
                background: #C0392B !important; color: #ffffff !important;
                border: none !important; border-radius: 6px !important;
                font-size: 12px !important; font-weight: 700 !important;
                padding: 8px 16px !important;
            }
            </style>""", unsafe_allow_html=True)
            st.download_button(
                "⬇ Download Portfolio PDF",
                data=pdf_html.encode(),
                file_name=f"PropVestIQ_Portfolio_{today.replace(' ','_')}.html",
                mime="text/html",
                key="tok_results_pdf"
            )

    st.markdown("</div>", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE — MY PORTFOLIO
# ═══════════════════════════════════════════════════════════════════════════════
elif st.session_state.page == 'my_portfolio':
    nav_bar()
    st.markdown("<div style='padding:24px 48px 80px'>", unsafe_allow_html=True)

    st.markdown("""
    <div style='padding:10px 0 20px'>
        <div style='font-family:Tenor Sans,serif;font-size:32px;font-weight:700;
        color:#EAE6DF;letter-spacing:-.3px'>My Portfolio</div>
        <div style='font-family:Courier Prime,monospace;font-size:12px;color:#5C7070;
        margin-top:4px;letter-spacing:.5px'>Add your properties and see how your actual rent compares to market estimates</div>
    </div>
    """, unsafe_allow_html=True)

    # ── Name input ───────────────────────────────────────────────────────────
    name_col, _ = st.columns([3, 5])
    with name_col:
        portfolio_name = st.text_input("Your name (for the PDF report)", placeholder="e.g. Amir Khan",
                                        key="portfolio_name_input")

    # ── Add Property Form ────────────────────────────────────────────────────
    section_label("Add a Property")

    all_buildings = run_query("SELECT DISTINCT building_name FROM dashboard_listings ORDER BY building_name")
    building_options = all_buildings['building_name'].tolist() if len(all_buildings) > 0 else []

    with st.form("add_property_form", clear_on_submit=True):
        form_cols = st.columns([4, 1, 1, 2, 1])
        with form_cols[0]:
            selected_building = st.selectbox("Property", options=[""] + building_options,
                format_func=lambda x: x.split(',')[0].strip() if x else "— Select a property —")
        with form_cols[1]:
            bed_options = ["Studio", "1", "2", "3", "4", "5+"]
            selected_beds = st.selectbox("Bedrooms", options=bed_options)
        with form_cols[2]:
            input_sqft = st.number_input("Sqft", min_value=100, max_value=50000, value=800, step=10)
        with form_cols[3]:
            input_rent = st.number_input("Your Annual Rent (AED)", min_value=0, max_value=10000000,
                value=50000, step=1000)
        with form_cols[4]:
            st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
            add_clicked = st.form_submit_button("+ Add", use_container_width=True)

        if add_clicked and selected_building:
            fv_data = run_query("""
                SELECT listed_price, fair_value_aed, premium_discount_pct,
                       confidence_score, demand_score, demand_label, median_ppsf
                FROM dashboard_listings
                WHERE building_name = :building AND number_of_bedrooms = :beds
                ORDER BY ABS(sqft - :sqft) ASC
                LIMIT 1
            """, params={"building": selected_building, "beds": selected_beds, "sqft": input_sqft})

            yr_data = run_query("""
                SELECT estimated_annual_rent, listed_yield_pct, yield_confidence
                FROM yield_results
                WHERE property_id IN (
                    SELECT property_id FROM bayut_matched_clean WHERE building_name = :building LIMIT 1
                ) AND number_of_bedrooms = :beds
                LIMIT 1
            """, params={"building": selected_building, "beds": selected_beds})

            prop = {
                "building": selected_building,
                "bname": selected_building.split(',')[0].strip(),
                "barea": ', '.join(p.strip() for p in selected_building.split(',')[1:]),
                "bedrooms": selected_beds,
                "sqft": input_sqft,
                "actual_rent": input_rent,
            }
            if len(fv_data) > 0:
                r = fv_data.iloc[0]
                prop["fair_value_aed"]      = r.get("fair_value_aed")
                prop["listed_price"]        = r.get("listed_price")
                prop["premium_discount_pct"]= r.get("premium_discount_pct")
                prop["confidence_score"]    = r.get("confidence_score")
                prop["demand_score"]        = r.get("demand_score")
                prop["demand_label"]        = r.get("demand_label")
                prop["median_ppsf"]         = r.get("median_ppsf")
            if len(yr_data) > 0:
                y = yr_data.iloc[0]
                prop["estimated_rent"]   = y.get("estimated_annual_rent")
                prop["yield_confidence"] = y.get("yield_confidence")
            else:
                prop["estimated_rent"]   = None
                prop["yield_confidence"] = None

            st.session_state.portfolio_properties.append(prop)
            st.session_state.portfolio_submitted = False
            st.rerun()

    # ── Property List ────────────────────────────────────────────────────────
    props = st.session_state.portfolio_properties

    if props:
        section_label("Your Properties")

        TH_P = "padding:9px 12px;font-family:Courier Prime,monospace;font-size:11px;font-weight:700;color:#fff;background:#0D1520;border-bottom:1px solid rgba(255,255,255,0.12);text-align:center;letter-spacing:.8px;text-transform:uppercase;white-space:nowrap;"
        TD_P = "padding:9px 12px;border-bottom:1px solid rgba(255,255,255,0.05);font-size:13px;vertical-align:middle;text-align:center;"

        rows_html = ""
        total_actual = sum(p["actual_rent"] for p in props)

        for i, p in enumerate(props):
            rows_html += f"""<tr>
                <td style='{TD_P}text-align:left'><span style='font-family:Nunito Sans,sans-serif;font-weight:600;color:#EAE6DF'>{p['bname']}</span><br>
                <span style='font-size:11px;color:#5C7070'>{p['barea']}</span></td>
                <td style='{TD_P}'>{p['bedrooms']}</td>
                <td style='{TD_P}'>{int(p['sqft']):,}</td>
                <td style='{TD_P}'><span style='color:#22C4B0;font-weight:700'>AED {int(p['actual_rent']):,}</span></td>
            </tr>"""

        st.markdown(f"""
        <div style='overflow-x:auto'>
        <table style='border-collapse:collapse;width:100%'>
            <thead><tr>
                <th style='{TH_P}text-align:left'>Property</th>
                <th style='{TH_P}'>Beds</th>
                <th style='{TH_P}'>Sqft</th>
                <th style='{TH_P}'>Your Annual Rent</th>
            </tr></thead>
            <tbody style='background:#112030'>{rows_html}</tbody>
            <tfoot>
                <tr style='background:#0D1520'>
                    <td colspan='3' style='padding:10px 12px;font-family:Courier Prime,monospace;font-size:12px;font-weight:700;color:#9BA8A6;text-align:right;letter-spacing:1px;text-transform:uppercase'>Total Annual Rent</td>
                    <td style='padding:10px 12px;text-align:center;font-family:Tenor Sans,serif;font-size:18px;font-weight:700;color:#22C4B0'>AED {int(total_actual):,}</td>
                </tr>
            </tfoot>
        </table></div>
        """, unsafe_allow_html=True)

        # Remove buttons
        rm_cols = st.columns(len(props) + 1)
        for i in range(len(props)):
            if st.button(f"✕ Remove {props[i]['bname'][:20]}", key=f"rm_port_{i}"):
                st.session_state.portfolio_properties.pop(i)
                st.session_state.portfolio_submitted = False
                st.rerun()

        st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

        sub_col1, sub_col2, sub_col3 = st.columns([3, 2, 3])
        with sub_col2:
            if st.button("📊  Analyse Portfolio", use_container_width=True):
                st.session_state.portfolio_submitted = True
                st.rerun()

    # ── Analysis Results ─────────────────────────────────────────────────────
    if st.session_state.portfolio_submitted and props:
        st.markdown("<div style='height:32px'></div>", unsafe_allow_html=True)
        section_label("Portfolio Analysis")

        TH_A = "padding:9px 12px;font-family:Courier Prime,monospace;font-size:11px;font-weight:700;color:#fff;background:#0D1520;border-bottom:1px solid rgba(255,255,255,0.12);text-align:center;letter-spacing:.8px;text-transform:uppercase;white-space:nowrap;"
        TD_A = "padding:9px 12px;border-bottom:1px solid rgba(255,255,255,0.05);font-size:13px;vertical-align:middle;text-align:center;"

        total_actual    = sum(p["actual_rent"] for p in props)
        total_estimated = sum(float(p["estimated_rent"]) for p in props if p.get("estimated_rent") is not None)

        def _market_verdict(actual_rent, estimated_rent):
            """Returns (label, colour) for market rate comparison. ±10% = On Par."""
            try:
                if not estimated_rent or float(estimated_rent) == 0:
                    return "—", "#5C7070"
                var = (float(actual_rent) - float(estimated_rent)) / float(estimated_rent) * 100
                if var > 10:
                    return "Above Market", "#2EB87A"
                elif var < -10:
                    return "Below Market", "#E05C5C"
                else:
                    return "On Par", "#E8C84A"
            except:
                return "—", "#5C7070"

        rows_a = ""
        pdf_rows = ""
        for p in props:
            _fv = p.get("fair_value_aed")
            try:
                _actual_yield = float(p["actual_rent"]) / float(_fv) * 100 if _fv and float(_fv) > 0 else None
            except: _actual_yield = None

            _est = p.get("estimated_rent")
            try:
                _est_yield = float(_est) / float(_fv) * 100 if _est and _fv and float(_fv) > 0 else None
            except: _est_yield = None

            try:
                if _est and float(_est) > 0:
                    _var_pct = (float(p["actual_rent"]) - float(_est)) / float(_est) * 100
                    _var_col = "#2EB87A" if _var_pct >= 0 else "#E05C5C"
                    _var_str = f"<span style='color:{_var_col};font-weight:700'>{'+' if _var_pct >= 0 else ''}{_var_pct:.1f}%</span>"
                    _var_plain = f"{'+' if _var_pct >= 0 else ''}{_var_pct:.1f}%"
                else:
                    _var_str = "<span style='color:#5C7070'>—</span>"
                    _var_plain = "—"
            except:
                _var_str = "<span style='color:#5C7070'>—</span>"
                _var_plain = "—"

            _verdict, _vc = _market_verdict(p["actual_rent"], _est)
            _verdict_pill = f"<span style='background:rgba(0,0,0,0.2);color:{_vc};border:1px solid {_vc};padding:3px 10px;border-radius:50px;font-size:12px;font-weight:700;font-family:Courier Prime,monospace;white-space:nowrap'>{_verdict}</span>"

            _fv_str   = f"AED {int(float(_fv)):,}" if _fv else "—"
            _fv_col   = "#22C4B0" if _fv else "#5C7070"
            _ds       = p.get("demand_score")
            _dl       = p.get("demand_label", "—") or "—"
            _ds_str   = f"{float(_ds):.1f} · {_dl}" if _ds is not None and str(_ds) not in ('','nan') else "—"
            try: _dc  = demand_colour(_dl, _ds)[0]
            except: _dc = "#5C7070"
            _liq_grade = 'N/A'
            _rl, _re, _rc, _rbg, _rbd = get_recommendation(
                p.get("listed_price"), _fv, p.get("actual_rent"), _ds, _liq_grade
            )
            _yconf    = p.get("yield_confidence","")
            _ydot     = "●" if _yconf == "High" else "◐" if _yconf == "Medium" else "○" if _yconf else ""
            _ydot_col = "#2EB87A" if _yconf == "High" else "#E8C84A" if _yconf == "Medium" else "#9BA8A6"
            _est_disp = f"AED {int(float(_est)):,}" if _est else "—"

            rows_a += f"""<tr>
                <td style='{TD_A}text-align:left'>
                    <span style='font-family:Nunito Sans,sans-serif;font-weight:600;color:#EAE6DF'>{p['bname']}</span><br>
                    <span style='font-size:11px;color:#5C7070'>{p['bedrooms']} BR · {int(p['sqft']):,} sqft</span>
                </td>
                <td style='{TD_A}'><span style='color:#22C4B0;font-weight:700'>AED {int(p['actual_rent']):,}</span></td>
                <td style='{TD_A}'><span style='color:#9BA8A6'>{_est_disp} <span style='color:{_ydot_col}'>{_ydot}</span></span></td>
                <td style='{TD_A}'>{_var_str}</td>
                <td style='{TD_A}'>{_verdict_pill}</td>
                <td style='{TD_A}'><span style='color:{"#2EB87A" if _actual_yield else "#5C7070"}'>{f"{_actual_yield:.1f}%" if _actual_yield else "—"}</span></td>
                <td style='{TD_A}'><span style='color:{"#4A9FD4" if _est_yield else "#5C7070"}'>{f"{_est_yield:.1f}%" if _est_yield else "—"}</span></td>
                <td style='{TD_A}'><span style='color:{_fv_col};font-weight:600'>{_fv_str}</span></td>
                <td style='{TD_A}'><span style='color:{_dc}'>{_ds_str}</span></td>
                <td style='{TD_A}'><span style='background:{_rbg};color:{_rc};border:1px solid {_rbd};padding:3px 10px;border-radius:50px;font-size:12px;font-weight:700;font-family:Courier Prime,monospace;white-space:nowrap'>{_re} {_rl}</span></td>
            </tr>"""

            # PDF row
            pdf_rows += (
                f"<tr><td>{p['bname']}<br><small>{p['bedrooms']} BR · {int(p['sqft']):,} sqft</small></td>"
                f"<td>AED {int(p['actual_rent']):,}</td>"
                f"<td>{_est_disp}</td>"
                f"<td>{_var_plain}</td>"
                f"<td style='color:{_vc};font-weight:700'>{_verdict}</td>"
                f"<td>{f'{_actual_yield:.1f}%' if _actual_yield else '—'}</td>"
                f"<td>{f'{_est_yield:.1f}%' if _est_yield else '—'}</td>"
                f"<td>{_fv_str}</td>"
                f"<td>{_ds_str}</td>"
                f"<td>{_re} {_rl}</td></tr>"
            )

        # Totals row
        _total_est_str = f"AED {int(total_estimated):,}" if total_estimated > 0 else "—"
        try:
            _total_var_pct = (total_actual - total_estimated) / total_estimated * 100 if total_estimated > 0 else None
            _total_var_str = f"{'+' if _total_var_pct >= 0 else ''}{_total_var_pct:.1f}%" if _total_var_pct is not None else "—"
            _total_var_col = "#2EB87A" if _total_var_pct and _total_var_pct >= 0 else "#E05C5C"
        except: _total_var_str, _total_var_col = "—", "#5C7070"

        _overall_verdict, _overall_vc = _market_verdict(total_actual, total_estimated if total_estimated > 0 else None)

        st.markdown(f"""
        <div style='overflow-x:auto'>
        <table style='border-collapse:collapse;width:100%'>
            <thead><tr>
                <th style='{TH_A}text-align:left'>Property</th>
                <th style='{TH_A}'>Your Rent</th>
                <th style='{TH_A}'>Est. Market Rent</th>
                <th style='{TH_A}'>Variance</th>
                <th style='{TH_A}'>Market Rate</th>
                <th style='{TH_A}'>Your Yield</th>
                <th style='{TH_A}'>Market Yield</th>
                <th style='{TH_A}'>Fair Value</th>
                <th style='{TH_A}'>Demand Score</th>
                <th style='{TH_A}'>Rating</th>
            </tr></thead>
            <tbody style='background:#112030'>{rows_a}</tbody>
            <tfoot style='background:#0D1520'>
                <tr>
                    <td style='padding:12px;font-family:Courier Prime,monospace;font-size:11px;font-weight:700;color:#9BA8A6;text-transform:uppercase;letter-spacing:1px'>Portfolio Total</td>
                    <td style='padding:12px;text-align:center;font-family:Tenor Sans,serif;font-size:16px;font-weight:700;color:#22C4B0'>AED {int(total_actual):,}</td>
                    <td style='padding:12px;text-align:center;font-family:Tenor Sans,serif;font-size:16px;font-weight:700;color:#9BA8A6'>{_total_est_str}</td>
                    <td style='padding:12px;text-align:center;font-family:Tenor Sans,serif;font-size:16px;font-weight:700;color:{_total_var_col}'>{_total_var_str}</td>
                    <td style='padding:12px;text-align:center;font-family:Tenor Sans,serif;font-size:14px;font-weight:700;color:{_overall_vc}'>{_overall_verdict}</td>
                    <td colspan='5'></td>
                </tr>
            </tfoot>
        </table></div>
        """, unsafe_allow_html=True)

        # ── Summary cards ────────────────────────────────────────────────────
        st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
        _n = len(props)
        _fv_total = sum(float(p.get("fair_value_aed") or 0) for p in props)
        _avg_actual_yield = (total_actual / _fv_total * 100) if _fv_total > 0 else None

        sc1, sc2, sc3, sc4, sc5 = st.columns(5)
        def _scard(col, label, value, color="#22C4B0"):
            with col:
                st.markdown(f"""
                <div style='background:#112030;border:1px solid rgba(255,255,255,0.07);
                border-radius:10px;padding:16px;text-align:center'>
                    <div style='font-family:Courier Prime,monospace;font-size:9px;font-weight:700;
                    letter-spacing:1.5px;text-transform:uppercase;color:#5C7070;margin-bottom:8px'>{label}</div>
                    <div style='font-family:Tenor Sans,serif;font-size:22px;font-weight:700;color:{color}'>{value}</div>
                </div>""", unsafe_allow_html=True)

        _scard(sc1, "Properties", str(_n))
        _scard(sc2, "Total Annual Rent", f"AED {int(total_actual):,}")
        _scard(sc3, "Est. Market Rent", f"AED {int(total_estimated):,}" if total_estimated > 0 else "—", "#9BA8A6")
        _scard(sc4, "Avg Portfolio Yield", f"{_avg_actual_yield:.1f}%" if _avg_actual_yield else "—", "#2EB87A" if _avg_actual_yield and _avg_actual_yield >= 6.5 else "#E8C84A")
        _scard(sc5, "Portfolio vs Market", _overall_verdict, _overall_vc)

        # ── PDF ───────────────────────────────────────────────────────────────
        st.markdown("<div style='height:32px'></div>", unsafe_allow_html=True)
        from datetime import datetime as _dt
        _report_date = _dt.now().strftime("%d %B %Y")
        _report_name = portfolio_name.strip() if portfolio_name and portfolio_name.strip() else "Portfolio Owner"

        _pdf_port = f"""<!DOCTYPE html><html><head><meta charset='UTF-8'><style>
        body{{font-family:Arial,sans-serif;padding:36px;color:#111;font-size:12px;}}
        h1{{font-size:22px;color:#1A9B8C;margin-bottom:2px;}}
        .meta{{font-size:11px;color:#555;margin-bottom:24px;}}
        h2{{font-size:14px;color:#1A9B8C;border-bottom:1px solid #ddd;padding-bottom:4px;margin:20px 0 10px;}}
        table{{border-collapse:collapse;width:100%;margin-bottom:20px;}}
        th{{background:#1A9B8C;color:#fff;padding:8px 10px;text-align:left;font-size:11px;}}
        td{{padding:7px 10px;border-bottom:1px solid #eee;vertical-align:top;}}
        .verdict-above{{color:#1a7a4a;font-weight:700;}} .verdict-below{{color:#c0392b;font-weight:700;}} .verdict-par{{color:#9a7a00;font-weight:700;}}
        .summary-grid{{display:grid;grid-template-columns:repeat(5,1fr);gap:12px;margin-bottom:24px;}}
        .summary-card{{background:#f7f9fb;border-left:3px solid #1A9B8C;padding:10px 12px;}}
        .summary-card-label{{font-size:9px;text-transform:uppercase;letter-spacing:1px;color:#888;margin-bottom:4px;}}
        .summary-card-value{{font-size:16px;font-weight:700;color:#1A9B8C;}}
        .disc{{margin-top:24px;font-size:9px;color:#999;border-top:1px solid #ddd;padding-top:8px;}}
        </style></head><body>
        <h1>PropVestIQ — Portfolio Report</h1>
        <div class='meta'>Prepared for: <strong>{_report_name}</strong> &nbsp;|&nbsp; Generated: {_report_date}</div>
        <h2>Portfolio Summary</h2>
        <div class='summary-grid'>
            <div class='summary-card'><div class='summary-card-label'>Properties</div><div class='summary-card-value'>{_n}</div></div>
            <div class='summary-card'><div class='summary-card-label'>Total Annual Rent</div><div class='summary-card-value'>AED {int(total_actual):,}</div></div>
            <div class='summary-card'><div class='summary-card-label'>Est. Market Rent</div><div class='summary-card-value'>{"AED " + f"{int(total_estimated):,}" if total_estimated > 0 else "—"}</div></div>
            <div class='summary-card'><div class='summary-card-label'>Avg Portfolio Yield</div><div class='summary-card-value'>{f"{_avg_actual_yield:.1f}%" if _avg_actual_yield else "—"}</div></div>
            <div class='summary-card'><div class='summary-card-label'>Portfolio vs Market</div><div class='summary-card-value' style='color:{"#1a7a4a" if _overall_verdict == "Above Market" else "#c0392b" if _overall_verdict == "Below Market" else "#9a7a00"}'>{_overall_verdict}</div></div>
        </div>
        <h2>Property Breakdown</h2>
        <table>
            <thead><tr>
                <th>Property</th><th>Your Rent</th><th>Est. Market</th>
                <th>Variance</th><th>Market Rate</th><th>Your Yield</th>
                <th>Market Yield</th><th>Fair Value</th><th>Demand</th><th>Rating</th>
            </tr></thead>
            <tbody>{pdf_rows}</tbody>
            <tfoot><tr style='background:#f0f4f4;font-weight:700'>
                <td>Portfolio Total</td>
                <td>AED {int(total_actual):,}</td>
                <td>{_total_est_str}</td>
                <td style='color:{"#1a7a4a" if _total_var_col=="#2EB87A" else "#c0392b"}'>{_total_var_str}</td>
                <td style='color:{"#1a7a4a" if _overall_verdict=="Above Market" else "#c0392b" if _overall_verdict=="Below Market" else "#9a7a00"};font-weight:700'>{_overall_verdict}</td>
                <td colspan='5'></td>
            </tr></tfoot>
        </table>
        <div class='disc'>This report is generated by PropVestIQ for informational purposes only and does not constitute financial advice. Market rent estimates are based on area-level rental transaction data. Always conduct independent due diligence. &nbsp;|&nbsp; &plusmn;10% = On Par with market rate.</div>
        </body></html>"""

        st.markdown("<div style='height:24px'></div>", unsafe_allow_html=True)
        _fname = f"PropVestIQ_Portfolio_{_report_name.replace(' ','_')}.html"
        st.download_button("⬇ Download Portfolio Report", data=_pdf_port.encode(),
                           file_name=_fname, mime="text/html", key="port_pdf_dl")

    elif not props:
        st.markdown("""
        <div style='background:#112030;border:1px solid rgba(255,255,255,0.07);border-radius:10px;
        padding:40px;text-align:center;margin-top:24px'>
            <div style='font-size:32px;margin-bottom:12px'>🏠</div>
            <div style='font-family:Tenor Sans,serif;font-size:20px;color:#EAE6DF;margin-bottom:8px'>No properties yet</div>
            <div style='font-family:Nunito Sans,sans-serif;font-size:14px;color:#5C7070'>Use the form above to add your properties</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("</div>", unsafe_allow_html=True)

