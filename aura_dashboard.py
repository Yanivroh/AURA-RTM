import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv
import streamlit as st
from datetime import datetime as dt
import plotly.graph_objects as go
import plotly.express as px
from io import BytesIO

# Load environment variables
load_dotenv()

# Set page configuration
st.set_page_config(
    page_title="Aura Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Configuration constants - Available Brands
BRANDS = [
    "ntt_docomo", "softbank", "htc", "motorola_eu", "samsung_om", "lenovo_sea", 
    "at-t", "kddisamsung", "com.aura.oobe.vodafone", "wiko", "newsroom", "asus", 
    "motorola_apac", "samsung_na", "samsung_sea", "t-mobile", "vodafone", "lenovo", 
    "samsung", "vodafoneSamsung", "lenovo_latam", "softbanksamsung", "sprint", "tinno", 
    "hutchison", "lenovo_na", "samsung_itd", "samsung_cis", "rakuten", "lenovo_eu", 
    "motorola", "sliide", "oppo_latam", "honor", "dish", "oppo_cis", "samsung_eu", 
    "Bouygues", "solutions", "lenovo_apac", "motorola_latam", "deutschetelekomsamsung", 
    "samsung_gl", "samsung_mea", "oppo_sea", "clearly_google_play", "dish-sdk", 
    "motorola_north_america", "sony", "cricket", "deutschetelekom", "oppo_eu", 
    "samsungsfr", "huawei", "ntt_docomo_samsung", "orange", "rakutensamsung", "kddi", 
    "lenovo_mea", "oppo", "lenovo_cis", "bouygues-primary", "oppo_mea"
]

FEATURES = ['oobe', 'silent', 'gotw', 'publisher promotion', 'reef', 'reengagement promotion', 'recurring OOBE']

# Custom CSS for dark theme with readable text
st.markdown("""
<style>
    /* Main app background with gradient */
    .stApp {
        background: linear-gradient(135deg, #0e1117 0%, #1a1d29 100%);
        color: #fafafa;
    }
    
    /* Main content area */
    .main .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
    
    /* Sidebar with modern gradient */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #1e2130 0%, #262730 100%);
        border-right: 1px solid #3d4050;
    }
    
    [data-testid="stSidebar"] * {
        color: #fafafa !important;
    }
    
    /* Headers - modern and bold */
    h1 {
        color: #fafafa !important;
        font-size: 2.5rem !important;
        font-weight: 800 !important;
        background: linear-gradient(90deg, #4CAF50 0%, #2196F3 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 1rem !important;
    }
    
    h2, h3 {
        color: #fafafa !important;
        font-weight: 700 !important;
    }
    
    h4, h5, h6 {
        color: #e0e0e0 !important;
    }
    
    /* Streamlit metric cards - enhanced */
    [data-testid="stMetricValue"] {
        font-size: 2rem !important;
        font-weight: 700 !important;
        color: #fafafa !important;
    }
    
    [data-testid="stMetricLabel"] {
        font-size: 1rem !important;
        color: #b0b0b0 !important;
        font-weight: 600 !important;
    }
    
    [data-testid="stMetricDelta"] {
        font-size: 1rem !important;
        font-weight: 600 !important;
    }
    
    /* Metric container with hover effect */
    div[data-testid="metric-container"] {
        background: linear-gradient(135deg, #1e2130 0%, #252836 100%);
        border-radius: 12px;
        padding: 1.2rem;
        border: 1px solid #3d4050;
        box-shadow: 0 4px 12px rgba(0,0,0,0.3);
        transition: all 0.3s ease;
    }
    
    div[data-testid="metric-container"]:hover {
        transform: translateY(-4px);
        box-shadow: 0 8px 24px rgba(76, 175, 80, 0.2);
        border-color: #4CAF50;
    }
    
    /* Insights box */
    .insight-box {
        background: linear-gradient(135deg, #2196F3 0%, #1976D2 100%);
        border-radius: 12px;
        padding: 1.5rem;
        margin-bottom: 1.5rem;
        box-shadow: 0 4px 12px rgba(33, 150, 243, 0.3);
        border: 1px solid #42A5F5;
    }
    
    .insight-box h3 {
        color: #fff !important;
        margin-bottom: 1rem;
    }
    
    .insight-item {
        background: rgba(255,255,255,0.1);
        border-radius: 8px;
        padding: 0.8rem;
        margin: 0.5rem 0;
        border-left: 3px solid #FFC107;
    }
    
    /* Make all text readable */
    p, span, div, label {
        color: #fafafa !important;
    }
    
    /* Streamlit widgets */
    .stSelectbox label, .stMultiSelect label, .stCheckbox label {
        color: #fafafa !important;
        font-weight: 600 !important;
    }
    
    /* Buttons - modern style */
    .stButton button {
        background: linear-gradient(135deg, #4CAF50 0%, #45a049 100%);
        color: white !important;
        border: none;
        border-radius: 8px;
        padding: 0.6rem 1.2rem;
        font-weight: 600;
        transition: all 0.3s ease;
        box-shadow: 0 2px 8px rgba(76, 175, 80, 0.3);
    }
    
    .stButton button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(76, 175, 80, 0.4);
    }
    
    /* Data tables - modern dark theme */
    .dataframe {
        color: #fafafa !important;
        background-color: #1e2130 !important;
        border-radius: 8px;
        overflow: hidden;
    }
    
    .dataframe th {
        background: linear-gradient(135deg, #2196F3 0%, #1976D2 100%) !important;
        color: #fff !important;
        font-weight: 600 !important;
        padding: 12px !important;
    }
    
    .dataframe td {
        color: #fafafa !important;
        padding: 10px !important;
        border-bottom: 1px solid #3d4050;
    }
    
    .dataframe tr:hover {
        background-color: #252836 !important;
    }
    
    /* Tabs - modern style */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background-color: #1e2130;
        border-radius: 8px;
        padding: 4px;
    }
    
    .stTabs [data-baseweb="tab"] {
        border-radius: 6px;
        color: #b0b0b0;
        font-weight: 600;
        padding: 8px 16px;
    }
    
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #4CAF50 0%, #45a049 100%);
        color: white !important;
    }
    
    /* Info/Warning/Success boxes */
    .stAlert {
        background-color: #1e2130 !important;
        color: #fafafa !important;
        border-radius: 8px !important;
        border-left: 4px solid #2196F3 !important;
    }
    
    /* Loading spinner */
    .stSpinner > div {
        border-top-color: #4CAF50 !important;
    }
    
    /* Plotly charts - dark theme */
    .js-plotly-plot {
        border-radius: 12px;
        overflow: hidden;
    }
</style>
""", unsafe_allow_html=True)

def get_connection():
    """Establish connection to Redshift database"""
    try:
        # Get credentials from environment variables
        conn_params = {
            'dbname': os.getenv('REDSHIFT_DB'),
            'user': os.getenv('REDSHIFT_USER'),
            'password': os.getenv('REDSHIFT_PASS'),
            'host': os.getenv('REDSHIFT_HOST'),
            'port': int(os.getenv('REDSHIFT_PORT', 5439))
        }
        
        # Attempt to establish connection
        conn = psycopg2.connect(**conn_params)
        conn.autocommit = True
        return conn
        
    except (psycopg2.OperationalError, Exception):
        return None

def build_sql_query(selected_source=None, selected_brands=None, selected_features=None):
    """Build SQL query dynamically with selected brands and features"""
    brands_to_use = selected_brands if selected_brands else BRANDS
    brands_str = "', '".join(brands_to_use)
    features_to_use = selected_features if selected_features else FEATURES
    features_str = "', '".join(features_to_use)
    
    # Add source filter if specified
    source_filter = f"AND source = '{selected_source}'" if selected_source else ""
    
    return f"""
WITH 
today_metrics AS (
    SELECT
        brand,
        feature,
        COALESCE(SUM(revenue), 0) AS revenue_today,
        COALESCE(SUM(notification_shown), 0) AS notif_today,
        COALESCE(SUM(experience_shown), 0) AS exp_today,
        COALESCE(SUM(install_success), 0) AS install_today
    FROM apps.supply_aura_rtm
    WHERE brand IN ('{brands_str}')
      AND feature IN ('{features_str}')
      {source_filter}
      AND date_hour >= TRUNC(GETDATE())
      AND date_hour <= GETDATE()
    GROUP BY brand, feature
),
last_week_metrics AS (
    SELECT
        brand,
        feature,
        COALESCE(SUM(revenue), 0) AS revenue_last_week,
        COALESCE(SUM(notification_shown), 0) AS notif_last_week,
        COALESCE(SUM(experience_shown), 0) AS exp_last_week,
        COALESCE(SUM(install_success), 0) AS install_last_week
    FROM apps.supply_aura_rtm
    WHERE brand IN ('{brands_str}')
      AND feature IN ('{features_str}')
      {source_filter}
      AND date_hour >= DATEADD(day, -7, TRUNC(GETDATE()))
      AND date_hour <= DATEADD(hour, -2, DATEADD(day, -7, GETDATE()))
    GROUP BY brand, feature
)
SELECT
    COALESCE(t.brand, l.brand) AS brand,
    COALESCE(t.feature, l.feature) AS feature,
    COALESCE(t.revenue_today, 0) AS revenue_today,
    COALESCE(l.revenue_last_week, 0) AS revenue_last_week,
    COALESCE(t.revenue_today, 0) - COALESCE(l.revenue_last_week, 0) AS revenue_diff,
    CASE 
        WHEN COALESCE(l.revenue_last_week, 0) > 0 
        THEN ROUND((COALESCE(t.revenue_today, 0) - COALESCE(l.revenue_last_week, 0)) / COALESCE(l.revenue_last_week, 1) * 100, 1)
        ELSE NULL 
    END AS revenue_pct_diff,
    COALESCE(t.notif_today, 0) AS notif_today,
    COALESCE(l.notif_last_week, 0) AS notif_last_week,
    COALESCE(t.notif_today, 0) - COALESCE(l.notif_last_week, 0) AS notif_diff,
    CASE 
        WHEN COALESCE(l.notif_last_week, 0) > 0 
        THEN ROUND((COALESCE(t.notif_today, 0) - COALESCE(l.notif_last_week, 0)) / COALESCE(l.notif_last_week, 1) * 100, 1)
        ELSE NULL 
    END AS notif_pct_diff,
    COALESCE(t.exp_today, 0) AS exp_today,
    COALESCE(l.exp_last_week, 0) AS exp_last_week,
    COALESCE(t.exp_today, 0) - COALESCE(l.exp_last_week, 0) AS exp_diff,
    CASE 
        WHEN COALESCE(l.exp_last_week, 0) > 0 
        THEN ROUND((COALESCE(t.exp_today, 0) - COALESCE(l.exp_last_week, 0)) / COALESCE(l.exp_last_week, 1) * 100, 1)
        ELSE NULL 
    END AS exp_pct_diff,
    COALESCE(t.install_today, 0) AS install_today,
    COALESCE(l.install_last_week, 0) AS install_last_week,
    COALESCE(t.install_today, 0) - COALESCE(l.install_last_week, 0) AS install_diff,
    CASE 
        WHEN COALESCE(l.install_last_week, 0) > 0 
        THEN ROUND((COALESCE(t.install_today, 0) - COALESCE(l.install_last_week, 0)) / COALESCE(l.install_last_week, 1) * 100, 1)
        ELSE NULL 
    END AS install_pct_diff
FROM today_metrics t
FULL OUTER JOIN last_week_metrics l 
    ON t.brand = l.brand 
    AND t.feature = l.feature
ORDER BY brand, feature
"""

def build_new_devices_query(selected_brands=None, selected_source=None, selected_features=None):
    """Build simple query for new_devices"""
    brands_to_use = selected_brands if selected_brands else BRANDS
    brands_str = "', '".join(brands_to_use)
    features_to_use = selected_features if selected_features else FEATURES
    features_str = "', '".join(features_to_use)
    source_filter = f"AND source = '{selected_source}'" if selected_source else ""
    
    return f"""
SELECT 
    COALESCE(SUM(CASE WHEN date_hour >= TRUNC(GETDATE()) AND date_hour <= GETDATE() THEN new_devices ELSE 0 END), 0) AS new_devices_today,
    COALESCE(SUM(CASE WHEN date_hour >= DATEADD(day, -7, TRUNC(GETDATE())) AND date_hour <= DATEADD(hour, -2, DATEADD(day, -7, GETDATE())) THEN new_devices ELSE 0 END), 0) AS new_devices_last_week
FROM apps.supply_aura_rtm
WHERE brand IN ('{brands_str}')
  AND feature IN ('{features_str}')
  {source_filter}
"""

def build_new_devices_hourly_query(selected_source=None, selected_brands=None, selected_features=None):
    """Build hourly query for new_devices only"""
    brands_to_use = selected_brands if selected_brands else BRANDS
    brands_str = "', '".join(brands_to_use)
    features_to_use = selected_features if selected_features else FEATURES
    features_str = "', '".join(features_to_use)
    source_filter = f"AND source = '{selected_source}'" if selected_source else ""
    
    return f"""
SELECT 
    EXTRACT(HOUR FROM date_hour) AS hour_of_day,
    SUM(CASE WHEN date_hour >= TRUNC(GETDATE()) AND date_hour <= GETDATE() THEN new_devices ELSE 0 END) AS new_devices_today,
    SUM(CASE WHEN date_hour >= DATEADD(day, -7, TRUNC(GETDATE())) AND date_hour <= DATEADD(hour, -2, DATEADD(day, -7, GETDATE())) THEN new_devices ELSE 0 END) AS new_devices_last_week
FROM apps.supply_aura_rtm
WHERE brand IN ('{brands_str}')
  AND feature IN ('{features_str}')
  {source_filter}
  AND (
    (date_hour >= TRUNC(GETDATE()) AND date_hour <= GETDATE())
    OR
    (date_hour >= DATEADD(day, -7, TRUNC(GETDATE())) AND date_hour <= DATEADD(hour, -2, DATEADD(day, -7, GETDATE())))
  )
GROUP BY EXTRACT(HOUR FROM date_hour)
ORDER BY hour_of_day
"""

def build_hourly_query(selected_source=None, selected_brands=None, selected_features=None):
    """Build hourly SQL query dynamically with selected brands and features"""
    brands_to_use = selected_brands if selected_brands else BRANDS
    brands_str = "', '".join(brands_to_use)
    features_to_use = selected_features if selected_features else FEATURES
    features_str = "', '".join(features_to_use)
    source_filter = f"AND source = '{selected_source}'" if selected_source else ""
    
    return f"""
WITH 
todays_hourly AS (
    SELECT 
        brand,
        feature,
        EXTRACT(HOUR FROM date_hour) AS hour_of_day,
        COALESCE(SUM(revenue), 0) AS revenue,
        COALESCE(SUM(notification_shown), 0) AS notifications,
        COALESCE(SUM(experience_shown), 0) AS experiences,
        COALESCE(SUM(install_success), 0) AS installs,
        COALESCE(SUM(new_devices), 0) AS new_devices
    FROM apps.supply_aura_rtm
    WHERE brand IN ('{brands_str}')
      AND feature IN ('{features_str}')
      {source_filter}
      AND date_hour >= TRUNC(GETDATE())
      AND date_hour <= GETDATE()
    GROUP BY brand, feature, EXTRACT(HOUR FROM date_hour)
),
last_week_hourly AS (
    SELECT 
        brand,
        feature,
        EXTRACT(HOUR FROM date_hour) AS hour_of_day,
        COALESCE(SUM(revenue), 0) AS revenue,
        COALESCE(SUM(notification_shown), 0) AS notifications,
        COALESCE(SUM(experience_shown), 0) AS experiences,
        COALESCE(SUM(install_success), 0) AS installs,
        COALESCE(SUM(new_devices), 0) AS new_devices
    FROM apps.supply_aura_rtm
    WHERE brand IN ('{brands_str}')
      AND feature IN ('{features_str}')
      {source_filter}
      AND date_hour >= DATEADD(day, -7, TRUNC(GETDATE()))
      AND date_hour <= DATEADD(hour, -2, DATEADD(day, -7, GETDATE()))
    GROUP BY brand, feature, EXTRACT(HOUR FROM date_hour)
)
SELECT 
    COALESCE(t.brand, l.brand) AS brand,
    COALESCE(t.feature, l.feature) AS feature,
    COALESCE(t.hour_of_day, l.hour_of_day) AS hour_of_day,
    COALESCE(t.revenue, 0) AS revenue_today,
    COALESCE(l.revenue, 0) AS revenue_last_week,
    COALESCE(t.notifications, 0) AS notif_today,
    COALESCE(l.notifications, 0) AS notif_last_week,
    COALESCE(t.experiences, 0) AS exp_today,
    COALESCE(l.experiences, 0) AS exp_last_week,
    COALESCE(t.installs, 0) AS install_today,
    COALESCE(l.installs, 0) AS install_last_week,
    COALESCE(t.new_devices, 0) AS new_devices_today,
    COALESCE(l.new_devices, 0) AS new_devices_last_week
FROM todays_hourly t
FULL OUTER JOIN last_week_hourly l 
    ON t.brand = l.brand 
    AND t.feature = l.feature 
    AND t.hour_of_day = l.hour_of_day
ORDER BY hour_of_day
"""

@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_sample_data():
    """Generate sample data for demonstration purposes"""
    import random
    from datetime import datetime, timedelta
    
    # Generate sample data for the last 7 days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)
    
    # Configuration constants
    CUSTOMERS = [
        "ntt_docomo", "softbank", "htc", "motorola_eu", "samsung_om", "lenovo_sea", 
        "at-t", "kddisamsung", "com.aura.oobe.vodafone", "wiko", "newsroom", "asus", 
        "motorola_apac", "samsung_na", "samsung_sea", "t-mobile", "vodafone", "lenovo", 
        "samsung", "vodafoneSamsung", "lenovo_latam", "softbanksamsung", "sprint", "tinno", 
        "hutchison", "lenovo_na", "samsung_itd", "samsung_cis", "rakuten", "lenovo_eu", 
        "motorola", "sliide", "oppo_latam", "honor", "dish", "oppo_cis", "samsung_eu", 
        "Bouygues", "solutions", "lenovo_apac", "motorola_latam", "deutschetelekomsamsung", 
        "samsung_gl", "samsung_mea", "oppo_sea", "clearly_google_play", "dish-sdk", 
        "motorola_north_america", "sony", "cricket", "deutschetelekom", "oppo_eu", 
        "samsungsfr", "huawei", "ntt_docomo_samsung", "orange", "rakutensamsung", "kddi", 
        "lenovo_mea", "oppo", "lenovo_cis", "bouygues-primary", "oppo_mea"
    ]
    FEATURES = ['oobe', 'silent', 'gotw', 'publisher promotion', 'reef', 'reengagement promotion', 'recurring OOBE']
    
    data = []
    for brand in BRANDS:
        for feature in FEATURES:
            base_value = random.randint(100, 1000)
            data.append({
                'brand': brand,
                'feature': feature,
                'revenue_today': base_value * random.uniform(0.8, 1.2),
                'revenue_last_week': base_value,
                'notif_today': base_value * random.randint(80, 120) // 100,
                'notif_last_week': base_value,
                'exp_today': base_value * random.randint(80, 120) // 100,
                'exp_last_week': base_value,
                'install_today': base_value * random.randint(80, 120) // 100,
                'install_last_week': base_value,
                'new_devices_today': base_value * random.randint(70, 130) // 100,
                'new_devices_last_week': base_value,
            })
    
    df = pd.DataFrame(data)
    
    # Calculate differences and percentages
    for metric in ['revenue', 'notif', 'exp', 'install', 'new_devices']:
        df[f'{metric}_diff'] = df[f'{metric}_today'] - df[f'{metric}_last_week']
        df[f'{metric}_pct_diff'] = (df[f'{metric}_today'] / df[f'{metric}_last_week'] - 1) * 100
    
    return df

@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_data(selected_source=None, selected_brands=None, selected_features=None):
    """Fetch data from Redshift with selected filters"""
    conn = get_connection()
    
    if conn is None:
        st.error("❌ Could not connect to database. Please check your credentials.")
        return pd.DataFrame(), False
    
    try:
        # Build and execute main query
        query = build_sql_query(selected_source, selected_brands, selected_features)
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = 120000")  # 120 seconds (2 minutes)
        
        with st.sidebar:
            with st.spinner("🔍 Executing query... This may take up to 2 minutes."):
                df = pd.read_sql(query, conn)
                
                # Get new_devices separately (faster query)
                try:
                    new_devices_df = pd.read_sql(build_new_devices_query(selected_brands, selected_source, selected_features), conn)
                    
                    if not new_devices_df.empty:
                        # Store new_devices separately (not per row!)
                        today_val = new_devices_df['new_devices_today'].iloc[0]
                        last_week_val = new_devices_df['new_devices_last_week'].iloc[0]
                        
                        # Store as metadata in df.attrs (not as columns!)
                        df.attrs['new_devices_today'] = today_val
                        df.attrs['new_devices_last_week'] = last_week_val
                        df.attrs['new_devices_diff'] = today_val - last_week_val
                        df.attrs['new_devices_pct_diff'] = ((today_val - last_week_val) / last_week_val * 100) if last_week_val > 0 else 0
                    else:
                        st.warning("⚠️ New devices query returned empty DataFrame")
                        df.attrs['new_devices_today'] = 0
                        df.attrs['new_devices_last_week'] = 0
                        df.attrs['new_devices_diff'] = 0
                        df.attrs['new_devices_pct_diff'] = 0
                except Exception as e:
                    # If new_devices query fails, add zeros
                    st.error(f"❌ New devices query failed: {str(e)}")
                    df.attrs['new_devices_today'] = 0
                    df.attrs['new_devices_last_week'] = 0
                    df.attrs['new_devices_diff'] = 0
                    df.attrs['new_devices_pct_diff'] = 0
        
        if df.empty:
            return get_sample_data(), False
            
        return df, True
        
    except (psycopg2.OperationalError, Exception):
        return get_sample_data(), False
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

def export_to_excel(df, filename="aura_data.xlsx"):
    """Export DataFrame to Excel file"""
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Aura Data')
    output.seek(0)
    return output

def format_metric(value, is_currency=False):
    """Format metric value with appropriate formatting"""
    if pd.isna(value):
        return "N/A"
    if is_currency:
        return f"${value:,.2f}"
    return f"{value:,.0f}"

def render_metric_box(title, value, prev_value, is_currency=False):
    """Render a simple metric box"""
    delta = None
    if prev_value != 0 and value is not None and prev_value is not None:
        delta = f"{((value - prev_value) / prev_value) * 100:+.1f}%"
    
    st.markdown(f"""
    <div class="metric-box">
        <div class="metric-value">{format_metric(value, is_currency)}</div>
        <div class="metric-label">{title}</div>
        <div class="metric-delta">{delta}</div>
    </div>
    """, unsafe_allow_html=True)

def plot_hourly_comparison(df, metric, title, y_axis_label, israel_time=True, chart_key=None, new_devices_hourly=None):
    """Helper function to plot hourly comparison charts with improved interactivity"""
    try:
        # Special handling for new_devices (passed separately)
        if metric == 'new_devices':
            if new_devices_hourly is not None and not new_devices_hourly.empty:
                hourly_agg = new_devices_hourly.copy()
            else:
                st.info(f"No data available for {title}")
                return
        else:
            # Aggregate data by hour
            hourly_agg = df.groupby('hour_of_day').agg({
                f'{metric}_today': 'sum',
                f'{metric}_last_week': 'sum'
            }).reset_index()
        
        # The data is already in Israel time from the DB
        # No conversion needed
        hour_column = 'hour_of_day'
        if israel_time:
            x_axis_title = 'Hour of Day (Israel Time)'
        else:
            x_axis_title = 'Hour of Day (UTC)'
        
        # Get the current hour to limit today's data
        from datetime import datetime
        import pytz
        
        # Get current hour in the appropriate timezone
        if israel_time:
            israel_tz = pytz.timezone('Asia/Jerusalem')
            current_hour = datetime.now(israel_tz).hour
        else:
            current_hour = datetime.utcnow().hour
        
        # Filter out hours with no data FIRST
        hourly_agg_filtered = hourly_agg[
            (hourly_agg[f'{metric}_today'] > 0) | 
            (hourly_agg[f'{metric}_last_week'] > 0)
        ].copy()
        
        # Then filter by current hour for today's data
        today_data = hourly_agg_filtered[
            (hourly_agg_filtered['hour_of_day'] <= current_hour) &
            (hourly_agg_filtered[f'{metric}_today'] > 0)
        ].copy()
        last_week_data = hourly_agg_filtered[
            hourly_agg_filtered[f'{metric}_last_week'] > 0
        ].copy()
        
        # Sort by the display hour column
        today_data = today_data.sort_values(hour_column)
        last_week_data = last_week_data.sort_values(hour_column)
        
        if today_data.empty and last_week_data.empty:
            st.info(f"No data available for {title}")
            return
        
        # Create the plot with go.Figure for better control
        fig = go.Figure()
        
        # Add today's line (only up to current hour)
        if not today_data.empty:
            fig.add_trace(go.Scatter(
                x=today_data['hour_of_day'],
                y=today_data[f'{metric}_today'],
                name='Today',
                mode='lines+markers',
                line=dict(color='#0066CC', width=4),
                marker=dict(size=10, symbol='circle', line=dict(width=2, color='white')),
                hovertemplate='<b>Today</b><br>Hour: %{x}:00<br>Value: %{y:,.0f}<extra></extra>'
            ))
        
        # Add last week's line
        if not last_week_data.empty:
            fig.add_trace(go.Scatter(
                x=last_week_data['hour_of_day'],
                y=last_week_data[f'{metric}_last_week'],
                name='Last Week',
                mode='lines+markers',
                line=dict(color='#FF8C00', width=4, dash='dash'),
                marker=dict(size=10, symbol='diamond', line=dict(width=2, color='white')),
                hovertemplate='<b>Last Week</b><br>Hour: %{x}:00<br>Value: %{y:,.0f}<extra></extra>'
            ))
        
        # Update layout with better visibility
        fig.update_layout(
            title=dict(
                text=title, 
                font=dict(size=18, color='#1f1f1f', family='Arial Black'),
                x=0.5,
                xanchor='center'
            ),
            xaxis_title=dict(text=x_axis_title, font=dict(size=14, color='#1f1f1f')),
            yaxis_title=dict(text=y_axis_label, font=dict(size=14, color='#1f1f1f')),
            plot_bgcolor='#f8f9fa',
            paper_bgcolor='white',
            hovermode='x unified',
            height=450,
            xaxis=dict(
                tickmode='linear',
                tick0=0,
                dtick=1,
                showgrid=True,
                gridcolor='#e0e0e0',
                gridwidth=1,
                linecolor='#333',
                linewidth=2,
                mirror=True,
                tickfont=dict(size=12, color='#1f1f1f')
            ),
            yaxis=dict(
                showgrid=True,
                gridcolor='#e0e0e0',
                gridwidth=1,
                linecolor='#333',
                linewidth=2,
                mirror=True,
                rangemode='tozero',
                tickfont=dict(size=12, color='#1f1f1f')
            ),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1,
                bgcolor='rgba(255,255,255,0.9)',
                bordercolor='#333',
                borderwidth=1,
                font=dict(size=12, color='#1f1f1f')
            ),
            margin=dict(l=60, r=40, t=80, b=60)
        )
        
        # Use unique key if provided, otherwise generate from metric and title
        unique_key = chart_key if chart_key else f"chart_{metric}_{title.replace(' ', '_')}"
        st.plotly_chart(fig, use_container_width=True, key=unique_key)
        
    except Exception as e:
        st.error(f"Error generating {title} chart: {str(e)}")

def aggregate_brands_data(df, selected_brands):
    """Aggregate data from multiple brands into a single combined view"""
    if df.empty:
        return df
    
    # Create a copy and add a combined brand name
    aggregated_df = df.copy()
    combined_brand_name = f"Combined ({len(selected_brands)} brands)"
    
    # Group by feature and aggregate all metrics
    agg_dict = {
        'revenue_today': 'sum',
        'revenue_last_week': 'sum',
        'notif_today': 'sum',
        'notif_last_week': 'sum',
        'exp_today': 'sum',
        'exp_last_week': 'sum',
        'install_today': 'sum',
        'install_last_week': 'sum'
    }
    
    aggregated = aggregated_df.groupby('feature').agg(agg_dict).reset_index()
    aggregated['brand'] = combined_brand_name
    
    # Preserve new_devices from attrs if exists
    if hasattr(df, 'attrs'):
        aggregated.attrs['new_devices_today'] = df.attrs.get('new_devices_today', 0)
        aggregated.attrs['new_devices_last_week'] = df.attrs.get('new_devices_last_week', 0)
    
    # Recalculate differences and percentages
    for metric in ['revenue', 'notif', 'exp', 'install']:
        aggregated[f'{metric}_diff'] = aggregated[f'{metric}_today'] - aggregated[f'{metric}_last_week']
        aggregated[f'{metric}_pct_diff'] = (
            (aggregated[f'{metric}_today'] - aggregated[f'{metric}_last_week']) / 
            aggregated[f'{metric}_last_week'] * 100
        ).replace([float('inf'), -float('inf')], 0).fillna(0)
    
    return aggregated

def aggregate_hourly_data(df, selected_brands):
    """Aggregate hourly data from multiple brands"""
    if df.empty:
        return df
    
    combined_brand_name = f"Combined ({len(selected_brands)} brands)"
    
    # Group by feature and hour, aggregate metrics
    agg_dict = {
        'revenue_today': 'sum',
        'revenue_last_week': 'sum',
        'notif_today': 'sum',
        'notif_last_week': 'sum',
        'exp_today': 'sum',
        'exp_last_week': 'sum',
        'install_today': 'sum',
        'install_last_week': 'sum'
    }
    
    aggregated = df.groupby(['feature', 'hour_of_day']).agg(agg_dict).reset_index()
    aggregated['brand'] = combined_brand_name
    
    return aggregated

def generate_insights(filtered_df, filtered_hourly_df, new_devices_today, new_devices_last_week):
    """Generate smart insights from the data"""
    insights = []
    
    # Revenue insight
    revenue_today = filtered_df['revenue_today'].sum()
    revenue_last_week = filtered_df['revenue_last_week'].sum()
    revenue_change = ((revenue_today - revenue_last_week) / revenue_last_week * 100) if revenue_last_week > 0 else 0
    
    if abs(revenue_change) > 20:
        emoji = "🔥" if revenue_change > 0 else "⚠️"
        insights.append(f"{emoji} Revenue {'+' if revenue_change > 0 else ''}{revenue_change:.1f}% vs last week - {'Great performance!' if revenue_change > 0 else 'Needs attention'}")
    
    # Peak hour insight
    if filtered_hourly_df is not None and not filtered_hourly_df.empty:
        hourly_notif = filtered_hourly_df.groupby('hour_of_day')['notif_today'].sum()
        if not hourly_notif.empty:
            peak_hour = hourly_notif.idxmax()
            peak_value = hourly_notif.max()
            insights.append(f"📊 Peak hour today: {int(peak_hour)}:00 ({peak_value:,.0f} notifications)")
    
    # New devices insight
    if new_devices_today > 0:
        nd_change = ((new_devices_today - new_devices_last_week) / new_devices_last_week * 100) if new_devices_last_week > 0 else 0
        if abs(nd_change) > 10:
            emoji = "📈" if nd_change > 0 else "📉"
            insights.append(f"{emoji} New Devices {'+' if nd_change > 0 else ''}{nd_change:.1f}% - {new_devices_today:,.0f} today")
    
    # Top performing feature
    top_feature = filtered_df.groupby('feature')['revenue_today'].sum().idxmax()
    top_revenue = filtered_df.groupby('feature')['revenue_today'].sum().max()
    insights.append(f"⭐ Top feature: {top_feature} (${top_revenue:,.2f})")
    
    return insights

def render_overview_tab(filtered_df, filtered_hourly_df=None, israel_time=True, new_devices_hourly=None):
    """Render the overview tab with key metrics, data table, and hourly charts"""
    # Calculate totals
    revenue_today = filtered_df['revenue_today'].sum()
    revenue_last_week = filtered_df['revenue_last_week'].sum()
    notif_today = filtered_df['notif_today'].sum()
    notif_last_week = filtered_df['notif_last_week'].sum()
    exp_today = filtered_df['exp_today'].sum()
    exp_last_week = filtered_df['exp_last_week'].sum()
    install_today = filtered_df['install_today'].sum()
    install_last_week = filtered_df['install_last_week'].sum()
    # Get new_devices from df.attrs (stored separately, not per row)
    new_devices_today = filtered_df.attrs.get('new_devices_today', 0)
    new_devices_last_week = filtered_df.attrs.get('new_devices_last_week', 0)
    
    # Calculate eCPI (effective Cost Per Install) = Revenue / Installs
    ecpi_today = (revenue_today / install_today) if install_today > 0 else 0
    ecpi_last_week = (revenue_last_week / install_last_week) if install_last_week > 0 else 0
    
    # Calculate RPU (Revenue Per User) = Revenue / Experiences
    rpu_today = (revenue_today / exp_today) if exp_today > 0 else 0
    rpu_last_week = (revenue_last_week / exp_last_week) if exp_last_week > 0 else 0
    
    # Display metrics in a grid using st.metric
    st.subheader("📊 Key Metrics")
    
    # First row - main metrics
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        delta_pct = ((revenue_today - revenue_last_week) / revenue_last_week * 100) if revenue_last_week > 0 else 0
        st.metric(
            label="💰 Revenue",
            value=f"${revenue_today:,.2f}",
            delta=f"{delta_pct:+.1f}%"
        )
    
    with col2:
        delta_pct = ((notif_today - notif_last_week) / notif_last_week * 100) if notif_last_week > 0 else 0
        st.metric(
            label="🔔 Notifications",
            value=f"{notif_today:,.0f}",
            delta=f"{delta_pct:+.1f}%"
        )
    
    with col3:
        delta_pct = ((exp_today - exp_last_week) / exp_last_week * 100) if exp_last_week > 0 else 0
        st.metric(
            label="👁️ Experiences",
            value=f"{exp_today:,.0f}",
            delta=f"{delta_pct:+.1f}%"
        )
    
    with col4:
        delta_pct = ((install_today - install_last_week) / install_last_week * 100) if install_last_week > 0 else 0
        st.metric(
            label="📥 Installs",
            value=f"{install_today:,.0f}",
            delta=f"{delta_pct:+.1f}%"
        )
    
    with col5:
        delta_pct = ((new_devices_today - new_devices_last_week) / new_devices_last_week * 100) if new_devices_last_week > 0 else 0
        st.metric(
            label="📱 New Devices",
            value=f"{new_devices_today:,.0f}",
            delta=f"{delta_pct:+.1f}%"
        )
    
    # Second row - calculated metrics
    st.markdown("---")
    col6, col7, col8, col9, col10 = st.columns(5)
    
    with col6:
        delta_pct = ((ecpi_today - ecpi_last_week) / ecpi_last_week * 100) if ecpi_last_week > 0 else 0
        st.metric(
            label="💵 eCPI",
            value=f"${ecpi_today:.3f}",
            delta=f"{delta_pct:+.1f}%",
            help="Effective Cost Per Install = Revenue / Installs"
        )
    
    with col7:
        delta_pct = ((rpu_today - rpu_last_week) / rpu_last_week * 100) if rpu_last_week > 0 else 0
        st.metric(
            label="💎 RPU",
            value=f"${rpu_today:.4f}",
            delta=f"{delta_pct:+.1f}%",
            help="Revenue Per User = Revenue / Experiences"
        )
    
    with col8:
        st.metric(label="", value="")
    
    with col9:
        st.metric(label="", value="")
    
    with col10:
        st.metric(label="", value="")
    
    # Show data table
    st.subheader("📋 Detailed Data")
    
    # Create a copy for display
    display_df = filtered_df.copy()
    
    # Format columns
    for col in display_df.columns:
        if 'pct_diff' in col:
            display_df[col] = display_df[col].apply(lambda x: f"{x:+.1f}%" if pd.notnull(x) else "N/A")
        elif 'revenue' in col:
            display_df[col] = display_df[col].apply(lambda x: f"${x:,.2f}" if pd.notnull(x) else "N/A")
        elif pd.api.types.is_numeric_dtype(display_df[col]):
            display_df[col] = display_df[col].apply(lambda x: f"{x:,.0f}" if pd.notnull(x) else "N/A")
    
    st.dataframe(display_df, use_container_width=True, height=400)
    
    # Export button
    excel_data = export_to_excel(filtered_df)
    st.download_button(
        label="📥 Download Excel",
        data=excel_data,
        file_name=f"aura_data_{dt.now().strftime('%Y%m%d_%H%M')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=False
    )
    
    # Add hourly performance charts
    if filtered_hourly_df is not None and not filtered_hourly_df.empty:
        st.markdown("---")
        st.subheader("📈 Hourly Performance")
        st.caption("Key metrics by hour - Today vs Last Week")
        
        # Display charts vertically for better visibility
        plot_hourly_comparison(filtered_hourly_df, 'revenue', '💰 Revenue by Hour', 'Revenue ($)', israel_time, 'overview_revenue', new_devices_hourly)
        plot_hourly_comparison(filtered_hourly_df, 'notif', '🔔 Notifications by Hour', 'Notifications', israel_time, 'overview_notif', new_devices_hourly)
        plot_hourly_comparison(filtered_hourly_df, 'new_devices', '📱 New Devices by Hour', 'New Devices', israel_time, 'overview_new_devices', new_devices_hourly)

def render_hourly_tab(filtered_hourly_df, israel_time=True):
    """Render the hourly trends tab with interactive charts"""
    if filtered_hourly_df.empty:
        st.warning("No hourly data available.")
        return
    
    st.subheader("📈 Hourly Trends")
    
    # Create two columns for charts
    col1, col2 = st.columns(2)
    
    with col1:
        plot_hourly_comparison(filtered_hourly_df, 'revenue', '💰 Revenue by Hour', 'Revenue ($)', israel_time, 'hourly_revenue')
        plot_hourly_comparison(filtered_hourly_df, 'exp', '👁️ Experiences by Hour', 'Experiences', israel_time, 'hourly_exp')
    
    with col2:
        plot_hourly_comparison(filtered_hourly_df, 'notif', '🔔 Notifications by Hour', 'Notifications', israel_time, 'hourly_notif')
        plot_hourly_comparison(filtered_hourly_df, 'install', '📥 Installs by Hour', 'Installs', israel_time, 'hourly_install')

def render_comparison_tab(filtered_df):
    """Render the comparison tab with brand/feature breakdowns"""
    st.subheader("📊 Brand & Feature Comparison")
    
    # Brand comparison
    st.markdown("### By Brand")
    brand_summary = filtered_df.groupby('brand').agg({
        'revenue_today': 'sum',
        'revenue_last_week': 'sum',
        'notif_today': 'sum',
        'exp_today': 'sum',
        'install_today': 'sum'
    }).reset_index()
    
    # Create bar chart for revenue by brand
    fig = px.bar(
        brand_summary,
        x='brand',
        y=['revenue_today', 'revenue_last_week'],
        title='Revenue Comparison by Brand',
        labels={'value': 'Revenue ($)', 'variable': 'Period'},
        barmode='group',
        color_discrete_map={'revenue_today': '#1f77b4', 'revenue_last_week': '#ff7f0e'}
    )
    fig.update_layout(height=400, plot_bgcolor='white', paper_bgcolor='white')
    st.plotly_chart(fig, use_container_width=True)
    
    # Feature comparison
    st.markdown("### By Feature")
    feature_summary = filtered_df.groupby('feature').agg({
        'revenue_today': 'sum',
        'revenue_last_week': 'sum',
        'notif_today': 'sum',
        'exp_today': 'sum',
        'install_today': 'sum'
    }).reset_index()
    
    # Create bar chart for revenue by feature
    fig = px.bar(
        feature_summary,
        x='feature',
        y=['revenue_today', 'revenue_last_week'],
        title='Revenue Comparison by Feature',
        labels={'value': 'Revenue ($)', 'variable': 'Period'},
        barmode='group',
        color_discrete_map={'revenue_today': '#1f77b4', 'revenue_last_week': '#ff7f0e'}
    )
    fig.update_layout(height=400, plot_bgcolor='white', paper_bgcolor='white')
    st.plotly_chart(fig, use_container_width=True)

def render_dashboard(df, hourly_df, is_real_data, new_devices_hourly=None):
    """Render the enhanced dashboard with filters and charts"""
    st.title("📊 Aura Dashboard")
    
    # Display last updated time and data source
    col1, col2 = st.columns([3, 1])
    with col1:
        st.caption(f"Last updated: {dt.now().strftime('%Y-%m-%d %H:%M')}")
    with col2:
        if is_real_data:
            st.success("🟢 Live Data", icon="✅")
        else:
            st.warning("🟡 Sample Data", icon="⚠️")
    
    if df.empty or hourly_df.empty:
        st.warning("No data available. Please check your database connection.")
        return
    
    # Get unique values for filters
    all_brands = sorted(df['brand'].unique().tolist())
    all_features = sorted(df['feature'].unique().tolist())
    
    # Sidebar filters
    with st.sidebar:
        st.header("🔍 View Options")
        
        # Timezone selection
        st.markdown("### 🕐 Time Zone")
        use_israel_time = st.checkbox(
            "Show Israel Time (UTC+2)",
            value=True,
            help="Convert hourly data to Israel timezone"
        )
        
        # Add refresh button
        if st.button("🔄 Refresh Data", use_container_width=True):
            # Clear both cache and session state
            st.cache_data.clear()
            for key in ['df', 'hourly_df', 'is_real_data', 'new_devices_hourly']:
                if key in st.session_state:
                    del st.session_state[key]
            st.rerun()
        
        # Add some space
        st.markdown("---")
        
        # Show data summary
        st.subheader("ℹ️ Data Summary")
        st.caption(f"Total Rows: {len(df):,}")
        st.caption(f"Brands: {len(df['brand'].unique())}")
        st.caption(f"Features: {len(df['feature'].unique())}")
    
    # Data is already filtered by the query
    filtered_df = df.copy()
    filtered_hourly_df = hourly_df.copy() if not hourly_df.empty else pd.DataFrame()
    
    # Show warning if no data
    if filtered_df.empty:
        st.warning("⚠️ No data available for the selected filters.")
        return
    
    # Create tabs for different views
    tab1, tab2 = st.tabs(["📊 Overview", "📈 Hourly Trends"])
    
    with tab1:
        render_overview_tab(filtered_df, filtered_hourly_df, use_israel_time, new_devices_hourly)
    
    with tab2:
        render_hourly_tab(filtered_hourly_df, use_israel_time)

def main():
    """Main function to run the Streamlit app"""
    try:
        # Get filters BEFORE loading data for efficiency
        with st.sidebar:
            st.markdown("### 📡 Source Filter")
            source_options = ["All", "pre-install", "FOTA"]
            selected_source_display = st.selectbox(
                'Data Source',
                source_options,
                index=0,
                help="Filter by data source",
                key="source_filter_main"
            )
            selected_source = None if selected_source_display == "All" else selected_source_display
            
            st.markdown("### 🏷️ Brands Filter")
            st.caption("Select brands to query")
            select_all_brands = st.checkbox("Select All Brands", value=False, key="select_all_main")
            
            if select_all_brands:
                selected_brands = BRANDS
            else:
                selected_brands = st.multiselect(
                    'Choose Brands',
                    BRANDS,
                    default=[],
                    help="Select specific brands to query",
                    key="brands_filter_main"
                )
            
            st.markdown("### 🎯 Features Filter")
            st.caption("Select features to query")
            select_all_features_main = st.checkbox("Select All Features", value=True, key="select_all_features_main")
            
            if select_all_features_main:
                selected_features = FEATURES
            else:
                selected_features = st.multiselect(
                    'Choose Features',
                    FEATURES,
                    default=[],
                    help="Select specific features to query",
                    key="features_filter_main"
                )
            
            # Check if both brands and features are selected
            if not selected_brands or not selected_features:
                if not selected_brands:
                    st.info("👆 Please select at least one brand")
                if not selected_features:
                    st.info("👆 Please select at least one feature")
                return
            
            # Combine brands option (if multiple brands selected)
            combine_brands = False
            if len(selected_brands) > 1:
                combine_brands = st.checkbox(
                    "🔗 Combine selected brands into one aggregated view",
                    value=False,
                    help="Query and aggregate all selected brands together (faster!)",
                    key="combine_brands_main"
                )
            
            st.markdown("---")
            
            # Show what will be queried
            st.caption(f"📊 {len(selected_brands)} brand(s) × {len(selected_features)} feature(s)")
            if combine_brands:
                st.caption("🔗 Will combine brands into one view")
            
            # Load Data button
            load_data = st.button(
                "🚀 Load Data",
                type="primary",
                use_container_width=True,
                help="Click to run the query with selected filters"
            )
            
            if load_data:
                # Store in session state
                st.session_state['selected_source'] = selected_source
                st.session_state['selected_brands'] = selected_brands
                st.session_state['selected_features'] = selected_features
                st.session_state['combine_brands'] = combine_brands
                st.session_state['data_loaded'] = True
        
        # Check if we should load data
        if not st.session_state.get('data_loaded', False):
            st.info("👆 Click 'Load Data' in the sidebar to run the query")
            return
        
        # Get stored values from session state
        selected_source = st.session_state.get('selected_source')
        selected_brands = st.session_state.get('selected_brands')
        selected_features = st.session_state.get('selected_features', FEATURES)
        combine_brands = st.session_state.get('combine_brands', False)
        
        # Check if we already have data in session state
        if 'df' in st.session_state and not st.session_state['df'].empty:
            # Use cached data from session state
            st.sidebar.success("✅ Using cached data")
            df = st.session_state['df']
            hourly_df = st.session_state.get('hourly_df', pd.DataFrame())
            is_real_data = st.session_state.get('is_real_data', False)
            new_devices_hourly = st.session_state.get('new_devices_hourly', None)
        else:
            # Load the data with selected filters (first time only)
            st.sidebar.info("🔄 Loading fresh data...")
            with st.spinner(f'Loading data for {len(selected_brands)} brand(s) × {len(selected_features)} feature(s)...'):
                df, is_real_data = get_data(selected_source, selected_brands, selected_features)
                
                # Get hourly data for charts
                hourly_df = pd.DataFrame()
                new_devices_hourly = None
                try:
                    conn = get_connection()
                    if conn:
                        hourly_df = pd.read_sql(build_hourly_query(selected_source, selected_brands, selected_features), conn)
                        
                        # Get new_devices hourly separately (not grouped by feature)
                        try:
                            new_devices_hourly = pd.read_sql(build_new_devices_hourly_query(selected_source, selected_brands, selected_features), conn)
                        except Exception as e:
                            new_devices_hourly = None
                        
                        conn.close()
                except Exception as e:
                    st.sidebar.warning(f"⚠️ Could not load hourly data: {str(e)}")
            
                # Apply aggregation if requested (before rendering)
                if combine_brands and len(selected_brands) > 1 and not df.empty:
                    st.info(f"📊 Showing combined view of {len(selected_brands)} brands: {', '.join(selected_brands[:3])}{'...' if len(selected_brands) > 3 else ''}")
                    df = aggregate_brands_data(df, selected_brands)
                    if not hourly_df.empty:
                        hourly_df = aggregate_hourly_data(hourly_df, selected_brands)
            
                # Store data in session state
                st.session_state['df'] = df
                st.session_state['hourly_df'] = hourly_df
                st.session_state['is_real_data'] = is_real_data
                st.session_state['new_devices_hourly'] = new_devices_hourly
        
        # Render the dashboard
        if not df.empty:
            render_dashboard(df, hourly_df, is_real_data, new_devices_hourly)
        else:
            st.error("No data available. Please check your database connection.")
            
    except Exception as e:
        st.error(f"An error occurred: {str(e)}")
        st.exception(e)  # Show full error details for debugging

if __name__ == "__main__":
    main()
