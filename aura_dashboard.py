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
    /* Main app background */
    .stApp {
        background-color: #0e1117;
        color: #fafafa;
    }
    
    /* Main content area */
    .main .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        background-color: #0e1117;
    }
    
    /* Sidebar */
    [data-testid="stSidebar"] {
        background-color: #262730;
    }
    
    [data-testid="stSidebar"] * {
        color: #fafafa !important;
    }
    
    /* Headers - white and readable */
    h1, h2, h3, h4, h5, h6 {
        color: #fafafa !important;
    }
    
    /* Metric boxes */
    .metric-box {
        background: #1e1e1e;
        border-radius: 10px;
        padding: 1.5rem;
        margin-bottom: 1rem;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0,0,0,0.3);
        border: 1px solid #333;
    }
    
    .metric-value {
        font-size: 1.8rem;
        font-weight: 700;
        color: #fafafa;
        margin: 0.5rem 0;
    }
    
    .metric-label {
        font-size: 1rem;
        color: #b0b0b0;
        margin-bottom: 0.5rem;
    }
    
    .metric-delta {
        font-size: 0.9rem;
        font-weight: 500;
    }
    
    .metric-delta.positive {
        color: #4caf50;
    }
    
    .metric-delta.negative {
        color: #f44336;
    }
    
    /* Make all text readable */
    p, span, div, label {
        color: #fafafa !important;
    }
    
    /* Streamlit widgets */
    .stSelectbox label, .stMultiSelect label, .stCheckbox label {
        color: #fafafa !important;
    }
    
    /* Data tables */
    .dataframe {
        color: #fafafa !important;
        background-color: #1e1e1e !important;
    }
    
    .dataframe th {
        background-color: #262730 !important;
        color: #fafafa !important;
    }
    
    .dataframe td {
        color: #fafafa !important;
    }
    
    /* Info/Warning/Error boxes */
    .stAlert {
        background-color: #1e1e1e !important;
        color: #fafafa !important;
    }
</style>
""", unsafe_allow_html=True)

def get_connection():
    """Establish a connection to the Redshift database with detailed error handling"""
    try:
        # Load connection parameters from environment variables (SECURE)
        host = os.getenv('REDSHIFT_HOST')
        dbname = os.getenv('REDSHIFT_DB')
        user = os.getenv('REDSHIFT_USER')
        password = os.getenv('REDSHIFT_PASS')
        port = os.getenv('REDSHIFT_PORT', 5439)
        
        # Debug log the connection details (without password)
        debug_info = f"""
        🛠️ Connection Details:
        - Host: {host}
        - Port: {port}
        - Database: {dbname}
        - User: {user}
        - Password: {'*' * 8 if password else 'Not set'}
        """
        st.sidebar.code(debug_info, language='bash')
        
        # Validate required environment variables
        if not all([host, dbname, user, password]):
            missing = [k for k, v in {
                'REDSHIFT_HOST': host,
                'REDSHIFT_DB': dbname,
                'REDSHIFT_USER': user,
                'REDSHIFT_PASS': password
            }.items() if not v]
            
            st.error(f"❌ Missing required database parameters: {', '.join(missing)}.\nPlease check your .env file.")
            return None
        
        # Connection parameters
        conn_params = {
            'host': host,
            'database': dbname,
            'user': user,
            'password': password,
            'port': int(port),
            'connect_timeout': 10,
            'sslmode': 'require',
            'keepalives': 1,
            'keepalives_idle': 30,
            'keepalives_interval': 10,
            'keepalives_count': 5
        }
        
        # Attempt to establish connection
        conn = psycopg2.connect(**conn_params)
        conn.autocommit = True
        return conn
        
    except (psycopg2.OperationalError, Exception):
        return None

def build_sql_query():
    """Build SQL query dynamically with all available brands"""
    brands_str = "', '".join(BRANDS)
    features_str = "', '".join(FEATURES)
    
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
      AND date_hour >= DATEADD(day, -7, TRUNC(GETDATE()))
      AND date_hour <= DATEADD(day, -7, GETDATE())
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

def build_hourly_query():
    """Build hourly SQL query dynamically with all available brands"""
    brands_str = "', '".join(BRANDS)
    features_str = "', '".join(FEATURES)
    
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
        COALESCE(SUM(install_success), 0) AS installs
    FROM apps.supply_aura_rtm
    WHERE brand IN ('{brands_str}')
      AND feature IN ('{features_str}')
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
        COALESCE(SUM(install_success), 0) AS installs
    FROM apps.supply_aura_rtm
    WHERE brand IN ('{brands_str}')
      AND feature IN ('{features_str}')
      AND date_hour >= DATEADD(day, -7, TRUNC(GETDATE()))
      AND date_hour <= DATEADD(day, -7, GETDATE())
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
    COALESCE(l.installs, 0) AS install_last_week
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
            })
    
    df = pd.DataFrame(data)
    
    # Calculate differences and percentages
    for metric in ['revenue', 'notif', 'exp', 'install']:
        df[f'{metric}_diff'] = df[f'{metric}_today'] - df[f'{metric}_last_week']
        df[f'{metric}_pct_diff'] = (df[f'{metric}_today'] / df[f'{metric}_last_week'] - 1) * 100
    
    return df

@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_data():
    """Fetch data from Redshift and return as DataFrame"""
    conn = None
    try:
        conn = get_connection()
        
        if conn is None:
            return get_sample_data(), False
            
        # Set statement timeout (in milliseconds)
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = 120000")  # 120 seconds (2 minutes)
        
        with st.sidebar:
            with st.spinner("🔍 Executing query... This may take up to 2 minutes."):
                df = pd.read_sql(build_sql_query(), conn)
        
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

def plot_hourly_comparison(df, metric, title, y_axis_label, israel_time=True):
    """Helper function to plot hourly comparison charts with improved interactivity"""
    try:
        # Aggregate data by hour
        hourly_agg = df.groupby('hour_of_day').agg({
            f'{metric}_today': 'sum',
            f'{metric}_last_week': 'sum'
        }).reset_index()
        
        # Convert to Israel time if requested (UTC+2, or UTC+3 during DST)
        if israel_time:
            # Add 2 hours for Israel Standard Time (you can adjust to 3 for DST)
            hourly_agg['hour_israel'] = (hourly_agg['hour_of_day'] + 2) % 24
            hour_column = 'hour_israel'
            x_axis_title = 'Hour of Day (Israel Time)'
        else:
            hour_column = 'hour_of_day'
            x_axis_title = 'Hour of Day (UTC)'
        
        # Get the current hour to limit today's data
        from datetime import datetime
        current_hour = datetime.now().hour
        
        # Filter out hours with no data FIRST
        hourly_agg_filtered = hourly_agg[
            (hourly_agg[f'{metric}_today'] > 0) | 
            (hourly_agg[f'{metric}_last_week'] > 0)
        ].copy()
        
        # Then filter by current hour for today's data
        if israel_time:
            today_data = hourly_agg_filtered[
                (hourly_agg_filtered['hour_israel'] <= current_hour) &
                (hourly_agg_filtered[f'{metric}_today'] > 0)
            ].copy()
            last_week_data = hourly_agg_filtered[
                hourly_agg_filtered[f'{metric}_last_week'] > 0
            ].copy()
        else:
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
                x=today_data[hour_column] if israel_time else today_data['hour_of_day'],
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
                x=last_week_data[hour_column] if israel_time else last_week_data['hour_of_day'],
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
        
        st.plotly_chart(fig, use_container_width=True)
        
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

def render_overview_tab(filtered_df):
    """Render the overview tab with key metrics and data table"""
    # Calculate totals
    revenue_today = filtered_df['revenue_today'].sum()
    revenue_last_week = filtered_df['revenue_last_week'].sum()
    notif_today = filtered_df['notif_today'].sum()
    notif_last_week = filtered_df['notif_last_week'].sum()
    exp_today = filtered_df['exp_today'].sum()
    exp_last_week = filtered_df['exp_last_week'].sum()
    install_today = filtered_df['install_today'].sum()
    install_last_week = filtered_df['install_last_week'].sum()
    
    # Display metrics in a grid using st.metric
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        delta_pct = ((revenue_today - revenue_last_week) / revenue_last_week * 100) if revenue_last_week > 0 else 0
        st.metric(
            label="💰 Revenue (Today)",
            value=f"${revenue_today:,.2f}",
            delta=f"{delta_pct:+.1f}%"
        )
    
    with col2:
        delta_pct = ((notif_today - notif_last_week) / notif_last_week * 100) if notif_last_week > 0 else 0
        st.metric(
            label="🔔 Notifications (Today)",
            value=f"{notif_today:,.0f}",
            delta=f"{delta_pct:+.1f}%"
        )
    
    with col3:
        delta_pct = ((exp_today - exp_last_week) / exp_last_week * 100) if exp_last_week > 0 else 0
        st.metric(
            label="👁️ Experiences (Today)",
            value=f"{exp_today:,.0f}",
            delta=f"{delta_pct:+.1f}%"
        )
    
    with col4:
        delta_pct = ((install_today - install_last_week) / install_last_week * 100) if install_last_week > 0 else 0
        st.metric(
            label="📥 Installs (Today)",
            value=f"{install_today:,.0f}",
            delta=f"{delta_pct:+.1f}%"
        )
    
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

def render_hourly_tab(filtered_hourly_df, israel_time=True):
    """Render the hourly trends tab with interactive charts"""
    if filtered_hourly_df.empty:
        st.warning("No hourly data available.")
        return
    
    st.subheader("📈 Hourly Trends")
    
    # Create two columns for charts
    col1, col2 = st.columns(2)
    
    with col1:
        plot_hourly_comparison(filtered_hourly_df, 'revenue', '💰 Revenue by Hour', 'Revenue ($)', israel_time)
        plot_hourly_comparison(filtered_hourly_df, 'exp', '👁️ Experiences by Hour', 'Experiences', israel_time)
    
    with col2:
        plot_hourly_comparison(filtered_hourly_df, 'notif', '🔔 Notifications by Hour', 'Notifications', israel_time)
        plot_hourly_comparison(filtered_hourly_df, 'install', '📥 Installs by Hour', 'Installs', israel_time)

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

def render_dashboard(df, hourly_df, is_real_data):
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
        st.header("🔍 Filters")
        
        # Brand filter with multiselect
        st.markdown("### 🏷️ Brands")
        select_all_brands = st.checkbox("Select All Brands", value=True)
        
        if select_all_brands:
            selected_brands = all_brands
        else:
            selected_brands = st.multiselect(
                'Choose Brands',
                all_brands,
                default=[],
                help="Select one or more brands to filter"
            )
        
        st.caption(f"Selected: {len(selected_brands)} brands")
        
        # Aggregate brands option
        if len(selected_brands) > 1:
            aggregate_brands = st.checkbox(
                "🔗 Combine selected brands into one view",
                value=False,
                help="Aggregate all selected brands data together"
            )
        else:
            aggregate_brands = False
        
        # Feature filter with multiselect
        st.markdown("### 🎯 Features")
        select_all_features = st.checkbox("Select All Features", value=True)
        
        if select_all_features:
            selected_features = all_features
        else:
            selected_features = st.multiselect(
                'Choose Features',
                all_features,
                default=[],
                help="Select one or more features to filter"
            )
        
        st.caption(f"Selected: {len(selected_features)} features")
        
        # Timezone selection
        st.markdown("### 🕐 Time Zone")
        use_israel_time = st.checkbox(
            "Show Israel Time (UTC+2)",
            value=True,
            help="Convert hourly data to Israel timezone"
        )
        
        # Add refresh button
        if st.button("🔄 Refresh Data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
        
        # Add some space
        st.markdown("---")
        
        # Show data summary
        st.subheader("ℹ️ Data Summary")
        st.caption(f"Total Rows: {len(df):,}")
        st.caption(f"Brands: {len(df['brand'].unique())}")
        st.caption(f"Features: {len(df['feature'].unique())}")
    
    # Apply filters to data
    filtered_df = df.copy()
    filtered_hourly_df = hourly_df.copy() if not hourly_df.empty else pd.DataFrame()
    
    # Filter by selected brands
    if selected_brands:
        filtered_df = filtered_df[filtered_df['brand'].isin(selected_brands)]
        if not filtered_hourly_df.empty:
            filtered_hourly_df = filtered_hourly_df[filtered_hourly_df['brand'].isin(selected_brands)]
    
    # Filter by selected features
    if selected_features:
        filtered_df = filtered_df[filtered_df['feature'].isin(selected_features)]
        if not filtered_hourly_df.empty:
            filtered_hourly_df = filtered_hourly_df[filtered_hourly_df['feature'].isin(selected_features)]
    
    # Show warning if no data after filtering
    if filtered_df.empty:
        st.warning("⚠️ No data matches the selected filters. Please adjust your selection.")
        return
    
    # Apply aggregation if requested
    if aggregate_brands and len(selected_brands) > 1:
        st.info(f"📊 Showing combined view of {len(selected_brands)} brands: {', '.join(selected_brands[:3])}{'...' if len(selected_brands) > 3 else ''}")
        filtered_df = aggregate_brands_data(filtered_df, selected_brands)
        if not filtered_hourly_df.empty:
            filtered_hourly_df = aggregate_hourly_data(filtered_hourly_df, selected_brands)
    
    # Create tabs for different views
    tab1, tab2, tab3 = st.tabs(["📊 Overview", "📈 Hourly Trends", "🔍 Comparison"])
    
    with tab1:
        render_overview_tab(filtered_df)
    
    with tab2:
        render_hourly_tab(filtered_hourly_df, use_israel_time)
    
    with tab3:
        render_comparison_tab(filtered_df)

def main():
    """Main function to run the Streamlit app"""
    try:
        # Load the data
        with st.spinner('Loading data...'):
            df, is_real_data = get_data()
            
            # Get hourly data for charts
            hourly_df = pd.DataFrame()
            try:
                conn = get_connection()
                if conn:
                    hourly_df = pd.read_sql(build_hourly_query(), conn)
                    conn.close()
            except Exception as e:
                st.sidebar.warning(f"⚠️ Could not load hourly data: {str(e)}")
        
        # Render the dashboard
        if not df.empty:
            render_dashboard(df, hourly_df, is_real_data)
        else:
            st.error("No data available. Please check your database connection.")
            
    except Exception as e:
        st.error(f"An error occurred: {str(e)}")
        st.exception(e)  # Show full error details for debugging

if __name__ == "__main__":
    main()
