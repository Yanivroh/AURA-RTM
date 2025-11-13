# 📊 Aura Dashboard

A comprehensive, interactive analytics dashboard for monitoring Aura supply metrics across multiple brands and features. Built with Streamlit and Plotly for real-time data visualization and analysis.

![Python](https://img.shields.io/badge/python-3.8+-blue.svg)
![Streamlit](https://img.shields.io/badge/streamlit-1.29.0-red.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)

## ✨ Features

### 📈 Real-Time Analytics
- **Live Data Connection** to Redshift database
- **Sample Data Mode** for testing and development
- **Auto-refresh** with configurable cache (5 minutes TTL)
- **Performance Optimized** with 2-minute query timeout

### 🎯 Advanced Filtering
- **Multi-Brand Selection** - Choose from 62+ brands
- **Multi-Feature Selection** - Filter by specific features
- **Brand Aggregation** - Combine multiple brands into a single view
- **"Select All" Options** - Quick selection for all brands/features

### 📊 Interactive Visualizations
- **Hourly Trends** - Compare today vs. last week by hour
- **Brand Comparison** - Side-by-side brand performance analysis
- **Feature Breakdown** - Analyze performance by feature type
- **Israel Time Zone Support** - Display data in local time (UTC+2)

### 📋 Data Management
- **Excel Export** - Download filtered data with timestamp
- **Detailed Tables** - Formatted data with percentage changes
- **Real-time Metrics** - Revenue, Notifications, Experiences, Installs
- **Delta Indicators** - Visual comparison with last week

## 🚀 Quick Start

### Prerequisites
- Python 3.8 or higher
- Access to Redshift database (or use sample data mode)

### Installation

1. **Clone the repository**
```bash
git clone <your-repo-url>
cd windsurf-project-3
```

2. **Create virtual environment**
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```

4. **Configure environment variables**
Create a `.env` file in the project root:
```env
REDSHIFT_HOST=your-redshift-host.amazonaws.com
REDSHIFT_DB=your_database_name
REDSHIFT_USER=your_username
REDSHIFT_PASS=your_password
REDSHIFT_PORT=5439
```

⚠️ **Security Note:** Never commit the `.env` file to git!

5. **Run the dashboard**
```bash
streamlit run aura_dashboard.py
```

The dashboard will open in your browser at `http://localhost:8501`

## 📖 Usage Guide

### Main Dashboard Tabs

#### 📊 Overview Tab
- **Key Metrics Cards** - Today's performance with week-over-week comparison
- **Detailed Data Table** - Complete dataset with all metrics
- **Excel Export** - Download button for offline analysis

#### 📈 Hourly Trends Tab
- **Revenue by Hour** - Hourly revenue comparison
- **Notifications by Hour** - Notification delivery patterns
- **Experiences by Hour** - User experience trends
- **Installs by Hour** - Installation success rates

#### 🔍 Comparison Tab
- **By Brand** - Compare performance across brands
- **By Feature** - Analyze feature effectiveness

### Sidebar Controls

#### 🏷️ Brands Filter
- **Select All Brands** - Include all 62+ brands
- **Custom Selection** - Choose specific brands
- **Combine Brands** - Aggregate multiple brands into one view

#### 🎯 Features Filter
- **Select All Features** - Include all features
- **Custom Selection** - Choose specific features

#### 🕐 Time Zone
- **Israel Time (UTC+2)** - Display hours in local time
- **UTC** - Show original database time

#### 🔄 Refresh Data
- Clear cache and reload data from database

## 🏗️ Architecture

### Technology Stack
- **Frontend:** Streamlit
- **Visualization:** Plotly (go.Figure, px.bar)
- **Data Processing:** Pandas
- **Database:** Amazon Redshift (PostgreSQL)
- **Styling:** Custom CSS

### Key Components

```
aura_dashboard.py
├── Configuration (BRANDS, FEATURES)
├── Database Connection (get_connection)
├── Query Builders (build_sql_query, build_hourly_query)
├── Data Processing
│   ├── get_data() - Main metrics
│   ├── get_sample_data() - Demo data
│   ├── aggregate_brands_data() - Brand aggregation
│   └── aggregate_hourly_data() - Hourly aggregation
├── Visualization
│   ├── plot_hourly_comparison() - Hourly charts
│   ├── render_overview_tab() - Overview display
│   ├── render_hourly_tab() - Hourly trends
│   └── render_comparison_tab() - Comparisons
└── Main Dashboard (render_dashboard, main)
```

### Data Flow
```
Redshift DB → SQL Query → Pandas DataFrame → Filters → 
Aggregation (optional) → Visualization → Streamlit UI
```

## 🎨 Customization

### Adding New Brands
Edit the `BRANDS` list in `aura_dashboard.py`:
```python
BRANDS = [
    "your_new_brand",
    # ... existing brands
]
```

### Adding New Features
Edit the `FEATURES` list:
```python
FEATURES = ['oobe', 'silent', 'gotw', 'your_new_feature']
```

### Adjusting Time Zone
Modify the timezone offset in `plot_hourly_comparison()`:
```python
# For UTC+3 (Israel DST):
hourly_agg['hour_israel'] = (hourly_agg['hour_of_day'] + 3) % 24
```

### Changing Cache Duration
Modify the TTL in cache decorators:
```python
@st.cache_data(ttl=600)  # 10 minutes instead of 5
```

## 🔒 Security Best Practices

1. **Never commit `.env` file** - Contains sensitive credentials
2. **Use environment variables** - All credentials from `.env`
3. **Secure connections** - SSL/TLS enabled for Redshift
4. **Timeout protection** - 2-minute query timeout
5. **Error handling** - Graceful fallback to sample data

## 📊 Supported Brands (62+)

The dashboard supports analysis for:
- Major carriers: NTT Docomo, Softbank, KDDI, Rakuten, T-Mobile, AT&T, Vodafone
- OEMs: Samsung, Motorola, Lenovo, Oppo, HTC, Sony, Huawei, Honor
- Regions: NA, EU, APAC, LATAM, MEA, SEA, CIS

## 🎯 Supported Features

- **OOBE** - Out of Box Experience
- **Silent** - Silent installations
- **GOTW** - Game of the Week
- **Publisher Promotion** - Publisher campaigns
- **Reef** - Reef feature
- **Reengagement Promotion** - User reengagement

## 🐛 Troubleshooting

### Database Connection Issues
```
❌ Failed to establish database connection
```
**Solution:** Check your `.env` file credentials and network access

### Query Timeout
```
❌ Query cancelled on user's request
```
**Solution:** Query takes >2 minutes. Try filtering by fewer brands/features

### No Data Available
```
⚠️ No data matches the selected filters
```
**Solution:** Adjust your brand/feature selection or check date range

### Sample Data Mode
If database connection fails, the dashboard automatically uses sample data for demonstration.

## 📝 Development

### Project Structure
```
windsurf-project-3/
├── aura_dashboard.py      # Main application
├── requirements.txt       # Python dependencies
├── .env                   # Environment variables (not in git)
├── .gitignore            # Git ignore rules
├── README.md             # This file
└── setup_and_launch.sh   # Quick start script
```

### Running Tests
```bash
# Test with sample data (no DB required)
streamlit run aura_dashboard.py
# Uncheck database connection in sidebar
```

### Code Style
- Follow PEP 8 guidelines
- Use type hints where applicable
- Document functions with docstrings
- Keep functions focused and modular

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 👥 Authors

- **Yaniv Rohberg** - Initial work

## 🙏 Acknowledgments

- Streamlit team for the amazing framework
- Plotly for interactive visualizations
- The Aura team for requirements and feedback

## 📞 Support

For issues, questions, or suggestions:
- Open an issue on GitHub
- Contact the development team

---

**Built with ❤️ using Streamlit and Plotly**
