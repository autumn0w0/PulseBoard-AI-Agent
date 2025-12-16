# streamlit run app.py
import streamlit as st
import requests
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Tuple
import io
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from functools import wraps, lru_cache
import numpy as np
import time

load_dotenv()

# ============================================
# Configuration
# ============================================
class Config:
    """Application configuration"""
    API_URL = os.getenv("API_URL", "http://localhost:8000")
    API_VERSION = "v1"
    SESSION_TIMEOUT = 1800  # 30 minutes
    CACHE_TTL = 300  # 5 minutes
    REQUEST_TIMEOUT = 30
    MAX_RETRIES = 3
    
    DOMAIN_OPTIONS = [
        "finance", "healthcare", "ecommerce", "education", "entertainment", 
        "technology", "marketing", "manufacturing", "logistics", "retail", 
        "telecom", "energy", "real estate", "government", "other"
    ]
    
    DOMAIN_BADGE_MAP = {
        'finance': ('💰', '#e3f2fd', '#1976d2'),
        'healthcare': ('🏥', '#f3e5f5', '#7b1fa2'),
        'ecommerce': ('🛒', '#e8f5e9', '#388e3c'),
        'technology': ('💻', '#e1f5fe', '#0277bd'),
        'education': ('📚', '#fff3e0', '#ef6c00'),
        'marketing': ('📢', '#fce4ec', '#c2185b'),
        'manufacturing': ('🏭', '#e0f2f1', '#00695c'),
        'retail': ('🏪', '#f1f8e9', '#558b2f'),
        'other': ('📁', '#f5f5f5', '#616161')
    }
    
    CHART_COLOR_SCHEME = px.colors.qualitative.Set3
    
    FILE_TYPES = {
        'csv': ['csv'],
        'excel': ['xlsx', 'xls'],
        'json': ['json']
    }

# Page configuration
st.set_page_config(
    page_title="PulseBoard.ai",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
    menu_items={
        'About': "PulseBoard.ai - Intelligent Business Analytics"
    }
)

# ============================================
# Utilities
# ============================================
class Logger:
    """Simple logging utility"""
    @staticmethod
    def info(msg: str):
        if os.getenv("DEBUG", "false").lower() == "true":
            st.sidebar.caption(f"ℹ️ {msg}")
    
    @staticmethod
    def error(msg: str):
        st.error(f"❌ {msg}")
    
    @staticmethod
    def success(msg: str):
        st.success(f"✅ {msg}")
    
    @staticmethod
    def warning(msg: str):
        st.warning(f"⚠️ {msg}")


def handle_errors(func):
    """Decorator for error handling"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except requests.exceptions.ConnectionError:
            Logger.error("Cannot connect to server. Please check if the API is running.")
            return None
        except requests.exceptions.Timeout:
            Logger.error("Request timeout. Server is taking too long to respond.")
            return None
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                Logger.error("Authentication failed. Please login again.")
                SessionState.clear_auth()
            elif e.response.status_code == 404:
                Logger.error("Resource not found.")
            else:
                Logger.error(f"Server error: {e.response.status_code}")
            return None
        except Exception as e:
            Logger.error(f"Unexpected error: {str(e)}")
            return None
    return wrapper


def format_timestamp(ts: Optional[str]) -> str:
    """Format ISO timestamp to readable format"""
    if not ts:
        return "N/A"
    try:
        dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
        return dt.strftime("%b %d, %Y • %I:%M %p")
    except:
        return ts


def format_duration(seconds: int) -> str:
    """Format duration in seconds to readable format"""
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        return f"{seconds // 60}m {seconds % 60}s"
    else:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        return f"{hours}h {minutes}m"


def get_file_type(filename: str) -> str:
    """Detect file type from filename"""
    ext = filename.lower().split('.')[-1]
    for ftype, extensions in Config.FILE_TYPES.items():
        if ext in extensions:
            return ftype
    return 'auto'


# ============================================
# Session State Manager
# ============================================
class SessionState:
    """Centralized session state management"""
    
    DEFAULTS = {
        'logged_in': False,
        'user_data': None,
        'current_page': "login",
        'creating_project': False,
        'project_created': False,
        'last_activity': None,
        'projects_cache': None,
        'cache_timestamp': None,
        'dashboard_project_id': None,
        'dashboard_project_name': None,
        'upload_complete': False,
        'processing_started': False,
        'delete_confirmation': None,
        'signup_success': False
    }
    
    @staticmethod
    def init():
        """Initialize session state with defaults"""
        for key, value in SessionState.DEFAULTS.items():
            if key not in st.session_state:
                st.session_state[key] = value
    
    @staticmethod
    def clear_auth():
        """Clear authentication-related session data"""
        keys_to_clear = [
            'logged_in', 'user_data', 'projects_cache', 
            'cache_timestamp', 'dashboard_project_id', 'dashboard_project_name'
        ]
        for key in keys_to_clear:
            st.session_state.pop(key, None)
        SessionState.init()
    
    @staticmethod
    def update_activity():
        """Update last activity timestamp"""
        st.session_state.last_activity = datetime.now()
    
    @staticmethod
    def is_expired() -> bool:
        """Check if session has expired"""
        if not st.session_state.get('last_activity'):
            return False
        elapsed = (datetime.now() - st.session_state.last_activity).seconds
        return elapsed > Config.SESSION_TIMEOUT
    
    @staticmethod
    def clear_project_creation():
        """Clear project creation state"""
        keys = [
            'creating_project', 'project_created', 'created_project_data',
            'upload_complete', 'upload_result', 'processing_started', 'processing_start'
        ]
        for key in keys:
            st.session_state.pop(key, None)
    
    @staticmethod
    def is_cache_valid() -> bool:
        """Check if cached data is still valid"""
        if not st.session_state.get('cache_timestamp'):
            return False
        elapsed = (datetime.now() - st.session_state.cache_timestamp).seconds
        return elapsed < Config.CACHE_TTL


# ============================================
# API Client
# ============================================
class API:
    """Centralized API client with improved error handling"""
    
    BASE_URL = f"{Config.API_URL}/api/{Config.API_VERSION}"
    
    @staticmethod
    @handle_errors
    def _request(method: str, endpoint: str, **kwargs) -> Optional[Dict]:
        """Make API request with error handling and retries"""
        url = f"{API.BASE_URL}{endpoint}"
        
        # Add default timeout
        kwargs.setdefault('timeout', Config.REQUEST_TIMEOUT)
        
        # Retry logic
        for attempt in range(Config.MAX_RETRIES):
            try:
                response = requests.request(method, url, **kwargs)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                if attempt == Config.MAX_RETRIES - 1:
                    raise
                time.sleep(1 * (attempt + 1))  # Exponential backoff
        
        return None
    
    # ========== User Management ==========
    @staticmethod
    def login(email: str, password: str) -> Optional[Dict]:
        """User login"""
        return API._request(
            "POST", 
            "/users/login",
            json={"email": email, "password": password}
        )
    
    @staticmethod
    def register(email: str, first_name: str, last_name: str, password: str) -> Optional[Dict]:
        """User registration"""
        return API._request(
            "POST",
            "/users/register",
            json={
                "email": email,
                "first_name": first_name,
                "last_name": last_name,
                "password": password
            }
        )
    
    @staticmethod
    def get_user_details(user_id: str) -> Optional[Dict]:
        """Get user details"""
        return API._request("GET", f"/users/{user_id}")
    
    # ========== Project Management ==========
    @staticmethod
    def create_project(user_id: str, name: str, domain: str) -> Optional[Dict]:
        """Create new project"""
        return API._request(
            "POST",
            "/projects",
            json={
                "user_id": user_id,
                "project_name": name,
                "domain": domain
            }
        )
    
    @staticmethod
    def get_all_projects(user_id: str) -> Optional[Dict]:
        """Get all user projects"""
        return API._request("GET", f"/projects/user/{user_id}")
    
    @staticmethod
    def get_recent_projects(user_id: str, limit: int = 5) -> Optional[Dict]:
        """Get recent projects"""
        return API._request(
            "GET",
            f"/projects/user/{user_id}/recent",
            params={"limit": limit}
        )
    
    @staticmethod
    def get_project_count(user_id: str) -> Optional[Dict]:
        """Get project count"""
        return API._request("GET", f"/projects/user/{user_id}/count")
    
    @staticmethod
    def update_project_last_used(project_id: str) -> Optional[Dict]:
        """Update project last used timestamp"""
        return API._request("PUT", f"/projects/{project_id}/last-used")
    
    @staticmethod
    def delete_project(project_id: str, user_id: str) -> Optional[Dict]:
        """Delete project"""
        return API._request(
            "DELETE",
            f"/projects/{project_id}",
            params={"user_id": user_id}
        )
    
    # ========== Data Management ==========
    @staticmethod
    def upload_data(project_id: str, user_id: str, file, file_type: str = "auto") -> Optional[Dict]:
        """Upload data file to project"""
        filename = file.name if hasattr(file, 'name') else 'file'
        file_bytes = file.getvalue() if hasattr(file, 'getvalue') else file
        
        files = {
            "file": (filename, io.BytesIO(file_bytes), "application/octet-stream")
        }
        data = {
            "user_id": user_id,
            "file_type": file_type
        }
        
        return API._request(
            "POST",
            f"/projects/{project_id}/upload",
            files=files,
            data=data
        )
    
    @staticmethod
    def get_upload_status(project_id: str, user_id: str) -> Optional[Dict]:
        """Check upload status"""
        return API._request(
            "GET",
            f"/projects/{project_id}/upload-status",
            params={"user_id": user_id}
        )
    
    @staticmethod
    def process_pipeline(project_id: str) -> Optional[Dict]:
        """Run data processing pipeline"""
        return API._request("POST", f"/projects/{project_id}/process")
    
    # ========== Dashboard & Charts ==========
    @staticmethod
    def get_dashboard_layout(project_id: str, user_id: str, rows: int = 2, cols: int = 3) -> Optional[Dict]:
        """Get dashboard layout"""
        return API._request(
            "GET",
            f"/dashboard/{project_id}/layout",
            params={"user_id": user_id, "rows": rows, "cols": cols}
        )
    
    @staticmethod
    def get_all_charts(project_id: str, user_id: str) -> Optional[Dict]:
        """Get all project charts"""
        return API._request(
            "GET",
            f"/dashboard/{project_id}/charts",
            params={"user_id": user_id}
        )
    
    @staticmethod
    def get_chart(project_id: str, chart_id: str, user_id: str) -> Optional[Dict]:
        """Get specific chart"""
        return API._request(
            "GET",
            f"/dashboard/{project_id}/charts/{chart_id}",
            params={"user_id": user_id}
        )
    
    @staticmethod
    def get_direct_charts(project_id: str, user_id: str) -> Optional[Dict]:
        """Get direct display charts"""
        return API._request(
            "GET",
            f"/dashboard/{project_id}/direct-charts",
            params={"user_id": user_id}
        )
    
    # ========== Health Check ==========
    @staticmethod
    def health_check() -> Optional[Dict]:
        """Check API health"""
        try:
            response = requests.get(
                f"{Config.API_URL}/health",
                timeout=5
            )
            return response.json()
        except:
            return None


# ============================================
# Styles
# ============================================
def load_styles():
    """Load custom CSS styles"""
    st.markdown("""
    <style>
        /* Main Layout */
        .main { padding: 0 1rem; }
        .block-container { padding-top: 2rem; }
        
        /* Authentication Container */
        .auth-container {
            max-width: 450px;
            margin: 2rem auto;
            padding: 2.5rem;
            border-radius: 20px;
            background: var(--background-color);
            box-shadow: 0 10px 40px rgba(0, 0, 0, 0.1);
        }
        
        /* Welcome Section */
        .welcome-title {
            text-align: center;
            font-size: 3rem;
            font-weight: 800;
            background: linear-gradient(135deg, #667eea, #764ba2);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            margin-bottom: 0.5rem;
            line-height: 1.2;
        }
        
        .welcome-subtitle {
            text-align: center;
            font-size: 1.2rem;
            opacity: 0.7;
            margin-bottom: 2rem;
        }
        
        /* Section Titles */
        .section-title {
            font-size: 1.8rem;
            font-weight: 700;
            margin: 2rem 0 1.5rem;
            padding-bottom: 0.5rem;
            border-bottom: 3px solid var(--primary-color);
        }
        
        /* Status Badges */
        .status-badge {
            display: inline-block;
            padding: 6px 14px;
            border-radius: 20px;
            font-size: 0.85rem;
            font-weight: 600;
            margin-left: 0.5rem;
        }
        
        /* Project Cards */
        .project-card {
            padding: 1.5rem;
            border-radius: 12px;
            background: var(--background-color);
            box-shadow: 0 4px 12px rgba(0, 0, 0, 0.05);
            margin-bottom: 1rem;
            transition: transform 0.2s, box-shadow 0.2s;
        }
        
        .project-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 6px 20px rgba(0, 0, 0, 0.1);
        }
        
        /* Empty State */
        .empty-state {
            text-align: center;
            padding: 4rem 2rem;
            opacity: 0.6;
        }
        
        .empty-state h3 {
            font-size: 1.5rem;
            margin-bottom: 0.5rem;
        }
        
        /* Chart Container */
        .chart-container {
            padding: 1rem;
            border-radius: 10px;
            background: var(--background-color);
            box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
            margin-bottom: 1rem;
        }
        
        /* Metrics */
        .metric-card {
            padding: 1.5rem;
            border-radius: 12px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            text-align: center;
        }
        
        /* Processing Indicator */
        .processing-box {
            padding: 2rem;
            border-radius: 15px;
            background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
            color: white;
            text-align: center;
            margin: 2rem 0;
        }
        
        /* Custom Buttons */
        .stButton > button {
            border-radius: 8px;
            transition: all 0.2s;
        }
        
        .stButton > button:hover {
            transform: translateY(-1px);
            box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
        }
    </style>
    """, unsafe_allow_html=True)


# ============================================
# Components
# ============================================
class ProjectCard:
    """Render project card component"""
    
    @staticmethod
    def render(project: Dict, user_id: str, index: int):
        """Render a single project card"""
        pid = project['project_id']
        name = project.get('name_of_project', 'Unnamed Project')
        domain = project.get('domain', 'other')
        created = project.get('created_at', '')
        last_used = project.get('last_used_at', '')
        
        # Get badge config
        emoji, bg_color, text_color = Config.DOMAIN_BADGE_MAP.get(
            domain, 
            Config.DOMAIN_BADGE_MAP['other']
        )
        
        with st.container():
            col1, col2, col3 = st.columns([3, 2, 1])
            
            with col1:
                st.markdown(f"### {emoji} {name}")
                st.markdown(
                    f'<span class="status-badge" style="background: {bg_color}; color: {text_color};">'
                    f'{domain.title()}</span>',
                    unsafe_allow_html=True
                )
            
            with col2:
                st.caption(f"📅 Created: {format_timestamp(created)}")
                st.caption(f"⏰ Last Used: {format_timestamp(last_used)}")
            
            with col3:
                btn_col1, btn_col2 = st.columns(2)
                
                with btn_col1:
                    if st.button("📂", key=f"open_{index}", use_container_width=True, help="Open Dashboard"):
                        API.update_project_last_used(pid)
                        st.session_state.update({
                            'current_page': 'dashboard',
                            'dashboard_project_id': pid,
                            'dashboard_project_name': name
                        })
                        st.rerun()
                
                with btn_col2:
                    if st.button("🗑️", key=f"delete_{index}", use_container_width=True, help="Delete Project"):
                        st.session_state.delete_confirmation = {
                            'project_id': pid,
                            'project_name': name
                        }
                        st.rerun()
            
            st.divider()


class ChartRenderer:
    """Enhanced chart rendering with multiple chart types"""
    
    @staticmethod
    def render(chart: Optional[Dict], index: int):
        """Render a chart based on its type"""
        if not chart:
            st.info("📊 No chart data available")
            return
        
        title = chart.get('chart_title', f'Chart {index + 1}')
        description = chart.get('description', '')
        chart_type = chart.get('chart_type', 'table')
        data = chart.get('data', [])
        config = chart.get('config', {})
        
        with st.container():
            st.markdown(f"#### 📊 {title}")
            if description:
                st.caption(description)
            
            try:
                df = pd.DataFrame(data)
                
                if df.empty:
                    st.warning("No data available for this chart")
                    return
                
                # Route to appropriate renderer
                if chart_type == 'bar_chart':
                    ChartRenderer._render_bar(df, title, config)
                elif chart_type == 'line_chart':
                    ChartRenderer._render_line(df, title, config)
                elif chart_type == 'pie_chart':
                    ChartRenderer._render_pie(df, title, config)
                elif chart_type == 'histogram':
                    ChartRenderer._render_histogram(df, title, config)
                elif chart_type == 'geo_map':
                    ChartRenderer._render_geo(df, title, config)
                elif chart_type == 'scatter_plot':
                    ChartRenderer._render_scatter(df, title, config)
                elif chart_type == 'heatmap':
                    ChartRenderer._render_heatmap(df, title, config)
                else:
                    ChartRenderer._render_table(df, title)
                    
            except Exception as e:
                st.error(f"Error rendering chart: {str(e)}")
                # Fallback to table
                if data:
                    st.dataframe(pd.DataFrame(data).head(10), use_container_width=True)
    
    @staticmethod
    def _get_columns(df: pd.DataFrame) -> Tuple[Optional[str], str]:
        """Intelligently determine x and y columns"""
        y_col = 'value'
        x_col = None
        
        # Find x column (non-value, non-id columns)
        for col in df.columns:
            if col not in ['value', 'count', '_id', 'chart_id']:
                x_col = col
                break
        
        if not x_col and len(df.columns) >= 2:
            x_col = df.columns[0]
            y_col = df.columns[1] if df.columns[1] != x_col else 'value'
        
        return x_col, y_col
    
    @staticmethod
    def _render_bar(df: pd.DataFrame, title: str, config: Dict):
        """Render bar chart"""
        x_col, y_col = ChartRenderer._get_columns(df)
        
        if x_col and y_col in df.columns:
            fig = px.bar(
                df, 
                x=x_col, 
                y=y_col,
                title=title,
                color_discrete_sequence=Config.CHART_COLOR_SCHEME,
                height=400
            )
            fig.update_layout(
                xaxis_title=x_col.replace('_', ' ').title(),
                yaxis_title=y_col.replace('_', ' ').title(),
                showlegend=False
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            ChartRenderer._render_table(df, title)
    
    @staticmethod
    def _render_line(df: pd.DataFrame, title: str, config: Dict):
        """Render line chart"""
        x_col, y_col = ChartRenderer._get_columns(df)
        
        if x_col and y_col in df.columns:
            df_sorted = df.sort_values(x_col)
            fig = px.line(
                df_sorted,
                x=x_col,
                y=y_col,
                title=title,
                markers=True,
                height=400
            )
            fig.update_layout(
                xaxis_title=x_col.replace('_', ' ').title(),
                yaxis_title=y_col.replace('_', ' ').title()
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            ChartRenderer._render_table(df, title)
    
    @staticmethod
    def _render_pie(df: pd.DataFrame, title: str, config: Dict):
        """Render pie chart"""
        x_col, y_col = ChartRenderer._get_columns(df)
        name_col = config.get('category') or x_col
        
        if name_col and y_col in df.columns:
            # Filter and limit
            df_filtered = df[df[name_col].notna()].nlargest(10, y_col)
            
            fig = px.pie(
                df_filtered,
                names=name_col,
                values=y_col,
                title=title,
                color_discrete_sequence=Config.CHART_COLOR_SCHEME,
                height=400
            )
            fig.update_traces(
                textposition='inside',
                textinfo='percent+label',
                hovertemplate='<b>%{label}</b><br>Value: %{value}<br>Percentage: %{percent}'
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            ChartRenderer._render_table(df, title)
    
    @staticmethod
    def _render_histogram(df: pd.DataFrame, title: str, config: Dict):
        """Render histogram"""
        if 'range' in df.columns:
            _, y_col = ChartRenderer._get_columns(df)
            fig = px.bar(
                df,
                x='range',
                y=y_col,
                title=title,
                color_discrete_sequence=Config.CHART_COLOR_SCHEME,
                height=400
            )
            fig.update_layout(
                xaxis_title='Range',
                yaxis_title='Count',
                xaxis={'categoryorder': 'array', 'categoryarray': df['range'].tolist()}
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            ChartRenderer._render_table(df, title)
    
    @staticmethod
    def _render_geo(df: pd.DataFrame, title: str, config: Dict):
        """Render geographic data"""
        if 'country' in df.columns:
            _, y_col = ChartRenderer._get_columns(df)
            df_geo = df[df['country'].notna()].nlargest(15, y_col)
            
            fig = px.bar(
                df_geo,
                x='country',
                y=y_col,
                title=title,
                color_discrete_sequence=Config.CHART_COLOR_SCHEME,
                height=400
            )
            fig.update_layout(
                xaxis_title='Country',
                yaxis_title='Count',
                xaxis_tickangle=-45
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            ChartRenderer._render_table(df, title)
    
    @staticmethod
    def _render_scatter(df: pd.DataFrame, title: str, config: Dict):
        """Render scatter plot"""
        x_col, y_col = ChartRenderer._get_columns(df)
        
        if x_col and y_col in df.columns:
            fig = px.scatter(
                df,
                x=x_col,
                y=y_col,
                title=title,
                color_discrete_sequence=Config.CHART_COLOR_SCHEME,
                height=400
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            ChartRenderer._render_table(df, title)
    
    @staticmethod
    def _render_heatmap(df: pd.DataFrame, title: str, config: Dict):
        """Render heatmap (correlation matrix)"""
        numeric_cols = df.select_dtypes(include=[np.number])
        
        if len(numeric_cols.columns) > 1:
            corr = numeric_cols.corr()
            fig = px.imshow(
                corr,
                title=title,
                color_continuous_scale='RdBu_r',
                aspect='auto',
                height=400
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            ChartRenderer._render_table(df, title)
    
    @staticmethod
    def _render_table(df: pd.DataFrame, title: str):
        """Render data as table (fallback)"""
        display_df = df.head(20)
        st.dataframe(display_df, use_container_width=True, height=400)
        
        if len(df) > 20:
            st.caption(f"Showing 20 of {len(df)} rows")


# ============================================
# Pages
# ============================================
class LoginPage:
    """Login page component"""
    
    @staticmethod
    def render():
        st.markdown('<div class="welcome-title">🚀 PulseBoard.ai</div>', unsafe_allow_html=True)
        st.markdown('<div class="welcome-subtitle">Transform data into intelligent insights</div>', unsafe_allow_html=True)
        
        # Show signup success message
        if st.session_state.get('signup_success'):
            st.success("✅ Account created successfully! Please login with your credentials.")
            st.session_state.signup_success = False
        
        _, center_col, _ = st.columns([1, 2, 1])
        
        with center_col:
            st.markdown('<div class="auth-container">', unsafe_allow_html=True)
            
            with st.form("login_form", clear_on_submit=True):
                st.subheader("🔐 Login")
                
                email = st.text_input("Email", placeholder="your.email@company.com")
                password = st.text_input("Password", type="password", placeholder="Enter your password")
                
                col1, col2 = st.columns([1, 1])
                with col1:
                    submit = st.form_submit_button("🚀 Login", type="primary", use_container_width=True)
                with col2:
                    if st.form_submit_button("📝 Sign Up", use_container_width=True):
                        st.session_state.current_page = "signup"
                        st.rerun()
                
                if submit:
                    if not email or not password:
                        st.error("Please fill in all fields")
                    else:
                        with st.spinner("Logging in..."):
                            result = API.login(email, password)
                            
                            if result and result.get("status") == "success":
                                SessionState.clear_auth()
                                st.session_state.update({
                                    'logged_in': True,
                                    'user_data': result.get("data", {}).get("user") or result.get("user"),
                                    'current_page': 'home'
                                })
                                SessionState.update_activity()
                                Logger.success("Login successful!")
                                time.sleep(0.5)
                                st.rerun()
                            else:
                                Logger.error("Invalid email or password")
            
            st.markdown('</div>', unsafe_allow_html=True)
            
            # API health check indicator
            with st.expander("🔧 System Status"):
                health = API.health_check()
                if health and health.get("status") == "healthy":
                    st.success(f"✅ API Online (v{health.get('version', 'N/A')})")
                else:
                    st.error("❌ API Offline")


class SignupPage:
    """User registration page"""
    
    @staticmethod
    def render():
        st.markdown('<div class="welcome-title">🎯 Join PulseBoard.ai</div>', unsafe_allow_html=True)
        st.markdown('<div class="welcome-subtitle">Start your data journey today</div>', unsafe_allow_html=True)
        
        _, center_col, _ = st.columns([1, 2, 1])
        
        with center_col:
            st.markdown('<div class="auth-container">', unsafe_allow_html=True)
            
            with st.form("signup_form", clear_on_submit=True):
                st.subheader("📝 Create Account")
                
                col1, col2 = st.columns(2)
                with col1:
                    first_name = st.text_input("First Name *", placeholder="John")
                with col2:
                    last_name = st.text_input("Last Name *", placeholder="Doe")
                
                email = st.text_input("Email *", placeholder="john.doe@company.com")
                
                col1, col2 = st.columns(2)
                with col1:
                    password = st.text_input("Password *", type="password", placeholder="Min 8 characters")
                with col2:
                    confirm_password = st.text_input("Confirm *", type="password", placeholder="Confirm password")
                
                st.caption("* Password must be at least 8 characters with uppercase, lowercase, and digit")
                
                col1, col2 = st.columns([1, 1])
                with col1:
                    submit = st.form_submit_button("✨ Create Account", type="primary", use_container_width=True)
                with col2:
                    if st.form_submit_button("← Back to Login", use_container_width=True):
                        st.session_state.current_page = "login"
                        st.rerun()
                
                if submit:
                    # Validation
                    if not all([first_name, last_name, email, password, confirm_password]):
                        st.error("Please fill in all required fields")
                    elif password != confirm_password:
                        st.error("Passwords do not match")
                    elif len(password) < 8:
                        st.error("Password must be at least 8 characters long")
                    else:
                        with st.spinner("Creating account..."):
                            result = API.register(email, first_name, last_name, password)
                            
                            if result and result.get("status") == "success":
                                st.session_state.update({
                                    'signup_success': True,
                                    'current_page': 'login'
                                })
                                time.sleep(0.5)
                                st.rerun()
                            else:
                                error_msg = "Registration failed. Email may already be in use."
                                if result and "message" in result:
                                    error_msg = result["message"]
                                Logger.error(error_msg)
            
            st.markdown('</div>', unsafe_allow_html=True)


class HomePage:
    """Main dashboard home page"""
    
    @staticmethod
    def render():
        user_data = st.session_state.user_data or {}
        user_id = user_data.get("user_id")
        user_name = user_data.get("name", "User")
        
        # Sidebar
        with st.sidebar:
            st.markdown(f"### 👋 Welcome, {user_name.split()[0]}!")
            st.divider()
            
            # User details
            with st.expander("👤 Profile", expanded=False):
                details = API.get_user_details(user_id)
                if details and details.get("status") == "success":
                    user_info = details.get("data", {}).get("user") or details.get("user")
                    st.write(f"**Email:** {user_info.get('email')}")
                    st.write(f"**User ID:** {user_id}")
            
            st.divider()
            
            if st.button("🚪 Logout", type="primary", use_container_width=True):
                SessionState.clear_auth()
                st.session_state.current_page = "login"
                st.rerun()
        
        # Main content
        st.markdown(f"# 🎯 Welcome back, {user_name}!")
        
        # Get projects
        projects_data = HomePage._get_projects(user_id)
        projects = projects_data.get("projects", []) if projects_data else []
        
        # Header metrics
        col1, col2, col3 = st.columns([2, 1, 1])
        
        with col1:
            st.markdown("### Your Projects")
        
        with col2:
            st.metric("Total Projects", len(projects), delta=None)
        
        with col3:
            if st.button("➕ New Project", type="primary", use_container_width=True):
                st.session_state.creating_project = True
                st.rerun()
        
        st.divider()
        
        # Delete confirmation dialog
        if delete_conf := st.session_state.get('delete_confirmation'):
            st.warning(f"⚠️ Are you sure you want to delete **{delete_conf['project_name']}**?")
            st.caption("This action cannot be undone. All data and charts will be permanently deleted.")
            
            col1, col2, col3 = st.columns([1, 1, 2])
            
            with col1:
                if st.button("✅ Confirm Delete", type="primary", use_container_width=True):
                    with st.spinner("Deleting project..."):
                        result = API.delete_project(delete_conf['project_id'], user_id)
                        
                        if result and result.get("status") == "success":
                            Logger.success(f"Project '{delete_conf['project_name']}' deleted successfully")
                            st.session_state.pop('delete_confirmation', None)
                            st.session_state.pop('projects_cache', None)  # Clear cache
                            time.sleep(1)
                            st.rerun()
                        else:
                            Logger.error("Failed to delete project")
            
            with col2:
                if st.button("❌ Cancel", use_container_width=True):
                    st.session_state.pop('delete_confirmation', None)
                    st.rerun()
            
            return
        
        # Project list
        if projects:
            st.markdown('<div class="section-title">📁 Your Projects</div>', unsafe_allow_html=True)
            
            for idx, project in enumerate(projects):
                ProjectCard.render(project, user_id, idx)
        else:
            st.markdown('''
                <div class="empty-state">
                    <h3>📂 No Projects Yet</h3>
                    <p>Create your first project to get started with data analytics</p>
                </div>
            ''', unsafe_allow_html=True)
    
    @staticmethod
    def _get_projects(user_id: str) -> Optional[Dict]:
        """Get projects with caching"""
        # Check cache
        if SessionState.is_cache_valid() and st.session_state.get('projects_cache'):
            return st.session_state.projects_cache
        
        # Fetch from API
        result = API.get_all_projects(user_id)
        if result:
            st.session_state.projects_cache = result
            st.session_state.cache_timestamp = datetime.now()
        
        return result


class CreateProjectPage:
    """Project creation wizard"""
    
    @staticmethod
    def render():
        user_id = st.session_state.user_data.get("user_id")
        
        # Sidebar navigation
        with st.sidebar:
            st.markdown("### 📊 New Project")
            st.divider()
            
            # Progress indicator
            current_step = CreateProjectPage._get_current_step()
            steps = ["Project Info", "Upload Data", "Generate Dashboard"]
            
            for idx, step in enumerate(steps, 1):
                if idx < current_step:
                    st.success(f"✅ {step}")
                elif idx == current_step:
                    st.info(f"▶️ {step}")
                else:
                    st.caption(f"⭕ {step}")
            
            st.divider()
            
            if st.button("🏠 Back to Home", use_container_width=True):
                SessionState.clear_project_creation()
                st.session_state.creating_project = False
                st.rerun()
        
        # Main content - route to appropriate step
        if st.session_state.get('processing_started'):
            CreateProjectPage._render_processing()
        elif st.session_state.get('project_created'):
            if st.session_state.get('upload_complete'):
                CreateProjectPage._render_generate()
            else:
                CreateProjectPage._render_upload(user_id)
        else:
            CreateProjectPage._render_form(user_id)
    
    @staticmethod
    def _get_current_step() -> int:
        """Determine current wizard step"""
        if st.session_state.get('processing_started'):
            return 3
        elif st.session_state.get('upload_complete'):
            return 3
        elif st.session_state.get('project_created'):
            return 2
        return 1
    
    @staticmethod
    def _render_form(user_id: str):
        """Step 1: Project information form"""
        st.markdown("# 📊 Create New Project")
        st.markdown("### Step 1: Project Information")
        
        with st.form("create_project_form"):
            project_name = st.text_input(
                "Project Name *",
                placeholder="e.g., Q4 Sales Analysis",
                help="Choose a descriptive name for your project"
            )
            
            domain = st.selectbox(
                "Domain *",
                options=Config.DOMAIN_OPTIONS,
                help="Select the business domain for your project"
            )
            
            st.caption("* Required fields")
            
            col1, col2 = st.columns([1, 3])
            with col1:
                submit = st.form_submit_button("➡️ Next: Upload Data", type="primary", use_container_width=True)
            
            if submit:
                if not project_name.strip():
                    st.error("Please enter a project name")
                elif not domain:
                    st.error("Please select a domain")
                else:
                    with st.spinner("Creating project..."):
                        result = API.create_project(user_id, project_name.strip(), domain)
                        
                        if result and result.get("status") == "success":
                            project_data = result.get("data", {}).get("project") or result.get("project")
                            st.session_state.update({
                                'project_created': True,
                                'created_project_data': {
                                    'project_id': project_data.get('project_id'),
                                    'project_name': project_data.get('name_of_project')
                                }
                            })
                            Logger.success(f"Project '{project_name}' created successfully!")
                            time.sleep(0.5)
                            st.rerun()
                        else:
                            Logger.error("Failed to create project")
    
    @staticmethod
    def _render_upload(user_id: str):
        """Step 2: Data upload"""
        project_data = st.session_state.created_project_data
        project_id = project_data['project_id']
        project_name = project_data['project_name']
        
        st.markdown(f"# 📊 {project_name}")
        st.success(f"✅ Project created successfully!")
        
        st.markdown("### Step 2: Upload Your Data")
        st.info("📁 Supported formats: CSV, Excel (.xlsx, .xls), JSON")
        
        uploaded_file = st.file_uploader(
            "Choose a file",
            type=list(sum(Config.FILE_TYPES.values(), [])),
            help="Upload your dataset to analyze"
        )
        
        if uploaded_file:
            # Show file info
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Filename", uploaded_file.name)
            with col2:
                file_size = uploaded_file.size / 1024 / 1024  # MB
                st.metric("Size", f"{file_size:.2f} MB")
            with col3:
                file_type = get_file_type(uploaded_file.name)
                st.metric("Type", file_type.upper())
            
            # Upload button
            col1, col2 = st.columns([1, 3])
            with col1:
                if st.button("📤 Upload & Continue", type="primary", use_container_width=True):
                    with st.spinner("Uploading data..."):
                        result = API.upload_data(project_id, user_id, uploaded_file, file_type)
                        
                        if result and result.get("status") == "success":
                            st.session_state.update({
                                'upload_complete': True,
                                'upload_result': result
                            })
                            Logger.success(f"Uploaded {result.get('records_inserted', 0)} records")
                            time.sleep(0.5)
                            st.rerun()
                        else:
                            Logger.error("Failed to upload data")
        else:
            st.warning("Please select a file to upload")
    
    @staticmethod
    def _render_generate():
        """Step 3: Generate dashboard"""
        project_data = st.session_state.created_project_data
        project_id = project_data['project_id']
        project_name = project_data['project_name']
        upload_result = st.session_state.get('upload_result', {})
        
        st.markdown(f"# 📊 {project_name}")
        st.success("✅ Data uploaded successfully!")
        
        st.markdown("### Step 3: Generate Your Dashboard")
        
        # Show upload summary
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Records", upload_result.get('records_inserted', 0))
        with col2:
            st.metric("Columns", len(upload_result.get('columns', [])))
        with col3:
            st.metric("Collection", upload_result.get('collection_name', 'N/A'))
        
        # Data preview
        if sample_data := upload_result.get('sample_data'):
            with st.expander("👀 Data Preview", expanded=True):
                st.dataframe(pd.DataFrame(sample_data), use_container_width=True)
        
        st.divider()
        
        # Generate button
        st.markdown("#### 🚀 Ready to Generate")
        st.info("⏱️ Dashboard generation typically takes 5-10 minutes. You'll be notified when complete.")
        
        col1, col2, col3 = st.columns([1, 1, 2])
        
        with col1:
            if st.button("🚀 Generate Dashboard", type="primary", use_container_width=True):
                with st.spinner("Starting pipeline..."):
                    result = API.process_pipeline(project_id)
                    
                    if result and result.get("status") == "success":
                        st.session_state.update({
                            'processing_started': True,
                            'processing_start': datetime.now()
                        })
                        Logger.success("Dashboard generation started!")
                        time.sleep(0.5)
                        st.rerun()
                    else:
                        Logger.error("Failed to start processing pipeline")
        
        with col2:
            if st.button("🏠 Return Home", use_container_width=True):
                SessionState.clear_project_creation()
                st.session_state.creating_project = False
                st.rerun()
    
    @staticmethod
    def _render_processing():
        """Processing status page"""
        project_data = st.session_state.created_project_data
        project_name = project_data['project_name']
        start_time = st.session_state.get('processing_start', datetime.now())
        
        st.markdown(f"# ⚙️ Processing: {project_name}")
        
        st.markdown('''
            <div class="processing-box">
                <h2>🔄 Dashboard Generation in Progress</h2>
                <p>Your dashboard is being created. This typically takes 5-10 minutes.</p>
            </div>
        ''', unsafe_allow_html=True)
        
        # Show elapsed time
        elapsed = (datetime.now() - start_time).seconds
        st.metric("Elapsed Time", format_duration(elapsed))
        
        # Processing steps
        st.markdown("### Processing Steps:")
        steps = [
            "🔍 Analyzing data types",
            "🔎 Detecting anomalies",
            "📊 Suggesting visualizations",
            "🎨 Creating charts",
            "🗄️ Preparing data for search",
            "🔢 Vectorizing content",
            "💾 Storing in database"
        ]
        
        for step in steps:
            st.caption(step)
        
        st.divider()
        
        # Return home button
        col1, col2, col3 = st.columns([1, 1, 2])
        with col1:
            if st.button("🏠 Return Home", use_container_width=True):
                SessionState.clear_project_creation()
                st.session_state.creating_project = False
                st.rerun()
        
        st.info("💡 You can safely leave this page. Check back in a few minutes!")


class DashboardPage:
    """Project dashboard page with charts"""
    
    @staticmethod
    def render(project_id: str, project_name: str):
        user_id = st.session_state.user_data.get("user_id")
        
        # Sidebar
        with st.sidebar:
            st.markdown(f"## 📊 {project_name}")
            st.divider()
            
            st.caption(f"**Project ID:** {project_id}")
            
            st.divider()
            
            if st.button("🏠 Back to Home", type="primary", use_container_width=True):
                st.session_state.current_page = "home"
                st.session_state.pop('dashboard_project_id', None)
                st.session_state.pop('dashboard_project_name', None)
                st.rerun()
        
        # Main content
        st.markdown(f"# 📈 {project_name}")
        
        # Load dashboard data
        with st.spinner("Loading dashboard..."):
            dashboard_data = API.get_dashboard_layout(project_id, user_id, rows=2, cols=3)
        
        if not dashboard_data or dashboard_data.get("status") != "success":
            st.warning("⚠️ No dashboard data available yet. The dashboard may still be processing.")
            st.info("Please check back in a few minutes or return to home to create a new project.")
            return
        
        charts = dashboard_data.get("data", {}).get("charts") or dashboard_data.get("charts", [])
        
        if not charts:
            st.warning("📊 No charts available yet")
            st.info("The dashboard generation may still be in progress. Please check back shortly.")
            return
        
        # Dashboard header
        col1, col2 = st.columns([3, 1])
        with col1:
            st.markdown(f"### 📊 Dashboard Overview")
        with col2:
            st.metric("Charts", len(charts))
        
        st.divider()
        
        # Render charts in grid
        grid_layout = dashboard_data.get("data", {}).get("grid_layout") or dashboard_data.get("grid_layout", [])
        
        if grid_layout:
            # Use provided grid layout
            for row_idx, row in enumerate(grid_layout):
                cols = st.columns(3)
                for col_idx, chart in enumerate(row):
                    with cols[col_idx]:
                        if chart:
                            ChartRenderer.render(chart, row_idx * 3 + col_idx)
        else:
            # Fallback: create 2x3 grid
            for row in range(2):
                cols = st.columns(3)
                for col_idx in range(3):
                    chart_idx = row * 3 + col_idx
                    with cols[col_idx]:
                        if chart_idx < len(charts):
                            ChartRenderer.render(charts[chart_idx], chart_idx)


# ============================================
# Main Application
# ============================================
def main():
    """Main application entry point"""
    
    # Initialize session state
    SessionState.init()
    
    # Load styles
    load_styles()
    
    # Check session expiration
    if SessionState.is_expired():
        SessionState.clear_auth()
        st.warning("⏱️ Your session has expired. Please login again.")
        st.session_state.current_page = "login"
    
    # Update activity
    if st.session_state.logged_in:
        SessionState.update_activity()
    
    # Route to appropriate page
    if st.session_state.logged_in:
        # Authenticated routes
        current_page = st.session_state.get('current_page', 'home')
        
        if current_page == 'dashboard' and st.session_state.get('dashboard_project_id'):
            DashboardPage.render(
                st.session_state.dashboard_project_id,
                st.session_state.dashboard_project_name
            )
        elif st.session_state.creating_project:
            CreateProjectPage.render()
        else:
            HomePage.render()
    else:
        # Public routes
        if st.session_state.current_page == "signup":
            SignupPage.render()
        else:
            LoginPage.render()


# ============================================
# Application Entry Point
# ============================================
if __name__ == "__main__":
    main()