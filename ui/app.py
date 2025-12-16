# streamlit run app.py
import streamlit as st
import requests
import os
from dotenv import load_dotenv
from datetime import datetime
from typing import Dict, Any, Optional, List
import io
import pandas as pd
import plotly.express as px
from functools import wraps
import numpy as np

load_dotenv()

# ============================================
# Configuration
# ============================================
class Config:
    API_URL = os.getenv("API_URL", "http://localhost:8000")
    SESSION_TIMEOUT = 1800
    CACHE_TTL = 60
    REQUEST_TIMEOUT = 10
    DOMAIN_OPTIONS = ["finance", "healthcare", "ecommerce", "education", "entertainment", 
                     "technology", "marketing", "manufacturing", "logistics", "retail", 
                     "telecom", "energy", "other"]
    DOMAIN_BADGE_MAP = {
        'finance': 'status-badge-finance', 'healthcare': 'status-badge-healthcare',
        'ecommerce': 'status-badge-ecommerce', 'technology': 'status-badge-technology',
    }

st.set_page_config(page_title="PulseBoard.ai", page_icon="📊", layout="wide", 
                   initial_sidebar_state="collapsed")

# ============================================
# Utilities
# ============================================
def handle_errors(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ConnectionError:
            st.error("🔌 Cannot connect to server")
        except Exception as e:
            st.error(f"❌ Error: {str(e)}")
        return None
    return wrapper

def format_timestamp(ts: str) -> str:
    try:
        dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
        return dt.strftime("%b %d, %Y • %I:%M %p")
    except:
        return ts

# ============================================
# Session State
# ============================================
class SessionState:
    DEFAULTS = {'logged_in': False, 'user_data': None, 'current_page': "login", 
                'creating_project': False, 'project_created': False, 'last_activity': None}
    
    @staticmethod
    def init():
        for k, v in SessionState.DEFAULTS.items():
            st.session_state.setdefault(k, v)
    
    @staticmethod
    def clear_auth():
        for key in ['logged_in', 'user_data', 'projects_data', 'all_projects']:
            st.session_state.pop(key, None)
        SessionState.init()
    
    @staticmethod
    def update_activity():
        st.session_state.last_activity = datetime.now()
    
    @staticmethod
    def is_expired():
        if not st.session_state.get('last_activity'):
            return False
        return (datetime.now() - st.session_state.last_activity).seconds > Config.SESSION_TIMEOUT

# ============================================
# API Client
# ============================================
class API:
    @staticmethod
    @handle_errors
    def _req(method, endpoint, **kwargs):
        url = f"{Config.API_URL}{endpoint}"
        r = requests.request(method, url, timeout=Config.REQUEST_TIMEOUT, **kwargs)
        r.raise_for_status()
        return r.json()
    
    @staticmethod
    def login(email, password):
        return API._req("POST", "/user-login", json={"email": email, "password": password})
    
    @staticmethod
    def create_user(email, first, last, password):
        return API._req("POST", "/create-user", 
                       json={"email": email, "first_name": first, "last_name": last, "password": password})
    
    @staticmethod
    def get_user_details(uid):
        return API._req("GET", f"/user-details/{uid}")
    
    @staticmethod
    def get_all_projects(uid):
        return API._req("GET", f"/all-projects/{uid}")
    
    @staticmethod
    def update_project_last_used(pid):
        return API._req("PUT", "/update-project-last-used", json={"project_id": pid})
    
    @staticmethod
    def delete_project(uid, pid):
        return API._req("DELETE", "/delete-project", json={"user_id": uid, "project_id": pid})
    
    @staticmethod
    def create_project(uid, name, domain):
        return API._req("POST", "/create-project", 
                       json={"user_id": uid, "project_name": name, "domain": domain})
    
    @staticmethod
    def upload_data(pid, uid, file, ftype):
        fname = file.name if hasattr(file, 'name') else 'file'
        fbytes = file.getvalue() if hasattr(file, 'getvalue') else file
        files = {"file": (fname, io.BytesIO(fbytes), "application/octet-stream")}
        return API._req("POST", f"/upload-data/{pid}", files=files, data={"user_id": uid, "file_type": ftype})
    
    @staticmethod
    def process_pipeline(pid):
        return API._req("POST", f"/data-process-pipeline/{pid}")
    
    @staticmethod
    def check_pipeline_status(pid):
        return API._req("GET", f"/pipeline-status/{pid}")
    
    @staticmethod
    def get_dashboard_layout(pid, uid):
        return API._req("GET", f"/dashboard/{pid}/dashboard-layout", params={"user_id": uid})

# ============================================
# Styles
# ============================================
def load_styles():
    st.markdown("""<style>
        .main{padding:0 1rem}.auth-container{max-width:400px;margin:0 auto;padding:30px;border-radius:15px;
        background:var(--background-color);box-shadow:0 8px 32px rgba(0,0,0,.1)}
        .welcome-title{text-align:center;font-size:2.5rem;font-weight:800;
        background:linear-gradient(135deg,#667eea,#764ba2);-webkit-background-clip:text;
        -webkit-text-fill-color:transparent;margin-bottom:.5rem}
        .welcome-subtitle{text-align:center;font-size:1.1rem;opacity:.8;margin-bottom:2rem}
        .section-title{font-size:1.5rem;font-weight:700;margin:30px 0 20px;padding-bottom:10px;
        border-bottom:2px solid var(--primary-color)}
        .status-badge{display:inline-block;padding:4px 12px;border-radius:20px;font-size:.8rem}
        .status-badge-finance{background:#e3f2fd;color:#1976d2}
        .status-badge-healthcare{background:#f3e5f5;color:#7b1fa2}
        .status-badge-ecommerce{background:#e8f5e9;color:#388e3c}
        .status-badge-other{background:#f5f5f5;color:#616161}
        .empty-state{text-align:center;padding:60px 20px;opacity:.6}
    </style>""", unsafe_allow_html=True)

# ============================================
# Components
# ============================================
def render_project_card(proj, uid, suffix, idx):
    pid, name, domain = proj['project_id'], proj.get('name_of_project', 'Unnamed'), proj.get('domain', 'general')
    
    c1, c2, c3 = st.columns([3, 2, 1])
    with c1:
        st.markdown(f"**{name}**")
        badge = Config.DOMAIN_BADGE_MAP.get(domain, 'status-badge-other')
        st.markdown(f'<span class="status-badge {badge}">{domain.title()}</span>', unsafe_allow_html=True)
    with c2:
        st.caption(f"📅 {format_timestamp(proj.get('created_at', ''))}")
        st.caption(f"⏰ {format_timestamp(proj.get('last_used_at', ''))}")
    with c3:
        b1, b2 = st.columns(2)
        with b1:
            if st.button("📂", key=f"o_{suffix}_{idx}", use_container_width=True):
                API.update_project_last_used(pid)
                st.session_state.update({'current_page': 'dashboard', 'dashboard_project_id': pid, 
                                        'dashboard_project_name': name})
                st.rerun()
        with b2:
            if st.button("🗑️", key=f"d_{suffix}_{idx}", use_container_width=True):
                st.session_state.delete_confirmation = {'project_id': pid, 'project_name': name}
                st.rerun()
    st.divider()

# ============================================
# Chart Renderer
# ============================================
class ChartRenderer:
    @staticmethod
    def render(chart, idx):
        if not chart:
            return
        
        title = chart.get('chart_title', 'Chart')
        desc = chart.get('description', '')
        ctype = chart.get('chart_type', '')
        data = chart.get('data', [])
        config = chart.get('config', {})
        
        with st.expander(f"📊 {title}", expanded=True):
            if desc:
                st.caption(desc)
            
            try:
                df = pd.DataFrame(data)
                if df.empty:
                    st.warning("No data available")
                    return
                
                # Determine x and y columns intelligently
                x_col = None
                y_col = 'value'  # Default y is 'value' from your data structure
                
                # Find the x-axis column
                for col in df.columns:
                    if col not in ['value', 'count', '_id', 'chart_id']:
                        x_col = col
                        break
                
                if not x_col and len(df.columns) >= 2:
                    x_col = df.columns[0]
                    y_col = df.columns[1] if df.columns[1] != x_col else 'value'
                
                # Render based on chart type
                if ctype == 'bar_chart' and x_col:
                    fig = px.bar(df, x=x_col, y=y_col, title=title, height=400)
                    fig.update_layout(xaxis_title=x_col.replace('_', ' ').title(), 
                                     yaxis_title='Count')
                    st.plotly_chart(fig, use_container_width=True)
                    
                elif ctype == 'line_chart' and x_col:
                    # Sort by x_col for proper line chart
                    df_sorted = df.sort_values(x_col)
                    fig = px.line(df_sorted, x=x_col, y=y_col, title=title, height=400,
                                 markers=True)
                    fig.update_layout(xaxis_title=x_col.replace('_', ' ').title(),
                                     yaxis_title='Count')
                    st.plotly_chart(fig, use_container_width=True)
                    
                elif ctype == 'pie_chart':
                    # For pie charts, find name and value columns
                    name_col = config.get('category') or x_col
                    val_col = y_col
                    if name_col and val_col in df.columns:
                        # Filter out null values and limit to top 10
                        df_filtered = df[df[name_col].notna()].nlargest(10, val_col)
                        fig = px.pie(df_filtered, names=name_col, values=val_col, 
                                    title=title, height=400)
                        fig.update_traces(textposition='inside', textinfo='percent+label')
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.dataframe(df, use_container_width=True)
                        
                elif ctype == 'histogram':
                    # Histogram with range labels
                    if 'range' in df.columns and y_col in df.columns:
                        fig = px.bar(df, x='range', y=y_col, title=title, height=400)
                        fig.update_layout(xaxis_title='Duration Range',
                                         yaxis_title='Count',
                                         xaxis={'categoryorder': 'array', 
                                               'categoryarray': df['range'].tolist()})
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.dataframe(df, use_container_width=True)
                        
                elif ctype == 'geo_map':
                    # Geographic map - show as bar chart for now
                    if 'country' in df.columns:
                        # Filter out nulls and get top 15
                        df_geo = df[df['country'].notna()].nlargest(15, y_col)
                        fig = px.bar(df_geo, x='country', y=y_col, 
                                    title=title, height=400)
                        fig.update_layout(xaxis_title='Country',
                                         yaxis_title='Count',
                                         xaxis_tickangle=-45)
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.dataframe(df.head(20), use_container_width=True)
                        
                elif ctype == 'scatter_plot' and x_col:
                    fig = px.scatter(df, x=x_col, y=y_col, title=title, height=400)
                    st.plotly_chart(fig, use_container_width=True)
                    
                elif ctype == 'heatmap':
                    numeric = df.select_dtypes(include=[np.number])
                    if len(numeric.columns) > 1:
                        corr = numeric.corr()
                        fig = px.imshow(corr, title=title, height=400,
                                       labels=dict(color="Correlation"))
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.dataframe(df, use_container_width=True)
                        
                else:
                    # Default: show as table
                    st.dataframe(df.head(20), use_container_width=True)
                    if len(df) > 20:
                        st.caption(f"Showing 20 of {len(df)} rows")
                        
            except Exception as e:
                st.error(f"Error rendering chart: {str(e)}")
                st.dataframe(pd.DataFrame(data).head(10), use_container_width=True)

# ============================================
# Pages
# ============================================
class LoginPage:
    @staticmethod
    def render():
        st.markdown('<div class="welcome-title">Welcome to PulseBoard.ai</div>', unsafe_allow_html=True)
        st.markdown('<div class="welcome-subtitle">Intelligent insights for your business</div>', unsafe_allow_html=True)
        
        _, col, _ = st.columns([1, 2, 1])
        with col:
            st.markdown('<div class="auth-container">', unsafe_allow_html=True)
            if st.session_state.get('signup_success'):
                st.success("✅ Account created! Please login.")
                st.session_state.signup_success = False
            
            with st.form("login"):
                st.subheader("Login")
                email, pw = st.text_input("Email"), st.text_input("Password", type="password")
                if st.form_submit_button("Login", type="primary", use_container_width=True):
                    if email and pw:
                        res = API.login(email, pw)
                        if res and res.get("status") == "success":
                            SessionState.clear_auth()
                            st.session_state.update({'logged_in': True, 'user_data': res["user"], 'current_page': 'home'})
                            SessionState.update_activity()
                            st.rerun()
                    else:
                        st.error("Fill all fields")
            st.markdown('</div>', unsafe_allow_html=True)
            if st.button("Create Account", use_container_width=True):
                st.session_state.current_page = "signup"
                st.rerun()

class SignupPage:
    @staticmethod
    def render():
        _, col, _ = st.columns([1, 2, 1])
        with col:
            st.markdown('<div class="welcome-title">Join PulseBoard.ai</div>', unsafe_allow_html=True)
            st.markdown('<div class="auth-container">', unsafe_allow_html=True)
            
            with st.form("signup"):
                st.subheader("Create Account")
                c1, c2 = st.columns(2)
                first, last = c1.text_input("First Name"), c2.text_input("Last Name")
                email = st.text_input("Email")
                c1, c2 = st.columns(2)
                pw, conf = c1.text_input("Password", type="password"), c2.text_input("Confirm", type="password")
                
                if st.form_submit_button("Create", type="primary", use_container_width=True):
                    if all([first, last, email, pw, conf]) and pw == conf and len(pw) >= 6:
                        res = API.create_user(email, first, last, pw)
                        if res and res.get("status") == "success":
                            st.session_state.update({'signup_success': True, 'current_page': 'login'})
                            st.rerun()
                    else:
                        st.error("Check all fields")
            st.markdown('</div>', unsafe_allow_html=True)
            if st.button("← Back", use_container_width=True):
                st.session_state.current_page = "login"
                st.rerun()

class HomePage:
    @staticmethod
    def render():
        uid, name = st.session_state.user_data.get("user_id"), st.session_state.user_data.get("name", "User")
        
        with st.sidebar:
            st.markdown(f"### 👋 {name.split()[0]}")
            details = API.get_user_details(uid)
            if details and details.get("status") == "success":
                st.markdown("---")
                st.write(f"**Email:** {details['user'].get('email')}")
            st.markdown("---")
            if st.button("🚪 Logout", use_container_width=True):
                SessionState.clear_auth()
                st.session_state.current_page = "login"
                st.rerun()
        
        c1, c2, c3 = st.columns([3, 1, 1])
        with c1:
            st.markdown(f"# 👋 Welcome, {name}!")
        
        proj_res = API.get_all_projects(uid)
        projs = proj_res.get("projects", []) if proj_res else []
        
        with c2:
            st.metric("Projects", len(projs))
        with c3:
            if st.button("📊 New", type="primary", use_container_width=True):
                st.session_state.creating_project = True
                st.rerun()
        
        st.markdown("---")
        
        if conf := st.session_state.get('delete_confirmation'):
            st.warning(f"⚠️ Delete '{conf['project_name']}'?")
            c1, c2 = st.columns(2)
            if c1.button("✅ Delete", type="primary", use_container_width=True):
                res = API.delete_project(uid, conf['project_id'])
                if res and res.get("status") == "success":
                    st.success("✅ Deleted!")
                    st.session_state.pop('delete_confirmation', None)
                    st.rerun()
            if c2.button("❌ Cancel", use_container_width=True):
                st.session_state.pop('delete_confirmation', None)
                st.rerun()
            return
        
        if projs:
            st.markdown('<div class="section-title">📂 Projects</div>', unsafe_allow_html=True)
            for i, p in enumerate(projs):
                render_project_card(p, uid, "all", i)
        else:
            st.markdown('<div class="empty-state"><h3>No projects</h3></div>', unsafe_allow_html=True)

class CreateProjectPage:
    @staticmethod
    def render():
        uid = st.session_state.user_data.get("user_id")
        
        with st.sidebar:
            st.markdown("### New Project")
            if st.button("← Back", use_container_width=True):
                st.session_state.creating_project = False
                st.rerun()
        
        st.markdown("# 📊 Create Project")
        
        if st.session_state.get('processing_started'):
            CreateProjectPage._processing()
        elif st.session_state.get('project_created'):
            if st.session_state.get('upload_complete'):
                CreateProjectPage._upload_success()
            else:
                CreateProjectPage._upload(uid)
        else:
            CreateProjectPage._form(uid)
    
    @staticmethod
    def _form(uid):
        with st.form("create"):
            name = st.text_input("Project Name *")
            domain = st.selectbox("Domain *", Config.DOMAIN_OPTIONS)
            if st.form_submit_button("Create", type="primary", use_container_width=True):
                if name.strip() and domain:
                    res = API.create_project(uid, name.strip(), domain)
                    if res and res.get("status") == "success":
                        st.session_state.update({
                            'project_created': True,
                            'created_project_data': {
                                'project_id': res['project'].get('project_id'),
                                'project_name': res['project'].get('name_of_project')
                            }
                        })
                        st.rerun()
    
    @staticmethod
    def _upload(uid):
        pd = st.session_state.created_project_data
        pid = pd['project_id']
        st.success(f"✅ '{pd['project_name']}' created!")
        st.markdown("### 📁 Upload Data")
        
        file = st.file_uploader("File", type=["csv", "xlsx", "xls", "json"])
        if file:
            ftype = 'csv' if file.name.endswith('.csv') else 'excel' if file.name.endswith(('.xlsx', '.xls')) else 'json'
            if st.button("📤 Upload", type="primary", use_container_width=True):
                res = API.upload_data(pid, uid, file, ftype)
                if res and res.get("status") == "success":
                    st.session_state.update({'upload_complete': True, 'upload_result': res})
                    st.rerun()
    
    @staticmethod
    def _upload_success():
        st.success("✅ Data uploaded!")
        st.markdown("### 🚀 Generate Dashboard")
        pid = st.session_state.created_project_data['project_id']
        
        if st.button("🚀 Generate", type="primary", use_container_width=True):
            res = API.process_pipeline(pid)
            if res and res.get("status") == "success":
                st.session_state.update({'processing_started': True, 'processing_start': datetime.now()})
                st.rerun()
        
        c1, c2 = st.columns(2)
        if c1.button("🏠 Home", use_container_width=True):
            st.session_state.update({'creating_project': False, 'project_created': False})
            st.rerun()
    
    @staticmethod
    def _processing():
        st.markdown("# ⏳ Generating Dashboard")
        st.info("Processing... (~30 min)")
        
        if st.button("🏠 Home", use_container_width=True):
            st.session_state.update({'creating_project': False, 'processing_started': False})
            st.rerun()

class DashboardPage:
    @staticmethod
    def render(pid, pname):
        uid = st.session_state.user_data.get("user_id")
        
        with st.sidebar:
            st.markdown(f"## 📊 {pname}")
            if st.button("🏠 Back", use_container_width=True, type="primary"):
                st.session_state.current_page = "home"
                st.rerun()
        
        st.markdown(f"# 📈 {pname}")
        
        dash = API.get_dashboard_layout(pid, uid)
        if not dash or not dash.get("charts"):
            st.warning("No charts available")
            return
        
        charts = dash.get("charts", [])
        st.markdown(f"### 📊 Dashboard ({len(charts)} charts)")
        
        # 2x3 grid
        for row in range(2):
            cols = st.columns(3)
            for col_idx in range(3):
                idx = row * 3 + col_idx
                with cols[col_idx]:
                    if idx < len(charts):
                        ChartRenderer.render(charts[idx], idx)

# ============================================
# Main
# ============================================
def main():
    SessionState.init()
    load_styles()
    
    if SessionState.is_expired():
        SessionState.clear_auth()
        st.warning("Session expired")
    
    if st.session_state.logged_in:
        if st.session_state.get('current_page') == 'dashboard' and st.session_state.get('dashboard_project_id'):
            DashboardPage.render(st.session_state.dashboard_project_id, st.session_state.dashboard_project_name)
        elif st.session_state.creating_project:
            CreateProjectPage.render()
        else:
            HomePage.render()
    else:
        if st.session_state.current_page == "signup":
            SignupPage.render()
        else:
            LoginPage.render()

if __name__ == "__main__":
    main()