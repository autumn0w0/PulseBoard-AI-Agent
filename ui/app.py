#streamlit run app.py
import streamlit as st
import requests
import os
from dotenv import load_dotenv
from datetime import datetime
from typing import Dict, Any, Optional, Tuple
import json
import asyncio
from functools import lru_cache
import io

# Load environment variables
load_dotenv()

# ============================================
# Configuration & Constants
# ============================================
API_URL = os.getenv("API_URL", "http://localhost:8000")
SESSION_TIMEOUT = 1800  # 30 minutes

# Page configuration
st.set_page_config(
    page_title="PulseBoard.ai",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ============================================
# Session State Management
# ============================================
class SessionStateManager:
    """Centralized session state management"""
    
    @staticmethod
    def initialize():
        """Initialize all session state variables"""
        defaults = {
            'logged_in': False,
            'user_data': None,
            'user_details': None,
            'current_page': "login",
            'projects_data': None,
            'all_projects': None,
            'creating_project': False,
            'project_created': False,
            'created_project_data': None,
            'uploading_data': False,
            'upload_complete': False,
            'upload_result': None,
            'signup_success': False,
            'last_refresh': None
        }
        
        for key, value in defaults.items():
            if key not in st.session_state:
                st.session_state[key] = value
    
    @staticmethod
    def clear_user_session():
        """Clear all user-related session data"""
        keys_to_clear = [
            'logged_in', 'user_data', 'user_details',
            'projects_data', 'all_projects', 'last_refresh'
        ]
        for key in keys_to_clear:
            if key in st.session_state:
                del st.session_state[key]
        SessionStateManager.initialize()

# ============================================
# API Client
# ============================================
class APIClient:
    """Centralized API client with error handling and caching"""
    
    @staticmethod
    def _make_request(method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        """Generic request handler with error handling"""
        try:
            url = f"{API_URL}{endpoint}"
            response = requests.request(method, url, **kwargs, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.ConnectionError:
            raise ConnectionError("Cannot connect to server. Please make sure the API server is running.")
        except requests.exceptions.Timeout:
            raise TimeoutError("Request timed out. Please try again.")
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                raise ValueError("Resource not found")
            elif e.response.status_code == 409:
                raise ValueError("Resource already exists")
            else:
                raise
        except Exception as e:
            raise Exception(f"API request failed: {str(e)}")
    
    @staticmethod
    def login(email: str, password: str) -> Dict[str, Any]:
        """Authenticate user"""
        return APIClient._make_request(
            "POST", "/user-login",
            json={"email": email, "password": password}
        )
    
    @staticmethod
    def create_user(email: str, first_name: str, last_name: str, password: str) -> Dict[str, Any]:
        """Create new user"""
        return APIClient._make_request(
            "POST", "/create-user",
            json={
                "email": email,
                "first_name": first_name,
                "last_name": last_name,
                "password": password
            }
        )
    
    @staticmethod
    def get_user_details(user_id: str) -> Dict[str, Any]:
        """Get user details"""
        return APIClient._make_request("GET", f"/user-details/{user_id}")
    
    @staticmethod
    def get_recent_projects(user_id: str) -> Dict[str, Any]:
        """Get recent projects"""
        return APIClient._make_request("GET", f"/recent-projects/{user_id}")
    
    @staticmethod
    def get_all_projects(user_id: str) -> Dict[str, Any]:
        """Get all projects"""
        return APIClient._make_request("GET", f"/all-projects/{user_id}")
    
    @staticmethod
    def update_project_last_used(project_id: str) -> Dict[str, Any]:
        """Update project last used timestamp"""
        return APIClient._make_request(
            "PUT", "/update-project-last-used",
            json={"project_id": project_id}
        )
    
    @staticmethod
    def delete_project(user_id: str, project_id: str) -> Dict[str, Any]:
        """Delete project"""
        return APIClient._make_request(
            "DELETE", "/delete-project",
            json={"user_id": user_id, "project_id": project_id}
        )
    
    @staticmethod
    def create_project(user_id: str, project_name: str, domain: str) -> Dict[str, Any]:
        """Create new project"""
        return APIClient._make_request(
            "POST", "/create-project",
            json={
                "user_id": user_id,
                "project_name": project_name,
                "domain": domain
            }
        )
    
    @staticmethod
    def upload_data(project_id: str, user_id: str, file, file_type: str) -> Dict[str, Any]:
        """Upload data file to project"""
        # Convert file bytes to BytesIO object for proper file upload
        file_bytes = file.getvalue() if hasattr(file, 'getvalue') else file
        file_name = file.name if hasattr(file, 'name') else 'uploaded_file'
        
        # Create a proper file-like object
        file_obj = io.BytesIO(file_bytes)
        
        # Prepare the multipart form data
        files = {
            "file": (file_name, file_obj, "application/octet-stream")
        }
        data = {
            "user_id": user_id, 
            "file_type": file_type
        }
        
        return APIClient._make_request(
            "POST", f"/upload-data/{project_id}",
            files=files,
            data=data
        )
    
    @staticmethod
    def get_upload_status(project_id: str, user_id: str) -> Dict[str, Any]:
        """Get upload status for a project"""
        return APIClient._make_request(
            "GET", f"/upload-status/{project_id}",
            params={"user_id": user_id}
        )
    
    @staticmethod
    def process_pipeline(project_id: str) -> Dict[str, Any]:
        """Start data processing pipeline"""
        return APIClient._make_request(
            "POST", f"/data-process-pipeline/{project_id}"
        )
    
    @staticmethod
    def check_pipeline_status(project_id: str) -> Dict[str, Any]:
        """Check pipeline status"""
        return APIClient._make_request(
            "GET", f"/pipeline-status/{project_id}"
        )

# ============================================
# UI Components
# ============================================
class UIComponents:
    """Reusable UI components"""
    
    @staticmethod
    def load_css():
        """Load custom CSS styles"""
        st.markdown("""
            <style>
            .main { padding: 0rem 1rem; }
            .stButton > button { margin-top: 10px; }
            
            /* Containers */
            .auth-container {
                max-width: 400px;
                margin: 0 auto;
                padding: 30px;
                border-radius: 15px;
                background: var(--background-color);
                box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
                border: 1px solid var(--secondary-background-color);
            }
            
            /* Typography */
            .welcome-title {
                text-align: center;
                font-size: 2.5rem;
                font-weight: 800;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                margin-bottom: 0.5rem;
            }
            
            .welcome-subtitle {
                text-align: center;
                font-size: 1.1rem;
                color: var(--text-color);
                opacity: 0.8;
                margin-bottom: 2rem;
            }
            
            /* Cards */
            .project-card {
                padding: 20px;
                border-radius: 12px;
                margin-bottom: 15px;
                background: linear-gradient(145deg, var(--background-color), var(--secondary-background-color));
                border: 1px solid var(--secondary-background-color);
                transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
            }
            
            .project-card:hover {
                transform: translateY(-4px);
                box-shadow: 0 12px 24px rgba(0, 0, 0, 0.15);
                border-color: var(--primary-color);
            }
            
            /* Messages */
            .success-message {
                padding: 12px 16px;
                background: linear-gradient(135deg, #d4edda, #c3e6cb);
                color: #155724;
                border-radius: 8px;
                margin-bottom: 20px;
                border-left: 4px solid #28a745;
            }
            
            .error-message {
                padding: 12px 16px;
                background: linear-gradient(135deg, #f8d7da, #f5c6cb);
                color: #721c24;
                border-radius: 8px;
                margin-bottom: 20px;
                border-left: 4px solid #dc3545;
            }
            
            /* Buttons */
            .form-button {
                width: 100% !important;
                font-weight: 600 !important;
                border-radius: 8px !important;
                padding: 10px 24px !important;
                transition: all 0.3s ease !important;
            }
            
            /* Sections */
            .section-title {
                font-size: 1.5rem;
                font-weight: 700;
                margin: 30px 0 20px 0;
                padding-bottom: 10px;
                border-bottom: 2px solid var(--primary-color);
                position: relative;
            }
            
            .section-title::after {
                content: '';
                position: absolute;
                bottom: -2px;
                left: 0;
                width: 60px;
                height: 2px;
                background: linear-gradient(90deg, var(--primary-color), transparent);
            }
            
            /* Empty states */
            .empty-state {
                text-align: center;
                padding: 60px 20px;
                color: var(--text-color);
                opacity: 0.6;
            }
            
            /* Status badges */
            .status-badge {
                display: inline-block;
                padding: 4px 12px;
                border-radius: 20px;
                font-size: 0.8rem;
                font-weight: 500;
                margin-right: 8px;
                margin-bottom: 8px;
            }
            
            .status-badge-finance { background: #e3f2fd; color: #1976d2; }
            .status-badge-healthcare { background: #f3e5f5; color: #7b1fa2; }
            .status-badge-ecommerce { background: #e8f5e9; color: #388e3c; }
            .status-badge-other { background: #f5f5f5; color: #616161; }
            
            /* Animations */
            @keyframes fadeIn {
                from { opacity: 0; transform: translateY(10px); }
                to { opacity: 1; transform: translateY(0); }
            }
            
            .fade-in {
                animation: fadeIn 0.5s ease-out;
            }
            </style>
        """, unsafe_allow_html=True)
    
    @staticmethod
    def format_timestamp(timestamp_str: str) -> str:
        """Format ISO timestamp to readable format"""
        try:
            dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            return dt.strftime("%b %d, %Y • %I:%M %p")
        except:
            return timestamp_str
    
    @staticmethod
    def get_domain_badge_class(domain: str) -> str:
        """Get CSS class for domain badge"""
        domain_map = {
            'finance': 'status-badge-finance',
            'healthcare': 'status-badge-healthcare',
            'ecommerce': 'status-badge-ecommerce',
            'education': 'status-badge-education',
            'technology': 'status-badge-technology',
            'marketing': 'status-badge-marketing',
        }
        return domain_map.get(domain, 'status-badge-other')
    
    @staticmethod
    def project_card(project: Dict[str, Any], user_id: str, key_suffix: str = ""):
        """Render a project card"""
        project_id = project.get('project_id')
        project_name = project.get('name_of_project', 'Unnamed Project')
        domain = project.get('domain', 'general')
        
        with st.container():
            col1, col2, col3 = st.columns([3, 2, 1])
            
            with col1:
                st.markdown(f"**{project_name}**", help=f"Project ID: {project_id}")
                domain_badge = UIComponents.get_domain_badge_class(domain)
                st.markdown(f'<span class="status-badge {domain_badge}">{domain.title()}</span>', 
                          unsafe_allow_html=True)
            
            with col2:
                created_at = UIComponents.format_timestamp(project.get('created_at', ''))
                last_used = UIComponents.format_timestamp(project.get('last_used_at', ''))
                st.caption(f"📅 Created: {created_at}")
                st.caption(f"⏰ Last Used: {last_used}")
            
            with col3:
                btn_col1, btn_col2 = st.columns(2)
                with btn_col1:
                    if st.button("📂", key=f"open_{key_suffix}_{project_id}", 
                               help="Open Project", use_container_width=True):
                        return ("open", project_id)
                with btn_col2:
                    if st.button("🗑️", key=f"delete_{key_suffix}_{project_id}",
                               help="Delete Project", use_container_width=True):
                        return ("delete", project_id)
            
            st.divider()
        return (None, None)

# ============================================
# Page Components
# ============================================
class LoginPage:
    """Login page component"""
    
    @staticmethod
    def render():
        """Render login page"""
        st.markdown('<div class="welcome-title">Welcome to PulseBoard.ai</div>', unsafe_allow_html=True)
        st.markdown('<div class="welcome-subtitle">Intelligent insights for your business</div>', unsafe_allow_html=True)
        
        with st.container():
            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                st.markdown('<div class="auth-container fade-in">', unsafe_allow_html=True)
                
                # Success message from signup
                if st.session_state.get('signup_success'):
                    st.markdown('<div class="success-message">✅ Account created successfully! Please login.</div>', 
                              unsafe_allow_html=True)
                    st.session_state.signup_success = False
                
                with st.form("login_form"):
                    st.subheader("Login to Your Account")
                    
                    email = st.text_input("Email", placeholder="Enter your email", key="login_email")
                    password = st.text_input("Password", type="password", placeholder="Enter your password", 
                                           key="login_password")
                    
                    submitted = st.form_submit_button("Login", type="primary", use_container_width=True)
                    
                    if submitted:
                        if not email or not password:
                            st.error("Please fill in all fields")
                        else:
                            with st.spinner("Authenticating..."):
                                try:
                                    result = APIClient.login(email, password)
                                    if result.get("status") == "success":
                                        SessionStateManager.clear_user_session()
                                        st.session_state.logged_in = True
                                        st.session_state.user_data = result["user"]
                                        st.session_state.current_page = "home"
                                        st.rerun()
                                    else:
                                        st.error(f"❌ {result.get('message', 'Login failed')}")
                                except Exception as e:
                                    st.error(f"❌ {str(e)}")
                
                st.markdown('</div>', unsafe_allow_html=True)
                
                # Signup link
                st.markdown("---")
                st.markdown('<div style="text-align: center; margin-bottom: 10px;">New here?</div>', 
                          unsafe_allow_html=True)
                if st.button("Create Account", key="goto_signup", use_container_width=True):
                    st.session_state.current_page = "signup"
                    st.rerun()

class SignupPage:
    """Signup page component"""
    
    @staticmethod
    def render():
        """Render signup page"""
        col1_title, col2_title, col3_title = st.columns([1, 2, 1])
        with col2_title:
            st.markdown('<div class="welcome-title">Join PulseBoard.ai</div>', unsafe_allow_html=True)
            st.markdown('<div class="welcome-subtitle">Create your account to get started</div>', 
                      unsafe_allow_html=True)
        
        with st.container():
            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                st.markdown('<div class="auth-container fade-in">', unsafe_allow_html=True)
                
                with st.form("signup_form"):
                    st.subheader("Create Your Account")
                    
                    col1_name, col2_name = st.columns(2)
                    with col1_name:
                        first_name = st.text_input("First Name", placeholder="Enter your first name")
                    with col2_name:
                        last_name = st.text_input("Last Name", placeholder="Enter your last name")
                    
                    email = st.text_input("Email", placeholder="Enter your email")
                    
                    col1_pw, col2_pw = st.columns(2)
                    with col1_pw:
                        password = st.text_input("Password", type="password", 
                                               placeholder="Create a password")
                    with col2_pw:
                        confirm_password = st.text_input("Confirm Password", type="password",
                                                       placeholder="Confirm your password")
                    
                    submitted = st.form_submit_button("Create Account", type="primary", 
                                                    use_container_width=True)
                    
                    if submitted:
                        if not all([first_name, last_name, email, password, confirm_password]):
                            st.error("Please fill in all fields")
                        elif password != confirm_password:
                            st.error("Passwords do not match")
                        elif len(password) < 6:
                            st.error("Password must be at least 6 characters long")
                        else:
                            with st.spinner("Creating account..."):
                                try:
                                    result = APIClient.create_user(email, first_name, last_name, password)
                                    if result.get("status") == "success":
                                        st.session_state.signup_success = True
                                        st.session_state.current_page = "login"
                                        st.rerun()
                                    else:
                                        st.error(f"❌ {result.get('message', 'Signup failed')}")
                                except Exception as e:
                                    st.error(f"❌ {str(e)}")
                
                st.markdown('</div>', unsafe_allow_html=True)
                
                # Back to login
                st.markdown('<div class="back-button-container">', unsafe_allow_html=True)
                if st.button("← Back to Login", use_container_width=True):
                    st.session_state.current_page = "login"
                    st.rerun()
                st.markdown('</div>', unsafe_allow_html=True)

class HomePage:
    """Home page component"""
    
    @staticmethod
    def _load_data(user_id: str):
        """Load user data with caching"""
        if (st.session_state.get('last_refresh') is None or 
            (datetime.now() - st.session_state.last_refresh).seconds > 60):
            
            with st.spinner("Loading data..."):
                try:
                    # Parallel data loading
                    user_details = APIClient.get_user_details(user_id)
                    recent_projects = APIClient.get_recent_projects(user_id)
                    all_projects = APIClient.get_all_projects(user_id)
                    
                    if user_details.get("status") == "success":
                        st.session_state.user_details = user_details.get("user", {})
                    
                    if recent_projects.get("status") == "success":
                        st.session_state.projects_data = recent_projects.get("projects", [])
                    
                    if all_projects.get("status") == "success":
                        st.session_state.all_projects = all_projects.get("projects", [])
                    
                    st.session_state.last_refresh = datetime.now()
                    
                except Exception as e:
                    st.error(f"Failed to load data: {str(e)}")
    
    @staticmethod
    def _render_sidebar(user_name: str):
        """Render sidebar"""
        with st.sidebar:
            st.markdown(f"### 👋 Welcome, {user_name.split()[0] if user_name else 'User'}!")
            
            if 'user_details' in st.session_state:
                st.markdown("---")
                st.markdown("#### 📋 Account Info")
                st.write(f"**Email:** {st.session_state.user_details.get('email', 'N/A')}")
                st.write(f"**User ID:** {st.session_state.user_data.get('user_id')}")
            
            st.markdown("---")
            
            if st.button("🚪 Logout", type="secondary", use_container_width=True):
                SessionStateManager.clear_user_session()
                st.session_state.current_page = "login"
                st.rerun()
    
    @staticmethod
    def _render_delete_confirmation(project_id: str, project_name: str, user_id: str):
        """Render project deletion confirmation dialog"""
        st.markdown(f"<div class='section-title'>⚠️ Confirm Deletion</div>", unsafe_allow_html=True)
        
        with st.container():
            st.warning(f"**Are you sure you want to delete '{project_name}'?**")
            st.markdown("*This action cannot be undone.*")
            
            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                # Confirmation buttons in a container
                with st.container():
                    confirm_col1, confirm_col2 = st.columns(2)
                    
                    with confirm_col1:
                        if st.button("✅ Yes, Delete", 
                                   key="confirm_delete_yes",
                                   use_container_width=True,
                                   type="primary"):
                            with st.spinner("Deleting project..."):
                                try:
                                    result = APIClient.delete_project(user_id, project_id)
                                    if result.get("status") == "success":
                                        st.success(f"✅ Project '{project_name}' deleted successfully!")
                                        # Clear confirmation state
                                        if 'delete_confirmation' in st.session_state:
                                            del st.session_state.delete_confirmation
                                        # Clear cached data to force refresh
                                        st.session_state.projects_data = None
                                        st.session_state.all_projects = None
                                        st.session_state.last_refresh = None
                                        # Force immediate rerun
                                        st.rerun()
                                    else:
                                        st.error(f"❌ {result.get('message', 'Deletion failed')}")
                                except Exception as e:
                                    st.error(f"❌ Failed to delete: {str(e)}")
                    
                    with confirm_col2:
                        if st.button("❌ Cancel", 
                                   key="confirm_delete_no",
                                   use_container_width=True):
                            # Clear confirmation state
                            if 'delete_confirmation' in st.session_state:
                                del st.session_state.delete_confirmation
                            st.rerun()
    
    @staticmethod
    def _render_project_list(projects: list, title: str, user_id: str, key_suffix: str):
        """Render a list of projects"""
        if not projects:
            st.markdown(f"""
                <div class="empty-state">
                    <h3>No projects found</h3>
                    <p>Create your first project to get started!</p>
                </div>
            """, unsafe_allow_html=True)
            return
        
        st.markdown(f'<div class="section-title">{title}</div>', unsafe_allow_html=True)
        
        for idx, project in enumerate(projects):
            project_id = project.get('project_id')
            project_name = project.get('name_of_project', 'Unnamed Project')
            domain = project.get('domain', 'general')
            
            with st.container():
                col1, col2, col3 = st.columns([3, 2, 1])
                
                with col1:
                    st.markdown(f"**{project_name}**", help=f"Project ID: {project_id}")
                    domain_badge = UIComponents.get_domain_badge_class(domain)
                    st.markdown(f'<span class="status-badge {domain_badge}">{domain.title()}</span>', 
                              unsafe_allow_html=True)
                
                with col2:
                    created_at = UIComponents.format_timestamp(project.get('created_at', ''))
                    last_used = UIComponents.format_timestamp(project.get('last_used_at', ''))
                    st.caption(f"📅 Created: {created_at}")
                    st.caption(f"⏰ Last Used: {last_used}")
                
                with col3:
                    btn_col1, btn_col2 = st.columns(2)
                    with btn_col1:
                        if st.button("📂", 
                                   key=f"open_{key_suffix}_{project_id}_{idx}",
                                   help="Open Project", 
                                   use_container_width=True):
                            with st.spinner("Opening project..."):
                                try:
                                    APIClient.update_project_last_used(project_id)
                                    st.success(f"Opening {project_name}...")
                                    # Clear cached data
                                    st.session_state.projects_data = None
                                    st.session_state.all_projects = None
                                    st.session_state.last_refresh = None
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"❌ Failed to open: {str(e)}")
                    
                    with btn_col2:
                        if st.button("🗑️", 
                                   key=f"delete_{key_suffix}_{project_id}_{idx}",
                                   help="Delete Project",
                                   use_container_width=True):
                            # Set the project for deletion confirmation
                            st.session_state.delete_confirmation = {
                                'project_id': project_id,
                                'project_name': project_name
                            }
                            st.rerun()
                
                st.divider()
    
    @staticmethod
    def render():
        """Render home page"""
        user_id = st.session_state.user_data.get("user_id")
        user_name = st.session_state.user_data.get("name", "User")
        
        # Load data
        HomePage._load_data(user_id)
        
        # Render sidebar
        HomePage._render_sidebar(user_name)
        
        # Main content
        col1, col2, col3 = st.columns([3, 1, 1])
        with col1:
            st.markdown(f"# 👋 Welcome, {user_name}!")
            st.markdown(f"#### Your intelligent analytics dashboard")
        
        with col2:
            if st.session_state.all_projects:
                st.metric("Total Projects", len(st.session_state.all_projects))
        
        with col3:
            if st.button("📊 New Project", type="primary", use_container_width=True):
                st.session_state.creating_project = True
                st.rerun()
        
        st.markdown("---")
        
        # Check if we're showing a delete confirmation
        if 'delete_confirmation' in st.session_state and st.session_state.delete_confirmation:
            # Show only the confirmation dialog
            project_id = st.session_state.delete_confirmation['project_id']
            project_name = st.session_state.delete_confirmation['project_name']
            HomePage._render_delete_confirmation(project_id, project_name, user_id)
            return  # Exit early, don't show project lists
        
        # If no delete confirmation, show normal project lists
        
        # Recent Projects
        HomePage._render_project_list(
            st.session_state.projects_data[:3] if st.session_state.projects_data else [],
            "📈 Recent Activity",
            user_id,
            "recent"
        )
        
        st.markdown("---")
        
        # All Projects
        HomePage._render_project_list(
            st.session_state.all_projects if st.session_state.all_projects else [],
            "📂 All Projects",
            user_id,
            "all"
        )

class CreateProjectPage:
    """Create project page component"""
    
    @staticmethod
    def _reset_upload_state():
        """Reset upload-related states"""
        upload_states = ['project_created', 'created_project_data', 
                        'uploading_data', 'upload_complete', 'upload_result',
                        'processing_started', 'processing_project_id', 'processing_start_time']
        for state in upload_states:
            if state in st.session_state:
                del st.session_state[state]
    
    @staticmethod
    def _render_upload_section(user_id: str):
        """Render data upload section"""
        project_name = st.session_state.created_project_data.get("project_name")
        project_id = st.session_state.created_project_data.get("project_id")
        
        st.success(f"✅ Project '{project_name}' created successfully!")
        st.info(f"**Project ID:** {project_id}")
        
        st.markdown("---")
        st.markdown("### 📁 Upload Your Data")
        st.markdown("Upload your dataset to start analyzing. Supported formats: CSV, Excel (XLS/XLSX), JSON")
        
        # Store uploaded file in session state to persist between reruns
        if 'uploaded_file' not in st.session_state:
            st.session_state.uploaded_file = None
        if 'file_type' not in st.session_state:
            st.session_state.file_type = "auto"
        
        uploaded_file = st.file_uploader(
            "Choose a file",
            type=["csv", "xlsx", "xls", "json"],
            help="Upload CSV, Excel, or JSON files",
            key="file_uploader_widget"
        )
        
        # Update session state when file is uploaded
        if uploaded_file is not None:
            st.session_state.uploaded_file = uploaded_file
            
            # Auto-detect file type based on extension
            filename = uploaded_file.name.lower()
            if filename.endswith('.csv'):
                st.session_state.file_type = "csv"
            elif filename.endswith('.xlsx') or filename.endswith('.xls'):
                st.session_state.file_type = "excel"
            elif filename.endswith('.json'):
                st.session_state.file_type = "json"
            else:
                st.session_state.file_type = "auto"
        
        # Show file info if uploaded
        if st.session_state.uploaded_file:
            col1, col2 = st.columns([3, 1])
            with col1:
                st.info(f"📄 **Selected file:** {st.session_state.uploaded_file.name}")
                st.caption(f"Size: {len(st.session_state.uploaded_file.getvalue()) / 1024:.1f} KB")
            
            # File type selector
            file_type = st.selectbox(
                "File Type",
                options=["auto", "csv", "excel", "json"],
                index=["auto", "csv", "excel", "json"].index(st.session_state.file_type),
                help="Auto-detect or manually select file type"
            )
            
            # Upload button
            if st.button("📤 Upload Data", 
                        type="primary", 
                        use_container_width=True,
                        key="upload_data_button"):
                
                with st.spinner("Uploading and processing data..."):
                    try:
                        # Get the file from session state
                        file_to_upload = st.session_state.uploaded_file
                        
                        # Upload the data
                        result = APIClient.upload_data(
                            project_id, 
                            user_id, 
                            file_to_upload, 
                            file_type
                        )
                        
                        if result.get("status") == "success":
                            st.session_state.upload_complete = True
                            st.session_state.upload_result = result
                            # Clear uploaded file from session state
                            if 'uploaded_file' in st.session_state:
                                del st.session_state.uploaded_file
                            if 'file_type' in st.session_state:
                                del st.session_state.file_type
                            st.rerun()
                        else:
                            st.error(f"❌ {result.get('message', 'Upload failed')}")
                            
                    except Exception as e:
                        st.error(f"❌ Upload failed: {str(e)}")
                        st.error(f"Details: Make sure the API server is running at {API_URL}")
        else:
            st.warning("⚠️ Please select a file to upload")
    
    @staticmethod
    def _render_upload_success():
        """Render upload success section"""
        records_inserted = st.session_state.upload_result.get("records_inserted", 0)
        
        st.success(f"✅ Data uploaded successfully! ({records_inserted} records)")
        
        # REMOVED DATA PREVIEW SECTION
        
        st.markdown("---")
        st.markdown("### 🚀 Ready to Generate Dashboard!")
        st.markdown("""
        Your data has been uploaded and is ready for analysis.
        
        Click the **Generate Dashboard** button to start the automated data processing pipeline.
        This will:
        
        1. **Analyze Data Structure** - Identify data types and relationships
        2. **Process Data** - Clean, transform, and prepare your data
        3. **Generate Insights** - Create AI-powered analytics and visualizations
        4. **Build Dashboard** - Create an interactive dashboard with key metrics
        
        ⏰ **Estimated time:** Approximately 30 minutes
        """)
        
        project_id = st.session_state.created_project_data.get("project_id")
        
        # Generate Dashboard button
        if st.button("🚀 Generate Dashboard", 
                    type="primary", 
                    use_container_width=True,
                    help="Start the data processing pipeline to create your dashboard"):
            
            with st.spinner("Starting data processing pipeline..."):
                try:
                    # Call the data processing pipeline API
                    result = APIClient.process_pipeline(project_id)
                    
                    if result.get("status") == "success":
                        st.session_state.processing_started = True
                        st.session_state.processing_project_id = project_id
                        st.session_state.processing_start_time = datetime.now()
                        st.rerun()
                    else:
                        st.error(f"❌ {result.get('message', 'Failed to start processing')}")
                        
                except Exception as e:
                    st.error(f"❌ Failed to start processing pipeline: {str(e)}")
        
        # Alternative actions
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🏠 Go to Dashboard", use_container_width=True):
                CreateProjectPage._reset_upload_state()
                st.session_state.creating_project = False
                st.session_state.projects_data = None
                st.session_state.all_projects = None
                st.rerun()
        
        with col2:
            if st.button("➕ Upload More Data", use_container_width=True):
                CreateProjectPage._reset_upload_state()
                st.rerun()
    
    @staticmethod
    def _render_processing_section():
        """Render processing in progress section"""
        project_id = st.session_state.processing_project_id
        
        st.markdown("# ⏳ Dashboard Generation Started")
        
        # Show progress information
        with st.container():
            st.info("""
            ## Your dashboard is being generated!
            
            We've started the data processing pipeline. This typically takes about **30 minutes** to complete.
            
            **What's happening:**
            - 📊 Analyzing your data structure
            - 🧹 Cleaning and preparing data
            - 🔍 Identifying key insights and patterns
            - 📈 Creating visualizations and metrics
            - 🎨 Building your interactive dashboard
            
            **You can:**
            - Close this window and come back later
            - Check your email for completion notification
            - Return to your projects dashboard
            
            We'll notify you when your dashboard is ready!
            """)
        
        # Timer display
        if st.session_state.processing_start_time:
            elapsed = datetime.now() - st.session_state.processing_start_time
            elapsed_minutes = elapsed.total_seconds() / 60
            
            # Progress bar
            progress_percent = min((elapsed_minutes / 30) * 100, 95)  # Cap at 95% until done
            st.progress(progress_percent / 100)
            
            st.caption(f"⏰ Elapsed time: {int(elapsed_minutes)} minutes (Est. 30 minutes total)")
        
        # Check status button
        if st.button("🔄 Check Status", use_container_width=True):
            with st.spinner("Checking pipeline status..."):
                try:
                    # Call status check API
                    status_result = APIClient.check_pipeline_status(project_id)
                    
                    if status_result.get("status") == "completed":
                        st.success("✅ Dashboard generation completed!")
                        st.balloons()
                        # Redirect or show next steps
                    elif status_result.get("status") == "in_progress":
                        st.info("Dashboard generation is still in progress...")
                    elif status_result.get("status") == "failed":
                        st.error("Dashboard generation failed. Please try again.")
                    else:
                        st.info(f"Status: {status_result.get('status', 'unknown')}")
                        
                except Exception as e:
                    st.error(f"❌ Failed to check status: {str(e)}")
        
        # Navigation buttons
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🏠 Return to Dashboard", use_container_width=True):
                CreateProjectPage._reset_upload_state()
                st.session_state.creating_project = False
                st.session_state.projects_data = None
                st.session_state.all_projects = None
                st.rerun()
        
        with col2:
            if st.button("📋 View My Projects", use_container_width=True):
                CreateProjectPage._reset_upload_state()
                st.session_state.creating_project = False
                st.rerun()
    
    @staticmethod
    def _render_create_form(user_id: str):
        """Render project creation form"""
        with st.form("create_project_form"):
            st.subheader("Project Details")
            
            project_name = st.text_input(
                "Project Name *",
                placeholder="Enter a descriptive name for your project",
                help="e.g., Sales Analysis 2024, Customer Behavior Dashboard",
                key="project_name_input"
            )
            
            domain_options = [
                "finance", "healthcare", "ecommerce", "education", 
                "entertainment", "technology", "marketing", "manufacturing",
                "logistics", "retail", "telecom", "energy", "other"
            ]
            
            domain = st.selectbox(
                "Domain *",
                options=domain_options,
                help="Select the primary domain for your data analysis",
                key="domain_select"
            )
            
            if st.form_submit_button("Create Project", type="primary", use_container_width=True):
                if not project_name or not project_name.strip():
                    st.error("Please enter a project name")
                elif not domain:
                    st.error("Please select a domain")
                else:
                    with st.spinner("Creating project..."):
                        try:
                            result = APIClient.create_project(user_id, project_name.strip(), domain)
                            if result.get("status") == "success":
                                project = result.get("project", {})
                                st.session_state.project_created = True
                                st.session_state.created_project_data = {
                                    "project_id": project.get("project_id"),
                                    "project_name": project.get("name_of_project")
                                }
                                # Clear any previous upload state
                                if 'uploaded_file' in st.session_state:
                                    del st.session_state.uploaded_file
                                if 'file_type' in st.session_state:
                                    del st.session_state.file_type
                                st.rerun()
                            else:
                                st.error(f"❌ {result.get('message', 'Project creation failed')}")
                        except Exception as e:
                            st.error(f"❌ {str(e)}")
    
    @staticmethod
    def render():
        """Render create project page"""
        user_id = st.session_state.user_data.get("user_id")
        user_name = st.session_state.user_data.get("name", "User")
        
        # Sidebar
        with st.sidebar:
            st.markdown(f"### Creating New Project")
            st.markdown(f"**User:** {user_name}")
            st.markdown(f"**User ID:** {user_id}")
            st.markdown("---")
            
            if st.button("← Back to Dashboard", use_container_width=True):
                CreateProjectPage._reset_upload_state()
                # Clear uploaded file state
                if 'uploaded_file' in st.session_state:
                    del st.session_state.uploaded_file
                if 'file_type' in st.session_state:
                    del st.session_state.file_type
                st.session_state.creating_project = False
                st.rerun()
        
        # Main content
        st.markdown(f"# 📊 Create New Project")
        st.markdown("---")
        
        # Check if processing has started
        if st.session_state.get('processing_started'):
            CreateProjectPage._render_processing_section()
        # Show upload section if project was created
        elif st.session_state.get('project_created') and st.session_state.get('created_project_data'):
            if st.session_state.get('upload_complete'):
                CreateProjectPage._render_upload_success()
            else:
                CreateProjectPage._render_upload_section(user_id)
        else:
            CreateProjectPage._render_create_form(user_id)

# ============================================
# Main Application
# ============================================
def main():
    """Main application router"""
    # Initialize session state
    SessionStateManager.initialize()
    
    # Load CSS
    UIComponents.load_css()
    
    # Check authentication timeout
    if (st.session_state.logged_in and st.session_state.get('last_refresh') and 
        (datetime.now() - st.session_state.last_refresh).seconds > SESSION_TIMEOUT):
        SessionStateManager.clear_user_session()
        st.session_state.current_page = "login"
        st.warning("Session expired. Please login again.")
    
    # Route to appropriate page
    if st.session_state.logged_in:
        if st.session_state.creating_project:
            CreateProjectPage.render()
        else:
            HomePage.render()
    else:
        if st.session_state.current_page == "login":
            LoginPage.render()
        elif st.session_state.current_page == "signup":
            SignupPage.render()
        else:
            st.session_state.current_page = "login"
            st.rerun()

if __name__ == "__main__":
    main()