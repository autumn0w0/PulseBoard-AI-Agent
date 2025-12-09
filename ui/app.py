#streamlit run app.py
import streamlit as st
import requests
import os
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

# API Configuration
API_URL = os.getenv("API_URL", "http://localhost:8000")

# Page configuration
st.set_page_config(
    page_title="PulseBoard.ai",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Session state initialization
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
if 'user_data' not in st.session_state:
    st.session_state.user_data = None
if 'current_page' not in st.session_state:
    st.session_state.current_page = "login"
if 'projects_data' not in st.session_state:
    st.session_state.projects_data = None
if 'all_projects' not in st.session_state:
    st.session_state.all_projects = None

# Custom CSS for better styling (theme-aware)
st.markdown("""
    <style>
    .main {
        padding: 0rem 1rem;
    }
    .stButton > button {
        margin-top: 10px;
    }
    .login-container {
        max-width: 400px;
        margin: 0 auto;
        padding: 20px;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    .signup-container {
        max-width: 500px;
        margin: 0 auto;
        padding: 20px;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    .welcome-title {
        text-align: center;
        font-size: 2.5rem;
        font-weight: bold;
        color: var(--primary-color);
        margin-bottom: 1rem;
    }
    .welcome-subtitle {
        text-align: center;
        font-size: 1.2rem;
        color: var(--text-color);
        margin-bottom: 2rem;
    }
    .success-message {
        padding: 10px;
        background-color: #d4edda;
        color: #155724;
        border-radius: 5px;
        margin-bottom: 20px;
    }
    .error-message {
        padding: 10px;
        background-color: #f8d7da;
        color: #721c24;
        border-radius: 5px;
        margin-bottom: 20px;
    }
    .form-button {
        width: 100% !important;
    }
    .back-button-container {
        text-align: center;
        margin-top: 20px;
    }
    .project-card {
        padding: 20px;
        border-radius: 10px;
        margin-bottom: 15px;
        background-color: var(--background-color);
        border: 1px solid var(--secondary-background-color);
        transition: transform 0.2s;
    }
    .project-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1);
    }
    .project-name {
        font-size: 1.2rem;
        font-weight: bold;
        margin-bottom: 5px;
        color: var(--text-color);
    }
    .project-domain {
        font-size: 0.9rem;
        color: var(--text-color);
        background-color: var(--secondary-background-color);
        padding: 3px 8px;
        border-radius: 12px;
        display: inline-block;
        margin-bottom: 10px;
    }
    .project-time {
        font-size: 0.8rem;
        color: var(--text-color);
        opacity: 0.7;
    }
    .empty-state {
        text-align: center;
        padding: 40px;
        color: var(--text-color);
        opacity: 0.7;
    }
    .section-title {
        font-size: 1.5rem;
        font-weight: bold;
        margin-bottom: 20px;
        color: var(--text-color);
        padding-bottom: 10px;
        border-bottom: 2px solid var(--primary-color);
    }
    </style>
""", unsafe_allow_html=True)

def login_user(email: str, password: str):
    """Authenticate user via API"""
    try:
        response = requests.post(
            f"{API_URL}/user-login",
            json={
                "email": email,
                "password": password
            }
        )
        
        if response.status_code == 200:
            data = response.json()
            if data.get("status") == "success":
                return {"success": True, "data": data}
            else:
                return {"success": False, "error": data.get("message", "Login failed")}
        else:
            return {"success": False, "error": f"Login failed with status code: {response.status_code}"}
            
    except requests.exceptions.ConnectionError:
        return {"success": False, "error": "Cannot connect to server. Please make sure the API server is running."}
    except Exception as e:
        return {"success": False, "error": str(e)}

def create_user(email: str, first_name: str, last_name: str, password: str):
    """Create new user via API"""
    try:
        response = requests.post(
            f"{API_URL}/create-user",
            json={
                "email": email,
                "first_name": first_name,
                "last_name": last_name,
                "password": password
            }
        )
        
        if response.status_code == 200:
            data = response.json()
            if data.get("status") == "success":
                return {"success": True, "data": data}
            else:
                return {"success": False, "error": data.get("message", "Signup failed")}
        elif response.status_code == 409:
            return {"success": False, "error": "User already exists with this email."}
        else:
            return {"success": False, "error": f"Signup failed with status code: {response.status_code}"}
            
    except requests.exceptions.ConnectionError:
        return {"success": False, "error": "Cannot connect to server. Please make sure the API server is running."}
    except Exception as e:
        return {"success": False, "error": str(e)}

def get_user_details_from_api(user_id: str):
    """Get user details from API"""
    try:
        response = requests.get(f"{API_URL}/user-details/{user_id}")
        
        if response.status_code == 200:
            data = response.json()
            if data.get("status") == "success":
                return {"success": True, "data": data.get("user", {})}
            else:
                return {"success": False, "error": data.get("message", "Failed to get user details")}
        else:
            return {"success": False, "error": f"Failed with status code: {response.status_code}"}
            
    except requests.exceptions.ConnectionError:
        return {"success": False, "error": "Cannot connect to server."}
    except Exception as e:
        return {"success": False, "error": str(e)}

def get_recent_projects_from_api(user_id: str):
    """Get recent projects from API"""
    try:
        response = requests.get(f"{API_URL}/recent-projects/{user_id}")
        
        if response.status_code == 200:
            data = response.json()
            if data.get("status") == "success":
                return {"success": True, "data": data.get("projects", [])}
            else:
                return {"success": False, "error": data.get("message", "Failed to get projects")}
        else:
            return {"success": False, "error": f"Failed with status code: {response.status_code}"}
            
    except requests.exceptions.ConnectionError:
        return {"success": False, "error": "Cannot connect to server."}
    except Exception as e:
        return {"success": False, "error": str(e)}

def get_all_projects_from_api(user_id: str):
    """Get all projects from API"""
    try:
        response = requests.get(f"{API_URL}/all-projects/{user_id}")
        
        if response.status_code == 200:
            data = response.json()
            if data.get("status") == "success":
                return {"success": True, "data": data.get("projects", [])}
            else:
                return {"success": False, "error": data.get("message", "Failed to get projects")}
        else:
            return {"success": False, "error": f"Failed with status code: {response.status_code}"}
            
    except requests.exceptions.ConnectionError:
        return {"success": False, "error": "Cannot connect to server."}
    except Exception as e:
        return {"success": False, "error": str(e)}

def update_project_last_used(project_id: str):
    """Update project last used timestamp"""
    try:
        response = requests.put(
            f"{API_URL}/update-project-last-used",
            json={"project_id": project_id}
        )
        
        if response.status_code == 200:
            data = response.json()
            if data.get("status") == "success":
                return {"success": True, "data": data}
            else:
                return {"success": False, "error": data.get("message", "Failed to update project")}
        else:
            return {"success": False, "error": f"Failed with status code: {response.status_code}"}
            
    except requests.exceptions.ConnectionError:
        return {"success": False, "error": "Cannot connect to server."}
    except Exception as e:
        return {"success": False, "error": str(e)}

def format_timestamp(timestamp_str: str) -> str:
    """Format ISO timestamp to readable format"""
    try:
        dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        return dt.strftime("%b %d, %Y %I:%M %p")
    except:
        return timestamp_str

def login_page():
    """Render login page"""
    # Welcome title
    st.markdown('<div class="welcome-title">Welcome to PulseBoard.ai</div>', unsafe_allow_html=True)
    st.markdown('<div class="welcome-subtitle">Intelligent insights for your business</div>', unsafe_allow_html=True)
    
    # Create a container for the login form
    with st.container():
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.markdown('<div class="login-container">', unsafe_allow_html=True)
            
            # Display success message if redirected from signup
            if 'signup_success' in st.session_state and st.session_state.signup_success:
                st.markdown('<div class="success-message">✅ Account created successfully! Please login with your credentials.</div>', unsafe_allow_html=True)
                # Clear the flag after showing
                st.session_state.signup_success = False
            
            # Display error message if any
            if 'login_error' in st.session_state:
                st.markdown(f'<div class="error-message">{st.session_state.login_error}</div>', unsafe_allow_html=True)
                del st.session_state.login_error
            
            # Login form
            with st.form("login_form"):
                st.subheader("Login to Your Account")
                
                email = st.text_input("Email", placeholder="Enter your email")
                password = st.text_input("Password", type="password", placeholder="Enter your password")
                
                # Form submission button
                col1_btn, col2_btn = st.columns([1, 1])
                with col2_btn:
                    submitted = st.form_submit_button("Login", type="primary", use_container_width=True)
                
                if submitted:
                    if not email or not password:
                        st.error("Please fill in all fields")
                    else:
                        with st.spinner("Logging in..."):
                            result = login_user(email, password)
                            if result["success"]:
                                # Store user data in session
                                st.session_state.logged_in = True
                                st.session_state.user_data = result["data"]["user"]
                                st.session_state.current_page = "home"
                                st.rerun()
                            else:
                                st.session_state.login_error = result["error"]
                                st.rerun()
            
            st.markdown('</div>', unsafe_allow_html=True)
            
            # Signup button
            st.markdown("---")
            st.markdown('<div style="text-align: center; margin-bottom: 10px;">Don\'t have an account?</div>', unsafe_allow_html=True)
            col1_signup, col2_signup, col3_signup = st.columns([1, 2, 1])
            with col2_signup:
                if st.button("Sign Up", key="goto_signup", use_container_width=True):
                    st.session_state.current_page = "signup"
                    st.rerun()

def signup_page():
    """Render signup page"""
    # Center the title for signup page
    col1_title, col2_title, col3_title = st.columns([1, 2, 1])
    with col2_title:
        st.markdown('<div class="welcome-title">Join PulseBoard.ai</div>', unsafe_allow_html=True)
        st.markdown('<div class="welcome-subtitle">Create your account to get started</div>', unsafe_allow_html=True)
    
    # Create a container for the signup form
    with st.container():
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.markdown('<div class="signup-container">', unsafe_allow_html=True)
            
            # Display error message if any
            if 'signup_error' in st.session_state:
                st.markdown(f'<div class="error-message">{st.session_state.signup_error}</div>', unsafe_allow_html=True)
                del st.session_state.signup_error
            
            # Signup form
            with st.form("signup_form"):
                st.subheader("Create Your Account")
                
                # Two-column layout for first and last name
                col1_name, col2_name = st.columns(2)
                with col1_name:
                    first_name = st.text_input("First Name", placeholder="Enter your first name")
                with col2_name:
                    last_name = st.text_input("Last Name", placeholder="Enter your last name")
                
                email = st.text_input("Email", placeholder="Enter your email")
                
                # Password with confirmation
                col1_pw, col2_pw = st.columns(2)
                with col1_pw:
                    password = st.text_input("Password", type="password", placeholder="Create a password")
                with col2_pw:
                    confirm_password = st.text_input("Confirm Password", type="password", placeholder="Confirm your password")
                
                # Form submission button
                col1_btn, col2_btn = st.columns([1, 1])
                with col2_btn:
                    submitted = st.form_submit_button("Create Account", type="primary", use_container_width=True)
                
                if submitted:
                    # Validation
                    if not all([first_name, last_name, email, password, confirm_password]):
                        st.error("Please fill in all fields")
                    elif password != confirm_password:
                        st.error("Passwords do not match")
                    elif len(password) < 6:
                        st.error("Password must be at least 6 characters long")
                    else:
                        with st.spinner("Creating account..."):
                            result = create_user(email, first_name, last_name, password)
                            if result["success"]:
                                # Set success flag and redirect to login
                                st.session_state.signup_success = True
                                st.session_state.current_page = "login"
                                st.rerun()
                            else:
                                st.session_state.signup_error = result["error"]
                                st.rerun()
            
            st.markdown('</div>', unsafe_allow_html=True)
            
            # Back to Login button (below the form with same width)
            st.markdown('<div class="back-button-container">', unsafe_allow_html=True)
            col1_back, col2_back, col3_back = st.columns([1, 2, 1])
            with col2_back:
                if st.button("← Back to Login", use_container_width=True):
                    st.session_state.current_page = "login"
                    st.rerun()
            st.markdown('</div>', unsafe_allow_html=True)

def home_page():
    """Render home page"""
    # Get user_id from session
    user_id = st.session_state.user_data.get("user_id")
    user_name = st.session_state.user_data.get("name", "User")
    
    # Fetch user details from API
    if 'user_details' not in st.session_state:
        with st.spinner("Loading user details..."):
            user_result = get_user_details_from_api(user_id)
            if user_result["success"]:
                st.session_state.user_details = user_result["data"]
            else:
                st.error(f"Failed to load user details: {user_result['error']}")
    
    # Fetch recent projects
    if st.session_state.projects_data is None:
        with st.spinner("Loading recent projects..."):
            recent_result = get_recent_projects_from_api(user_id)
            if recent_result["success"]:
                st.session_state.projects_data = recent_result["data"]
            else:
                st.error(f"Failed to load recent projects: {recent_result['error']}")
    
    # Fetch all projects
    if st.session_state.all_projects is None:
        with st.spinner("Loading all projects..."):
            all_result = get_all_projects_from_api(user_id)
            if all_result["success"]:
                st.session_state.all_projects = all_result["data"]
            else:
                st.error(f"Failed to load projects: {all_result['error']}")
    
    # Sidebar
    with st.sidebar:
        st.markdown(f"### 👋 Welcome, {user_name.split()[0] if user_name else 'User'}!")
        
        # User info
        if 'user_details' in st.session_state:
            st.markdown("---")
            st.markdown("#### 📋 Account Info")
            st.write(f"**Email:** {st.session_state.user_details.get('email', 'N/A')}")
            st.write(f"**User ID:** {user_id}")
        
        # Quick actions
        st.markdown("---")
        st.markdown("#### 🎯 Quick Actions")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🔄 Refresh", use_container_width=True):
                st.session_state.projects_data = None
                st.session_state.all_projects = None
                st.rerun()
        
        with col2:
            if st.button("📊 New Project", use_container_width=True):
                st.info("Create Project feature coming soon!")
        
        st.markdown("---")
        
        # Logout button
        if st.button("🚪 Logout", type="secondary", use_container_width=True):
            st.session_state.logged_in = False
            st.session_state.user_data = None
            st.session_state.user_details = None
            st.session_state.projects_data = None
            st.session_state.all_projects = None
            st.session_state.current_page = "login"
            st.rerun()
    
    # Main content area
    # Welcome header
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown(f"# 👋 Welcome, {user_name}!")
        st.markdown(f"#### Your intelligent analytics dashboard")
    
    with col2:
        if st.session_state.all_projects:
            total_projects = len(st.session_state.all_projects)
            st.metric("Total Projects", total_projects)
    
    st.markdown("---")
    
    # Recent Activity Section
    st.markdown('<div class="section-title">📈 Recent Activity</div>', unsafe_allow_html=True)
    
    if st.session_state.projects_data:
        if len(st.session_state.projects_data) > 0:
            # Create columns for project cards
            cols = st.columns(min(3, len(st.session_state.projects_data)))
            
            for idx, project in enumerate(st.session_state.projects_data):
                with cols[idx % len(cols)]:
                    with st.container():
                        st.markdown(f"""
                            <div class="project-card">
                                <div class="project-name">{project.get('name_of_project', 'Unnamed Project')}</div>
                                <div class="project-domain">{project.get('domain', 'general').title()}</div>
                                <div class="project-time">
                                    <strong>Last Used:</strong> {format_timestamp(project.get('last_used_at', ''))}
                                </div>
                                <div class="project-time">
                                    <strong>Created:</strong> {format_timestamp(project.get('created_at', ''))}
                                </div>
                            </div>
                        """, unsafe_allow_html=True)
                        
                        # Project actions
                        project_id = project.get('project_id')
                        col1_action, col2_action = st.columns(2)
                        with col1_action:
                            if st.button("📂 Open", key=f"open_{project_id}", use_container_width=True):
                                with st.spinner("Opening project..."):
                                    # Update last used timestamp
                                    update_result = update_project_last_used(project_id)
                                    if update_result["success"]:
                                        st.success(f"Opening {project.get('name_of_project')}...")
                                        # Here you would navigate to the project page
                                        # For now, just refresh the project data
                                        st.session_state.projects_data = None
                                        st.rerun()
                                    else:
                                        st.error(f"Failed to open project: {update_result['error']}")
                        
                        with col2_action:
                            if st.button("⚙️ Settings", key=f"settings_{project_id}", use_container_width=True):
                                st.info(f"Settings for {project.get('name_of_project')} coming soon!")
        else:
            st.markdown("""
                <div class="empty-state">
                    <h3>No projects yet</h3>
                    <p>Create your first project to get started with analytics!</p>
                </div>
            """, unsafe_allow_html=True)
    else:
        st.info("Loading recent projects...")
    
    st.markdown("---")
    
    # All Projects Section
    st.markdown('<div class="section-title">📂 All Projects</div>', unsafe_allow_html=True)
    
    if st.session_state.all_projects:
        if len(st.session_state.all_projects) > 0:
            # Create a table for all projects
            for project in st.session_state.all_projects:
                with st.container():
                    col1, col2, col3 = st.columns([3, 2, 1])
                    
                    with col1:
                        st.markdown(f"**{project.get('name_of_project', 'Unnamed Project')}**")
                        st.caption(f"Domain: {project.get('domain', 'general').title()} • ID: {project.get('project_id')}")
                    
                    with col2:
                        st.caption(f"📅 Created: {format_timestamp(project.get('created_at', ''))}")
                        st.caption(f"⏰ Last Used: {format_timestamp(project.get('last_used_at', ''))}")
                    
                    with col3:
                        project_id = project.get('project_id')
                        if st.button("Open", key=f"open_all_{project_id}", use_container_width=True):
                            with st.spinner("Opening..."):
                                update_result = update_project_last_used(project_id)
                                if update_result["success"]:
                                    st.success(f"Opening project...")
                                    st.session_state.projects_data = None
                                    st.rerun()
                                else:
                                    st.error(f"Failed to open: {update_result['error']}")
                    
                    st.divider()
        else:
            st.markdown("""
                <div class="empty-state">
                    <h3>No projects found</h3>
                    <p>Start by creating your first analytics project!</p>
                </div>
            """, unsafe_allow_html=True)
    else:
        st.info("Loading all projects...")
    
    # Empty space at bottom
    st.markdown("<br><br>", unsafe_allow_html=True)

def main():
    """Main application router"""
    
    # Check if user is logged in
    if st.session_state.logged_in:
        home_page()
    else:
        # Show login or signup based on current page
        if st.session_state.current_page == "login":
            login_page()
        elif st.session_state.current_page == "signup":
            signup_page()
        else:
            st.session_state.current_page = "login"
            st.rerun()

if __name__ == "__main__":
    main()