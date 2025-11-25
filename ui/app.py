#streamlit run app.py
import streamlit as st
import requests
import os
from dotenv import load_dotenv
import time
        
import sys
sys.path.append("..")

# Load environment variables
load_dotenv()

# Get API URL from environment
API_URL = os.getenv("API_URL", "http://localhost:8000")

# Configure page
st.set_page_config(
    page_title="Pulse Board",
    page_icon="📊",
    layout="wide"
)

def login_user(email, password):
    """Login user via API"""
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
            return True, data.get("message", "Login successful"), data.get("user")
        elif response.status_code == 401:
            return False, "Invalid email or password", None
        else:
            return False, f"Login failed: {response.text}", None
            
    except requests.exceptions.RequestException as e:
        return False, f"Connection error: {str(e)}", None

def register_user(email, first_name, last_name, password):
    """Register new user via API"""
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
            return True, "Registration successful!"
        elif response.status_code == 409:
            return False, "User already exists"
        else:
            return False, f"Registration failed: {response.text}"
            
    except requests.exceptions.RequestException as e:
        return False, f"Connection error: {str(e)}"

def create_project(user_id, project_name, domain):
    """Create new project via API"""
    try:
        response = requests.post(
            f"{API_URL}/create-project",
            json={
                "user_id": user_id,
                "project_name": project_name,
                "domain": domain
            }
        )
        
        if response.status_code == 200:
            data = response.json()
            return True, "Project created successfully!", data
        elif response.status_code == 404:
            return False, "User not found", None
        else:
            return False, f"Project creation failed: {response.text}", None
            
    except requests.exceptions.RequestException as e:
        return False, f"Connection error: {str(e)}", None

def main():
    # Custom CSS for better styling
    st.markdown("""
        <style>
        .main {
            padding: 1rem;
        }
        .stButton>button {
            width: 100%;
            border-radius: 5px;
            height: 3em;
        }
        .project-card {
            padding: 1.5rem;
            border: 1px solid #ddd;
            border-radius: 10px;
            margin-bottom: 1rem;
            background-color: #f9f9f9;
        }
        .section-header {
            font-size: 1.5rem;
            font-weight: bold;
            margin-top: 2rem;
            margin-bottom: 1rem;
        }
        </style>
    """, unsafe_allow_html=True)

    # Initialize session state
    if 'page' not in st.session_state:
        st.session_state.page = 'login'
    if 'logged_in' not in st.session_state:
        st.session_state.logged_in = False
    if 'user_data' not in st.session_state:
        st.session_state.user_data = None
    if 'current_view' not in st.session_state:
        st.session_state.current_view = 'dashboard'

    # If user is logged in, show appropriate view
    if st.session_state.logged_in:
        if st.session_state.current_view == 'dashboard':
            show_dashboard()
        elif st.session_state.current_view == 'new_project':
            show_new_project_page()
        return

    # Show login or signup page based on session state
    if st.session_state.page == 'login':
        show_login_page()
    else:
        show_signup_page()

def show_dashboard():
    """Show the main dashboard after login"""
    user = st.session_state.user_data
    
    # Sidebar with logout button
    with st.sidebar:
        st.title("Menu")
        st.write("---")
        if st.button("🚪 Logout", use_container_width=True):
            st.session_state.logged_in = False
            st.session_state.user_data = None
            st.session_state.page = 'login'
            st.session_state.current_view = 'dashboard'
            st.rerun()
    
    # Main content
    if user:
        st.title(f"Welcome, {user.get('first_name', '')} {user.get('last_name', '')}! 👋")
    
    st.write("---")
    
    # New Project Button
    if st.button("➕ Create New Project", use_container_width=True, type="primary"):
        st.session_state.current_view = 'new_project'
        st.rerun()
    
    st.write("")
    
    # Recent Activity Section
    st.markdown('<div class="section-header">📌 Recent Activity</div>', unsafe_allow_html=True)
    
    # Placeholder for recent projects (3 at a time)
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        <div class="project-card">
            <h4>Project 1</h4>
            <p>Domain: Healthcare</p>
            <p>Last updated: 2 days ago</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="project-card">
            <h4>Project 2</h4>
            <p>Domain: Finance</p>
            <p>Last updated: 5 days ago</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div class="project-card">
            <h4>Project 3</h4>
            <p>Domain: E-commerce</p>
            <p>Last updated: 1 week ago</p>
        </div>
        """, unsafe_allow_html=True)
    
    st.write("")
    
    # All Projects Section
    st.markdown('<div class="section-header">📂 All Projects</div>', unsafe_allow_html=True)
    
    # Placeholder for all projects
    for i in range(1, 6):
        col1, col2, col3, col4 = st.columns([3, 2, 2, 1])
        with col1:
            st.write(f"**Project {i}**")
        with col2:
            st.write("Domain: Various")
        with col3:
            st.write(f"Created: {i} weeks ago")
        with col4:
            if st.button("Open", key=f"open_{i}"):
                st.info(f"Opening Project {i}...")
        st.write("---")

def show_new_project_page():
    """Show the new project creation page"""
    user = st.session_state.user_data
    
    # Sidebar with back button
    with st.sidebar:
        st.title("Menu")
        st.write("---")
        if st.button("⬅️ Back to Dashboard", use_container_width=True):
            st.session_state.current_view = 'dashboard'
            st.rerun()
        st.write("---")
        if st.button("🚪 Logout", use_container_width=True):
            st.session_state.logged_in = False
            st.session_state.user_data = None
            st.session_state.page = 'login'
            st.session_state.current_view = 'dashboard'
            st.rerun()
    
    st.title("Create New Project")
    st.write("Fill in the details to create a new project")
    st.write("---")
    
    with st.form("new_project_form"):
        project_name = st.text_input("📝 Project Name", placeholder="Enter project name")
        
        domain = st.selectbox(
            "🏢 Domain",
            options=[
                "Healthcare",
                "Finance",
                "E-commerce",
                "Education",
                "Manufacturing",
                "Retail",
                "Technology",
                "Other"
            ]
        )
        
        # If "Other" is selected, show text input
        if domain == "Other":
            domain = st.text_input("Enter custom domain", placeholder="Enter your domain")
        
        st.write("")
        col1, col2, col3 = st.columns([1, 1, 2])
        
        with col1:
            submit_button = st.form_submit_button("Create Project", type="primary")
        with col2:
            cancel_button = st.form_submit_button("Cancel")
        
        if submit_button:
            if project_name and domain:
                with st.spinner("Creating project..."):
                    user_id = user.get('user_id')
                    success, message, project_data = create_project(user_id, project_name, domain)
                
                if success:
                    st.success(message)
                    st.write("---")
                    st.subheader("Project Created Successfully! 🎉")
                    
                    if project_data:
                        project_info = project_data.get('project', {})
                        st.write(f"**Project ID:** {project_info.get('project_id', 'N/A')}")
                        st.write(f"**Project Name:** {project_info.get('project_name', 'N/A')}")
                        st.write(f"**Domain:** {project_info.get('domain', 'N/A')}")
                    
                    st.write("---")
                    st.info("Next steps: Upload data and start processing")
                    
                    # Placeholder buttons for next steps
                    col1, col2 = st.columns(2)
                    with col1:
                        if st.button("📤 Upload Data", use_container_width=True):
                            st.info("Upload data feature coming soon...")
                    with col2:
                        if st.button("⚙️ Start Data Processing", use_container_width=True):
                            st.info("Data processing feature coming soon...")
                else:
                    st.error(message)
            else:
                st.warning("Please fill in all required fields")
        
        if cancel_button:
            st.session_state.current_view = 'dashboard'
            st.rerun()

def show_login_page():
    """Show the login page"""
    st.title("🔐 Pulse Board Login")
    
    with st.container():
        with st.form("login_form"):
            email = st.text_input("📧 Email", placeholder="Enter your email")
            password = st.text_input("🔒 Password", type="password", placeholder="Enter your password")
            
            col1, col2 = st.columns([3, 1])
            with col1:
                login_button = st.form_submit_button("Login")
            with col2:
                signup_button = st.form_submit_button("Sign Up")
            
            if login_button:
                if email and password:
                    with st.spinner("Logging in..."):
                        success, message, user_data = login_user(email, password)
                        
                    if success:
                        st.session_state.logged_in = True
                        st.session_state.user_data = user_data
                        st.session_state.current_view = 'dashboard'
                        st.success(message)
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error(message)
                else:
                    st.warning("Please fill in all fields")
            
            if signup_button:
                st.session_state.page = 'signup'
                st.rerun()

def show_signup_page():
    """Show the signup page"""
    st.title("👤 Create Account")
    
    with st.container():
        with st.form("signup_form"):
            col1, col2 = st.columns(2)
            with col1:
                first_name = st.text_input("First Name", placeholder="Enter first name")
            with col2:
                last_name = st.text_input("Last Name", placeholder="Enter last name")
            
            email = st.text_input("📧 Email", placeholder="Enter your email")
            password = st.text_input("🔒 Password", type="password", placeholder="Create a password")
            confirm_password = st.text_input("✅ Confirm Password", type="password", placeholder="Confirm your password")
            
            col1, col2 = st.columns([3, 1])
            with col1:
                signup_button = st.form_submit_button("Create Account")
            with col2:
                back_button = st.form_submit_button("Back to Login")
            
            if signup_button:
                if all([first_name, last_name, email, password, confirm_password]):
                    if password == confirm_password:
                        if len(password) >= 8:
                            with st.spinner("Creating account..."):
                                success, message = register_user(email, first_name, last_name, password)
                                
                            if success:
                                st.success(message)
                                st.info("Redirecting to login page...")
                                time.sleep(2)
                                st.session_state.page = 'login'
                                st.rerun()
                            else:
                                st.error(message)
                        else:
                            st.warning("Password must be at least 8 characters long")
                    else:
                        st.warning("Passwords do not match")
                else:
                    st.warning("Please fill in all fields")
            
            if back_button:
                st.session_state.page = 'login'
                st.rerun()

if __name__ == "__main__":
    main()