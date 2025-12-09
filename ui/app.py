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
if 'creating_project' not in st.session_state:
    st.session_state.creating_project = False

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

def delete_project_from_api(user_id: str, project_id: str):
    """Delete project via API"""
    try:
        response = requests.delete(
            f"{API_URL}/delete-project",
            json={
                "user_id": user_id,
                "project_id": project_id
            }
        )
        
        if response.status_code == 200:
            data = response.json()
            if data.get("status") == "success":
                return {"success": True, "data": data}
            else:
                return {"success": False, "error": data.get("message", "Failed to delete project")}
        else:
            return {"success": False, "error": f"Failed with status code: {response.status_code}"}
            
    except requests.exceptions.ConnectionError:
        return {"success": False, "error": "Cannot connect to server."}
    except Exception as e:
        return {"success": False, "error": str(e)}
    
def create_project_page():
    """Render create project page"""
    
    # Get user info from session
    user_id = st.session_state.user_data.get("user_id")
    user_name = st.session_state.user_data.get("name", "User")
    
    # Track project creation and upload states
    if 'project_created' not in st.session_state:
        st.session_state.project_created = False
    if 'created_project_data' not in st.session_state:
        st.session_state.created_project_data = None
    if 'uploading_data' not in st.session_state:
        st.session_state.uploading_data = False
    if 'upload_complete' not in st.session_state:
        st.session_state.upload_complete = False
    if 'upload_result' not in st.session_state:
        st.session_state.upload_result = None
    
    # Back button in sidebar
    with st.sidebar:
        st.markdown(f"### Creating New Project")
        st.markdown(f"**User:** {user_name}")
        st.markdown(f"**User ID:** {user_id}")
        
        st.markdown("---")
        
        if st.button("← Back to Dashboard", use_container_width=True):
            # Reset all states
            st.session_state.creating_project = False
            st.session_state.project_created = False
            st.session_state.created_project_data = None
            st.session_state.uploading_data = False
            st.session_state.upload_complete = False
            st.session_state.upload_result = None
            st.rerun()
    
    # Main content
    st.markdown(f"# 📊 Create New Project")
    st.markdown("---")
    
    # Show upload section if project was created
    if st.session_state.project_created and st.session_state.created_project_data:
        project_name_created = st.session_state.created_project_data.get("project_name")
        project_id = st.session_state.created_project_data.get("project_id")
        
        st.success(f"✅ Project '{project_name_created}' created successfully!")
        st.info(f"**Project ID:** {project_id}")
        
        # Show upload form
        st.markdown("---")
        st.markdown("### 📁 Upload Your Data")
        st.markdown("Upload your dataset to start analyzing. Supported formats: CSV, Excel (XLS/XLSX), JSON")
        
        if not st.session_state.upload_complete:
            # Upload form
            with st.form("upload_data_form"):
                uploaded_file = st.file_uploader(
                    "Choose a file",
                    type=["csv", "xlsx", "xls", "json"],
                    help="Upload CSV, Excel, or JSON files"
                )
                
                # File type selection
                if uploaded_file:
                    filename = uploaded_file.name.lower()
                    if filename.endswith('.csv'):
                        default_type = "csv"
                    elif filename.endswith(('.xlsx', '.xls')):
                        default_type = "excel"
                    elif filename.endswith('.json'):
                        default_type = "json"
                    else:
                        default_type = "auto"
                    
                    file_type = st.selectbox(
                        "File Type",
                        options=["auto", "csv", "excel", "json"],
                        index=0 if default_type == "auto" else ["auto", "csv", "excel", "json"].index(default_type),
                        help="Auto-detect or manually select file type"
                    )
                
                col1, col2, col3 = st.columns([1, 2, 1])
                with col2:
                    upload_submitted = st.form_submit_button(
                        "📤 Upload Data",
                        type="primary",
                        use_container_width=True,
                        disabled=not uploaded_file
                    )
                
                if upload_submitted and uploaded_file:
                    st.session_state.uploading_data = True
                    
                    with st.spinner("Uploading and processing data..."):
                        try:
                            # Prepare form data
                            files = {"file": (uploaded_file.name, uploaded_file.getvalue())}
                            data = {
                                "user_id": user_id,
                                "file_type": file_type
                            }
                            
                            # Call upload API
                            response = requests.post(
                                f"{API_URL}/upload-data/{project_id}",
                                files=files,
                                data=data
                            )
                            
                            if response.status_code == 200:
                                upload_data = response.json()
                                if upload_data.get("status") == "success":
                                    st.session_state.upload_complete = True
                                    st.session_state.upload_result = upload_data
                                    st.rerun()
                                else:
                                    st.error(f"❌ Upload failed: {upload_data.get('message')}")
                            else:
                                st.error(f"❌ Upload failed with status: {response.status_code}")
                                
                        except requests.exceptions.ConnectionError:
                            st.error("❌ Cannot connect to server.")
                        except Exception as e:
                            st.error(f"❌ Error uploading file: {str(e)}")
        
        # Show upload success
        if st.session_state.upload_complete and st.session_state.upload_result:
            records_inserted = st.session_state.upload_result.get("records_inserted", 0)
            columns = st.session_state.upload_result.get("columns", [])
            
            st.success(f"✅ Data uploaded successfully! ({records_inserted} records)")
            
            # Show data preview
            with st.expander("📊 Data Preview", expanded=True):
                if columns:
                    st.write(f"**Columns:** {', '.join(columns)}")
                
                sample_data = st.session_state.upload_result.get("sample_data", [])
                if sample_data:
                    st.write("**Sample data:**")
                    st.json(sample_data[:3])  # Show first 3 records
            
            # Next steps
            st.markdown("---")
            st.markdown("### 🚀 Ready to Analyze!")
            st.markdown("""
            Your data has been uploaded and is ready for analysis. You can now:
            
            1. **Run Data Processing** - Automatically analyze data types and structure
            2. **Generate Insights** - Get AI-powered insights from your data
            3. **Create Charts** - Visualize your data with smart chart suggestions
            """)
            
            # Action buttons
            col1_action, col2_action, col3_action = st.columns(3)
            with col1_action:
                if st.button("🏠 Go to Dashboard", use_container_width=True):
                    # Reset all states
                    st.session_state.creating_project = False
                    st.session_state.project_created = False
                    st.session_state.created_project_data = None
                    st.session_state.uploading_data = False
                    st.session_state.upload_complete = False
                    st.session_state.upload_result = None
                    st.session_state.projects_data = None
                    st.session_state.all_projects = None
                    st.rerun()
            
            with col2_action:
                if st.button("🔍 Run Data Processing", type="primary", use_container_width=True):
                    st.info("Data processing pipeline coming soon!")
                    # Here you would call the data processing pipeline
            
            with col3_action:
                if st.button("➕ Upload More Data", use_container_width=True):
                    st.session_state.uploading_data = False
                    st.session_state.upload_complete = False
                    st.session_state.upload_result = None
                    st.rerun()
        
        return  # Don't show the project creation form
    
    # Create project form (only shown if no project was just created)
    with st.form("create_project_form"):
        st.subheader("Project Details")
        
        # Project name
        project_name = st.text_input(
            "Project Name *",
            placeholder="Enter a descriptive name for your project",
            help="e.g., Sales Analysis 2024, Customer Behavior Dashboard"
        )
        
        # Domain selection
        domain_options = [
            "finance", "healthcare", "ecommerce", "education", 
            "entertainment", "technology", "marketing", "manufacturing",
            "logistics", "retail", "telecom", "energy", "other"
        ]
        
        domain = st.selectbox(
            "Domain *",
            options=domain_options,
            help="Select the primary domain for your data analysis"
        )
        
        # Form submission
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            submitted = st.form_submit_button(
                "Create Project",
                type="primary",
                use_container_width=True
            )
        
        if submitted:
            # Validation
            if not project_name or not project_name.strip():
                st.error("Please enter a project name")
            elif not domain:
                st.error("Please select a domain")
            else:
                with st.spinner("Creating project..."):
                    try:
                        # Call create project API
                        response = requests.post(
                            f"{API_URL}/create-project",
                            json={
                                "user_id": user_id,
                                "project_name": project_name.strip(),
                                "domain": domain
                            }
                        )
                        
                        if response.status_code == 200:
                            data = response.json()
                            if data.get("status") == "success":
                                project_id = data.get("project", {}).get("project_id")
                                project_name_created = data.get("project", {}).get("name_of_project")
                                
                                # Store success data in session
                                st.session_state.project_created = True
                                st.session_state.created_project_data = {
                                    "project_id": project_id,
                                    "project_name": project_name_created
                                }
                                st.rerun()
                                
                            else:
                                error_msg = data.get("message", "Failed to create project")
                                st.error(f"❌ {error_msg}")
                        elif response.status_code == 404:
                            st.error("❌ User not found. Please log in again.")
                        elif response.status_code == 409:
                            st.error("❌ A project with this name already exists.")
                        else:
                            st.error(f"❌ Failed to create project (Status: {response.status_code})")
                            
                    except requests.exceptions.ConnectionError:
                        st.error("❌ Cannot connect to server. Please make sure the API server is running.")
                    except Exception as e:
                        st.error(f"❌ Error creating project: {str(e)}")

def create_project_via_api(user_id: str, project_name: str, domain: str):
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
            if data.get("status") == "success":
                return {"success": True, "data": data}
            elif data.get("status") == "user_not_found":
                return {"success": False, "error": "User not found"}
            else:
                return {"success": False, "error": data.get("message", "Failed to create project")}
        elif response.status_code == 404:
            return {"success": False, "error": "User not found"}
        else:
            return {"success": False, "error": f"Failed with status code: {response.status_code}"}
            
    except requests.exceptions.ConnectionError:
        return {"success": False, "error": "Cannot connect to server."}
    except Exception as e:
        return {"success": False, "error": str(e)}
    
def upload_data_to_project(project_id: str, user_id: str, file, file_type: str = "auto"):
    """Upload data file to project via API"""
    try:
        # Prepare form data
        files = {"file": (file.name, file.getvalue())}
        data = {
            "user_id": user_id,
            "file_type": file_type
        }
        
        response = requests.post(
            f"{API_URL}/upload-data/{project_id}",
            files=files,
            data=data
        )
        
        if response.status_code == 200:
            data = response.json()
            if data.get("status") == "success":
                return {"success": True, "data": data}
            else:
                return {"success": False, "error": data.get("message", "Upload failed")}
        else:
            return {"success": False, "error": f"Upload failed with status: {response.status_code}"}
            
    except requests.exceptions.ConnectionError:
        return {"success": False, "error": "Cannot connect to server."}
    except Exception as e:
        return {"success": False, "error": str(e)}

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
    
    # Sidebar - Simplified (removed quick actions)
    with st.sidebar:
        st.markdown(f"### 👋 Welcome, {user_name.split()[0] if user_name else 'User'}!")
        
        # User info
        if 'user_details' in st.session_state:
            st.markdown("---")
            st.markdown("#### 📋 Account Info")
            st.write(f"**Email:** {st.session_state.user_details.get('email', 'N/A')}")
            st.write(f"**User ID:** {user_id}")
        
        st.markdown("---")
        
        # Only logout button remains
        if st.button("🚪 Logout", type="secondary", use_container_width=True):
            st.session_state.logged_in = False
            st.session_state.user_data = None
            st.session_state.user_details = None
            st.session_state.projects_data = None
            st.session_state.all_projects = None
            st.session_state.current_page = "login"
            st.rerun()
    
    # Main content area
    # Welcome header with New Project button
    col1, col2, col3 = st.columns([3, 1, 1])
    with col1:
        st.markdown(f"# 👋 Welcome, {user_name}!")
        st.markdown(f"#### Your intelligent analytics dashboard")
    
    with col2:
        if st.session_state.all_projects:
            total_projects = len(st.session_state.all_projects)
            st.metric("Total Projects", total_projects)
    
    with col3:
        if st.button("📊 New Project", type="primary", use_container_width=True):
            st.session_state.creating_project = True
            st.rerun()
    
    st.markdown("---")
    
    # ... rest of the home_page function remains the same ...
    
    # Recent Activity Section - Changed to list view
    st.markdown('<div class="section-title">📈 Recent Activity</div>', unsafe_allow_html=True)
    
    if st.session_state.projects_data:
        if len(st.session_state.projects_data) > 0:
            # Show up to 3 most recent projects in list view
            recent_to_show = st.session_state.projects_data[:3]
            
            for project in recent_to_show:
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
                        col_open, col_delete = st.columns(2)
                        with col_open:
                            if st.button("📂", key=f"open_recent_{project_id}", help="Open Project", use_container_width=True):
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
                        
                        with col_delete:
                            if st.button("🗑️", key=f"delete_recent_{project_id}", help="Delete Project", use_container_width=True):
                                # Show confirmation dialog
                                if st.session_state.get(f"confirm_delete_{project_id}", False):
                                    with st.spinner("Deleting project..."):
                                        delete_result = delete_project_from_api(user_id, project_id)
                                        if delete_result["success"]:
                                            st.success(f"Project '{project.get('name_of_project')}' deleted successfully!")
                                            # Refresh data
                                            st.session_state.projects_data = None
                                            st.session_state.all_projects = None
                                            st.rerun()
                                        else:
                                            st.error(f"Failed to delete: {delete_result['error']}")
                                else:
                                    st.session_state[f"confirm_delete_{project_id}"] = True
                                    st.warning(f"Click again to confirm deletion of '{project.get('name_of_project')}'")
                                    st.rerun()
                    
                    st.divider()
        else:
            st.markdown("""
                <div class="empty-state">
                    <h3>No projects yet</h3>
                    <p>Create your first project to get started with analytics!</p>
                </div>
            """, unsafe_allow_html=True)
    else:
        st.info("No recent projects...")
    
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
                        col_open, col_delete = st.columns(2)
                        with col_open:
                            if st.button("📂", key=f"open_all_{project_id}", help="Open Project", use_container_width=True):
                                with st.spinner("Opening..."):
                                    update_result = update_project_last_used(project_id)
                                    if update_result["success"]:
                                        st.success(f"Opening project...")
                                        st.session_state.projects_data = None
                                        st.rerun()
                                    else:
                                        st.error(f"Failed to open: {update_result['error']}")
                        
                        with col_delete:
                            if st.button("🗑️", key=f"delete_all_{project_id}", help="Delete Project", use_container_width=True):
                                # Show confirmation dialog
                                if st.session_state.get(f"confirm_delete_all_{project_id}", False):
                                    with st.spinner("Deleting project..."):
                                        delete_result = delete_project_from_api(user_id, project_id)
                                        if delete_result["success"]:
                                            st.success(f"Project '{project.get('name_of_project')}' deleted successfully!")
                                            # Refresh data
                                            st.session_state.projects_data = None
                                            st.session_state.all_projects = None
                                            st.rerun()
                                        else:
                                            st.error(f"Failed to delete: {delete_result['error']}")
                                else:
                                    st.session_state[f"confirm_delete_all_{project_id}"] = True
                                    st.warning(f"Click again to confirm deletion of '{project.get('name_of_project')}'")
                                    st.rerun()
                    
                    st.divider()
        else:
            st.markdown("""
                <div class="empty-state">
                    <h3>No projects found</h3>
                    <p>Start by creating your first analytics project!</p>
                </div>
            """, unsafe_allow_html=True)
    else:
        st.info("No projects aviable...")
    
    # Empty space at bottom
    st.markdown("<br><br>", unsafe_allow_html=True)

def main():
    """Main application router"""
    
    # Check if user is logged in
    if st.session_state.logged_in:
        if st.session_state.creating_project:
            create_project_page()
        else:
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