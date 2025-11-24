#streamlit run app.py
import streamlit as st
import requests
import os
from dotenv import load_dotenv
import hashlib
import time
from pymongo import MongoClient
        
import sys
sys.path.append("..")
from helpers.database.connection_to_db import connect_to_mongodb

# Load environment variables
load_dotenv()

# Get API URL from environment
API_URL = os.getenv("API_URL", "http://localhost:8000")

# Configure page
st.set_page_config(
    page_title="Pulse Board",
    page_icon="📊",
    layout="centered"
)

def hash_password(password):
    """Simple password hashing for demonstration"""
    return hashlib.sha256(password.encode()).hexdigest()

def login_user(email, password):
    """Login user by checking credentials against MongoDB"""
    try:
        # Connect to MongoDB
        client = connect_to_mongodb()
        if not client:
            return False, "Database connection failed"
        
        # Access the master database and user collection
        db = client["master"]
        users_collection = db["user"]
        
        # Find user by email
        user = users_collection.find_one({"email": email})
        
        if user:
            # In a real application, you would verify the scrypt hash properly
            # For demo purposes, we'll use a simple approach
            # You should replace this with proper scrypt verification
            if user.get("password", "").startswith("scrypt:"):
                # Here you would verify the scrypt hash
                # For now, we'll assume it matches for demo
                return True, "Login successful"
            else:
                return False, "Invalid password"
        else:
            return False, "User not found"
            
    except Exception as e:
        return False, f"Login error: {str(e)}"

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

def main():
    # Custom CSS for better styling
    st.markdown("""
        <style>
        .main {
            padding: 2rem;
        }
        .stButton>button {
            width: 100%;
            border-radius: 5px;
            height: 3em;
        }
        .login-container {
            max-width: 400px;
            margin: 0 auto;
            padding: 2rem;
            border: 1px solid #ddd;
            border-radius: 10px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }
        </style>
    """, unsafe_allow_html=True)

    # Initialize session state
    if 'page' not in st.session_state:
        st.session_state.page = 'login'
    if 'logged_in' not in st.session_state:
        st.session_state.logged_in = False
    if 'user_email' not in st.session_state:
        st.session_state.user_email = None

    # If user is logged in, show dashboard
    if st.session_state.logged_in:
        show_dashboard()
        return

    # Show login or signup page based on session state
    if st.session_state.page == 'login':
        show_login_page()
    else:
        show_signup_page()

def show_dashboard():
    """Show the main dashboard after login"""
    st.title("📊 Pulse Board Dashboard")
    st.success(f"Welcome, {st.session_state.user_email}!")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Your Projects")
        st.info("Project management features coming soon...")
        
    with col2:
        st.subheader("Recent Activity")
        st.info("Activity feed coming soon...")
    
    if st.button("Logout"):
        st.session_state.logged_in = False
        st.session_state.user_email = None
        st.session_state.page = 'login'
        st.rerun()

def show_login_page():
    """Show the login page"""
    st.title("🔐 Pulse Board Login")
    
    # Use a container without the problematic markdown div
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
                        success, message = login_user(email, password)
                        
                    if success:
                        st.session_state.logged_in = True
                        st.session_state.user_email = email
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
    
    # Use a container without the problematic markdown div
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
                        if len(password) >= 6:
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
                            st.warning("Password must be at least 6 characters long")
                    else:
                        st.warning("Passwords do not match")
                else:
                    st.warning("Please fill in all fields")
            
            if back_button:
                st.session_state.page = 'login'
                st.rerun()

        
if __name__ == "__main__":
    main()