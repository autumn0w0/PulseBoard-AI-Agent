# File: ai_agents/registration/user_login.py

from werkzeug.security import check_password_hash
import sys
sys.path.append("../..")
from helpers.database.connection_to_db import connect_to_mongodb
from helpers.logger import get_logger

logger = get_logger(__name__)

def run_user_login(email: str, password: str) -> dict:
    """
    Authenticate user with email and password.
    
    Args:
        email (str): User's email address
        password (str): User's password (plain text)
    
    Returns:
        dict: Response with status and user data
            Success: {
                "status": "success",
                "message": "Login successful",
                "user": {...}
            }
            Failure: {
                "status": "failed",
                "message": "Error description"
            }
    """
    try:
        logger.info(f"Login attempt for email: {email}")
        
        # Connect to MongoDB
        client = connect_to_mongodb()
        if not client:
            logger.error("Database connection failed")
            return {
                "status": "failed",
                "message": "Database connection failed"
            }
        
        # Access the master database and user collection
        db = client["master"]
        users_collection = db["user"]
        
        # Find user by email
        user = users_collection.find_one({"email": email})
        
        if not user:
            logger.warning(f"User not found: {email}")
            return {
                "status": "failed",
                "message": "Invalid email or password"
            }
        
        # Verify password using werkzeug's check_password_hash
        # This works with scrypt hashes (format: "scrypt:...")
        stored_password = user.get("password", "")
        
        if not check_password_hash(stored_password, password):
            logger.warning(f"Invalid password attempt for user: {email}")
            return {
                "status": "failed",
                "message": "Invalid email or password"
            }
        
        # Login successful - prepare user data
        logger.info(f"Login successful for user: {user.get('user_id')}")
        
        # Remove sensitive information
        user_data = {
            "user_id": user.get("user_id"),
            "email": user.get("email"),
            "first_name": user.get("first_name"),
            "last_name": user.get("last_name"),
            "created_at": user.get("created_at")
        }
        
        # Include _id if present (will be converted to string in endpoint)
        if "_id" in user:
            user_data["_id"] = user["_id"]
        
        return {
            "status": "success",
            "message": "Login successful",
            "user": user_data
        }
        
    except Exception as e:
        logger.error(f"Error during login: {str(e)}")
        return {
            "status": "failed",
            "message": f"Login error: {str(e)}"
        }