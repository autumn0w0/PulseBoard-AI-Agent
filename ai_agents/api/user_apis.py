import os
import sys
from dotenv import load_dotenv
from werkzeug.security import generate_password_hash,check_password_hash
sys.path.append("../..")
from helpers.database.connection_to_db import connect_to_mongodb
from helpers.logger import get_logger
from pipelines.registration.user_creation import get_next_user_id, add_client_config
from pipelines.registration.project_creation import get_next_project_id,create_project_object, create_mongodb_collections


# Load environment variables
load_dotenv()

# Database and collection names
MASTER_DB_NAME = "master"
USER_COLLECTION_NAME = "user"
CLIENT_CONFIG_COLLECTION_NAME = "client_config"

def run_user_creation(email, first_name, last_name, password):
    """
    Main function to create user and client config entries
    
    Args:
        email: User's email address
        first_name: User's first name
        last_name: User's last name
        password: User's password (will be hashed)
    
    Returns:
        dict: Dictionary containing user and client config documents
    """
    # Connect to MongoDB using existing connection function
    mongo_client = connect_to_mongodb()
    if not mongo_client:
        raise Exception("Failed to connect to MongoDB")
    
    try:
        # Get the master database and collections
        db = mongo_client[MASTER_DB_NAME]
        users_collection = db[USER_COLLECTION_NAME]
        client_config_collection = db[CLIENT_CONFIG_COLLECTION_NAME]
        
        # Check if user with this email already exists
        existing_user = users_collection.find_one({"email": email})
        if existing_user:
            print(f"User with email {email} already exists!")
            return {
                "user": existing_user,
                "client_config": None,
                "status": "user_already_exists"
            }
        
        # Generate next user ID
        user_id = get_next_user_id(users_collection)
        
        # Create full name
        full_name = f"{first_name} {last_name}"
        
        # Hash the password
        password_hash = generate_password_hash(password)
        
        # Create user document
        user_doc = {
            "user_id": user_id,
            "name": full_name,
            "email": email,
            "password": password_hash
        }
        
        # Insert user into database
        result = users_collection.insert_one(user_doc)
        user_doc['_id'] = result.inserted_id
        print(f"User created successfully with ID: {user_id}")
        
        # Create client config entry
        config_doc = add_client_config(user_id, client_config_collection)
        
        return {
            "user": user_doc,
            "client_config": config_doc,
            "status": "success"
        }
        
    except Exception as e:
        print(f"Error during user creation: {e}")
        raise
    finally:
        # Close MongoDB connection
        mongo_client.close()

def run_project_creation(user_id, project_name, domain):
    """
    Main function to add a project to user's configuration and create collections
    
    Args:
        user_id: User's ID (e.g., UID001)
        project_name: Name of the project
        domain: Domain of the project
    
    Returns:
        dict: Dictionary containing updated client config, project info, and collections
    """
    # Connect to MongoDB using existing connection function
    mongo_client = connect_to_mongodb()
    if not mongo_client:
        raise Exception("Failed to connect to MongoDB")
    
    try:
        # Get the master database and client_config collection
        db = mongo_client[MASTER_DB_NAME]
        client_config_collection = db[CLIENT_CONFIG_COLLECTION_NAME]
        
        # Find user's client config
        client_config = client_config_collection.find_one({"user_id": user_id})
        if not client_config:
            print(f"No client configuration found for user: {user_id}")
            return {
                "status": "user_not_found",
                "client_config": None,
                "project": None,
                "collections_created": None
            }
        
        # Generate next project ID
        project_id = get_next_project_id(user_id, client_config)
        
        # Create project object
        project_obj = create_project_object(project_id, project_name, domain)
        
        # Add project to the projects array
        result = client_config_collection.update_one(
            {"user_id": user_id},
            {"$push": {"projects": project_obj}}
        )
        
        if result.modified_count > 0:
            print(f"Project created successfully with ID: {project_id}")
            
            # Create MongoDB collections
            collections_result = create_mongodb_collections(
                user_id, 
                project_id, 
                project_obj['mongodb']['collections']
            )
            
            # Fetch updated client config
            updated_config = client_config_collection.find_one({"user_id": user_id})
            
            return {
                "status": "success",
                "client_config": updated_config,
                "project": project_obj,
                "collections_created": collections_result['created_collections']
            }
        else:
            print("Failed to add project")
            return {
                "status": "failed",
                "client_config": None,
                "project": None,
                "collections_created": None
            }
        
    except Exception as e:
        print(f"Error during project creation: {e}")
        raise
    finally:
        # Close MongoDB connection
        mongo_client.close()

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
        get_logger.info(f"Login attempt for email: {email}")
        
        # Connect to MongoDB
        client = connect_to_mongodb()
        if not client:
            get_logger.error("Database connection failed")
            return {
                "status": "failed",
                "message": "Database connection failed"
            }
        
        # Access the master database and user collection
        db = MASTER_DB_NAME 
        users_collection = USER_COLLECTION_NAME
        
        # Find user by email
        user = users_collection.find_one({"email": email})
        
        if not user:
            get_logger.warning(f"User not found: {email}")
            return {
                "status": "failed",
                "message": "Invalid email or password"
            }
        
        # Verify password using werkzeug's check_password_hash
        # This works with scrypt hashes (format: "scrypt:...")
        stored_password = user.get("password", "")
        
        if not check_password_hash(stored_password, password):
            get_logger.warning(f"Invalid password attempt for user: {email}")
            return {
                "status": "failed",
                "message": "Invalid email or password"
            }
        
        # Login successful - prepare user data
        get_logger.info(f"Login successful for user: {user.get('user_id')}")
        
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
        get_logger.error(f"Error during login: {str(e)}")
        return {
            "status": "failed",
            "message": f"Login error: {str(e)}"
        }