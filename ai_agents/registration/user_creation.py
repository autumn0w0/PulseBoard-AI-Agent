import os
import sys
from dotenv import load_dotenv
from werkzeug.security import generate_password_hash
sys.path.append("../../")
from helpers.database.connection_to_db import connect_to_mongodb

# Load environment variables
load_dotenv()

# Database and collection names
MASTER_DB_NAME = "master"
USER_COLLECTION_NAME = "user"
CLIENT_CONFIG_COLLECTION_NAME = "client_config"

def get_next_user_id(users_collection):
    """
    Generate the next user ID by finding the highest existing ID
    If no users exist, return UID001
    
    Args:
        users_collection: MongoDB collection object
    
    Returns:
        str: Next user ID (e.g., UID001, UID002, etc.)
    """
    # Find the user with the highest user_id
    last_user = users_collection.find_one(
        sort=[("user_id", -1)]
    )
    
    if last_user is None:
        # No users exist, start with UID001
        return "UID001"
    
    # Extract the numeric part from the last user_id (e.g., "UID005" -> 5)
    last_id = last_user['user_id']
    numeric_part = int(last_id.replace("UID", ""))
    
    # Increment and format with leading zeros
    next_id = numeric_part + 1
    return f"UID{next_id:03d}"

def add_client_config(user_id, client_config_collection):
    """
    Add a client configuration entry for the user
    
    Args:
        user_id: User's ID (e.g., UID001)
        client_config_collection: MongoDB collection object
    
    Returns:
        dict: The created client config document
    """
    # Create client config document
    config_doc = {
        "user_id": user_id,
        "db_name": user_id
    }
    
    # Insert into database
    result = client_config_collection.insert_one(config_doc)
    config_doc['_id'] = result.inserted_id
    
    print(f"Client config created for user: {user_id}")
    
    return config_doc

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

if __name__ == "__main__":
    # Get user input
    print("=== Add New User ===")
    email = input("Enter email: ").strip()
    first_name = input("Enter first name: ").strip()
    last_name = input("Enter last name: ").strip()
    
    # Get password (using getpass for security - hides input)
    import getpass
    password = getpass.getpass("Enter password: ").strip()
    password_confirm = getpass.getpass("Confirm password: ").strip()
    
    # Validate password
    if password != password_confirm:
        print("Error: Passwords do not match!")
        sys.exit(1)
    
    if len(password) < 8:
        print("Error: Password must be at least 8 characters long!")
        sys.exit(1)
    
    try:
        # Create user and client config using run function
        result = run_user_creation(email, first_name, last_name, password)
        
        # Display created entries
        if result["status"] == "success":
            print("\n" + "="*50)
            print("User Created Successfully!")
            print("="*50)
            print(f"User ID: {result['user']['user_id']}")
            print(f"Name: {result['user']['name']}")
            print(f"Email: {result['user']['email']}")
            print(f"\nClient Config:")
            print(f"User ID: {result['client_config']['user_id']}")
            print(f"DB Name: {result['client_config']['db_name']}")
        elif result["status"] == "user_already_exists":
            print("\nUser already exists. No new entries created.")
    except Exception as e:
        print(f"Error: {e}")