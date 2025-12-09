import os
import sys
from datetime import datetime
from dotenv import load_dotenv

sys.path.append("../..")
from helpers.database.connection_to_db import connect_to_mongodb

# Load environment variables
load_dotenv()

# Database and collection names
MASTER_DB_NAME = "master"
CLIENT_CONFIG_COLLECTION_NAME = "client_config"

def get_next_project_id(user_id, client_config):
    """
    Generate the next project ID for a user
    If no projects exist, return UID001PJ001
    
    Args:
        user_id: User's ID (e.g., UID001)
        client_config: User's client config document
    
    Returns:
        str: Next project ID (e.g., UID001PJ001, UID001PJ002, etc.)
    """
    projects = client_config.get("projects", [])
    
    if not projects:
        return f"{user_id}PJ001"
    
    # Find the highest project number
    max_project_num = 0
    for project in projects:
        project_id = project.get("project_id", "")
        if "PJ" in project_id:
            try:
                project_num = int(project_id.split("PJ")[1])
                max_project_num = max(max_project_num, project_num)
            except (ValueError, IndexError):
                continue
    
    # Increment and format with leading zeros
    next_project_num = max_project_num + 1
    return f"{user_id}PJ{next_project_num:03d}"

def create_project_object(project_id, project_name, domain):
    """
    Create a project object with all required collections and timestamps
    
    Args:
        project_id: Generated project ID
        project_name: Name of the project
        domain: Domain of the project
    
    Returns:
        dict: Project object with all configurations
    """
    # Get current timestamp
    current_timestamp = datetime.utcnow()
    
    project_obj = {
        "project_id": project_id,
        "name_of_project": project_name,
        "domain": domain,
        "created_at": current_timestamp, 
        "last_used_at": current_timestamp, 
        "mongodb": {
            "collections": {
                f"{project_id}_charts": f"{project_id}_charts",
                f"{project_id}_cleaned_data": f"{project_id}_cleaned_data",
                f"{project_id}_cleaned_dt": f"{project_id}_cleaned_dt",
                f"{project_id}_data": f"{project_id}_data",
                f"{project_id}_data_type": f"{project_id}_data_type",
                f"{project_id}_weaviate_cd": f"{project_id}_weaviate_cd",
                f"{project_id}_weaviate_cdt": f"{project_id}_weaviate_cdt",
                f"{project_id}_weaviate_vectors_cd": f"{project_id}_weaviate_vectors_cd",
                f"{project_id}_weaviate_vectors_cdt": f"{project_id}_weaviate_vectors_cdt",
            }
        },
        "weaviate": {
            "collections": {
                f"{project_id}_weviate_cd": f"{project_id}_weviate_cd",
                f"{project_id}_weviate_cdt": f"{project_id}_weviate_cdt",

            }
        }
    }
    
    return project_obj

def create_mongodb_collections(user_id, project_id, mongodb_collections):
    """
    Create empty MongoDB collections for the project
    
    Args:
        user_id: User's ID (which is also the database name)
        project_id: Project ID
        mongodb_collections: Dictionary of collection names from project config
    
    Returns:
        dict: Dictionary with status and created collections list
    """
    mongo_client = connect_to_mongodb()
    if not mongo_client:
        raise Exception("Failed to connect to MongoDB")
    
    try:
        db = mongo_client[user_id]
        created_collections = []
        
        # Create each collection
        for key, collection_name in mongodb_collections.items():
            collection = db[collection_name]
            
            # Check if collection already exists
            if collection_name not in db.list_collection_names():
                # Insert and delete a dummy document to create the collection
                dummy_doc = {"_temp": "dummy"}
                result = collection.insert_one(dummy_doc)
                collection.delete_one({"_id": result.inserted_id})
                created_collections.append(collection_name)
                print(f"Created collection: {collection_name}")
            else:
                print(f"Collection already exists: {collection_name}")
                created_collections.append(collection_name)
        
        return {
            "status": "success",
            "created_collections": created_collections
        }
        
    except Exception as e:
        print(f"Error creating MongoDB collections: {e}")
        raise
    finally:
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
        
        # Create project object with timestamps
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
        mongo_client.close()

if __name__ == "__main__":
    # Get user input
    print("=== Add New Project ===")
    user_id = input("Enter User ID (e.g., UID001): ").strip()
    project_name = input("Enter Project Name: ").strip()
    domain = input("Enter Domain: ").strip()
    
    try:
        # Create project using run function
        result = run_project_creation(user_id, project_name, domain)
        
        # Display created project
        if result["status"] == "success":
            print("\n" + "="*50)
            print("Project Created Successfully!")
            print("="*50)
            print(f"Project ID: {result['project']['project_id']}")
            print(f"Project Name: {result['project']['name_of_project']}")
            print(f"Domain: {result['project']['domain']}")
            print(f"Created At: {result['project']['created_at']}")
            print(f"Last Used At: {result['project']['last_used_at']}")
            print(f"\nMongoDB Collections Created:")
            for collection in result['collections_created']:
                print(f"  ✓ {collection}")
            print(f"\nWeaviate Collections Configured:")
            for key, value in result['project']['weaviate']['collections'].items():
                print(f"  - {value}")
        elif result["status"] == "user_not_found":
            print("\nUser not found. Please create the user first.")
        else:
            print("\nFailed to create project.")
    except Exception as e:
        print(f"Error: {e}")