import os
import sys
from dotenv import load_dotenv
from werkzeug.security import generate_password_hash, check_password_hash
from typing import Dict, Any, Optional
from contextlib import contextmanager

sys.path.append("../..")
from helpers.database.connection_to_db import connect_to_mongodb
from helpers.logger import get_logger
from pipelines.registration.user_creation import get_next_user_id, add_client_config
from pipelines.registration.project_creation import (
    get_next_project_id, 
    create_project_object, 
    create_mongodb_collections
)

logger = get_logger()
load_dotenv()

# Constants
MASTER_DB_NAME = "master"
USER_COLLECTION_NAME = "user"
CLIENT_CONFIG_COLLECTION_NAME = "client_config"

# Status constants
STATUS_SUCCESS = "success"
STATUS_FAILED = "failed"
STATUS_USER_EXISTS = "user_already_exists"
STATUS_USER_NOT_FOUND = "user_not_found"


@contextmanager
def get_db_connection():
    """Context manager for MongoDB connections with automatic cleanup"""
    client = connect_to_mongodb()
    if not client:
        raise Exception("Failed to connect to MongoDB")
    try:
        yield client
    finally:
        client.close()


def get_collections(client):
    """Get master database collections"""
    db = client[MASTER_DB_NAME]
    return (
        db[USER_COLLECTION_NAME],
        db[CLIENT_CONFIG_COLLECTION_NAME]
    )


def sanitize_user_data(user: Dict[str, Any], include_id: bool = True) -> Dict[str, Any]:
    """
    Remove sensitive information and prepare user data for response
    
    Args:
        user: User document from database
        include_id: Whether to include MongoDB _id field
    
    Returns:
        Sanitized user data dictionary
    """
    user_data = {
        "user_id": user.get("user_id"),
        "email": user.get("email"),
        "name": user.get("name"),
        "created_at": user.get("created_at")
    }
    
    if include_id and "_id" in user:
        user_data["_id"] = user["_id"]
    
    return user_data


def create_response(
    status: str, 
    message: Optional[str] = None, 
    **kwargs
) -> Dict[str, Any]:
    """
    Create standardized response dictionary
    
    Args:
        status: Response status
        message: Optional message
        **kwargs: Additional fields to include in response
    
    Returns:
        Response dictionary
    """
    response = {"status": status}
    if message:
        response["message"] = message
    response.update(kwargs)
    return response


def run_user_creation(
    email: str, 
    first_name: str, 
    last_name: str, 
    password: str
) -> Dict[str, Any]:
    """
    Create user and client config entries
    
    Args:
        email: User's email address
        first_name: User's first name
        last_name: User's last name
        password: User's password (will be hashed)
    
    Returns:
        Dictionary containing user and client config documents
    """
    try:
        with get_db_connection() as mongo_client:
            users_collection, client_config_collection = get_collections(mongo_client)
            
            # Check for existing user
            existing_user = users_collection.find_one({"email": email})
            if existing_user:
                logger.info(f"User with email {email} already exists")
                return create_response(
                    STATUS_USER_EXISTS,
                    user=existing_user,
                    client_config=None
                )
            
            # Generate user ID and prepare document
            user_id = get_next_user_id(users_collection)
            user_doc = {
                "user_id": user_id,
                "name": f"{first_name} {last_name}",
                "email": email,
                "password": generate_password_hash(password)
            }
            
            # Insert user
            result = users_collection.insert_one(user_doc)
            user_doc['_id'] = result.inserted_id
            logger.info(f"User created successfully with ID: {user_id}")
            
            # Create client config
            config_doc = add_client_config(user_id, client_config_collection)
            
            return create_response(
                STATUS_SUCCESS,
                user=user_doc,
                client_config=config_doc
            )
            
    except Exception as e:
        logger.error(f"Error during user creation: {e}")
        raise


def run_project_creation(
    user_id: str, 
    project_name: str, 
    domain: str
) -> Dict[str, Any]:
    """
    Add a project to user's configuration and create collections
    
    Args:
        user_id: User's ID (e.g., UID001)
        project_name: Name of the project
        domain: Domain of the project
    
    Returns:
        Dictionary containing updated client config, project info, and collections
    """
    try:
        with get_db_connection() as mongo_client:
            _, client_config_collection = get_collections(mongo_client)
            
            # Find user's client config
            client_config = client_config_collection.find_one({"user_id": user_id})
            if not client_config:
                logger.warning(f"No client configuration found for user: {user_id}")
                return create_response(
                    STATUS_USER_NOT_FOUND,
                    client_config=None,
                    project=None,
                    collections_created=None
                )
            
            # Generate project ID and create object
            project_id = get_next_project_id(user_id, client_config)
            project_obj = create_project_object(project_id, project_name, domain)
            
            # Add project to user's projects array
            result = client_config_collection.update_one(
                {"user_id": user_id},
                {"$push": {"projects": project_obj}}
            )
            
            if result.modified_count == 0:
                logger.error("Failed to add project")
                return create_response(
                    STATUS_FAILED,
                    client_config=None,
                    project=None,
                    collections_created=None
                )
            
            logger.info(f"Project created successfully with ID: {project_id}")
            
            # Create MongoDB collections
            collections_result = create_mongodb_collections(
                user_id, 
                project_id, 
                project_obj['mongodb']['collections']
            )
            
            # Fetch updated client config
            updated_config = client_config_collection.find_one({"user_id": user_id})
            
            return create_response(
                STATUS_SUCCESS,
                client_config=updated_config,
                project=project_obj,
                collections_created=collections_result['created_collections']
            )
        
    except Exception as e:
        logger.error(f"Error during project creation: {e}")
        raise


def run_user_login(email: str, password: str) -> Dict[str, Any]:
    """
    Authenticate user with email and password
    
    Args:
        email: User's email address
        password: User's password (plain text)
    
    Returns:
        Dictionary with status, message, and user data if successful
    """
    try:
        logger.info(f"Login attempt for email: {email}")
        
        with get_db_connection() as client:
            users_collection, _ = get_collections(client)
            
            # Find user by email
            user = users_collection.find_one({"email": email})
            
            if not user:
                logger.warning(f"User not found: {email}")
                return create_response(
                    STATUS_FAILED,
                    message="Invalid email or password"
                )
            
            # Verify password
            stored_password = user.get("password", "")
            if not check_password_hash(stored_password, password):
                logger.warning(f"Invalid password attempt for user: {email}")
                return create_response(
                    STATUS_FAILED,
                    message="Invalid email or password"
                )
            
            # Login successful
            logger.info(f"Login successful for user: {user.get('user_id')}")
            user_data = sanitize_user_data(user)
            
            return create_response(
                STATUS_SUCCESS,
                message="Login successful",
                user=user_data
            )
        
    except Exception as e:
        logger.error(f"Error during login: {str(e)}")
        return create_response(
            STATUS_FAILED,
            message=f"Login error: {str(e)}"
        )


def verify_user_password(user_id: str, password: str) -> bool:
    """
    Verify a user's password (useful for sensitive operations)
    
    Args:
        user_id: User's ID
        password: Password to verify
    
    Returns:
        True if password is correct, False otherwise
    """
    try:
        with get_db_connection() as client:
            users_collection, _ = get_collections(client)
            user = users_collection.find_one({"user_id": user_id})
            
            if not user:
                return False
            
            stored_password = user.get("password", "")
            return check_password_hash(stored_password, password)
            
    except Exception as e:
        logger.error(f"Error verifying password: {str(e)}")
        return False


def update_user_password(user_id: str, old_password: str, new_password: str) -> Dict[str, Any]:
    """
    Update user's password after verifying old password
    
    Args:
        user_id: User's ID
        old_password: Current password
        new_password: New password
    
    Returns:
        Response dictionary with status and message
    """
    try:
        with get_db_connection() as client:
            users_collection, _ = get_collections(client)
            
            # Verify old password
            if not verify_user_password(user_id, old_password):
                logger.warning(f"Invalid old password for user: {user_id}")
                return create_response(
                    STATUS_FAILED,
                    message="Current password is incorrect"
                )
            
            # Update password
            new_password_hash = generate_password_hash(new_password)
            result = users_collection.update_one(
                {"user_id": user_id},
                {"$set": {"password": new_password_hash}}
            )
            
            if result.modified_count > 0:
                logger.info(f"Password updated successfully for user: {user_id}")
                return create_response(
                    STATUS_SUCCESS,
                    message="Password updated successfully"
                )
            else:
                return create_response(
                    STATUS_FAILED,
                    message="Failed to update password"
                )
            
    except Exception as e:
        logger.error(f"Error updating password: {str(e)}")
        return create_response(
            STATUS_FAILED,
            message=f"Password update error: {str(e)}"
        )


def check_email_exists(email: str) -> bool:
    """
    Check if email already exists in database
    
    Args:
        email: Email address to check
    
    Returns:
        True if email exists, False otherwise
    """
    try:
        with get_db_connection() as client:
            users_collection, _ = get_collections(client)
            return users_collection.find_one({"email": email}) is not None
    except Exception as e:
        logger.error(f"Error checking email existence: {str(e)}")
        return False


def get_user_by_id(user_id: str) -> Optional[Dict[str, Any]]:
    """
    Get user data by user_id
    
    Args:
        user_id: User's ID
    
    Returns:
        Sanitized user data or None if not found
    """
    try:
        with get_db_connection() as client:
            users_collection, _ = get_collections(client)
            user = users_collection.find_one({"user_id": user_id})
            
            if user:
                return sanitize_user_data(user)
            return None
            
    except Exception as e:
        logger.error(f"Error retrieving user: {str(e)}")
        return None