import re
import io
import sys
from datetime import datetime, timezone
from typing import Dict, Any, Optional

import pandas as pd
from bson import ObjectId
from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from pydantic import BaseModel, EmailStr, field_validator

sys.path.append("..")

# Registration
from pipelines.registration.project_creation import run_project_creation
from pipelines.registration.user_creation import run_user_creation

# Helper
from helpers.logger import get_logger
from helpers.database.connection_to_db import connect_to_mongodb

# Agent
from ai_agents.agent.middleware_node import run_middleware

# User
from ai_agents.api.user_apis import run_user_login

# Dashboard
from ai_agents.api.dashboard_apis import (
    run_pdp,
    get_user_details,
    get_recent_projects,
    get_all_projects,
    update_project_last_used,
    get_user_projects_count,
    upload_data_to_project,
    get_project_upload_status,
    delete_project
)

logger = get_logger(__name__)
app = FastAPI(title="PulseBoard.ai API", version="1.0.0")


# ============================================================================
# Pydantic Models
# ============================================================================

class UserCreateRequest(BaseModel):
    email: EmailStr
    first_name: str
    last_name: str
    password: str
    
    @field_validator('password')
    @classmethod
    def validate_password(cls, v: str) -> str:
        """Validate password strength"""
        if len(v) < 8:
            raise ValueError('Password must be at least 8 characters long')
        if not re.search(r'[A-Z]', v):
            raise ValueError('Password must contain at least one uppercase letter')
        if not re.search(r'[a-z]', v):
            raise ValueError('Password must contain at least one lowercase letter')
        if not re.search(r'\d', v):
            raise ValueError('Password must contain at least one digit')
        return v
    
    @field_validator('first_name', 'last_name')
    @classmethod
    def validate_name(cls, v: str) -> str:
        """Validate names are not empty"""
        if not v or not v.strip():
            raise ValueError('Name cannot be empty')
        return v.strip()


class UserLoginRequest(BaseModel):
    email: EmailStr
    password: str


class ProjectCreateRequest(BaseModel):
    user_id: str
    project_name: str
    domain: str


class ProjectUpdateRequest(BaseModel):
    project_id: str


class ProjectDeleteRequest(BaseModel):
    project_id: str
    user_id: str


class MiddlewareQueryRequest(BaseModel):
    project_id: str
    query: str
    master_db_name: str = "master"


# ============================================================================
# Helper Functions
# ============================================================================

def convert_objectid_to_str(data: Any) -> Any:
    """Recursively convert all ObjectId instances to strings in nested data structures"""
    if isinstance(data, dict):
        return {key: convert_objectid_to_str(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [convert_objectid_to_str(item) for item in data]
    elif isinstance(data, ObjectId):
        return str(data)
    return data


def sanitize_user_response(user_dict: dict) -> dict:
    """Remove sensitive data and convert ObjectIds"""
    if "_id" in user_dict:
        user_dict["_id"] = str(user_dict["_id"])
    user_dict.pop("password", None)
    return user_dict


class FileWrapper:
    """Wrapper for file upload compatibility"""
    def __init__(self, content: bytes, filename: str):
        self.file = content
        self.filename = filename


# ============================================================================
# User Management Endpoints
# ============================================================================

@app.post("/create-user", tags=["User Management"])
async def create_user(user_data: UserCreateRequest):
    """
    Create a new user account
    
    Returns:
        User details and client configuration
    
    Raises:
        409: User already exists
        400: Validation error
        500: Server error
    """
    try:
        logger.info(f"Creating user with email: {user_data.email}")
        
        result = run_user_creation(
            email=user_data.email,
            first_name=user_data.first_name,
            last_name=user_data.last_name,
            password=user_data.password
        )
        
        if result["status"] == "user_already_exists":
            logger.warning(f"User already exists: {user_data.email}")
            raise HTTPException(status_code=409, detail="User already exists")
        
        # Sanitize response
        if result.get("user"):
            result["user"] = sanitize_user_response(result["user"])
        if result.get("client_config") and "_id" in result["client_config"]:
            result["client_config"]["_id"] = str(result["client_config"]["_id"])
        
        logger.info(f"User created successfully: {result['user']['user_id']}")
        return result
        
    except HTTPException:
        raise
    except ValueError as e:
        logger.error(f"Validation error: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error creating user: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/user-login", tags=["User Management"])
async def login_user(login_data: UserLoginRequest):
    """
    Authenticate user and return user information
    
    Returns:
        User details on successful authentication
    
    Raises:
        401: Invalid credentials
        500: Server error
    """
    try:
        logger.info(f"Login request for email: {login_data.email}")
        
        result = run_user_login(
            email=login_data.email,
            password=login_data.password
        )
        
        if result["status"] == "failed":
            logger.warning(f"Login failed: {result['message']}")
            raise HTTPException(status_code=401, detail=result["message"])
        
        # Sanitize response
        if result.get("user"):
            result["user"] = sanitize_user_response(result["user"])
        
        logger.info(f"Login successful: {result['user']['user_id']}")
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in login endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Project Management Endpoints
# ============================================================================

@app.post("/create-project", tags=["Project Management"])
async def create_project(project_data: ProjectCreateRequest):
    """
    Create a new project for a user
    
    Returns:
        Project details and updated client configuration
    
    Raises:
        404: User not found
        500: Failed to create project
    """
    try:
        logger.info(f"Creating project for user: {project_data.user_id}")
        
        result = run_project_creation(
            user_id=project_data.user_id,
            project_name=project_data.project_name,
            domain=project_data.domain
        )
        
        if result["status"] == "user_not_found":
            logger.warning(f"User not found: {project_data.user_id}")
            raise HTTPException(status_code=404, detail="User not found")
        
        if result["status"] == "failed":
            logger.error("Failed to create project")
            raise HTTPException(status_code=500, detail="Failed to create project")
        
        # Convert ObjectId
        if result.get("client_config") and "_id" in result["client_config"]:
            result["client_config"]["_id"] = str(result["client_config"]["_id"])
        
        logger.info(f"Project created successfully: {result['project']['project_id']}")
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating project: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/delete-project", tags=["Project Management"])
async def delete_project_endpoint(request: ProjectDeleteRequest):
    """
    Delete a project and all its associated data
    
    Returns:
        Details of deleted resources
    
    Raises:
        404: Project not found
        500: Failed to delete project
    """
    try:
        logger.info(f"Deleting project: user_id={request.user_id}, project_id={request.project_id}")
        
        deleted_data = delete_project(request.user_id, request.project_id)
        
        logger.info(f"Project deleted successfully: {request.project_id}")
        return {
            "status": "success",
            "message": "Project deleted successfully",
            "deleted_project": deleted_data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in delete_project endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to delete project: {str(e)}")


@app.put("/update-project-last-used", tags=["Project Management"])
async def update_project_last_used_endpoint(request: ProjectUpdateRequest):
    """
    Update the last_used_at timestamp for a project
    
    Returns:
        Updated project details
    
    Raises:
        400: Invalid project_id format
        404: Project not found
        500: Failed to update project
    """
    try:
        logger.info(f"Updating last_used_at for project: {request.project_id}")
        
        # Extract user_id from project_id (format: UID002PJ001)
        user_id = request.project_id.split('PJ')[0]
        
        if not user_id.startswith('UID'):
            raise HTTPException(status_code=400, detail="Invalid project_id format")
        
        updated_project = update_project_last_used(user_id, request.project_id)
        
        logger.info(f"Project last_used_at updated successfully: {request.project_id}")
        return {
            "status": "success",
            "message": "Project last_used_at updated successfully",
            "project": updated_project
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in update_project_last_used endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to update project: {str(e)}")


# ============================================================================
# Dashboard Endpoints
# ============================================================================

@app.get("/user-details/{user_id}", tags=["Dashboard"])
async def get_user_details_endpoint(user_id: str):
    """Get user details for the dashboard"""
    try:
        logger.info(f"Getting user details for user_id: {user_id}")
        result = get_user_details(user_id)
        logger.info(f"User details retrieved successfully for user_id: {user_id}")
        
        return {
            "status": "success",
            "user": result
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_user_details endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get user details: {str(e)}")


@app.get("/recent-projects/{user_id}", tags=["Dashboard"])
async def get_recent_projects_endpoint(user_id: str, limit: int = 3):
    """
    Get recent projects for a user
    
    Args:
        user_id: User identifier
        limit: Maximum number of projects to return (1-50, default: 3)
    """
    try:
        logger.info(f"Getting recent projects for user_id: {user_id}, limit: {limit}")
        
        if limit < 1 or limit > 50:
            raise HTTPException(status_code=400, detail="Limit must be between 1 and 50")
        
        projects = get_recent_projects(user_id, limit)
        
        logger.info(f"Retrieved {len(projects)} recent projects for user_id: {user_id}")
        return {
            "status": "success",
            "total_projects": len(projects),
            "projects": projects
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_recent_projects endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get recent projects: {str(e)}")


@app.get("/all-projects/{user_id}", tags=["Dashboard"])
async def get_all_projects_endpoint(user_id: str):
    """Get all projects for a user, sorted by most recent"""
    try:
        logger.info(f"Getting all projects for user_id: {user_id}")
        projects = get_all_projects(user_id)
        
        logger.info(f"Retrieved {len(projects)} projects for user_id: {user_id}")
        return {
            "status": "success",
            "total_projects": len(projects),
            "projects": projects
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_all_projects endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get projects: {str(e)}")


@app.get("/project-count/{user_id}", tags=["Dashboard"])
async def get_project_count_endpoint(user_id: str):
    """Get the total number of projects for a user"""
    try:
        logger.info(f"Getting project count for user_id: {user_id}")
        result = get_user_projects_count(user_id)
        
        logger.info(f"Project count retrieved for user_id: {user_id}")
        return {
            "status": "success",
            "total_projects": result["total_projects"]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_project_count endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get project count: {str(e)}")


# ============================================================================
# Data Processing Endpoints
# ============================================================================

@app.post("/data-process-pipeline/{project_id}", tags=["Data Processing"])
async def process_project_pipeline(project_id: str) -> Dict[str, Any]:
    """
    Run the complete data processing pipeline for a project
    
    Args:
        project_id: The unique identifier for the project
        
    Returns:
        JSON response containing results from all pipeline steps
    """
    logger.info(f"Starting project data pipeline for project {project_id}")
    
    try:
        results = run_pdp(project_id)
        return {
            "status": "success",
            "project_id": project_id,
            "message": "Project data pipeline completed successfully",
            "results": results
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in project data pipeline: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Unexpected error in project data pipeline: {str(e)}"
        )

@app.post("/agent-query", tags=["Data Processing"])
async def query_middleware(request: MiddlewareQueryRequest):
    """
    Process user query through the middleware node.
    
    The middleware will classify the intent and route to appropriate pipeline:
    - Data Analysis (Analyst Node)
    - Chart Insights (RAG Charts Node)
    - Data Schema (RAG Data Node)
    - General queries
    
    Returns:
        AI-generated response based on the query type
    """
    try:
        logger.info(f"Processing middleware query for project: {request.project_id}")
        logger.info(f"Query: {request.query}")
        
        if not request.project_id or not request.query:
            raise HTTPException(
                status_code=400, 
                detail="Both project_id and query are required"
            )
        
        response = run_middleware(
            project_id=request.project_id,
            query=request.query,
            master_db_name=request.master_db_name
        )
        
        logger.info(f"Middleware query completed for project {request.project_id}")
        return {"response": response}
        
    except HTTPException:
        raise
    except ValueError as e:
        logger.error(f"Validation error in middleware query: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except ConnectionError as e:
        logger.error(f"Connection error in middleware query: {str(e)}")
        raise HTTPException(status_code=503, detail=f"Database connection failed: {str(e)}")
    except Exception as e:
        logger.error(f"Error in middleware query: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/upload-data/{project_id}", tags=["Data Upload"])
async def upload_data(
    project_id: str,
    file: UploadFile = File(...),
    user_id: str = Form(...),
    file_type: str = Form("auto")
):
    """
    Upload data file to a project
    
    Args:
        project_id: Project identifier
        file: Data file (CSV, Excel, or JSON)
        user_id: User identifier
        file_type: File type (csv, excel, json, or auto for auto-detection)
    
    Returns:
        Upload status and basic data statistics
    """
    mongo_client = None
    try:
        logger.info(f"Uploading data for project: {project_id}, user: {user_id}")
        
        mongo_client = connect_to_mongodb()
        if not mongo_client:
            logger.error("Failed to connect to MongoDB")
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        # Read file contents
        contents = await file.read()
        
        # Call the helper function with bytes and filename
        result = upload_data_to_project(
            mongo_client=mongo_client,
            project_id=project_id,
            user_id=user_id,
            file_contents=contents,
            filename=file.filename,
            file_type=file_type
        )
        
        logger.info(f"Data uploaded successfully for project: {project_id}")
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error uploading data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to upload data: {str(e)}")
    finally:
        if mongo_client:
            mongo_client.close()

@app.get("/upload-status/{project_id}", tags=["Data Upload"])
async def get_upload_status(
    project_id: str,
    user_id: str
):
    """
    Check if project has data uploaded
    
    Args:
        project_id: Project identifier
        user_id: User identifier
    
    Returns:
        Upload status information
    """
    mongo_client = None
    try:
        logger.info(f"Checking upload status for project: {project_id}, user: {user_id}")
        
        mongo_client = connect_to_mongodb()
        if not mongo_client:
            logger.error("Failed to connect to MongoDB")
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        result = get_project_upload_status(
            mongo_client=mongo_client,
            project_id=project_id,
            user_id=user_id
        )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error checking upload status: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to check upload status: {str(e)}")
    finally:
        if mongo_client:
            mongo_client.close()

# ============================================================================
# Health Check
# ============================================================================

@app.get("/health-check", tags=["System"])
async def health_check():
    """Health check endpoint for monitoring"""
    return {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat() + "Z",
        "service": "PulseBoard.ai API"
    }


# ============================================================================
# Application Entry Point
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)