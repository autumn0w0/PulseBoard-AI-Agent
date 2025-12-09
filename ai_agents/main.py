import re
from fastapi import FastAPI, HTTPException
from bson import ObjectId
from pydantic import BaseModel, EmailStr, field_validator
from typing import Dict, Any
import sys
sys.path.append("..")
#-- registration --
from pipelines.registration.project_creation import run_project_creation
from pipelines.registration.user_creation import run_user_creation
#-- helper --
from helpers.logger import get_logger
#-- processing --
from ai_agents.api.bashboard_apis import run_pdp
#-- agent --
from ai_agents.agent.middleware_node import run_middleware
#-- user --
from ai_agents.api.user_apis import run_user_login
#-- dashboard --
from ai_agents.api.bashboard_apis import (
    get_user_details,
    get_recent_projects,
    get_all_projects,
    update_project_last_used,
    get_user_projects_count
)

logger = get_logger(__name__)

app = FastAPI()

class ProjectCreateRequest(BaseModel):
    user_id: str
    project_name: str
    domain: str

class DataPipelineRequest(BaseModel):
    project_id: str

class MiddlewareQueryRequest(BaseModel):
    project_id: str
    query: str
    master_db_name: str = "master"

def convert_objectid_to_str(data):
    """
    Recursively convert all ObjectId instances to strings in nested data structures
    """
    if isinstance(data, dict):
        return {key: convert_objectid_to_str(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [convert_objectid_to_str(item) for item in data]
    elif isinstance(data, ObjectId):
        return str(data)
    else:
        return data

class UserCreateRequest(BaseModel):
    email: EmailStr  # Changed to EmailStr for automatic email validation
    first_name: str
    last_name: str
    password: str
    
    @field_validator('password')
    @classmethod
    def validate_password(cls, v):
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
    def validate_name(cls, v):
        """Validate names are not empty"""
        if not v or not v.strip():
            raise ValueError('Name cannot be empty')
        return v.strip()

# Add these with your other Pydantic models
class UserLoginRequest(BaseModel):
    email: EmailStr
    password: str

# Dashboard Pydantic models
class ProjectUpdateRequest(BaseModel):
    project_id: str

class UserIdRequest(BaseModel):
    user_id: str

@app.post("/create-user")
async def create_user(user_data: UserCreateRequest):
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
        
        # Convert ObjectId to string
        if result.get("user") and "_id" in result["user"]:
            result["user"]["_id"] = str(result["user"]["_id"])
            # Remove password from response for security
            result["user"].pop("password", None)
            
        if result.get("client_config") and "_id" in result["client_config"]:
            result["client_config"]["_id"] = str(result["client_config"]["_id"])
        
        logger.info(f"User created successfully: {result['user']['user_id']}")
        return result
        
    except HTTPException:
        raise
    except ValueError as e:
        # Handle Pydantic validation errors
        logger.error(f"Validation error: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error creating user: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/create-project")
async def create_project(project_data: ProjectCreateRequest):
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
        
        # Convert ObjectId to string in client_config
        if result.get("client_config") and "_id" in result["client_config"]:
            result["client_config"]["_id"] = str(result["client_config"]["_id"])
        
        logger.info(f"Project created successfully: {result['project']['project_id']}")
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating project: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/data-process-pipeline")
async def process_project_pipeline(project_id: str) -> Dict[str, Any]:
    """
    API endpoint to run the complete data processing pipeline for a project.
    
    Args:
        project_id: The unique identifier for the project
        
    Returns:
        JSON response containing results from all pipeline steps
        
    Example:
        POST /projects/abc123/process
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
    except HTTPException as he:
        # Re-raise HTTPExceptions as-is
        raise he
    except Exception as e:
        logger.error(f"Unexpected error in project data pipeline: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Unexpected error in project data pipeline: {str(e)}"
        )

@app.post("/agent-query")
async def query_middleware(request: MiddlewareQueryRequest):
    """
    Process user query through the middleware node.
    The middleware will classify the intent and route to appropriate pipeline:
    - Data Analysis (Analyst Node)
    - Chart Insights (RAG Charts Node)
    - Data Schema (RAG Data Node)
    - General queries
    
    Example request:
    {
        "project_id": "UID001PJ001",
        "query": "What is the average salary?"
    }
    
    Example response:
    {
        "response": "The average salary is $75,000..."
    }
    """
    try:
        project_id = request.project_id
        query = request.query
        master_db_name = request.master_db_name
        
        logger.info(f"Processing middleware query for project: {project_id}")
        logger.info(f"Query: {query}")
        
        # Validate inputs
        if not project_id or not query:
            raise HTTPException(
                status_code=400, 
                detail="Both project_id and query are required"
            )
        
        # Run middleware
        response = run_middleware(
            project_id=project_id,
            query=query,
            master_db_name=master_db_name
        )
        
        logger.info(f"Middleware query completed for project {project_id}")
        
        return {
            "response": response
        }
        
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

# Add this endpoint to your FastAPI app
@app.post("/user-login")
async def login_user(login_data: UserLoginRequest):
    """
    Authenticate user and return user information.
    
    Example request:
    {
        "email": "user@example.com",
        "password": "YourPassword123"
    }
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
        
        # Convert ObjectId to string if present
        if result.get("user") and "_id" in result["user"]:
            result["user"]["_id"] = str(result["user"]["_id"])
        
        logger.info(f"Login successful: {result['user']['user_id']}")
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in login endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# Dashboard API Endpoints
@app.get("/user-details/{user_id}")
async def get_user_details_endpoint(user_id: str):
    """
    Get user details for the dashboard
    
    Example request:
    GET /user-details/UID002
    
    Example response:
    {
        "_id": "6937c374491b077a8f3611e2",
        "user_id": "UID002",
        "name": "Akhilesh Damke",
        "email": "akhileshdamke7860@gmail.com"
    }
    """
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

@app.get("/recent-projects/{user_id}")
async def get_recent_projects_endpoint(user_id: str, limit: int = 3):
    """
    Get recent projects for a user (default: 3 most recent)
    
    Example request:
    GET /recent-projects/UID002
    GET /recent-projects/UID002?limit=5
    
    Example response:
    {
        "status": "success",
        "projects": [
            {
                "project_id": "UID002PJ001",
                "name_of_project": "My Analytics Project",
                "domain": "entertainment",
                "created_at": "2025-12-09T06:49:25.612Z",
                "last_used_at": "2025-12-09T06:49:25.612Z"
            }
        ]
    }
    """
    try:
        logger.info(f"Getting recent projects for user_id: {user_id}, limit: {limit}")
        
        # Validate limit parameter
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

@app.get("/all-projects/{user_id}")
async def get_all_projects_endpoint(user_id: str):
    """
    Get all projects for a user, sorted by most recent
    
    Example request:
    GET /all-projects/UID002
    
    Example response:
    {
        "status": "success",
        "total_projects": 1,
        "projects": [
            {
                "project_id": "UID002PJ001",
                "name_of_project": "My Analytics Project",
                "domain": "entertainment",
                "created_at": "2025-12-09T06:49:25.612Z",
                "last_used_at": "2025-12-09T06:49:25.612Z"
            }
        ]
    }
    """
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

@app.put("/update-project-last-used")
async def update_project_last_used_endpoint(request: ProjectUpdateRequest):
    """
    Update the last_used_at timestamp for a project
    
    Example request:
    PUT /update-project-last-used
    {
        "project_id": "UID002PJ001"
    }
    
    Example response:
    {
        "status": "success",
        "message": "Project last_used_at updated successfully",
        "project": {
            "project_id": "UID002PJ001",
            "name_of_project": "My Analytics Project",
            "domain": "entertainment",
            "created_at": "2025-12-09T06:49:25.612Z",
            "last_used_at": "2025-12-10T10:30:00.000Z"
        }
    }
    """
    try:
        logger.info(f"Updating last_used_at for project: {request.project_id}")
        
        # Note: This endpoint assumes the user_id is available from session/token
        # In a real implementation, you would get user_id from JWT token or session
        # For now, we'll extract it from the project_id (UID002PJ001 → UID002)
        # Or you could pass user_id in the request
        
        # Extract user_id from project_id (assuming format: UID002PJ001)
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

@app.get("/project-count/{user_id}")
async def get_project_count_endpoint(user_id: str):
    """
    Get the total number of projects for a user
    
    Example request:
    GET /project-count/UID002
    
    Example response:
    {
        "status": "success",
        "total_projects": 5
    }
    """
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

@app.get("/health-check")
async def health_check():
    """
    Health check endpoint for monitoring
    
    Example response:
    {
        "status": "healthy",
        "timestamp": "2025-12-09T06:49:25.612Z"
    }
    """
    from datetime import datetime, timezone
    return {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat() + "Z",
        "service": "PulseBoard.ai API"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)