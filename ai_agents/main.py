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
from pipelines.registration.project_creation import run_project_creation
#-- helper --
from helpers.logger import get_logger
#-- processing --
from ai_agents.api.bashboard_apis import run_pdp
#-- agent --
from ai_agents.agent.middleware_node import run_middleware
#-- user --
from ai_agents.api.user_apis import run_user_login

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

# Add this with your other Pydantic models
class UserLoginRequest(BaseModel):
    email: EmailStr
    password: str

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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)