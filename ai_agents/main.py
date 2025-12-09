import re
from fastapi import (
    FastAPI, 
    HTTPException,
    UploadFile, 
    File, 
    Form, 
    HTTPException)
from bson import ObjectId
from pydantic import (
    BaseModel, 
    EmailStr, 
    field_validator)
from typing import Dict, Any
import pandas as pd
import io
import sys
sys.path.append("..")
#-- registration --
from pipelines.registration.project_creation import run_project_creation
from pipelines.registration.user_creation import run_user_creation
#-- helper --
from helpers.logger import get_logger
from helpers.database.connection_to_db import connect_to_mongodb
#-- agent --
from ai_agents.agent.middleware_node import run_middleware
#-- user --
from ai_agents.api.user_apis import run_user_login
#-- dashboard --
from ai_agents.api.dashboard_apis import (
    run_pdp,
    get_user_details,
    get_recent_projects,
    get_all_projects,
    update_project_last_used,
    get_user_projects_count,
    upload_data_to_project,
    get_project_upload_status
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

class ProjectDeleteRequest(BaseModel):
    project_id: str
    user_id: str  # Add user_id to the request for security

@app.delete("/delete-project")
async def delete_project_endpoint(request: ProjectDeleteRequest):
    """
    Delete a project and all its associated data
    
    Example request:
    DELETE /delete-project
    {
        "user_id": "UID002",
        "project_id": "UID002PJ001"
    }
    
    Example response:
    {
        "status": "success",
        "message": "Project deleted successfully",
        "deleted_project": {
            "project_id": "UID002PJ001",
            "project_name": "My Analytics Project",
            "mongo_collections_deleted": ["UID002PJ001_data", ...],
            "weaviate_collections_deleted": ["UID002PJ001_weviate_cd", ...],
            "total_mongo_collections": 9,
            "total_weaviate_collections": 2
        }
    }
    """
    try:
        logger.info(f"Deleting project: user_id={request.user_id}, project_id={request.project_id}")
        
        # Import the delete function
        from ai_agents.api.dashboard_apis import delete_project
        
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
    
@app.post("/upload-data/{project_id}")
async def upload_data(
    project_id: str,
    file: UploadFile = File(...),
    user_id: str = Form(...),
    file_type: str = Form("auto")
):
    """
    Upload data file to a project
    
    Example request:
    POST /upload-data/UID002PJ001
    Form data:
    - file: data.csv
    - user_id: UID002
    - file_type: csv (optional, can be: csv, excel, json, auto)
    
    Example response:
    {
        "status": "success",
        "message": "Data uploaded successfully",
        "records_inserted": 150,
        "collection_name": "UID002PJ001_data"
    }
    """
    try:
        logger.info(f"Uploading data for project: {project_id}, user: {user_id}")
        
        # Validate project belongs to user
        mongo_client = connect_to_mongodb()
        if not mongo_client:
            logger.error("Failed to connect to MongoDB")
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        master_db = mongo_client["master"]
        client_config_collection = master_db["client_config"]
        
        # Check if project exists and belongs to user
        client_config = client_config_collection.find_one({"user_id": user_id})
        if not client_config:
            raise HTTPException(status_code=404, detail="User not found")
        
        project_exists = False
        for project in client_config.get("projects", []):
            if project.get("project_id") == project_id:
                project_exists = True
                break
        
        if not project_exists:
            raise HTTPException(status_code=404, detail="Project not found")
        
        # Read file content
        contents = await file.read()
        
        # Determine file type
        if file_type == "auto":
            # Auto-detect based on file extension
            filename = file.filename.lower()
            if filename.endswith('.csv'):
                file_type = 'csv'
            elif filename.endswith(('.xlsx', '.xls')):
                file_type = 'excel'
            elif filename.endswith('.json'):
                file_type = 'json'
            else:
                raise HTTPException(status_code=400, detail="Unsupported file type")
        
        # Parse data based on file type
        df = None
        try:
            if file_type == 'csv':
                df = pd.read_csv(io.BytesIO(contents))
            elif file_type == 'excel':
                df = pd.read_excel(io.BytesIO(contents))
            elif file_type == 'json':
                df = pd.read_json(io.BytesIO(contents))
            else:
                raise HTTPException(status_code=400, detail="Unsupported file type")
        except Exception as e:
            logger.error(f"Error parsing file: {str(e)}")
            raise HTTPException(status_code=400, detail=f"Error parsing file: {str(e)}")
        
        # Convert dataframe to list of dictionaries
        records = df.to_dict('records')
        
        if not records:
            raise HTTPException(status_code=400, detail="No data found in file")
        
        # Get user's database
        db_name = client_config.get("db_name", user_id)
        collection_name = f"{project_id}_data"
        
        # Insert data into MongoDB
        user_db = mongo_client[db_name]
        collection = user_db[collection_name]
        
        # Clear existing data if any
        collection.delete_many({})
        
        # Insert new data
        result = collection.insert_many(records)
        records_inserted = len(result.inserted_ids)
        
        logger.info(f"Uploaded {records_inserted} records to {collection_name}")
        
        return {
            "status": "success",
            "message": "Data uploaded successfully",
            "records_inserted": records_inserted,
            "collection_name": collection_name,
            "columns": list(df.columns),
            "sample_data": records[:5] if records else []
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error uploading data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to upload data: {str(e)}")

# Optional: Add an endpoint to get upload status/progress
@app.get("/upload-status/{project_id}")
async def get_upload_status(project_id: str, user_id: str):
    """
    Check if project has data uploaded
    
    Example response:
    {
        "has_data": true,
        "records_count": 150,
        "last_uploaded": "2025-01-01T12:00:00Z"
    }
    """
    try:
        mongo_client = connect_to_mongodb()
        if not mongo_client:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        master_db = mongo_client["master"]
        client_config_collection = master_db["client_config"]
        
        # Check if project exists
        client_config = client_config_collection.find_one({"user_id": user_id})
        if not client_config:
            raise HTTPException(status_code=404, detail="User not found")
        
        # Get database name
        db_name = client_config.get("db_name", user_id)
        collection_name = f"{project_id}_data"
        
        # Check if collection exists and has data
        user_db = mongo_client[db_name]
        if collection_name in user_db.list_collection_names():
            collection = user_db[collection_name]
            count = collection.count_documents({})
            
            # Get last document's upload time if available
            last_doc = collection.find_one(sort=[("_id", -1)])
            last_uploaded = None
            if last_doc and "_id" in last_doc:
                last_uploaded = last_doc["_id"].generation_time.isoformat() + "Z"
            
            return {
                "has_data": count > 0,
                "records_count": count,
                "last_uploaded": last_uploaded,
                "collection_name": collection_name
            }
        else:
            return {
                "has_data": False,
                "records_count": 0,
                "collection_name": collection_name
            }
            
    except Exception as e:
        logger.error(f"Error checking upload status: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to check upload status: {str(e)}")
    
@app.post("/upload-data/{project_id}")
async def upload_data(
    project_id: str,
    file: UploadFile = File(...),
    user_id: str = Form(...),
    file_type: str = Form("auto")
):
    """
    Upload data file to a project
    
    Example request:
    POST /upload-data/UID002PJ001
    Form data:
    - file: data.csv
    - user_id: UID002
    - file_type: csv (optional, can be: csv, excel, json, auto)
    
    Example response:
    {
        "status": "success",
        "message": "Data uploaded successfully",
        "records_inserted": 150,
        "collection_name": "UID002PJ001_data"
    }
    """
    try:
        # Connect to MongoDB
        mongo_client = connect_to_mongodb()
        if not mongo_client:
            logger.error("Failed to connect to MongoDB")
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        # Read file content
        contents = await file.read()
        
        # Create a file-like object for the service
        class FileWrapper:
            def __init__(self, content, filename):
                self.file = content
                self.filename = filename
        
        file_wrapper = FileWrapper(contents, file.filename)
        
        # Call service function
        result = upload_data_to_project(
            mongo_client=mongo_client,
            project_id=project_id,
            user_id=user_id,
            file=file_wrapper,
            file_type=file_type
        )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error uploading data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to upload data: {str(e)}")


@app.get("/upload-status/{project_id}")
async def get_upload_status(project_id: str, user_id: str):
    """
    Check if project has data uploaded
    
    Example response:
    {
        "has_data": true,
        "records_count": 150,
        "last_uploaded": "2025-01-01T12:00:00Z"
    }
    """
    try:
        # Connect to MongoDB
        mongo_client = connect_to_mongodb()
        if not mongo_client:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        # Call service function
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)