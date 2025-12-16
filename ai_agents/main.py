import re
import sys
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from bson import ObjectId
from fastapi import FastAPI, HTTPException, UploadFile, File, Form, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr, field_validator, Field

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
    delete_project,
    get_project_charts,
    get_specific_chart,
    get_chart_types,
    get_direct_charts,
    serialize_mongo_doc
)

# Initialize
logger = get_logger(__name__)
app = FastAPI(
    title="PulseBoard.ai API",
    version="2.0.0",
    description="Advanced Dashboard and Analytics API",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS Configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================================
# Pydantic Models
# ============================================================================

class UserCreateRequest(BaseModel):
    """User registration request model with validation"""
    email: EmailStr
    first_name: str = Field(..., min_length=1, max_length=50)
    last_name: str = Field(..., min_length=1, max_length=50)
    password: str = Field(..., min_length=8)
    
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
        """Validate names are not empty and trimmed"""
        if not v or not v.strip():
            raise ValueError('Name cannot be empty')
        return v.strip()


class UserLoginRequest(BaseModel):
    """User login request model"""
    email: EmailStr
    password: str


class ProjectCreateRequest(BaseModel):
    """Project creation request model"""
    user_id: str = Field(..., pattern=r'^UID\d+$')
    project_name: str = Field(..., min_length=1, max_length=100)
    domain: str = Field(..., min_length=1, max_length=50)


class ProjectUpdateRequest(BaseModel):
    """Project update request model"""
    project_id: str = Field(..., pattern=r'^UID\d+PJ\d+$')


class ProjectDeleteRequest(BaseModel):
    """Project deletion request model"""
    project_id: str = Field(..., pattern=r'^UID\d+PJ\d+$')
    user_id: str = Field(..., pattern=r'^UID\d+$')


class MiddlewareQueryRequest(BaseModel):
    """Middleware query request model"""
    project_id: str = Field(..., pattern=r'^UID\d+PJ\d+$')
    query: str = Field(..., min_length=1, max_length=1000)
    master_db_name: str = "master"


class StandardResponse(BaseModel):
    """Standard API response model"""
    status: str
    message: Optional[str] = None
    data: Optional[Dict[str, Any]] = None


# ============================================================================
# Helper Functions
# ============================================================================

def sanitize_response(data: Any) -> Any:
    """
    Recursively sanitize response data:
    - Convert ObjectId to string
    - Remove password fields
    - Handle nested structures
    """
    if isinstance(data, dict):
        sanitized = {}
        for key, value in data.items():
            if key == "password":
                continue
            if isinstance(value, ObjectId):
                sanitized[key] = str(value)
            elif key == "_id" and isinstance(value, ObjectId):
                sanitized[key] = str(value)
            else:
                sanitized[key] = sanitize_response(value)
        return sanitized
    elif isinstance(data, list):
        return [sanitize_response(item) for item in data]
    elif isinstance(data, ObjectId):
        return str(data)
    return data


def create_success_response(
    message: str = "Success",
    data: Optional[Dict[str, Any]] = None,
    **kwargs
) -> Dict[str, Any]:
    """Create standardized success response"""
    response = {
        "status": "success",
        "message": message
    }
    if data:
        response["data"] = sanitize_response(data)
    response.update(sanitize_response(kwargs))
    return response


def create_error_response(
    message: str,
    status_code: int = 500
) -> HTTPException:
    """Create standardized error response"""
    return HTTPException(
        status_code=status_code,
        detail={
            "status": "error",
            "message": message
        }
    )


def extract_user_id_from_project_id(project_id: str) -> str:
    """Extract user_id from project_id (format: UID002PJ001)"""
    if 'PJ' not in project_id:
        raise ValueError("Invalid project_id format")
    return project_id.split('PJ')[0]


# ============================================================================
# User Management Endpoints
# ============================================================================

@app.post("/api/v1/users/register", tags=["User Management"])
async def create_user(user_data: UserCreateRequest):
    """
    Register a new user account
    
    - **email**: Valid email address
    - **first_name**: User's first name (1-50 characters)
    - **last_name**: User's last name (1-50 characters)
    - **password**: Strong password (min 8 chars, uppercase, lowercase, digit)
    
    Returns user details and initial configuration
    """
    try:
        logger.info(f"User registration attempt: {user_data.email}")
        
        result = run_user_creation(
            email=user_data.email,
            first_name=user_data.first_name,
            last_name=user_data.last_name,
            password=user_data.password
        )
        
        if result["status"] == "user_already_exists":
            raise create_error_response(
                "User with this email already exists",
                409
            )
        
        logger.info(f"User registered successfully: {result['user']['user_id']}")
        return create_success_response(
            message="User registered successfully",
            user=result["user"],
            client_config=result.get("client_config")
        )
        
    except HTTPException:
        raise
    except ValueError as e:
        logger.error(f"Validation error: {str(e)}")
        raise create_error_response(str(e), 400)
    except Exception as e:
        logger.error(f"Registration error: {str(e)}")
        raise create_error_response(f"Registration failed: {str(e)}", 500)


@app.post("/api/v1/users/login", tags=["User Management"])
async def login_user(login_data: UserLoginRequest):
    """
    Authenticate user and return user information
    
    - **email**: User's registered email
    - **password**: User's password
    
    Returns user details on successful authentication
    """
    try:
        logger.info(f"Login attempt: {login_data.email}")
        
        result = run_user_login(
            email=login_data.email,
            password=login_data.password
        )
        
        if result["status"] == "failed":
            raise create_error_response(
                result.get("message", "Invalid credentials"),
                401
            )
        
        logger.info(f"Login successful: {result['user']['user_id']}")
        return create_success_response(
            message="Login successful",
            user=result["user"]
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Login error: {str(e)}")
        raise create_error_response(f"Login failed: {str(e)}", 500)


@app.get("/api/v1/users/{user_id}", tags=["User Management"])
async def get_user_details_endpoint(user_id: str):
    """
    Get user details
    
    - **user_id**: User identifier (format: UID001)
    
    Returns complete user profile information
    """
    try:
        logger.info(f"Fetching user details: {user_id}")
        user = get_user_details(user_id)
        
        return create_success_response(
            message="User details retrieved",
            user=user
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching user: {str(e)}")
        raise create_error_response(f"Failed to get user details: {str(e)}", 500)


# ============================================================================
# Project Management Endpoints
# ============================================================================

@app.post("/api/v1/projects", tags=["Project Management"])
async def create_project(project_data: ProjectCreateRequest):
    """
    Create a new project for a user
    
    - **user_id**: User identifier (format: UID001)
    - **project_name**: Project name (1-100 characters)
    - **domain**: Project domain/category
    
    Returns project details and updated configuration
    """
    try:
        logger.info(f"Creating project for user: {project_data.user_id}")
        
        result = run_project_creation(
            user_id=project_data.user_id,
            project_name=project_data.project_name,
            domain=project_data.domain
        )
        
        if result["status"] == "user_not_found":
            raise create_error_response("User not found", 404)
        
        if result["status"] == "failed":
            raise create_error_response("Failed to create project", 500)
        
        logger.info(f"Project created: {result['project']['project_id']}")
        return create_success_response(
            message="Project created successfully",
            project=result["project"],
            collections_created=result.get("collections_created")
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Project creation error: {str(e)}")
        raise create_error_response(f"Failed to create project: {str(e)}", 500)


@app.get("/api/v1/projects/user/{user_id}", tags=["Project Management"])
async def get_all_projects_endpoint(user_id: str):
    """
    Get all projects for a user, sorted by most recent
    
    - **user_id**: User identifier
    
    Returns list of all user's projects
    """
    try:
        logger.info(f"Fetching all projects for user: {user_id}")
        projects = get_all_projects(user_id)
        
        return create_success_response(
            message=f"Retrieved {len(projects)} projects",
            total_projects=len(projects),
            projects=projects
        )
        
    except Exception as e:
        logger.error(f"Error fetching projects: {str(e)}")
        raise create_error_response(f"Failed to get projects: {str(e)}", 500)


@app.get("/api/v1/projects/user/{user_id}/recent", tags=["Project Management"])
async def get_recent_projects_endpoint(
    user_id: str,
    limit: int = Query(default=3, ge=1, le=50)
):
    """
    Get recent projects for a user
    
    - **user_id**: User identifier
    - **limit**: Number of projects to return (1-50, default: 3)
    
    Returns most recently used projects
    """
    try:
        logger.info(f"Fetching recent projects: user={user_id}, limit={limit}")
        projects = get_recent_projects(user_id, limit)
        
        return create_success_response(
            message=f"Retrieved {len(projects)} recent projects",
            total_projects=len(projects),
            projects=projects
        )
        
    except Exception as e:
        logger.error(f"Error fetching recent projects: {str(e)}")
        raise create_error_response(f"Failed to get recent projects: {str(e)}", 500)


@app.get("/api/v1/projects/user/{user_id}/count", tags=["Project Management"])
async def get_project_count_endpoint(user_id: str):
    """
    Get total project count for a user
    
    - **user_id**: User identifier
    
    Returns total number of projects
    """
    try:
        logger.info(f"Fetching project count: {user_id}")
        result = get_user_projects_count(user_id)
        
        return create_success_response(
            message="Project count retrieved",
            total_projects=result["total_projects"]
        )
        
    except Exception as e:
        logger.error(f"Error fetching project count: {str(e)}")
        raise create_error_response(f"Failed to get project count: {str(e)}", 500)


@app.put("/api/v1/projects/{project_id}/last-used", tags=["Project Management"])
async def update_project_last_used_endpoint(project_id: str):
    """
    Update project's last_used_at timestamp
    
    - **project_id**: Project identifier (format: UID001PJ001)
    
    Updates timestamp when user accesses the project
    """
    try:
        logger.info(f"Updating last_used_at: {project_id}")
        
        user_id = extract_user_id_from_project_id(project_id)
        updated_project = update_project_last_used(user_id, project_id)
        
        return create_success_response(
            message="Project timestamp updated",
            project=updated_project
        )
        
    except ValueError as e:
        raise create_error_response(str(e), 400)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating timestamp: {str(e)}")
        raise create_error_response(f"Failed to update project: {str(e)}", 500)


@app.delete("/api/v1/projects/{project_id}", tags=["Project Management"])
async def delete_project_endpoint(
    project_id: str,
    user_id: str = Query(...)
):
    """
    Delete a project and all associated data
    
    - **project_id**: Project identifier
    - **user_id**: User identifier (query parameter)
    
    Removes project and all MongoDB/Weaviate collections
    """
    try:
        logger.info(f"Deleting project: {project_id} (user: {user_id})")
        
        deleted_data = delete_project(user_id, project_id)
        
        logger.info(f"Project deleted: {project_id}")
        return create_success_response(
            message="Project deleted successfully",
            deleted_project=deleted_data
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting project: {str(e)}")
        raise create_error_response(f"Failed to delete project: {str(e)}", 500)


# ============================================================================
# Data Upload & Processing Endpoints
# ============================================================================

@app.post("/api/v1/projects/{project_id}/upload", tags=["Data Management"])
async def upload_data(
    project_id: str,
    file: UploadFile = File(...),
    user_id: str = Form(...),
    file_type: str = Form("auto")
):
    """
    Upload data file to a project
    
    - **project_id**: Project identifier
    - **file**: Data file (CSV, Excel, or JSON)
    - **user_id**: User identifier
    - **file_type**: File type (csv, excel, json, or auto)
    
    Returns upload status and data statistics
    """
    mongo_client = None
    try:
        logger.info(f"Data upload: project={project_id}, file={file.filename}")
        
        mongo_client = connect_to_mongodb()
        if not mongo_client:
            raise create_error_response("Database connection failed", 500)
        
        contents = await file.read()
        
        result = upload_data_to_project(
            mongo_client=mongo_client,
            project_id=project_id,
            user_id=user_id,
            file_contents=contents,
            filename=file.filename,
            file_type=file_type
        )
        
        logger.info(f"Upload successful: {result['records_inserted']} records")
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Upload error: {str(e)}")
        raise create_error_response(f"Upload failed: {str(e)}", 500)
    finally:
        if mongo_client:
            mongo_client.close()


@app.get("/api/v1/projects/{project_id}/upload-status", tags=["Data Management"])
async def get_upload_status(
    project_id: str,
    user_id: str = Query(...)
):
    """
    Check project data upload status
    
    - **project_id**: Project identifier
    - **user_id**: User identifier
    
    Returns information about uploaded data
    """
    mongo_client = None
    try:
        logger.info(f"Checking upload status: project={project_id}")
        
        mongo_client = connect_to_mongodb()
        if not mongo_client:
            raise create_error_response("Database connection failed", 500)
        
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
        raise create_error_response(f"Status check failed: {str(e)}", 500)
    finally:
        if mongo_client:
            mongo_client.close()


@app.post("/api/v1/projects/{project_id}/process", tags=["Data Management"])
async def process_project_pipeline(project_id: str):
    """
    Run complete data processing pipeline
    
    - **project_id**: Project identifier
    
    Executes all pipeline steps:
    1. Data type finding
    2. Data anomaly detection
    3. Chart suggestion
    4. Chart pipeline
    5. Data flattening for Weaviate
    6. Vectorization
    7. Data to Weaviate
    
    Returns results from all pipeline steps
    """
    try:
        logger.info(f"Starting pipeline: {project_id}")
        
        results = run_pdp(project_id)
        
        logger.info(f"Pipeline completed: {project_id}")
        return create_success_response(
            message="Data processing completed",
            project_id=project_id,
            results=results
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Pipeline error: {str(e)}")
        raise create_error_response(f"Processing failed: {str(e)}", 500)


# ============================================================================
# AI Agent Endpoints
# ============================================================================

@app.post("/api/v1/agent/query", tags=["AI Agent"])
async def query_middleware(request: MiddlewareQueryRequest):
    """
    Process user query through AI middleware
    
    - **project_id**: Project identifier
    - **query**: User's natural language query (1-1000 characters)
    - **master_db_name**: Database name (default: "master")
    
    The middleware classifies intent and routes to:
    - Data Analysis (Analyst Node)
    - Chart Insights (RAG Charts Node)
    - Data Schema (RAG Data Node)
    - General queries
    
    Returns AI-generated response based on query type
    """
    try:
        logger.info(f"AI query: project={request.project_id}")
        logger.debug(f"Query: {request.query}")
        
        response = run_middleware(
            project_id=request.project_id,
            query=request.query,
            master_db_name=request.master_db_name
        )
        
        logger.info(f"AI query completed: {request.project_id}")
        return create_success_response(
            message="Query processed",
            response=response
        )
        
    except ValueError as e:
        logger.error(f"Validation error: {str(e)}")
        raise create_error_response(str(e), 400)
    except ConnectionError as e:
        logger.error(f"Connection error: {str(e)}")
        raise create_error_response(f"Database connection failed: {str(e)}", 503)
    except Exception as e:
        logger.error(f"AI query error: {str(e)}")
        raise create_error_response(f"Query processing failed: {str(e)}", 500)


# ============================================================================
# Dashboard & Charts Endpoints
# ============================================================================

@app.get("/api/v1/dashboard/{project_id}/charts", tags=["Dashboard"])
async def get_project_charts_endpoint(
    project_id: str,
    user_id: str = Query(...)
):
    """
    Get all charts for a project
    
    - **project_id**: Project identifier
    - **user_id**: User identifier
    
    Returns all available charts
    """
    try:
        logger.info(f"Fetching charts: project={project_id}, user={user_id}")
        
        charts = get_project_charts(user_id, project_id)
        
        return create_success_response(
            message=f"Retrieved {len(charts)} charts",
            project_id=project_id,
            total_charts=len(charts),
            charts=charts
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching charts: {str(e)}")
        raise create_error_response(f"Failed to get charts: {str(e)}", 500)


@app.get("/api/v1/dashboard/{project_id}/charts/{chart_id}", tags=["Dashboard"])
async def get_specific_chart_endpoint(
    project_id: str,
    chart_id: str,
    user_id: str = Query(...)
):
    """
    Get a specific chart by ID
    
    - **project_id**: Project identifier
    - **chart_id**: Chart identifier
    - **user_id**: User identifier
    
    Returns chart details and data
    """
    try:
        logger.info(f"Fetching chart: {chart_id} (project={project_id})")
        
        chart = get_specific_chart(user_id, project_id, chart_id)
        
        return create_success_response(
            message="Chart retrieved",
            project_id=project_id,
            chart_id=chart_id,
            chart=chart
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching chart: {str(e)}")
        raise create_error_response(f"Failed to get chart: {str(e)}", 500)


@app.get("/api/v1/dashboard/{project_id}/chart-types", tags=["Dashboard"])
async def get_chart_types_endpoint(
    project_id: str,
    user_id: str = Query(...)
):
    """
    Get all chart types in project
    
    - **project_id**: Project identifier
    - **user_id**: User identifier
    
    Returns available chart types and counts
    """
    try:
        logger.info(f"Fetching chart types: project={project_id}")
        
        chart_types = get_chart_types(user_id, project_id)
        
        return create_success_response(
            message=f"Retrieved {len(chart_types)} chart types",
            project_id=project_id,
            total_types=len(chart_types),
            chart_types=chart_types
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching chart types: {str(e)}")
        raise create_error_response(f"Failed to get chart types: {str(e)}", 500)


@app.get("/api/v1/dashboard/{project_id}/direct-charts", tags=["Dashboard"])
async def get_direct_charts_endpoint(
    project_id: str,
    user_id: str = Query(...)
):
    """
    Get charts with display_mode='direct' for dashboard
    
    - **project_id**: Project identifier
    - **user_id**: User identifier
    
    Returns charts optimized for dashboard display
    """
    try:
        logger.info(f"Fetching direct charts: project={project_id}")
        
        charts = get_direct_charts(user_id, project_id)
        
        return create_success_response(
            message=f"Retrieved {len(charts)} direct charts",
            project_id=project_id,
            total_direct_charts=len(charts),
            charts=charts
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching direct charts: {str(e)}")
        raise create_error_response(f"Failed to get direct charts: {str(e)}", 500)


@app.get("/api/v1/dashboard/{project_id}/layout", tags=["Dashboard"])
async def get_dashboard_layout_endpoint(
    project_id: str,
    user_id: str = Query(...),
    rows: int = Query(default=2, ge=1, le=10),
    cols: int = Query(default=3, ge=1, le=6)
):
    """
    Get optimized dashboard layout with direct charts
    
    - **project_id**: Project identifier
    - **user_id**: User identifier
    - **rows**: Number of rows in grid (default: 2)
    - **cols**: Number of columns in grid (default: 3)
    
    Returns charts organized in grid layout
    """
    try:
        logger.info(f"Creating layout: project={project_id}, grid={rows}x{cols}")
        
        charts = get_direct_charts(user_id, project_id)
        
        # Organize charts into grid
        grid_layout = []
        max_charts = rows * cols
        
        for i in range(0, len(charts[:max_charts]), cols):
            row = charts[i:i+cols]
            # Pad row if needed
            while len(row) < cols:
                row.append(None)
            grid_layout.append(row)
        
        # Pad rows if needed
        while len(grid_layout) < rows:
            grid_layout.append([None] * cols)
        
        logger.info(f"Layout created: {len(charts)} charts, {rows}x{cols} grid")
        return create_success_response(
            message="Dashboard layout created",
            project_id=project_id,
            total_charts=len(charts),
            grid_config={"rows": rows, "cols": cols},
            grid_layout=grid_layout,
            charts=charts[:max_charts]
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating layout: {str(e)}")
        raise create_error_response(f"Failed to create layout: {str(e)}", 500)


# ============================================================================
# Health & Monitoring
# ============================================================================

@app.get("/health", tags=["System"])
async def health_check():
    """
    Health check endpoint for monitoring
    
    Returns service status and timestamp
    """
    return {
        "status": "healthy",
        "service": "PulseBoard.ai API",
        "version": "2.0.0",
        "timestamp": datetime.now(timezone.utc).isoformat() + "Z"
    }


@app.get("/", tags=["System"])
async def root():
    """
    API root endpoint
    
    Returns API information and documentation links
    """
    return {
        "service": "PulseBoard.ai API",
        "version": "2.0.0",
        "docs": "/docs",
        "redoc": "/redoc",
        "health": "/health"
    }


# ============================================================================
# Application Entry Point
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info",
        access_log=True
    )