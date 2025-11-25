from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from bson import ObjectId
import sys
import os
from fastapi import HTTPException
from pydantic import BaseModel, EmailStr, field_validator
import re
from fastapi import HTTPException
from pydantic import BaseModel, EmailStr, field_validator
import re

from ai_agents.registration.user_creation import run_user_creation
from ai_agents.registration.project_creation import run_project_creation
from ai_agents.pipelines.data_type_finding import run_dtf
from ai_agents.pipelines.data_anomaly import run_cdt
from ai_agents.pipelines.chart_suggestion import run_cs
from ai_agents.pipelines.data_cleaning import run_chart_pipeline
from ai_agents.agent.middleware_node import run_middleware
from helpers.logger import get_logger
from ai_agents.pipelines.data_flatted_weviate import run_dfw
from ai_agents.pipelines.vectorization import run_v
from ai_agents.pipelines.data_to_weviate import run_dtw
from ai_agents.registration.user_login import run_user_login

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
async def process_data_pipeline(pipeline_data: DataPipelineRequest):
    """
    Run the complete data processing pipeline:
    1. Data Type Finding (run_dtf)
    2. Data Anomaly Detection (run_cdt)
    3. Chart Suggestion (run_cs)
    4. Data Cleaning/Chart Pipeline (run_chart_pipeline)
    5. Data Flattening for Weaviate (run_dfw)
    6. Vectorization (run_v)
    7. Data to Weaviate (run_dtw)
    """
    try:
        project_id = pipeline_data.project_id
        logger.info(f"Starting data pipeline for project: {project_id}")
        
        results = {}
        
        # Step 1: Run Data Type Finding
        logger.info(f"Step 1: Running data type finding for project {project_id}")
        try:
            dtf_result = run_dtf(project_id)
            results["data_type_finding"] = dtf_result
            logger.info(f"Data type finding completed for project {project_id}")
        except Exception as e:
            logger.error(f"Error in data type finding: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Data type finding failed: {str(e)}")
        
        # Step 2: Run Data Anomaly Detection
        logger.info(f"Step 2: Running data anomaly detection for project {project_id}")
        try:
            cdt_result = run_cdt(project_id)
            results["data_anomaly"] = cdt_result
            logger.info(f"Data anomaly detection completed for project {project_id}")
        except Exception as e:
            logger.error(f"Error in data anomaly detection: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Data anomaly detection failed: {str(e)}")
        
        # Step 3: Run Chart Suggestion
        logger.info(f"Step 3: Running chart suggestion for project {project_id}")
        try:
            cs_result = run_cs(project_id)
            results["chart_suggestion"] = cs_result
            logger.info(f"Chart suggestion completed for project {project_id}")
        except Exception as e:
            logger.error(f"Error in chart suggestion: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Chart suggestion failed: {str(e)}")
        
        # Step 4: Run Chart Pipeline
        logger.info(f"Step 4: Running chart pipeline for project {project_id}")
        try:
            chart_pipeline_result = run_chart_pipeline(project_id)
            results["chart_pipeline"] = chart_pipeline_result
            logger.info(f"Chart pipeline completed for project {project_id}")
        except Exception as e:
            logger.error(f"Error in chart pipeline: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Chart pipeline failed: {str(e)}")
        
        # Step 5: Run Data Flattening for Weaviate
        logger.info(f"Step 5: Running data flattening for Weaviate for project {project_id}")
        try:
            dfw_result = run_dfw(project_id)
            results["data_flattened_weaviate"] = dfw_result
            logger.info(f"Data flattening for Weaviate completed for project {project_id}")
        except Exception as e:
            logger.error(f"Error in data flattening for Weaviate: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Data flattening for Weaviate failed: {str(e)}")
        
        # Step 6: Run Vectorization
        logger.info(f"Step 6: Running vectorization for project {project_id}")
        try:
            v_result = run_v(project_id)
            results["vectorization"] = v_result
            logger.info(f"Vectorization completed for project {project_id}")
        except Exception as e:
            logger.error(f"Error in vectorization: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Vectorization failed: {str(e)}")
        
        # Step 7: Run Data to Weaviate
        logger.info(f"Step 7: Running data to Weaviate for project {project_id}")
        try:
            dtw_result = run_dtw(project_id)
            results["data_to_weaviate"] = dtw_result
            logger.info(f"Data to Weaviate completed for project {project_id}")
        except Exception as e:
            logger.error(f"Error in data to Weaviate: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Data to Weaviate failed: {str(e)}")
        
        logger.info(f"Data pipeline completed successfully for project {project_id}")
        
        # Convert all ObjectId instances to strings before returning
        response = {
            "status": "success",
            "project_id": project_id,
            "results": convert_objectid_to_str(results),
            "message": "Data pipeline executed successfully"
        }
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in data pipeline: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

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