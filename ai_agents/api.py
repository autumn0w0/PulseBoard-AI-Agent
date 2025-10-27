from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from bson import ObjectId
import sys
import os
from ai_agents.registration.user_creation import run_user_creation
from ai_agents.registration.project_creation import run_project_creation
from ai_agents.pipelines.data_type_finding import run_dtf
from ai_agents.pipelines.data_anomaly import run_cdt
from ai_agents.pipelines.chart_suggestion import run_cs
from ai_agents.pipelines.data_cleaning import run_chart_pipeline
from helpers.logger import get_logger

logger = get_logger(__name__)

app = FastAPI()

class UserCreateRequest(BaseModel):
    email: str
    first_name: str
    last_name: str

class ProjectCreateRequest(BaseModel):
    user_id: str
    project_name: str
    domain: str

class DataPipelineRequest(BaseModel):
    project_id: str

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

@app.post("/users/create")
async def create_user(user_data: UserCreateRequest):
    try:
        logger.info(f"Creating user with email: {user_data.email}")
        
        result = run_user_creation(
            email=user_data.email,
            first_name=user_data.first_name,
            last_name=user_data.last_name
        )
        
        if result["status"] == "user_already_exists":
            logger.warning(f"User already exists: {user_data.email}")
            raise HTTPException(status_code=409, detail="User already exists")
        
        # Convert ObjectId to string
        if result.get("user") and "_id" in result["user"]:
            result["user"]["_id"] = str(result["user"]["_id"])
        if result.get("client_config") and "_id" in result["client_config"]:
            result["client_config"]["_id"] = str(result["client_config"]["_id"])
        
        logger.info(f"User created successfully: {result['user']['user_id']}")
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating user: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/projects/create")
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

@app.post("/data/process-pipeline")
async def process_data_pipeline(pipeline_data: DataPipelineRequest):
    """
    Run the complete data processing pipeline:
    1. Data Type Finding (run_dtf)
    2. Data Anomaly Detection (run_cdt)
    3. Chart Suggestion (run_cs)
    4. Data Cleaning/Chart Pipeline (run_chart_pipeline)
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)