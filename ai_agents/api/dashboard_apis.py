from fastapi import APIRouter, HTTPException, UploadFile
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple
from bson import ObjectId
import sys
import io
import pandas as pd
import numpy as np

sys.path.append("../..")
from pipelines.processing.data_type_finding import run_dtf
from pipelines.processing.data_anomaly import run_cdt
from pipelines.processing.chart_suggestion import run_cs
from pipelines.processing.data_cleaning import run_chart_pipeline
from pipelines.processing.data_flatted_weviate import run_dfw
from pipelines.processing.vectorization import run_v
from pipelines.processing.data_to_weviate import run_dtw
from helpers.database.connection_to_db import connect_to_mongodb
from helpers.database.connect_to_weaviate import connect_to_weaviatedb

# Initialize logger
logger = logging.getLogger(__name__)

# Initialize router
router = APIRouter()


def run_pdp(project_id: str) -> Dict[str, Any]:
    """
    Run the complete Project Data Pipeline (PDP) for a given project.
    
    Args:
        project_id: The unique identifier for the project
        
    Returns:
        Dictionary containing results from all pipeline steps
        
    Raises:
        HTTPException: If any step in the pipeline fails
    """
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
    
    logger.info(f"Project Data Pipeline completed successfully for project {project_id}")
    return results

def get_user_details(user_id: str) -> Dict[str, Any]:
    """
    Get user details from the user collection
    
    Args:
        user_id: The user's unique identifier
        
    Returns:
        Dictionary containing user details
        
    Raises:
        HTTPException: If user not found or database error
    """
    try:
        # Connect to MongoDB
        mongo_client = connect_to_mongodb()
        if not mongo_client:
            logger.error("Failed to connect to MongoDB")
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        # Access the master database and user collection
        master_db = mongo_client["master"]
        users_collection = master_db["user"]
        
        # Find user by user_id
        user = users_collection.find_one({"user_id": user_id})
        
        if not user:
            logger.warning(f"User not found with user_id: {user_id}")
            raise HTTPException(status_code=404, detail="User not found")
        
        # Convert ObjectId to string and remove sensitive data
        user["_id"] = str(user["_id"])
        user.pop("password", None)  # Remove password for security
        
        logger.info(f"Retrieved user details for user_id: {user_id}")
        return user
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving user details: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve user details: {str(e)}")

def get_recent_projects(user_id: str, limit: int = 3) -> List[Dict[str, Any]]:
    """
    Get recent projects for a user, sorted by last_used_at (most recent first)
    
    Args:
        user_id: The user's unique identifier
        limit: Maximum number of projects to return (default: 3)
        
    Returns:
        List of project dictionaries
        
    Raises:
        HTTPException: If database error occurs
    """
    try:
        # Connect to MongoDB
        mongo_client = connect_to_mongodb()
        if not mongo_client:
            logger.error("Failed to connect to MongoDB")
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        # Access the master database and client_config collection
        master_db = mongo_client["master"]
        client_config_collection = master_db["client_config"]
        
        # Find user's client config
        client_config = client_config_collection.find_one({"user_id": user_id})
        
        if not client_config or "projects" not in client_config:
            logger.info(f"No projects found for user_id: {user_id}")
            return []
        
        # Get projects array
        projects = client_config.get("projects", [])
        
        # Helper function to extract datetime for sorting
        def get_last_used_datetime(project):
            last_used = project.get("last_used_at")
            if last_used is None:
                return datetime.min
            # Handle both formats: direct datetime or {"$date": datetime}
            if isinstance(last_used, dict) and "$date" in last_used:
                return last_used["$date"]
            elif isinstance(last_used, datetime):
                return last_used
            else:
                return datetime.min
        
        # Sort projects by last_used_at in descending order (most recent first)
        sorted_projects = sorted(
            projects, 
            key=get_last_used_datetime,
            reverse=True
        )
        
        # Limit the number of projects
        recent_projects = sorted_projects[:limit]
        
        # Convert timestamps to ISO format strings
        for project in recent_projects:
            # Convert created_at
            created_at = project.get("created_at")
            if isinstance(created_at, dict) and "$date" in created_at:
                project["created_at"] = created_at["$date"].isoformat() + "Z"
            elif isinstance(created_at, datetime):
                project["created_at"] = created_at.isoformat() + "Z"
            
            # Convert last_used_at
            last_used_at = project.get("last_used_at")
            if isinstance(last_used_at, dict) and "$date" in last_used_at:
                project["last_used_at"] = last_used_at["$date"].isoformat() + "Z"
            elif isinstance(last_used_at, datetime):
                project["last_used_at"] = last_used_at.isoformat() + "Z"
            
            # Remove MongoDB/Weaviate collections if present to reduce response size
            project.pop("mongodb", None)
            project.pop("weaviate", None)
        
        logger.info(f"Retrieved {len(recent_projects)} recent projects for user_id: {user_id}")
        return recent_projects
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving recent projects: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve recent projects: {str(e)}")

def get_all_projects(user_id: str) -> List[Dict[str, Any]]:
    """
    Get all projects for a user, sorted by last_used_at (most recent first)
    
    Args:
        user_id: The user's unique identifier
        
    Returns:
        List of all project dictionaries
        
    Raises:
        HTTPException: If database error occurs
    """
    try:
        # Connect to MongoDB
        mongo_client = connect_to_mongodb()
        if not mongo_client:
            logger.error("Failed to connect to MongoDB")
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        # Access the master database and client_config collection
        master_db = mongo_client["master"]
        client_config_collection = master_db["client_config"]
        
        # Find user's client config
        client_config = client_config_collection.find_one({"user_id": user_id})
        
        if not client_config or "projects" not in client_config:
            logger.info(f"No projects found for user_id: {user_id}")
            return []
        
        # Get projects array
        projects = client_config.get("projects", [])
        
        # Helper function to extract datetime for sorting
        def get_last_used_datetime(project):
            last_used = project.get("last_used_at")
            if last_used is None:
                return datetime.min
            # Handle both formats: direct datetime or {"$date": datetime}
            if isinstance(last_used, dict) and "$date" in last_used:
                return last_used["$date"]
            elif isinstance(last_used, datetime):
                return last_used
            else:
                return datetime.min
        
        # Sort projects by last_used_at in descending order (most recent first)
        sorted_projects = sorted(
            projects, 
            key=get_last_used_datetime,
            reverse=True
        )
        
        # Convert timestamps to ISO format strings
        for project in sorted_projects:
            # Convert created_at
            created_at = project.get("created_at")
            if isinstance(created_at, dict) and "$date" in created_at:
                project["created_at"] = created_at["$date"].isoformat() + "Z"
            elif isinstance(created_at, datetime):
                project["created_at"] = created_at.isoformat() + "Z"
            
            # Convert last_used_at
            last_used_at = project.get("last_used_at")
            if isinstance(last_used_at, dict) and "$date" in last_used_at:
                project["last_used_at"] = last_used_at["$date"].isoformat() + "Z"
            elif isinstance(last_used_at, datetime):
                project["last_used_at"] = last_used_at.isoformat() + "Z"
            
            # Remove MongoDB/Weaviate collections if present to reduce response size
            project.pop("mongodb", None)
            project.pop("weaviate", None)
        
        logger.info(f"Retrieved {len(sorted_projects)} projects for user_id: {user_id}")
        return sorted_projects
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving all projects: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve projects: {str(e)}")

def update_project_last_used(user_id: str, project_id: str) -> Dict[str, Any]:
    """
    Update the last_used_at timestamp for a project
    
    Args:
        user_id: The user's unique identifier
        project_id: The project's unique identifier
        
    Returns:
        Updated project information
        
    Raises:
        HTTPException: If project not found or database error
    """
    try:
        # Connect to MongoDB
        mongo_client = connect_to_mongodb()
        if not mongo_client:
            logger.error("Failed to connect to MongoDB")
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        # Access the master database and client_config collection
        master_db = mongo_client["master"]
        client_config_collection = master_db["client_config"]
        
        # Current timestamp in UTC
        current_time = datetime.now(timezone.utc)
        
        # Update the specific project's last_used_at field
        # Using arrayFilters to target the specific project within the projects array
        result = client_config_collection.find_one_and_update(
            {
                "user_id": user_id,
                "projects.project_id": project_id
            },
            {
                "$set": {
                    "projects.$.last_used_at": {"$date": current_time}
                }
            },
            return_document=True
        )
        
        if not result:
            logger.warning(f"Project not found: user_id={user_id}, project_id={project_id}")
            raise HTTPException(status_code=404, detail="Project not found")
        
        # Find the updated project
        updated_project = None
        for project in result.get("projects", []):
            if project.get("project_id") == project_id:
                updated_project = project
                
                # Convert timestamps to ISO format
                if "created_at" in project and "$date" in project["created_at"]:
                    project["created_at"] = project["created_at"]["$date"].isoformat() + "Z"
                
                if "last_used_at" in project and "$date" in project["last_used_at"]:
                    project["last_used_at"] = project["last_used_at"]["$date"].isoformat() + "Z"
                
                # Remove MongoDB/Weaviate collections
                project.pop("mongodb", None)
                project.pop("weaviate", None)
                break
        
        if not updated_project:
            logger.error(f"Failed to find updated project after update: {project_id}")
            raise HTTPException(status_code=500, detail="Failed to retrieve updated project")
        
        logger.info(f"Updated last_used_at for project: {project_id} to {current_time}")
        return updated_project
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating project last_used_at: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to update project: {str(e)}")

def get_user_projects_count(user_id: str) -> Dict[str, int]:
    """
    Get the count of projects for a user
    
    Args:
        user_id: The user's unique identifier
        
    Returns:
        Dictionary with project count
        
    Raises:
        HTTPException: If database error occurs
    """
    try:
        # Connect to MongoDB
        mongo_client = connect_to_mongodb()
        if not mongo_client:
            logger.error("Failed to connect to MongoDB")
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        # Access the master database and client_config collection
        master_db = mongo_client["master"]
        client_config_collection = master_db["client_config"]
        
        # Find user's client config
        client_config = client_config_collection.find_one({"user_id": user_id})
        
        if not client_config or "projects" not in client_config:
            return {"total_projects": 0}
        
        total_projects = len(client_config.get("projects", []))
        
        logger.info(f"Retrieved project count for user_id: {user_id} - total: {total_projects}")
        return {"total_projects": total_projects}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error counting user projects: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to count projects: {str(e)}")
    
def delete_project(user_id: str, project_id: str) -> Dict[str, Any]:
    """
    Delete a project and all its associated data from MongoDB and Weaviate
    
    Args:
        user_id: The user's unique identifier
        project_id: The project's unique identifier
        
    Returns:
        Dictionary containing deletion results
        
    Raises:
        HTTPException: If project not found or deletion fails
    """
    try:
        # Connect to MongoDB
        mongo_client = connect_to_mongodb()
        if not mongo_client:
            logger.error("Failed to connect to MongoDB")
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        # Access the master database and client_config collection
        master_db = mongo_client["master"]
        client_config_collection = master_db["client_config"]
        
        # First, find the project to get collection names
        client_config = client_config_collection.find_one({"user_id": user_id})
        
        if not client_config:
            logger.warning(f"User not found: {user_id}")
            raise HTTPException(status_code=404, detail="User not found")
        
        # Find the specific project
        project_to_delete = None
        project_index = -1
        for idx, project in enumerate(client_config.get("projects", [])):
            if project.get("project_id") == project_id:
                project_to_delete = project
                project_index = idx
                break
        
        if not project_to_delete:
            logger.warning(f"Project not found: {project_id}")
            raise HTTPException(status_code=404, detail="Project not found")
        
        # Get database name for user
        db_name = client_config.get("db_name", user_id)
        
        # Step 1: Delete MongoDB collections
        mongo_collections_deleted = []
        if "mongodb" in project_to_delete and "collections" in project_to_delete["mongodb"]:
            mongo_collections = project_to_delete["mongodb"]["collections"]
            
            for collection_key, collection_name in mongo_collections.items():
                try:
                    # Access the user's database and drop the collection
                    user_db = mongo_client[db_name]
                    collection = user_db[collection_name]
                    
                    # Drop the collection
                    collection.drop()
                    mongo_collections_deleted.append(collection_name)
                    logger.info(f"Deleted MongoDB collection: {collection_name}")
                except Exception as e:
                    logger.warning(f"Failed to delete MongoDB collection {collection_name}: {str(e)}")
        
        # Step 2: Delete Weaviate collections
        weaviate_collections_deleted = []
        if "weaviate" in project_to_delete and "collections" in project_to_delete["weaviate"]:
            weaviate_collections = project_to_delete["weaviate"]["collections"]
            
            try:
                
                # Connect to Weaviate
                weaviate_client = connect_to_weaviatedb()
                if weaviate_client:
                    for collection_key, collection_name in weaviate_collections.items():
                        try:
                            # Delete Weaviate collection
                            weaviate_client.collections.delete(collection_name)
                            weaviate_collections_deleted.append(collection_name)
                            logger.info(f"Deleted Weaviate collection: {collection_name}")
                        except Exception as e:
                            logger.warning(f"Failed to delete Weaviate collection {collection_name}: {str(e)}")
                else:
                    logger.warning("Failed to connect to Weaviate")
            except Exception as e:
                logger.error(f"Error connecting to Weaviate: {str(e)}")
        
        # Step 3: Remove project from client_config
        result = client_config_collection.update_one(
            {"user_id": user_id},
            {"$pull": {"projects": {"project_id": project_id}}}
        )
        
        if result.modified_count == 0:
            logger.error(f"Failed to remove project from client_config: {project_id}")
            raise HTTPException(status_code=500, detail="Failed to remove project from configuration")
        
        logger.info(f"Successfully deleted project: {project_id}")
        
        return {
            "project_id": project_id,
            "project_name": project_to_delete.get("name_of_project", ""),
            "mongo_collections_deleted": mongo_collections_deleted,
            "weaviate_collections_deleted": weaviate_collections_deleted,
            "total_mongo_collections": len(mongo_collections_deleted),
            "total_weaviate_collections": len(weaviate_collections_deleted)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting project: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to delete project: {str(e)}")

def serialize_mongo_doc(doc: Dict) -> Dict:
    """
    Convert MongoDB document to JSON-serializable format
    Converts ObjectId to string
    """
    if doc is None:
        return None
    
    serialized = {}
    for key, value in doc.items():
        if isinstance(value, ObjectId):
            serialized[key] = str(value)
        elif isinstance(value, dict):
            serialized[key] = serialize_mongo_doc(value)
        elif isinstance(value, list):
            serialized[key] = [
                serialize_mongo_doc(item) if isinstance(item, dict) else item
                for item in value
            ]
        else:
            serialized[key] = value
    return serialized

def validate_project_access(
    mongo_client,
    user_id: str,
    project_id: str
) -> Tuple[Dict, str]:
    """
    Validate if project exists and belongs to user
    
    Returns:
        Tuple of (client_config, db_name)
    
    Raises:
        HTTPException: If validation fails
    """
    master_db = mongo_client["master"]
    client_config_collection = master_db["client_config"]
    
    client_config = client_config_collection.find_one({"user_id": user_id})
    if not client_config:
        raise HTTPException(status_code=404, detail="User not found")
    
    project_exists = any(
        project.get("project_id") == project_id 
        for project in client_config.get("projects", [])
    )
    
    if not project_exists:
        raise HTTPException(status_code=404, detail="Project not found")
    
    db_name = client_config.get("db_name", user_id)
    return client_config, db_name


def detect_file_type(filename: str, file_type: str) -> str:
    """
    Detect file type based on extension or provided type
    """
    if file_type != "auto":
        return file_type
    
    filename_lower = filename.lower()
    if filename_lower.endswith('.csv'):
        return 'csv'
    elif filename_lower.endswith(('.xlsx', '.xls')):
        return 'excel'
    elif filename_lower.endswith('.json'):
        return 'json'
    else:
        raise HTTPException(status_code=400, detail="Unsupported file type")



def parse_file_to_dataframe(contents: bytes, file_type: str) -> pd.DataFrame:
    """
    Parse file contents into pandas DataFrame
    """
    try:
        if file_type == 'csv':
            return pd.read_csv(io.BytesIO(contents))
        elif file_type == 'excel':
            return pd.read_excel(io.BytesIO(contents))
        elif file_type == 'json':
            return pd.read_json(io.BytesIO(contents))
        else:
            raise HTTPException(status_code=400, detail="Unsupported file type")
    except Exception as e:
        logger.error(f"Error parsing file: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Error parsing file: {str(e)}")


def upload_data_to_project(
    mongo_client,
    project_id: str,
    user_id: str,
    file_contents: bytes,
    filename: str,
    file_type: str = "auto"
) -> Dict[str, Any]:
    """
    Upload data file to a project
    
    Args:
        mongo_client: MongoDB client
        project_id: Project identifier
        user_id: User identifier
        file_contents: File contents as bytes
        filename: Original filename
        file_type: File type (csv, excel, json, or auto)
    
    Returns:
        Upload status and statistics
    """
    logger.info(f"Uploading data for project: {project_id}, user: {user_id}")
    
    # Validate project access
    client_config, db_name = validate_project_access(mongo_client, user_id, project_id)
    
    # Detect file type
    detected_file_type = detect_file_type(filename, file_type)
    
    # Parse file to DataFrame
    df = parse_file_to_dataframe(file_contents, detected_file_type)
    
    # Convert to records
    records = df.to_dict('records')
    
    if not records:
        raise HTTPException(status_code=400, detail="No data found in file")
    
    # Prepare collection
    collection_name = f"{project_id}_data"
    user_db = mongo_client[db_name]
    collection = user_db[collection_name]
    
    # Clear existing data and insert new data
    collection.delete_many({})
    result = collection.insert_many(records)
    records_inserted = len(result.inserted_ids)
    
    logger.info(f"Uploaded {records_inserted} records to {collection_name}")
    
    # Prepare sample data without ObjectId fields
    sample_data = []
    if records:
        for record in records[:5]:
            # Create a copy without _id field
            sample_record = {k: v for k, v in record.items() if k != '_id'}
            sample_data.append(sample_record)
    
    return {
        "status": "success",
        "message": "Data uploaded successfully",
        "records_inserted": records_inserted,
        "collection_name": collection_name,
        "columns": list(df.columns),
        "sample_data": sample_data
    }

def clean_dataframe_for_json(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean DataFrame to ensure JSON compatibility
    - Replace NaN with None
    - Replace infinity with None
    - Handle other non-JSON-compliant values
    """
    # Replace pandas NA types
    df = df.replace({pd.NA: None, pd.NaT: None})
    
    # Replace NaN and infinity values with None
    df = df.replace([np.nan, np.inf, -np.inf], None)
    
    # Ensure all NaN values are converted to None
    df = df.where(pd.notnull(df), None)
    
    return df

def upload_data_to_project(
    mongo_client,
    project_id: str,
    user_id: str,
    file_contents: bytes,
    filename: str,
    file_type: str = "auto"
) -> Dict[str, Any]:
    """
    Upload data file to a project
    
    Args:
        mongo_client: MongoDB client
        project_id: Project identifier
        user_id: User identifier
        file_contents: File contents as bytes
        filename: Original filename
        file_type: File type (csv, excel, json, or auto)
    
    Returns:
        Upload status and statistics
    """
    logger.info(f"Uploading data for project: {project_id}, user: {user_id}")
    
    # Validate project access
    client_config, db_name = validate_project_access(mongo_client, user_id, project_id)
    
    # Detect file type
    detected_file_type = detect_file_type(filename, file_type)
    
    # Parse file to DataFrame
    df = parse_file_to_dataframe(file_contents, detected_file_type)
    
    # Clean DataFrame for JSON compatibility
    df = clean_dataframe_for_json(df)
    
    # Convert to records
    records = df.to_dict('records')
    
    if not records:
        raise HTTPException(status_code=400, detail="No data found in file")
    
    # Prepare collection
    collection_name = f"{project_id}_data"
    user_db = mongo_client[db_name]
    collection = user_db[collection_name]
    
    # Clear existing data and insert new data
    collection.delete_many({})
    result = collection.insert_many(records)
    records_inserted = len(result.inserted_ids)
    
    logger.info(f"Uploaded {records_inserted} records to {collection_name}")
    
    # Prepare sample data without ObjectId fields and ensure JSON compatibility
    sample_data = []
    if records:
        for record in records[:5]:
            # Create a copy without _id field
            sample_record = {k: v for k, v in record.items() if k != '_id'}
            sample_data.append(sample_record)
    
    return {
        "status": "success",
        "message": "Data uploaded successfully",
        "records_inserted": records_inserted,
        "collection_name": collection_name,
        "columns": list(df.columns),
        "sample_data": sample_data
    }


def get_project_upload_status(
    mongo_client,
    project_id: str,
    user_id: str
) -> Dict[str, Any]:
    """
    Check if project has data uploaded
    
    Args:
        mongo_client: MongoDB client
        project_id: Project identifier
        user_id: User identifier
    
    Returns:
        Upload status information including record count and last upload time
    """
    logger.info(f"Checking upload status for project: {project_id}, user: {user_id}")
    
    # Validate project access
    client_config, db_name = validate_project_access(mongo_client, user_id, project_id)
    
    # Check collection
    collection_name = f"{project_id}_data"
    user_db = mongo_client[db_name]
    
    if collection_name not in user_db.list_collection_names():
        return {
            "status": "success",
            "has_data": False,
            "records_count": 0,
            "collection_name": collection_name
        }
    
    collection = user_db[collection_name]
    count = collection.count_documents({})
    
    # Get last document's upload time if available
    last_uploaded = None
    last_doc = collection.find_one(sort=[("_id", -1)])
    if last_doc and "_id" in last_doc:
        try:
            last_uploaded = last_doc["_id"].generation_time.isoformat() + "Z"
        except Exception as e:
            logger.warning(f"Could not extract timestamp from ObjectId: {str(e)}")
    
    # Get columns from first document if data exists
    columns = []
    if count > 0:
        first_doc = collection.find_one()
        if first_doc:
            columns = [k for k in first_doc.keys() if k != '_id']
    
    return {
        "status": "success",
        "has_data": count > 0,
        "records_count": count,
        "last_uploaded": last_uploaded,
        "collection_name": collection_name,
        "columns": columns
    }