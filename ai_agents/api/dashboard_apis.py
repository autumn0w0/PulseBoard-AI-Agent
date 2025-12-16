from fastapi import APIRouter, HTTPException, UploadFile
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple
from bson import ObjectId
import json
import sys
import io
import pandas as pd
import numpy as np
from functools import lru_cache
from contextlib import contextmanager

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

# Constants
SUPPORTED_FILE_EXTENSIONS = {
    '.csv': 'csv',
    '.xlsx': 'excel',
    '.xls': 'excel',
    '.json': 'json'
}

PIPELINE_STEPS = [
    ("data_type_finding", run_dtf, "Data type finding"),
    ("data_anomaly", run_cdt, "Data anomaly detection"),
    ("chart_suggestion", run_cs, "Chart suggestion"),
    ("chart_pipeline", run_chart_pipeline, "Chart pipeline"),
    ("data_flattened_weaviate", run_dfw, "Data flattening for Weaviate"),
    ("vectorization", run_v, "Vectorization"),
    ("data_to_weaviate", run_dtw, "Data to Weaviate")
]


@contextmanager
def get_mongo_connection():
    """Context manager for MongoDB connections"""
    client = connect_to_mongodb()
    if not client:
        raise HTTPException(status_code=500, detail="Database connection failed")
    try:
        yield client
    finally:
        # Add cleanup if needed
        pass


def run_pdp(project_id: str) -> Dict[str, Any]:
    """
    Run the complete Project Data Pipeline (PDP) for a given project.
    Optimized with unified error handling and logging.
    """
    results = {}
    
    for step_num, (result_key, pipeline_func, step_name) in enumerate(PIPELINE_STEPS, 1):
        logger.info(f"Step {step_num}: Running {step_name} for project {project_id}")
        try:
            results[result_key] = pipeline_func(project_id)
            logger.info(f"{step_name} completed for project {project_id}")
        except Exception as e:
            logger.error(f"Error in {step_name}: {str(e)}")
            raise HTTPException(
                status_code=500, 
                detail=f"{step_name} failed: {str(e)}"
            )
    
    logger.info(f"Project Data Pipeline completed successfully for project {project_id}")
    return results


def serialize_mongo_doc(doc: Dict) -> Dict:
    """Convert MongoDB document to JSON-serializable format"""
    if doc is None:
        return None
    
    return {
        key: (
            str(value) if isinstance(value, ObjectId) else
            serialize_mongo_doc(value) if isinstance(value, dict) else
            [serialize_mongo_doc(item) if isinstance(item, dict) else item 
             for item in value] if isinstance(value, list) else
            value
        )
        for key, value in doc.items()
    }


def format_timestamp(timestamp: Any) -> Optional[str]:
    """Convert various timestamp formats to ISO string"""
    if timestamp is None:
        return None
    if isinstance(timestamp, dict) and "$date" in timestamp:
        return timestamp["$date"].isoformat() + "Z"
    if isinstance(timestamp, datetime):
        return timestamp.isoformat() + "Z"
    return None


def format_project_timestamps(project: Dict) -> None:
    """Format created_at and last_used_at timestamps in place"""
    project["created_at"] = format_timestamp(project.get("created_at"))
    project["last_used_at"] = format_timestamp(project.get("last_used_at"))
    project.pop("mongodb", None)
    project.pop("weaviate", None)


def get_last_used_datetime(project: Dict) -> datetime:
    """Extract datetime from project for sorting"""
    last_used = project.get("last_used_at")
    if isinstance(last_used, dict) and "$date" in last_used:
        return last_used["$date"]
    if isinstance(last_used, datetime):
        return last_used
    return datetime.min


def get_user_details(user_id: str) -> Dict[str, Any]:
    """Get user details from the user collection"""
    with get_mongo_connection() as mongo_client:
        users_collection = mongo_client["master"]["user"]
        user = users_collection.find_one({"user_id": user_id})
        
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        user["_id"] = str(user["_id"])
        user.pop("password", None)
        
        logger.info(f"Retrieved user details for user_id: {user_id}")
        return user


def get_projects_sorted(user_id: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Get projects for a user, sorted by last_used_at.
    Unified function for both recent and all projects.
    """
    with get_mongo_connection() as mongo_client:
        client_config = mongo_client["master"]["client_config"].find_one(
            {"user_id": user_id}
        )
        
        if not client_config or "projects" not in client_config:
            logger.info(f"No projects found for user_id: {user_id}")
            return []
        
        projects = client_config.get("projects", [])
        sorted_projects = sorted(projects, key=get_last_used_datetime, reverse=True)
        
        if limit:
            sorted_projects = sorted_projects[:limit]
        
        for project in sorted_projects:
            format_project_timestamps(project)
        
        logger.info(f"Retrieved {len(sorted_projects)} projects for user_id: {user_id}")
        return sorted_projects


def get_recent_projects(user_id: str, limit: int = 3) -> List[Dict[str, Any]]:
    """Get recent projects for a user"""
    return get_projects_sorted(user_id, limit)


def get_all_projects(user_id: str) -> List[Dict[str, Any]]:
    """Get all projects for a user"""
    return get_projects_sorted(user_id)


def update_project_last_used(user_id: str, project_id: str) -> Dict[str, Any]:
    """Update the last_used_at timestamp for a project"""
    with get_mongo_connection() as mongo_client:
        client_config_collection = mongo_client["master"]["client_config"]
        current_time = datetime.now(timezone.utc)
        
        result = client_config_collection.find_one_and_update(
            {"user_id": user_id, "projects.project_id": project_id},
            {"$set": {"projects.$.last_used_at": {"$date": current_time}}},
            return_document=True
        )
        
        if not result:
            raise HTTPException(status_code=404, detail="Project not found")
        
        for project in result.get("projects", []):
            if project.get("project_id") == project_id:
                format_project_timestamps(project)
                logger.info(f"Updated last_used_at for project: {project_id}")
                return project
        
        raise HTTPException(status_code=500, detail="Failed to retrieve updated project")


def get_user_projects_count(user_id: str) -> Dict[str, int]:
    """Get the count of projects for a user"""
    with get_mongo_connection() as mongo_client:
        client_config = mongo_client["master"]["client_config"].find_one(
            {"user_id": user_id}
        )
        
        total = len(client_config.get("projects", [])) if client_config else 0
        logger.info(f"Retrieved project count for user_id: {user_id} - total: {total}")
        return {"total_projects": total}


def validate_project_access(
    mongo_client,
    user_id: str,
    project_id: str
) -> Tuple[Dict, str]:
    """Validate if project exists and belongs to user"""
    client_config = mongo_client["master"]["client_config"].find_one(
        {"user_id": user_id}
    )
    
    if not client_config:
        raise HTTPException(status_code=404, detail="User not found")
    
    project_exists = any(
        project.get("project_id") == project_id 
        for project in client_config.get("projects", [])
    )
    
    if not project_exists:
        raise HTTPException(status_code=404, detail="Project not found")
    
    return client_config, client_config.get("db_name", user_id)


def delete_collections(client, collection_names: Dict, collection_type: str) -> List[str]:
    """Generic function to delete collections"""
    deleted = []
    for collection_name in collection_names.values():
        try:
            if collection_type == "mongodb":
                client[collection_name].drop()
            else:  # weaviate
                client.collections.delete(collection_name)
            deleted.append(collection_name)
            logger.info(f"Deleted {collection_type} collection: {collection_name}")
        except Exception as e:
            logger.warning(f"Failed to delete {collection_type} collection {collection_name}: {str(e)}")
    return deleted


def delete_project(user_id: str, project_id: str) -> Dict[str, Any]:
    """Delete a project and all its associated data"""
    with get_mongo_connection() as mongo_client:
        client_config, db_name = validate_project_access(mongo_client, user_id, project_id)
        
        # Find the project
        project_to_delete = next(
            (p for p in client_config.get("projects", []) if p.get("project_id") == project_id),
            None
        )
        
        if not project_to_delete:
            raise HTTPException(status_code=404, detail="Project not found")
        
        # Delete MongoDB collections
        mongo_collections_deleted = []
        if "mongodb" in project_to_delete and "collections" in project_to_delete["mongodb"]:
            user_db = mongo_client[db_name]
            mongo_collections_deleted = delete_collections(
                user_db, 
                project_to_delete["mongodb"]["collections"],
                "mongodb"
            )
        
        # Delete Weaviate collections
        weaviate_collections_deleted = []
        if "weaviate" in project_to_delete and "collections" in project_to_delete["weaviate"]:
            try:
                weaviate_client = connect_to_weaviatedb()
                if weaviate_client:
                    weaviate_collections_deleted = delete_collections(
                        weaviate_client,
                        project_to_delete["weaviate"]["collections"],
                        "weaviate"
                    )
            except Exception as e:
                logger.error(f"Error connecting to Weaviate: {str(e)}")
        
        # Remove project from config
        result = mongo_client["master"]["client_config"].update_one(
            {"user_id": user_id},
            {"$pull": {"projects": {"project_id": project_id}}}
        )
        
        if result.modified_count == 0:
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


def detect_file_type(filename: str, file_type: str) -> str:
    """Detect file type based on extension or provided type"""
    if file_type != "auto":
        return file_type
    
    extension = '.' + filename.lower().rsplit('.', 1)[-1] if '.' in filename else ''
    detected_type = SUPPORTED_FILE_EXTENSIONS.get(extension)
    
    if not detected_type:
        raise HTTPException(status_code=400, detail="Unsupported file type")
    
    return detected_type


def parse_file_to_dataframe(contents: bytes, file_type: str) -> pd.DataFrame:
    """Parse file contents into pandas DataFrame"""
    parsers = {
        'csv': pd.read_csv,
        'excel': pd.read_excel,
        'json': pd.read_json
    }
    
    parser = parsers.get(file_type)
    if not parser:
        raise HTTPException(status_code=400, detail="Unsupported file type")
    
    try:
        return parser(io.BytesIO(contents))
    except Exception as e:
        logger.error(f"Error parsing file: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Error parsing file: {str(e)}")


def clean_dataframe_for_json(df: pd.DataFrame) -> pd.DataFrame:
    """Clean DataFrame to ensure JSON compatibility"""
    df = df.replace({pd.NA: None, pd.NaT: None, np.nan: None, np.inf: None, -np.inf: None})
    return df.where(pd.notnull(df), None)


def upload_data_to_project(
    mongo_client,
    project_id: str,
    user_id: str,
    file_contents: bytes,
    filename: str,
    file_type: str = "auto"
) -> Dict[str, Any]:
    """Upload data file to a project"""
    logger.info(f"Uploading data for project: {project_id}, user: {user_id}")
    
    client_config, db_name = validate_project_access(mongo_client, user_id, project_id)
    detected_file_type = detect_file_type(filename, file_type)
    
    df = parse_file_to_dataframe(file_contents, detected_file_type)
    df = clean_dataframe_for_json(df)
    
    records = df.to_dict('records')
    if not records:
        raise HTTPException(status_code=400, detail="No data found in file")
    
    collection_name = f"{project_id}_data"
    collection = mongo_client[db_name][collection_name]
    
    collection.delete_many({})
    result = collection.insert_many(records)
    records_inserted = len(result.inserted_ids)
    
    logger.info(f"Uploaded {records_inserted} records to {collection_name}")
    
    sample_data = [
        {k: v for k, v in record.items() if k != '_id'} 
        for record in records[:5]
    ]
    
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
    """Check if project has data uploaded"""
    logger.info(f"Checking upload status for project: {project_id}, user: {user_id}")
    
    client_config, db_name = validate_project_access(mongo_client, user_id, project_id)
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
    
    last_uploaded = None
    if count > 0:
        last_doc = collection.find_one(sort=[("_id", -1)])
        if last_doc and "_id" in last_doc:
            try:
                last_uploaded = last_doc["_id"].generation_time.isoformat() + "Z"
            except Exception as e:
                logger.warning(f"Could not extract timestamp: {str(e)}")
    
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


def convert_chart_objectids(chart: Dict) -> None:
    """Convert ObjectIds in chart to strings in place"""
    chart['_id'] = str(chart['_id'])
    chart['chart_id'] = str(chart['chart_id'])
    
    if 'data' in chart and isinstance(chart['data'], list):
        for item in chart['data']:
            if isinstance(item, dict) and '_id' in item and isinstance(item['_id'], ObjectId):
                item['_id'] = str(item['_id'])


def get_project_charts_collection(mongo_client, user_id: str, project_id: str):
    """Get charts collection for a project with validation"""
    client_config = mongo_client["master"]["client_config"].find_one({"user_id": user_id})
    
    if not client_config or "projects" not in client_config:
        raise HTTPException(status_code=404, detail=f"User {user_id} not found or has no projects")
    
    project_exists = any(
        p.get("project_id") == project_id 
        for p in client_config.get("projects", [])
    )
    
    if not project_exists:
        raise HTTPException(status_code=404, detail=f"Project {project_id} not found")
    
    collection_name = f"{project_id}_cleaned_data"
    project_db = mongo_client[user_id]
    
    if collection_name not in project_db.list_collection_names():
        raise HTTPException(status_code=404, detail=f"No charts found for project {project_id}")
    
    return project_db[collection_name]


def get_project_charts(user_id: str, project_id: str) -> List[Dict[str, Any]]:
    """Get all charts for a specific project"""
    with get_mongo_connection() as mongo_client:
        charts_collection = get_project_charts_collection(mongo_client, user_id, project_id)
        charts = list(charts_collection.find({}).sort("chart_id", 1))
        
        for chart in charts:
            convert_chart_objectids(chart)
        
        logger.info(f"Retrieved {len(charts)} charts for project {project_id}")
        return charts


def get_specific_chart(user_id: str, project_id: str, chart_id: str) -> Dict[str, Any]:
    """Get a specific chart by chart_id"""
    with get_mongo_connection() as mongo_client:
        charts_collection = get_project_charts_collection(mongo_client, user_id, project_id)
        
        # Try ObjectId conversion first, fallback to string
        try:
            chart = charts_collection.find_one({"chart_id": ObjectId(chart_id)})
        except:
            chart = charts_collection.find_one({"chart_id": chart_id})
        
        if not chart:
            raise HTTPException(status_code=404, detail=f"Chart {chart_id} not found")
        
        convert_chart_objectids(chart)
        logger.info(f"Retrieved chart {chart_id} for project {project_id}")
        return chart


def get_chart_types(user_id: str, project_id: str) -> List[Dict[str, Any]]:
    """Get all chart types available in the project"""
    with get_mongo_connection() as mongo_client:
        charts_collection = get_project_charts_collection(mongo_client, user_id, project_id)
        chart_types = charts_collection.distinct("chart_type")
        
        chart_counts = [
            {"type": chart_type, "count": charts_collection.count_documents({"chart_type": chart_type})}
            for chart_type in chart_types
        ]
        
        logger.info(f"Retrieved {len(chart_counts)} chart types for project {project_id}")
        return chart_counts


def get_direct_charts(user_id: str, project_id: str) -> List[Dict[str, Any]]:
    """Get only charts with display_mode = 'direct' for dashboard display"""
    with get_mongo_connection() as mongo_client:
        try:
            charts_collection = get_project_charts_collection(mongo_client, user_id, project_id)
        except HTTPException as e:
            if "No charts found" in str(e.detail):
                return []
            raise
        
        charts = list(charts_collection.find({"display_mode": "direct"}).sort("chart_id", 1))
        
        for chart in charts:
            convert_chart_objectids(chart)
        
        logger.info(f"Retrieved {len(charts)} direct charts for project {project_id}")
        return charts


class JSONEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle ObjectId and datetime"""
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat() + "Z"
        return super().default(obj)