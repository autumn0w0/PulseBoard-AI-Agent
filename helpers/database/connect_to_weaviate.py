import os
from dotenv import load_dotenv
from fastapi import HTTPException
from pymongo import MongoClient
from typing import List, Dict, Any
import weaviate
from weaviate.util import generate_uuid5
from weaviate.classes.init import Auth
from datetime import datetime, timedelta, timezone
from pymongo import ReturnDocument

import sys
sys.path.append("../..")
from helpers.database.thread_shared_storage import ThreadUserProjectStorage
from helpers.logger import get_logger

logger = get_logger()
load_dotenv()

weaviate_api_key = os.getenv("WEAVIATE_API_KEY")
weaviate_host = os.getenv("WEAVIATE_HOST")
weaviate_port = os.getenv("WEAVIATE_PORT")
weaviate_is_secure = os.getenv("WEAVIATE_SECURE")
weaviate_grpc_host = os.getenv("WEAVIATE_GRPC_HOST")
weaviate_grpc_port = os.getenv("WEAVIATE_GRPC_PORT")
DATASET_TYPE = "DATASET"
# Construct the MongoDB connection string

def connect_to_weaviatedb():
    """
    Connects to the Weaviate DB instance using the provided connection string.

    Returns:
        WeaviateClient: The MongoDB client instance.
    """
    try:
        if not all(
            [weaviate_host, weaviate_port, weaviate_grpc_host, weaviate_grpc_port]
        ):
            raise ValueError("Missing required Weaviate connection parameters.")

        # Use secure or insecure connection based on configuration
        auth = Auth.api_key(weaviate_api_key) if weaviate_api_key else None

        client = weaviate.connect_to_custom(
            http_host=weaviate_host,
            http_port=weaviate_port,
            http_secure=weaviate_is_secure,
            grpc_host=weaviate_grpc_host,
            grpc_port=weaviate_grpc_port,
            grpc_secure=weaviate_is_secure,
            auth_credentials=auth,
        )

        if client.is_ready():
            logger.debug("Connected successfully to WeaviateDB!")
            return client
        else:
            logger.error("Weaviate client is not ready.")
            return None
    except Exception as e:
        logger.error(f"Failed to connect to WeaviateDB: {e}")
        return None

def get_project_weaviate_collections(project_id: str) -> Dict[str, Any]:
    """
    Returns a dictionary of Weaviate client objects for a given project.
    """
    # Connect to Weaviate
    weaviate_client = connect_to_weaviatedb()
    if not weaviate_client:
        raise HTTPException(status_code=500, detail="Weaviate connection failed")

    # Get user/project config
    storage = ThreadUserProjectStorage().get_thread_storage()
    return storage.get_project_weaviate_collections(project_id)