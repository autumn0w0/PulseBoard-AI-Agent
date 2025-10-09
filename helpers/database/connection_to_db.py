import os
from dotenv import load_dotenv
from fastapi import HTTPException
from pymongo import MongoClient
from typing import List, Dict, Any
import traceback
import hashlib
import requests
import weaviate
from weaviate.util import generate_uuid5
from weaviate.classes.init import Auth
from datetime import datetime, timedelta, timezone
import redis
import uuid
from pymongo import ReturnDocument
import ast

import sys
sys.path.append("../..")
from helpers.database.thread_shared_storage import ThreadUserProjectStorage
from helpers.logger import get_logger

logger = get_logger()
load_dotenv()
# mongo
mongo_user = os.getenv("MONGODB_USERNAME")
mongo_password = os.getenv("MONGODB_PASSWORD")
mongo_host = os.getenv("MONGODB_HOST")
mongo_port = os.getenv("MONGODB_PORT")
mongo_auth_mechanism = os.getenv("MONGODB_AUTHMECHANISM")

weaviate_api_key = os.getenv("WEAVIATE_API_KEY")
weaviate_host = os.getenv("WEAVIATE_HOST")
weaviate_port = os.getenv("WEAVIATE_PORT")
weaviate_is_secure = os.getenv("WEAVIATE_SECURE")
weaviate_grpc_host = os.getenv("WEAVIATE_GRPC_HOST")
weaviate_grpc_port = os.getenv("WEAVIATE_GRPC_PORT")
DATASET_TYPE = "PRODUCT CATALOG"
# Construct the MongoDB connection string
MONGO_CONNECTION_STRING = f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}/?authMechanism={mongo_auth_mechanism}"


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


def connect_to_mongodb():
    """
    Connects to the MongoDB instance using the provided connection string.

    Returns:
        MongoClient: The MongoDB client instance.
    """
    try:
        client = MongoClient(MONGO_CONNECTION_STRING)
        logger.debug("Connected successfully to MongoDB!")
        return client
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        return None
    

def get_project_db_collections(project_id: str) -> Dict[str, Any]:
    """
    Returns a dictionary of PyMongo collection objects for a given project.
    """
    # Connect to MongoDB
    mongo_client = connect_to_mongodb()
    if not mongo_client:
        raise HTTPException(status_code=500, detail="MongoDB connection failed")

    # Get user/project config from thread storage
    storage = ThreadUserProjectStorage().get_thread_storage()
    project_collections_config = storage.get_project_mongodb_collections(project_id)
    db_name = storage.get_user_data()["db_name"]

    # Build dictionary of PyMongo collection objects
    collections = {}
    for key, collection_name in project_collections_config.items():
        collections[key] = mongo_client[db_name][collection_name]

    return collections

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
