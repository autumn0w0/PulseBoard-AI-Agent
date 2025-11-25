import sys
import json
import re
import os
from typing import Dict, List, Optional
from dotenv import load_dotenv
import google.generativeai as genai

sys.path.append("../../")
from helpers.database.connection_to_db import connect_to_mongodb
from helpers.logger import get_logger

# Load environment variables
load_dotenv()

logger = get_logger(__name__)

# Get embedding model from environment variable
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "models/text-embedding-004")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

# Configure Google API
if GOOGLE_API_KEY:
    genai.configure(api_key=GOOGLE_API_KEY)
else:
    logger.warning("GOOGLE_API_KEY not found in environment variables")


def generate_embedding(text: str) -> List[float]:
    """
    Generate embeddings using Google's embedding model.
    
    Args:
        text: Text to embed
        
    Returns:
        List of floats representing the embedding vector
    """
    try:
        result = genai.embed_content(
            model=EMBEDDING_MODEL,
            content=text,
            task_type="retrieval_document"
        )
        return result['embedding']
    except Exception as e:
        logger.error(f"Error generating embedding: {e}")
        raise


def parse_project_id(project_id: str) -> tuple:
    """
    Parse project_id into user_id and project number.
    
    Args:
        project_id: Project ID in format {user_id}PJ00x
        
    Returns:
        Tuple of (user_id, project_id)
    """
    match = re.match(r'(.+?)(PJ\d+)$', project_id)
    if match:
        user_id = match.group(1)
        return user_id, project_id
    else:
        raise ValueError(f"Invalid project_id format: {project_id}")


def check_user_and_project_exist(client, project_id: str, master_db_name: str = "master") -> Optional[Dict]:
    """
    Check if user_id and project_id exist in client_config collection.
    
    Args:
        client: MongoDB client
        project_id: Project ID to check
        master_db_name: Name of the master database
        
    Returns:
        Project configuration dict if exists, None otherwise
    """
    user_id, _ = parse_project_id(project_id)
    
    master_db = client[master_db_name]
    client_config = master_db.client_config.find_one({"user_id": user_id})
    
    if not client_config:
        logger.error(f"User ID '{user_id}' not found in client_config")
        return None
    
    project_info = None
    for project in client_config.get("projects", []):
        if project.get("project_id") == project_id:
            project_info = project
            break
    
    if not project_info:
        logger.error(f"Project ID '{project_id}' not found for user '{user_id}'")
        return None
    
    return {
        "user_id": user_id,
        "db_name": client_config.get("db_name"),
        "project_info": project_info
    }


def create_text_from_weaviate_cdt(record: Dict) -> str:
    """
    Converts weaviate_cdt record into meaningful text for embeddings.
    """
    parts = []
    if "attribute" in record:
        parts.append(f"Attribute: {record['attribute']}")
    if "data_type" in record:
        parts.append(f"Data Type: {record['data_type']}")
    if "original_data_type" in record:
        parts.append(f"Original Type: {record['original_data_type']}")
    if "sample" in record and isinstance(record["sample"], list):
        sample_text = ", ".join(str(s) for s in record["sample"][:5])
        parts.append(f"Sample Values: {sample_text}")
    return "\n".join(parts)


def create_text_from_weaviate_cd(record: Dict) -> str:
    """
    Converts weaviate_cd record into text for embeddings.
    Only includes metadata, NOT the actual data field.
    """
    meta_parts = []

    if "chart_title" in record:
        meta_parts.append(f"Chart Title: {record['chart_title']}")
    if "chart_type" in record:
        meta_parts.append(f"Chart Type: {record['chart_type']}")
    if "description" in record:
        meta_parts.append(f"Description: {record['description']}")
    if "config" in record:
        meta_parts.append(f"Config: {json.dumps(record['config'])}")

    return "\n".join(meta_parts)


def vectorize_and_store(client, db_name: str, project_id: str, text_records: List[tuple], source_type: str) -> int:
    """
    Sends text to Google's embedding model and stores results in separate vector collections.
    
    Args:
        client: MongoDB client
        db_name: Database name
        project_id: The project identifier
        text_records: List of tuples (record_id, text_block)
        source_type: Either 'weaviate_cdt' or 'weaviate_cd'
        
    Returns:
        Number of successfully vectorized records
    """
    db = client[db_name]
    
    # Create separate collection names based on source type
    if source_type == "weaviate_cdt":
        vector_collection = f"{project_id}_weaviate_vectors_cdt"
    else:  # weaviate_cd
        vector_collection = f"{project_id}_weaviate_vectors_cd"
    
    vector_db = db[vector_collection]

    logger.info(f"Vectorizing {len(text_records)} documents from {source_type}")
    logger.info(f"Using embedding model: {EMBEDDING_MODEL}")
    logger.info(f"Storing in collection: {vector_collection}")
    
    success_count = 0

    for rec_id, text_block in text_records:
        try:
            if not text_block.strip():
                logger.warning(f"Skipping empty text for record {rec_id}")
                continue

            # Generate embedding using Google's embedding model
            vector = generate_embedding(text_block)

            # Store only essential fields: project_id, source_id, and vector
            vector_db.insert_one({
                "project_id": project_id,
                "source_id": rec_id,
                "vector": vector
            })
            
            success_count += 1
            logger.debug(f"Successfully vectorized {source_type} record {rec_id}")
            
        except Exception as e:
            logger.error(f"Error vectorizing {source_type} record {rec_id}: {e}")

    logger.info(f"✅ Finished vectorizing {source_type}: {success_count}/{len(text_records)} successful")
    return success_count


def run_v(project_id: str, master_db_name: str = "master") -> Dict:
    """
    Main function to vectorize all data for a given project.
    Stores vectors in {project_id}_weaviate_vectors_cd and {project_id}_weaviate_vectors_cdt collections.
    
    Args:
        project_id: The project identifier (e.g., "UID001PJ001")
        master_db_name: Name of the master database (default: "master")
    
    Returns:
        dict: Summary of vectorization results
        
    Example:
        run_v("UID001PJ001")
    """
    logger.info(f"Processing project: {project_id}")
    
    # Connect to MongoDB
    client = connect_to_mongodb()
    if not client:
        logger.error("Failed to connect to MongoDB")
        return {"success": False, "error": "Failed to connect to MongoDB"}
    
    try:
        # Check if user and project exist
        config = check_user_and_project_exist(client, project_id, master_db_name)
        if not config:
            return {"success": False, "error": "Project not found"}
        
        db_name = config["db_name"]
        
        logger.info(f"User ID: {config['user_id']}")
        logger.info(f"Database: {db_name}")
        
        db = client[db_name]
        
        # Define collection names for Weaviate collections
        weaviate_cdt_coll = f"{project_id}_weaviate_cdt"
        weaviate_cd_coll = f"{project_id}_weaviate_cd"
        
        # Check if collections exist
        existing_collections = db.list_collection_names()
        
        if weaviate_cdt_coll not in existing_collections:
            logger.warning(f"Collection '{weaviate_cdt_coll}' does not exist")
        
        if weaviate_cd_coll not in existing_collections:
            logger.warning(f"Collection '{weaviate_cd_coll}' does not exist")

        # Fetch records from both collections
        logger.info(f"Fetching records from {weaviate_cdt_coll}")
        weaviate_cdt_records = list(db[weaviate_cdt_coll].find()) if weaviate_cdt_coll in existing_collections else []
        logger.info(f"Found {len(weaviate_cdt_records)} records in {weaviate_cdt_coll}")

        logger.info(f"Fetching records from {weaviate_cd_coll}")
        weaviate_cd_records = list(db[weaviate_cd_coll].find()) if weaviate_cd_coll in existing_collections else []
        logger.info(f"Found {len(weaviate_cd_records)} records in {weaviate_cd_coll}")

        if not weaviate_cdt_records and not weaviate_cd_records:
            logger.error("No data found in either collection")
            return {"success": False, "error": "No data found to vectorize"}

        # --- Process weaviate_cdt records ---
        weaviate_cdt_texts = []
        for rec in weaviate_cdt_records:
            text_block = create_text_from_weaviate_cdt(rec)
            weaviate_cdt_texts.append((str(rec["_id"]), text_block))

        cdt_success = 0
        if weaviate_cdt_texts:
            cdt_success = vectorize_and_store(client, db_name, project_id, weaviate_cdt_texts, "weaviate_cdt")

        # --- Process weaviate_cd records ---
        weaviate_cd_texts = []
        for rec in weaviate_cd_records:
            text_block = create_text_from_weaviate_cd(rec)
            weaviate_cd_texts.append((str(rec["_id"]), text_block))

        cd_success = 0
        if weaviate_cd_texts:
            cd_success = vectorize_and_store(client, db_name, project_id, weaviate_cd_texts, "weaviate_cd")

        # Summary
        summary = {
            "success": True,
            "project_id": project_id,
            "database": db_name,
            "weaviate_cdt_count": len(weaviate_cdt_texts),
            "weaviate_cdt_vectorized": cdt_success,
            "weaviate_cdt_collection": f"{project_id}_weaviate_vectors_cdt",
            "weaviate_cd_count": len(weaviate_cd_texts),
            "weaviate_cd_vectorized": cd_success,
            "weaviate_cd_collection": f"{project_id}_weaviate_vectors_cd",
            "total_vectorized": cdt_success + cd_success
        }

        logger.info("🎯 All vectorization completed successfully!")
        logger.info(f"Summary:\n{json.dumps(summary, indent=2)}")
        
        print(f"\n✓ Successfully processed project '{project_id}'")
        print(f"  - Vectorized {cdt_success}/{len(weaviate_cdt_texts)} weaviate_cdt records → {project_id}_weaviate_vectors_cdt")
        print(f"  - Vectorized {cd_success}/{len(weaviate_cd_texts)} weaviate_cd records → {project_id}_weaviate_vectors_cd")
        print(f"  - Total: {cdt_success + cd_success} vectors created")
        
        return summary

    except Exception as e:
        logger.error(f"Error in run_v for project {project_id}: {e}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}
        
    finally:
        client.close()


def main():
    """
    Command-line entry point for the vectorization pipeline.
    """
    if len(sys.argv) < 2:
        print("Usage: python vectorization.py <project_id>")
        print("Example: python vectorization.py UID001PJ001")
        sys.exit(1)

    project_id = sys.argv[1]
    result = run_v(project_id)
    
    if not result.get("success"):
        sys.exit(1)


if __name__ == "__main__":
    main()