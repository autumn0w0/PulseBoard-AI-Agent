import sys
import os
from dotenv import load_dotenv
import weaviate
import weaviate.classes.config as wvc
from bson import ObjectId

sys.path.append("../..")
from helpers.database.connection_to_db import connect_to_mongodb
from helpers.database.connect_to_weaviate import connect_to_weaviatedb
from helpers.logger import get_logger
from weaviate.util import generate_uuid5

logger = get_logger("run_dtw")
load_dotenv()


# ==================== STEP 2: DATA FLATTENING ====================

def convert_to_weaviate_ready(project_id):
    """
    Convert cleaned chart and attribute data to Weaviate-ready format.
    Reads from {project_id}_cleaned_data and {project_id}_cleaned_dt collections.
    Writes to {project_id}_weaviate_cd and {project_id}_weaviate_cdt collections.
    
    CRITICAL: Creates a mapping between cleaned data _id and vector source_id
    """
    mongo_client = None
    
    try:
        # Extract user_id
        user_id = project_id.split("PJ")[0]
        logger.info(f"Running Weaviate conversion for {project_id} (user {user_id})")
        
        # Connect to MongoDB
        mongo_client = connect_to_mongodb()
        if not mongo_client:
            logger.error("Failed to connect to MongoDB")
            return False
        
        # Try to get database name from storage, fallback to user_id
        db_name = user_id  # Default fallback
        
        try:
            from helpers.database.thread_shared_storage import ThreadUserProjectStorage
            storage = ThreadUserProjectStorage().get_thread_storage()
            db_name = storage.get_user_data()["db_name"]
            logger.info(f"Got db_name from storage: {db_name}")
        except Exception as e:
            logger.warning(f"Could not get db_name from storage: {e}. Using user_id as db_name: {db_name}")
        
        # Select MongoDB database
        mongo_db = mongo_client[db_name]
        logger.info(f"Connected to database: {db_name}")
        
        # Collection names
        cleaned_charts_coll = f"{project_id}_cleaned_data"
        cleaned_attrs_coll = f"{project_id}_cleaned_dt"
        weaviate_charts_coll = f"{project_id}_weaviate_cd"
        weaviate_attrs_coll = f"{project_id}_weaviate_cdt"
        vector_charts_coll = f"{project_id}_weaviate_vectors_cd"
        vector_attrs_coll = f"{project_id}_weaviate_vectors_cdt"
        
        # Fetch cleaned data
        charts = list(mongo_db[cleaned_charts_coll].find())
        attributes = list(mongo_db[cleaned_attrs_coll].find())
        
        logger.info(f"Found {len(charts)} charts and {len(attributes)} attributes to process.")
        
        # Build vector lookup maps
        logger.info("Building vector lookup maps...")
        chart_vectors = {}
        for vec_doc in mongo_db[vector_charts_coll].find():
            chart_vectors[vec_doc.get("source_id")] = vec_doc.get("vector")
        
        attr_vectors = {}
        for vec_doc in mongo_db[vector_attrs_coll].find():
            attr_vectors[vec_doc.get("source_id")] = vec_doc.get("vector")
        
        logger.info(f"Found {len(chart_vectors)} chart vectors and {len(attr_vectors)} attribute vectors")
        
        # Clear old Weaviate-ready collections
        mongo_db[weaviate_charts_coll].delete_many({})
        mongo_db[weaviate_attrs_coll].delete_many({})
        logger.info("Cleared old Weaviate-ready data.")
        
        # ==================== PROCESS CHARTS ====================
        charts_to_insert = []
        charts_success = 0
        charts_failed = 0
        charts_no_vector = 0
        
        for chart in charts:
            try:
                chart_id = str(chart["_id"])
                
                # Check if vector exists for this chart
                if chart_id not in chart_vectors:
                    logger.warning(f"No vector found for chart {chart.get('chart_title', 'Unknown')} (id: {chart_id})")
                    charts_no_vector += 1
                    continue
                
                # ✨ CRITICAL: Use the cleaned data _id as source_id (matches vector collection)
                chart_doc = {
                    "source_id": chart_id,  # This matches the source_id in vector collection
                    "chart_id": chart.get("chart_id", ""),
                    "chart_title": chart.get("chart_title", "Unknown"),
                    "chart_type": chart.get("chart_type", ""),
                    "description": chart.get("description", ""),
                    "config_text": chart.get("config_text", ""),
                    "data_text": chart.get("data_text", ""),
                    "combined_text": f"""Chart Title: {chart.get('chart_title', '')}
        Chart Type: {chart.get('chart_type', '')}
        Display Mode: {chart.get('display_mode', 'direct')}
        Description: {chart.get('description', '')}
        Config: {chart.get('config_text', '')}
        Data Summary: {chart.get('data_text', '')}""",
                }
                
                charts_to_insert.append(chart_doc)
                charts_success += 1
                
            except Exception as e:
                logger.error(f"Failed to process chart {chart.get('chart_title', 'Unknown')}: {e}")
                charts_failed += 1
        
        # Insert charts
        if charts_to_insert:
            mongo_db[weaviate_charts_coll].insert_many(charts_to_insert)
            logger.info(f"✅ Stored {len(charts_to_insert)} chart documents in {weaviate_charts_coll}")
        
        # ==================== PROCESS ATTRIBUTES ====================
        attrs_to_insert = []
        attrs_success = 0
        attrs_failed = 0
        attrs_no_vector = 0
        
        for attr in attributes:
            try:
                attr_id = str(attr["_id"])
                
                # Check if vector exists for this attribute
                if attr_id not in attr_vectors:
                    logger.warning(f"No vector found for attribute {attr.get('attribute', 'Unknown')} (id: {attr_id})")
                    attrs_no_vector += 1
                    continue
                
                # ✨ CRITICAL: Use the cleaned data _id as source_id (matches vector collection)
                attr_doc = {
                    "source_id": attr_id,  # This matches the source_id in vector collection
                    "attribute": attr.get("attribute", ""),
                    "data_type": attr.get("data_type", ""),
                    "original_type": attr.get("original_type", ""),
                    "was_corrected": attr.get("was_corrected", False),
                    "sample_text": attr.get("sample_text", ""),
                    "combined_text": f"""Attribute: {attr.get('attribute', '')}
        Data Type: {attr.get('data_type', '')}
        Original Type: {attr.get('original_type', '')}
        Was Corrected: {attr.get('was_corrected', False)}
        Sample Values: {attr.get('sample_text', '')}""",
                }
                
                attrs_to_insert.append(attr_doc)
                attrs_success += 1
                
            except Exception as e:
                logger.error(f"Failed to process attribute {attr.get('attribute', 'Unknown')}: {e}")
                attrs_failed += 1
        
        # Insert attributes
        if attrs_to_insert:
            mongo_db[weaviate_attrs_coll].insert_many(attrs_to_insert)
            logger.info(f"✅ Stored {len(attrs_to_insert)} attribute documents in {weaviate_attrs_coll}")
        
        logger.info(f"🎯 Conversion completed:")
        logger.info(f"   Charts: {charts_success} successful, {charts_no_vector} no vector, {charts_failed} failed")
        logger.info(f"   Attributes: {attrs_success} successful, {attrs_no_vector} no vector, {attrs_failed} failed")
        
        return True
        
    except Exception as e:
        logger.error(f"Error during Weaviate conversion: {e}", exc_info=True)
        return False
    finally:
        # Close connection
        if mongo_client:
            mongo_client.close()
            logger.info("MongoDB connection closed.")


# ==================== STEP 3: PUSH TO WEAVIATE ====================

def create_weaviate_collections(weaviate_client, project_id):
    """
    Create Weaviate collections for charts and attributes if they don't exist.
    Uses manual vectorization since vectors are pre-computed in MongoDB.
    """
    try:
        # Collection names - using project_id for unique collections per project
        charts_collection_name = f"{project_id}_weaviate_cd"
        attributes_collection_name = f"{project_id}_weaviate_cdt"
        
        collections = weaviate_client.collections
        
        # Create charts collection if not exists
        if not collections.exists(charts_collection_name):
            collections.create(
                name=charts_collection_name,
                properties=[
                    wvc.Property(name="chart_id", data_type=wvc.DataType.TEXT),
                    wvc.Property(name="chart_title", data_type=wvc.DataType.TEXT),
                    wvc.Property(name="chart_type", data_type=wvc.DataType.TEXT),
                    wvc.Property(name="description", data_type=wvc.DataType.TEXT),
                    wvc.Property(name="config_text", data_type=wvc.DataType.TEXT),
                    wvc.Property(name="data_text", data_type=wvc.DataType.TEXT),
                    wvc.Property(name="combined_text", data_type=wvc.DataType.TEXT),
                ],
                vectorizer_config=wvc.Configure.Vectorizer.none(),
            )
            logger.info(f"✅ Created Weaviate collection: {charts_collection_name}")
        else:
            logger.info(f"Collection {charts_collection_name} already exists")
        
        # Create attributes collection if not exists
        if not collections.exists(attributes_collection_name):
            collections.create(
                name=attributes_collection_name,
                properties=[
                    wvc.Property(name="attribute", data_type=wvc.DataType.TEXT),
                    wvc.Property(name="data_type", data_type=wvc.DataType.TEXT),
                    wvc.Property(name="original_type", data_type=wvc.DataType.TEXT),
                    wvc.Property(name="was_corrected", data_type=wvc.DataType.BOOL),
                    wvc.Property(name="sample_text", data_type=wvc.DataType.TEXT),
                    wvc.Property(name="combined_text", data_type=wvc.DataType.TEXT),
                ],
                vectorizer_config=wvc.Configure.Vectorizer.none(),
            )
            logger.info(f"✅ Created Weaviate collection: {attributes_collection_name}")
        else:
            logger.info(f"Collection {attributes_collection_name} already exists")
        
        return charts_collection_name, attributes_collection_name
        
    except Exception as e:
        logger.error(f"Error creating Weaviate collections: {e}", exc_info=True)
        return None, None


def push_to_weaviate_with_vectors(project_id):
    """
    Push MongoDB collections to Weaviate with pre-computed vectors.
    Uses an optimized approach with vector lookup maps.
    """
    mongo_client = None
    weaviate_client = None
    
    try:
        # Extract user_id
        user_id = project_id.split("PJ")[0]
        logger.info(f"Starting Weaviate push for project {project_id} (user {user_id})")
        
        # Connect to MongoDB
        mongo_client = connect_to_mongodb()
        if not mongo_client:
            logger.error("Failed to connect to MongoDB")
            return False
        
        # Try to get database name from storage, fallback to user_id
        db_name = user_id  # Default fallback
        
        try:
            from helpers.database.thread_shared_storage import ThreadUserProjectStorage
            storage = ThreadUserProjectStorage().get_thread_storage()
            db_name = storage.get_user_data()["db_name"]
            logger.info(f"Got db_name from storage: {db_name}")
        except Exception as e:
            logger.warning(f"Could not get db_name from storage: {e}. Using user_id as db_name: {db_name}")
        
        # Select MongoDB database
        mongo_db = mongo_client[db_name]
        logger.info(f"Connected to MongoDB database: {db_name}")
        
        # Connect to Weaviate
        weaviate_client = connect_to_weaviatedb()
        if not weaviate_client:
            logger.error("Failed to connect to Weaviate")
            return False
        
        logger.info("Connected to Weaviate successfully")
        
        # Create Weaviate collections if needed
        charts_weaviate_coll, attrs_weaviate_coll = create_weaviate_collections(weaviate_client, project_id)
        
        if not charts_weaviate_coll or not attrs_weaviate_coll:
            logger.error("Failed to create/verify Weaviate collections")
            return False
        
        # Collection names
        charts_mongo_coll = f"{project_id}_weaviate_cd"
        attrs_mongo_coll = f"{project_id}_weaviate_cdt"
        vector_charts_coll = f"{project_id}_weaviate_vectors_cd"
        vector_attrs_coll = f"{project_id}_weaviate_vectors_cdt"
        
        # Build vector lookup maps for efficient access
        logger.info("Building vector lookup maps...")
        chart_vectors = {}
        for vec_doc in mongo_db[vector_charts_coll].find():
            chart_vectors[vec_doc.get("source_id")] = vec_doc.get("vector")
        
        attr_vectors = {}
        for vec_doc in mongo_db[vector_attrs_coll].find():
            attr_vectors[vec_doc.get("source_id")] = vec_doc.get("vector")
        
        logger.info(f"Loaded {len(chart_vectors)} chart vectors and {len(attr_vectors)} attribute vectors")
        
        # ==================== PUSH CHARTS ====================
        logger.info(f"Pushing charts from {charts_mongo_coll} to {charts_weaviate_coll}...")
        
        charts = list(mongo_db[charts_mongo_coll].find())
        logger.info(f"Found {len(charts)} charts to push")
        
        charts_success = 0
        charts_failed = 0
        
        weaviate_charts = weaviate_client.collections.get(charts_weaviate_coll)
        
        for chart in charts:
            try:
                source_id = chart.get("source_id", "")
                chart_title = chart.get("chart_title", "Unknown")
                
                if not source_id:
                    logger.warning(f"Skipping chart {chart_title} - no source_id field")
                    charts_failed += 1
                    continue
                
                # Get vector from lookup map
                vector = chart_vectors.get(source_id)
                
                if not vector:
                    logger.warning(f"Skipping chart {chart_title} - no vector for source_id: {source_id}")
                    charts_failed += 1
                    continue
                
                # Prepare chart data (exclude MongoDB _id and source_id)
                chart_data = {
                    "chart_id": chart.get("chart_id", ""),
                    "chart_title": chart_title,
                    "chart_type": chart.get("chart_type", ""),
                    "description": chart.get("description", ""),
                    "config_text": chart.get("config_text", ""),
                    "data_text": chart.get("data_text", ""),
                    "combined_text": chart.get("combined_text", ""),
                }
                
                # Generate consistent UUID based on source_id
                uuid = generate_uuid5(source_id)
                
                # Insert with vector in Weaviate
                weaviate_charts.data.insert(
                    properties=chart_data,
                    uuid=uuid,
                    vector=vector
                )
                
                charts_success += 1
                logger.debug(f"✓ Pushed chart: {chart_title}")
                
            except Exception as e:
                logger.error(f"Failed to push chart {chart.get('chart_title', 'Unknown')}: {e}")
                charts_failed += 1
        
        logger.info(f"✅ Charts: {charts_success} successful, {charts_failed} failed")
        
        # ==================== PUSH ATTRIBUTES ====================
        logger.info(f"Pushing attributes from {attrs_mongo_coll} to {attrs_weaviate_coll}...")
        
        attributes = list(mongo_db[attrs_mongo_coll].find())
        logger.info(f"Found {len(attributes)} attributes to push")
        
        attrs_success = 0
        attrs_failed = 0
        
        weaviate_attrs = weaviate_client.collections.get(attrs_weaviate_coll)
        
        for attr in attributes:
            try:
                source_id = attr.get("source_id", "")
                attr_name = attr.get("attribute", "Unknown")
                
                if not source_id:
                    logger.warning(f"Skipping attribute {attr_name} - no source_id field")
                    attrs_failed += 1
                    continue
                
                # Get vector from lookup map
                vector = attr_vectors.get(source_id)
                
                if not vector:
                    logger.warning(f"Skipping attribute {attr_name} - no vector for source_id: {source_id}")
                    attrs_failed += 1
                    continue
                
                # Prepare attribute data (exclude MongoDB _id and source_id)
                attr_data = {
                    "attribute": attr_name,
                    "data_type": attr.get("data_type", ""),
                    "original_type": attr.get("original_type", ""),
                    "was_corrected": attr.get("was_corrected", False),
                    "sample_text": attr.get("sample_text", ""),
                    "combined_text": attr.get("combined_text", ""),
                }
                
                # Generate consistent UUID based on source_id
                uuid = generate_uuid5(source_id)
                
                # Insert with vector in Weaviate
                weaviate_attrs.data.insert(
                    properties=attr_data,
                    uuid=uuid,
                    vector=vector
                )
                
                attrs_success += 1
                logger.debug(f"✓ Pushed attribute: {attr_name}")
                
            except Exception as e:
                logger.error(f"Failed to push attribute {attr.get('attribute', 'Unknown')}: {e}")
                attrs_failed += 1
        
        logger.info(f"✅ Attributes: {attrs_success} successful, {attrs_failed} failed")
        
        # Summary
        logger.info(f"🎯 Weaviate push completed!")
        logger.info(f"   Total: {charts_success + attrs_success} successful, {charts_failed + attrs_failed} failed")
        
        return (charts_success > 0 or attrs_success > 0)
        
    except Exception as e:
        logger.error(f"Error during Weaviate push: {e}", exc_info=True)
        return False
    finally:
        # Close connections
        if mongo_client:
            mongo_client.close()
            logger.info("MongoDB connection closed")
        
        if weaviate_client:
            weaviate_client.close()
            logger.info("Weaviate connection closed")


# ==================== MAIN ORCHESTRATOR ====================

def run_dtw(project_id):
    """
    Main orchestrator function that runs the complete Data-To-Weaviate (DTW) pipeline:
    1. Converts cleaned data to Weaviate-ready format with proper source_id mapping
    2. Pushes data to Weaviate with pre-computed vectors
    
    Args:
        project_id: Project ID in format "UID001PJ001"
    
    Returns:
        bool: True if all steps completed successfully, False otherwise
    """
    user_id = project_id.split("PJ")[0]
    logger.info(f"=" * 80)
    logger.info(f"🚀 Starting DTW Pipeline for Project: {project_id}")
    logger.info(f"   User ID: {user_id}")
    logger.info(f"=" * 80)
    
    try:
        # ==================== STEP 1: Convert to Weaviate Format ====================
        logger.info("\n" + "=" * 80)
        logger.info("🔄 STEP 1/2: Converting Data to Weaviate-Ready Format...")
        logger.info("=" * 80)
        
        if not convert_to_weaviate_ready(project_id):
            logger.error("❌ Step 1 failed: Conversion error")
            return False
        
        logger.info("✅ Step 1 completed: Data converted to Weaviate format")
        
        # ==================== STEP 2: Push to Weaviate ====================
        logger.info("\n" + "=" * 80)
        logger.info("☁️  STEP 2/2: Pushing Data to Weaviate with Vectors...")
        logger.info("=" * 80)
        
        if not push_to_weaviate_with_vectors(project_id):
            logger.error("❌ Step 2 failed: Weaviate push error")
            return False
        
        logger.info("✅ Step 2 completed: Data pushed to Weaviate successfully")
        
        # ==================== SUCCESS ====================
        logger.info("\n" + "=" * 80)
        logger.info("🎉 DTW PIPELINE COMPLETED SUCCESSFULLY!")
        logger.info(f"   Project: {project_id}")
        logger.info(f"   All data has been processed and pushed to Weaviate")
        logger.info("=" * 80)
        
        return True
        
    except Exception as e:
        logger.error(f"❌ DTW Pipeline failed with unexpected error: {e}", exc_info=True)
        logger.info("\n" + "=" * 80)
        logger.info("💥 DTW PIPELINE FAILED")
        logger.info(f"   Project: {project_id}")
        logger.info(f"   Error: {str(e)}")
        logger.info("=" * 80)
        return False


# ----------------------------- CLI Entrypoint -----------------------------

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python run_dtw.py <project_id>")
        print("Example: python run_dtw.py UID001PJ001")
        sys.exit(1)
    
    project_id = sys.argv[1]
    success = run_dtw(project_id)
    sys.exit(0 if success else 1)