import sys
import json
sys.path.append("../..")
from helpers.database.connection_to_db import connect_to_mongodb
from helpers.logger import get_logger

logger = get_logger("convert_to_weaviate_ready")

def serialize_cleaned_data(chart_doc):
    """Flatten chart/insight document into a Weaviate-ready form."""
    try:
        chart_title = chart_doc.get("chart_title", "")
        chart_type = chart_doc.get("chart_type", "")
        description = chart_doc.get("description", "")
        display_mode = chart_doc.get("display_mode", "")
        config = chart_doc.get("config", {})
        data = chart_doc.get("data", [])

        config_text = ", ".join([f"{k}: {v}" for k, v in config.items()])
        data_summary = []

        # convert data into short readable summary text
        for row in data[:15]:  # limit to first 15 rows
            if isinstance(row, dict):
                data_summary.append(", ".join([f"{k}: {v}" for k, v in row.items()]))

        data_text = " | ".join(data_summary) if data_summary else "No data summary available."

        combined_text = f"""
        Chart Title: {chart_title}
        Chart Type: {chart_type}
        Display Mode: {display_mode}
        Description: {description}
        Config: {config_text}
        Data Summary: {data_text}
        """

        return {
            "chart_id": str(chart_doc.get("_id")),
            "chart_title": chart_title,
            "chart_type": chart_type,
            "description": description,
            "config_text": config_text,
            "data_text": data_text,
            "combined_text": combined_text.strip()
        }

    except Exception as e:
        logger.error(f"Error serializing chart doc: {e}", exc_info=True)
        return None


def serialize_cleaned_dt(attr_doc):
    """Flatten attribute metadata into a Weaviate-ready form."""
    try:
        attribute = attr_doc.get("attribute", "")
        data_type = attr_doc.get("data_type", "")
        original_type = attr_doc.get("original_data_type", "")
        samples = attr_doc.get("sample", [])
        was_corrected = attr_doc.get("was_corrected", False)

        sample_text = ", ".join([str(s) for s in samples[:5]]) if isinstance(samples, list) else str(samples)

        combined_text = f"""
        Attribute: {attribute}
        Data Type: {data_type}
        Original Type: {original_type}
        Was Corrected: {was_corrected}
        Sample Values: {sample_text}
        """

        return {
            "attribute": attribute,
            "data_type": data_type,
            "original_type": original_type,
            "was_corrected": was_corrected,
            "sample_text": sample_text,
            "combined_text": combined_text.strip()
        }

    except Exception as e:
        logger.error(f"Error serializing attribute doc: {e}", exc_info=True)
        return None


def convert_to_weaviate_ready(project_id):
    """
    Converts cleaned data to Weaviate-ready format.
    Reads chart configs and attributes, flattens them, and stores results.
    """
    try:
        # Extract user_id
        user_id = project_id.split("PJ")[0]
        logger.info(f"Running Weaviate conversion for {project_id} (user {user_id})")

        # Connect to MongoDB using existing helper function
        client = connect_to_mongodb()
        if not client:
            logger.error("Failed to connect to MongoDB")
            return

        # Try to get database name from storage, fallback to user_id
        db_name = user_id  # Default fallback
        
        try:
            from helpers.database.thread_shared_storage import ThreadUserProjectStorage
            storage = ThreadUserProjectStorage().get_thread_storage()
            db_name = storage.get_user_data()["db_name"]
            logger.info(f"Got db_name from storage: {db_name}")
        except Exception as e:
            logger.warning(f"Could not get db_name from storage: {e}. Using user_id as db_name: {db_name}")

        # Select database
        db = client[db_name]
        logger.info(f"Connected to database: {db_name}")

        # Collections
        cleaned_data_coll = f"{project_id}_cleaned_data"
        cleaned_dt_coll = f"{project_id}_cleaned_dt"

        weaviate_cd_coll = f"{project_id}_weaviate_cd"
        weaviate_cdt_coll = f"{project_id}_weaviate_cdt"

        # Get data
        charts = list(db[cleaned_data_coll].find())
        attributes = list(db[cleaned_dt_coll].find())

        logger.info(f"Found {len(charts)} charts and {len(attributes)} attributes to process.")

        # Clear previous processed data
        db[weaviate_cd_coll].delete_many({})
        db[weaviate_cdt_coll].delete_many({})
        logger.info("Cleared old Weaviate-ready data.")

        # Process charts
        processed_charts = 0
        failed_charts = 0
        weaviate_cd_docs = []
        
        for chart in charts:
            try:
                flat_doc = serialize_cleaned_data(chart)
                if flat_doc:
                    weaviate_cd_docs.append(flat_doc)
                    processed_charts += 1
                else:
                    failed_charts += 1
            except Exception as e:
                logger.error(f"Error processing chart: {e}", exc_info=True)
                failed_charts += 1

        if weaviate_cd_docs:
            db[weaviate_cd_coll].insert_many(weaviate_cd_docs)
            logger.info(f"✅ Stored {len(weaviate_cd_docs)} chart documents in {weaviate_cd_coll}")

        # Process attributes
        processed_attrs = 0
        failed_attrs = 0
        weaviate_cdt_docs = []
        
        for attr in attributes:
            try:
                flat_doc = serialize_cleaned_dt(attr)
                if flat_doc:
                    weaviate_cdt_docs.append(flat_doc)
                    processed_attrs += 1
                else:
                    failed_attrs += 1
            except Exception as e:
                logger.error(f"Error processing attribute: {e}", exc_info=True)
                failed_attrs += 1

        if weaviate_cdt_docs:
            db[weaviate_cdt_coll].insert_many(weaviate_cdt_docs)
            logger.info(f"✅ Stored {len(weaviate_cdt_docs)} attribute documents in {weaviate_cdt_coll}")

        logger.info(f"🎯 Conversion completed: Charts ({processed_charts} successful, {failed_charts} failed), Attributes ({processed_attrs} successful, {failed_attrs} failed).")

    except Exception as e:
        logger.error(f"Error during conversion: {e}", exc_info=True)
    finally:
        # Close MongoDB connection
        if 'client' in locals() and client:
            client.close()
            logger.info("MongoDB connection closed.")

def run_dfw(project_id):
    """
    Run the complete Data-For-Weaviate (DFW) pipeline.
    
    Args:
        project_id (str): The project ID to process
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        logger.info(f"🚀 Starting DFW pipeline for project: {project_id}")
        
        # Validate project_id format
        if not project_id or "PJ" not in project_id:
            logger.error(f"Invalid project_id format: {project_id}")
            return False
        
        # Run the conversion to Weaviate-ready format
        logger.info("Step 1: Converting data to Weaviate-ready format...")
        convert_to_weaviate_ready(project_id)
        
        logger.info(f"✅ DFW pipeline completed successfully for {project_id}")
        return True
        
    except Exception as e:
        logger.error(f"❌ DFW pipeline failed for {project_id}: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python run_dfw.py <project_id>")
        sys.exit(1)
    
    project_id = sys.argv[1]
    success = run_dfw(project_id)
    sys.exit(0 if success else 1)