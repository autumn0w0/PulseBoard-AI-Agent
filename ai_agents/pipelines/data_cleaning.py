import sys
import re
from bson import ObjectId
import os

# Add parent directory to path
sys.path.append("../..")

# Import after path is set
from helpers.logger import get_logger

# Import MongoDB connection components directly
from dotenv import load_dotenv
from pymongo import MongoClient

logger = get_logger("chart_pipeline")

# Load environment variables
load_dotenv()

# MongoDB configuration
mongo_user = os.getenv("MONGODB_USERNAME")
mongo_password = os.getenv("MONGODB_PASSWORD")
mongo_host = os.getenv("MONGODB_HOST")
mongo_port = os.getenv("MONGODB_PORT")
mongo_auth_mechanism = os.getenv("MONGODB_AUTHMECHANISM")

MONGO_CONNECTION_STRING = f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}/?authMechanism={mongo_auth_mechanism}"


def connect_to_mongodb_local():
    """
    Local MongoDB connection function to avoid circular imports.
    """
    try:
        client = MongoClient(MONGO_CONNECTION_STRING)
        logger.debug("Connected successfully to MongoDB!")
        return client
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        return None


# ----------------------------- Utility Functions -----------------------------

def parse_aggregation_expression(expr: str):
    """
    Parses expressions like:
    count(type), sum(value), avg(duration)
    Returns: ("count", "type")
    """
    expr = expr.strip().lower()
    match = re.match(r"(count|sum|avg|min|max)\((.*?)\)", expr)
    if not match:
        return None, None
    return match.group(1), match.group(2).strip()


def generate_common_aggregation(field: str, agg_func: str, group_key: str):
    """
    Builds MongoDB aggregation $group stage dynamically.
    """
    if agg_func == "count":
        return {"$group": {"_id": f"${group_key}", "value": {"$sum": 1}}}
    elif agg_func == "sum":
        return {"$group": {"_id": f"${group_key}", "value": {"$sum": f"${field}"}}}
    elif agg_func == "avg":
        return {"$group": {"_id": f"${group_key}", "value": {"$avg": f"${field}"}}}
    elif agg_func == "min":
        return {"$group": {"_id": f"${group_key}", "value": {"$min": f"${field}"}}}
    elif agg_func == "max":
        return {"$group": {"_id": f"${group_key}", "value": {"$max": f"${field}"}}}
    else:
        return None


# ----------------------------- Pipeline Generator -----------------------------

def generate_pipeline(chart_type, config):
    """
    Generate an aggregation pipeline for multiple chart types dynamically.
    Supports: bar_chart, pie_chart, line_chart, geo_map, scatter_plot, histogram
    """
    chart_type = chart_type.lower()

    # HISTOGRAM → bin data into ranges
    if chart_type == "histogram":
        field = config.get("field")
        bins = config.get("bins", 10)
        
        if not field:
            logger.warning(f"Histogram missing 'field' in config: {config}")
            return []
        
        # For histogram, we need to:
        # 1. Get min/max values to determine bin ranges
        # 2. Use $bucket or manual grouping to create bins
        # Using $bucket for automatic binning
        pipeline = [
            {
                "$match": {
                    field: {"$exists": True, "$ne": None, "$type": ["int", "double", "long", "decimal"]}
                }
            },
            {
                "$bucketAuto": {
                    "groupBy": f"${field}",
                    "buckets": bins,
                    "output": {
                        "count": {"$sum": 1},
                        "min": {"$min": f"${field}"},
                        "max": {"$max": f"${field}"}
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "range": {
                        "$concat": [
                            {"$toString": "$_id.min"},
                            " - ",
                            {"$toString": "$_id.max"}
                        ]
                    },
                    "min": "$_id.min",
                    "max": "$_id.max",
                    "count": "$count"
                }
            }
        ]
        return pipeline

    # BAR / PIE / LINE / GEO → aggregate form
    elif chart_type in ["bar_chart", "pie_chart", "line_chart", "geo_map"]:
        # Identify axis/fields
        if chart_type == "geo_map":
            group_key = config.get("location_field")
            value_expr = config.get("value_field")
        elif chart_type == "pie_chart":
            group_key = config.get("category")
            value_expr = config.get("value")
        else:
            group_key = config.get("x_axis")
            value_expr = config.get("y_axis")

        if not group_key or not value_expr:
            logger.warning(f"Missing required config for {chart_type}: {config}")
            return []

        agg_func, field = parse_aggregation_expression(value_expr)
        if not agg_func:
            logger.warning(f"Invalid aggregation expression: {value_expr}")
            return []

        group_stage = generate_common_aggregation(field, agg_func, group_key)
        if not group_stage:
            return []

        pipeline = [
            group_stage,
            {"$project": {"_id": 0, group_key: "$_id", "value": 1}}
        ]
        return pipeline

    # SCATTER PLOT → direct mapping (no aggregation)
    elif chart_type == "scatter_plot":
        x_field = config.get("x_axis")
        y_field = config.get("y_axis")
        if not x_field or not y_field:
            logger.warning(f"Scatter plot missing x_axis/y_axis in config: {config}")
            return []
        return [
            {"$project": {"_id": 0, x_field: 1, y_field: 1}}
        ]

    else:
        logger.warning(f"Unsupported chart type: {chart_type}")
        return []


# ----------------------------- Main Runner -----------------------------

def run_chart_pipeline(project_id):
    """
    Runs chart pipeline for any chart type.
    Reads chart configs, runs dynamic pipelines, and stores results.
    """
    try:
        # Extract user_id
        user_id = project_id.split("PJ")[0]
        logger.info(f"Running chart pipeline for {project_id} (user {user_id})")

        # Connect to MongoDB using local function
        client = connect_to_mongodb_local()
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

        # Define collections
        chart_collection = f"{project_id}_charts"  # Fixed: was _chart, should be _charts
        data_collection = f"{project_id}__data"
        output_collection = f"{project_id}_cleaned_data"

        # Get charts
        charts = list(db[chart_collection].find())
        if not charts:
            logger.warning("No chart configs found.")
            return

        logger.info(f"Found {len(charts)} charts for project {project_id}")

        # Optional: clear previous results
        db[output_collection].delete_many({})
        logger.info("Cleared old cleaned data.")

        # Process charts
        for chart in charts:
            chart_id = str(chart["_id"])
            chart_type = chart.get("chart_type", "unknown")
            chart_title = chart.get("title", "Untitled Chart")
            config = chart.get("config", {})

            logger.info(f"Processing chart: {chart_title} ({chart_type})")

            pipeline = generate_pipeline(chart_type, config)
            if not pipeline:
                logger.warning(f"No pipeline generated for chart {chart_id}")
                continue

            # Run aggregation
            result = list(db[data_collection].aggregate(pipeline))

            # Store results
            db[output_collection].insert_one({
                "chart_id": chart_id,
                "chart_type": chart_type,
                "chart_title": chart_title,
                "config": config,
                "data": result
            })

            logger.info(f"✅ {chart_title} processed ({len(result)} records).")

        logger.info("🎯 All chart pipelines executed successfully.")

    except Exception as e:
        logger.error(f"Error running chart pipeline: {e}", exc_info=True)
    finally:
        # Close MongoDB connection
        if 'client' in locals() and client:
            client.close()
            logger.info("MongoDB connection closed.")


# ----------------------------- CLI Entrypoint -----------------------------

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python data_cleaning.py <project_id>")
        sys.exit(1)

    project_id = sys.argv[1]
    run_chart_pipeline(project_id)