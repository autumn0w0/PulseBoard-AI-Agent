import sys
import re
from bson import ObjectId

# Add parent directory to path
sys.path.append("../..")

# Import after path is set
from helpers.database.connection_to_db import connect_to_mongodb
from helpers.logger import get_logger

logger = get_logger("chart_pipeline")


# ----------------------------- Utility Functions -----------------------------

def parse_aggregation_expression(expr: str):
    """
    Parses expressions like:
    count(type), sum(value), avg(duration), min(price), max(score)
    Returns: ("count", "type") or ("sum", "value")
    """
    expr = expr.strip().lower()
    match = re.match(r"(count|sum|avg|min|max)\s*\(\s*(.*?)\s*\)", expr)
    if not match:
        return None, None
    return match.group(1), match.group(2).strip()


def is_numeric_field(field_value):
    """
    Check if a field value can be treated as numeric
    """
    if isinstance(field_value, (int, float)):
        return True
    if isinstance(field_value, str):
        # Try to extract numeric value from string
        try:
            float(field_value.replace(',', '').strip())
            return True
        except:
            pass
    return False


def generate_common_aggregation(field: str, agg_func: str, group_key: str):
    """
    Builds MongoDB aggregation $group stage dynamically.
    Handles both direct fields and nested fields with dot notation.
    """
    # Handle count separately as it doesn't need a field reference
    if agg_func == "count":
        return {"$group": {"_id": f"${group_key}", "value": {"$sum": 1}}}
    
    # For other aggregations, create a safe numeric conversion
    numeric_field = {
        "$cond": {
            "if": {"$or": [
                {"$eq": [{"$type": f"${field}"}, "int"]},
                {"$eq": [{"$type": f"${field}"}, "double"]},
                {"$eq": [{"$type": f"${field}"}, "long"]},
                {"$eq": [{"$type": f"${field}"}, "decimal"]}
            ]},
            "then": f"${field}",
            "else": {
                "$convert": {
                    "input": {
                        "$trim": {
                            "input": {
                                "$replaceAll": {
                                    "input": {"$toString": f"${field}"},
                                    "find": ",",
                                    "replacement": ""
                                }
                            }
                        }
                    },
                    "to": "double",
                    "onError": 0,
                    "onNull": 0
                }
            }
        }
    }
    
    if agg_func == "sum":
        return {"$group": {"_id": f"${group_key}", "value": {"$sum": numeric_field}}}
    elif agg_func == "avg":
        return {"$group": {"_id": f"${group_key}", "value": {"$avg": numeric_field}}}
    elif agg_func == "min":
        return {"$group": {"_id": f"${group_key}", "value": {"$min": numeric_field}}}
    elif agg_func == "max":
        return {"$group": {"_id": f"${group_key}", "value": {"$max": numeric_field}}}
    else:
        return None


# ----------------------------- Pipeline Generator -----------------------------

def generate_pipeline(chart_type, config):
    """
    Generate an aggregation pipeline for multiple chart types dynamically.
    Supports: bar_chart, pie_chart, line_chart, geo_map, scatter_plot, histogram
    Works with any dataset structure.
    """
    chart_type = chart_type.lower()

    # HISTOGRAM → bin numeric data into ranges
    if chart_type == "histogram":
        field = config.get("field")
        bins = config.get("bins", 10)
        
        if not field:
            logger.warning(f"Histogram missing 'field' in config: {config}")
            return []
        
        # Dynamic histogram pipeline that handles various data types
        pipeline = [
            # Match documents where field exists
            {
                "$match": {
                    field: {"$exists": True, "$ne": None, "$ne": ""}
                }
            },
            # Convert field to numeric value (handles strings, numbers, etc.)
            {
                "$addFields": {
                    "numeric_value": {
                        "$convert": {
                            "input": {
                                "$cond": {
                                    "if": {"$in": [{"$type": f"${field}"}, ["int", "double", "long", "decimal"]]},
                                    "then": f"${field}",
                                    "else": {
                                        # For strings, try to extract first number
                                        "$toDouble": {
                                            "$trim": {
                                                "input": {
                                                    "$arrayElemAt": [
                                                        {"$split": [
                                                            {"$replaceAll": {
                                                                "input": {"$toString": f"${field}"},
                                                                "find": ",",
                                                                "replacement": ""
                                                            }},
                                                            " "
                                                        ]},
                                                        0
                                                    ]
                                                }
                                            }
                                        }
                                    }
                                }
                            },
                            "to": "double",
                            "onError": None,
                            "onNull": None
                        }
                    }
                }
            },
            # Filter out non-numeric values
            {
                "$match": {
                    "numeric_value": {
                        "$ne": None,
                        "$type": ["double", "int", "long", "decimal"]
                    }
                }
            },
            # Create automatic bins
            {
                "$bucketAuto": {
                    "groupBy": "$numeric_value",
                    "buckets": bins,
                    "output": {
                        "count": {"$sum": 1},
                        "min": {"$min": "$numeric_value"},
                        "max": {"$max": "$numeric_value"}
                    }
                }
            },
            # Format output
            {
                "$project": {
                    "_id": 0,
                    "range": {
                        "$concat": [
                            {"$toString": {"$round": ["$_id.min", 2]}},
                            " - ",
                            {"$toString": {"$round": ["$_id.max", 2]}}
                        ]
                    },
                    "min": {"$round": ["$_id.min", 2]},
                    "max": {"$round": ["$_id.max", 2]},
                    "count": "$count",
                    "value": "$count"
                }
            }
        ]
        return pipeline

    # BAR / PIE / LINE / GEO → aggregate form
    elif chart_type in ["bar_chart", "pie_chart", "line_chart", "geo_map"]:
        # Identify axis/fields based on chart type
        if chart_type == "geo_map":
            group_key = config.get("location_field")
            value_expr = config.get("value_field")
        elif chart_type == "pie_chart":
            group_key = config.get("category")
            value_expr = config.get("value")
        else:  # bar_chart, line_chart
            group_key = config.get("x_axis")
            value_expr = config.get("y_axis")

        if not group_key or not value_expr:
            logger.warning(f"Missing required config for {chart_type}: {config}")
            return []

        # Parse aggregation expression
        agg_func, field = parse_aggregation_expression(value_expr)
        if not agg_func:
            logger.warning(f"Invalid aggregation expression: {value_expr}")
            return []

        # Generate group stage
        group_stage = generate_common_aggregation(field, agg_func, group_key)
        if not group_stage:
            return []

        # Build complete pipeline with filters for valid data
        pipeline = [
            # Filter: group_key must exist and not be null/empty
            {
                "$match": {
                    group_key: {"$exists": True, "$ne": None, "$ne": ""}
                }
            },
            # Add field filter if not count operation
            *([{
                "$match": {
                    field: {"$exists": True, "$ne": None}
                }
            }] if agg_func != "count" else []),
            # Group stage
            group_stage,
            # Project output
            {
                "$project": {
                    "_id": 0,
                    group_key: "$_id",
                    "value": 1
                }
            },
            # Sort by value descending (useful for bar charts)
            {"$sort": {"value": -1}}
        ]
        return pipeline

# SCATTER PLOT → direct mapping (no aggregation)
    elif chart_type == "scatter_plot":
        x_field = config.get("x_axis")
        y_field = config.get("y_axis")
        
        if not x_field or not y_field:
            logger.warning(f"Scatter plot missing x_axis/y_axis in config: {config}")
            return []
        
        # Build pipeline with numeric conversion for both axes
        pipeline = [
            # Match documents where both fields exist
            {
                "$match": {
                    x_field: {"$exists": True, "$ne": None, "$ne": ""},
                    y_field: {"$exists": True, "$ne": None, "$ne": ""}
                }
            },
            # Convert fields to numeric if needed
            {
                "$addFields": {
                    f"{x_field}_numeric": {
                        "$convert": {
                            "input": {
                                "$cond": {
                                    "if": {"$in": [{"$type": f"${x_field}"}, ["int", "double", "long", "decimal"]]},
                                    "then": f"${x_field}",
                                    "else": {
                                        "$trim": {
                                            "input": {
                                                "$replaceAll": {
                                                    "input": {
                                                        "$replaceAll": {
                                                            "input": {"$toString": f"${x_field}"},
                                                            "find": ",",
                                                            "replacement": ""
                                                        }
                                                    },
                                                    "find": " ",
                                                    "replacement": ""
                                                }
                                            }
                                        }
                                    }
                                }
                            },
                            "to": "double",
                            "onError": None,
                            "onNull": None
                        }
                    },
                    f"{y_field}_numeric": {
                        "$convert": {
                            "input": {
                                "$cond": {
                                    "if": {"$in": [{"$type": f"${y_field}"}, ["int", "double", "long", "decimal"]]},
                                    "then": f"${y_field}",
                                    "else": {
                                        "$trim": {
                                            "input": {
                                                "$replaceAll": {
                                                    "input": {
                                                        "$replaceAll": {
                                                            "input": {"$toString": f"${y_field}"},
                                                            "find": ",",
                                                            "replacement": ""
                                                        }
                                                    },
                                                    "find": " ",
                                                    "replacement": ""
                                                }
                                            }
                                        }
                                    }
                                }
                            },
                            "to": "double",
                            "onError": None,
                            "onNull": None
                        }
                    }
                }
            },
            # Filter valid numeric pairs
            {
                "$match": {
                    f"{x_field}_numeric": {"$ne": None},
                    f"{y_field}_numeric": {"$ne": None}
                }
            },
            # Project final output
            {
                "$project": {
                    "_id": 0,
                    x_field: f"${x_field}_numeric",
                    y_field: f"${y_field}_numeric"
                }
            },
            # Limit for performance (adjust as needed)
            {"$limit": 10000}
        ]
        return pipeline

    else:
        logger.warning(f"Unsupported chart type: {chart_type}")
        return []


# ----------------------------- Main Runner -----------------------------

def run_chart_pipeline(project_id):
    """
    Runs chart pipeline for any chart type.
    Reads chart configs, runs dynamic pipelines, and stores results.
    Works with any dataset structure - Netflix, Amazon, Finance, etc.
    Deletes charts that produce zero records after processing.
    """
    try:
        # Extract user_id
        user_id = project_id.split("PJ")[0]
        logger.info(f"Running chart pipeline for {project_id} (user {user_id})")

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

        # Define collections
        chart_collection = f"{project_id}_charts"
        data_collection = f"{project_id}_data"
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
        processed_count = 0
        failed_count = 0
        deleted_count = 0
        
        for chart in charts:
            chart_id = str(chart["_id"])
            chart_type = chart.get("chart_type", "unknown")
            chart_title = chart.get("title", "Untitled Chart")
            config = chart.get("config", {})
            display_mode = chart.get("display_mode", "direct")
            description = chart.get("description", "")

            logger.info(f"Processing chart: {chart_title} ({chart_type})")

            try:
                # Generate pipeline
                pipeline = generate_pipeline(chart_type, config)
                if not pipeline:
                    logger.warning(f"No pipeline generated for chart {chart_id}")
                    failed_count += 1
                    continue

                # Run aggregation
                result = list(db[data_collection].aggregate(pipeline))

                # Check if result is empty
                if len(result) == 0:
                    logger.warning(f"⚠️ Chart '{chart_title}' produced 0 records. Deleting from chart collection.")
                    
                    # Delete the chart from the chart collection
                    db[chart_collection].delete_one({"_id": ObjectId(chart_id)})
                    deleted_count += 1
                    
                    logger.info(f"🗑️ Deleted chart: {chart_title} (ID: {chart_id})")
                    continue

                # Store results (only if we have data)
                db[output_collection].insert_one({
                    "chart_id": chart_id,
                    "chart_type": chart_type,
                    "chart_title": chart_title,
                    "description": description,
                    "display_mode": display_mode,
                    "config": config,
                    "data": result,
                    # "record_count": len(result)
                })

                logger.info(f"✅ {chart_title} processed ({len(result)} records).")
                processed_count += 1
                
            except Exception as e:
                logger.error(f"Error processing chart {chart_title}: {e}", exc_info=True)
                failed_count += 1

        logger.info(f"🎯 Chart pipeline completed: {processed_count} successful, {failed_count} failed, {deleted_count} deleted (zero records).")

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