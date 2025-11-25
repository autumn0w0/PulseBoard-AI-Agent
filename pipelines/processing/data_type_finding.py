from typing import Dict, List, Any, Optional
import re
import pandas as pd
import sys

sys.path.append("../../")
from helpers.database.connection_to_db import connect_to_mongodb


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
        print(f"User ID '{user_id}' not found in client_config")
        return None
    
    project_info = None
    for project in client_config.get("projects", []):
        if project.get("project_id") == project_id:
            project_info = project
            break
    
    if not project_info:
        print(f"Project ID '{project_id}' not found for user '{user_id}'")
        return None
    
    return {
        "user_id": user_id,
        "db_name": client_config.get("db_name"),
        "project_info": project_info
    }


def infer_pandas_dtype(series: pd.Series) -> str:
    """
    Infer data type using pandas dtype inference.
    
    Args:
        series: Pandas series to analyze
        
    Returns:
        String representation of the data type
    """
    # Try to infer better types
    try:
        # Attempt to convert to datetime (suppress warnings)
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            pd.to_datetime(series, errors='raise')
        return "datetime"
    except (ValueError, TypeError):
        pass
    
    # Check pandas dtype
    dtype_str = str(series.dtype)
    
    if dtype_str.startswith('int'):
        return "integer"
    elif dtype_str.startswith('float'):
        return "float"
    elif dtype_str == 'bool':
        return "boolean"
    elif dtype_str == 'object':
        # Further inspect object types
        sample = series.dropna()
        if len(sample) == 0:
            return "null"
        
        first_val = sample.iloc[0]
        if isinstance(first_val, (list, tuple)):
            return "array"
        elif isinstance(first_val, dict):
            return "object"
        else:
            return "string"
    elif 'datetime' in dtype_str:
        return "datetime"
    else:
        return "string"


def analyze_collection_data_types(client, db_name: str, collection_name: str, use_pandas: bool = True, sample_count: int = 5) -> List[Dict]:
    """
    Analyze data types of all fields in a collection.
    
    Args:
        client: MongoDB client
        db_name: Database name
        collection_name: Collection name
        use_pandas: Use pandas for type inference (default: True)
        sample_count: Number of sample values to include (default: 5)
        
    Returns:
        List of dictionaries with attribute, data_type, and sample
    """
    db = client[db_name]
    collection = db[collection_name]
    
    # Get sample documents
    sample_size = 1000 if use_pandas else 100
    documents = list(collection.find().limit(sample_size))
    
    if not documents:
        print(f"No data found in collection '{collection_name}'")
        return []
    
    if use_pandas:
        # Convert to DataFrame
        df = pd.DataFrame(documents)
        
        # Remove _id column
        if '_id' in df.columns:
            df = df.drop('_id', axis=1)
        
        result = []
        for column in df.columns:
            try:
                data_type = infer_pandas_dtype(df[column])
                
                # Get sample values (non-null, unique)
                sample_values = df[column].dropna().unique()[:sample_count].tolist()
                
                # Convert datetime objects to strings for JSON serialization
                sample_values = [
                    str(val) if isinstance(val, (pd.Timestamp, pd.Timedelta)) else val
                    for val in sample_values
                ]
                
                result.append({
                    "attribute": column,
                    "data_type": data_type,
                    "sample": sample_values
                })
            except Exception as e:
                print(f"Error analyzing column '{column}': {e}")
                result.append({
                    "attribute": column,
                    "data_type": "unknown",
                    "sample": []
                })
        
        return sorted(result, key=lambda x: x['attribute'])
    
    else:
        # Basic type checking
        field_types = {}
        field_samples = {}
        
        for doc in documents:
            for field, value in doc.items():
                if field == "_id":
                    continue
                
                if value is None:
                    data_type = "null"
                elif isinstance(value, bool):
                    data_type = "boolean"
                elif isinstance(value, int):
                    data_type = "integer"
                elif isinstance(value, float):
                    data_type = "float"
                elif isinstance(value, str):
                    data_type = "string"
                elif isinstance(value, list):
                    data_type = "array"
                elif isinstance(value, dict):
                    data_type = "object"
                else:
                    data_type = "unknown"
                
                if field not in field_types:
                    field_types[field] = set()
                    field_samples[field] = []
                
                field_types[field].add(data_type)
                
                # Collect sample values (avoid duplicates and nulls)
                if value is not None and value not in field_samples[field] and len(field_samples[field]) < sample_count:
                    field_samples[field].append(value)
        
        result = []
        for field, types in sorted(field_types.items()):
            data_type = "/".join(sorted(types)) if len(types) > 1 else list(types)[0]
            result.append({
                "attribute": field,
                "data_type": data_type,
                "sample": field_samples.get(field, [])
            })
        
        return result


def save_data_types(client, db_name: str, project_id: str, data_types: List[Dict]) -> bool:
    """
    Save data types to {project_id}_data_type collection.
    
    Args:
        client: MongoDB client
        db_name: Database name
        project_id: Project ID
        data_types: List of data type mappings
        
    Returns:
        True if successful, False otherwise
    """
    db = client[db_name]
    data_type_collection_name = f"{project_id}_data_type"
    collection = db[data_type_collection_name]
    
    collection.delete_many({})
    
    if data_types:
        collection.insert_many(data_types)
        print(f"Saved {len(data_types)} data type mappings to '{data_type_collection_name}'")
        return True
    else:
        print("No data types to save")
        return False


def run_dtf(project_id: str, use_pandas: bool = True, master_db_name: str = "master") -> bool:
    """
    Main function to analyze and save data types for a project.
    
    Args:
        project_id: Project ID to process (e.g., "UID001PJ001")
        use_pandas: Use pandas for type inference (default: True)
        master_db_name: Name of the master database (default: "master")
        
    Returns:
        True if successful, False otherwise
        
    Example:
        run("UID001PJ001")
        run("UID001PJ001", use_pandas=False)
    """
    print(f"Processing project: {project_id}")
    print(f"Method: {'Pandas (lightweight)' if use_pandas else 'Basic type checking'}")
    
    # Connect to MongoDB
    client = connect_to_mongodb()
    if not client:
        print("Failed to connect to MongoDB")
        return False
    
    try:
        # Check if user and project exist
        config = check_user_and_project_exist(client, project_id, master_db_name)
        if not config:
            return False
        
        db_name = config["db_name"]
        data_collection_name = f"{project_id}_data"
        
        print(f"User ID: {config['user_id']}")
        print(f"Database: {db_name}")
        print(f"Data Collection: {data_collection_name}")
        
        # Check if data collection exists
        db = client[db_name]
        if data_collection_name not in db.list_collection_names():
            print(f"Collection '{data_collection_name}' does not exist in database '{db_name}'")
            return False
        
        # Analyze data types
        print("Analyzing data types...")
        data_types = analyze_collection_data_types(client, db_name, data_collection_name, use_pandas)
        
        if not data_types:
            print("No data types found")
            return False
        
        # Display found data types
        print("\nFound data types:")
        for dt in data_types:
            sample_preview = dt['sample'][:3] if len(dt['sample']) > 3 else dt['sample']
            sample_str = str(sample_preview) if sample_preview else "[]"
            print(f"  - {dt['attribute']}: {dt['data_type']}")
            print(f"    Sample: {sample_str}")
        
        # Save data types
        print("\nSaving data types...")
        success = save_data_types(client, db_name, project_id, data_types)
        
        if success:
            print(f"\n✓ Successfully processed project '{project_id}'")
        
        return success
        
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        client.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python data_type_finding.py <project_id> [--basic]")
        print("Example: python data_type_finding.py UID001PJ001")
        print("         python data_type_finding.py UID001PJ001 --basic")
        sys.exit(1)
    
    project_id = sys.argv[1]
    use_pandas = "--basic" not in sys.argv
    
    success = run_dtf(project_id, use_pandas=use_pandas)
    
    if not success:
        sys.exit(1)