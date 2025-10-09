from pymongo import MongoClient
from typing import Dict, List, Any, Optional
import re

class ProjectDataTypeAnalyzer:
    def __init__(self, master_db_uri: str, master_db_name: str = "master"):
        """
        Initialize the analyzer with MongoDB connection details.
        
        Args:
            master_db_uri: MongoDB connection URI
            master_db_name: Name of the master database (default: "master")
        """
        self.client = MongoClient(master_db_uri)
        self.master_db = self.client[master_db_name]
        
    def parse_project_id(self, project_id: str) -> tuple:
        """
        Parse project_id into user_id and project number.
        
        Args:
            project_id: Project ID in format {user_id}PJ00x
            
        Returns:
            Tuple of (user_id, project_id)
        """
        # Extract user_id (everything before 'PJ')
        match = re.match(r'(.+?)(PJ\d+)$', project_id)
        if match:
            user_id = match.group(1)
            return user_id, project_id
        else:
            raise ValueError(f"Invalid project_id format: {project_id}")
    
    def check_user_and_project_exist(self, project_id: str) -> Optional[Dict]:
        """
        Check if user_id and project_id exist in client_config collection.
        
        Args:
            project_id: Project ID to check
            
        Returns:
            Project configuration dict if exists, None otherwise
        """
        user_id, _ = self.parse_project_id(project_id)
        
        # Find user in client_config collection
        client_config = self.master_db.client_config.find_one({"user_id": user_id})
        
        if not client_config:
            print(f"User ID '{user_id}' not found in client_config")
            return None
        
        # Check if project exists in user's projects
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
    
    def infer_data_type(self, value: Any) -> str:
        """
        Infer the data type of a value.
        
        Args:
            value: The value to check
            
        Returns:
            String representation of the data type
        """
        if value is None:
            return "null"
        elif isinstance(value, bool):
            return "boolean"
        elif isinstance(value, int):
            return "integer"
        elif isinstance(value, float):
            return "float"
        elif isinstance(value, str):
            return "string"
        elif isinstance(value, list):
            return "array"
        elif isinstance(value, dict):
            return "object"
        else:
            return "unknown"
    
    def analyze_collection_data_types(self, db_name: str, collection_name: str) -> List[Dict]:
        """
        Analyze data types of all fields in a collection.
        
        Args:
            db_name: Database name
            collection_name: Collection name
            
        Returns:
            List of dictionaries with attribute and data_type
        """
        db = self.client[db_name]
        collection = db[collection_name]
        
        # Get sample documents to infer data types
        documents = list(collection.find().limit(100))
        
        if not documents:
            print(f"No data found in collection '{collection_name}'")
            return []
        
        # Collect all unique fields and their data types
        field_types = {}
        
        for doc in documents:
            for field, value in doc.items():
                if field == "_id":
                    continue  # Skip MongoDB's internal _id field
                
                data_type = self.infer_data_type(value)
                
                if field not in field_types:
                    field_types[field] = set()
                field_types[field].add(data_type)
        
        # Create result list with consolidated data types
        result = []
        for field, types in sorted(field_types.items()):
            # If multiple types found, join them
            data_type = "/".join(sorted(types)) if len(types) > 1 else list(types)[0]
            result.append({
                "attribute": field,
                "data_type": data_type
            })
        
        return result
    
    def save_data_types(self, db_name: str, project_id: str, data_types: List[Dict]) -> bool:
        """
        Save data types to {project_id}_data_type collection.
        
        Args:
            db_name: Database name
            project_id: Project ID
            data_types: List of data type mappings
            
        Returns:
            True if successful, False otherwise
        """
        db = self.client[db_name]
        data_type_collection_name = f"{project_id}_data_type"
        collection = db[data_type_collection_name]
        
        # Clear existing data
        collection.delete_many({})
        
        # Insert new data types
        if data_types:
            collection.insert_many(data_types)
            print(f"Saved {len(data_types)} data type mappings to '{data_type_collection_name}'")
            return True
        else:
            print("No data types to save")
            return False
    
    def process_project(self, project_id: str) -> bool:
        """
        Main method to process a project and save its data types.
        
        Args:
            project_id: Project ID to process
            
        Returns:
            True if successful, False otherwise
        """
        print(f"Processing project: {project_id}")
        
        # Step 1: Check if user and project exist
        config = self.check_user_and_project_exist(project_id)
        if not config:
            return False
        
        db_name = config["db_name"]
        data_collection_name = f"{project_id}_data"
        
        print(f"User ID: {config['user_id']}")
        print(f"Database: {db_name}")
        print(f"Data Collection: {data_collection_name}")
        
        # Step 2: Check if data collection exists
        db = self.client[db_name]
        if data_collection_name not in db.list_collection_names():
            print(f"Collection '{data_collection_name}' does not exist in database '{db_name}'")
            return False
        
        # Step 3: Analyze data types
        print("Analyzing data types...")
        data_types = self.analyze_collection_data_types(db_name, data_collection_name)
        
        if not data_types:
            print("No data types found")
            return False
        
        # Display found data types
        print("\nFound data types:")
        for dt in data_types:
            print(f"  - {dt['attribute']}: {dt['data_type']}")
        
        # Step 4: Save data types
        print("\nSaving data types...")
        success = self.save_data_types(db_name, project_id, data_types)
        
        if success:
            print(f"\n✓ Successfully processed project '{project_id}'")
        
        return success
    
    def close(self):
        """Close MongoDB connection."""
        self.client.close()


# Example usage
if __name__ == "__main__":
    import sys
    
    # Configuration
    MONGO_URI = "mongodb://admin:pulsebord@localhost:27017/datatodashboard?authSource=admin&authMechanism=SCRAM-SHA-256"
    MASTER_DB_NAME = "master"
    
    # Check if project_id is provided as command line argument
    if len(sys.argv) < 2:
        print("Usage: python data_type_finding.py <project_id>")
        print("Example: python data_type_finding.py UID001PJ001")
        sys.exit(1)
    
    project_id = sys.argv[1]
    
    # Initialize analyzer
    analyzer = ProjectDataTypeAnalyzer(MONGO_URI, MASTER_DB_NAME)
    
    try:
        # Process a project
        success = analyzer.process_project(project_id)
        
        if success:
            print("\nData type analysis completed successfully!")
        else:
            print("\nData type analysis failed.")
            sys.exit(1)
            
    except Exception as e:
        print(f"\nError: {str(e)}")
        sys.exit(1)
    finally:
        analyzer.close()