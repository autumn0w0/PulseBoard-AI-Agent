import sys
import json
import re
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
from tqdm import tqdm
import weaviate
from weaviate.util import generate_uuid5

sys.path.append("../../")
from helpers.database.connection_to_db import connect_to_mongodb
from helpers.database.connect_to_weaviate import connect_to_weaviatedb
from helpers.logger import get_logger

# Load environment variables
load_dotenv()

logger = get_logger(__name__)


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


class MongoToWeaviateMigrator:
    """
    Migrates data from MongoDB to Weaviate for a specific project.
    Handles both chart data (_cd) and column data types (_cdt) collections.
    """
    
    def __init__(self, project_id: str, master_db_name: str = "master"):
        """
        Initialize the migrator with MongoDB and Weaviate connections.
        
        Args:
            project_id: Project ID (e.g., "UID001PJ001")
            master_db_name: Name of the master database
        """
        self.project_id = project_id
        self.user_id, _ = parse_project_id(project_id)
        self.master_db_name = master_db_name
        
        # Connect to MongoDB
        self.mongo_client = connect_to_mongodb()
        if not self.mongo_client:
            raise ConnectionError("Failed to connect to MongoDB")
        
        # Verify project exists and get config
        self.config = check_user_and_project_exist(
            self.mongo_client, 
            project_id, 
            master_db_name
        )
        if not self.config:
            raise ValueError(f"Project {project_id} not found")
        
        self.db_name = self.config["db_name"]
        self.db = self.mongo_client[self.db_name]
        
        # Connect to Weaviate
        self.weaviate_client = connect_to_weaviatedb()
        if not self.weaviate_client:
            raise ConnectionError("Failed to connect to Weaviate")
        
        logger.info(f"Initialized migrator for project: {project_id}")
        logger.info(f"User ID: {self.user_id}, Database: {self.db_name}")
    
    def create_weaviate_collection(self, collection_suffix: str) -> str:
        """
        Create or get Weaviate collection for the given suffix.
        
        Args:
            collection_suffix: Either '_cd' or '_cdt'
            
        Returns:
            Collection name in Weaviate
        """
        # Generate collection name (Weaviate uses class names)
        # Convert {project_id}_weviate_cd to a valid class name
        collection_name = f"{self.project_id}_weviate{collection_suffix}"
        # Weaviate class names should start with uppercase and be alphanumeric
        class_name = collection_name.replace('_', '').replace('-', '')
        class_name = class_name[0].upper() + class_name[1:]
        
        # Check if collection already exists
        try:
            if self.weaviate_client.collections.exists(class_name):
                logger.info(f"Collection {class_name} already exists in Weaviate")
                return class_name
        except Exception as e:
            logger.debug(f"Collection check error (may not exist yet): {e}")
        
        # Define properties based on collection type
        if collection_suffix == '_cd':
            # Chart data properties
            properties = [
                weaviate.classes.config.Property(
                    name="chart_id",
                    data_type=weaviate.classes.config.DataType.TEXT,
                    description="Chart ID"
                ),
                weaviate.classes.config.Property(
                    name="chart_title",
                    data_type=weaviate.classes.config.DataType.TEXT,
                    description="Title of the chart"
                ),
                weaviate.classes.config.Property(
                    name="chart_type",
                    data_type=weaviate.classes.config.DataType.TEXT,
                    description="Type of chart"
                ),
                weaviate.classes.config.Property(
                    name="description",
                    data_type=weaviate.classes.config.DataType.TEXT,
                    description="Chart description"
                ),
                weaviate.classes.config.Property(
                    name="config_text",
                    data_type=weaviate.classes.config.DataType.TEXT,
                    description="Chart configuration as text"
                ),
                weaviate.classes.config.Property(
                    name="data_text",
                    data_type=weaviate.classes.config.DataType.TEXT,
                    description="Chart data summary"
                ),
                weaviate.classes.config.Property(
                    name="combined_text",
                    data_type=weaviate.classes.config.DataType.TEXT,
                    description="Combined text for searching"
                ),
                weaviate.classes.config.Property(
                    name="source_id",
                    data_type=weaviate.classes.config.DataType.TEXT,
                    description="Original MongoDB _id"
                ),
                weaviate.classes.config.Property(
                    name="project_id",
                    data_type=weaviate.classes.config.DataType.TEXT,
                    description="Project ID"
                )
            ]
        else:  # _cdt
            # Column data type properties
            properties = [
                weaviate.classes.config.Property(
                    name="attribute",
                    data_type=weaviate.classes.config.DataType.TEXT,
                    description="Attribute name"
                ),
                weaviate.classes.config.Property(
                    name="data_type",
                    data_type=weaviate.classes.config.DataType.TEXT,
                    description="Data type"
                ),
                weaviate.classes.config.Property(
                    name="original_type",
                    data_type=weaviate.classes.config.DataType.TEXT,
                    description="Original data type"
                ),
                weaviate.classes.config.Property(
                    name="was_corrected",
                    data_type=weaviate.classes.config.DataType.BOOL,
                    description="Whether the type was corrected"
                ),
                weaviate.classes.config.Property(
                    name="sample_text",
                    data_type=weaviate.classes.config.DataType.TEXT,
                    description="Sample values"
                ),
                weaviate.classes.config.Property(
                    name="combined_text",
                    data_type=weaviate.classes.config.DataType.TEXT,
                    description="Combined text for searching"
                ),
                weaviate.classes.config.Property(
                    name="source_id",
                    data_type=weaviate.classes.config.DataType.TEXT,
                    description="Original MongoDB _id"
                ),
                weaviate.classes.config.Property(
                    name="project_id",
                    data_type=weaviate.classes.config.DataType.TEXT,
                    description="Project ID"
                )
            ]
        
        # Create collection with vector configuration
        self.weaviate_client.collections.create(
            name=class_name,
            description=f"Data for {self.project_id}{collection_suffix}",
            vectorizer_config=weaviate.classes.config.Configure.Vectorizer.none(),
            properties=properties
        )
        
        logger.info(f"Created Weaviate collection: {class_name}")
        return class_name
    
    def migrate_collection(self, collection_suffix: str) -> Dict[str, int]:
        """
        Migrate a collection from MongoDB to Weaviate.
        
        Args:
            collection_suffix: Either '_cd' or '_cdt'
            
        Returns:
            Dictionary with migration statistics
        """
        # Collection names in MongoDB
        data_collection = f"{self.project_id}_weaviate{collection_suffix}"
        vector_collection = f"{self.project_id}_weaviate_vectors{collection_suffix}"
        
        logger.info(f"Starting migration for {data_collection}")
        
        # Check if collections exist
        existing_collections = self.db.list_collection_names()
        
        if data_collection not in existing_collections:
            logger.warning(f"Collection {data_collection} does not exist in MongoDB")
            return {"total": 0, "success": 0, "errors": 0, "no_vector": 0}
        
        if vector_collection not in existing_collections:
            logger.warning(f"Vector collection {vector_collection} does not exist in MongoDB")
            return {"total": 0, "success": 0, "errors": 0, "no_vector": 0}
        
        # Fetch data from MongoDB
        data_docs = list(self.db[data_collection].find())
        logger.info(f"Found {len(data_docs)} documents in {data_collection}")
        
        if not data_docs:
            logger.warning(f"No documents found in {data_collection}")
            return {"total": 0, "success": 0, "errors": 0, "no_vector": 0}
        
        # Create or get Weaviate collection
        class_name = self.create_weaviate_collection(collection_suffix)
        collection = self.weaviate_client.collections.get(class_name)
        
        # Load vectors from MongoDB
        logger.info(f"Loading vectors from {vector_collection}")
        vectors = {}
        vector_docs = self.db[vector_collection].find()
        for vec_doc in vector_docs:
            source_id = str(vec_doc['source_id'])
            vectors[source_id] = vec_doc['vector']
        
        logger.info(f"Loaded {len(vectors)} vectors from {vector_collection}")
        
        # Migrate data with progress bar
        stats = {
            "total": len(data_docs),
            "success": 0,
            "errors": 0,
            "no_vector": 0
        }
        
        with collection.batch.dynamic() as batch:
            for doc in tqdm(data_docs, desc=f"Migrating {data_collection}"):
                try:
                    # Get the source_id
                    source_id = str(doc['_id'])
                    
                    # Prepare properties
                    properties = {}
                    for key, value in doc.items():
                        if key == '_id':
                            properties['source_id'] = str(value)
                        elif key not in ['vector']:
                            # Handle different data types
                            if value is None:
                                continue
                            elif isinstance(value, (str, int, float, bool)):
                                properties[key] = value
                            elif isinstance(value, dict):
                                properties[key] = json.dumps(value)
                            elif isinstance(value, list):
                                properties[key] = json.dumps(value)
                            else:
                                properties[key] = str(value)
                    
                    # Ensure project_id is included
                    if 'project_id' not in properties:
                        properties['project_id'] = self.project_id
                    
                    # Get vector
                    vector = vectors.get(source_id)
                    
                    if vector:
                        # Add object with vector
                        uuid = generate_uuid5(source_id)
                        batch.add_object(
                            properties=properties,
                            vector=vector,
                            uuid=uuid
                        )
                        stats["success"] += 1
                    else:
                        logger.warning(f"No vector found for source_id: {source_id}")
                        # Add without vector
                        uuid = generate_uuid5(source_id)
                        batch.add_object(
                            properties=properties,
                            uuid=uuid
                        )
                        stats["no_vector"] += 1
                        stats["success"] += 1
                        
                except Exception as e:
                    logger.error(f"Error migrating document {doc.get('_id')}: {str(e)}")
                    stats["errors"] += 1
        
        logger.info(f"Migration completed for {data_collection}")
        logger.info(f"Stats: {json.dumps(stats, indent=2)}")
        
        return stats
    
    def migrate_all(self) -> Dict[str, Any]:
        """
        Migrate both _cd and _cdt collections.
        
        Returns:
            Dictionary with complete migration summary
        """
        logger.info(f"Starting full migration for project {self.project_id}")
        
        summary = {
            "success": True,
            "project_id": self.project_id,
            "database": self.db_name,
            "collections": {}
        }
        
        try:
            # Migrate chart data (_cd)
            logger.info("=" * 60)
            logger.info("Migrating Chart Data (_cd)")
            logger.info("=" * 60)
            cd_stats = self.migrate_collection('_cd')
            summary["collections"]["chart_data"] = {
                "collection": f"{self.project_id}_weaviate_cd",
                "stats": cd_stats
            }
            
            # Migrate column data types (_cdt)
            logger.info("=" * 60)
            logger.info("Migrating Column Data Types (_cdt)")
            logger.info("=" * 60)
            cdt_stats = self.migrate_collection('_cdt')
            summary["collections"]["column_data"] = {
                "collection": f"{self.project_id}_weaviate_cdt",
                "stats": cdt_stats
            }
            
            # Calculate totals
            total_success = cd_stats["success"] + cdt_stats["success"]
            total_errors = cd_stats["errors"] + cdt_stats["errors"]
            total_processed = cd_stats["total"] + cdt_stats["total"]
            
            summary["totals"] = {
                "processed": total_processed,
                "success": total_success,
                "errors": total_errors
            }
            
            logger.info("=" * 60)
            logger.info("🎯 Full Migration Completed!")
            logger.info("=" * 60)
            logger.info(f"Summary:\n{json.dumps(summary, indent=2)}")
            
            print(f"\n✓ Successfully migrated project '{self.project_id}'")
            print(f"  - Chart Data (_cd): {cd_stats['success']}/{cd_stats['total']} documents")
            print(f"  - Column Data (_cdt): {cdt_stats['success']}/{cdt_stats['total']} documents")
            print(f"  - Total: {total_success}/{total_processed} documents migrated")
            if total_errors > 0:
                print(f"  ⚠ Errors: {total_errors}")
            
            return summary
            
        except Exception as e:
            logger.error(f"Migration failed: {str(e)}")
            summary["success"] = False
            summary["error"] = str(e)
            return summary
    
    def close(self):
        """Close connections."""
        if self.mongo_client:
            self.mongo_client.close()
        if self.weaviate_client:
            self.weaviate_client.close()
        logger.info("Connections closed")


def run_dtw(project_id: str, master_db_name: str = "master") -> Dict[str, Any]:
    """
    Run data to Weaviate migration.
    
    Args:
        project_id: Project ID (e.g., "UID001PJ001")
        master_db_name: Name of the master database
    
    Returns:
        Dictionary with migration summary
    
    Example:
        run_dtw("UID001PJ001")
    """
    migrator = None
    try:
        migrator = MongoToWeaviateMigrator(
            project_id=project_id,
            master_db_name=master_db_name
        )
        result = migrator.migrate_all()
        return result
        
    except Exception as e:
        logger.error(f"Migration failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "error": str(e),
            "project_id": project_id
        }
        
    finally:
        if migrator:
            migrator.close()


def main():
    """
    Command-line entry point for the migration pipeline.
    """
    if len(sys.argv) < 2:
        print("Usage: python data_to_weaviate.py <project_id>")
        print("Example: python data_to_weaviate.py UID001PJ001")
        sys.exit(1)

    project_id = sys.argv[1]
    result = run_dtw(project_id)
    
    if result.get("success"):
        sys.exit(0)
    else:
        print(f"\n❌ Migration failed: {result.get('error', 'Unknown error')}")
        sys.exit(1)


if __name__ == "__main__":
    main()