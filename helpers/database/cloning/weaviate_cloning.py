import weaviate
from weaviate.auth import AuthApiKey
import argparse
import json
from tqdm import tqdm
import time
import weaviate.classes.config as wvc_config
import requests
from typing import Optional, List


def create_weaviate_client(
    host: str,
    port: str,
    grpc_host: str,
    grpc_port: str,
    secure: str,
    api_key: Optional[str] = None,
) -> weaviate.WeaviateClient:
    """Create a Weaviate client with the given parameters using v4 client"""
    connection_params = weaviate.connect.ConnectionParams.from_params(
        http_host=host,
        http_port=int(port),
        http_secure=secure.lower() == "true",
        grpc_host=grpc_host,
        grpc_port=int(grpc_port),
        grpc_secure=secure.lower() == "true",
    )

    auth_credentials = AuthApiKey(api_key=api_key) if api_key else None

    client = weaviate.WeaviateClient(
        connection_params=connection_params, auth_client_secret=auth_credentials
    )

    client.connect()
    return client


def check_ollama_availability() -> bool:
    """Check if Ollama service is available at localhost:11434"""
    try:
        response = requests.get("http://localhost:11434/api/version", timeout=3)
        return response.status_code == 200
    except Exception:
        return False


def should_use_ollama(
    source_vectorizer: str, available_modules: List[str], ollama_available: bool
) -> bool:
    """Determine if we should use Ollama vectorizer"""
    return (
        source_vectorizer == "text2vec-ollama"
        and "text2vec-ollama" in available_modules
        and ollama_available
    )


def clone_collection(
    source_client: weaviate.WeaviateClient,
    target_client: weaviate.WeaviateClient,
    collection_name: str,
    available_modules: List[str],
    ollama_available: bool,
) -> bool:
    """Clone a single collection from source to target.
    Returns True if successful, False if skipped or failed."""
    print(f"\n--- Processing collection '{collection_name}' ---")

    # Get source collection
    source_collection = source_client.collections.get(collection_name)
    source_schema = source_collection.config.get().to_dict()
    source_count = source_collection.aggregate.over_all().total_count

    # Check target collection if it exists
    target_collections = target_client.collections.list_all()
    target_count = 0
    if collection_name in target_collections:
        target_count = (
            target_client.collections.get(collection_name)
            .aggregate.over_all()
            .total_count
        )

    # Skip if counts match and target has data
    if source_count == target_count and target_count > 0:
        print(
            f"Skipping '{collection_name}' (source={source_count}, target={target_count})"
        )
        return True

    # Skip if no objects in source
    if source_count == 0:
        print(f"Skipping empty collection '{collection_name}'")
        return True

    # Get collection configuration
    properties = [
        wvc_config.Property(name=prop.name, data_type=prop.data_type)
        for prop in source_collection.config.get().properties
    ]

    # Determine vectorizer configuration
    source_vectorizer = source_collection.config.get().vectorizer
    print(f"Source vectorizer: {source_vectorizer}")

    if should_use_ollama(source_vectorizer, available_modules, ollama_available):
        vectorizer_config = wvc_config.Configure.Vectorizer.text2vec_ollama()
        print("Using text2vec-ollama for target")
    else:
        vectorizer_config = wvc_config.Configure.Vectorizer.none()
        print("Using no vectorizer - will copy vectors directly")

    # Delete existing collection in target if it exists
    if collection_name in target_collections:
        print(f"Deleting existing collection '{collection_name}' from target")
        target_client.collections.delete(collection_name)
        time.sleep(1)  # Wait for deletion to complete

    # Create collection in target
    print(f"Creating collection '{collection_name}' in target")
    target_client.collections.create(
        name=collection_name, properties=properties, vectorizer_config=vectorizer_config
    )

    # Clone objects with progress tracking
    batch_size = 50
    total_imported = 0
    failed_objects = 0

    with tqdm(total=source_count, desc=f"Importing {collection_name}") as pbar:
        cursor = None
        while True:
            # Fetch objects from source
            result = source_collection.query.fetch_objects(
                limit=batch_size, after=cursor, include_vector=True
            )

            if not result.objects:
                break

            # Prepare batch for target
            target_collection = target_client.collections.get(collection_name)
            with target_collection.batch.dynamic() as batch:
                for obj in result.objects:
                    try:
                        batch.add_object(
                            properties=obj.properties,
                            uuid=obj.uuid,
                            vector=obj.vector["default"] if obj.vector else None,
                        )
                    except Exception as e:
                        print(f"Error adding object {obj.uuid}: {str(e)}")
                        failed_objects += 1

            # Update progress
            batch_count = len(result.objects)
            total_imported += batch_count
            pbar.update(batch_count)
            cursor = result.objects[-1].uuid if result.objects else None

            # Small delay between batches
            time.sleep(0.1)

    # Verify results
    final_count = target_collection.aggregate.over_all().total_count
    success = final_count == source_count
    status = "SUCCESS" if success else "PARTIAL"
    print(f"Import {status}: {final_count}/{source_count} objects")
    if failed_objects > 0:
        print(f"Warning: {failed_objects} objects failed to import")

    return success


def clone_weaviate(
    source_client: weaviate.WeaviateClient,
    target_client: weaviate.WeaviateClient,
    class_filter: Optional[List[str]] = None,
) -> None:
    """Clone data from source Weaviate to target Weaviate"""
    try:
        # Verify connections
        if not source_client.is_ready() or not target_client.is_ready():
            raise Exception("One or both Weaviate instances are not ready")

        # Get target capabilities
        target_meta = target_client.get_meta()
        available_modules = [m.lower() for m in target_meta.get("modules", {}).keys()]
        ollama_available = check_ollama_availability()

        print(f"\nTarget Weaviate modules: {', '.join(available_modules)}")
        print(f"Ollama service available: {ollama_available}")

        # Get collections to clone
        collection_names = list(source_client.collections.list_all().keys())
        if class_filter:
            collection_names = [
                name for name in collection_names if name in class_filter
            ]

        if not collection_names:
            print("No collections found to clone")
            return

        print(f"\nCollections to clone: {', '.join(collection_names)}")

        # Clone each collection
        success_count = 0
        for collection_name in collection_names:
            try:
                if clone_collection(
                    source_client,
                    target_client,
                    collection_name,
                    available_modules,
                    ollama_available,
                ):
                    success_count += 1
            except Exception as e:
                print(f"Failed to clone collection '{collection_name}': {str(e)}")
                continue

        print(
            f"\nCloning completed: {success_count}/{len(collection_names)} collections successful"
        )

    except Exception as e:
        print(f"Error during cloning: {str(e)}")
    finally:
        source_client.close()
        target_client.close()


if __name__ == "__main__":
    # Configuration
    SOURCE_CONFIG = {
        "host": "65.2.179.8",
        "port": "8080",
        "grpc_host": "65.2.179.8",
        "grpc_port": "50051",
        "secure": "false",
        "api_key": "v0f6qw49D1GZbxE",
    }

    TARGET_CONFIG = {
        "host": "127.0.0.1",
        "port": "8080",
        "grpc_host": "127.0.0.1",
        "grpc_port": "50051",
        "secure": "false",
        "api_key": "",
    }

    # Parse arguments
    parser = argparse.ArgumentParser(description="Clone Weaviate data")
    parser.add_argument(
        "--classes", help="Optional comma-separated list of classes to clone"
    )
    parser.add_argument(
        "--force", action="store_true", help="Force reimport even if counts match"
    )
    args = parser.parse_args()

    class_filter = args.classes.split(",") if args.classes else None

    print("\nStarting Weaviate clone process...")
    print(f"Source: {SOURCE_CONFIG['host']}:{SOURCE_CONFIG['port']}")
    print(f"Target: {TARGET_CONFIG['host']}:{TARGET_CONFIG['port']}")
    if class_filter:
        print(f"Filtering to classes: {', '.join(class_filter)}")
    print("-" * 50)

    try:
        # Create clients
        source_client = create_weaviate_client(**SOURCE_CONFIG)
        target_client = create_weaviate_client(**TARGET_CONFIG)

        # Start clone process
        clone_weaviate(source_client, target_client, class_filter)

    except KeyboardInterrupt:
        print("\nMigration interrupted by user")
    except Exception as e:
        print(f"Fatal error: {str(e)}")
