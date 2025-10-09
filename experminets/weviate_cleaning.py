import os
from dotenv import load_dotenv
import weaviate
from weaviate.classes.init import Auth

# Load environment variables from .env file
load_dotenv()

# Get configuration from environment variables
WEAVIATE_HOST = os.getenv("WEAVIATE_HOST", "127.0.0.1")
WEAVIATE_PORT = os.getenv("WEAVIATE_PORT", "8080")
WEAVIATE_GRPC_HOST = os.getenv("WEAVIATE_GRPC_HOST", "127.0.0.1")
WEAVIATE_GRPC_PORT = os.getenv("WEAVIATE_GRPC_PORT", "50051")
WEAVIATE_SECURE = os.getenv("WEAVIATE_SECURE", "false").lower() == "true"
WEAVIATE_API_KEY = os.getenv("WEAVIATE_API_KEY", "")

# Build the connection URL
protocol = "https" if WEAVIATE_SECURE else "http"
weaviate_url = f"{protocol}://{WEAVIATE_HOST}:{WEAVIATE_PORT}"

print("=" * 70)
print("⚠️  WEAVIATE DATA DELETION SCRIPT ⚠️")
print("=" * 70)
print(f"Connected to: {weaviate_url}")
print(f"gRPC endpoint: {WEAVIATE_GRPC_HOST}:{WEAVIATE_GRPC_PORT}")
print("=" * 70)

try:
    # Connect to Weaviate
    if WEAVIATE_API_KEY:
        client = weaviate.connect_to_custom(
            http_host=WEAVIATE_HOST,
            http_port=int(WEAVIATE_PORT),
            http_secure=WEAVIATE_SECURE,
            grpc_host=WEAVIATE_GRPC_HOST,
            grpc_port=int(WEAVIATE_GRPC_PORT),
            grpc_secure=WEAVIATE_SECURE,
            auth_credentials=Auth.api_key(WEAVIATE_API_KEY)
        )
    else:
        client = weaviate.connect_to_custom(
            http_host=WEAVIATE_HOST,
            http_port=int(WEAVIATE_PORT),
            http_secure=WEAVIATE_SECURE,
            grpc_host=WEAVIATE_GRPC_HOST,
            grpc_port=int(WEAVIATE_GRPC_PORT),
            grpc_secure=WEAVIATE_SECURE
        )
    
    # Check if connection is ready
    if not client.is_ready():
        print("✗ Failed to connect to Weaviate")
        exit(1)
    
    print("✓ Successfully connected to Weaviate!\n")
    
    # Get all collections
    collections = client.collections.list_all()
    
    if not collections:
        print("ℹ️  No collections found. Weaviate is already empty.")
        client.close()
        exit(0)
    
    # Display collections to be deleted
    print(f"Found {len(collections)} collection(s) to delete:\n")
    
    for collection_name, collection_config in collections.items():
        collection = client.collections.get(collection_name)
        try:
            aggregate_result = collection.aggregate.over_all(total_count=True)
            count = aggregate_result.total_count
            print(f"  • {collection_name} ({count} objects)")
        except:
            print(f"  • {collection_name}")
    
    print("\n" + "=" * 70)
    print("⚠️  WARNING: This will permanently delete ALL collections and data!")
    print("=" * 70)
    
    # Safety confirmation
    confirmation = input("\nType 'DELETE ALL' to confirm deletion: ")
    
    if confirmation != "DELETE ALL":
        print("\n❌ Deletion cancelled. No changes were made.")
        client.close()
        exit(0)
    
    print("\n🗑️  Starting deletion process...\n")
    
    # Delete each collection
    deleted_count = 0
    failed_count = 0
    
    for collection_name in collections.keys():
        try:
            client.collections.delete(collection_name)
            print(f"  ✓ Deleted collection: {collection_name}")
            deleted_count += 1
        except Exception as e:
            print(f"  ✗ Failed to delete {collection_name}: {str(e)}")
            failed_count += 1
    
    # Summary
    print("\n" + "=" * 70)
    print("DELETION SUMMARY")
    print("=" * 70)
    print(f"✓ Successfully deleted: {deleted_count} collection(s)")
    if failed_count > 0:
        print(f"✗ Failed to delete: {failed_count} collection(s)")
    print("\n✅ Weaviate instance has been cleaned!")
    
    # Verify deletion
    remaining = client.collections.list_all()
    if remaining:
        print(f"\n⚠️  Warning: {len(remaining)} collection(s) still remain")
    else:
        print("✓ Verified: All collections have been deleted")
    
    # Close the connection
    client.close()
    
except Exception as e:
    print(f"Error: {str(e)}")
    print("\nMake sure:")
    print("1. Weaviate is running")
    print("2. The connection details in .env are correct")
    print("3. You have 'weaviate-client' and 'python-dotenv' installed:")
    print("   pip install weaviate-client python-dotenv")