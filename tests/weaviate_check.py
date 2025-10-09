import weaviate
from weaviate.classes.init import Auth
import sys
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def test_weaviate_connection():
    """Test Weaviate connection and basic operations"""
    
    print("🔍 Testing Weaviate Connection...")
    print("-" * 50)
    
    # Load configuration from environment variables
    weaviate_host = os.getenv("WEAVIATE_HOST", "127.0.0.1")
    weaviate_port = int(os.getenv("WEAVIATE_PORT", "8080"))
    weaviate_grpc_port = int(os.getenv("WEAVIATE_GRPC_PORT", "50051"))
    weaviate_secure = os.getenv("WEAVIATE_SECURE", "false").lower() == "true"
    weaviate_api_key = os.getenv("WEAVIATE_API_KEY", "")
    
    print(f"📋 Configuration:")
    print(f"   Host: {weaviate_host}")
    print(f"   Port: {weaviate_port}")
    print(f"   gRPC Port: {weaviate_grpc_port}")
    print(f"   Secure: {weaviate_secure}")
    print(f"   API Key: {'Set' if weaviate_api_key else 'Not set'}")
    print("-" * 50)
    
    try:
        # Connect to Weaviate with environment variables
        if weaviate_api_key:
            # Connect with API key authentication
            client = weaviate.connect_to_local(
                host=weaviate_host,
                port=weaviate_port,
                grpc_port=weaviate_grpc_port,
                auth_credentials=Auth.api_key(weaviate_api_key)
            )
        else:
            # Connect without authentication
            client = weaviate.connect_to_local(
                host=weaviate_host,
                port=weaviate_port,
                grpc_port=weaviate_grpc_port
            )
        
        print("✅ Successfully connected to Weaviate!")
        
        # Check if Weaviate is ready
        if client.is_ready():
            print("✅ Weaviate is ready!")
        else:
            print("❌ Weaviate is not ready")
            return False
        
        # Get meta information
        meta = client.get_meta()
        print(f"\n📊 Weaviate Version: {meta.get('version', 'Unknown')}")
        
        # List available modules
        modules = meta.get('modules', {})
        print(f"\n🧩 Available Modules:")
        for module_name, module_info in modules.items():
            print(f"   - {module_name}")
        
        # Test: Create a test collection
        print("\n🧪 Testing Collection Creation...")
        collection_name = "TestCollection"
        
        # Delete if exists
        if client.collections.exists(collection_name):
            client.collections.delete(collection_name)
            print(f"   Deleted existing '{collection_name}'")
        
        # Create new collection
        test_collection = client.collections.create(
            name=collection_name,
            description="A test collection to verify Weaviate is working"
        )
        print(f"✅ Created collection: {collection_name}")
        
        # Test: Insert data
        print("\n📝 Testing Data Insertion...")
        test_collection.data.insert(
            properties={
                "name": "Test Item 1",
                "description": "This is a test item"
            }
        )
        test_collection.data.insert(
            properties={
                "name": "Test Item 2",
                "description": "This is another test item"
            }
        )
        print("✅ Inserted 2 test items")
        
        # Test: Query data
        print("\n🔎 Testing Data Query...")
        response = test_collection.query.fetch_objects(limit=10)
        
        print(f"✅ Retrieved {len(response.objects)} objects:")
        for obj in response.objects:
            print(f"   - {obj.properties.get('name')}: {obj.properties.get('description')}")
        
        # Test: Count objects
        total = len(test_collection)
        print(f"\n📊 Total objects in collection: {total}")
        
        # Cleanup
        print("\n🧹 Cleaning up...")
        client.collections.delete(collection_name)
        print(f"✅ Deleted test collection: {collection_name}")
        
        print("\n" + "=" * 50)
        print("🎉 All tests passed! Weaviate is working correctly!")
        print("=" * 50)
        
        client.close()
        return True
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        print("\n💡 Troubleshooting:")
        print("   1. Make sure Docker container is running: docker ps")
        print("   2. Check if ports 8080 and 50051 are accessible")
        print("   3. Try: docker-compose up -d")
        return False

if __name__ == "__main__":
    success = test_weaviate_connection()
    sys.exit(0 if success else 1)