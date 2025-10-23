import weaviate
from weaviate.classes.init import Auth
import json

def inspect_weaviate():
    """
    Inspect and display all data present in Weaviate database.
    Shows collections (classes), their schemas, and object counts.
    """
    
    # Weaviate connection settings
    WEAVIATE_HOST = "127.0.0.1"
    WEAVIATE_PORT = 8080
    WEAVIATE_GRPC_PORT = 50051
    
    try:
        # Connect to Weaviate (v4 client)
        client = weaviate.connect_to_custom(
            http_host=WEAVIATE_HOST,
            http_port=WEAVIATE_PORT,
            http_secure=False,
            grpc_host=WEAVIATE_HOST,
            grpc_port=WEAVIATE_GRPC_PORT,
            grpc_secure=False
        )
        
        print("✓ Successfully connected to Weaviate")
        print(f"  Host: {WEAVIATE_HOST}:{WEAVIATE_PORT}")
        print(f"  gRPC: {WEAVIATE_HOST}:{WEAVIATE_GRPC_PORT}")
        
        # Get server meta information
        meta = client.get_meta()
        print(f"\n📊 Weaviate Server Info:")
        print(f"  Version: {meta.get('version', 'N/A')}")
        
        # Get all collections (classes)
        collections = client.collections.list_all()
        
        if not collections:
            print("\n⚠ No collections found in Weaviate")
            client.close()
            return
        
        print(f"\n📂 Found {len(collections)} collection(s):\n")
        
        total_objects = 0
        
        # Iterate through each collection
        for collection_name in collections:
            print(f"{'='*70}")
            print(f"📦 Collection: {collection_name}")
            print(f"{'='*70}")
            
            try:
                collection = client.collections.get(collection_name)
                
                # Get collection schema/config
                config = collection.config.get()
                print(f"\n  Description: {config.description if config.description else 'No description'}")
                print(f"  Vectorizer: {config.vectorizer}")
                
                # Count objects in collection
                response = collection.aggregate.over_all(total_count=True)
                count = response.total_count
                total_objects += count
                
                print(f"\n  📊 Total Objects: {count}")
                
                # Get properties/schema
                if config.properties:
                    print(f"\n  📋 Properties ({len(config.properties)}):")
                    for prop in config.properties:
                        print(f"    • {prop.name} ({prop.data_type})")
                
                # Show sample objects (first 5)
                if count > 0:
                    print(f"\n  🔍 Sample Objects (showing up to 5):")
                    
                    sample = collection.query.fetch_objects(limit=5)
                    
                    for idx, obj in enumerate(sample.objects, 1):
                        print(f"\n    Object {idx}:")
                        print(f"      UUID: {obj.uuid}")
                        print(f"      Properties:")
                        for key, value in obj.properties.items():
                            # Truncate long values
                            str_value = str(value)
                            if len(str_value) > 100:
                                str_value = str_value[:100] + "..."
                            print(f"        • {key}: {str_value}")
                
                print()
                
            except Exception as e:
                print(f"  ❌ Error inspecting collection '{collection_name}': {e}\n")
        
        print(f"{'='*70}")
        print(f"🎉 SUMMARY")
        print(f"{'='*70}")
        print(f"  Total Collections: {len(collections)}")
        print(f"  Total Objects: {total_objects}")
        print(f"{'='*70}")
        
        # Close connection
        client.close()
        print("\n✓ Connection closed")
        
    except Exception as e:
        print(f"❌ Error connecting to Weaviate: {e}")
        print("\nMake sure:")
        print("  1. Weaviate is running")
        print("  2. The host and port are correct")
        print("  3. You have the weaviate-client package installed:")
        print("     pip install weaviate-client")

if __name__ == "__main__":
    print("🔍 Weaviate Database Inspector")
    print("="*70)
    inspect_weaviate()