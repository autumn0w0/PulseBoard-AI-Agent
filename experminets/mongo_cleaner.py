from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure

def delete_all_data():
    # MongoDB connection string - try default port 27017
    connection_string = "mongodb://admin:pulsebord@localhost:27017/?authSource=admin&authMechanism=SCRAM-SHA-256"
    
    # System databases that should NOT be deleted
    system_dbs = ['admin', 'local', 'config']
    
    print("If connection fails, try these commands to check MongoDB:")
    print("1. Check if MongoDB is running: net start | findstr -i mongo")
    print("2. Start MongoDB: net start MongoDB")
    print("3. Check MongoDB status in Services (services.msc)")
    print("-" * 60)
    
    try:
        # Connect to MongoDB
        print("\nConnecting to MongoDB...")
        client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)
        
        # Test the connection
        client.admin.command('ping')
        print("✓ Connected successfully!")
        
        # List all databases
        all_databases = client.list_database_names()
        print(f"\nFound databases: {all_databases}")
        
        # Filter out system databases
        user_databases = [db for db in all_databases if db not in system_dbs]
        
        if not user_databases:
            print("\n✓ No user databases found to delete.")
            client.close()
            return
        
        print(f"\nDatabases to be deleted: {user_databases}")
        
        # Show collections in each database
        print("\n" + "=" * 60)
        print("DATABASE CONTENTS:")
        print("=" * 60)
        for db_name in user_databases:
            db = client[db_name]
            collections = db.list_collection_names()
            print(f"\n📁 Database: {db_name}")
            if collections:
                for coll_name in collections:
                    count = db[coll_name].count_documents({})
                    print(f"   └─ Collection: {coll_name} ({count} documents)")
            else:
                print("   └─ (empty)")
        
        print("\n" + "=" * 60)
        
        # Confirm deletion
        print("\n⚠️  WARNING: This will PERMANENTLY delete:")
        print(f"   - {len(user_databases)} database(s)")
        print("   - ALL collections inside them")
        print("   - ALL data inside those collections")
        print("\nThis action CANNOT be undone!")
        
        confirm = input("\nType 'DELETE ALL' to confirm: ")
        
        if confirm == 'DELETE ALL':
            print("\n🗑️  Deleting all user databases...")
            
            for db_name in user_databases:
                print(f"   Dropping database: {db_name}...", end=" ")
                client.drop_database(db_name)
                print("✓ Done")
            
            print(f"\n✓ Successfully deleted {len(user_databases)} database(s)!")
            
            # Show remaining databases
            remaining = client.list_database_names()
            print(f"\nRemaining databases: {remaining}")
        else:
            print("\n✗ Deletion cancelled. (You must type 'DELETE ALL' exactly)")
        
        # Close the connection
        client.close()
        
    except ConnectionFailure as e:
        print(f"\n✗ Failed to connect to MongoDB: {e}")
        print("\nTroubleshooting:")
        print("- Make sure MongoDB is running")
        print("- Check if the port is correct (27017 is default)")
        print("- Verify your username and password")
    except OperationFailure as e:
        print(f"\n✗ Operation failed: {e}")
        print("\nThis might be an authentication issue.")
    except Exception as e:
        print(f"\n✗ An error occurred: {e}")

if __name__ == "__main__":
    delete_all_data()