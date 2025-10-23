from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure

def delete_everything_from_server(connection_string):
    """
    Delete ALL databases (and their collections/data) from the MongoDB server.
    Excludes system databases (admin, local, config) and skips databases with permission issues.
    
    Args:
        connection_string (str): MongoDB connection string
    """
    try:
        # Connect to MongoDB
        client = MongoClient(connection_string)
        
        # Test the connection
        client.admin.command('ping')
        print("✓ Successfully connected to MongoDB server")
        
        # System databases to exclude
        system_dbs = ['admin', 'local', 'config']
        
        # Get all database names
        all_databases = client.list_database_names()
        
        # Filter out system databases
        user_databases = [db for db in all_databases if db not in system_dbs]
        
        if not user_databases:
            print("⚠ No user databases found on the server")
            return
        
        print(f"\n📂 Found {len(user_databases)} user database(s) on the server:")
        for db_name in user_databases:
            print(f"  • {db_name}")
        
        total_dropped = 0
        skipped_databases = []
        
        # Drop each database
        for db_name in user_databases:
            print(f"\n{'='*60}")
            print(f"🗑️  Dropping database: {db_name}")
            print(f"{'='*60}")
            
            try:
                # Drop the entire database (removes all collections and data)
                client.drop_database(db_name)
                print(f"  ✅ Successfully dropped database '{db_name}'")
                total_dropped += 1
                
            except OperationFailure as e:
                if e.code == 13:  # Unauthorized
                    print(f"  ⚠ Skipping database '{db_name}': No permission to drop")
                    skipped_databases.append(db_name)
                else:
                    print(f"  ❌ Error dropping database '{db_name}': {e}")
                    skipped_databases.append(db_name)
            except Exception as e:
                print(f"  ❌ Unexpected error dropping database '{db_name}': {e}")
                skipped_databases.append(db_name)
        
        print(f"\n{'='*60}")
        print(f"🎉 DELETION COMPLETE!")
        print(f"✅ Total databases dropped: {total_dropped}")
        
        if skipped_databases:
            print(f"\n⚠ Skipped {len(skipped_databases)} database(s) due to permission issues:")
            for db in skipped_databases:
                print(f"  • {db}")
        
        print(f"{'='*60}")
        
        # Close the connection
        client.close()
        
    except ConnectionFailure as e:
        print(f"❌ Failed to connect to MongoDB: {e}")
    except OperationFailure as e:
        print(f"❌ Operation failed: {e}")
    except Exception as e:
        print(f"❌ An error occurred: {e}")

if __name__ == "__main__":
    # MongoDB connection string
    CONNECTION_STRING = "mongodb://admin:pulsebord@localhost:27017/datatodashboard?authSource=admin&authMechanism=SCRAM-SHA-256"
    
    # Warning prompt
    print("🔥🔥🔥 EXTREME WARNING 🔥🔥🔥")
    print("=" * 60)
    print("This will COMPLETELY DELETE ALL DATABASES on the MongoDB server!")
    print("This includes:")
    print("  • ALL databases (except admin, local, config)")
    print("  • ALL collections in those databases")
    print("  • ALL data in those collections")
    print("\n⚠️  System databases (admin, local, config) will be preserved.")
    print("⚠️  Databases without proper permissions will be skipped.")
    print("\n🔥 THIS ACTION CANNOT BE UNDONE! 🔥")
    print("🔥 EVERYTHING WILL BE PERMANENTLY DELETED! 🔥")
    print("=" * 60)
    
    confirmation = input("\nType 'DELETE EVERYTHING PERMANENTLY' to confirm: ")
    
    if confirmation == "DELETE EVERYTHING PERMANENTLY":
        print("\n🚀 Starting complete deletion process...\n")
        delete_everything_from_server(CONNECTION_STRING)
    else:
        print("❌ Operation cancelled. Nothing was deleted.")