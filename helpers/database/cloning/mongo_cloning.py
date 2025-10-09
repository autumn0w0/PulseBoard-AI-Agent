import pymongo
import argparse
from tqdm import tqdm
import sys


def clone_mongodb(
    source_connection_string, target_connection_string, database_name=None
):
    """
    Clone data from source MongoDB to target MongoDB
    Args:
        source_connection_string: Connection string for source MongoDB
        target_connection_string: Connection string for target MongoDB
        database_name: Optional database name to clone (if None, will clone all databases)
    """
    try:
        # Connect to source and target MongoDB instances
        source_client = pymongo.MongoClient(source_connection_string)
        target_client = pymongo.MongoClient(target_connection_string)

        print("Connected to both MongoDB instances successfully!")

        # Get list of databases from source
        source_db_names = source_client.list_database_names()

        # Filter out system databases
        system_dbs = ["admin", "local", "config"]
        db_names_to_clone = [db for db in source_db_names if db not in system_dbs]

        # If specific database is provided, only clone that one
        if database_name:
            if database_name in db_names_to_clone:
                db_names_to_clone = [database_name]
            else:
                print(f"Error: Database '{database_name}' not found in source MongoDB")
                return

        for db_name in db_names_to_clone:
            print(f"\nCloning database: {db_name}")
            source_db = source_client[db_name]
            target_db = target_client[db_name]

            # Get all collections in the database
            collections = source_db.list_collection_names()

            for collection_name in collections:
                print(f"  Cloning collection: {collection_name}")
                source_collection = source_db[collection_name]

                # Drop the target collection if it already exists
                if collection_name in target_db.list_collection_names():
                    target_db[collection_name].drop()

                # Explicitly create the collection in target (even if empty)
                target_collection = target_db[collection_name]
                
                # Count documents for progress bar
                total_docs = source_collection.count_documents({})

                # Copy all documents in batches
                if total_docs > 0:
                    batch_size = 1000
                    with tqdm(
                        total=total_docs, desc=f"    Progress", unit="docs"
                    ) as pbar:
                        for i in range(0, total_docs, batch_size):
                            docs = list(
                                source_collection.find().skip(i).limit(batch_size)
                            )
                            if docs:
                                # Remove MongoDB _id field if documents will be inserted with new _id
                                # Uncomment the following line if you want new IDs:
                                # for doc in docs:
                                #     if '_id' in doc:
                                #         del doc['_id']

                                if docs:
                                    # Insert documents in batch
                                    target_collection.insert_many(docs)
                                pbar.update(len(docs))
                else:
                    print("    Collection is empty - creating empty collection in target")
                    # Ensure empty collection exists by accessing it
                    # MongoDB creates collection on first write, so we do a dummy operation
                    target_db.create_collection(collection_name)

                # Clone indexes
                print("    Cloning indexes...")
                indexes = source_collection.index_information()
                for index_name, index_info in indexes.items():
                    # Skip the default _id index
                    if index_name != "_id_":
                        keys = index_info["key"]
                        options = {
                            k: v
                            for k, v in index_info.items()
                            if k not in ["ns", "v", "key"]
                        }
                        target_collection.create_index(keys, **options)

                print(f"    Completed cloning collection: {collection_name}")

        print("\nCloning completed successfully!")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Close connections
        if "source_client" in locals():
            source_client.close()
        if "target_client" in locals():
            target_client.close()


if __name__ == "__main__":
    # Pre-configured connection strings
    SOURCE_CONNECTION_STRING = (
        "PRODUNCTION STRING"
    )
    TARGET_CONNECTION_STRING = (
        "LOCAL STRING"
    )

    # Allow overriding via command line if needed
    parser = argparse.ArgumentParser(
        description="Clone MongoDB databases and collections"
    )
    parser.add_argument(
        "--source",
        default=SOURCE_CONNECTION_STRING,
        help="Source MongoDB connection string (production)",
    )
    parser.add_argument(
        "--target",
        default=TARGET_CONNECTION_STRING,
        help="Target MongoDB connection string (local)",
    )
    parser.add_argument("--database", help="Optional specific database to clone")

    args = parser.parse_args()

    print("Starting MongoDB clone process...")
    print(f"Source: Production MongoDB on 65.2.179.8")
    print(f"Target: Local MongoDB on localhost")
    print("Direction: Production → Local")
    print("-" * 50)

    clone_mongodb(args.source, args.target, args.database)