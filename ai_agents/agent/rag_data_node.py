import sys
import json
import re
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv

sys.path.append("../../")
from helpers.database.connection_to_db import connect_to_mongodb
from helpers.database.connect_to_weaviate import connect_to_weaviatedb
from helpers.llm.call_llm import call_llm
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


class RAGPipeline:
    """
    Retrieval-Augmented Generation pipeline for querying project attribute data.
    Retrieves context from Weaviate attributes collection and generates answers using LLM.
    """
    
    def __init__(self, project_id: str, master_db_name: str = "master"):
        """
        Initialize RAG pipeline with connections to MongoDB and Weaviate.
        
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
        
        # Extract project metadata
        self.project_name = self.config["project_info"].get("name_of_project", "Unknown Project")
        self.project_domain = self.config["project_info"].get("domain", "Unknown Domain")
        
        # Connect to Weaviate
        self.weaviate_client = connect_to_weaviatedb()
        if not self.weaviate_client:
            raise ConnectionError("Failed to connect to Weaviate")
        
        # Generate Weaviate collection name (attributes only)
        self.cdt_class_name = self._get_class_name('_cdt')
        
        logger.info(f"Initialized RAG pipeline for project: {project_id}")
        logger.info(f"Project Name: {self.project_name}, Domain: {self.project_domain}")
        logger.info(f"User ID: {self.user_id}, Database: {self.db_name}")
        logger.info(f"Weaviate attributes collection: {self.cdt_class_name}")
    
    def _get_class_name(self, collection_suffix: str) -> str:
        """
        Generate Weaviate class name from project_id and suffix.
        
        Args:
            collection_suffix: Collection suffix '_cdt' for attributes
            
        Returns:
            Weaviate class name
        """
        collection_name = f"{self.project_id}_weviate{collection_suffix}"
        class_name = collection_name.replace('_', '').replace('-', '')
        class_name = class_name[0].upper() + class_name[1:]
        return class_name
    
    def _get_query_vector_gemini(self, query: str) -> Optional[List[float]]:
        """
        Generate embedding vector using Gemini.
        
        Args:
            query: User's search query
            
        Returns:
            Embedding vector or None if generation fails
        """
        try:
            import google.generativeai as genai
            import os
            
            # Configure Gemini
            api_key = os.getenv("GEMINI_API_KEY")
            if not api_key:
                logger.warning("GEMINI_API_KEY not found in environment variables")
                return None
            
            genai.configure(api_key=api_key)
            
            # Generate embedding using Gemini
            result = genai.embed_content(
                model="models/embedding-001",
                content=query,
                task_type="retrieval_query"
            )
            
            vector = result['embedding']
            logger.info(f"Generated query vector with {len(vector)} dimensions using Gemini")
            return vector
            
        except ImportError:
            logger.warning("google-generativeai not installed")
            return None
        except Exception as e:
            logger.warning(f"Error generating query vector with Gemini: {e}")
            return None
    
    def _get_query_vector_cohere(self, query: str) -> Optional[List[float]]:
        """
        Generate embedding vector using Cohere.
        
        Args:
            query: User's search query
            
        Returns:
            Embedding vector or None if generation fails
        """
        try:
            import cohere
            import os
            
            # Configure Cohere
            api_key = os.getenv("COHERE_API_KEY")
            if not api_key:
                logger.error("COHERE_API_KEY not found in environment variables")
                return None
            
            co = cohere.Client(api_key)
            
            # Generate embedding using Cohere (768 dimensions to match Gemini)
            response = co.embed(
                texts=[query],
                model="embed-english-light-v3.0",  # 384D model
                input_type="search_query",
                embedding_types=["float"]
            )
            
            vector = response.embeddings.float_[0]
            
            # Pad to 768 dimensions if needed (to match Gemini's 768D)
            if len(vector) < 768:
                vector = vector + [0.0] * (768 - len(vector))
                logger.info(f"Padded Cohere vector from {len(response.embeddings.float_[0])} to 768 dimensions")
            elif len(vector) > 768:
                vector = vector[:768]
                logger.info(f"Truncated Cohere vector from {len(response.embeddings.float_[0])} to 768 dimensions")
            
            logger.info(f"Generated query vector with {len(vector)} dimensions using Cohere")
            return vector
            
        except ImportError:
            logger.error("cohere not installed. Run: pip install cohere")
            return None
        except Exception as e:
            logger.error(f"Error generating query vector with Cohere: {e}")
            return None
    
    def _get_query_vector(self, query: str) -> Optional[List[float]]:
        """
        Generate embedding vector for the query.
        Tries Gemini first, falls back to Cohere if Gemini fails.
        
        Args:
            query: User's search query
            
        Returns:
            Embedding vector or None if all methods fail
        """
        # Try Gemini first
        vector = self._get_query_vector_gemini(query)
        if vector is not None:
            return vector
        
        # Fallback to Cohere
        logger.info("Falling back to Cohere for embedding generation")
        vector = self._get_query_vector_cohere(query)
        if vector is not None:
            return vector
        
        logger.error("Failed to generate query vector with both Gemini and Cohere")
        return None
    
    def _is_counting_query(self, query: str) -> bool:
        """
        Detect if query is asking for count/number/overview of attributes.
        These queries need ALL attributes, not just semantic matches.
        
        Args:
            query: User's query string
            
        Returns:
            True if it's a counting/overview query, False otherwise
        """
        counting_keywords = [
            # Direct counting
            'how many', 'number of', 'count', 'total', 'how much',
            
            # Overview requests
            'overview', 'summary', 'describe the data', 'what is the data',
            'tell me about the data', 'explain the data', 'data structure',
            'dataset structure', 'describe dataset', 'explain dataset',
            
            # Listing requests
            'all attributes', 'list all', 'show all', 'give me all',
            'what attributes', 'which attributes', 'available attributes',
            'what columns', 'which columns', 'available columns',
            'list attributes', 'show attributes', 'list columns',
            
            # General questions
            'what does this data contain', 'what is in the data',
            'what kind of data', 'what type of data'
        ]
        
        query_lower = query.lower()
        return any(keyword in query_lower for keyword in counting_keywords)
    
    def get_total_attribute_count(self) -> int:
        """
        Get the total number of UNIQUE attributes in the Weaviate collection.
        Counts distinct attribute names, not total documents.
        
        Returns:
            Total count of unique attributes
        """
        try:
            if self.weaviate_client.collections.exists(self.cdt_class_name):
                cdt_collection = self.weaviate_client.collections.get(self.cdt_class_name)
                
                # Fetch all objects and count unique attribute names
                response = cdt_collection.query.fetch_objects(
                    limit=1000,
                    return_properties=["attribute"]
                )
                
                # Extract unique attribute names
                unique_attributes = set()
                for obj in response.objects:
                    attr_name = obj.properties.get("attribute")
                    if attr_name:
                        unique_attributes.add(attr_name)
                
                total_count = len(unique_attributes)
                total_documents = len(response.objects)
                
                logger.info(f"Total UNIQUE attributes: {total_count} (from {total_documents} documents)")
                return total_count
            else:
                logger.warning(f"Collection {self.cdt_class_name} does not exist")
                return 0
                
        except Exception as e:
            logger.error(f"Error getting total attribute count: {e}")
            return 0
    
    def get_all_attribute_names(self) -> List[str]:
        """
        Retrieve all UNIQUE attribute names from the Weaviate collection.
        
        Returns:
            List of unique attribute names
        """
        try:
            if self.weaviate_client.collections.exists(self.cdt_class_name):
                cdt_collection = self.weaviate_client.collections.get(self.cdt_class_name)
                
                # Fetch all objects with just the attribute name
                response = cdt_collection.query.fetch_objects(
                    limit=1000,  # Set high limit to get all
                    return_properties=["attribute"]
                )
                
                # Get unique attribute names only
                unique_attributes = set()
                for obj in response.objects:
                    attr_name = obj.properties.get("attribute")
                    if attr_name:
                        unique_attributes.add(attr_name)
                
                attribute_names = sorted(list(unique_attributes))  # Sort for consistent output
                logger.info(f"Retrieved {len(attribute_names)} UNIQUE attribute names (from {len(response.objects)} documents)")
                return attribute_names
            else:
                logger.warning(f"Collection {self.cdt_class_name} does not exist")
                return []
                
        except Exception as e:
            logger.error(f"Error retrieving attribute names: {e}")
            return []
    
    def get_mongodb_attribute_count(self) -> int:
        """
        Get the number of attributes/columns from MongoDB collection.
        This provides a cross-reference to verify Weaviate data.
        
        Returns:
            Number of columns in MongoDB collection
        """
        try:
            # Look for the main data collection (usually project_id)
            collection_name = self.project_id
            
            if collection_name in self.db.list_collection_names():
                collection = self.db[collection_name]
                
                # Get one document to count fields
                sample_doc = collection.find_one()
                if sample_doc:
                    # Exclude MongoDB's _id field
                    field_count = len([k for k in sample_doc.keys() if k != '_id'])
                    logger.info(f"MongoDB collection '{collection_name}' has {field_count} fields")
                    return field_count
                else:
                    logger.warning(f"No documents found in MongoDB collection '{collection_name}'")
                    return 0
            else:
                logger.warning(f"MongoDB collection '{collection_name}' not found")
                return 0
                
        except Exception as e:
            logger.error(f"Error getting MongoDB attribute count: {e}")
            return 0
    
    def retrieve_all_attributes(self) -> Optional[str]:
        """
        Retrieve ALL attributes without semantic filtering.
        Used for overview/counting queries.
        
        Returns:
            Combined context text with all attributes
        """
        results = []
        
        try:
            if self.weaviate_client.collections.exists(self.cdt_class_name):
                cdt_collection = self.weaviate_client.collections.get(self.cdt_class_name)
                
                # Fetch all objects
                response = cdt_collection.query.fetch_objects(
                    limit=1000,  # Set high enough to get all attributes
                    return_properties=[
                        "combined_text", 
                        "attribute", 
                        "data_type"
                    ]
                )
                
                for obj in response.objects:
                    props = obj.properties
                    text = props.get("combined_text", "")
                    attribute = props.get("attribute", "Unknown Attribute")
                    data_type = props.get("data_type", "")
                    results.append(f"[Attribute: {attribute}] ({data_type})\n{text}")
                    
                logger.info(f"Retrieved ALL {len(response.objects)} attributes from {self.cdt_class_name}")
            else:
                logger.warning(f"Collection {self.cdt_class_name} does not exist in Weaviate")
                
        except Exception as e:
            logger.error(f"Error retrieving all attributes: {e}")
        
        if not results:
            logger.warning("No attributes retrieved from Weaviate")
            return None
        
        context_text = "\n\n".join(results)
        return context_text
    
    def retrieve_context(self, query: str, top_k: int = 6) -> Optional[str]:
        """
        Query the attribute data collection in Weaviate and return relevant context.
        
        Args:
            query: User's search query
            top_k: Number of top results to retrieve
            
        Returns:
            Combined context text or None if no results found
        """
        results = []
        
        # Generate query vector
        query_vector = self._get_query_vector(query)
        if not query_vector:
            logger.error("Failed to generate query vector. Cannot perform semantic search.")
            return None
        
        # Query column data types collection (_cdt)
        try:
            if self.weaviate_client.collections.exists(self.cdt_class_name):
                cdt_collection = self.weaviate_client.collections.get(self.cdt_class_name)
                response = cdt_collection.query.near_vector(
                    near_vector=query_vector,
                    limit=top_k,
                    return_properties=[
                        "combined_text", 
                        "attribute", 
                        "data_type"
                    ]
                )
                
                for obj in response.objects:
                    props = obj.properties
                    text = props.get("combined_text", "")
                    attribute = props.get("attribute", "Unknown Attribute")
                    data_type = props.get("data_type", "")
                    results.append(f"[Attribute: {attribute}] ({data_type})\n{text}")
                    
                logger.info(f"Retrieved {len(response.objects)} attribute results from {self.cdt_class_name}")
            else:
                logger.warning(f"Collection {self.cdt_class_name} does not exist in Weaviate")
                
        except Exception as e:
            logger.error(f"Error retrieving from {self.cdt_class_name}: {e}")
        
        # Combine results
        if not results:
            logger.warning("No attribute context retrieved from Weaviate")
            return None
        
        context_text = "\n\n".join(results)
        logger.info(f"Retrieved {len(results)} attribute context snippets from Weaviate")
        return context_text
    
    def generate_answer(self, query: str, context: Optional[str], total_attributes: int, all_attribute_names: List[str] = None) -> str:
        """
        Combine the user's query and the retrieved attribute context, then call LLM.
        Uses the call_llm helper which automatically handles fallback.
        Includes project metadata and attribute count for additional context.
        
        Args:
            query: User's question
            context: Retrieved context from Weaviate
            total_attributes: Total number of attributes in the dataset
            all_attribute_names: List of all attribute names (optional)
            
        Returns:
            LLM-generated answer
        """
        if not context:
            return "No relevant attribute information found in the dataset."
        
        try:
            # Prepare attribute names string
            attribute_names_str = ""
            if all_attribute_names:
                attribute_names_str = ", ".join(all_attribute_names)
            
            # Use call_llm with Jinja template
            response = call_llm(
                prompt_or_template="rag_data.jinja",
                use_template=True,
                use_fallback=True,
                context_variables={
                    "query": query,
                    "context": context,
                    "project_name": self.project_name,
                    "project_domain": self.project_domain,
                    "total_attributes": total_attributes,
                    "attribute_names": attribute_names_str
                }
            )
            
            # Extract text from response (handle different response types)
            if hasattr(response, 'content'):
                answer = response.content
            elif hasattr(response, 'text'):
                answer = response.text
            elif isinstance(response, str):
                answer = response
            else:
                answer = str(response)
            
            logger.info("Generated answer using call_llm helper with Jinja template")
            return answer.strip()
            
        except Exception as e:
            logger.error(f"Error calling LLM via call_llm helper: {e}")
            import traceback
            traceback.print_exc()
            return f"Error generating answer: {str(e)}"
    
    def run(self, query: str) -> str:
        """
        Complete RAG flow: retrieve + reason + respond.
        Intelligently handles counting vs. semantic search queries.
        
        Args:
            query: User's question
            
        Returns:
            Final response from the pipeline
        """
        logger.info(f"Running RAG for query: {query}")
        logger.info(f"Project context - Name: {self.project_name}, Domain: {self.project_domain}")
        
        # Get total attribute count first (always needed for context)
        total_attributes = self.get_total_attribute_count()
        
        # Also get MongoDB count for verification
        mongodb_count = self.get_mongodb_attribute_count()
        logger.info(f"📊 Attribute count - Weaviate (unique): {total_attributes}, MongoDB: {mongodb_count}")
        
        # Check if this is a counting/overview query
        is_counting_query = self._is_counting_query(query)
        
        if is_counting_query:
            logger.info("Detected counting/overview query - retrieving ALL attributes")
            
            # Retrieve all attributes for counting queries
            context = self.retrieve_all_attributes()
            
            # Get all attribute names for listing
            all_attribute_names = self.get_all_attribute_names()
            
            # Generate answer with full context
            response = self.generate_answer(query, context, total_attributes, all_attribute_names)
        else:
            logger.info("Detected specific query - using semantic search")
            
            # Use semantic search with moderate top_k
            context = self.retrieve_context(query, top_k=8)
            
            # Generate answer with semantic context
            response = self.generate_answer(query, context, total_attributes)
        
        return response
    
    def close(self):
        """Close connections."""
        if self.mongo_client:
            self.mongo_client.close()
        if self.weaviate_client:
            self.weaviate_client.close()
        logger.info("Connections closed")


def run_rag_data(project_id: str, query: str, master_db_name: str = "master") -> str:
    """
    Run RAG pipeline for a given project and query.
    
    Args:
        project_id: Project ID (e.g., "UID001PJ001")
        query: User's question
        master_db_name: Name of the master database
    
    Returns:
        Response from the RAG pipeline
    
    Example:
        response = run_rag_data("UID001PJ001", "What attributes are available?")
    """
    pipeline = None
    try:
        pipeline = RAGPipeline(
            project_id=project_id,
            master_db_name=master_db_name
        )
        result = pipeline.run(query)
        return result
        
    except Exception as e:
        logger.error(f"RAG pipeline failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return f"Error: {str(e)}"
        
    finally:
        if pipeline:
            pipeline.close()


def main():
    """
    Command-line entry point for the RAG pipeline.
    """
    if len(sys.argv) < 3:
        print("Usage: python rag_pipeline_attributes.py <project_id> '<query>'")
        print("Example: python rag_pipeline_attributes.py UID001PJ001 'What attributes are available?'")
        sys.exit(1)

    project_id = sys.argv[1]
    query = sys.argv[2]
    
    result = run_rag_data(project_id, query)
    
    print("\n💬 RAG Response:\n")
    print(result)
    print()


if __name__ == "__main__":
    main()