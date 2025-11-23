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
    Retrieval-Augmented Generation pipeline for querying project chart data.
    Retrieves context from Weaviate charts collection and generates answers using LLM.
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
        
        # Generate Weaviate collection name (charts only)
        self.cd_class_name = self._get_class_name('_cd')
        
        logger.info(f"Initialized RAG pipeline for project: {project_id}")
        logger.info(f"Project Name: {self.project_name}, Domain: {self.project_domain}")
        logger.info(f"User ID: {self.user_id}, Database: {self.db_name}")
        logger.info(f"Weaviate charts collection: {self.cd_class_name}")
    
    def _get_class_name(self, collection_suffix: str) -> str:
        """
        Generate Weaviate class name from project_id and suffix.
        
        Args:
            collection_suffix: Collection suffix '_cd' for charts
            
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
        Detect if query is asking for count/number/overview of charts.
        These queries need ALL charts, not just semantic matches.
        
        Args:
            query: User's query string
            
        Returns:
            True if it's a counting/overview query, False otherwise
        """
        counting_keywords = [
            # Direct counting
            'how many', 'number of', 'count', 'total', 'how much',
            
            # Overview requests
            'overview', 'summary', 'describe the charts', 'what charts',
            'tell me about the charts', 'explain the charts', 'chart overview',
            'show me all charts', 'available charts', 'what visualizations',
            
            # Listing requests
            'all charts', 'list all', 'show all', 'give me all',
            'what charts are', 'which charts', 'available visualizations',
            'list charts', 'show charts', 'display all',
            
            # General questions
            'what is visualized', 'what do the charts show',
            'what kind of charts', 'what visualizations are available'
        ]
        
        query_lower = query.lower()
        return any(keyword in query_lower for keyword in counting_keywords)
    
    def get_direct_display_charts_from_mongodb(self) -> List[str]:
        """
        Get list of chart titles that have display_mode: "direct" from MongoDB.
        These are the charts actually shown on the dashboard.
        
        Returns:
            List of chart titles with display_mode="direct"
        """
        try:
            # Look for charts collection in MongoDB
            charts_collection_name = f"{self.project_id}_charts"
            
            if charts_collection_name in self.db.list_collection_names():
                charts_collection = self.db[charts_collection_name]
                
                # Query for charts with display_mode="direct"
                direct_charts = charts_collection.find({"display_mode": "direct"})
                
                chart_titles = []
                for chart in direct_charts:
                    title = chart.get("chart_title") or chart.get("title") or chart.get("name")
                    if title:
                        chart_titles.append(title)
                
                logger.info(f"Found {len(chart_titles)} charts with display_mode='direct' in MongoDB")
                return chart_titles
            else:
                logger.warning(f"MongoDB collection '{charts_collection_name}' not found")
                return []
                
        except Exception as e:
            logger.error(f"Error getting direct display charts from MongoDB: {e}")
            return []
    
    def get_total_chart_count(self, filter_direct_only: bool = True) -> int:
        """
        Get the total number of UNIQUE charts in the Weaviate collection.
        Optionally filters to only charts with display_mode="direct" from MongoDB.
        
        Args:
            filter_direct_only: If True, only count charts shown on dashboard (display_mode="direct")
        
        Returns:
            Total count of unique charts
        """
        try:
            if self.weaviate_client.collections.exists(self.cd_class_name):
                cd_collection = self.weaviate_client.collections.get(self.cd_class_name)
                
                # Fetch all objects and count unique chart titles
                response = cd_collection.query.fetch_objects(
                    limit=1000,
                    return_properties=["chart_title"]
                )
                
                # Extract unique chart titles
                unique_charts = set()
                for obj in response.objects:
                    chart_title = obj.properties.get("chart_title")
                    if chart_title:
                        unique_charts.add(chart_title)
                
                # Filter by display_mode="direct" if requested
                if filter_direct_only:
                    direct_chart_titles = set(self.get_direct_display_charts_from_mongodb())
                    if direct_chart_titles:
                        # Only count charts that are in the "direct" display list
                        unique_charts = unique_charts.intersection(direct_chart_titles)
                        logger.info(f"Filtered to {len(unique_charts)} charts with display_mode='direct'")
                
                total_count = len(unique_charts)
                total_documents = len(response.objects)
                
                logger.info(f"Total UNIQUE charts: {total_count} (from {total_documents} documents)")
                return total_count
            else:
                logger.warning(f"Collection {self.cd_class_name} does not exist")
                return 0
                
        except Exception as e:
            logger.error(f"Error getting total chart count: {e}")
            return 0
    
    def get_all_chart_titles(self, filter_direct_only: bool = True) -> List[str]:
        """
        Retrieve all UNIQUE chart titles from the Weaviate collection.
        Optionally filters to only charts with display_mode="direct" from MongoDB.
        
        Args:
            filter_direct_only: If True, only return charts shown on dashboard (display_mode="direct")
        
        Returns:
            List of unique chart titles with types
        """
        try:
            if self.weaviate_client.collections.exists(self.cd_class_name):
                cd_collection = self.weaviate_client.collections.get(self.cd_class_name)
                
                # Fetch all objects with chart title and type
                response = cd_collection.query.fetch_objects(
                    limit=1000,
                    return_properties=["chart_title", "chart_type"]
                )
                
                # Get unique chart titles with types
                unique_charts = {}
                for obj in response.objects:
                    chart_title = obj.properties.get("chart_title")
                    chart_type = obj.properties.get("chart_type", "unknown")
                    if chart_title and chart_title not in unique_charts:
                        unique_charts[chart_title] = chart_type
                
                # Filter by display_mode="direct" if requested
                if filter_direct_only:
                    direct_chart_titles = set(self.get_direct_display_charts_from_mongodb())
                    if direct_chart_titles:
                        # Only include charts that are in the "direct" display list
                        unique_charts = {k: v for k, v in unique_charts.items() if k in direct_chart_titles}
                        logger.info(f"Filtered to {len(unique_charts)} charts with display_mode='direct'")
                
                chart_list = [f"{title} ({ctype})" for title, ctype in sorted(unique_charts.items())]
                logger.info(f"Retrieved {len(chart_list)} UNIQUE chart titles")
                return chart_list
            else:
                logger.warning(f"Collection {self.cd_class_name} does not exist")
                return []
                
        except Exception as e:
            logger.error(f"Error retrieving chart titles: {e}")
            return []
    
    def retrieve_all_charts(self, filter_direct_only: bool = True) -> Optional[str]:
        """
        Retrieve ALL charts without semantic filtering.
        Optionally filters to only charts with display_mode="direct" from MongoDB.
        Used for overview/counting queries.
        
        Args:
            filter_direct_only: If True, only retrieve charts shown on dashboard (display_mode="direct")
        
        Returns:
            Combined context text with all charts
        """
        results = []
        
        # Get list of direct display charts if filtering
        direct_chart_titles = None
        if filter_direct_only:
            direct_chart_titles = set(self.get_direct_display_charts_from_mongodb())
            if not direct_chart_titles:
                logger.warning("No direct display charts found in MongoDB, retrieving all charts")
                direct_chart_titles = None
        
        try:
            if self.weaviate_client.collections.exists(self.cd_class_name):
                cd_collection = self.weaviate_client.collections.get(self.cd_class_name)
                
                # Fetch all objects
                response = cd_collection.query.fetch_objects(
                    limit=1000,
                    return_properties=[
                        "combined_text", 
                        "chart_title", 
                        "chart_type", 
                        "description"
                    ]
                )
                
                for obj in response.objects:
                    props = obj.properties
                    title = props.get("chart_title", "Untitled Chart")
                    
                    # Filter by direct display mode if requested
                    if direct_chart_titles and title not in direct_chart_titles:
                        continue
                    
                    text = props.get("combined_text", "")
                    chart_type = props.get("chart_type", "")
                    results.append(f"[Chart: {title}] ({chart_type})\n{text}")
                
                if filter_direct_only and direct_chart_titles:
                    logger.info(f"Retrieved {len(results)} charts with display_mode='direct' from {self.cd_class_name}")
                else:
                    logger.info(f"Retrieved ALL {len(results)} charts from {self.cd_class_name}")
            else:
                logger.warning(f"Collection {self.cd_class_name} does not exist in Weaviate")
                
        except Exception as e:
            logger.error(f"Error retrieving all charts: {e}")
        
        if not results:
            logger.warning("No charts retrieved from Weaviate")
            return None
        
        context_text = "\n\n".join(results)
        return context_text
    
    def retrieve_context(self, query: str, top_k: int = 6, filter_direct_only: bool = True) -> Optional[str]:
        """
        Query the chart data collection in Weaviate and return relevant context.
        Optionally filters to only charts with display_mode="direct" from MongoDB.
        
        Args:
            query: User's search query
            top_k: Number of top results to retrieve
            filter_direct_only: If True, only retrieve charts shown on dashboard (display_mode="direct")
            
        Returns:
            Combined context text or None if no results found
        """
        results = []
        
        # Get list of direct display charts if filtering
        direct_chart_titles = None
        if filter_direct_only:
            direct_chart_titles = set(self.get_direct_display_charts_from_mongodb())
            if not direct_chart_titles:
                logger.warning("No direct display charts found in MongoDB, retrieving all charts")
                direct_chart_titles = None
        
        # Generate query vector
        query_vector = self._get_query_vector(query)
        if not query_vector:
            logger.error("Failed to generate query vector. Cannot perform semantic search.")
            return None
        
        # Query chart data collection (_cd)
        try:
            if self.weaviate_client.collections.exists(self.cd_class_name):
                cd_collection = self.weaviate_client.collections.get(self.cd_class_name)
                
                # Fetch more results to account for filtering
                fetch_limit = top_k * 3 if direct_chart_titles else top_k
                
                response = cd_collection.query.near_vector(
                    near_vector=query_vector,
                    limit=fetch_limit,
                    return_properties=[
                        "combined_text", 
                        "chart_title", 
                        "chart_type", 
                        "description"
                    ]
                )
                
                for obj in response.objects:
                    if len(results) >= top_k:
                        break
                        
                    props = obj.properties
                    title = props.get("chart_title", "Untitled Chart")
                    
                    # Filter by direct display mode if requested
                    if direct_chart_titles and title not in direct_chart_titles:
                        continue
                    
                    text = props.get("combined_text", "")
                    chart_type = props.get("chart_type", "")
                    results.append(f"[Chart: {title}] ({chart_type})\n{text}")
                
                if filter_direct_only and direct_chart_titles:
                    logger.info(f"Retrieved {len(results)} charts with display_mode='direct' from {self.cd_class_name}")
                else:
                    logger.info(f"Retrieved {len(results)} chart results from {self.cd_class_name}")
            else:
                logger.warning(f"Collection {self.cd_class_name} does not exist in Weaviate")
                
        except Exception as e:
            logger.error(f"Error retrieving from {self.cd_class_name}: {e}")
        
        # Combine results
        if not results:
            logger.warning("No chart context retrieved from Weaviate")
            return None
        
        context_text = "\n\n".join(results)
        logger.info(f"Retrieved {len(results)} chart context snippets from Weaviate")
        return context_text
    
    def generate_answer(self, query: str, context: Optional[str], total_charts: int, all_chart_titles: List[str] = None) -> str:
        """
        Combine the user's query and the retrieved chart context, then call LLM.
        Uses the call_llm helper which automatically handles fallback.
        Includes project metadata and chart count for additional context.
        
        Args:
            query: User's question
            context: Retrieved context from Weaviate
            total_charts: Total number of charts in the dataset
            all_chart_titles: List of all chart titles (optional)
            
        Returns:
            LLM-generated answer
        """
        if not context:
            return "No relevant chart information found in the dataset."
        
        try:
            # Prepare chart titles string
            chart_titles_str = ""
            if all_chart_titles:
                chart_titles_str = ", ".join(all_chart_titles)
            
            # Use call_llm with Jinja template
            response = call_llm(
                prompt_or_template="rag_charts.jinja",
                use_template=True,
                use_fallback=True,
                context_variables={
                    "query": query,
                    "context": context,
                    "project_name": self.project_name,
                    "project_domain": self.project_domain,
                    "total_charts": total_charts,
                    "chart_titles": chart_titles_str
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
        
        # Get total chart count first (always needed for context)
        total_charts = self.get_total_chart_count()
        logger.info(f"📊 Total charts in collection: {total_charts}")
        
        # Check if this is a counting/overview query
        is_counting_query = self._is_counting_query(query)
        
        if is_counting_query:
            logger.info("Detected counting/overview query - retrieving ALL charts")
            
            # Retrieve all charts for counting queries
            context = self.retrieve_all_charts()
            
            # Get all chart titles for listing
            all_chart_titles = self.get_all_chart_titles()
            
            # Generate answer with full context
            response = self.generate_answer(query, context, total_charts, all_chart_titles)
        else:
            logger.info("Detected specific query - using semantic search")
            
            # Use semantic search with moderate top_k
            context = self.retrieve_context(query, top_k=8)
            
            # Generate answer with semantic context
            response = self.generate_answer(query, context, total_charts)
        
        return response
    
    def close(self):
        """Close connections."""
        if self.mongo_client:
            self.mongo_client.close()
        if self.weaviate_client:
            self.weaviate_client.close()
        logger.info("Connections closed")


def run_rag_charts(project_id: str, query: str, master_db_name: str = "master") -> str:
    """
    Run RAG pipeline for a given project and query.
    
    Args:
        project_id: Project ID (e.g., "UID001PJ001")
        query: User's question
        master_db_name: Name of the master database
    
    Returns:
        Response from the RAG pipeline
    
    Example:
        response = run_rag_charts("UID001PJ001", "What are the sales trends?")
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
        print("Usage: python rag_charts_node.py <project_id> '<query>'")
        print("Example: python rag_charts_node.py UID001PJ001 'What are the sales trends?'")
        sys.exit(1)

    project_id = sys.argv[1]
    query = sys.argv[2]
    
    result = run_rag_charts(project_id, query)
    
    print("\n💬 RAG Response:\n")
    print(result)
    print()


if __name__ == "__main__":
    main()