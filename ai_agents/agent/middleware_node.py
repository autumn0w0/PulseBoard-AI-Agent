import sys
import json
import re
from typing import Optional, Dict
from dotenv import load_dotenv

sys.path.append("../../")
from helpers.database.connection_to_db import connect_to_mongodb
from helpers.llm.call_llm import call_llm
from helpers.logger import get_logger

# Import the run functions from each pipeline
from ai_agents.agent.analyst_node import run_analyst
from ai_agents.agent.rag_charts_node import run_rag_charts
from ai_agents.agent.rag_data_node import run_rag_data



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


class MiddlewareNode:
    """
    Middleware Node that classifies user queries and routes them to appropriate pipelines:
    - Analyst Node: For data analysis queries (aggregations, statistics, computations)
    - RAG Charts Node: For questions about charts, visualizations, and insights
    - RAG Data Node: For questions about dataset attributes, schema, and metadata
    """
    
    def __init__(self, project_id: str, master_db_name: str = "master"):
        """
        Initialize Middleware Node.
        
        Args:
            project_id: Project ID (e.g., "UID001PJ001")
            master_db_name: Name of the master database
        """
        self.project_id = project_id
        self.user_id, _ = parse_project_id(project_id)
        self.master_db_name = master_db_name
        
        # Connect to MongoDB to get project metadata
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
        
        # Extract project metadata
        self.project_name = self.config["project_info"].get("name_of_project", "Unknown Project")
        self.project_domain = self.config["project_info"].get("domain", "Unknown Domain")
        
        logger.info(f"[MiddlewareNode] Initialized for project: {project_id}")
        logger.info(f"[MiddlewareNode] Project Name: {self.project_name}, Domain: {self.project_domain}")
        logger.info(f"[MiddlewareNode] User ID: {self.user_id}")
    
    def classify_intent(self, user_query: str) -> str:
        """
        Classify user query intent using LLM.
        
        Possible intents:
        - "data_analysis": Queries requiring computation (avg, sum, count, group by, etc.)
        - "chart_insight": Queries about charts, visualizations, trends, patterns
        - "data_schema": Queries about dataset structure, attributes, columns, metadata
        - "general": System-related questions (greetings, capabilities, how to use)
        - "irrelevant": Questions unrelated to project data (general knowledge, definitions, etc.)
        
        Args:
            user_query: User's question
            
        Returns:
            Intent classification string
        """
        try:
            # Use call_llm with Jinja template for intent classification
            response = call_llm(
                prompt_or_template="classifier_prompt.jinja",
                use_template=True,
                use_fallback=True,
                context_variables={
                    "user_query": user_query,
                    "project_name": self.project_name,
                    "project_domain": self.project_domain
                }
            )
            
            # Extract text from response
            if hasattr(response, 'content'):
                llm_output = response.content
            elif hasattr(response, 'text'):
                llm_output = response.text
            elif isinstance(response, str):
                llm_output = response
            else:
                llm_output = str(response)
            
            # Clean up markdown code blocks if present
            llm_output = llm_output.strip()
            if llm_output.startswith("```json"):
                llm_output = llm_output[7:]
            elif llm_output.startswith("```"):
                llm_output = llm_output[3:]
            
            if llm_output.endswith("```"):
                llm_output = llm_output[:-3]
            
            llm_output = llm_output.strip()
            
            # Parse JSON response
            result = json.loads(llm_output)
            intent = result.get("intent", "general")
            
            logger.info(f"[MiddlewareNode] Classified intent: {intent}")
            logger.info(f"[MiddlewareNode] Reasoning: {result.get('reasoning', 'N/A')}")
            
            return intent
            
        except json.JSONDecodeError as e:
            logger.error(f"[MiddlewareNode] Failed to parse intent classification: {e}")
            logger.error(f"[MiddlewareNode] LLM output was: {llm_output}")
            return "general"
        except Exception as e:
            logger.error(f"[MiddlewareNode] Error in classify_intent: {e}")
            import traceback
            traceback.print_exc()
            return "general"
    
    def route_query(self, user_query: str, intent: str) -> str:
        """
        Route query to appropriate pipeline based on intent.
        
        Args:
            user_query: User's question
            intent: Classified intent
            
        Returns:
            Response from the appropriate pipeline
        """
        logger.info(f"[MiddlewareNode] Routing to: {intent}")
        
        try:
            if intent == "data_analysis":
                # Route to Analyst Node for data computation
                logger.info("[MiddlewareNode] → Analyst Node")
                return run_analyst(self.project_id, user_query, self.master_db_name)
            
            elif intent == "chart_insight":
                # Route to RAG Charts pipeline
                logger.info("[MiddlewareNode] → RAG Charts Node")
                return run_rag_charts(self.project_id, user_query, self.master_db_name)
            
            elif intent == "data_schema":
                # Route to RAG Data pipeline
                logger.info("[MiddlewareNode] → RAG Data Node")
                return run_rag_data(self.project_id, user_query, self.master_db_name)
            
            else:
                # General fallback - use general LLM response
                logger.info("[MiddlewareNode] → General LLM")
                return self.general_llm(user_query)
                
        except Exception as e:
            logger.error(f"[MiddlewareNode] Error routing query: {e}")
            import traceback
            traceback.print_exc()
            return f"Error processing query: {str(e)}"
    
    def general_llm(self, user_query: str) -> str:
        """
        Handle general queries that don't fit specific categories.
        
        Args:
            user_query: User's question
            
        Returns:
            General LLM response
        """
        try:
            response = call_llm(
                prompt_or_template="general_query.jinja",
                use_template=True,
                use_fallback=True,
                context_variables={
                    "user_query": user_query,
                    "project_name": self.project_name,
                    "project_domain": self.project_domain
                }
            )
            
            # Extract text from response
            if hasattr(response, 'content'):
                answer = response.content
            elif hasattr(response, 'text'):
                answer = response.text
            elif isinstance(response, str):
                answer = response
            else:
                answer = str(response)
            
            logger.info("[MiddlewareNode] Generated general response")
            return answer.strip()
            
        except Exception as e:
            logger.error(f"[MiddlewareNode] Error in general_llm: {e}")
            return f"I'm having trouble processing your query: {str(e)}"
    
    def run(self, user_query: str) -> str:
        """
        Main entry point: classify intent and route to appropriate pipeline.
        
        Args:
            user_query: User's question
            
        Returns:
            Final response from the appropriate pipeline
        """
        logger.info(f"[MiddlewareNode] Query received: {user_query}")
        logger.info(f"[MiddlewareNode] Project context - Name: {self.project_name}, Domain: {self.project_domain}")
        
        # STEP 1: Classify intent
        intent = self.classify_intent(user_query)
        
        # STEP 2: Route to appropriate pipeline
        response = self.route_query(user_query, intent)
        
        return response
    
    def close(self):
        """Close MongoDB connection."""
        if self.mongo_client:
            self.mongo_client.close()
        logger.info("[MiddlewareNode] MongoDB connection closed")


def run_middleware(project_id: str, query: str, master_db_name: str = "master") -> str:
    """
    Run Middleware Node for a given project and query.
    
    Args:
        project_id: Project ID (e.g., "UID001PJ001")
        query: User's question
        master_db_name: Name of the master database
    
    Returns:
        Response from the appropriate pipeline
    
    Example:
        response = run_middleware("UID001PJ001", "What is the average salary?")
    """
    middleware = None
    try:
        middleware = MiddlewareNode(
            project_id=project_id,
            master_db_name=master_db_name
        )
        result = middleware.run(query)
        return result
        
    except Exception as e:
        logger.error(f"Middleware Node failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return f"Error: {str(e)}"
        
    finally:
        if middleware:
            middleware.close()


def main():
    """
    Command-line entry point for the Middleware Node.
    """
    if len(sys.argv) < 3:
        print("Usage: python middleware_node.py <project_id> '<query>'")
        print("Example: python middleware_node.py UID001PJ001 'What is the average salary?'")
        sys.exit(1)

    project_id = sys.argv[1]
    query = sys.argv[2]
    
    result = run_middleware(project_id, query)
    
    print("\n🤖 Middleware Response:\n")
    print(result)
    print()


if __name__ == "__main__":
    main()