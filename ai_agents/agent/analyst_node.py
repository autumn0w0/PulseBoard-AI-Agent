import sys
import json
import re
from bson import json_util
from typing import Optional, Dict, List, Any
from dotenv import load_dotenv

sys.path.append("../../")
from helpers.database.connection_to_db import connect_to_mongodb
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


class AnalystNode:
    """
    The Analyst Node performs REAL analytical computation on raw data.

    Workflow:
      1. Get available attributes from cleaned_dt
      2. Analyze data statistics for better context
      3. Parse user query → query plan (LLM)
      4. Validate the plan (fields exist, ops allowed)
      5. Build MongoDB aggregation pipeline
      6. Execute on {project_id}_data
      7. Summarize result with LLM
    """

    def __init__(self, project_id: str, master_db_name: str = "master"):
        """
        Initialize Analyst Node with MongoDB connection.
        
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

        # Collection names
        data_collection_name = f"{project_id}_data"
        schema_collection_name = f"{project_id}_cleaned_dt"

        # Get collections
        self.data_collection = self.db[data_collection_name]
        self.schema_collection = self.db[schema_collection_name]

        # Load schema/attributes
        self.schema = list(self.schema_collection.find({}))
        self.fields = [item["attribute"] for item in self.schema]
        self.field_types = {item["attribute"]: item["data_type"] for item in self.schema}
        
        # Get data statistics for better context
        self.data_stats = self._get_data_statistics()

        logger.info(f"[AnalystNode] Initialized for project: {project_id}")
        logger.info(f"[AnalystNode] Project Name: {self.project_name}, Domain: {self.project_domain}")
        logger.info(f"[AnalystNode] User ID: {self.user_id}, Database: {self.db_name}")
        logger.info(f"[AnalystNode] Loaded schema with {len(self.fields)} fields: {self.fields}")
        logger.info(f"[AnalystNode] Total documents in collection: {self.data_stats.get('total_documents', 0)}")

    # -------------------------------------------------------------------------
    # DATA STATISTICS
    # -------------------------------------------------------------------------
    def _get_data_statistics(self) -> Dict[str, Any]:
        """
        Get basic statistics about the data collection.
        
        Returns:
            Dictionary with data statistics
        """
        try:
            total_docs = self.data_collection.count_documents({})
            
            # Get sample document to understand structure
            sample_doc = self.data_collection.find_one({}, projection={"_id": 0})
            
            # Categorize fields
            numeric_fields = [f for f, t in self.field_types.items() 
                            if t in ["integer", "float", "number", "numeric"]]
            categorical_fields = [f for f, t in self.field_types.items() 
                                if t in ["string", "text", "category"]]
            date_fields = [f for f, t in self.field_types.items() 
                         if "date" in t.lower() or "time" in t.lower()]
            
            stats = {
                "total_documents": total_docs,
                "total_fields": len(self.fields),
                "numeric_fields": numeric_fields,
                "categorical_fields": categorical_fields,
                "date_fields": date_fields,
                "sample_document": sample_doc
            }
            
            logger.info(f"[AnalystNode] Data stats - Docs: {total_docs}, "
                       f"Numeric fields: {len(numeric_fields)}, "
                       f"Categorical: {len(categorical_fields)}, "
                       f"Date: {len(date_fields)}")
            
            return stats
            
        except Exception as e:
            logger.error(f"[AnalystNode] Error getting data statistics: {e}")
            return {
                "total_documents": 0,
                "total_fields": len(self.fields),
                "numeric_fields": [],
                "categorical_fields": [],
                "date_fields": []
            }

    def _classify_query_type(self, query: str) -> str:
        """
        Classify the type of analytical query.
        
        Args:
            query: User's query string
            
        Returns:
            Query type: "aggregation", "counting", "filtering", "statistical"
        """
        query_lower = query.lower()
        
        # Counting queries
        if any(keyword in query_lower for keyword in [
            'how many', 'count', 'number of', 'total number'
        ]):
            return "counting"
        
        # Statistical queries
        if any(keyword in query_lower for keyword in [
            'average', 'mean', 'median', 'sum', 'total',
            'maximum', 'max', 'minimum', 'min', 'highest', 'lowest'
        ]):
            return "statistical"
        
        # Aggregation queries
        if any(keyword in query_lower for keyword in [
            'group by', 'breakdown', 'distribution', 'by category',
            'per', 'each', 'top', 'bottom'
        ]):
            return "aggregation"
        
        # Filtering queries
        if any(keyword in query_lower for keyword in [
            'show', 'list', 'find', 'get', 'where', 'filter'
        ]):
            return "filtering"
        
        return "general"

    # -------------------------------------------------------------------------
    # MAIN ENTRY POINT
    # -------------------------------------------------------------------------
    def run(self, user_query: str) -> str:
        """
        Complete analytical flow: parse → validate → execute → summarize.
        
        Args:
            user_query: User's analytical question
            
        Returns:
            Final analytical response
        """
        logger.info(f"[AnalystNode] Query received: {user_query}")
        logger.info(f"[AnalystNode] Project context - Name: {self.project_name}, Domain: {self.project_domain}")
        
        # Classify query type
        query_type = self._classify_query_type(user_query)
        logger.info(f"[AnalystNode] Classified query type: {query_type}")

        # STEP 1 — Build Query Plan from natural language
        plan = self.parse_intent(user_query, query_type)
        if plan is None:
            return "I couldn't understand your analytical query. Please rephrase it or try asking: 'What is the average [field]?', 'How many records are there?', 'Show me the top 10 [field]'"

        # STEP 2 — Validate plan (fields exist, op allowed)
        validation_error = self.validate_plan(plan)
        if validation_error:
            return validation_error

        # STEP 3 — Build Mongo pipeline
        pipeline = self.build_pipeline(plan)

        # STEP 4 — Execute pipeline
        result = self.execute_pipeline(pipeline)

        # STEP 5 — Summarize results via LLM
        return self.summarize(user_query, plan, result, query_type)

    # -------------------------------------------------------------------------
    # STEP 1 — LLM INTENT PARSER
    # -------------------------------------------------------------------------
    def parse_intent(self, query: str, query_type: str = "general") -> Optional[Dict]:
        """
        Converts natural language → structured query plan using LLM.
        
        Args:
            query: User's natural language query
            query_type: Classified query type for better context
            
        Returns:
            Query plan dictionary or None if parsing fails
        """
        try:
            # Prepare enhanced context
            context_vars = {
                "user_query": query,
                "query_type": query_type,
                "available_fields": json.dumps(self.fields),
                "field_types": json.dumps(self.field_types),
                "project_name": self.project_name,
                "project_domain": self.project_domain,
                "total_documents": self.data_stats.get("total_documents", 0),
                "numeric_fields": json.dumps(self.data_stats.get("numeric_fields", [])),
                "categorical_fields": json.dumps(self.data_stats.get("categorical_fields", [])),
                "date_fields": json.dumps(self.data_stats.get("date_fields", []))
            }
            
            # Use call_llm with Jinja template
            response = call_llm(
                prompt_or_template="analyst_plan.jinja",
                use_template=True,
                use_fallback=True,
                context_variables=context_vars
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
                llm_output = llm_output[7:]  # Remove ```json
            elif llm_output.startswith("```"):
                llm_output = llm_output[3:]  # Remove ```
            
            if llm_output.endswith("```"):
                llm_output = llm_output[:-3]  # Remove trailing ```
            
            llm_output = llm_output.strip()

            # Parse JSON response
            plan = json.loads(llm_output)
            logger.info(f"[AnalystNode] Query Plan: {plan}")
            return plan

        except json.JSONDecodeError as e:
            logger.error(f"[AnalystNode] Failed to parse LLM query plan as JSON: {e}")
            logger.error(f"[AnalystNode] LLM output was: {llm_output}")
            return None
        except Exception as e:
            logger.error(f"[AnalystNode] Error in parse_intent: {e}")
            import traceback
            traceback.print_exc()
            return None

    # -------------------------------------------------------------------------
    # STEP 2 — PLAN VALIDATION
    # -------------------------------------------------------------------------
    def validate_plan(self, plan: Dict) -> Optional[str]:
        """
        Validates:
          - field exists
          - operation supported
          - datatypes correct for numeric ops
        
        Args:
            plan: Query plan dictionary
            
        Returns:
            Error message with helpful suggestions if validation fails, None if valid
        """
        op = plan.get("operation")
        field = plan.get("field")

        supported_ops = {
            "group_by_count",
            "avg",
            "sum",
            "max",
            "min",
            "count",
            "filter_only"
        }

        if op not in supported_ops:
            return (f"❌ Operation '{op}' is not supported.\n\n"
                   f"✅ Supported operations:\n"
                   f"  • group_by_count - Group and count by a field\n"
                   f"  • count - Count total records\n"
                   f"  • avg, sum, max, min - Statistical operations on numeric fields\n"
                   f"  • filter_only - Filter data without aggregation")

        # Validate field (except for pure count)
        if field and field not in self.fields and op != "count":
            # Suggest similar fields
            similar_fields = [f for f in self.fields if field.lower() in f.lower()]
            
            error_msg = f"❌ Field '{field}' does not exist in your dataset.\n\n"
            
            if similar_fields:
                error_msg += f"💡 Did you mean one of these?\n"
                for sf in similar_fields[:5]:
                    error_msg += f"  • {sf} ({self.field_types.get(sf, 'unknown')})\n"
            else:
                error_msg += f"📋 Available fields ({len(self.fields)} total):\n"
                # Show first 10 fields
                for f in self.fields[:10]:
                    error_msg += f"  • {f} ({self.field_types.get(f, 'unknown')})\n"
                if len(self.fields) > 10:
                    error_msg += f"  ... and {len(self.fields) - 10} more"
            
            return error_msg

        # Numeric validation
        numeric_ops = {"avg", "sum", "max", "min"}
        if op in numeric_ops:
            field_type = self.field_types.get(field)
            if field_type not in ["integer", "float", "number", "numeric"]:
                numeric_fields = self.data_stats.get("numeric_fields", [])
                
                error_msg = (f"❌ Field '{field}' is type '{field_type}', not numeric. "
                           f"Cannot perform {op} operation.\n\n")
                
                if numeric_fields:
                    error_msg += f"💡 Try one of these numeric fields instead:\n"
                    for nf in numeric_fields[:5]:
                        error_msg += f"  • {nf}\n"
                else:
                    error_msg += "⚠️ No numeric fields found in this dataset."
                
                return error_msg

        return None

    # -------------------------------------------------------------------------
    # STEP 3 — BUILD MONGO PIPELINE
    # -------------------------------------------------------------------------
    def build_pipeline(self, plan: Dict) -> List[Dict]:
        """
        Build MongoDB aggregation pipeline from query plan.
        
        Args:
            plan: Query plan dictionary
            
        Returns:
            List of MongoDB pipeline stages
        """
        pipeline = []

        # Add filter
        if plan.get("filter"):
            pipeline.append({"$match": plan["filter"]})
            logger.info(f"[AnalystNode] Added filter stage: {plan['filter']}")

        op = plan.get("operation")
        field = plan.get("field")

        if op == "group_by_count":
            pipeline.extend([
                {"$group": {"_id": f"${field}", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ])
            logger.info(f"[AnalystNode] Added group_by_count for field: {field}")

        elif op == "avg":
            pipeline.append({"$group": {"_id": None, "value": {"$avg": f"${field}"}}})
            logger.info(f"[AnalystNode] Added avg aggregation for field: {field}")

        elif op == "sum":
            pipeline.append({"$group": {"_id": None, "value": {"$sum": f"${field}"}}})
            logger.info(f"[AnalystNode] Added sum aggregation for field: {field}")

        elif op == "max":
            pipeline.append({"$group": {"_id": None, "value": {"$max": f"${field}"}}})
            logger.info(f"[AnalystNode] Added max aggregation for field: {field}")

        elif op == "min":
            pipeline.append({"$group": {"_id": None, "value": {"$min": f"${field}"}}})
            logger.info(f"[AnalystNode] Added min aggregation for field: {field}")

        elif op == "count":
            pipeline.append({"$count": "total"})
            logger.info(f"[AnalystNode] Added count aggregation")

        elif op == "filter_only":
            # Just apply the filter, no aggregation
            logger.info(f"[AnalystNode] Filter-only operation, no aggregation")
            pass

        # Add limit
        if plan.get("limit"):
            pipeline.append({"$limit": plan["limit"]})
            logger.info(f"[AnalystNode] Added limit: {plan['limit']}")

        logger.info(f"[AnalystNode] Final Mongo Pipeline ({len(pipeline)} stages): {pipeline}")
        return pipeline

    # -------------------------------------------------------------------------
    # STEP 4 — EXECUTE PIPELINE
    # -------------------------------------------------------------------------
    def execute_pipeline(self, pipeline: List[Dict]) -> List[Dict]:
        """
        Execute MongoDB aggregation pipeline.
        
        Args:
            pipeline: MongoDB aggregation pipeline
            
        Returns:
            List of result documents
        """
        try:
            result = list(self.data_collection.aggregate(pipeline))
            logger.info(f"[AnalystNode] ✅ Pipeline executed successfully, returned {len(result)} results")
            
            # Log summary of results
            if result:
                if len(result) == 1 and result[0].get("_id") is None:
                    # Single aggregated value
                    logger.info(f"[AnalystNode] Result value: {result[0].get('value')}")
                elif len(result) <= 5:
                    # Small result set, log all
                    logger.debug(f"[AnalystNode] Results: {result}")
                else:
                    # Large result set, log count only
                    logger.info(f"[AnalystNode] Returned {len(result)} grouped results")
            
            return result
        except Exception as e:
            logger.error(f"[AnalystNode] ❌ Pipeline execution failed: {e}")
            import traceback
            traceback.print_exc()
            return []

    # -------------------------------------------------------------------------
    # STEP 5 — SUMMARIZE RESULT
    # -------------------------------------------------------------------------
    def summarize(self, user_query: str, plan: Dict, result: List[Dict], query_type: str = "general") -> str:
        """
        Generate natural language summary of analytical results.
        
        Args:
            user_query: Original user query
            plan: Query plan used
            result: Execution results
            query_type: Classified query type
            
        Returns:
            Natural language summary
        """
        try:
            # Prepare result summary
            result_summary = self._prepare_result_summary(result, plan)
            
            # Use call_llm with Jinja template
            response = call_llm(
                prompt_or_template="analyst_summary.jinja",
                use_template=True,
                use_fallback=True,
                context_variables={
                    "user_query": user_query,
                    "query_type": query_type,
                    "query_plan": json.dumps(plan, indent=2),
                    "result": json.dumps(result, default=json_util.default, indent=2),
                    "result_summary": result_summary,
                    "project_name": self.project_name,
                    "project_domain": self.project_domain,
                    "total_documents": self.data_stats.get("total_documents", 0)
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

            logger.info("[AnalystNode] ✅ Generated summary using call_llm helper with Jinja template")
            return answer.strip()

        except Exception as e:
            logger.error(f"[AnalystNode] ❌ Error generating summary: {e}")
            import traceback
            traceback.print_exc()
            return f"Analysis completed but error generating summary: {str(e)}\n\nRaw results: {json.dumps(result, default=json_util.default, indent=2)}"

    def _prepare_result_summary(self, result: List[Dict], plan: Dict) -> str:
        """
        Prepare a human-readable summary of results for the LLM.
        
        Args:
            result: Raw MongoDB result
            plan: Query plan used
            
        Returns:
            Human-readable result summary
        """
        if not result:
            return "No data found matching the query criteria."
        
        op = plan.get("operation")
        
        if op in ["avg", "sum", "max", "min"]:
            # Single value result
            value = result[0].get("value")
            return f"Result: {value}"
        
        elif op == "count":
            # Count result
            total = result[0].get("total", 0)
            return f"Total count: {total:,} records"
        
        elif op == "group_by_count":
            # Grouped results
            num_groups = len(result)
            total_count = sum(item.get("count", 0) for item in result)
            top_3 = result[:3]
            
            summary = f"Found {num_groups} distinct groups, {total_count:,} total records.\n"
            summary += "Top 3:\n"
            for item in top_3:
                group_name = item.get("_id", "Unknown")
                count = item.get("count", 0)
                summary += f"  • {group_name}: {count:,}\n"
            
            return summary
        
        else:
            return f"Returned {len(result)} results"

    def close(self):
        """Close MongoDB connection."""
        if self.mongo_client:
            self.mongo_client.close()
        logger.info("[AnalystNode] MongoDB connection closed")


def run_analyst(project_id: str, query: str, master_db_name: str = "master") -> str:
    """
    Run Analyst Node for a given project and query.
    
    Args:
        project_id: Project ID (e.g., "UID001PJ001")
        query: User's analytical query
        master_db_name: Name of the master database
    
    Returns:
        Response from the Analyst Node
    
    Example:
        response = run_analyst("UID001PJ001", "What is the average age?")
    """
    analyst = None
    try:
        analyst = AnalystNode(
            project_id=project_id,
            master_db_name=master_db_name
        )
        result = analyst.run(query)
        return result
        
    except Exception as e:
        logger.error(f"Analyst Node failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return f"Error: {str(e)}"
        
    finally:
        if analyst:
            analyst.close()


def main():
    """
    Command-line entry point for the Analyst Node.
    """
    if len(sys.argv) < 3:
        print("Usage: python analyst_node.py <project_id> '<query>'")
        print("Example: python analyst_node.py UID001PJ001 'What is the average salary?'")
        sys.exit(1)

    project_id = sys.argv[1]
    query = sys.argv[2]
    
    result = run_analyst(project_id, query)
    
    print("\n📊 Analyst Response:\n")
    print(result)
    print()


if __name__ == "__main__":
    main()