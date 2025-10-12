import sys
import os
from typing import List, Dict, Any, Optional
import json
from dotenv import load_dotenv
from pymongo import MongoClient

# Add parent directories to path for imports
sys.path.append("../..")
from helpers.logger import get_logger
from helpers.database.connection_to_db import connect_to_mongodb
from helpers.llm.call_llm import call_llm

logger = get_logger(__name__)
load_dotenv()


def extract_user_id_from_project_id(project_id: str) -> str:
    """
    Extract user_id from project_id.
    Example: UID001PJ001 -> UID001
    
    Args:
        project_id: The project ID (e.g., "UID001PJ001")
    
    Returns:
        The user ID (database name)
    """
    pj_index = project_id.find("PJ")
    if pj_index == -1:
        raise ValueError(f"Invalid project_id format: {project_id}")
    
    user_id = project_id[:pj_index]
    return user_id


def get_project_collections(project_id: str) -> tuple:
    """
    Get data_types and cleaned_dt collections for a given project.
    
    Args:
        project_id: The project ID (e.g., "UID001PJ001")
    
    Returns:
        Tuple of (mongo_client, data_types_collection, cleaned_dt_collection)
    """
    mongo_client = connect_to_mongodb()
    if not mongo_client:
        raise Exception("Failed to connect to MongoDB")
    
    db_name = extract_user_id_from_project_id(project_id)
    
    data_types_collection = f"{project_id}_data_type"
    cleaned_dt_collection = f"{project_id}_cleaned_dt"
    
    logger.info(f"Accessing database: {db_name}")
    logger.info(f"  - Data types collection: {data_types_collection}")
    logger.info(f"  - Cleaned data collection: {cleaned_dt_collection}")
    
    data_types_coll = mongo_client[db_name][data_types_collection]
    cleaned_dt_coll = mongo_client[db_name][cleaned_dt_collection]
    
    return mongo_client, data_types_coll, cleaned_dt_coll


def analyze_data_type_with_llm(
    attribute: str,
    declared_data_type: str,
    samples: List[str]
) -> Dict[str, Any]:
    """
    Use LLM to analyze if the declared data_type matches the sample data.
    
    Args:
        attribute: The attribute name
        declared_data_type: The declared data type
        samples: List of sample values
    
    Returns:
        Dictionary with analysis results
    """
    prompt = f"""You are a data type validation expert. Analyze the following data and determine the correct data type.

Attribute Name: {attribute}
Current Data Type: {declared_data_type}
Sample Values: {samples}

Determine the ACTUAL/CORRECT data type for these samples.

IMPORTANT RULES:
- If the attribute name suggests a temporal meaning (e.g., "year", "month", "date") AND the values are numeric years (like 2014, 2015, 2020), classify it as "datetime" NOT "integer"
- Be context-aware: consider both the attribute name and the values

Consider these data types:
- string: General text data
- integer: Whole numbers (but NOT years/dates)
- float: Decimal numbers
- boolean: True/False values
- datetime: Date and/or time values, includes year columns
- date: Date only (no time)
- time: Time only (no date)
- location: Geographic data (addresses, cities, countries, coordinates)
- email: Email addresses
- phone: Phone numbers
- url: Web URLs
- currency: Monetary values
- percentage: Percentage values
- array: Lists of values

Respond ONLY with a JSON object in this exact format (no markdown, no extra text):
{{
    "data_type": "the correct data type"
}}"""

    system_prompt = """You are a data type classification expert. 
Analyze data samples and return ONLY a JSON object with the correct data_type.
Be especially attentive to:
- Year columns: If attribute name contains "year", "yr", or similar AND values are years like 2014, 2015 -> use "datetime"
- Geographic data (addresses, cities, countries, coordinates) -> use "location"
- Temporal data: dates without time -> "date", dates with time -> "datetime"
- Numeric vs string distinctions
- Special formats (emails, phones, URLs)

Return ONLY valid JSON, no markdown formatting, no explanations."""

    try:
        response = call_llm(
            prompt_or_template=prompt,
            system_prompt=system_prompt,
            use_template=False,
            temperature=0.1,
        )
        
        # Extract response content
        if hasattr(response, 'content'):
            response_text = response.content
        elif hasattr(response, 'text'):
            response_text = response.text
        elif hasattr(response, 'candidates') and len(response.candidates) > 0:
            candidate = response.candidates[0]
            if hasattr(candidate, 'content') and hasattr(candidate.content, 'parts'):
                parts = candidate.content.parts
                if len(parts) > 0:
                    response_text = parts[0].text
                else:
                    response_text = str(response)
            else:
                response_text = str(response)
        else:
            response_text = str(response)
        
        response_text = response_text.strip()
        
        # Remove markdown code blocks
        if "```json" in response_text:
            response_text = response_text.split("```json")[1].split("```")[0].strip()
        elif "```" in response_text:
            response_text = response_text.split("```")[1].split("```")[0].strip()
        
        result = json.loads(response_text)
        
        if "data_type" not in result:
            raise ValueError("Response missing 'data_type' field")
        
        return result
        
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse LLM response as JSON: {e}")
        logger.error(f"Response was: {response_text[:500]}")
        return {"data_type": declared_data_type}
    except Exception as e:
        logger.error(f"Error calling LLM: {e}")
        return {"data_type": declared_data_type}


def get_actual_data_samples(
    cleaned_dt_collection,
    attribute: str,
    sample_size: int = 10
) -> Optional[List[Any]]:
    """
    Get actual sample values from the cleaned_dt collection.
    
    Args:
        cleaned_dt_collection: PyMongo collection object
        attribute: The attribute/field name to sample
        sample_size: Number of samples to retrieve
    
    Returns:
        List of sample values from the actual data
    """
    try:
        pipeline = [
            {"$match": {attribute: {"$exists": True, "$ne": None}}},
            {"$sample": {"size": sample_size}},
            {"$project": {attribute: 1, "_id": 0}}
        ]
        
        samples = list(cleaned_dt_collection.aggregate(pipeline))
        actual_samples = [doc.get(attribute) for doc in samples if attribute in doc]
        
        return actual_samples if actual_samples else None
        
    except Exception as e:
        logger.error(f"Error fetching actual data samples for '{attribute}': {e}")
        return None


def save_corrected_data_types(
    project_id: str,
    corrected_data: List[Dict[str, Any]]
) -> bool:
    """
    Save corrected data types to {project_id}_cleaned_dt collection.
    
    Args:
        project_id: The project ID
        corrected_data: List of corrected data type documents
    
    Returns:
        True if successful, False otherwise
    """
    try:
        mongo_client = connect_to_mongodb()
        if not mongo_client:
            raise Exception("Failed to connect to MongoDB")
        
        db_name = extract_user_id_from_project_id(project_id)
        cleaned_dt_collection_name = f"{project_id}_cleaned_dt"
        cleaned_dt_coll = mongo_client[db_name][cleaned_dt_collection_name]
        
        # Clear existing data
        cleaned_dt_coll.delete_many({})
        logger.info(f"Cleared existing data from {cleaned_dt_collection_name}")
        
        # Insert corrected data
        if corrected_data:
            cleaned_dt_coll.insert_many(corrected_data)
            logger.info(f"Inserted {len(corrected_data)} corrected records into {cleaned_dt_collection_name}")
        
        mongo_client.close()
        return True
        
    except Exception as e:
        logger.error(f"Error saving corrected data types: {e}")
        return False


def analyze_and_correct_data_types(
    project_id: str,
    use_actual_data: bool = True
) -> tuple:
    """
    Analyze all data type documents and prepare corrected versions.
    
    Args:
        project_id: The project ID (e.g., "UID001PJ001")
        use_actual_data: If True, fetch samples from actual data
    
    Returns:
        Tuple of (mongo_client, results_list, corrected_documents)
    """
    logger.info(f"Starting data type correction for project: {project_id}")
    
    mongo_client, data_types_coll, source_cleaned_dt_coll = get_project_collections(project_id)
    
    # Fetch all documents from data_types collection
    documents = list(data_types_coll.find({}))
    logger.info(f"Found {len(documents)} data type documents to analyze")
    
    if len(documents) == 0:
        logger.warning(f"⚠️  No documents found in {project_id}_data_type collection!")
        return mongo_client, [], []
    
    results = []
    corrected_documents = []
    
    for idx, doc in enumerate(documents, 1):
        attribute = doc.get("attribute")
        declared_data_type = doc.get("data_type")
        stored_samples = doc.get("sample", [])
        
        if not attribute or not declared_data_type:
            logger.warning(f"Skipping incomplete document: {doc.get('_id')}")
            continue
        
        # Determine which samples to use
        if use_actual_data:
            logger.info(f"[{idx}/{len(documents)}] Fetching actual data for: {attribute}")
            samples = get_actual_data_samples(source_cleaned_dt_coll, attribute, sample_size=10)
            if not samples:
                logger.warning(f"No actual data found for '{attribute}', using stored samples")
                samples = stored_samples
            else:
                logger.info(f"  - Found {len(samples)} actual samples")
        else:
            samples = stored_samples
        
        if not samples:
            logger.warning(f"No samples available for '{attribute}', skipping")
            continue
        
        logger.info(f"[{idx}/{len(documents)}] Analyzing: {attribute} (declared as {declared_data_type})")
        
        # Analyze with LLM
        analysis = analyze_data_type_with_llm(attribute, declared_data_type, samples)
        
        corrected_data_type = analysis.get("data_type", declared_data_type)
        is_different = corrected_data_type != declared_data_type
        
        # Create corrected document (exclude _id from source)
        corrected_doc = {
            "attribute": attribute,
            "data_type": corrected_data_type,
            "sample": samples[:5],
            "original_data_type": declared_data_type,
            "was_corrected": is_different
        }
        
        # Add any other fields from original document except _id
        for key, value in doc.items():
            if key not in ["_id", "attribute", "data_type", "sample"]:
                corrected_doc[key] = value
        
        corrected_documents.append(corrected_doc)
        
        result = {
            "attribute": attribute,
            "declared_data_type": declared_data_type,
            "corrected_data_type": corrected_data_type,
            "is_different": is_different,
            "sample_values": samples[:5],
        }
        
        results.append(result)
        
        if is_different:
            logger.warning(
                f"⚠️  TYPE CORRECTED: {attribute}\n"
                f"   Original: {declared_data_type}\n"
                f"   Corrected: {corrected_data_type}"
            )
        else:
            logger.info(f"✅ {attribute}: Data type is correct")
    
    return mongo_client, results, corrected_documents


def generate_report(results: List[Dict[str, Any]], project_id: str):
    """
    Generate a summary report of the analysis.
    
    Args:
        results: List of analysis results
        project_id: The project ID
    """
    print("\n" + "="*80)
    print(f"DATA TYPE CORRECTION REPORT - Project: {project_id}")
    print("="*80 + "\n")
    
    total = len(results)
    
    if total == 0:
        print("⚠️  No data to analyze. Collection is empty.")
        print("="*80 + "\n")
        return
    
    corrections = [r for r in results if r["is_different"]]
    correct = [r for r in results if not r["is_different"]]
    
    print(f"Total Attributes Analyzed: {total}")
    print(f"✅ Correct Data Types: {len(correct)}")
    print(f"✏️  Corrections Made: {len(corrections)}")
    print("\n" + "-"*80 + "\n")
    
    if corrections:
        print("CORRECTIONS APPLIED:\n")
        for result in corrections:
            print(f"Attribute: {result['attribute']}")
            print(f"  Original Type: {result['declared_data_type']}")
            print(f"  Corrected Type: {result['corrected_data_type']}")
            print(f"  Sample Values: {result['sample_values'][:3]}")
            print()
    
    print(f"✅ Corrected data saved to {project_id}_cleaned_dt collection")
    print("="*80 + "\n")


def run_cdt(project_id: str, use_actual_data: bool = True) -> bool:
    """
    Main function to run complete data type correction process.
    
    This function:
    1. Analyzes all data types in {project_id}_data_type collection
    2. Uses LLM to detect and correct any data type mismatches
    3. Saves corrected data to {project_id}_cleaned_dt collection
    
    Args:
        project_id: The project ID (e.g., "UID001PJ001")
        use_actual_data: If True, uses actual data samples for analysis
    
    Returns:
        True if successful, False otherwise
    
    Example:
        >>> run_cdt("UID001PJ001")
        True
    """
    # Validate project_id format
    if "PJ" not in project_id:
        logger.error(f"Invalid project_id format: {project_id}")
        print(f"Error: Project ID must contain 'PJ' (e.g., UID001PJ001)")
        return False
    
    try:
        logger.info(f"Starting CDT (Corrected Data Types) process for {project_id}")
        
        # Analyze and get corrected data
        mongo_client, results, corrected_documents = analyze_and_correct_data_types(
            project_id,
            use_actual_data=use_actual_data
        )
        
        if not corrected_documents:
            logger.error("No corrected documents to save")
            if mongo_client:
                mongo_client.close()
            return False
        
        # Save corrected data to cleaned_dt collection
        success = save_corrected_data_types(project_id, corrected_documents)
        
        if success:
            # Generate report
            generate_report(results, project_id)
            logger.info(f"✅ CDT process completed successfully for {project_id}")
        else:
            logger.error("Failed to save corrected data types")
        
        # Close MongoDB connection
        if mongo_client:
            mongo_client.close()
            logger.debug("MongoDB connection closed")
        
        return success
        
    except Exception as e:
        logger.error(f"Fatal error in run_cdt: {e}", exc_info=True)
        print(f"\n❌ Error: {e}")
        return False


def main():
    """Main entry point for the script."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Detect and correct data type anomalies using LLM'
    )
    parser.add_argument(
        'project_id',
        help='Project ID (e.g., UID001PJ001)'
    )
    parser.add_argument(
        '--no-actual-data',
        action='store_true',
        help='Use stored samples instead of fetching from actual data'
    )
    
    args = parser.parse_args()
    
    # Run the correction process
    use_actual_data = not args.no_actual_data
    success = run_cdt(args.project_id, use_actual_data=use_actual_data)
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()