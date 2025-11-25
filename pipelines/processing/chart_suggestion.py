import sys
import json
sys.path.append("../..")
from helpers.logger import get_logger
from helpers.database.connection_to_db import connect_to_mongodb
from helpers.llm.call_llm import call_llm

logger = get_logger(__name__)


def extract_project_info(project_id):
    """Extract user_id and db_name from project_id"""
    # project_id format: UID00xPJ00y
    user_id = project_id.split('PJ')[0]  # Extract UID00x
    db_name = user_id
    collection_name = f"{project_id}_cleaned_dt"
    
    return user_id, db_name, collection_name


def get_database(db_name):
    """Get MongoDB database instance"""
    client = connect_to_mongodb()
    if not client:
        raise Exception("Failed to connect to MongoDB")
    return client[db_name], client


def fetch_cleaned_data_attributes(project_id):
    """Fetch all attributes from the cleaned_dt collection"""
    client = None
    try:
        user_id, db_name, collection_name = extract_project_info(project_id)
        logger.info(f"Connecting to database: {db_name}, collection: {collection_name}")
        
        # Connect to MongoDB
        db, client = get_database(db_name)
        collection = db[collection_name]
        
        # Fetch all documents (attributes)
        attributes = list(collection.find({}, {'_id': 0}))
        
        logger.info(f"Found {len(attributes)} attributes")
        return attributes
    
    except Exception as e:
        logger.error(f"Error fetching attributes: {str(e)}")
        raise
    
    finally:
        if client:
            client.close()


def build_llm_prompt(attributes):
    """Build a comprehensive prompt for the LLM to suggest charts"""
    
    attributes_summary = []
    for attr in attributes:
        attr_info = {
            "attribute": attr.get("attribute"),
            "data_type": attr.get("data_type"),
            "original_data_type": attr.get("original_data_type"),
            "sample": attr.get("sample", [])[:5]  # Limit to 5 samples
        }
        attributes_summary.append(attr_info)
    
    prompt = f"""
You are a data visualization expert. Analyze the following dataset attributes and suggest ALL possible meaningful charts for visualization.

DATASET ATTRIBUTES:
{json.dumps(attributes_summary, indent=2)}

TASK:
Suggest 10-15 different chart visualizations covering all possibilities for this dataset. Categorize each chart as either:
- **"direct"**: Essential charts that should appear directly on the dashboard (top 5-7 most important)
- **"suggestion"**: Additional useful charts that users can optionally add

For each chart, provide:

1. **Chart Type**: Specify the type (bar_chart, line_chart, pie_chart, scatter_plot, heatmap, geo_map, histogram, box_plot, etc.)

2. **Display Mode**: Set as "direct" or "suggestion"
   - "direct" for core insights (overview metrics, key distributions, primary trends)
   - "suggestion" for deeper analysis, specific use cases, or alternative views

3. **Configuration**: Based on chart type, provide appropriate fields with calculations embedded directly:
   
   - For **bar_chart/line_chart**:
     * x_axis: attribute name (e.g., "type", "genre", "country")
     * y_axis: calculation (e.g., "count(type)", "avg(rating)", "sum(revenue)")
     * group_by: (optional) attribute for grouping
   
   - For **pie_chart**:
     * category: attribute name (e.g., "rating", "type")
     * value: calculation (e.g., "count(rating)", "sum(revenue)")
   
   - For **scatter_plot**:
     * x_axis: attribute or calculation (e.g., "release_year", "avg(rating)")
     * y_axis: attribute or calculation (e.g., "duration", "count(type)")
   
   - For **geo_map**:
     * location_field: attribute with location data (e.g., "country")
     * value_field: calculation (e.g., "count(country)", "avg(rating)")
     * map_type: "choropleth" or "marker"
   
   - For **heatmap**:
     * x_axis: attribute name (e.g., "month")
     * y_axis: attribute name (e.g., "day_of_week")
     * value: calculation (e.g., "count(*)", "avg(views)")
   
   - For **histogram**:
     * field: attribute name (e.g., "rating", "duration")
     * bins: number of bins (e.g., 10, 20)
   
   - For **box_plot**:
     * category: attribute name (e.g., "type", "genre")
     * value: attribute name (e.g., "duration", "rating")

4. **Title**: A descriptive title for the chart

5. **Description**: Brief explanation of insights this chart would reveal

6. **Priority**: Number 1-6 (1 = highest priority, only for "direct" charts)

CALCULATION SYNTAX:
- count(attribute) - Count occurrences of attribute
- unique(attribute) - Count unique values
- avg(attribute) - Average of numeric attribute
- sum(attribute) - Sum of numeric attribute
- min(attribute) - Minimum value
- max(attribute) - Maximum value
- count(*) - Total count

RESPONSE FORMAT (JSON array):
[
  {{
    "chart_type": "bar_chart",
    "display_mode": "direct",
    "priority": 1,
    "title": "Content Count by Type",
    "description": "Shows the number of Movies and TV Shows in the dataset",
    "config": {{
      "x_axis": "type",
      "y_axis": "count(type)"
    }}
  }},
  {{
    "chart_type": "pie_chart",
    "display_mode": "direct",
    "priority": 2,
    "title": "Distribution by Rating",
    "description": "Displays the proportion of content by rating category",
    "config": {{
      "category": "rating",
      "value": "count(rating)"
    }}
  }},
  {{
    "chart_type": "line_chart",
    "display_mode": "direct",
    "priority": 3,
    "title": "Content Released Over Years",
    "description": "Trend of content releases by year",
    "config": {{
      "x_axis": "release_year",
      "y_axis": "count(release_year)"
    }}
  }},
  {{
    "chart_type": "geo_map",
    "display_mode": "suggestion",
    "priority": 4,
    "title": "Content Distribution by Country",
    "description": "Geographic visualization of content origins",
    "config": {{
      "location_field": "country",
      "value_field": "count(country)",
      "map_type": "choropleth"
    }}
  }},
  {{
    "chart_type": "scatter_plot",
    "display_mode": "suggestion",
    "priority": null,
    "title": "Rating vs Duration Analysis",
    "description": "Correlation between content duration and ratings",
    "config": {{
      "x_axis": "duration",
      "y_axis": "rating"
    }}
  }}
]

GUIDELINES FOR CATEGORIZATION:
- "direct" charts (6 total):
  * Overall summary/count charts
  * Primary distributions (pie/bar charts of main categories)
  * Key trends over time
  * Most important KPIs
  
- "suggestion" charts (remaining):
  * Detailed breakdowns
  * Geographic maps
  * Correlation analyses
  * Box plots and histograms
  * Advanced analytical views

IMPORTANT:
- Embed calculations DIRECTLY into the axis/value fields (e.g., y_axis: "count(type)" NOT y_axis: "count")
- Suggest ALL possible meaningful charts - aim for completeness
- Only suggest charts that make sense given the available attributes and their data types
- Prioritize "direct" charts that provide immediate business value
- Ensure attribute names exactly match those provided
- Return ONLY the JSON array, no additional text
"""
    
    return prompt


def get_chart_suggestions(project_id):
    """Main function to get chart suggestions for a project"""
    try:
        logger.info(f"Starting chart suggestion generation for project: {project_id}")
        
        # Fetch cleaned data attributes
        attributes = fetch_cleaned_data_attributes(project_id)
        
        if not attributes:
            logger.warning("No attributes found in cleaned_dt collection")
            return []
        
        # Build LLM prompt
        prompt = build_llm_prompt(attributes)
        
        # Call LLM
        logger.info("Calling LLM for chart suggestions...")
        llm_response = call_llm(prompt)
        
        # Extract text from response object
        if hasattr(llm_response, 'text'):
            response_text = llm_response.text
        elif hasattr(llm_response, 'candidates'):
            # For Gemini's GenerateContentResponse
            response_text = llm_response.candidates[0].content.parts[0].text
        else:
            # If it's already a string
            response_text = str(llm_response)
        
        logger.info(f"LLM Response type: {type(llm_response)}")
        logger.debug(f"Raw response: {response_text[:500]}...")  # Log first 500 chars
        
        # Parse response
        try:
            suggestions = json.loads(response_text)
            logger.info(f"Generated {len(suggestions)} chart suggestions")
            
            # Separate and log direct vs suggestion charts
            direct_charts = [s for s in suggestions if s.get('display_mode') == 'direct']
            suggestion_charts = [s for s in suggestions if s.get('display_mode') == 'suggestion']
            logger.info(f"  - Direct charts: {len(direct_charts)}")
            logger.info(f"  - Suggestion charts: {len(suggestion_charts)}")
            
            return suggestions
        except json.JSONDecodeError as json_err:
            logger.error(f"Failed to parse LLM response as JSON: {json_err}")
            # Try to extract JSON from response if it's wrapped in text
            start_idx = response_text.find('[')
            end_idx = response_text.rfind(']') + 1
            if start_idx != -1 and end_idx > start_idx:
                json_str = response_text[start_idx:end_idx]
                suggestions = json.loads(json_str)
                logger.info(f"Successfully extracted JSON from response")
                
                # Log categorization
                direct_charts = [s for s in suggestions if s.get('display_mode') == 'direct']
                suggestion_charts = [s for s in suggestions if s.get('display_mode') == 'suggestion']
                logger.info(f"  - Direct charts: {len(direct_charts)}")
                logger.info(f"  - Suggestion charts: {len(suggestion_charts)}")
                
                return suggestions
            raise
    
    except Exception as e:
        logger.error(f"Error generating chart suggestions: {str(e)}")
        raise


def save_suggestions_to_db(project_id, suggestions):
    """Save chart suggestions to a new collection"""
    client = None
    try:
        user_id, db_name, _ = extract_project_info(project_id)
        collection_name = f"{project_id}_charts"
        
        db, client = get_database(db_name)
        collection = db[collection_name]
        
        # Clear existing suggestions
        collection.delete_many({})
        
        # Insert new suggestions
        if suggestions:
            collection.insert_many(suggestions)
            logger.info(f"Saved {len(suggestions)} suggestions to {collection_name}")
        
        return collection_name
    
    except Exception as e:
        logger.error(f"Error saving suggestions: {str(e)}")
        raise
    
    finally:
        if client:
            client.close()


def main():
    """Main execution function"""
    # Get project_id from command line argument
    if len(sys.argv) < 2:
        print("Usage: python chart_suggestions_generator.py <project_id>")
        print("Example: python chart_suggestions_generator.py UID001PJ001")
        sys.exit(1)
    
    project_id = sys.argv[1]
    
    try:
        run_cs(project_id)
    except Exception as e:
        logger.error(f"Failed to generate chart suggestions: {str(e)}")
        sys.exit(1)


def run_cs(project_id):
    """
    Main function to run chart suggestion generation
    
    Args:
        project_id (str): Project ID in format UID00xPJ00y
    
    Returns:
        list: Chart suggestions
    """
    try:
        logger.info(f"Starting chart suggestion generation for project: {project_id}")
        
        # Get chart suggestions
        suggestions = get_chart_suggestions(project_id)
        
        # Separate direct and suggestion charts
        direct_charts = [s for s in suggestions if s.get('display_mode') == 'direct']
        suggestion_charts = [s for s in suggestions if s.get('display_mode') == 'suggestion']
        
        # Print suggestions
        print("\n" + "="*80)
        print(f"CHART SUGGESTIONS FOR PROJECT: {project_id}")
        print("="*80)
        
        print(f"\n{'='*80}")
        print(f"DIRECT CHARTS (Auto-added to Dashboard): {len(direct_charts)}")
        print(f"{'='*80}")
        print(json.dumps(direct_charts, indent=2))
        
        print(f"\n{'='*80}")
        print(f"SUGGESTED CHARTS (Optional): {len(suggestion_charts)}")
        print(f"{'='*80}")
        print(json.dumps(suggestion_charts, indent=2))
        
        # Save to database
        collection_name = save_suggestions_to_db(project_id, suggestions)
        print(f"\n✓ Suggestions saved to collection: {collection_name}")
        print(f"✓ Total suggestions generated: {len(suggestions)}")
        print(f"  - Direct charts: {len(direct_charts)}")
        print(f"  - Suggestion charts: {len(suggestion_charts)}")
        
        logger.info(f"Successfully completed chart suggestion generation for {project_id}")
        return suggestions
        
    except Exception as e:
        logger.error(f"run_cs failed for project {project_id}: {str(e)}")
        raise


if __name__ == "__main__":
    main()