import argparse
import pandas as pd
import json
import sys
import os
from pathlib import Path
from typing import Dict, Any

# Add parent directories to path
sys.path.append("../../../")

from helpers.database.connection_to_db import connect_to_mongodb
from helpers.llm.call_llm import call_llm
from helpers.logger import get_logger

logger = get_logger(__name__)


class DatasetAnalyzer:
    def __init__(self, project_id: str):
        self.project_id = project_id
        self.user_id = project_id[:6]  # Extract UID001 from UID001PJ001
        self.mongo_client = None
        
        # HARDCODED TEMPLATE PATH - Get absolute path
        current_file = Path(__file__).resolve()  # backend/ai_agents/src/pipelines/data_analysis.py
        backend_root = current_file.parent.parent.parent.parent  # Go up to backend/
        self.template_path = backend_root / "ai_agents" / "src" / "prompts" / "data_analysis_prompt.jinja"
        
        logger.info(f"Template path: {self.template_path}")
        
        # Verify template exists and read it
        if not self.template_path.exists():
            raise FileNotFoundError(f"Template not found at: {self.template_path}")
        
        # Read the template content
        with open(self.template_path, 'r', encoding='utf-8') as f:
            self.template_content = f.read()
        
        logger.info(f"✓ Template loaded successfully")
        
    def extract_user_and_project_id(self) -> tuple:
        """Extract user_id and project_id from the combined ID"""
        user_id = self.project_id[:6]  # UID001
        project_id_only = self.project_id[6:]  # PJ001
        return user_id, project_id_only
    
    def fetch_data_from_mongodb(self) -> pd.DataFrame:
        """Fetch raw data from MongoDB"""
        try:
            logger.info(f"Connecting to MongoDB for project: {self.project_id}")
            self.mongo_client = connect_to_mongodb()
            
            if self.mongo_client is None:
                raise Exception("Failed to connect to MongoDB")
            
            db = self.mongo_client[self.user_id]
            collection_name = f"{self.project_id}_data"
            collection = db[collection_name]
            
            # Fetch all documents
            data = list(collection.find({}))
            
            if not data:
                raise ValueError(f"No data found in collection {collection_name}")
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Remove MongoDB's _id field if present
            if '_id' in df.columns:
                df = df.drop('_id', axis=1)
            
            logger.info(f"✓ Fetched {len(df)} records from {collection_name}")
            return df
            
        except Exception as e:
            logger.error(f"Error fetching data from MongoDB: {str(e)}")
            raise
    
    def get_data_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Generate comprehensive data summary for LLM analysis"""
        logger.info("Generating data summary...")
        
        summary = {
            "basic_info": {
                "total_rows": len(df),
                "total_columns": len(df.columns),
                "memory_usage": f"{df.memory_usage(deep=True).sum() / 1024**2:.2f} MB"
            },
            "columns": {}
        }
        
        for col in df.columns:
            col_info = {
                "dtype": str(df[col].dtype),
                "non_null_count": int(df[col].notna().sum()),
                "null_count": int(df[col].isna().sum()),
                "null_percentage": f"{(df[col].isna().sum() / len(df) * 100):.2f}%",
                "unique_values": int(df[col].nunique()),
            }
            
            # Add type-specific information
            if pd.api.types.is_numeric_dtype(df[col]):
                col_info.update({
                    "min": float(df[col].min()) if pd.notna(df[col].min()) else None,
                    "max": float(df[col].max()) if pd.notna(df[col].max()) else None,
                    "mean": float(df[col].mean()) if pd.notna(df[col].mean()) else None,
                    "median": float(df[col].median()) if pd.notna(df[col].median()) else None,
                    "std": float(df[col].std()) if pd.notna(df[col].std()) else None,
                })
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                col_info.update({
                    "min_date": str(df[col].min()),
                    "max_date": str(df[col].max()),
                    "date_range_days": (df[col].max() - df[col].min()).days if pd.notna(df[col].max()) else None
                })
            else:
                # For categorical/string columns
                top_values = df[col].value_counts().head(5).to_dict()
                col_info["top_5_values"] = {str(k): int(v) for k, v in top_values.items()}
            
            summary["columns"][col] = col_info
        
        # Add sample data (first 10 rows)
        summary["sample_data"] = df.head(10).to_dict(orient='records')
        
        # Check for duplicates
        summary["duplicates"] = {
            "duplicate_rows": int(df.duplicated().sum()),
            "duplicate_percentage": f"{(df.duplicated().sum() / len(df) * 100):.2f}%"
        }
        
        logger.info("✓ Data summary generated")
        return summary
    
    def render_template_manually(self, context_variables: Dict[str, Any]) -> str:
        """Manually render the Jinja template"""
        from jinja2 import Template
        
        template = Template(self.template_content)
        rendered = template.render(**context_variables)
        return rendered
    
    def analyze_with_llm(self, data_summary: Dict[str, Any]) -> Dict[str, Any]:
        """Send data to LLM for analysis using the call_llm function"""
        try:
            logger.info("Preparing data for LLM analysis...")
            
            # Prepare context variables for the Jinja template
            context_variables = {
                "basic_info": data_summary["basic_info"],
                "columns": data_summary["columns"],
                "sample_data": json.dumps(data_summary["sample_data"], indent=2, default=str),
                "duplicates": data_summary["duplicates"],
                "project_id": self.project_id,
                "user_id": self.user_id
            }
            
            logger.info("Rendering template manually...")
            # Render template manually
            rendered_prompt = self.render_template_manually(context_variables)
            
            logger.info(f"Prompt length: {len(rendered_prompt)} characters")
            logger.info("Calling LLM for dataset analysis...")
            
            # Call LLM with the rendered prompt directly (not as a template)
            response = call_llm(
                prompt_or_template=rendered_prompt,
                context_variables={},  # Empty since we already rendered
                use_template=False,  # Important: Set to False since we're passing rendered text
                temperature=0.3
            )
            
            # Extract text from response
            if hasattr(response, 'content'):
                response_text = response.content
            elif isinstance(response, str):
                response_text = response
            else:
                response_text = str(response)
            
            logger.info("✓ Received analysis from LLM")
            
            return {
                "analysis": response_text,
                "raw_summary": data_summary
            }
            
        except Exception as e:
            logger.error(f"Error analyzing with LLM: {str(e)}", exc_info=True)
            raise
    
    def save_results_to_mongodb(self, results: Dict[str, Any]):
        """Save analysis results to MongoDB collection"""
        try:
            logger.info(f"Saving results to MongoDB...")
            
            if self.mongo_client is None:
                logger.warning("MongoDB client not available, reconnecting...")
                self.mongo_client = connect_to_mongodb()
            
            db = self.mongo_client[self.user_id]
            collection_name = f"{self.project_id}_cleaned_data"
            collection = db[collection_name]
            
            # Prepare document to save
            document = {
                "project_id": self.project_id,
                "user_id": self.user_id,
                "analysis_timestamp": pd.Timestamp.now().isoformat(),
                "analysis": results['analysis'],
                "data_summary": results['raw_summary'],
                "status": "completed"
            }
            
            # Clear existing data and insert new analysis
            collection.delete_many({})
            result = collection.insert_one(document)
            
            logger.info(f"✓ Analysis saved to MongoDB collection: {collection_name}")
            logger.info(f"  Document ID: {result.inserted_id}")
            
            return result.inserted_id
            
        except Exception as e:
            logger.error(f"Error saving results to MongoDB: {str(e)}", exc_info=True)
            raise
    
    def save_results(self, results: Dict[str, Any], output_file: str = None):
        """Save analysis results to both MongoDB and files"""
        # Save to MongoDB
        self.save_results_to_mongodb(results)
        
        # Also save to files for backup/reference
        if output_file is None:
            output_file = f"{self.project_id}_analysis.json"
        
        logger.info(f"Saving results to file: {output_file}")
        
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        logger.info(f"✓ Analysis saved to {output_file}")
        
        # Also save a human-readable markdown version
        md_file = output_file.replace('.json', '.md')
        with open(md_file, 'w') as f:
            f.write(f"# Dataset Analysis Report\n\n")
            f.write(f"**Project ID**: {self.project_id}\n")
            f.write(f"**User ID**: {self.user_id}\n\n")
            f.write("---\n\n")
            f.write(results['analysis'])
        
        logger.info(f"✓ Markdown report saved to {md_file}")
    
    def cleanup(self):
        """Cleanup resources"""
        if self.mongo_client:
            self.mongo_client.close()
            logger.info("MongoDB connection closed")
    
    def run(self, output_file: str = None):
        """Main execution flow"""
        try:
            logger.info("=" * 60)
            logger.info(f"Dataset Analysis for Project: {self.project_id}")
            logger.info("=" * 60)
            
            # Step 1: Fetch data
            logger.info("Step 1: Fetching data from MongoDB...")
            df = self.fetch_data_from_mongodb()
            
            # Step 2: Generate summary
            logger.info("\nStep 2: Generating data summary...")
            data_summary = self.get_data_summary(df)
            
            # Step 3: Analyze with LLM
            logger.info("\nStep 3: Analyzing with LLM...")
            results = self.analyze_with_llm(data_summary)
            
            # Step 4: Save results
            logger.info("\nStep 4: Saving results to MongoDB and files...")
            self.save_results(results, output_file)
            
            logger.info("=" * 60)
            logger.info("Analysis Complete!")
            logger.info("=" * 60)
            
            return results
            
        except Exception as e:
            logger.error(f"Error during analysis: {str(e)}", exc_info=True)
            raise
        finally:
            self.cleanup()


def main():
    parser = argparse.ArgumentParser(description='Analyze dataset for dashboard creation')
    parser.add_argument('project_id', type=str, help='Project ID (e.g., UID001PJ001)')
    parser.add_argument('--output', type=str, help='Output file path for analysis results')
    
    args = parser.parse_args()
    
    # Validate project_id format
    if len(args.project_id) < 10:
        logger.error("Invalid project_id format. Expected format: UID001PJ001")
        return 1
    
    try:
        analyzer = DatasetAnalyzer(args.project_id)
        analyzer.run(args.output)
        return 0
    except Exception as e:
        logger.error(f"❌ Analysis failed: {str(e)}")
        return 1


if __name__ == "__main__":
    exit(main())