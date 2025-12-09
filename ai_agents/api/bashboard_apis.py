from fastapi import APIRouter, HTTPException
from typing import Dict, Any
import logging
import sys
sys.path.append("../..")
from pipelines.processing.data_type_finding import run_dtf
from pipelines.processing.data_anomaly import run_cdt
from pipelines.processing.chart_suggestion import run_cs
from pipelines.processing.data_cleaning import run_chart_pipeline
from pipelines.processing.data_flatted_weviate import run_dfw
from pipelines.processing.vectorization import run_v
from pipelines.processing.data_to_weviate import run_dtw

# Initialize logger
logger = logging.getLogger(__name__)

# Initialize router
router = APIRouter()


def run_pdp(project_id: str) -> Dict[str, Any]:
    """
    Run the complete Project Data Pipeline (PDP) for a given project.
    
    Args:
        project_id: The unique identifier for the project
        
    Returns:
        Dictionary containing results from all pipeline steps
        
    Raises:
        HTTPException: If any step in the pipeline fails
    """
    results = {}
    
    # Step 1: Run Data Type Finding
    logger.info(f"Step 1: Running data type finding for project {project_id}")
    try:
        dtf_result = run_dtf(project_id)
        results["data_type_finding"] = dtf_result
        logger.info(f"Data type finding completed for project {project_id}")
    except Exception as e:
        logger.error(f"Error in data type finding: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Data type finding failed: {str(e)}")
    
    # Step 2: Run Data Anomaly Detection
    logger.info(f"Step 2: Running data anomaly detection for project {project_id}")
    try:
        cdt_result = run_cdt(project_id)
        results["data_anomaly"] = cdt_result
        logger.info(f"Data anomaly detection completed for project {project_id}")
    except Exception as e:
        logger.error(f"Error in data anomaly detection: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Data anomaly detection failed: {str(e)}")
    
    # Step 3: Run Chart Suggestion
    logger.info(f"Step 3: Running chart suggestion for project {project_id}")
    try:
        cs_result = run_cs(project_id)
        results["chart_suggestion"] = cs_result
        logger.info(f"Chart suggestion completed for project {project_id}")
    except Exception as e:
        logger.error(f"Error in chart suggestion: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Chart suggestion failed: {str(e)}")
    
    # Step 4: Run Chart Pipeline
    logger.info(f"Step 4: Running chart pipeline for project {project_id}")
    try:
        chart_pipeline_result = run_chart_pipeline(project_id)
        results["chart_pipeline"] = chart_pipeline_result
        logger.info(f"Chart pipeline completed for project {project_id}")
    except Exception as e:
        logger.error(f"Error in chart pipeline: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Chart pipeline failed: {str(e)}")
    
    # Step 5: Run Data Flattening for Weaviate
    logger.info(f"Step 5: Running data flattening for Weaviate for project {project_id}")
    try:
        dfw_result = run_dfw(project_id)
        results["data_flattened_weaviate"] = dfw_result
        logger.info(f"Data flattening for Weaviate completed for project {project_id}")
    except Exception as e:
        logger.error(f"Error in data flattening for Weaviate: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Data flattening for Weaviate failed: {str(e)}")
    
    # Step 6: Run Vectorization
    logger.info(f"Step 6: Running vectorization for project {project_id}")
    try:
        v_result = run_v(project_id)
        results["vectorization"] = v_result
        logger.info(f"Vectorization completed for project {project_id}")
    except Exception as e:
        logger.error(f"Error in vectorization: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Vectorization failed: {str(e)}")
    
    # Step 7: Run Data to Weaviate
    logger.info(f"Step 7: Running data to Weaviate for project {project_id}")
    try:
        dtw_result = run_dtw(project_id)
        results["data_to_weaviate"] = dtw_result
        logger.info(f"Data to Weaviate completed for project {project_id}")
    except Exception as e:
        logger.error(f"Error in data to Weaviate: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Data to Weaviate failed: {str(e)}")
    
    logger.info(f"Project Data Pipeline completed successfully for project {project_id}")
    return results

