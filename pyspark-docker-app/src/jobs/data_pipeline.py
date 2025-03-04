import os
import sys
import importlib
import logging
import time
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("data_pipeline_orchestrator")

# Define the sequence of jobs to run
JOB_SEQUENCE = [
    "raw_to_bronze",
    "bronze_to_silver",
    "silver_to_gold"
]

def run_job(job_name):
    """
    Dynamically import and run a job module.
    
    Args:
        job_name (str): Name of the job module without .py extension
        
    Returns:
        bool: True if job completed successfully, False otherwise
    """
    start_time = time.time()
    logger.info(f"Starting job: {job_name}")
    
    try:
        # Dynamically import the job module
        job_module = importlib.import_module(f"src.jobs.{job_name}")
        
        # Run the main function of the job
        job_module.main()
        
        execution_time = time.time() - start_time
        logger.info(f"Job {job_name} completed successfully in {execution_time:.2f} seconds")
        return True
    
    except Exception as e:
        execution_time = time.time() - start_time
        logger.error(f"Job {job_name} failed after {execution_time:.2f} seconds: {str(e)}")
        return False

def run_pipeline():
    """
    Run the entire data pipeline, stopping if any job fails.
    
    Returns:
        bool: True if entire pipeline completed successfully, False otherwise
    """
    pipeline_start_time = time.time()
    pipeline_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    logger.info(f"Starting data pipeline execution (ID: {pipeline_id})")
    
    successful_jobs = 0
    failed_jobs = 0
    
    for job_name in JOB_SEQUENCE:
        if run_job(job_name):
            successful_jobs += 1
        else:
            failed_jobs += 1
            logger.error(f"Pipeline execution stopped due to failure in job: {job_name}")
            break  # Stop execution after first failure
    
    # Pipeline summary
    pipeline_execution_time = time.time() - pipeline_start_time
    total_jobs = successful_jobs + failed_jobs
    remaining_jobs = len(JOB_SEQUENCE) - total_jobs
    
    logger.info(f"Pipeline execution completed in {pipeline_execution_time:.2f} seconds")
    logger.info(f"Pipeline summary: {successful_jobs} jobs succeeded, {failed_jobs} jobs failed, {remaining_jobs} jobs not executed")
    
    # Return True only if all jobs succeeded
    return failed_jobs == 0

def main():
    """Main orchestration function"""
    success = run_pipeline()
    
    if not success:
        logger.error("Pipeline failed - one or more jobs encountered errors")
        sys.exit(1)
    else:
        logger.info("Pipeline completed successfully")
        sys.exit(0)

if __name__ == "__main__":
    main()