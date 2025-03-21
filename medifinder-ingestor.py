# Project Structure
'''
medifinder-ingestor/
├── config/
│   ├── __init__.py
│   └── settings.py
├── models/
│   ├── __init__.py
│   └── database.py
├── services/
│   ├── __init__.py
│   ├── parser.py
│   └── etl.py
├── utils/
│   ├── __init__.py
│   └── logger.py
├── .env.example
├── medifinder-ingestor.py
└── requirements.txt
'''

# Main application script - medifinder-ingestor.py
import argparse
import os
import sys
import time
from datetime import datetime

from config.settings import load_env
from models.database import DatabaseManager
from services.parser import FileParser
from services.etl import ETLProcessor
from utils.logger import setup_logger

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='MediFinderBot Data Ingestion Service')
    parser.add_argument('source_file', help='Path to the source data text file')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], 
                        default='INFO', help='Set logging level')
    args = parser.parse_args()
    
    # Load environment variables
    load_env()
    
    # Set up logging
    log_file = f"ingestor_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logger = setup_logger(args.log_level, log_file)
    
    logger.info(f"Starting data ingestion from {args.source_file}")
    start_time = time.time()
    
    try:
        # Initialize database connection
        db_manager = DatabaseManager()
        
        # Parse source file
        file_parser = FileParser(args.source_file)
        records = file_parser.parse()
        logger.info(f"Parsed {len(records)} records from source file")
        
        # Process records
        etl = ETLProcessor(db_manager)
        stats = etl.process_records(records)
        
        # Perform cleanup (set missing inventory items to zero stock)
        cleanup_count = etl.cleanup_missing_inventory()
        
        # Log statistics
        elapsed_time = time.time() - start_time
        logger.info(f"Ingestion completed in {elapsed_time:.2f} seconds")
        logger.info(f"Statistics: {stats}")
        logger.info(f"Cleanup: {cleanup_count} inventory items marked as zero stock")
        
    except Exception as e:
        logger.error(f"Ingestion failed: {str(e)}", exc_info=True)
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())