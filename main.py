#!/usr/bin/env python3
"""Main ETL pipeline orchestrator."""

import sys
import os
from pathlib import Path

# Add src to path
sys.path.insert(0, os.path.dirname(__file__))

from src.logger import setup_logger
from src.config import settings
from src.extractors import CSVExtractor
from src.transformers import EnergyTransformer
from src.loaders import CSVLoader

logger = setup_logger(__name__, settings.log_level)


def create_sample_data() -> None:
    """Create sample data for demonstration."""
    import pandas as pd
    
    sample_data = {
        'timestamp': pd.date_range('2024-01-01', periods=100, freq='h'),
        'building_id': [f'BLD_{i % 10:03d}' for i in range(100)],
        'energy_consumption_kwh': [50 + (i % 24) * 5 + (i % 3 - 1) * 10 for i in range(100)],
        'temperature_celsius': [15 + (i % 24) * 0.5 + (i % 5 - 2) for i in range(100)],
        'occupancy_count': [i % 150 for i in range(100)],
    }
    
    df = pd.DataFrame(sample_data)
    
    # Create data directory
    raw_dir = Path(settings.data_source_path)
    raw_dir.mkdir(parents=True, exist_ok=True)
    
    # Save sample data
    sample_file = raw_dir / "sample_energy_data.csv"
    df.to_csv(sample_file, index=False)
    logger.info(f"Sample data created at {sample_file}")


def run_etl_pipeline() -> None:
    """Execute the ETL pipeline."""
    try:
        logger.info("=" * 50)
        logger.info("Starting Urban Energy ETL Pipeline")
        logger.info("=" * 50)
        
        # Create sample data if source doesn't exist
        if not Path(settings.data_source_path).exists():
            logger.info("Source data not found, creating sample data...")
            create_sample_data()
        
        # Extract
        logger.info("\n[EXTRACT] Starting data extraction...")
        extractor = CSVExtractor(settings.data_source_path)
        raw_data = extractor.extract()
        
        if raw_data.empty:
            logger.error("No data extracted. Exiting.")
            return
        
        # Transform
        logger.info("\n[TRANSFORM] Starting data transformation...")
        transformer = EnergyTransformer()
        transformed_data = transformer.transform(raw_data)
        
        # Load
        logger.info("\n[LOAD] Starting data load...")
        loader = CSVLoader(settings.output_path)
        output_file = loader.load(
            transformed_data,
            filename="energy_data_processed.csv"
        )
        
        # Summary
        logger.info("\n" + "=" * 50)
        logger.info("ETL Pipeline Completed Successfully!")
        logger.info("=" * 50)
        logger.info(f"Rows processed: {len(transformed_data)}")
        logger.info(f"Output file: {output_file}")
        
    except Exception as e:
        logger.error(f"ETL Pipeline failed: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    run_etl_pipeline()
