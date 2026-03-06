"""CSV data extractor."""

import os
import pandas as pd
from typing import List, Optional
from pathlib import Path
from src.logger import setup_logger

logger = setup_logger(__name__)


class CSVExtractor:
    """Extract data from CSV files."""
    
    def __init__(self, source_path: str):
        """
        Initialize CSV extractor.
        
        Args:
            source_path: Path to CSV files or directory
        """
        self.source_path = source_path
        self.data = None
    
    def extract(self, pattern: Optional[str] = None) -> pd.DataFrame:
        """
        Extract data from CSV files.
        
        Args:
            pattern: Optional file pattern to filter files (e.g., "*.csv")
            
        Returns:
            Combined DataFrame from all CSV files
        """
        logger.info(f"Extracting data from {self.source_path}")
        
        files = self._get_csv_files(pattern)
        
        if not files:
            logger.warning(f"No CSV files found in {self.source_path}")
            return pd.DataFrame()
        
        dataframes = []
        for file_path in files:
            try:
                logger.info(f"Reading file: {file_path}")
                df = pd.read_csv(file_path)
                dataframes.append(df)
            except Exception as e:
                logger.error(f"Error reading {file_path}: {str(e)}")
        
        if dataframes:
            self.data = pd.concat(dataframes, ignore_index=True)
            logger.info(f"Successfully extracted {len(self.data)} rows")
            return self.data
        else:
            logger.error("No data extracted")
            return pd.DataFrame()
    
    def _get_csv_files(self, pattern: Optional[str] = None) -> List[str]:
        """
        Get list of CSV files from source path.
        
        Args:
            pattern: Optional file pattern
            
        Returns:
            List of file paths
        """
        source = Path(self.source_path)
        
        if source.is_file():
            return [str(source)]
        
        if source.is_dir():
            pattern = pattern or "*.csv"
            return sorted([str(f) for f in source.glob(pattern)])
        
        return []
