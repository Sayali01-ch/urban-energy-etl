"""CSV data loader."""

import os
import pandas as pd
from pathlib import Path
from src.logger import setup_logger

logger = setup_logger(__name__)


class CSVLoader:
    """Load data to CSV files."""
    
    def __init__(self, output_path: str):
        """
        Initialize CSV loader.
        
        Args:
            output_path: Directory path for output files
        """
        self.output_path = output_path
        self._ensure_output_directory()
    
    def load(self, data: pd.DataFrame, filename: str = "output.csv") -> str:
        """
        Load DataFrame to CSV file.
        
        Args:
            data: DataFrame to load
            filename: Output filename
            
        Returns:
            Full path to output file
        """
        try:
            output_file = os.path.join(self.output_path, filename)
            data.to_csv(output_file, index=False)
            logger.info(f"Data loaded successfully to {output_file}")
            return output_file
        except Exception as e:
            logger.error(f"Error loading data: {str(e)}")
            raise
    
    def _ensure_output_directory(self) -> None:
        """Create output directory if it doesn't exist."""
        Path(self.output_path).mkdir(parents=True, exist_ok=True)
        logger.debug(f"Output directory ensured at {self.output_path}")
