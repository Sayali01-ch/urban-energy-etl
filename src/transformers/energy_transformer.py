"""Energy data transformation module."""

import pandas as pd
from datetime import datetime
from src.logger import setup_logger

logger = setup_logger(__name__)


class EnergyTransformer:
    """Transform raw energy data into usable format."""
    
    def __init__(self):
        """Initialize energy transformer."""
        self.data = None
    
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Transform energy data.
        
        Args:
            data: Raw energy data DataFrame
            
        Returns:
            Transformed DataFrame
        """
        logger.info("Starting data transformation")
        
        self.data = data.copy()
        
        # Clean data
        self._clean_data()
        
        # Standardize columns
        self._standardize_columns()
        
        # Handle missing values
        self._handle_missing_values()
        
        # Add derived columns
        self._add_derived_columns()
        
        logger.info(f"Transformation complete: {len(self.data)} rows processed")
        return self.data
    
    def _clean_data(self) -> None:
        """Remove duplicates and invalid records."""
        logger.debug("Cleaning data")
        
        initial_count = len(self.data)
        self.data = self.data.drop_duplicates()
        removed_count = initial_count - len(self.data)
        
        if removed_count > 0:
            logger.info(f"Removed {removed_count} duplicate rows")
    
    def _standardize_columns(self) -> None:
        """Standardize column names and types."""
        logger.debug("Standardizing columns")
        
        # Convert column names to lowercase
        self.data.columns = self.data.columns.str.lower().str.replace(' ', '_')
        
        # Convert timestamp columns to datetime if they exist
        timestamp_cols = [col for col in self.data.columns if 'time' in col or 'date' in col]
        for col in timestamp_cols:
            try:
                self.data[col] = pd.to_datetime(self.data[col])
            except Exception as e:
                logger.warning(f"Could not convert {col} to datetime: {str(e)}")
    
    def _handle_missing_values(self) -> None:
        """Handle missing values in the data."""
        logger.debug("Handling missing values")
        
        missing_count = self.data.isnull().sum().sum()
        if missing_count > 0:
            logger.info(f"Found {missing_count} missing values")
            # Fill numeric columns with mean
            numeric_cols = self.data.select_dtypes(include=['number']).columns
            self.data[numeric_cols] = self.data[numeric_cols].fillna(self.data[numeric_cols].mean())
    
    def _add_derived_columns(self) -> None:
        """Add derived columns for analysis."""
        logger.debug("Adding derived columns")
        
        # Add timestamp if not exists
        if 'timestamp' not in self.data.columns:
            self.data['timestamp'] = datetime.now()
        
        # Add data quality flag
        self.data['data_quality'] = 'good'
