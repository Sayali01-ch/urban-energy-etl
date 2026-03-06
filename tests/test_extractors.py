"""Tests for data extractors."""

import pytest
import pandas as pd
import tempfile
from pathlib import Path
from src.extractors import CSVExtractor


def test_csv_extractor_with_file():
    """Test CSV extractor with a single file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create sample CSV
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'value': [10, 20, 30]
        })
        csv_path = Path(tmpdir) / "test.csv"
        df.to_csv(csv_path, index=False)
        
        # Extract
        extractor = CSVExtractor(str(csv_path))
        result = extractor.extract()
        
        assert not result.empty
        assert len(result) == 3
        assert list(result.columns) == ['id', 'value']


def test_csv_extractor_with_directory():
    """Test CSV extractor with a directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create multiple CSV files
        for i in range(3):
            df = pd.DataFrame({
                'id': [i * 10 + j for j in range(5)],
                'value': [j * 10 for j in range(5)]
            })
            csv_path = Path(tmpdir) / f"test_{i}.csv"
            df.to_csv(csv_path, index=False)
        
        # Extract
        extractor = CSVExtractor(tmpdir)
        result = extractor.extract()
        
        assert not result.empty
        assert len(result) == 15  # 3 files * 5 rows


def test_csv_extractor_empty_directory():
    """Test CSV extractor with empty directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        extractor = CSVExtractor(tmpdir)
        result = extractor.extract()
        
        assert result.empty


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
