"""Tests for data transformers."""

import pytest
import pandas as pd
from src.transformers import EnergyTransformer


def test_energy_transformer_basic():
    """Test basic energy transformation."""
    # Create sample data
    df = pd.DataFrame({
        'Timestamp': ['2024-01-01', '2024-01-02', '2024-01-03'],
        'Energy Consumption': [100, 150, 200],
        'Temperature': [15.5, 16.2, 17.1]
    })
    
    transformer = EnergyTransformer()
    result = transformer.transform(df)
    
    assert not result.empty
    assert len(result) == 3
    assert 'timestamp' in result.columns
    assert 'energy_consumption' in result.columns


def test_energy_transformer_removes_duplicates():
    """Test that transformer removes duplicate rows."""
    df = pd.DataFrame({
        'id': [1, 1, 2],
        'value': [10, 10, 20]
    })
    
    transformer = EnergyTransformer()
    result = transformer.transform(df)
    
    assert len(result) == 2


def test_energy_transformer_handles_missing_values():
    """Test that transformer handles missing values."""
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'value': [10, None, 30]
    })
    
    transformer = EnergyTransformer()
    result = transformer.transform(df)
    
    assert result['value'].isnull().sum() == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
