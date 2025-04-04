import os
import pytest
import pandas as pd
from pathlib import Path
import shutil
import tempfile
from datetime import datetime

from agroamigo_pipeline.processors.excel_processor import ExcelProcessor
from agroamigo_pipeline.utils.file_registry import FileRegistry

# Test data directory
TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), 'data', 'excel')

# Ensure test data directory exists
os.makedirs(TEST_DATA_DIR, exist_ok=True)

@pytest.fixture
def temp_dirs():
    """Create temporary directories for testing."""
    temp_source = tempfile.mkdtemp()
    temp_output = tempfile.mkdtemp()
    
    yield temp_source, temp_output
    
    # Clean up
    if os.path.exists(temp_source):
        shutil.rmtree(temp_source)
    if os.path.exists(temp_output):
        shutil.rmtree(temp_output)

@pytest.fixture
def sample_excel_file(temp_dirs):
    """Create a sample Excel file for testing."""
    temp_source, _ = temp_dirs
    
    # Create test data
    data = {
        'Producto': ['Hortalizas y verduras', 'Tomate', 'Cebolla', 'Frutas frescas', 'Manzana', 'Banano'],
        'Bogota, Corabastos': [None, 1500, 1200, None, 2000, 1000],
        'Medellin, Plaza Minorista': [None, 1600, 1300, None, 2100, 1100]
    }
    
    df = pd.DataFrame(data)
    
    # Create header with date
    header_df = pd.DataFrame({
        'Header': ['', '', 'viernes 15 de octubre de 2021', '', 'Precio por kilogramo']
    })
    
    # Save as Excel
    file_path = os.path.join(temp_source, 'test_excel.xlsx')
    with pd.ExcelWriter(file_path) as writer:
        header_df.to_excel(writer, sheet_name='Sheet1', index=False)
        df.to_excel(writer, sheet_name='Sheet1', startrow=len(header_df)+1, index=False)
    
    return file_path

def test_date_extraction(sample_excel_file):
    """Test date extraction from Excel file header."""
    processor = ExcelProcessor()
    df = pd.read_excel(sample_excel_file)
    
    date = processor._extract_date_from_header(df)
    
    assert date is not None
    assert isinstance(date, str)
    assert date == '2021-10-15'

def test_geography_parsing(sample_excel_file):
    """Test geography parsing from column headers."""
    processor = ExcelProcessor()
    df = pd.read_excel(sample_excel_file, skiprows=5)  # Skip header rows
    
    geography_info = processor._extract_geography_from_columns(df)
    
    assert len(geography_info) == 2
    assert 'Bogota' in [g['city'] for g in geography_info]
    assert 'Medellin' in [g['city'] for g in geography_info]
    assert 'Corabastos' in [g['market'] for g in geography_info]
    assert 'Plaza Minorista' in [g['market'] for g in geography_info]

def test_category_detection(sample_excel_file):
    """Test major category detection."""
    processor = ExcelProcessor()
    df = pd.read_excel(sample_excel_file, skiprows=5)  # Skip header rows
    
    major_categories = processor._identify_major_categories(df)
    
    assert len(major_categories) == 2
    assert 'Hortalizas y verduras' in major_categories
    assert 'Frutas frescas' in major_categories

def test_product_category_mapping(sample_excel_file):
    """Test mapping products to categories."""
    processor = ExcelProcessor()
    df = pd.read_excel(sample_excel_file, skiprows=5)  # Skip header rows
    
    major_categories = processor._identify_major_categories(df)
    product_map = processor._map_products_to_categories(df, major_categories)
    
    assert len(product_map) == 4  # 4 products (excluding category rows)
    assert product_map['Tomate'] == 'Hortalizas y verduras'
    assert product_map['Cebolla'] == 'Hortalizas y verduras'
    assert product_map['Manzana'] == 'Frutas frescas'
    assert product_map['Banano'] == 'Frutas frescas'

def test_end_to_end_processing(temp_dirs, sample_excel_file):
    """Test end-to-end processing of an Excel file."""
    temp_source, temp_output = temp_dirs
    
    processor = ExcelProcessor()
    stats = processor.process_excel_file(sample_excel_file, temp_output)
    
    # Check if output file was created
    output_files = list(Path(temp_output).glob('*.csv'))
    assert len(output_files) == 1
    
    # Check output data
    output_df = pd.read_csv(output_files[0])
    
    # Verify schema
    expected_columns = [
        'date', 'city', 'market', 'major_category', 'sub_category', 
        'product', 'presentation', 'units', 'daily_min', 'daily_max', 'source'
    ]
    for col in expected_columns:
        assert col in output_df.columns
    
    # Verify data
    assert len(output_df) == 8  # 4 products * 2 cities
    assert set(output_df['major_category'].unique()) == {'Hortalizas y verduras', 'Frutas frescas'}
    assert set(output_df['product'].unique()) == {'Tomate', 'Cebolla', 'Manzana', 'Banano'}
    assert set(output_df['city'].unique()) == {'Bogota', 'Medellin'}
    assert all(output_df['source'] == 'anexo')
    
    # Verify stats
    assert stats['unique_products'] == 4
    assert stats['unique_major_categories'] == 2
    assert stats['unique_subcategories'] == 0
    assert not stats['error_occurred']

def test_file_registry_integration(temp_dirs, sample_excel_file):
    """Test integration with file registry to avoid duplicate processing."""
    temp_source, temp_output = temp_dirs
    
    # Process file first time
    processor = ExcelProcessor()
    stats1 = processor.process_excel_file(sample_excel_file, temp_output)
    
    # Check file is in registry
    registry = FileRegistry()
    assert registry.file_processed(sample_excel_file)
    
    # Process again without force_reprocess
    stats2 = processor.process_excel_file(sample_excel_file, temp_output)
    
    # Should return the previously saved stats, not process again
    assert stats2 == registry.get_file_info(sample_excel_file)['processing_history'][-1]['stats']
    
    # Process again with force_reprocess
    stats3 = processor.process_excel_file(sample_excel_file, temp_output, force_reprocess=True)
    
    # Should process again and update registry
    assert stats3 != stats2