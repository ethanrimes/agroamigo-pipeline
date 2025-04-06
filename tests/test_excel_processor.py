import os
import pytest
import pandas as pd
import tempfile
import shutil
from pathlib import Path
import filecmp

from agroamigo_pipeline.processors.excel_processor import ExcelProcessor
# Define test data directory paths
TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), 'test_data', 'excel_processor')
SOURCE_DIR = os.path.join(TEST_DATA_DIR, 'source')
EXPECTED_DIR = os.path.join(TEST_DATA_DIR, 'expected')
OUTPUT_DIR = os.path.join(TEST_DATA_DIR, 'outputs')

@pytest.fixture(scope="function")
def setup_test_environment():
    """Create temporary output directory and clean it between tests"""
    # Ensure output directory exists and is clean
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Clear any existing files in output directory
    for file in os.listdir(OUTPUT_DIR):
        file_path = os.path.join(OUTPUT_DIR, file)
        if os.path.isfile(file_path):
            os.remove(file_path)
    
    # Use a temporary file for registry to avoid polluting the real registry
    temp_registry_file = tempfile.mktemp(suffix='.json')
    
    # Return the test directories and temporary registry path
    yield {
        "source_dir": SOURCE_DIR,
        "expected_dir": EXPECTED_DIR,
        "output_dir": OUTPUT_DIR,
        "registry_path": temp_registry_file
    }
    
    # Clean up temp registry file after test
    if os.path.exists(temp_registry_file):
        os.remove(temp_registry_file)

def test_process_single_excel_file(setup_test_environment, monkeypatch):
    """Test processing a single Excel file with validation against expected output"""
    test_env = setup_test_environment
    
    # Patch the registry path to use our temporary file
    monkeypatch.setattr('agroamigo_pipeline.utils.file_registry.PROCESSING_REGISTRY', 
                        test_env["registry_path"])
    
    # Initialize the processor
    processor = ExcelProcessor()
    
    # Get the test file
    source_file = os.path.join(test_env["source_dir"], "20210524-mayoristas_mayo_24_2021.xlsx")
    
    # Process the file
    result = processor.process_excel_file(
        file_path=source_file, 
        output_dir=test_env["output_dir"]
    )
    
    # Check results
    assert not result["error_occurred"]
    assert result["unique_products"] == 6  # 3 fruits + 3 vegetables
    assert result["unique_major_categories"] == 2  # Frutas and Hortalizas
    
    # Verify output file exists
    output_files = list(Path(test_env["output_dir"]).glob("*.csv"))
    assert len(output_files) == 1
    
    output_file = output_files[0]
    expected_file = os.path.join(test_env["expected_dir"], "20210525-mayoristas_mayo_24_2021.csv")
    
    # Compare the output against expected file
    df_output = pd.read_csv(output_file)
    df_expected = pd.read_csv(expected_file)
    
    # Sort both dataframes to ensure consistent comparison
    df_output = df_output.sort_values(by=['product', 'city']).reset_index(drop=True)
    df_expected = df_expected.sort_values(by=['product', 'city']).reset_index(drop=True)
    
    # Check that dataframes match
    pd.testing.assert_frame_equal(df_output, df_expected)
    
    # Verify FileRegistry integration
    registry = FileRegistry()
    assert registry.file_processed(source_file)
    file_info = registry.get_file_info(source_file)
    assert file_info['processing_history'][-1]['status'] == 'processed'

def test_batch_process_excel_files(setup_test_environment, monkeypatch):
    """Test batch processing of multiple Excel files with validation against expected outputs"""
    test_env = setup_test_environment
    
    # Patch the registry path to use our temporary file
    monkeypatch.setattr('agroamigo_pipeline.utils.file_registry.PROCESSING_REGISTRY', 
                        test_env["registry_path"])
    
    # Initialize the processor
    processor = ExcelProcessor()
    
    # Process all files in the source directory
    results = processor.process_excel_files(
        source_dir=test_env["source_dir"],
        output_dir=test_env["output_dir"]
    )
    
    # Check processing results
    assert results['total_files'] == 2  # We have 3 Excel files in the source directory
    assert results['processed_files'] == 2
    assert results['failed_files'] == 0
    
    # Verify output files exist
    output_files = sorted(list(Path(test_env["output_dir"]).glob("*.csv")))
    assert len(output_files) == 2
    
    # List of expected output files
    expected_files = [
        "20210525-mayoristas_mayo_24_2021.csv",
        "20210525-mayoristas_mayo_25_2021.csv",
    ]
    
    # Compare each output file with its expected version
    for i, output_file in enumerate(output_files):
        expected_file = os.path.join(test_env["expected_dir"], expected_files[i])
        
        df_output = pd.read_csv(output_file)
        df_expected = pd.read_csv(expected_file)
        
        # Sort both dataframes to ensure consistent comparison
        df_output = df_output.sort_values(by=['product', 'city']).reset_index(drop=True)
        df_expected = df_expected.sort_values(by=['product', 'city']).reset_index(drop=True)
        
        # Check that dataframes match
        pd.testing.assert_frame_equal(df_output, df_expected)
    
    # Verify all files are in the registry
    registry = FileRegistry()
    excel_files = list(Path(test_env["source_dir"]).glob("*.xlsx"))
    for file_path in excel_files:
        assert registry.file_processed(str(file_path))
        
    # Test reprocessing with force flag
    force_results = processor.process_excel_files(
        source_dir=test_env["source_dir"],
        output_dir=test_env["output_dir"],
        force_reprocess=True
    )
    
    assert force_results['processed_files'] == 2  # All files should be reprocessed