import os
import shutil
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def ensure_directory(directory_path):
    """
    Ensure a directory exists, creating it if necessary.
    
    Args:
        directory_path (str): Path to the directory
        
    Returns:
        str: Path to the directory
    """
    os.makedirs(directory_path, exist_ok=True)
    return directory_path

def get_files_by_extension(directory_path, extensions, recursive=False):
    """
    Get all files with specified extensions in a directory.
    
    Args:
        directory_path (str): Path to search
        extensions (list): List of file extensions to include
        recursive (bool): Whether to search recursively
        
    Returns:
        list: List of file paths
    """
    if not os.path.exists(directory_path):
        logger.warning(f"Directory does not exist: {directory_path}")
        return []
    
    extensions = [ext.lower() for ext in extensions]
    files = []
    
    if recursive:
        for root, _, filenames in os.walk(directory_path):
            for filename in filenames:
                if any(filename.lower().endswith(ext) for ext in extensions):
                    files.append(os.path.join(root, filename))
    else:
        for filename in os.listdir(directory_path):
            if os.path.isfile(os.path.join(directory_path, filename)) and any(filename.lower().endswith(ext) for ext in extensions):
                files.append(os.path.join(directory_path, filename))
    
    return files

def clean_filename(filename):
    """
    Clean a filename to be safe for all operating systems.
    
    Args:
        filename (str): Original filename
        
    Returns:
        str: Cleaned filename
    """
    # Replace characters that are problematic in filenames
    for char in ['<', '>', ':', '"', '/', '\\', '|', '?', '*']:
        filename = filename.replace(char, '_')
    
    # Ensure the filename isn't too long
    if len(filename) > 255:
        base, ext = os.path.splitext(filename)
        filename = base[:255-len(ext)] + ext
        
    return filename

def generate_output_path(input_path, output_dir, extension='.csv'):
    """
    Generate an output file path based on an input file path.
    
    Args:
        input_path (str): Input file path
        output_dir (str): Output directory
        extension (str): File extension for output file
        
    Returns:
        str: Output file path
    """
    # Ensure output directory exists
    ensure_directory(output_dir)
    
    # Get base filename without extension
    base_filename = os.path.splitext(os.path.basename(input_path))[0]
    
    # Clean the filename
    base_filename = clean_filename(base_filename)
    
    # Create output path
    output_path = os.path.join(output_dir, f"{base_filename}{extension}")
    
    # Handle file already exists
    counter = 1
    while os.path.exists(output_path):
        output_path = os.path.join(output_dir, f"{base_filename}_{counter}{extension}")
        counter += 1
    
    return output_path

def safe_copy_file(src_path, dest_path, overwrite=False):
    """
    Safely copy a file, creating the directory structure if needed.
    
    Args:
        src_path (str): Source file path
        dest_path (str): Destination file path
        overwrite (bool): Whether to overwrite if the file exists
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Check if source exists
        if not os.path.exists(src_path):
            logger.error(f"Source file does not exist: {src_path}")
            return False
        
        # Check if destination exists
        if os.path.exists(dest_path) and not overwrite:
            logger.warning(f"Destination file already exists and overwrite is False: {dest_path}")
            return False
        
        # Ensure destination directory exists
        ensure_directory(os.path.dirname(dest_path))
        
        # Copy the file
        shutil.copy2(src_path, dest_path)
        return True
        
    except Exception as e:
        logger.error(f"Error copying file from {src_path} to {dest_path}: {str(e)}")
        return False