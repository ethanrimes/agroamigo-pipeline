import os
import json
import time
from datetime import datetime
from pathlib import Path
from agroamigo_pipeline.config.settings import PROCESSING_REGISTRY

class FileRegistry:
    """
    Registry to track processed files and avoid duplicate processing.
    """
    def __init__(self, registry_path=PROCESSING_REGISTRY):
        self.registry_path = registry_path
        self._registry = self._load_registry()
    
    def _load_registry(self):
        """Load the registry from disk or create a new one."""
        if os.path.exists(self.registry_path):
            try:
                with open(self.registry_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except (json.JSONDecodeError, UnicodeDecodeError):
                # If registry is corrupted, create a new one
                return {'files': {}, 'last_updated': datetime.now().isoformat()}
        else:
            return {'files': {}, 'last_updated': datetime.now().isoformat()}
    
    def _save_registry(self):
        """Save the registry to disk."""
        self._registry['last_updated'] = datetime.now().isoformat()
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(self.registry_path), exist_ok=True)
        
        with open(self.registry_path, 'w', encoding='utf-8') as f:
            json.dump(self._registry, f, indent=2)
    
    def file_processed(self, file_path):
        """
        Check if a file has been processed before.
        
        Args:
            file_path (str): Path to the file to check
        
        Returns:
            bool: True if file has been processed, False otherwise
        """
        file_path = str(Path(file_path).resolve())
        return file_path in self._registry['files']
    
    def get_file_info(self, file_path):
        """
        Get information about a processed file.
        
        Args:
            file_path (str): Path to the file to check
        
        Returns:
            dict: File processing information or None if not found
        """
        file_path = str(Path(file_path).resolve())
        return self._registry['files'].get(file_path)
    
    def register_file(self, file_path, output_path=None, status="processed", error=None, stats=None):
        """
        Register a file as processed.
        
        Args:
            file_path (str): Path to the file being registered
            output_path (str): Path to the output file (if any)
            status (str): Processing status (processed, error, etc.)
            error (str): Error message if processing failed
            stats (dict): Processing statistics
        """
        file_path = str(Path(file_path).resolve())
        
        # Create or update file entry
        if file_path not in self._registry['files']:
            self._registry['files'][file_path] = {
                'first_processed': datetime.now().isoformat(),
                'processing_history': []
            }
        
        # Add processing record
        self._registry['files'][file_path]['processing_history'].append({
            'timestamp': datetime.now().isoformat(),
            'status': status,
            'output_path': output_path,
            'error': error,
            'stats': stats
        })
        
        # Update last processed timestamp
        self._registry['files'][file_path]['last_processed'] = datetime.now().isoformat()
        
        # Save the updated registry
        self._save_registry()
    
    def get_all_files(self, status=None):
        """
        Get all registered files, optionally filtered by status.
        
        Args:
            status (str): Filter by processing status
        
        Returns:
            dict: Dictionary of file paths and their info
        """
        if status is None:
            return self._registry['files']
        
        return {
            path: info for path, info in self._registry['files'].items()
            if info['processing_history'][-1]['status'] == status
        }