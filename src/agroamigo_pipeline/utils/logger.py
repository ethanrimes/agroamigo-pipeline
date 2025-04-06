import logging
import os
from logging.handlers import RotatingFileHandler

from agroamigo_pipeline.config.settings import (
    LOG_DIR, LOG_FORMAT, LOG_DATE_FORMAT, LOG_LEVEL,
    SCRAPER_LOG_DIR, EXTRACTOR_LOG_DIR, EXCEL_LOG_DIR, 
    PDF_LOG_DIR, ERROR_LOG_DIR, AGGREGATOR_LOG_DIR
)

def _create_component_logger(component_name, log_level):
    """Create a logger for a specific component."""
    # Map component names to log directories and proper logger names
    dir_map = {
        'scraper': {'dir': SCRAPER_LOG_DIR, 'name': 'scraper'},
        'file_extractor': {'dir': EXTRACTOR_LOG_DIR, 'name': 'file_extractor'},
        'excel_processor': {'dir': EXCEL_LOG_DIR, 'name': 'excel_processor'},
        'pdf_processor': {'dir': PDF_LOG_DIR, 'name': 'pdf_processor'},
        'error_handler': {'dir': ERROR_LOG_DIR, 'name': 'error_handler'},
        'data_aggregator': {'dir': AGGREGATOR_LOG_DIR, 'name': 'data_aggregator'}
    }
    
    # Get directory and ensure it exists
    log_info = dir_map.get(component_name, {'dir': LOG_DIR, 'name': component_name})
    log_dir = log_info['dir']
    logger_name = log_info['name']
    
    os.makedirs(log_dir, exist_ok=True)
    
    # Create logger with the proper unique name
    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)
    logger.propagate = False  # Don't propagate to root logger
    
    # Remove existing handlers if any
    if logger.handlers:
        logger.handlers.clear()
    
    # Create file handler with NO rotation, and overwrite mode
    log_file = os.path.join(log_dir, f"{logger_name}.log")
    file_handler = logging.FileHandler(
        log_file, mode='w', encoding='utf-8'  # 'w' mode for overwriting instead of appending
    )
    file_handler.setLevel(log_level)
    file_formatter = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT)
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(max(logging.INFO, log_level))  # Console shows INFO or higher
    console_formatter = logging.Formatter('%(levelname)s - %(name)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    return logger

def setup_logging(log_level=None):
    """Set up logging for all components with consistent format."""
    # Use provided log_level or default from config
    log_level = log_level or getattr(logging, LOG_LEVEL)
    
    # Create and configure the root logger
    logging.basicConfig(
        level=log_level,
        format=LOG_FORMAT,
        datefmt=LOG_DATE_FORMAT
    )
    
    # Create component-specific loggers
    loggers = {
        'scraper': _create_component_logger('scraper', log_level),
        'file_extractor': _create_component_logger('file_extractor', log_level),
        'excel_processor': _create_component_logger('excel_processor', log_level),
        'pdf_processor': _create_component_logger('pdf_processor', log_level),
        'error_handler': _create_component_logger('error_handler', log_level),
        'data_aggregator': _create_component_logger('data_aggregator', log_level)
    }
    
    # Log the initialization
    root_logger = logging.getLogger()
    root_logger.info(f"Logging initialized at level {logging.getLevelName(log_level)}")
    
    # Log debug-level information about the setup
    if log_level <= logging.DEBUG:
        root_logger.debug(f"Log directories configured:")
        for component, logger in loggers.items():
            handlers = [h for h in logger.handlers if isinstance(h, logging.FileHandler)]
            if handlers:
                root_logger.debug(f"  {component}: {handlers[0].baseFilename}")
    
    return loggers

def update_logger_level(logger, log_level):
    """Update an existing logger's level and its handlers' levels."""
    if not logger:
        return
        
    # Update logger level
    logger.setLevel(log_level)
    
    # Update handler levels
    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler):
            handler.setLevel(log_level)
        elif isinstance(handler, logging.StreamHandler):
            # For console handlers, respect minimum INFO level for readability
            handler.setLevel(max(logging.INFO, log_level))
    
    logger.debug(f"Logger level updated to {logging.getLevelName(log_level)}")

def log_function_call(logger, func_name, args=None, kwargs=None):
    """Log function calls with parameters at DEBUG level."""
    if logger.level <= logging.DEBUG:
        args_str = str(args) if args else ""
        kwargs_str = str(kwargs) if kwargs else ""
        logger.debug(f"Calling {func_name}({args_str}{', ' if args and kwargs else ''}{kwargs_str})")

def log_dataframe_info(logger, df, df_name):
    """Log information about a pandas DataFrame at DEBUG level."""
    if logger.level <= logging.DEBUG:
        logger.debug(f"DataFrame {df_name}: shape={df.shape}, columns={list(df.columns)}")
        if not df.empty:
            logger.debug(f"{df_name} sample:\n{df.head(3)}")

def log_dict_info(logger, data, dict_name):
    """Log information about a dictionary at DEBUG level."""
    if logger.level <= logging.DEBUG:
        logger.debug(f"Dictionary {dict_name}: {len(data)} items")
        if data:
            sample = {k: data[k] for k in list(data.keys())[:3]} if len(data) > 3 else data
            logger.debug(f"{dict_name} sample: {sample}")