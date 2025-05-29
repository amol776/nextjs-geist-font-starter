import logging
import os
from typing import Union, BinaryIO
from io import BytesIO
import pandas as pd
import streamlit as st
from datetime import datetime

def setup_logger() -> logging.Logger:
    """
    Set up and configure the application logger.
    
    Returns:
        logging.Logger: Configured logger instance
    """
    # Create logger
    logger = logging.getLogger('comparison_framework')
    logger.setLevel(logging.INFO)
    
    # Create handlers
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # Create formatters and add it to handlers
    log_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(log_format)
    
    # Add handlers to the logger
    logger.addHandler(console_handler)
    
    # Avoid duplicate logging
    logger.propagate = False
    
    return logger

def check_file_size(file: Union[str, BytesIO, BinaryIO]) -> int:
    """
    Check the size of a file in bytes.
    
    Args:
        file: File path or file-like object
    
    Returns:
        int: Size of the file in bytes
    """
    try:
        if isinstance(file, str):
            # If file is a path
            return os.path.getsize(file)
        elif isinstance(file, (BytesIO, BinaryIO)):
            # If file is a file-like object
            current_pos = file.tell()
            file.seek(0, os.SEEK_END)
            size = file.tell()
            file.seek(current_pos)
            return size
        else:
            raise ValueError("Unsupported file type")
    
    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.error(f"Error checking file size: {str(e)}")
        raise Exception(f"Failed to check file size: {str(e)}")

def format_file_size(size_bytes: int) -> str:
    """
    Format file size in human-readable format.
    
    Args:
        size_bytes: Size in bytes
    
    Returns:
        str: Formatted size string (e.g., "1.23 GB")
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"

def get_file_info(file: Union[str, BytesIO, BinaryIO]) -> dict:
    """
    Get information about a file.
    
    Args:
        file: File path or file-like object
    
    Returns:
        dict: Dictionary containing file information
    """
    try:
        size_bytes = check_file_size(file)
        
        info = {
            'size': size_bytes,
            'size_formatted': format_file_size(size_bytes),
            'is_large': size_bytes > 3 * 1024 * 1024 * 1024  # > 3GB
        }
        
        if isinstance(file, str):
            info.update({
                'name': os.path.basename(file),
                'path': os.path.abspath(file),
                'extension': os.path.splitext(file)[1].lower()
            })
        
        return info
    
    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.error(f"Error getting file info: {str(e)}")
        raise Exception(f"Failed to get file info: {str(e)}")

def create_download_link(data: bytes, filename: str) -> str:
    """
    Create a download link for a file.
    
    Args:
        data: File data as bytes
        filename: Name for the downloaded file
    
    Returns:
        str: HTML string containing the download link
    """
    try:
        b64 = pd.util.testing.base64.b64encode(data).decode()
        return f'<a href="data:application/octet-stream;base64,{b64}" download="{filename}">Download {filename}</a>'
    
    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.error(f"Error creating download link: {str(e)}")
        raise Exception(f"Failed to create download link: {str(e)}")

def show_progress_bar(iterable, message: str):
    """
    Display a progress bar for an iterable.
    
    Args:
        iterable: Iterable to track progress for
        message: Message to display with the progress bar
    
    Returns:
        Iterator that displays progress
    """
    try:
        # Get the total length if possible
        try:
            total = len(iterable)
        except TypeError:
            total = None
        
        # Create progress bar
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        for i, item in enumerate(iterable):
            # Update progress
            if total is not None:
                progress = (i + 1) / total
                progress_bar.progress(progress)
                status_text.text(f"{message} ({i + 1}/{total})")
            else:
                status_text.text(f"{message} (Item {i + 1})")
            
            yield item
        
        # Complete the progress bar
        progress_bar.progress(1.0)
        status_text.text(f"{message} (Completed)")
    
    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.error(f"Error in progress bar: {str(e)}")
        raise Exception(f"Failed to show progress: {str(e)}")

def generate_timestamp() -> str:
    """
    Generate a timestamp string for file naming.
    
    Returns:
        str: Formatted timestamp
    """
    return datetime.now().strftime('%Y%m%d_%H%M%S')

def validate_dataframe(df: pd.DataFrame, name: str = "DataFrame") -> None:
    """
    Validate a pandas DataFrame for basic requirements.
    
    Args:
        df: DataFrame to validate
        name: Name of the DataFrame for error messages
    
    Raises:
        ValueError: If validation fails
    """
    if df is None:
        raise ValueError(f"{name} is None")
    
    if len(df) == 0:
        raise ValueError(f"{name} is empty")
    
    if len(df.columns) == 0:
        raise ValueError(f"{name} has no columns")

def handle_error(error: Exception, user_message: str = None) -> None:
    """
    Handle an error by logging it and displaying a user-friendly message.
    
    Args:
        error: The exception that occurred
        user_message: Optional custom message to display to the user
    """
    logger = logging.getLogger(__name__)
    logger.error(f"Error occurred: {str(error)}")
    
    if user_message:
        st.error(user_message)
    else:
        st.error(f"An error occurred: {str(error)}")

def cleanup_temp_files(file_paths: list) -> None:
    """
    Clean up temporary files.
    
    Args:
        file_paths: List of file paths to delete
    """
    for file_path in file_paths:
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to delete temporary file {file_path}: {str(e)}")

class Timer:
    """Context manager for timing operations."""
    
    def __init__(self, operation_name: str):
        self.operation_name = operation_name
        self.logger = logging.getLogger(__name__)
    
    def __enter__(self):
        self.start_time = datetime.now()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        end_time = datetime.now()
        duration = end_time - self.start_time
        self.logger.info(f"{self.operation_name} completed in {duration}")
