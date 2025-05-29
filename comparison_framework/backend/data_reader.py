import pandas as pd
import numpy as np
import pyodbc
import requests
import json
import zipfile
from io import BytesIO
from sqlalchemy import create_engine
import logging
from .utils import check_file_size

logger = logging.getLogger(__name__)

def read_csv_dat(file, delimiter=',', chunksize=None):
    """
    Read CSV or DAT file into a pandas DataFrame.
    
    Args:
        file: File object or path
        delimiter: Column separator
        chunksize: Number of rows to read at a time for large files
    
    Returns:
        pandas DataFrame
    """
    try:
        # Check file size
        if check_file_size(file) > 3 * 1024 * 1024 * 1024:  # 3GB
            logger.info("Large file detected, using chunked reading")
            chunks = []
            for chunk in pd.read_csv(file, delimiter=delimiter, chunksize=500000):
                chunks.append(chunk)
            return pd.concat(chunks, ignore_index=True)
        else:
            return pd.read_csv(file, delimiter=delimiter)
    except Exception as e:
        logger.error(f"Error reading CSV/DAT file: {str(e)}")
        raise Exception(f"Failed to read file: {str(e)}")

def read_sql(server, database, username, password, query):
    """
    Read data from SQL Server using a query.
    
    Args:
        server: SQL Server hostname
        database: Database name
        username: SQL Server username
        password: SQL Server password
        query: SQL query to execute
    
    Returns:
        pandas DataFrame
    """
    try:
        connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
        engine = create_engine(connection_string)
        
        # Execute query in chunks for large datasets
        chunks = []
        for chunk in pd.read_sql(query, engine, chunksize=500000):
            chunks.append(chunk)
        
        return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
    except Exception as e:
        logger.error(f"Error reading from SQL Server: {str(e)}")
        raise Exception(f"Failed to read from SQL Server: {str(e)}")

def read_stored_proc(server, database, username, password, proc_name):
    """
    Execute a stored procedure and return results.
    
    Args:
        server: SQL Server hostname
        database: Database name
        username: SQL Server username
        password: SQL Server password
        proc_name: Name of the stored procedure
    
    Returns:
        pandas DataFrame
    """
    try:
        connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        
        # Execute stored procedure
        cursor.execute(f"EXEC {proc_name}")
        
        # Fetch results
        columns = [column[0] for column in cursor.description]
        results = []
        
        while True:
            rows = cursor.fetchmany(500000)  # Fetch in chunks
            if not rows:
                break
            results.extend(rows)
        
        return pd.DataFrame.from_records(results, columns=columns)
    except Exception as e:
        logger.error(f"Error executing stored procedure: {str(e)}")
        raise Exception(f"Failed to execute stored procedure: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()

def read_teradata(server, database, username, password, query):
    """
    Read data from Teradata using a query.
    
    Args:
        server: Teradata server hostname
        database: Database name
        username: Teradata username
        password: Teradata password
        query: SQL query to execute
    
    Returns:
        pandas DataFrame
    """
    try:
        connection_string = f"teradatasql://{username}:{password}@{server}/{database}"
        engine = create_engine(connection_string)
        
        # Execute query in chunks for large datasets
        chunks = []
        for chunk in pd.read_sql(query, engine, chunksize=500000):
            chunks.append(chunk)
        
        return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
    except Exception as e:
        logger.error(f"Error reading from Teradata: {str(e)}")
        raise Exception(f"Failed to read from Teradata: {str(e)}")

def read_api(url, method="GET", headers=None, body=None):
    """
    Read data from an API endpoint.
    
    Args:
        url: API endpoint URL
        method: HTTP method (GET or POST)
        headers: Request headers as dictionary
        body: Request body as dictionary
    
    Returns:
        pandas DataFrame
    """
    try:
        headers = json.loads(headers) if headers else {}
        body = json.loads(body) if body else {}
        
        if method.upper() == "GET":
            response = requests.get(url, headers=headers, params=body)
        else:
            response = requests.post(url, headers=headers, json=body)
        
        response.raise_for_status()
        data = response.json()
        
        # Handle different JSON structures
        if isinstance(data, list):
            return pd.DataFrame(data)
        elif isinstance(data, dict):
            # Try to find the data array in the response
            for key, value in data.items():
                if isinstance(value, list):
                    return pd.DataFrame(value)
            return pd.DataFrame([data])
        else:
            raise ValueError("Unexpected API response format")
    except Exception as e:
        logger.error(f"Error reading from API: {str(e)}")
        raise Exception(f"Failed to read from API: {str(e)}")

def read_parquet(file):
    """
    Read a Parquet file into a pandas DataFrame.
    
    Args:
        file: File object or path
    
    Returns:
        pandas DataFrame
    """
    try:
        return pd.read_parquet(file)
    except Exception as e:
        logger.error(f"Error reading Parquet file: {str(e)}")
        raise Exception(f"Failed to read Parquet file: {str(e)}")

def read_zipped_files(zip_file, delimiter=','):
    """
    Read flat files from a ZIP archive.
    
    Args:
        zip_file: ZIP file object or path
        delimiter: Column separator for flat files
    
    Returns:
        pandas DataFrame
    """
    try:
        with zipfile.ZipFile(zip_file) as z:
            # Get all file names in the ZIP
            flat_files = [f for f in z.namelist() if f.endswith(('.csv', '.dat', '.txt'))]
            
            if not flat_files:
                raise ValueError("No supported files found in ZIP archive")
            
            # Read and combine all files
            dfs = []
            for file in flat_files:
                with z.open(file) as f:
                    # Convert to BytesIO for pandas to read
                    buffer = BytesIO(f.read())
                    df = read_csv_dat(buffer, delimiter=delimiter)
                    dfs.append(df)
            
            return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    except Exception as e:
        logger.error(f"Error reading from ZIP archive: {str(e)}")
        raise Exception(f"Failed to read from ZIP archive: {str(e)}")
