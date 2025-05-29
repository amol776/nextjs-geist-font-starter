import pandas as pd
import numpy as np
from difflib import SequenceMatcher
import logging
from typing import Dict, List, Tuple

logger = logging.getLogger(__name__)

def string_similarity(a: str, b: str) -> float:
    """
    Calculate similarity ratio between two strings.
    
    Args:
        a: First string
        b: Second string
    
    Returns:
        float: Similarity ratio between 0 and 1
    """
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

def auto_map_columns(source_cols: List[str], target_cols: List[str], threshold: float = 0.8) -> Dict[str, str]:
    """
    Automatically map columns between source and target based on name similarity.
    
    Args:
        source_cols: List of source column names
        target_cols: List of target column names
        threshold: Minimum similarity threshold for automatic mapping
    
    Returns:
        Dict mapping source columns to target columns
    """
    try:
        # Convert source_cols and target_cols to lists if they're not already
        source_cols = list(source_cols)
        target_cols = list(target_cols)
        
        # Initialize mapping dictionary and used targets set
        mapping = {}
        used_targets = set()
        
        # First pass: exact matches (case-insensitive)
        for src_col in source_cols:
            src_col_str = str(src_col)  # Convert to string to handle non-string column names
            for tgt_col in target_cols:
                tgt_col_str = str(tgt_col)  # Convert to string to handle non-string column names
                if src_col_str.lower() == tgt_col_str.lower() and tgt_col not in used_targets:
                    mapping[src_col] = tgt_col
                    used_targets.add(tgt_col)
                    break
        
        # Second pass: fuzzy matching for remaining columns
        unmapped_sources = [col for col in source_cols if col not in mapping]
        for src_col in unmapped_sources:
            src_col_str = str(src_col)  # Convert to string for fuzzy matching
            best_match = None
            best_score = threshold
            
            for tgt_col in target_cols:
                if tgt_col not in used_targets:
                    tgt_col_str = str(tgt_col)  # Convert to string for fuzzy matching
                    score = string_similarity(src_col_str, tgt_col_str)
                    if score > best_score:
                        best_score = score
                        best_match = tgt_col
            
            if best_match:
                mapping[src_col] = best_match
                used_targets.add(best_match)
            else:
                # If no match found, map to None (will be shown as empty in UI)
                mapping[src_col] = None
        
        logger.info(f"Auto-mapped {len(mapping)} columns out of {len(source_cols)} source columns")
        return mapping
    
    except Exception as e:
        logger.error(f"Error in auto_map_columns: {str(e)}")
        raise Exception(f"Failed to auto-map columns: {str(e)}")

def validate_join_columns(mapping: Dict[str, str], join_columns: List[str], 
                         source_df: pd.DataFrame, target_df: pd.DataFrame) -> Tuple[bool, str]:
    """
    Validate that selected join columns are suitable for comparison.
    
    Args:
        mapping: Dictionary mapping source columns to target columns
        join_columns: List of source column names selected for joining
        source_df: Source DataFrame
        target_df: Target DataFrame
    
    Returns:
        Tuple of (is_valid, error_message)
    """
    try:
        if not join_columns:
            return False, "No join columns selected"
        
        # Check if all join columns exist in mapping
        for src_col in join_columns:
            if src_col not in mapping:
                return False, f"Join column '{src_col}' not found in column mapping"
        
        # Get corresponding target columns
        target_join_cols = [mapping[src_col] for src_col in join_columns]
        
        # Check for null values in join columns
        for src_col in join_columns:
            if source_df[src_col].isnull().any():
                return False, f"Source join column '{src_col}' contains null values"
        
        for tgt_col in target_join_cols:
            if target_df[tgt_col].isnull().any():
                return False, f"Target join column '{tgt_col}' contains null values"
        
        # Check for duplicates in join columns combination
        src_duplicates = source_df.duplicated(subset=join_columns).any()
        tgt_duplicates = target_df.duplicated(subset=target_join_cols).any()
        
        if src_duplicates or tgt_duplicates:
            return False, "Selected join columns contain duplicate combinations"
        
        return True, "Join columns validated successfully"
    
    except Exception as e:
        logger.error(f"Error in validate_join_columns: {str(e)}")
        raise Exception(f"Failed to validate join columns: {str(e)}")

def apply_column_mapping(df: pd.DataFrame, mapping: Dict[str, str], 
                        is_source: bool = True) -> pd.DataFrame:
    """
    Apply column mapping to DataFrame by renaming columns.
    
    Args:
        df: DataFrame to apply mapping to
        mapping: Dictionary mapping source columns to target columns
        is_source: Whether this is the source DataFrame (True) or target DataFrame (False)
    
    Returns:
        DataFrame with renamed columns
    """
    try:
        if is_source:
            # For source DataFrame, use mapping keys as current names and values as new names
            rename_dict = mapping
        else:
            # For target DataFrame, create inverse mapping
            rename_dict = {v: k for k, v in mapping.items()}
        
        # Only rename columns that exist in the DataFrame
        existing_cols = [col for col in rename_dict.keys() if col in df.columns]
        rename_dict = {k: rename_dict[k] for k in existing_cols}
        
        return df.rename(columns=rename_dict)
    
    except Exception as e:
        logger.error(f"Error in apply_column_mapping: {str(e)}")
        raise Exception(f"Failed to apply column mapping: {str(e)}")

def get_excluded_columns(mapping_df: pd.DataFrame) -> List[str]:
    """
    Get list of columns that should be excluded from comparison.
    
    Args:
        mapping_df: DataFrame containing mapping information with 'Exclude from Comparison' column
    
    Returns:
        List of source column names to exclude
    """
    try:
        excluded = mapping_df[mapping_df['Exclude from Comparison']]['Source Column'].tolist()
        logger.info(f"Excluding {len(excluded)} columns from comparison")
        return excluded
    
    except Exception as e:
        logger.error(f"Error in get_excluded_columns: {str(e)}")
        raise Exception(f"Failed to get excluded columns: {str(e)}")

def validate_mapping(source_df: pd.DataFrame, target_df: pd.DataFrame, 
                    mapping: Dict[str, str], type_mapping: Dict[str, str] = None) -> Tuple[bool, str]:
    """
    Validate that the column mapping is complete and correct.
    
    Args:
        source_df: Source DataFrame
        target_df: Target DataFrame
        mapping: Dictionary mapping source columns to target columns
        type_mapping: Optional dictionary mapping columns to desired data types
    
    Returns:
        Tuple of (is_valid, error_message)
    """
    try:
        # Remove None or empty string mappings
        valid_mapping = {k: v for k, v in mapping.items() if v and not pd.isna(v)}
        
        # Check if any columns are mapped
        if not valid_mapping:
            return False, "No valid column mappings found"
        
        # Check if mapped target columns exist
        missing_target = [col for col in valid_mapping.values() 
                         if col not in target_df.columns]
        if missing_target:
            return False, f"Target columns not found: {', '.join(missing_target)}"
        
        # Validate data types compatibility
        for src_col, tgt_col in valid_mapping.items():
            src_dtype = str(source_df[src_col].dtype)
            tgt_dtype = str(target_df[tgt_col].dtype)
            
            # If type_mapping is provided, use it to check compatibility
            if type_mapping and src_col in type_mapping:
                desired_type = type_mapping[src_col]
                try:
                    # Try converting a sample of the data to the desired type
                    sample = source_df[src_col].head(100)
                    if desired_type == 'int64':
                        pd.to_numeric(sample, downcast='integer')
                    elif desired_type == 'float64':
                        pd.to_numeric(sample, downcast='float')
                    elif desired_type == 'datetime64[ns]':
                        pd.to_datetime(sample)
                    elif desired_type == 'bool':
                        sample.astype(bool)
                    # string type conversion is always possible
                except Exception as e:
                    return False, f"Cannot convert {src_col} to type {desired_type}: {str(e)}"
            else:
                # Without type_mapping, check basic compatibility
                if not are_dtypes_compatible(src_dtype, tgt_dtype):
                    return False, f"Incompatible data types for {src_col} ({src_dtype}) and {tgt_col} ({tgt_dtype})"
        
        return True, "Mapping validated successfully"
    
    except Exception as e:
        logger.error(f"Error in validate_mapping: {str(e)}")
        raise Exception(f"Failed to validate mapping: {str(e)}")

def are_dtypes_compatible(dtype1: str, dtype2: str) -> bool:
    """
    Check if two data types are compatible for comparison.
    
    Args:
        dtype1: First data type as string
        dtype2: Second data type as string
    
    Returns:
        bool indicating whether the types are compatible
    """
    # Define groups of compatible types
    type_groups = {
        'numeric': {'int32', 'int64', 'float32', 'float64', 'int', 'float',
                   'integer', 'numeric', 'decimal', 'double', 'real'},
        'string': {'object', 'string', 'str', 'varchar', 'nvarchar', 'char', 
                  'nchar', 'text', 'ntext'},
        'datetime': {'datetime64', 'datetime64[ns]', 'datetime', 'timestamp', 'date'},
        'boolean': {'bool', 'boolean', 'bit'}
    }
    
    # Convert type strings to lowercase for comparison
    dtype1 = dtype1.lower()
    dtype2 = dtype2.lower()
    
    # Check if types are in the same group
    for group_types in type_groups.values():
        if any(t in dtype1 for t in group_types) and any(t in dtype2 for t in group_types):
            return True
    
    # Special cases
    if ('int' in dtype1 or 'float' in dtype1) and ('int' in dtype2 or 'float' in dtype2):
        return True
    
    # String types can accept any type
    if any(t in dtype1 for t in type_groups['string']) or any(t in dtype2 for t in type_groups['string']):
        return True
    
    # Check for exact match
    return dtype1 == dtype2
