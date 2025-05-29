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
        mapping = {}
        used_targets = set()
        
        # First pass: exact matches (case-insensitive)
        for src_col in source_cols:
            for tgt_col in target_cols:
                if src_col.lower() == tgt_col.lower() and tgt_col not in used_targets:
                    mapping[src_col] = tgt_col
                    used_targets.add(tgt_col)
                    break
        
        # Second pass: fuzzy matching for remaining columns
        unmapped_sources = [col for col in source_cols if col not in mapping]
        for src_col in unmapped_sources:
            best_match = None
            best_score = threshold
            
            for tgt_col in target_cols:
                if tgt_col not in used_targets:
                    score = string_similarity(src_col, tgt_col)
                    if score > best_score:
                        best_score = score
                        best_match = tgt_col
            
            if best_match:
                mapping[src_col] = best_match
                used_targets.add(best_match)
        
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
                    mapping: Dict[str, str]) -> Tuple[bool, str]:
    """
    Validate that the column mapping is complete and correct.
    
    Args:
        source_df: Source DataFrame
        target_df: Target DataFrame
        mapping: Dictionary mapping source columns to target columns
    
    Returns:
        Tuple of (is_valid, error_message)
    """
    try:
        # Check if all source columns are mapped
        unmapped_source = [col for col in source_df.columns if col not in mapping]
        if unmapped_source:
            return False, f"Source columns not mapped: {', '.join(unmapped_source)}"
        
        # Check if all target columns in mapping exist
        missing_target = [col for col in mapping.values() 
                         if col not in target_df.columns]
        if missing_target:
            return False, f"Target columns not found: {', '.join(missing_target)}"
        
        # Validate data types compatibility
        for src_col, tgt_col in mapping.items():
            src_dtype = source_df[src_col].dtype
            tgt_dtype = target_df[tgt_col].dtype
            
            if not are_dtypes_compatible(src_dtype, tgt_dtype):
                return False, f"Incompatible data types for {src_col} ({src_dtype}) and {tgt_col} ({tgt_dtype})"
        
        return True, "Mapping validated successfully"
    
    except Exception as e:
        logger.error(f"Error in validate_mapping: {str(e)}")
        raise Exception(f"Failed to validate mapping: {str(e)}")

def are_dtypes_compatible(dtype1: np.dtype, dtype2: np.dtype) -> bool:
    """
    Check if two data types are compatible for comparison.
    
    Args:
        dtype1: First data type
        dtype2: Second data type
    
    Returns:
        bool indicating whether the types are compatible
    """
    # Convert dtypes to strings for easier comparison
    dtype1_str = str(dtype1)
    dtype2_str = str(dtype2)
    
    # Define groups of compatible types
    numeric_types = {'int32', 'int64', 'float32', 'float64'}
    string_types = {'object', 'string'}
    datetime_types = {'datetime64', 'datetime64[ns]'}
    
    # Check if both types belong to the same group
    if dtype1_str in numeric_types and dtype2_str in numeric_types:
        return True
    if dtype1_str in string_types and dtype2_str in string_types:
        return True
    if dtype1_str in datetime_types and dtype2_str in datetime_types:
        return True
    
    # Check for exact match
    return dtype1_str == dtype2_str
