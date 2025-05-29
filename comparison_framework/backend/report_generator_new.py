def generate_ydata_profile(source_df: pd.DataFrame, target_df: pd.DataFrame, 
                         mapping_df: pd.DataFrame) -> Tuple[BytesIO, BytesIO, BytesIO]:
    """
    Generate Y-Data Profiling reports including individual profiles and comparison.
    
    Args:
        source_df: Source DataFrame
        target_df: Target DataFrame
        mapping_df: DataFrame containing column mapping information
    
    Returns:
        Tuple of (source_profile, target_profile, comparison_profile) as BytesIO objects
    """
    try:
        # Create mapping dictionary from mapping_df
        column_mapping = dict(zip(
            mapping_df['Source Column'],
            mapping_df['Target Column']
        ))
        
        # Filter out unmapped and excluded columns
        excluded_columns = mapping_df[mapping_df['Exclude from Comparison']]['Source Column'].tolist()
        valid_columns = {
            src: tgt for src, tgt in column_mapping.items()
            if tgt and not pd.isna(tgt) and src not in excluded_columns
        }
        
        # Prepare DataFrames for comparison
        source_cols = list(valid_columns.keys())
        target_cols = [valid_columns[src] for src in source_cols]
        
        source_compare = source_df[source_cols].copy()
        target_compare = target_df[target_cols].copy()
        
        # Rename target columns to match source columns for comparison
        target_compare.columns = source_cols
        
        # Convert problematic data types to string
        for col in source_compare.columns:
            if source_compare[col].dtype.name not in ['int64', 'float64', 'bool', 'datetime64[ns]', 'object']:
                source_compare[col] = source_compare[col].astype(str)
            if target_compare[col].dtype.name not in ['int64', 'float64', 'bool', 'datetime64[ns]', 'object']:
                target_compare[col] = target_compare[col].astype(str)
        
        # Handle null values
        source_compare = source_compare.fillna(pd.NA)
        target_compare = target_compare.fillna(pd.NA)
        
        try:
            # Generate individual profiles
            source_profile = ProfileReport(
                source_compare,
                title="Source Data Profile",
                minimal=True,
                explorative=True
            )
            target_profile = ProfileReport(
                target_compare,
                title="Target Data Profile",
                minimal=True,
                explorative=True
            )
            
            # Generate comparison profile
            comparison_profile = source_profile.compare(target_profile)
            
            # Save to BytesIO objects
            source_output = BytesIO()
            target_output = BytesIO()
            comparison_output = BytesIO()
            
            source_profile.to_file(source_output)
            target_profile.to_file(target_output)
            comparison_profile.to_file(comparison_output)
            
            source_output.seek(0)
            target_output.seek(0)
            comparison_output.seek(0)
            
            return source_output, target_output, comparison_output
            
        except Exception as e:
            logger.error(f"Error in profile generation: {str(e)}")
            # Fallback to basic HTML reports
            source_output = BytesIO()
            target_output = BytesIO()
            comparison_output = BytesIO()
            
            source_report = f"""
            <html><head><title>Source Data Profile</title></head>
            <body><h1>Source Data Profile</h1>{source_compare.describe().to_html()}</body></html>
            """
            target_report = f"""
            <html><head><title>Target Data Profile</title></head>
            <body><h1>Target Data Profile</h1>{target_compare.describe().to_html()}</body></html>
            """
            comparison_report = f"""
            <html>
            <head><title>Data Comparison Report</title></head>
            <body>
            <h1>Data Comparison Report</h1>
            <h2>Source Data Summary</h2>{source_compare.describe().to_html()}
            <h2>Target Data Summary</h2>{target_compare.describe().to_html()}
            </body></html>
            """
            
            source_output.write(source_report.encode('utf-8'))
            target_output.write(target_report.encode('utf-8'))
            comparison_output.write(comparison_report.encode('utf-8'))
            
            source_output.seek(0)
            target_output.seek(0)
            comparison_output.seek(0)
            
            return source_output, target_output, comparison_output
    
    except Exception as e:
        logger.error(f"Error generating Y-Data profile: {str(e)}")
        raise Exception(f"Failed to generate Y-Data profile: {str(e)}")
