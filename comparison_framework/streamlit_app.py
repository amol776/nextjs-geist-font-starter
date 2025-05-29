import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, date, timezone
import os
from typing import Dict, List, Optional, Union, Any
import logging
from backend.data_reader import (
    read_csv_dat,
    read_sql,
    read_stored_proc,
    read_teradata,
    read_api,
    read_parquet,
    read_zipped_files
)
from backend.mapping_utils import (
    auto_map_columns,
    validate_join_columns,
    validate_mapping,
    apply_column_mapping,
    get_excluded_columns
)
from backend.report_generator import (
    generate_datacompy_report,
    generate_ydata_profile,
    generate_regression_report,
    generate_difference_report,
    create_consolidated_report
)
from backend.utils import check_file_size, setup_logger

# Set page config
st.set_page_config(
    page_title="Data Comparison Framework",
    page_icon="📊",
    layout="wide"
)

# Set up logging
logger = logging.getLogger(__name__)
if not logger.handlers:
    logger = setup_logger()

# Define the type mapping dictionary
TYPE_MAPPING = {
    # Integer types
    'int': 'int64',
    'int32': 'int64',
    'int64': 'int64',
    'integer': 'int64',
    'numeric': 'int64',
    'bigint': 'int64',
    'smallint': 'int64',
    'tinyint': 'int64',
    
    # Float types
    'Float': 'float64',
    'float': 'float64',
    'float32': 'float64',
    'float64': 'float64',
    'decimal': 'float64',
    'double': 'float64',
    'real': 'float64',
    
    # String types
    'varchar': 'string',
    'nvarchar': 'string',
    'char': 'string',
    'nchar': 'string',
    'text': 'string',
    'ntext': 'string',
    'string': 'string',
    'object': 'string',
    
    # Date/Time types
    'date': 'datetime64[ns]',
    'datetime': 'datetime64[ns]',
    'datetime64': 'datetime64[ns]',
    'timestamp': 'datetime64[ns]',
    
    # Boolean types
    'Boolean': 'bool',
    'bool': 'bool',
    'bit': 'bool',
    
    # Default type
    'unknown': 'string'
}

def main():
    # Add header with image
    st.image("https://images.pexels.com/photos/414612/pexels-photo-414612.jpeg", 
             caption="Data Comparison Dashboard", 
             use_column_width=True)
    
    st.title("Data Comparison Framework")
    
    # Create sidebar for source and target selection
    with st.sidebar:
        st.header("Configuration")
        
        # Source selection
        st.subheader("Source Configuration")
        source_type = st.selectbox(
            "Select Source Type",
            ["CSV file", "DAT file", "SQL Server", "Stored Procs", 
             "Teradata", "API", "Parquet file", "Zipped Flat files"],
            key="source_type"
        )
        
        # Target selection
        st.subheader("Target Configuration")
        target_type = st.selectbox(
            "Select Target Type",
            ["CSV file", "DAT file", "SQL Server", "Stored Procs", 
             "Teradata", "API", "Parquet file", "Zipped Flat files"],
            key="target_type"
        )
        
        # Delimiter selection for file types
        if source_type in ["CSV file", "DAT file", "Zipped Flat files"]:
            source_delimiter = st.text_input("Source Delimiter", value=",", key="source_delimiter")
        if target_type in ["CSV file", "DAT file", "Zipped Flat files"]:
            target_delimiter = st.text_input("Target Delimiter", value=",", key="target_delimiter")

    # Main panel
    col1, col2 = st.columns(2)
    
    # Source data input
    with col1:
        st.header("Source Data")
        source_data = handle_data_input("source", source_type)
    
    # Target data input
    with col2:
        st.header("Target Data")
        target_data = handle_data_input("target", target_type)
    
    # If both source and target data are loaded
    if source_data is not None and target_data is not None:
        # Auto-map columns
        column_mapping = auto_map_columns(source_data.columns, target_data.columns)
        
        st.header("Column Mapping")
        
        # Create initial mapping DataFrame
        mapping_df = pd.DataFrame({
            'Source Column': list(column_mapping.keys()),
            'Target Column': list(column_mapping.values()),
            'Exclude from Comparison': [False] * len(column_mapping)
        })
        
        # Allow manual editing of mappings
        st.write("You can edit the mappings below. Select target columns from the dropdown and check boxes to exclude columns from comparison.")
        
        # Create a selection widget for each source column
        edited_mappings = []
        for idx, row in mapping_df.iterrows():
            col1, col2, col3 = st.columns([2, 2, 1])
            
            with col1:
                st.text(row['Source Column'])  # Source column (read-only)
            
            with col2:
                # Dropdown for target column selection
                selected_target = st.selectbox(
                    f"Map to target column",
                    options=[''] + list(target_data.columns),
                    index=0 if row['Target Column'] not in target_data.columns 
                          else list(target_data.columns).index(row['Target Column']) + 1,
                    key=f"target_col_{idx}",
                    help="Select the corresponding target column"
                )
            
            with col3:
                # Checkbox for exclusion
                exclude = st.checkbox(
                    "Exclude",
                    value=row['Exclude from Comparison'],
                    key=f"exclude_{idx}",
                    help="Check to exclude this column from comparison"
                )
            
            edited_mappings.append({
                'Source Column': row['Source Column'],
                'Target Column': selected_target if selected_target else None,
                'Exclude from Comparison': exclude
            })
        
        # Convert edited mappings to DataFrame
        edited_mapping = pd.DataFrame(edited_mappings)
        
        # Create valid mappings dictionary (excluding None/empty values and excluded columns)
        valid_mappings = {
            row['Source Column']: row['Target Column']
            for _, row in edited_mapping.iterrows()
            if row['Target Column'] and not pd.isna(row['Target Column']) and not row['Exclude from Comparison']
        }
        
        # Show mapping summary
        if valid_mappings:
            st.success(f"✅ {len(valid_mappings)} columns mapped successfully")
        else:
            st.warning("⚠️ No valid column mappings. Please map at least one column.")
        
        # Join columns selection
        st.subheader("Join Columns")
        
        # Create list of available join columns from valid mappings
        available_columns = [(f"{src} → {tgt}", src) 
                           for src, tgt in valid_mappings.items()
                           if not edited_mapping[edited_mapping['Source Column'] == src]['Exclude from Comparison'].iloc[0]]
        
        if available_columns:
            selected_join_columns = st.multiselect(
                "Select Join Columns",
                options=[col[0] for col in available_columns],
                help="Select one or more columns to join the datasets"
            )
        else:
            st.warning("No valid column mappings available for join selection. Please map at least one column.")
            selected_join_columns = []
        
        # Data type mapping
        st.subheader("Data Type Mapping")
        dtype_mapping = {}
        for col in source_data.columns:
            # Get current column type
            current_type = str(source_data[col].dtype)
            
            # Map numpy/pandas types to our type mapping
            if 'int' in current_type:
                base_type = 'int'
            elif 'float' in current_type:
                base_type = 'Float'
            elif 'datetime' in current_type:
                base_type = 'datetime'
            elif 'bool' in current_type:
                base_type = 'Boolean'
            elif 'object' in current_type:
                # Try to infer type from data
                sample = source_data[col].dropna().head(100)
                if len(sample) > 0:
                    if all(isinstance(x, (int, np.integer)) for x in sample):
                        base_type = 'int'
                    elif all(isinstance(x, (float, np.floating)) for x in sample):
                        base_type = 'Float'
                    elif all(isinstance(x, bool) for x in sample):
                        base_type = 'Boolean'
                    elif all(isinstance(x, (date, datetime, pd.Timestamp)) for x in sample):
                        base_type = 'datetime'
                    else:
                        base_type = 'varchar'
                else:
                    base_type = 'varchar'
            else:
                base_type = 'varchar'
            
            # Get mapped type from TYPE_MAPPING
            mapped_type = TYPE_MAPPING.get(base_type, TYPE_MAPPING['varchar'])
            
            # Create options list from TYPE_MAPPING values
            type_options = sorted(set(TYPE_MAPPING.values()))
            
            # Create the selectbox with proper type selection
            dtype_mapping[col] = st.selectbox(
                f"Type for {col}",
                options=type_options,
                index=type_options.index(mapped_type),
                help=f"Current type: {current_type}"
            )
        
        # Validate mappings and show comparison button
        if valid_mappings:
            # Validate data type compatibility
            is_valid, error_message = validate_mapping(
                source_data, target_data, 
                valid_mappings, 
                dtype_mapping
            )
            
            if not is_valid:
                st.error(f"❌ Mapping validation failed: {error_message}")
                st.info("💡 Please review your column mappings and data types.")
            
            # Compare button
            compare_button = st.button(
                "Compare",
                type="primary",
                disabled=not (selected_join_columns and is_valid),
                help="Generate comparison reports for the mapped columns"
            )
            
            if compare_button:
                try:
                    with st.spinner("Generating comparison reports..."):
                        progress_text = st.empty()
                        progress_bar = st.progress(0)
                        
                        # Get the actual join columns and their mappings
                        progress_text.text("Preparing join columns...")
                        join_cols = []
                        join_mappings = {}
                        for col_pair in selected_join_columns:
                            src_col = col_pair.split(" → ")[0]
                            tgt_col = valid_mappings[src_col]
                            join_cols.append(src_col)
                            join_mappings[src_col] = tgt_col
                        
                        progress_bar.progress(10)
                        
                        # Apply data type conversions
                        progress_text.text("Converting data types...")
                        for col, dtype in dtype_mapping.items():
                            try:
                                if dtype == 'int64':
                                    source_data[col] = pd.to_numeric(source_data[col], errors='coerce').astype('Int64')
                                elif dtype == 'float64':
                                    source_data[col] = pd.to_numeric(source_data[col], errors='coerce')
                                elif dtype == 'datetime64[ns]':
                                    source_data[col] = pd.to_datetime(source_data[col], errors='coerce')
                                elif dtype == 'bool':
                                    source_data[col] = source_data[col].astype('boolean')
                                # string type conversion is not needed
                            except Exception as e:
                                st.error(f"❌ Error converting {col} to {dtype}: {str(e)}")
                                raise Exception(f"Data type conversion failed for column {col}")
                        
                        progress_bar.progress(20)
                        
                        # Generate reports with progress updates
                        progress_text.text("Generating DataCompy report...")
                        datacompy_report = generate_datacompy_report(
                            source_data, target_data, join_cols, edited_mapping, join_mappings
                        )
                        progress_bar.progress(40)
                        
                        progress_text.text("Generating Y-Data Profile...")
                        ydata_report = generate_ydata_profile(
                            source_data, target_data, edited_mapping
                        )
                        progress_bar.progress(60)
                        
                        progress_text.text("Generating Regression report...")
                        regression_report = generate_regression_report(
                            source_data, target_data, edited_mapping, dtype_mapping
                        )
                        progress_bar.progress(80)
                        
                        progress_text.text("Generating Difference report...")
                        difference_report = generate_difference_report(
                            source_data, target_data, join_cols, edited_mapping, join_mappings
                        )
                        progress_bar.progress(90)
                        
                        progress_text.text("Creating final reports...")
                        consolidated_report = create_consolidated_report(
                            datacompy_report,
                            ydata_report,
                            regression_report,
                            difference_report
                        )
                        progress_bar.progress(100)
                        progress_text.text("✅ Comparison completed!")
                        
                        # Provide download links
                        st.success("✅ Comparison completed successfully!")
                        
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.download_button(
                                label="📥 Download Consolidated Report",
                                data=consolidated_report,
                                file_name=f"consolidated_report_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.zip",
                                mime="application/zip",
                                help="Download all reports in a single ZIP file"
                            )
                        
                        with col2:
                            st.download_button(
                                label="📊 Download Individual Reports",
                                data=create_individual_reports_zip(
                                    datacompy_report,
                                    ydata_report,
                                    regression_report,
                                    difference_report
                                ),
                                file_name=f"individual_reports_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.zip",
                                mime="application/zip",
                                help="Download reports as separate files in a ZIP"
                            )
                
                except Exception as e:
                    # Clear progress indicators
                    if 'progress_text' in locals():
                        progress_text.empty()
                    if 'progress_bar' in locals():
                        progress_bar.empty()
                    
                    # Show detailed error message
                    error_msg = str(e)
                    if "Failed to read file" in error_msg:
                        st.error("❌ Error reading file. Please check the file format and delimiter.")
                        st.info("💡 Try selecting a different delimiter or check if the file is properly formatted.")
                    elif "SQL Server" in error_msg:
                        st.error("❌ Database connection error. Please check your connection details.")
                        st.info("💡 Verify your server address, credentials, and ensure the database is accessible.")
                    elif "data type conversion" in error_msg:
                        st.error("❌ Data type conversion error. Please review your data type mappings.")
                        st.info("💡 Some columns may contain values incompatible with the selected data types.")
                    else:
                        st.error(f"❌ Error during comparison: {error_msg}")
                    
                    # Log the full error
                    logger.error(f"Comparison error: {error_msg}", exc_info=True)

def handle_data_input(prefix: str, data_type: str) -> Optional[pd.DataFrame]:
    """Handle different types of data input based on the selected type."""
    try:
        if data_type in ["CSV file", "DAT file", "Parquet file"]:
            uploaded_file = st.file_uploader(
                f"Upload {data_type}",
                type=["csv", "dat", "parquet"],
                key=f"{prefix}_file"
            )
            if uploaded_file:
                if data_type == "Parquet file":
                    return read_parquet(uploaded_file)
                else:
                    delimiter = st.session_state.get(f"{prefix}_delimiter", ",")
                    return read_csv_dat(uploaded_file, delimiter)
                    
        elif data_type in ["SQL Server", "Teradata", "Stored Procs"]:
            with st.expander(f"{data_type} Connection Details"):
                server = st.text_input("Server", key=f"{prefix}_server")
                database = st.text_input("Database", key=f"{prefix}_database")
                auth_method = st.radio(
                    "Authentication Method",
                    ["Windows Authentication", "SQL Server Authentication"],
                    key=f"{prefix}_auth_method",
                    help="Choose Windows Authentication to use your Windows credentials"
                )
                
                username = None
                password = None
                if auth_method == "SQL Server Authentication":
                    username = st.text_input("Username", key=f"{prefix}_username")
                    password = st.text_input("Password", type="password", key=f"{prefix}_password")
                
                if data_type == "Stored Procs":
                    proc_name = st.text_input("Stored Procedure Name", key=f"{prefix}_proc")
                    if server and database and proc_name and (auth_method == "Windows Authentication" or (username and password)):
                        return read_stored_proc(server, database, username, password, proc_name)
                else:
                    query = st.text_area("SQL Query", key=f"{prefix}_query")
                    if server and database and query and (auth_method == "Windows Authentication" or (username and password)):
                        if data_type == "SQL Server":
                            return read_sql(server, database, username, password, query)
                        else:
                            return read_teradata(server, database, username, password, query)
                            
        elif data_type == "API":
            with st.expander("API Details"):
                api_url = st.text_input("API URL", key=f"{prefix}_api_url")
                method = st.selectbox("Method", ["GET", "POST"], key=f"{prefix}_api_method")
                headers = st.text_area("Headers (JSON)", key=f"{prefix}_api_headers")
                body = st.text_area("Body (JSON)", key=f"{prefix}_api_body")
                if api_url:
                    return read_api(api_url, method, headers, body)
                    
        elif data_type == "Zipped Flat files":
            uploaded_zip = st.file_uploader(
                "Upload ZIP file",
                type=["zip"],
                key=f"{prefix}_zip"
            )
            if uploaded_zip:
                delimiter = st.session_state.get(f"{prefix}_delimiter", ",")
                return read_zipped_files(uploaded_zip, delimiter)
                
    except Exception as e:
        st.error(f"Error loading {prefix} data: {str(e)}")
        logger.error(f"Data loading error for {prefix}: {str(e)}")
        return None

if __name__ == "__main__":
    main()
