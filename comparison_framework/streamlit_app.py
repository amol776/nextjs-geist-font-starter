import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import os
from backend.data_reader import (
    read_csv_dat,
    read_sql,
    read_stored_proc,
    read_teradata,
    read_api,
    read_parquet,
    read_zipped_files
)
from backend.mapping_utils import auto_map_columns, validate_join_columns
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

# Initialize logger
logger = setup_logger()

# Define the type mapping dictionary
TYPE_MAPPING = {
    'int': 'int32',
    'int': 'int64',
    'numeric': 'int64',
    'bigint': 'int64',
    'smalllint': 'int64',
    'varchar': 'string',
    'nvarchar': 'string',
    'char': 'string',
    'date': 'datetime64[ns]',
    'datetime': 'datetime64[ns]',
    'decimal': 'float',
    'Float': 'float',
    'bit': 'bool',
    'nchar': 'char',
    'Boolean': 'bool'
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
        mapping_df = pd.DataFrame({
            'Source Column': column_mapping.keys(),
            'Target Column': column_mapping.values(),
            'Exclude from Comparison': False
        })
        
        edited_mapping = st.data_editor(
            mapping_df,
            hide_index=True,
            use_container_width=True
        )
        
        # Join columns selection
        st.subheader("Join Columns")
        available_columns = [(f"{src} → {tgt}", src) 
                           for src, tgt in column_mapping.items()]
        selected_join_columns = st.multiselect(
            "Select Join Columns",
            options=[col[0] for col in available_columns],
            help="Select one or more columns to join the datasets"
        )
        
        # Data type mapping
        st.subheader("Data Type Mapping")
        dtype_mapping = {}
        for col in source_data.columns:
            current_type = str(source_data[col].dtype)
            mapped_type = TYPE_MAPPING.get(current_type, current_type)
            dtype_mapping[col] = st.selectbox(
                f"Type for {col}",
                options=list(set(TYPE_MAPPING.values())),
                index=list(set(TYPE_MAPPING.values())).index(mapped_type)
            )
        
        # Compare button
        if st.button("Compare", type="primary"):
            try:
                with st.spinner("Generating comparison reports..."):
                    # Get the actual join columns from the mapping
                    join_cols = [col.split(" → ")[0] for col in selected_join_columns]
                    
                    # Generate reports
                    datacompy_report = generate_datacompy_report(
                        source_data, target_data, join_cols, edited_mapping
                    )
                    
                    ydata_report = generate_ydata_profile(
                        source_data, target_data, edited_mapping
                    )
                    
                    regression_report = generate_regression_report(
                        source_data, target_data, edited_mapping, dtype_mapping
                    )
                    
                    difference_report = generate_difference_report(
                        source_data, target_data, join_cols, edited_mapping
                    )
                    
                    # Create consolidated report
                    consolidated_report = create_consolidated_report(
                        datacompy_report,
                        ydata_report,
                        regression_report,
                        difference_report
                    )
                    
                    # Provide download links
                    st.success("Comparison completed successfully!")
                    
                    st.download_button(
                        label="Download Consolidated Report",
                        data=consolidated_report,
                        file_name=f"consolidated_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip",
                        mime="application/zip"
                    )
                    
            except Exception as e:
                st.error(f"Error during comparison: {str(e)}")
                logger.error(f"Comparison error: {str(e)}")

def handle_data_input(prefix, data_type):
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
                username = st.text_input("Username", key=f"{prefix}_username")
                password = st.text_input("Password", type="password", key=f"{prefix}_password")
                
                if data_type == "Stored Procs":
                    proc_name = st.text_input("Stored Procedure Name", key=f"{prefix}_proc")
                    if all([server, database, username, password, proc_name]):
                        return read_stored_proc(server, database, username, password, proc_name)
                else:
                    query = st.text_area("SQL Query", key=f"{prefix}_query")
                    if all([server, database, username, password, query]):
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
