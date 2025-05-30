import pandas as pd
import numpy as np
import datacompy
from ydata_profiling import ProfileReport
import xlsxwriter
from datetime import datetime
import os
import zipfile
from io import BytesIO
import logging
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

def generate_datacompy_report(source_df: pd.DataFrame, target_df: pd.DataFrame, 
                            join_columns: List[str], mapping_df: pd.DataFrame,
                            join_mappings: Dict[str, str]) -> BytesIO:
    """
    Generate a DataCompy comparison report.
    
    Args:
        source_df: Source DataFrame
        target_df: Target DataFrame
        join_columns: List of source columns to join on
        mapping_df: DataFrame containing column mapping information
        join_mappings: Dictionary mapping source join columns to target join columns
    
    Returns:
        BytesIO object containing the report
    """
    try:
        # Get excluded columns
        excluded_columns = mapping_df[mapping_df['Exclude from Comparison']]['Source Column'].tolist()
        
        # Create mapping dictionary from mapping_df
        column_mapping = dict(zip(
            mapping_df['Source Column'],
            mapping_df['Target Column']
        ))
        
        # Filter out unmapped and excluded columns
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
        
        # Get the target join column names
        target_join_columns = [join_mappings[src] for src in join_columns]
        
        # Rename join columns in target DataFrame to match source
        join_column_mapping = dict(zip(target_join_columns, join_columns))
        target_compare.rename(columns=join_column_mapping, inplace=True)
        
        # Ensure datetime columns are properly converted
        for col in source_compare.columns:
            if source_compare[col].dtype == 'datetime64[ns]':
                target_compare[col] = pd.to_datetime(target_compare[col], errors='coerce')
            elif target_compare[col].dtype == 'datetime64[ns]':
                source_compare[col] = pd.to_datetime(source_compare[col], errors='coerce')

        # Ensure all columns are strings to prevent type comparison issues
        source_compare.columns = source_compare.columns.astype(str)
        target_compare.columns = target_compare.columns.astype(str)

        # Create comparison object
        try:
            comparison = datacompy.Compare(
                df1=source_compare,
                df2=target_compare,
                join_columns=join_columns,
                df1_name='Source',
                df2_name='Target',
                on_index=False
            )
        except Exception as e:
            logger.error(f"Error creating comparison object: {str(e)}")
            # Try alternative comparison with converted data types
            source_compare = source_compare.astype(str)
            target_compare = target_compare.astype(str)
            comparison = datacompy.Compare(
                df1=source_compare,
                df2=target_compare,
                join_columns=join_columns,
                df1_name='Source',
                df2_name='Target',
                on_index=False
            )
        
        # Generate report
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            # Write summary
            pd.DataFrame({
                'Metric': ['Rows in Source', 'Rows in Target', 'Rows in Common', 'Rows Only in Source', 
                          'Rows Only in Target', 'Columns Match', 'All Row Values Match'],
                'Value': [len(source_compare), len(target_compare), comparison.intersect_rows,
                         comparison.df1_unq_rows, comparison.df2_unq_rows, 
                         comparison.all_columns_match, comparison.matches()]
            }).to_excel(writer, sheet_name='Summary', index=False)
            
            # Write column stats
            if hasattr(comparison, 'column_stats') and comparison.column_stats is not None:
                if isinstance(comparison.column_stats, pd.DataFrame):
                    comparison.column_stats.to_excel(writer, sheet_name='Column Stats', index=True)
                else:
                    pd.DataFrame(comparison.column_stats).to_excel(writer, sheet_name='Column Stats', index=True)
            
            # Write sample mismatches
            if hasattr(comparison, 'sample_mismatch'):
                mismatches = comparison.sample_mismatch(sample_size=10)
                if isinstance(mismatches, pd.DataFrame):
                    mismatches.to_excel(writer, sheet_name='Sample Mismatches', index=True)
                elif isinstance(mismatches, list) and mismatches:
                    pd.DataFrame(mismatches).to_excel(writer, sheet_name='Sample Mismatches', index=True)
                else:
                    pd.DataFrame({'Message': ['No mismatches found']}).to_excel(
                        writer, sheet_name='Sample Mismatches', index=False)
        
        output.seek(0)
        return output
    
    except Exception as e:
        logger.error(f"Error generating DataCompy report: {str(e)}")
        raise Exception(f"Failed to generate DataCompy report: {str(e)}")

def generate_ydata_profile(source_df: pd.DataFrame, target_df: pd.DataFrame, 
                         mapping_df: pd.DataFrame) -> BytesIO:
    """
    Generate Y-Data Profiling comparison report.
    
    Args:
        source_df: Source DataFrame
        target_df: Target DataFrame
        mapping_df: DataFrame containing column mapping information
    
    Returns:
        BytesIO object containing the report
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
        
        # Generate profiles
        source_profile = ProfileReport(source_compare, title="Source Data Profile")
        target_profile = ProfileReport(target_compare, title="Target Data Profile")
        
        # Compare profiles
        comparison_report = source_profile.compare(target_profile)
        
        # Save to BytesIO
        output = BytesIO()
        comparison_report.to_file(output)
        output.seek(0)
        return output
    
    except Exception as e:
        logger.error(f"Error generating Y-Data profile: {str(e)}")
        raise Exception(f"Failed to generate Y-Data profile: {str(e)}")

def generate_regression_report(source_df: pd.DataFrame, target_df: pd.DataFrame,
                            mapping_df: pd.DataFrame, dtype_mapping: Dict[str, str]) -> BytesIO:
    """
    Generate Excel-based regression report with multiple tabs.
    
    Args:
        source_df: Source DataFrame
        target_df: Target DataFrame
        mapping_df: DataFrame containing column mapping information
        dtype_mapping: Dictionary mapping columns to their desired data types
    
    Returns:
        BytesIO object containing the report
    """
    try:
        output = BytesIO()
        
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            workbook = writer.book
            
            # Create formats for PASS/FAIL cells
            pass_format = workbook.add_format({'bg_color': '#90EE90'})  # Light green
            fail_format = workbook.add_format({'bg_color': '#FFB6C6'})  # Light pink
            
            # Generate AggregationCheck tab
            _generate_aggregation_check(source_df, target_df, mapping_df, writer, 
                                     pass_format, fail_format)
            
            # Generate CountCheck tab
            _generate_count_check(source_df, target_df, writer, pass_format, fail_format)
            
            # Generate DistinctCheck tab
            _generate_distinct_check(source_df, target_df, mapping_df, writer, 
                                  pass_format, fail_format)
        
        output.seek(0)
        return output
    
    except Exception as e:
        logger.error(f"Error generating regression report: {str(e)}")
        raise Exception(f"Failed to generate regression report: {str(e)}")

def _generate_aggregation_check(source_df: pd.DataFrame, target_df: pd.DataFrame,
                              mapping_df: pd.DataFrame, writer: pd.ExcelWriter,
                              pass_format: xlsxwriter.format.Format,
                              fail_format: xlsxwriter.format.Format) -> None:
    """Generate the AggregationCheck tab in the regression report."""
    
    # Get numeric columns
    numeric_cols = source_df.select_dtypes(include=[np.number]).columns
    
    results = []
    for col in numeric_cols:
        if col in mapping_df['Source Column'].values:
            target_col = mapping_df[mapping_df['Source Column'] == col]['Target Column'].iloc[0]
            
            source_sum = source_df[col].sum()
            target_sum = target_df[target_col].sum()
            
            match = np.isclose(source_sum, target_sum, rtol=1e-05)
            
            results.append({
                'Source Column': col,
                'Target Column': target_col,
                'Source Sum': source_sum,
                'Target Sum': target_sum,
                'Result': 'PASS' if match else 'FAIL'
            })
    
    # Create DataFrame and write to Excel
    agg_df = pd.DataFrame(results)
    agg_df.to_excel(writer, sheet_name='AggregationCheck', index=False)
    
    # Apply conditional formatting
    worksheet = writer.sheets['AggregationCheck']
    result_col = agg_df.columns.get_loc('Result')
    for row in range(len(agg_df)):
        if agg_df.iloc[row]['Result'] == 'PASS':
            worksheet.write(row + 1, result_col, 'PASS', pass_format)
        else:
            worksheet.write(row + 1, result_col, 'FAIL', fail_format)

def _generate_count_check(source_df: pd.DataFrame, target_df: pd.DataFrame,
                         writer: pd.ExcelWriter,
                         pass_format: xlsxwriter.format.Format,
                         fail_format: xlsxwriter.format.Format) -> None:
    """Generate the CountCheck tab in the regression report."""
    
    count_data = {
        'Source File Name': source_df.name if hasattr(source_df, 'name') else 'Source',
        'Target File Name': target_df.name if hasattr(target_df, 'name') else 'Target',
        'Source Count': len(source_df),
        'Target Count': len(target_df),
        'Result': 'PASS' if len(source_df) == len(target_df) else 'FAIL'
    }
    
    count_df = pd.DataFrame([count_data])
    count_df.to_excel(writer, sheet_name='CountCheck', index=False)
    
    # Apply conditional formatting
    worksheet = writer.sheets['CountCheck']
    result_col = count_df.columns.get_loc('Result')
    if count_data['Result'] == 'PASS':
        worksheet.write(1, result_col, 'PASS', pass_format)
    else:
        worksheet.write(1, result_col, 'FAIL', fail_format)

def _generate_distinct_check(source_df: pd.DataFrame, target_df: pd.DataFrame,
                           mapping_df: pd.DataFrame, writer: pd.ExcelWriter,
                           pass_format: xlsxwriter.format.Format,
                           fail_format: xlsxwriter.format.Format) -> None:
    """Generate the DistinctCheck tab in the regression report."""
    
    # Get non-numeric columns
    non_numeric_cols = source_df.select_dtypes(exclude=[np.number]).columns
    
    results = []
    for col in non_numeric_cols:
        if col in mapping_df['Source Column'].values:
            target_col = mapping_df[mapping_df['Source Column'] == col]['Target Column'].iloc[0]
            
            source_distinct = set(source_df[col].dropna().unique())
            target_distinct = set(target_df[target_col].dropna().unique())
            
            source_count = len(source_distinct)
            target_count = len(target_distinct)
            
            count_match = source_count == target_count
            values_match = source_distinct == target_distinct
            
            results.append({
                'Source Column': col,
                'Target Column': target_col,
                'Source Distinct Count': source_count,
                'Target Distinct Count': target_count,
                'Count Match': 'PASS' if count_match else 'FAIL',
                'Values Match': 'PASS' if values_match else 'FAIL',
                'Source Distinct Values': ', '.join(map(str, sorted(source_distinct))),
                'Target Distinct Values': ', '.join(map(str, sorted(target_distinct)))
            })
    
    # Create DataFrame and write to Excel
    distinct_df = pd.DataFrame(results)
    distinct_df.to_excel(writer, sheet_name='DistinctCheck', index=False)
    
    # Apply conditional formatting
    worksheet = writer.sheets['DistinctCheck']
    count_match_col = distinct_df.columns.get_loc('Count Match')
    values_match_col = distinct_df.columns.get_loc('Values Match')
    
    for row in range(len(distinct_df)):
        if distinct_df.iloc[row]['Count Match'] == 'PASS':
            worksheet.write(row + 1, count_match_col, 'PASS', pass_format)
        else:
            worksheet.write(row + 1, count_match_col, 'FAIL', fail_format)
            
        if distinct_df.iloc[row]['Values Match'] == 'PASS':
            worksheet.write(row + 1, values_match_col, 'PASS', pass_format)
        else:
            worksheet.write(row + 1, values_match_col, 'FAIL', fail_format)

def generate_difference_report(source_df: pd.DataFrame, target_df: pd.DataFrame,
                             join_columns: List[str], mapping_df: pd.DataFrame,
                             join_mappings: Dict[str, str]) -> BytesIO:
    """
    Generate side-by-side difference report.
    
    Args:
        source_df: Source DataFrame
        target_df: Target DataFrame
        join_columns: List of source columns to join on
        mapping_df: DataFrame containing column mapping information
        join_mappings: Dictionary mapping source join columns to target join columns
    
    Returns:
        BytesIO object containing the report
    """
    try:
        output = BytesIO()
        
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
        
        # Get the target join column names
        target_join_columns = [join_mappings[src] for src in join_columns]
        
        # Merge datasets
        merged = pd.merge(
            source_compare, target_compare,
            left_on=join_columns,
            right_on=target_join_columns,
            how='outer',
            suffixes=('_source', '_target'),
            indicator=True
        )
        
        # Find differences
        differences = merged[merged['_merge'] != 'both']
        
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            if len(differences) > 0:
                differences.to_excel(writer, sheet_name='Differences', index=False)
            else:
                pd.DataFrame({'Message': ['No differences found']}).to_excel(
                    writer, sheet_name='Differences', index=False)
        
        output.seek(0)
        return output
    
    except Exception as e:
        logger.error(f"Error generating difference report: {str(e)}")
        raise Exception(f"Failed to generate difference report: {str(e)}")

def create_individual_reports_zip(datacompy_report: BytesIO,
                                ydata_report: BytesIO,
                                regression_report: BytesIO,
                                difference_report: BytesIO) -> BytesIO:
    """
    Create a ZIP file containing individual reports in separate folders.
    
    Args:
        datacompy_report: DataCompy report as BytesIO
        ydata_report: Y-Data Profiling report as BytesIO
        regression_report: Regression report as BytesIO
        difference_report: Difference report as BytesIO
    
    Returns:
        BytesIO object containing the ZIP file with individual reports
    """
    try:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output = BytesIO()
        
        with zipfile.ZipFile(output, 'w', zipfile.ZIP_DEFLATED) as zf:
            # DataCompy Report
            zf.writestr(f'datacompy/datacompy_report_{timestamp}.xlsx', 
                       datacompy_report.getvalue())
            
            # Y-Data Profile
            zf.writestr(f'ydata_profile/comparison_profile_{timestamp}.html', 
                       ydata_report.getvalue())
            
            # Regression Report
            zf.writestr(f'regression/regression_report_{timestamp}.xlsx', 
                       regression_report.getvalue())
            
            # Difference Report
            zf.writestr(f'differences/difference_report_{timestamp}.xlsx', 
                       difference_report.getvalue())
            
            # Add a README file
            readme_content = """
Data Comparison Reports

1. datacompy/ - Contains detailed comparison report
2. ydata_profile/ - Contains comprehensive data profiling
3. regression/ - Contains aggregation, count, and distinct value checks
4. differences/ - Contains side-by-side difference report

Generated on: {timestamp}
            """.format(timestamp=timestamp)
            
            zf.writestr('README.txt', readme_content.strip())
        
        output.seek(0)
        return output
    
    except Exception as e:
        logger.error(f"Error creating individual reports zip: {str(e)}")
        raise Exception(f"Failed to create individual reports zip: {str(e)}")

def create_consolidated_report(datacompy_report: BytesIO,
                             ydata_report: BytesIO,
                             regression_report: BytesIO,
                             difference_report: BytesIO) -> BytesIO:
    """
    Combine all reports into a single consolidated report.
    
    Args:
        datacompy_report: DataCompy report as BytesIO
        ydata_report: Y-Data Profiling report as BytesIO
        regression_report: Regression report as BytesIO
        difference_report: Difference report as BytesIO
    
    Returns:
        BytesIO object containing the consolidated ZIP file
    """
    try:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output = BytesIO()
        
        with zipfile.ZipFile(output, 'w', zipfile.ZIP_DEFLATED) as zf:
            # Add all reports to a single consolidated file
            zf.writestr(f'reports/datacompy_report_{timestamp}.xlsx', 
                       datacompy_report.getvalue())
            zf.writestr(f'reports/ydata_profile_{timestamp}.html', 
                       ydata_report.getvalue())
            zf.writestr(f'reports/regression_report_{timestamp}.xlsx', 
                       regression_report.getvalue())
            zf.writestr(f'reports/difference_report_{timestamp}.xlsx', 
                       difference_report.getvalue())
            
            # Add a summary README
            readme_content = """
Consolidated Data Comparison Report

This ZIP file contains the following reports:
1. datacompy_report - Detailed comparison of datasets
2. ydata_profile - Comprehensive data profiling
3. regression_report - Aggregation, count, and distinct value checks
4. difference_report - Side-by-side differences

Generated on: {timestamp}
            """.format(timestamp=timestamp)
            
            zf.writestr('README.txt', readme_content.strip())
        
        output.seek(0)
        return output
    
    except Exception as e:
        logger.error(f"Error creating consolidated report: {str(e)}")
        raise Exception(f"Failed to create consolidated report: {str(e)}")
