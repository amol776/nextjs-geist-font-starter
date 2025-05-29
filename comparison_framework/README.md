# Data Comparison Framework

A robust framework for comparing data from various sources with an intuitive Streamlit UI.

## Features

- Support for multiple data sources:
  - CSV files
  - DAT files
  - SQL Server
  - Stored Procedures
  - Teradata
  - API endpoints
  - Parquet files
  - Flat files inside zipped folders

- Automatic column mapping with manual override capability
- Multiple join column selection
- Data type mapping and validation
- Column exclusion from comparison
- Comprehensive reporting:
  - DataCompy difference report
  - Y-Data Profiling comparison report
  - Excel-based regression report
  - Side-by-side difference report

## Installation

1. Clone the repository
2. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

1. Start the Streamlit application:
   ```bash
   streamlit run streamlit_app.py
   ```

2. Using the UI:
   - Select source and target data types from the sidebar
   - Upload files or provide connection details
   - Configure column mapping and join columns
   - Select columns to exclude from comparison
   - Click "Compare" to generate reports

## Reports Generated

### 1. DataCompy Report
- Detailed comparison of source and target datasets
- Row-level differences
- Column statistics

### 2. Y-Data Profiling Report
- Comprehensive data profiling
- Statistical analysis
- Data quality metrics

### 3. Regression Report
Contains three tabs:
- **AggregationCheck**: Compares aggregated values of numeric columns
- **CountCheck**: Verifies row counts between source and target
- **DistinctCheck**: Analyzes distinct values in non-numeric columns

### 4. Side-by-Side Difference Report
- Shows only the differences between source and target
- Clear indication when no differences are found

## Handling Large Files

The framework is designed to handle files larger than 3GB through:
- Chunked reading of data
- Memory-efficient processing
- Progress indicators for long-running operations

## Best Practices

1. **Data Types**: Ensure source and target data types are compatible
2. **Join Columns**: Select unique identifier columns for accurate comparison
3. **Memory Management**: For large files, monitor system resources
4. **Error Handling**: Check logs for detailed error messages

## Project Structure

```
comparison_framework/
├── requirements.txt        # Python package dependencies
├── streamlit_app.py       # Main Streamlit UI application
├── README.md             # Project documentation
└── backend/              # Backend modules
    ├── __init__.py       # Package initialization
    ├── data_reader.py    # Data source readers
    ├── mapping_utils.py  # Column mapping utilities
    ├── report_generator.py # Report generation
    └── utils.py          # Common utilities
```

## Key Components

### 1. Data Reader (`data_reader.py`)
- Handles reading from various data sources
- Supports chunked reading for large files
- Provides consistent DataFrame output

### 2. Mapping Utils (`mapping_utils.py`)
- Automatic column mapping
- Join column validation
- Data type compatibility checking

### 3. Report Generator (`report_generator.py`)
- Generates comprehensive comparison reports
- Supports multiple report formats
- Handles large datasets efficiently

### 4. Utils (`utils.py`)
- File size checking
- Progress tracking
- Error handling
- Logging setup

## Error Handling

The framework implements comprehensive error handling:
- Detailed error messages in logs
- User-friendly error displays in UI
- Graceful handling of large file operations
- Validation of inputs and data types

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
