import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from typing import Dict, Tuple

def generate_distribution_plot(data: pd.Series) -> str:
    """Generate HTML for a distribution plot using plotly."""
    fig = make_subplots(rows=1, cols=1)
    fig.add_trace(go.Histogram(x=data, name="Distribution"))
    fig.update_layout(
        title="Value Distribution",
        xaxis_title=data.name,
        yaxis_title="Count",
        showlegend=False,
        height=400
    )
    return fig.to_html(full_html=False, include_plotlyjs='cdn')

def generate_frequency_plot(data: pd.Series) -> str:
    """Generate HTML for a frequency plot using plotly."""
    value_counts = data.value_counts().head(20)  # Show top 20 values
    fig = go.Figure(data=[
        go.Bar(x=value_counts.index.astype(str), y=value_counts.values)
    ])
    fig.update_layout(
        title="Top 20 Value Frequencies",
        xaxis_title=data.name,
        yaxis_title="Count",
        height=400
    )
    return fig.to_html(full_html=False, include_plotlyjs='cdn')

def generate_comparison_plot(source_data: pd.Series, target_data: pd.Series) -> str:
    """Generate HTML for a comparison plot using plotly."""
    fig = make_subplots(rows=1, cols=2, subplot_titles=["Source", "Target"])
    
    if pd.api.types.is_numeric_dtype(source_data) and pd.api.types.is_numeric_dtype(target_data):
        fig.add_trace(go.Histogram(x=source_data, name="Source"), row=1, col=1)
        fig.add_trace(go.Histogram(x=target_data, name="Target"), row=1, col=2)
    else:
        source_counts = source_data.value_counts().head(20)
        target_counts = target_data.value_counts().head(20)
        fig.add_trace(go.Bar(x=source_counts.index.astype(str), y=source_counts.values, name="Source"), row=1, col=1)
        fig.add_trace(go.Bar(x=target_counts.index.astype(str), y=target_counts.values, name="Target"), row=1, col=2)
    
    fig.update_layout(height=400, showlegend=False)
    return fig.to_html(full_html=False, include_plotlyjs='cdn')

def generate_comparison_rows(source_stats: Dict, target_stats: Dict) -> str:
    """Generate HTML table rows comparing source and target statistics."""
    rows = []
    for key in source_stats.keys():
        if key in target_stats:
            source_val = source_stats[key]
            target_val = target_stats[key]
            
            # Calculate difference
            if isinstance(source_val, (int, float)) and isinstance(target_val, (int, float)):
                diff = target_val - source_val
                diff_str = f"{diff:+.2f}" if isinstance(diff, float) else f"{diff:+d}"
                class_name = "match" if abs(diff) < 0.0001 else "diff"
            else:
                diff_str = "N/A"
                class_name = ""
            
            # Format values
            source_str = f"{source_val:.2f}" if isinstance(source_val, float) else str(source_val)
            target_str = f"{target_val:.2f}" if isinstance(target_val, float) else str(target_val)
            
            rows.append(f"""
                <tr class="{class_name}">
                    <td>{key}</td>
                    <td>{source_str}</td>
                    <td>{target_str}</td>
                    <td>{diff_str}</td>
                </tr>
            """)
    
    return "\n".join(rows)

def calculate_column_stats(data: pd.Series) -> Dict:
    """Calculate statistics for a column."""
    stats = {
        'Count': len(data),
        'Unique Values': data.nunique(),
        'Missing Values': data.isna().sum(),
        'Missing %': (data.isna().sum() / len(data)) * 100,
    }
    
    if pd.api.types.is_numeric_dtype(data):
        stats.update({
            'Mean': data.mean(),
            'Std': data.std(),
            'Min': data.min(),
            'Max': data.max(),
            '25%': data.quantile(0.25),
            'Median': data.median(),
            '75%': data.quantile(0.75)
        })
    
    return stats
