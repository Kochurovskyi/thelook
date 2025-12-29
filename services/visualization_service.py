"""Visualization service for generating charts from query results"""
from typing import Optional, Dict, Any
import pandas as pd
import altair as alt
import time
import config
from utils.logger import ComponentLogger
from utils.tracing import trace_span


class VisualizationService:
    """Service for creating visualizations from query results"""
    
    def __init__(self):
        """Initialize visualization service"""
        self.logger = ComponentLogger("visualization_service")
        self.default_width = config.DEFAULT_CHART_WIDTH
        self.default_height = config.DEFAULT_CHART_HEIGHT
        
        self.logger.info(
            "Visualization service initialized",
            default_width=self.default_width,
            default_height=self.default_height
        )
    
    def detect_chart_type(
        self,
        df: pd.DataFrame,
        query_type: Optional[str] = None
    ) -> str:
        """
        Auto-detect appropriate chart type from data
        
        Args:
            df: DataFrame with query results
            query_type: Optional query type hint (from query metadata)
            
        Returns:
            Chart type string: 'bar', 'line', 'pie', 'scatter', 'table', or None
        """
        with trace_span("detect_chart_type", component="visualization_service") as span:
            if df.empty or len(df.columns) == 0:
                self.logger.debug(
                    "Cannot detect chart type - empty DataFrame",
                    row_count=len(df),
                    column_count=len(df.columns)
                )
                span.set_tag("chart_type", None)
                return None
        
            # Single column - table only
            if len(df.columns) == 1:
                chart_type = "table"
                self.logger.debug(
                    "Chart type detected: table (single column)",
                    column_count=1,
                    row_count=len(df)
                )
                span.set_tag("chart_type", chart_type)
                return chart_type
            
            # Two columns
            if len(df.columns) == 2:
                x_col = df.columns[0]
                y_col = df.columns[1]
                
                # Check if x is categorical and y is numeric
                if df[x_col].dtype == 'object' or df[x_col].dtype.name == 'category':
                    if pd.api.types.is_numeric_dtype(df[y_col]):
                        if len(df) <= 20:
                            chart_type = "bar"
                        else:
                            chart_type = "line"
                    else:
                        chart_type = "bar" if len(df) <= 20 else "line"
                # Both numeric - scatter
                elif pd.api.types.is_numeric_dtype(df[x_col]) and pd.api.types.is_numeric_dtype(df[y_col]):
                    chart_type = "scatter"
                # Default to bar for small datasets
                else:
                    chart_type = "bar" if len(df) <= 20 else "line"
                
                self.logger.debug(
                    "Chart type detected",
                    chart_type=chart_type,
                    column_count=2,
                    row_count=len(df),
                    x_column=x_col,
                    y_column=y_col
                )
                span.set_tag("chart_type", chart_type)
                return chart_type
            
            # Multiple columns - use query type hint
            if query_type == "temporal":
                chart_type = "line"
            elif query_type in ["ranking", "product_analysis", "sales_analysis"]:
                chart_type = "bar"
            else:
                chart_type = "table"
            
            self.logger.debug(
                "Chart type detected from query type",
                chart_type=chart_type,
                query_type=query_type,
                column_count=len(df.columns),
                row_count=len(df)
            )
            span.set_tag("chart_type", chart_type)
            return chart_type
    
    def create_bar_chart(
        self,
        df: pd.DataFrame,
        x_col: str,
        y_col: str,
        title: Optional[str] = None
    ) -> alt.Chart:
        """
        Create a bar chart
        
        Args:
            df: DataFrame with data
            x_col: Column name for x-axis
            y_col: Column name for y-axis
            title: Optional chart title
            
        Returns:
            Altair chart object
        """
        with trace_span("create_bar_chart", component="visualization_service", chart_type="bar") as span:
            start_time = time.time()
            
            try:
                chart = alt.Chart(df.head(50)).mark_bar().encode(
                    x=alt.X(x_col, type='nominal' if df[x_col].dtype == 'object' else 'quantitative', sort='-y'),
                    y=alt.Y(y_col, type='quantitative')
                ).properties(
                    width=self.default_width,
                    height=self.default_height,
                    title=title or f"{y_col} by {x_col}"
                )
                
                end_time = time.time()
                generation_time_ms = (end_time - start_time) * 1000
                
                self.logger.debug(
                    "Bar chart created",
                    x_column=x_col,
                    y_column=y_col,
                    row_count=len(df.head(50)),
                    generation_time_ms=generation_time_ms,
                    title=title
                )
                
                span.set_tag("generation_time_ms", generation_time_ms)
                span.set_tag("row_count", len(df.head(50)))
                span.set_tag("success", True)
                
                return chart
                
            except Exception as e:
                error_type = type(e).__name__
                self.logger.error(
                    "Bar chart creation failed",
                    x_column=x_col,
                    y_column=y_col,
                    error_type=error_type,
                    error_message=str(e)
                )
                span.set_tag("error", True)
                span.set_tag("error_type", error_type)
                raise
    
    def create_line_chart(
        self,
        df: pd.DataFrame,
        x_col: str,
        y_col: str,
        title: Optional[str] = None
    ) -> alt.Chart:
        """
        Create a line chart
        
        Args:
            df: DataFrame with data
            x_col: Column name for x-axis
            y_col: Column name for y-axis
            title: Optional chart title
            
        Returns:
            Altair chart object
        """
        with trace_span("create_line_chart", component="visualization_service", chart_type="line") as span:
            start_time = time.time()
            
            try:
                # Check if x_col is date-like
                x_type = 'temporal' if 'date' in str(df[x_col].dtype).lower() or 'time' in str(df[x_col].dtype).lower() else 'quantitative'
                
                chart = alt.Chart(df.head(100)).mark_line(point=True).encode(
                    x=alt.X(x_col, type=x_type),
                    y=alt.Y(y_col, type='quantitative')
                ).properties(
                    width=self.default_width,
                    height=self.default_height,
                    title=title or f"{y_col} over {x_col}"
                )
                
                end_time = time.time()
                generation_time_ms = (end_time - start_time) * 1000
                
                self.logger.debug(
                    "Line chart created",
                    x_column=x_col,
                    y_column=y_col,
                    x_type=x_type,
                    row_count=len(df.head(100)),
                    generation_time_ms=generation_time_ms,
                    title=title
                )
                
                span.set_tag("generation_time_ms", generation_time_ms)
                span.set_tag("row_count", len(df.head(100)))
                span.set_tag("x_type", x_type)
                span.set_tag("success", True)
                
                return chart
                
            except Exception as e:
                error_type = type(e).__name__
                self.logger.error(
                    "Line chart creation failed",
                    x_column=x_col,
                    y_column=y_col,
                    error_type=error_type,
                    error_message=str(e)
                )
                span.set_tag("error", True)
                span.set_tag("error_type", error_type)
                raise
    
    def create_scatter_chart(
        self,
        df: pd.DataFrame,
        x_col: str,
        y_col: str,
        title: Optional[str] = None
    ) -> alt.Chart:
        """
        Create a scatter chart
        
        Args:
            df: DataFrame with data
            x_col: Column name for x-axis
            y_col: Column name for y-axis
            title: Optional chart title
            
        Returns:
            Altair chart object
        """
        with trace_span("create_scatter_chart", component="visualization_service", chart_type="scatter") as span:
            start_time = time.time()
            
            try:
                chart = alt.Chart(df.head(100)).mark_circle(size=60).encode(
                    x=alt.X(x_col, type='quantitative'),
                    y=alt.Y(y_col, type='quantitative')
                ).properties(
                    width=self.default_width,
                    height=self.default_height,
                    title=title or f"{y_col} vs {x_col}"
                )
                
                end_time = time.time()
                generation_time_ms = (end_time - start_time) * 1000
                
                self.logger.debug(
                    "Scatter chart created",
                    x_column=x_col,
                    y_column=y_col,
                    row_count=len(df.head(100)),
                    generation_time_ms=generation_time_ms,
                    title=title
                )
                
                span.set_tag("generation_time_ms", generation_time_ms)
                span.set_tag("row_count", len(df.head(100)))
                span.set_tag("success", True)
                
                return chart
                
            except Exception as e:
                error_type = type(e).__name__
                self.logger.error(
                    "Scatter chart creation failed",
                    x_column=x_col,
                    y_column=y_col,
                    error_type=error_type,
                    error_message=str(e)
                )
                span.set_tag("error", True)
                span.set_tag("error_type", error_type)
                raise
    
    def create_visualization(
        self,
        df: pd.DataFrame,
        query_type: Optional[str] = None,
        title: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Create appropriate visualization from DataFrame
        
        Args:
            df: DataFrame with query results
            query_type: Optional query type hint
            title: Optional chart title
            
        Returns:
            Dictionary with visualization spec or None if no visualization
        """
        with trace_span("create_visualization", component="visualization_service", query_type=query_type) as span:
            start_time = time.time()
            
            try:
                if df.empty or len(df.columns) == 0:
                    self.logger.debug(
                        "Cannot create visualization - empty DataFrame",
                        row_count=len(df),
                        column_count=len(df.columns)
                    )
                    span.set_tag("visualization_created", False)
                    span.set_tag("reason", "empty_dataframe")
                    return None
                
                chart_type = self.detect_chart_type(df, query_type)
                
                if chart_type is None or chart_type == "table":
                    self.logger.debug(
                        "No visualization created - chart type is table or None",
                        chart_type=chart_type,
                        row_count=len(df),
                        column_count=len(df.columns)
                    )
                    span.set_tag("visualization_created", False)
                    span.set_tag("reason", "table_or_none")
                    return None
                
                if len(df.columns) < 2:
                    self.logger.debug(
                        "Cannot create visualization - insufficient columns",
                        column_count=len(df.columns)
                    )
                    span.set_tag("visualization_created", False)
                    span.set_tag("reason", "insufficient_columns")
                    return None
                
                x_col = df.columns[0]
                y_col = df.columns[1] if len(df.columns) > 1 else df.columns[0]
                
                self.logger.debug(
                    "Creating visualization",
                    chart_type=chart_type,
                    x_column=x_col,
                    y_column=y_col,
                    row_count=len(df),
                    query_type=query_type
                )
                
                if chart_type == "bar":
                    chart = self.create_bar_chart(df, x_col, y_col, title)
                elif chart_type == "line":
                    chart = self.create_line_chart(df, x_col, y_col, title)
                elif chart_type == "scatter":
                    chart = self.create_scatter_chart(df, x_col, y_col, title)
                else:
                    self.logger.debug(
                        "Unknown chart type",
                        chart_type=chart_type
                    )
                    span.set_tag("visualization_created", False)
                    span.set_tag("reason", "unknown_chart_type")
                    return None
                
                end_time = time.time()
                total_time_ms = (end_time - start_time) * 1000
                
                visualization = {
                    "type": chart_type,
                    "chart": chart,
                    "x_column": x_col,
                    "y_column": y_col,
                    "title": title
                }
                
                self.logger.info(
                    "Visualization created successfully",
                    chart_type=chart_type,
                    x_column=x_col,
                    y_column=y_col,
                    total_time_ms=total_time_ms,
                    row_count=len(df),
                    title=title
                )
                
                span.set_tag("visualization_created", True)
                span.set_tag("chart_type", chart_type)
                span.set_tag("total_time_ms", total_time_ms)
                span.set_tag("success", True)
                
                return visualization
                
            except Exception as e:
                error_type = type(e).__name__
                end_time = time.time()
                total_time_ms = (end_time - start_time) * 1000
                
                self.logger.error(
                    "Visualization creation failed",
                    error_type=error_type,
                    error_message=str(e),
                    total_time_ms=total_time_ms,
                    row_count=len(df),
                    column_count=len(df.columns),
                    query_type=query_type
                )
                
                span.set_tag("error", True)
                span.set_tag("error_type", error_type)
                span.set_tag("visualization_created", False)
                span.log(f"Visualization creation failed: {str(e)}", level="ERROR")
                
                # Visualization is optional, don't fail
                return None

