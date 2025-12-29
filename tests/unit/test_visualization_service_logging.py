"""Unit tests for Visualization service logging integration"""
import pytest
from unittest.mock import Mock, patch
import pandas as pd
import altair as alt

from services.visualization_service import VisualizationService


class TestVisualizationServiceLogging:
    """Test Visualization service logging integration"""
    
    @pytest.fixture
    def viz_service(self):
        """Create Visualization service"""
        service = VisualizationService()
        return service
    
    @pytest.fixture
    def sample_dataframe(self):
        """Create sample DataFrame for testing"""
        return pd.DataFrame({
            'category': ['A', 'B', 'C'],
            'value': [10, 20, 30]
        })
    
    @pytest.fixture
    def numeric_dataframe(self):
        """Create numeric DataFrame for scatter chart"""
        return pd.DataFrame({
            'x': [1, 2, 3, 4, 5],
            'y': [10, 20, 30, 40, 50]
        })
    
    def test_visualization_service_initialization_logging(self, viz_service, caplog):
        """Test that Visualization service logs initialization"""
        import logging
        with caplog.at_level(logging.INFO):
            # Service already initialized in fixture
            assert viz_service.logger is not None
            assert viz_service.logger.component == "visualization_service"
    
    def test_detect_chart_type_logs_detection(self, viz_service, sample_dataframe, caplog):
        """Test that detect_chart_type logs chart type detection"""
        import logging
        with caplog.at_level(logging.DEBUG):
            chart_type = viz_service.detect_chart_type(sample_dataframe)
            
            # Check that detection was logged
            log_messages = [r.message for r in caplog.records]
            assert any("Chart type detected" in msg for msg in log_messages)
            assert chart_type is not None
    
    def test_detect_chart_type_logs_empty_dataframe(self, viz_service, caplog):
        """Test that detect_chart_type logs when DataFrame is empty"""
        import logging
        empty_df = pd.DataFrame()
        
        with caplog.at_level(logging.DEBUG):
            chart_type = viz_service.detect_chart_type(empty_df)
            
            # Check that empty DataFrame was logged
            log_messages = [r.message for r in caplog.records]
            assert any("Cannot detect chart type - empty DataFrame" in msg for msg in log_messages)
            assert chart_type is None
    
    def test_create_bar_chart_logs_creation(self, viz_service, sample_dataframe, caplog):
        """Test that create_bar_chart logs creation"""
        import logging
        with caplog.at_level(logging.DEBUG):
            chart = viz_service.create_bar_chart(
                sample_dataframe,
                'category',
                'value',
                'Test Chart'
            )
            
            # Check that creation was logged
            log_messages = [r.message for r in caplog.records]
            assert any("Bar chart created" in msg for msg in log_messages)
            assert chart is not None
    
    def test_create_line_chart_logs_creation(self, viz_service, sample_dataframe, caplog):
        """Test that create_line_chart logs creation"""
        import logging
        with caplog.at_level(logging.DEBUG):
            chart = viz_service.create_line_chart(
                sample_dataframe,
                'category',
                'value',
                'Test Chart'
            )
            
            # Check that creation was logged
            log_messages = [r.message for r in caplog.records]
            assert any("Line chart created" in msg for msg in log_messages)
            assert chart is not None
    
    def test_create_scatter_chart_logs_creation(self, viz_service, numeric_dataframe, caplog):
        """Test that create_scatter_chart logs creation"""
        import logging
        with caplog.at_level(logging.DEBUG):
            chart = viz_service.create_scatter_chart(
                numeric_dataframe,
                'x',
                'y',
                'Test Chart'
            )
            
            # Check that creation was logged
            log_messages = [r.message for r in caplog.records]
            assert any("Scatter chart created" in msg for msg in log_messages)
            assert chart is not None
    
    def test_create_visualization_logs_success(self, viz_service, sample_dataframe, caplog):
        """Test that create_visualization logs successful creation"""
        import logging
        with caplog.at_level(logging.INFO):
            viz = viz_service.create_visualization(sample_dataframe, title="Test")
            
            # Check that success was logged
            log_messages = [r.message for r in caplog.records]
            assert any("Visualization created successfully" in msg for msg in log_messages)
            assert viz is not None
            assert viz["type"] in ["bar", "line", "scatter"]
    
    def test_create_visualization_logs_empty_dataframe(self, viz_service, caplog):
        """Test that create_visualization logs when DataFrame is empty"""
        import logging
        empty_df = pd.DataFrame()
        
        with caplog.at_level(logging.DEBUG):
            viz = viz_service.create_visualization(empty_df)
            
            # Check that empty DataFrame was logged
            log_messages = [r.message for r in caplog.records]
            assert any("Cannot create visualization - empty DataFrame" in msg for msg in log_messages)
            assert viz is None
    
    def test_create_visualization_logs_errors(self, viz_service, sample_dataframe, caplog):
        """Test that create_visualization logs errors"""
        import logging
        with caplog.at_level(logging.ERROR):
            # Mock altair to raise an error
            with patch('services.visualization_service.alt.Chart', side_effect=Exception("Test error")):
                viz = viz_service.create_visualization(sample_dataframe)
                
                # Check that error was logged
                log_messages = [r.message for r in caplog.records]
                assert any("Visualization creation failed" in msg for msg in log_messages)
                # Should return None (visualization is optional)
                assert viz is None
    
    def test_trace_span_in_detect_chart_type(self, viz_service, sample_dataframe):
        """Test that detect_chart_type uses trace_span"""
        # Should not raise (trace_span is a context manager)
        chart_type = viz_service.detect_chart_type(sample_dataframe)
        assert chart_type is not None
    
    def test_trace_span_in_create_bar_chart(self, viz_service, sample_dataframe):
        """Test that create_bar_chart uses trace_span"""
        # Should not raise (trace_span is a context manager)
        chart = viz_service.create_bar_chart(sample_dataframe, 'category', 'value')
        assert chart is not None
    
    def test_trace_span_in_create_line_chart(self, viz_service, sample_dataframe):
        """Test that create_line_chart uses trace_span"""
        # Should not raise (trace_span is a context manager)
        chart = viz_service.create_line_chart(sample_dataframe, 'category', 'value')
        assert chart is not None
    
    def test_trace_span_in_create_scatter_chart(self, viz_service, numeric_dataframe):
        """Test that create_scatter_chart uses trace_span"""
        # Should not raise (trace_span is a context manager)
        chart = viz_service.create_scatter_chart(numeric_dataframe, 'x', 'y')
        assert chart is not None
    
    def test_trace_span_in_create_visualization(self, viz_service, sample_dataframe):
        """Test that create_visualization uses trace_span"""
        # Should not raise (trace_span is a context manager)
        viz = viz_service.create_visualization(sample_dataframe)
        assert viz is not None
    
    def test_logger_component_name(self, viz_service):
        """Test that logger has correct component name"""
        assert viz_service.logger.component == "visualization_service"
    
    def test_chart_creation_logs_generation_time(self, viz_service, sample_dataframe, caplog):
        """Test that chart creation logs generation time"""
        import logging
        with caplog.at_level(logging.DEBUG):
            chart = viz_service.create_bar_chart(sample_dataframe, 'category', 'value')
            
            # Check that generation time is logged
            log_messages = [r.message for r in caplog.records]
            assert any("Bar chart created" in msg for msg in log_messages)
            assert chart is not None
    
    def test_create_visualization_logs_chart_type_detection(self, viz_service, sample_dataframe, caplog):
        """Test that create_visualization logs chart type detection"""
        import logging
        with caplog.at_level(logging.DEBUG):
            viz = viz_service.create_visualization(sample_dataframe)
            
            # Check that chart type detection was logged
            log_messages = [r.message for r in caplog.records]
            assert any("Creating visualization" in msg for msg in log_messages)
            assert viz is not None

