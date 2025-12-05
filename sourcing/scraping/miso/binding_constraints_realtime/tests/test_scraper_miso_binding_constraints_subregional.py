"""Tests for MISO Sub-Regional Binding Constraints scraper."""

import json
from datetime import datetime, date
from unittest.mock import Mock, patch

import pytest
import requests

from sourcing.scraping.miso.binding_constraints_realtime.scraper_miso_binding_constraints_subregional import (
    MisoBindingConstraintsSubregionalCollector,
)
from sourcing.infrastructure.collection_framework import (
    DownloadCandidate,
    ScrapingError,
)


@pytest.fixture
def mock_redis():
    """Mock Redis client."""
    redis_mock = Mock()
    redis_mock.ping.return_value = True
    redis_mock.get.return_value = None
    redis_mock.set.return_value = True
    return redis_mock


@pytest.fixture
def collector(mock_redis):
    """Create a MisoBindingConstraintsSubregionalCollector instance."""
    return MisoBindingConstraintsSubregionalCollector(
        start_date=datetime(2025, 12, 5, 12, 0, 0),
        end_date=datetime(2025, 12, 5, 12, 15, 0),  # 15 minutes = 4 snapshots
        dgroup="miso_binding_constraints_subregional",
        s3_bucket="test-bucket",
        s3_prefix="sourcing",
        redis_client=mock_redis,
        environment="dev",
    )


@pytest.fixture
def sample_api_response():
    """Sample API response matching MISO Sub-Regional Binding Constraints format."""
    return {
        "RefId": "05-Dec-2025 - Interval 12:30 EST",
        "SubRegionalConstraints": [
            {
                "Name": "MINN_METRO_WEST_01",
                "Region": "Minnesota",
                "Subregion": "Metro West",
                "ConstraintType": "TRANSMISSION_CAPACITY",
                "Period": "2025-12-05T12:30:00Z",
                "Price": 12.50,
                "ConstraintValue": 450.0,
                "ConstraintLimit": 500.0,
                "Utilization": 90.0,
                "OVERRIDE": False
            },
            {
                "Name": "ILL_URBAN_EAST_02",
                "Region": "Illinois",
                "Subregion": "Urban East",
                "ConstraintType": "VOLTAGE_LIMITATION",
                "Period": "2025-12-05T12:30:00Z",
                "Price": 8.75,
                "ConstraintValue": 320.0,
                "ConstraintLimit": 400.0,
                "Utilization": 80.0,
                "OVERRIDE": False
            },
            {
                "Name": "WISC_NORTH_03",
                "Region": "Wisconsin",
                "Subregion": "Northern Plains",
                "ConstraintType": "GENERATION_INTERCONNECTION",
                "Period": "2025-12-05T12:30:00Z",
                "Price": 15.25,
                "ConstraintValue": 580.0,
                "ConstraintLimit": 600.0,
                "Utilization": 96.67,
                "OVERRIDE": True
            }
        ]
    }


@pytest.fixture
def sample_api_response_empty():
    """Sample API response with no active constraints."""
    return {
        "RefId": "05-Dec-2025 - Interval 12:30 EST",
        "SubRegionalConstraints": []
    }


@pytest.fixture
def sample_candidate():
    """Sample DownloadCandidate for testing."""
    return DownloadCandidate(
        identifier="binding_constraints_subregional_20251205_120000.json",
        source_location="https://public-api.misoenergy.org/api/BindingConstraints/SubRegional",
        metadata={
            "data_type": "binding_constraints_subregional",
            "source": "miso",
            "date": "2025-12-05",
            "date_formatted": "20251205",
            "timestamp": "20251205_120000",
            "market_type": "real_time_constraints",
            "constraint_scope": "subregional",
        },
        collection_params={
            "headers": {
                "Accept": "application/json",
                "User-Agent": "MISO-Binding-Constraints-SubRegional-Collector/1.0",
            },
            "timeout": 30,
        },
        file_date=date(2025, 12, 5),
    )


class TestMisoBindingConstraintsSubregionalCollector:
    """Test suite for MisoBindingConstraintsSubregionalCollector."""

    def test_initialization(self, collector):
        """Test collector initialization."""
        assert collector.start_date == datetime(2025, 12, 5, 12, 0, 0)
        assert collector.end_date == datetime(2025, 12, 5, 12, 15, 0)
        assert collector.dgroup == "miso_binding_constraints_subregional"
        assert collector.BASE_URL == "https://public-api.misoenergy.org/api/BindingConstraints/SubRegional"

    def test_generate_candidates(self, collector):
        """Test candidate generation for date range."""
        candidates = collector.generate_candidates()

        # Should generate 4 candidates: 12:00, 12:05, 12:10, 12:15 (inclusive)
        assert len(candidates) == 4

        # Check first candidate
        first = candidates[0]
        assert first.identifier == "binding_constraints_subregional_20251205_120000.json"
        assert first.source_location == "https://public-api.misoenergy.org/api/BindingConstraints/SubRegional"
        assert first.metadata["data_type"] == "binding_constraints_subregional"
        assert first.metadata["source"] == "miso"
        assert first.metadata["date"] == "2025-12-05"
        assert first.metadata["constraint_scope"] == "subregional"
        assert first.file_date == date(2025, 12, 5)

        # Check collection params
        assert "Accept" in first.collection_params["headers"]
        assert first.collection_params["headers"]["Accept"] == "application/json"
        assert first.collection_params["timeout"] == 30

        # Check second candidate (5 minutes later)
        second = candidates[1]
        assert second.identifier == "binding_constraints_subregional_20251205_120500.json"
        assert second.metadata["timestamp"] == "20251205_120500"

    def test_generate_candidates_single_snapshot(self, mock_redis):
        """Test candidate generation for single snapshot."""
        collector = MisoBindingConstraintsSubregionalCollector(
            start_date=datetime(2025, 12, 5, 12, 0, 0),
            end_date=datetime(2025, 12, 5, 12, 0, 0),
            dgroup="miso_binding_constraints_subregional",
            s3_bucket="test-bucket",
            s3_prefix="sourcing",
            redis_client=mock_redis,
            environment="dev",
        )

        candidates = collector.generate_candidates()

        # Should generate exactly 1 candidate
        assert len(candidates) == 1
        assert candidates[0].identifier == "binding_constraints_subregional_20251205_120000.json"

    @patch('requests.get')
    def test_collect_content_success(self, mock_get, collector, sample_candidate, sample_api_response):
        """Test successful content collection."""
        # Mock API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_api_response
        mock_get.return_value = mock_response

        # Collect content
        content = collector.collect_content(sample_candidate)

        # Verify request
        mock_get.assert_called_once()
        call_args = mock_get.call_args
        assert call_args[0][0] == "https://public-api.misoenergy.org/api/BindingConstraints/SubRegional"
        assert call_args[1]["headers"]["Accept"] == "application/json"
        assert call_args[1]["timeout"] == 30

        # Verify content
        result = json.loads(content.decode('utf-8'))
        assert "data" in result
        assert "collection_metadata" in result
        assert result["data"]["RefId"] == "05-Dec-2025 - Interval 12:30 EST"
        assert len(result["data"]["SubRegionalConstraints"]) == 3

        # Verify constraint data preserved
        constraints = result["data"]["SubRegionalConstraints"]
        assert constraints[0]["Name"] == "MINN_METRO_WEST_01"
        assert constraints[0]["Region"] == "Minnesota"
        assert constraints[0]["Utilization"] == 90.0

    @patch('requests.get')
    def test_collect_content_empty_constraints(self, mock_get, collector, sample_candidate, sample_api_response_empty):
        """Test collection with no active constraints."""
        # Mock API response with empty constraints
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_api_response_empty
        mock_get.return_value = mock_response

        # Collect content
        content = collector.collect_content(sample_candidate)

        # Verify content
        result = json.loads(content.decode('utf-8'))
        assert result["data"]["RefId"] == "05-Dec-2025 - Interval 12:30 EST"
        assert len(result["data"]["SubRegionalConstraints"]) == 0

    @patch('requests.get')
    def test_collect_content_http_404(self, mock_get, collector, sample_candidate):
        """Test handling of 404 response."""
        # Mock 404 response
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_response)
        mock_get.return_value = mock_response

        # Should raise ScrapingError
        with pytest.raises(ScrapingError, match="Data not available"):
            collector.collect_content(sample_candidate)

    @patch('requests.get')
    def test_collect_content_http_429(self, mock_get, collector, sample_candidate):
        """Test handling of 429 rate limit."""
        # Mock 429 response
        mock_response = Mock()
        mock_response.status_code = 429
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_response)
        mock_get.return_value = mock_response

        # Should raise ScrapingError
        with pytest.raises(ScrapingError, match="Rate limit exceeded"):
            collector.collect_content(sample_candidate)

    @patch('requests.get')
    def test_collect_content_http_503(self, mock_get, collector, sample_candidate):
        """Test handling of 503 service unavailable."""
        # Mock 503 response
        mock_response = Mock()
        mock_response.status_code = 503
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_response)
        mock_get.return_value = mock_response

        # Should raise ScrapingError
        with pytest.raises(ScrapingError, match="Service unavailable"):
            collector.collect_content(sample_candidate)

    @patch('requests.get')
    def test_collect_content_invalid_json(self, mock_get, collector, sample_candidate):
        """Test handling of invalid JSON response."""
        # Mock response with invalid JSON
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.side_effect = json.JSONDecodeError("Invalid JSON", "", 0)
        mock_get.return_value = mock_response

        # Should raise ScrapingError
        with pytest.raises(ScrapingError, match="Invalid JSON response"):
            collector.collect_content(sample_candidate)

    @patch('requests.get')
    def test_collect_content_timeout(self, mock_get, collector, sample_candidate):
        """Test handling of request timeout."""
        # Mock timeout
        mock_get.side_effect = requests.exceptions.Timeout("Request timed out")

        # Should raise ScrapingError
        with pytest.raises(ScrapingError, match="Failed to fetch"):
            collector.collect_content(sample_candidate)

    def test_validate_content_success(self, collector, sample_candidate, sample_api_response):
        """Test validation of valid content."""
        # Create valid content
        result = {
            "data": sample_api_response,
            "collection_metadata": {
                "collected_at": "2025-12-05T12:35:00",
                "source_url": "https://public-api.misoenergy.org/api/BindingConstraints/SubRegional"
            }
        }
        content = json.dumps(result).encode('utf-8')

        # Should validate successfully
        assert collector.validate_content(content, sample_candidate) is True

    def test_validate_content_empty_constraints(self, collector, sample_candidate, sample_api_response_empty):
        """Test validation of response with no active constraints."""
        # Create valid content with empty constraints
        result = {
            "data": sample_api_response_empty,
            "collection_metadata": {
                "collected_at": "2025-12-05T12:35:00",
                "source_url": "https://public-api.misoenergy.org/api/BindingConstraints/SubRegional"
            }
        }
        content = json.dumps(result).encode('utf-8')

        # Should validate successfully (empty constraints is valid)
        assert collector.validate_content(content, sample_candidate) is True

    def test_validate_content_missing_data_field(self, collector, sample_candidate):
        """Test validation failure when 'data' field is missing."""
        content = json.dumps({"wrong_field": {}}).encode('utf-8')

        assert collector.validate_content(content, sample_candidate) is False

    def test_validate_content_missing_refid(self, collector, sample_candidate):
        """Test validation failure when RefId is missing."""
        result = {
            "data": {
                "SubRegionalConstraints": []
            }
        }
        content = json.dumps(result).encode('utf-8')

        assert collector.validate_content(content, sample_candidate) is False

    def test_validate_content_missing_constraints_array(self, collector, sample_candidate):
        """Test validation failure when SubRegionalConstraints array is missing."""
        result = {
            "data": {
                "RefId": "05-Dec-2025 - Interval 12:30 EST"
                # Missing SubRegionalConstraints
            }
        }
        content = json.dumps(result).encode('utf-8')

        assert collector.validate_content(content, sample_candidate) is False

    def test_validate_content_constraints_not_list(self, collector, sample_candidate):
        """Test validation failure when SubRegionalConstraints is not a list."""
        result = {
            "data": {
                "RefId": "05-Dec-2025 - Interval 12:30 EST",
                "SubRegionalConstraints": "not_a_list"
            }
        }
        content = json.dumps(result).encode('utf-8')

        assert collector.validate_content(content, sample_candidate) is False

    def test_validate_content_missing_constraint_field(self, collector, sample_candidate):
        """Test validation failure when constraint is missing required field."""
        result = {
            "data": {
                "RefId": "05-Dec-2025 - Interval 12:30 EST",
                "SubRegionalConstraints": [
                    {
                        "Name": "TEST_CONSTRAINT",
                        "Region": "Test",
                        # Missing Subregion and other fields
                    }
                ]
            }
        }
        content = json.dumps(result).encode('utf-8')

        assert collector.validate_content(content, sample_candidate) is False

    def test_validate_content_invalid_field_types(self, collector, sample_candidate):
        """Test validation failure for invalid field types."""
        result = {
            "data": {
                "RefId": "05-Dec-2025 - Interval 12:30 EST",
                "SubRegionalConstraints": [
                    {
                        "Name": 12345,  # Should be string
                        "Region": "Test",
                        "Subregion": "Test",
                        "ConstraintType": "TRANSMISSION_CAPACITY",
                        "Period": "2025-12-05T12:30:00Z",
                        "Price": 12.50,
                        "ConstraintValue": 450.0,
                        "ConstraintLimit": 500.0,
                        "Utilization": 90.0,
                        "OVERRIDE": False
                    }
                ]
            }
        }
        content = json.dumps(result).encode('utf-8')

        assert collector.validate_content(content, sample_candidate) is False

    def test_validate_content_non_numeric_price(self, collector, sample_candidate):
        """Test validation failure for non-numeric Price field."""
        result = {
            "data": {
                "RefId": "05-Dec-2025 - Interval 12:30 EST",
                "SubRegionalConstraints": [
                    {
                        "Name": "TEST_CONSTRAINT",
                        "Region": "Test",
                        "Subregion": "Test",
                        "ConstraintType": "TRANSMISSION_CAPACITY",
                        "Period": "2025-12-05T12:30:00Z",
                        "Price": "not_a_number",
                        "ConstraintValue": 450.0,
                        "ConstraintLimit": 500.0,
                        "Utilization": 90.0,
                        "OVERRIDE": False
                    }
                ]
            }
        }
        content = json.dumps(result).encode('utf-8')

        assert collector.validate_content(content, sample_candidate) is False

    def test_validate_content_non_numeric_utilization(self, collector, sample_candidate):
        """Test validation failure for non-numeric Utilization field."""
        result = {
            "data": {
                "RefId": "05-Dec-2025 - Interval 12:30 EST",
                "SubRegionalConstraints": [
                    {
                        "Name": "TEST_CONSTRAINT",
                        "Region": "Test",
                        "Subregion": "Test",
                        "ConstraintType": "TRANSMISSION_CAPACITY",
                        "Period": "2025-12-05T12:30:00Z",
                        "Price": 12.50,
                        "ConstraintValue": 450.0,
                        "ConstraintLimit": 500.0,
                        "Utilization": "ninety",
                        "OVERRIDE": False
                    }
                ]
            }
        }
        content = json.dumps(result).encode('utf-8')

        assert collector.validate_content(content, sample_candidate) is False

    def test_validate_content_utilization_out_of_range(self, collector, sample_candidate):
        """Test validation warning for utilization out of expected range."""
        result = {
            "data": {
                "RefId": "05-Dec-2025 - Interval 12:30 EST",
                "SubRegionalConstraints": [
                    {
                        "Name": "TEST_CONSTRAINT",
                        "Region": "Test",
                        "Subregion": "Test",
                        "ConstraintType": "TRANSMISSION_CAPACITY",
                        "Period": "2025-12-05T12:30:00Z",
                        "Price": 12.50,
                        "ConstraintValue": 450.0,
                        "ConstraintLimit": 500.0,
                        "Utilization": 150.0,  # Over 100%
                        "OVERRIDE": False
                    }
                ]
            }
        }
        content = json.dumps(result).encode('utf-8')

        # Should still validate (warning logged but not failure)
        assert collector.validate_content(content, sample_candidate) is True

    def test_validate_content_invalid_override_type(self, collector, sample_candidate):
        """Test validation failure for invalid OVERRIDE type."""
        result = {
            "data": {
                "RefId": "05-Dec-2025 - Interval 12:30 EST",
                "SubRegionalConstraints": [
                    {
                        "Name": "TEST_CONSTRAINT",
                        "Region": "Test",
                        "Subregion": "Test",
                        "ConstraintType": "TRANSMISSION_CAPACITY",
                        "Period": "2025-12-05T12:30:00Z",
                        "Price": 12.50,
                        "ConstraintValue": 450.0,
                        "ConstraintLimit": 500.0,
                        "Utilization": 90.0,
                        "OVERRIDE": "yes"  # Should be boolean or 0/1
                    }
                ]
            }
        }
        content = json.dumps(result).encode('utf-8')

        assert collector.validate_content(content, sample_candidate) is False

    def test_validate_content_override_numeric_zero_one(self, collector, sample_candidate):
        """Test validation success for OVERRIDE as 0 or 1."""
        result = {
            "data": {
                "RefId": "05-Dec-2025 - Interval 12:30 EST",
                "SubRegionalConstraints": [
                    {
                        "Name": "TEST_CONSTRAINT",
                        "Region": "Test",
                        "Subregion": "Test",
                        "ConstraintType": "TRANSMISSION_CAPACITY",
                        "Period": "2025-12-05T12:30:00Z",
                        "Price": 12.50,
                        "ConstraintValue": 450.0,
                        "ConstraintLimit": 500.0,
                        "Utilization": 90.0,
                        "OVERRIDE": 1  # Numeric 1 should be accepted
                    }
                ]
            }
        }
        content = json.dumps(result).encode('utf-8')

        # Should validate successfully (0/1 accepted as boolean alternative)
        assert collector.validate_content(content, sample_candidate) is True

    def test_validate_content_multiple_constraints(self, collector, sample_candidate, sample_api_response):
        """Test validation of multiple constraints."""
        result = {
            "data": sample_api_response,
            "collection_metadata": {
                "collected_at": "2025-12-05T12:35:00",
                "source_url": "https://public-api.misoenergy.org/api/BindingConstraints/SubRegional"
            }
        }
        content = json.dumps(result).encode('utf-8')

        # Should validate all 3 constraints successfully
        assert collector.validate_content(content, sample_candidate) is True

    def test_validate_content_multiple_constraints_one_invalid(self, collector, sample_candidate):
        """Test validation failure when one of multiple constraints is invalid."""
        result = {
            "data": {
                "RefId": "05-Dec-2025 - Interval 12:30 EST",
                "SubRegionalConstraints": [
                    {
                        "Name": "VALID_CONSTRAINT",
                        "Region": "Test",
                        "Subregion": "Test",
                        "ConstraintType": "TRANSMISSION_CAPACITY",
                        "Period": "2025-12-05T12:30:00Z",
                        "Price": 12.50,
                        "ConstraintValue": 450.0,
                        "ConstraintLimit": 500.0,
                        "Utilization": 90.0,
                        "OVERRIDE": False
                    },
                    {
                        "Name": "INVALID_CONSTRAINT",
                        "Region": "Test",
                        # Missing required fields
                    }
                ]
            }
        }
        content = json.dumps(result).encode('utf-8')

        assert collector.validate_content(content, sample_candidate) is False

    def test_validate_content_invalid_json(self, collector, sample_candidate):
        """Test validation failure for invalid JSON."""
        content = b"not valid json"

        assert collector.validate_content(content, sample_candidate) is False
