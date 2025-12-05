"""Tests for MISO CSAT Supply & Demand scraper."""

import json
from datetime import datetime, timedelta
from unittest.mock import MagicMock, Mock, patch

import pytest
import requests

from sourcing.scraping.miso.csat_supply_demand.scraper_miso_csat_supply_demand import (
    MisoCsatSupplyDemandCollector,
)
from sourcing.infrastructure.collection_framework import ScrapingError


@pytest.fixture
def mock_redis():
    """Create mock Redis client."""
    redis_client = MagicMock()
    redis_client.ping.return_value = True
    return redis_client


@pytest.fixture
def collector(mock_redis):
    """Create collector instance for testing."""
    return MisoCsatSupplyDemandCollector(
        start_datetime=datetime(2025, 12, 5, 10, 0, 0),
        end_datetime=datetime(2025, 12, 5, 11, 0, 0),
        region=None,
        dgroup="miso_csat_supply_demand",
        s3_bucket="test-bucket",
        s3_prefix="sourcing",
        redis_client=mock_redis,
        environment="dev",
    )


@pytest.fixture
def sample_api_response():
    """Sample API response matching expected structure."""
    return {
        "timestamp": "2025-12-05T10:00:00-05:00",
        "region": "MISO_TOTAL",
        "data": {
            "actualDemand": {
                "value": 75000,
                "unit": "MW",
                "timestamp": "2025-12-05T10:00:00-05:00"
            },
            "committedCapacity": {
                "value": 85000,
                "unit": "MW",
                "timestamp": "2025-12-05T10:00:00-05:00"
            },
            "availableCapacity": {
                "value": 10000,
                "unit": "MW",
                "timestamp": "2025-12-05T10:00:00-05:00",
                "calculationType": "committed_minus_demand"
            },
            "demandForecast": {
                "value": 78000,
                "unit": "MW",
                "timestamp": "2025-12-05T10:00:00-05:00",
                "forecastHorizon": "24h"
            },
            "committedCapacityForecast": {
                "value": 87000,
                "unit": "MW",
                "timestamp": "2025-12-05T10:00:00-05:00",
                "forecastHorizon": "24h"
            }
        },
        "adequacyScore": {
            "value": 0.85,
            "interpretation": "Adequate capacity margin",
            "riskLevel": "LOW"
        }
    }


class TestMisoCsatSupplyDemandCollector:
    """Test suite for MisoCsatSupplyDemandCollector."""

    def test_generate_candidates_single_interval(self, collector):
        """Test candidate generation for single 15-minute interval."""
        collector.end_datetime = collector.start_datetime  # Single snapshot

        candidates = collector.generate_candidates()

        assert len(candidates) == 1
        candidate = candidates[0]
        assert candidate.identifier == "csat_supply_demand_20251205T100000Z.json"
        assert candidate.source_location == "https://public-api.misoenergy.org/api/CsatSupplyDemand"
        assert candidate.metadata["data_type"] == "csat_supply_demand"
        assert candidate.metadata["source"] == "miso"
        assert candidate.metadata["region"] == "MISO_TOTAL"
        assert candidate.file_date == datetime(2025, 12, 5).date()

    def test_generate_candidates_multiple_intervals(self, collector):
        """Test candidate generation for multiple 15-minute intervals."""
        # 1 hour = 4 intervals (00, 15, 30, 45) + end point = 5 candidates
        candidates = collector.generate_candidates()

        assert len(candidates) == 5  # 10:00, 10:15, 10:30, 10:45, 11:00
        assert candidates[0].identifier == "csat_supply_demand_20251205T100000Z.json"
        assert candidates[1].identifier == "csat_supply_demand_20251205T101500Z.json"
        assert candidates[2].identifier == "csat_supply_demand_20251205T103000Z.json"
        assert candidates[3].identifier == "csat_supply_demand_20251205T104500Z.json"
        assert candidates[4].identifier == "csat_supply_demand_20251205T110000Z.json"

    def test_generate_candidates_with_region(self, mock_redis):
        """Test candidate generation with region filter."""
        collector = MisoCsatSupplyDemandCollector(
            start_datetime=datetime(2025, 12, 5, 10, 0, 0),
            end_datetime=datetime(2025, 12, 5, 10, 0, 0),
            region="SOUTH",
            dgroup="miso_csat_supply_demand",
            s3_bucket="test-bucket",
            s3_prefix="sourcing",
            redis_client=mock_redis,
            environment="dev",
        )

        candidates = collector.generate_candidates()

        assert len(candidates) == 1
        candidate = candidates[0]
        assert candidate.identifier == "csat_supply_demand_20251205T100000Z_south.json"
        assert candidate.metadata["region"] == "SOUTH"
        assert candidate.collection_params["query_params"] == {"region": "SOUTH"}

    def test_collect_content_success(self, collector, sample_api_response):
        """Test successful content collection."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_api_response

        with patch('requests.get', return_value=mock_response):
            candidates = collector.generate_candidates()
            content = collector.collect_content(candidates[0])

        assert content is not None
        data = json.loads(content.decode('utf-8'))
        assert "collection_timestamp" in data
        assert "api_response" in data
        assert data["api_response"] == sample_api_response
        assert data["metadata"]["data_type"] == "csat_supply_demand"

    def test_collect_content_404_no_data(self, collector):
        """Test handling of 404 (no data available)."""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            response=mock_response
        )

        with patch('requests.get', return_value=mock_response):
            candidates = collector.generate_candidates()
            content = collector.collect_content(candidates[0])

        # Should return empty response instead of raising error
        data = json.loads(content.decode('utf-8'))
        assert data["api_response"] is None
        assert "No data available" in data["note"]

    def test_collect_content_http_error(self, collector):
        """Test handling of HTTP errors (non-404)."""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            response=mock_response
        )

        with patch('requests.get', return_value=mock_response):
            candidates = collector.generate_candidates()

            with pytest.raises(ScrapingError, match="HTTP error fetching CSAT data"):
                collector.collect_content(candidates[0])

    def test_collect_content_timeout(self, collector):
        """Test handling of request timeout."""
        with patch('requests.get', side_effect=requests.exceptions.Timeout()):
            candidates = collector.generate_candidates()

            with pytest.raises(ScrapingError, match="Failed to fetch CSAT data"):
                collector.collect_content(candidates[0])

    def test_collect_content_invalid_json(self, collector):
        """Test handling of invalid JSON response."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.side_effect = json.JSONDecodeError("Expecting value", "", 0)

        with patch('requests.get', return_value=mock_response):
            candidates = collector.generate_candidates()

            with pytest.raises(ScrapingError, match="Invalid JSON response"):
                collector.collect_content(candidates[0])

    def test_validate_content_valid_data(self, collector, sample_api_response):
        """Test validation of valid content."""
        augmented_data = {
            "collection_timestamp": "2025-12-05T10:00:00Z",
            "api_response": sample_api_response,
            "metadata": {}
        }
        content = json.dumps(augmented_data).encode('utf-8')

        candidates = collector.generate_candidates()
        is_valid = collector.validate_content(content, candidates[0])

        assert is_valid is True

    def test_validate_content_null_response(self, collector):
        """Test validation of null API response (404 case)."""
        augmented_data = {
            "collection_timestamp": "2025-12-05T10:00:00Z",
            "api_response": None,
            "metadata": {},
            "note": "No data available"
        }
        content = json.dumps(augmented_data).encode('utf-8')

        candidates = collector.generate_candidates()
        is_valid = collector.validate_content(content, candidates[0])

        assert is_valid is True  # Null response is valid

    def test_validate_content_missing_api_response(self, collector):
        """Test validation fails when api_response field is missing."""
        content = json.dumps({"collection_timestamp": "2025-12-05T10:00:00Z"}).encode('utf-8')

        candidates = collector.generate_candidates()
        is_valid = collector.validate_content(content, candidates[0])

        assert is_valid is False

    def test_validate_content_missing_required_fields(self, collector, sample_api_response):
        """Test validation fails when required fields are missing."""
        # Remove required field
        del sample_api_response["adequacyScore"]

        augmented_data = {
            "collection_timestamp": "2025-12-05T10:00:00Z",
            "api_response": sample_api_response,
            "metadata": {}
        }
        content = json.dumps(augmented_data).encode('utf-8')

        candidates = collector.generate_candidates()
        is_valid = collector.validate_content(content, candidates[0])

        assert is_valid is False

    def test_validate_content_capacity_less_than_demand(self, collector, sample_api_response):
        """Test validation fails when committedCapacity < actualDemand."""
        # Make capacity less than demand (invalid condition)
        sample_api_response["data"]["committedCapacity"]["value"] = 70000
        sample_api_response["data"]["actualDemand"]["value"] = 75000

        augmented_data = {
            "collection_timestamp": "2025-12-05T10:00:00Z",
            "api_response": sample_api_response,
            "metadata": {}
        }
        content = json.dumps(augmented_data).encode('utf-8')

        candidates = collector.generate_candidates()
        is_valid = collector.validate_content(content, candidates[0])

        assert is_valid is False

    def test_validate_content_negative_values(self, collector, sample_api_response):
        """Test validation fails when values are negative."""
        sample_api_response["data"]["actualDemand"]["value"] = -1000

        augmented_data = {
            "collection_timestamp": "2025-12-05T10:00:00Z",
            "api_response": sample_api_response,
            "metadata": {}
        }
        content = json.dumps(augmented_data).encode('utf-8')

        candidates = collector.generate_candidates()
        is_valid = collector.validate_content(content, candidates[0])

        assert is_valid is False

    def test_validate_content_invalid_unit(self, collector, sample_api_response):
        """Test validation fails when unit is not MW."""
        sample_api_response["data"]["actualDemand"]["unit"] = "kW"

        augmented_data = {
            "collection_timestamp": "2025-12-05T10:00:00Z",
            "api_response": sample_api_response,
            "metadata": {}
        }
        content = json.dumps(augmented_data).encode('utf-8')

        candidates = collector.generate_candidates()
        is_valid = collector.validate_content(content, candidates[0])

        assert is_valid is False

    def test_validate_content_adequacy_score_out_of_range(self, collector, sample_api_response):
        """Test validation fails when adequacyScore is out of [0.0, 1.0] range."""
        sample_api_response["adequacyScore"]["value"] = 1.5

        augmented_data = {
            "collection_timestamp": "2025-12-05T10:00:00Z",
            "api_response": sample_api_response,
            "metadata": {}
        }
        content = json.dumps(augmented_data).encode('utf-8')

        candidates = collector.generate_candidates()
        is_valid = collector.validate_content(content, candidates[0])

        assert is_valid is False

    def test_validate_content_missing_forecast_horizon(self, collector, sample_api_response):
        """Test validation fails when forecastHorizon is missing."""
        del sample_api_response["data"]["demandForecast"]["forecastHorizon"]

        augmented_data = {
            "collection_timestamp": "2025-12-05T10:00:00Z",
            "api_response": sample_api_response,
            "metadata": {}
        }
        content = json.dumps(augmented_data).encode('utf-8')

        candidates = collector.generate_candidates()
        is_valid = collector.validate_content(content, candidates[0])

        assert is_valid is False

    def test_validate_content_available_capacity_mismatch(self, collector, sample_api_response):
        """Test validation warns but passes when availableCapacity doesn't match formula."""
        # Set availableCapacity to incorrect value (should be 85000 - 75000 = 10000)
        sample_api_response["data"]["availableCapacity"]["value"] = 12000

        augmented_data = {
            "collection_timestamp": "2025-12-05T10:00:00Z",
            "api_response": sample_api_response,
            "metadata": {}
        }
        content = json.dumps(augmented_data).encode('utf-8')

        candidates = collector.generate_candidates()
        is_valid = collector.validate_content(content, candidates[0])

        # Should still pass validation (warning only)
        assert is_valid is True

    def test_validate_content_invalid_json(self, collector):
        """Test validation fails for invalid JSON."""
        content = b"not valid json"

        candidates = collector.generate_candidates()
        is_valid = collector.validate_content(content, candidates[0])

        assert is_valid is False

    def test_validate_content_adequacy_score_boundary_values(self, collector, sample_api_response):
        """Test validation accepts boundary values for adequacyScore (0.0 and 1.0)."""
        # Test lower boundary
        sample_api_response["adequacyScore"]["value"] = 0.0
        augmented_data = {
            "collection_timestamp": "2025-12-05T10:00:00Z",
            "api_response": sample_api_response,
            "metadata": {}
        }
        content = json.dumps(augmented_data).encode('utf-8')
        candidates = collector.generate_candidates()
        assert collector.validate_content(content, candidates[0]) is True

        # Test upper boundary
        sample_api_response["adequacyScore"]["value"] = 1.0
        augmented_data["api_response"] = sample_api_response
        content = json.dumps(augmented_data).encode('utf-8')
        assert collector.validate_content(content, candidates[0]) is True

    def test_validate_content_zero_values_allowed(self, collector, sample_api_response):
        """Test validation allows zero values for capacity/demand."""
        sample_api_response["data"]["actualDemand"]["value"] = 0
        sample_api_response["data"]["committedCapacity"]["value"] = 0
        sample_api_response["data"]["availableCapacity"]["value"] = 0

        augmented_data = {
            "collection_timestamp": "2025-12-05T10:00:00Z",
            "api_response": sample_api_response,
            "metadata": {}
        }
        content = json.dumps(augmented_data).encode('utf-8')

        candidates = collector.generate_candidates()
        is_valid = collector.validate_content(content, candidates[0])

        assert is_valid is True

    @patch('sourcing.infrastructure.collection_framework.BaseCollector._upload_to_s3')
    @patch('requests.get')
    def test_run_collection_success(self, mock_get, mock_upload, collector, sample_api_response):
        """Test successful end-to-end collection."""
        # Setup mocks
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_api_response
        mock_get.return_value = mock_response
        mock_upload.return_value = ("version123", "etag123")

        # Mock hash registry to allow collection
        collector.hash_registry.exists = Mock(return_value=False)
        collector.hash_registry.register = Mock()

        # Run collection with single interval
        collector.end_datetime = collector.start_datetime
        results = collector.run_collection()

        assert results["total_candidates"] == 1
        assert results["collected"] == 1
        assert results["failed"] == 0
        assert results["skipped_duplicate"] == 0

    @patch('requests.get')
    def test_run_collection_handles_validation_failure(self, mock_get, collector):
        """Test that collection handles validation failures gracefully."""
        # Return invalid data
        invalid_response = {"invalid": "structure"}
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = invalid_response
        mock_get.return_value = mock_response

        collector.end_datetime = collector.start_datetime
        results = collector.run_collection()

        assert results["total_candidates"] == 1
        assert results["collected"] == 0
        assert results["failed"] == 1
        assert len(results["errors"]) == 1
        assert "validation failed" in results["errors"][0]["error"].lower()
