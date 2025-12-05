"""Tests for MISO Real-Time Total Load scraper."""

import json
from datetime import datetime, date
from unittest.mock import Mock, patch

import pytest
import requests

from sourcing.scraping.miso.realtime_total_load.scraper_miso_realtime_total_load import (
    MisoRealtimeTotalLoadCollector,
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
    """Create a MisoRealtimeTotalLoadCollector instance."""
    return MisoRealtimeTotalLoadCollector(
        start_date=datetime(2025, 12, 5, 12, 0, 0),
        end_date=datetime(2025, 12, 5, 12, 15, 0),  # 15 minutes = 3 snapshots
        dgroup="miso_realtime_total_load",
        s3_bucket="test-bucket",
        s3_prefix="sourcing",
        redis_client=mock_redis,
        environment="dev",
    )


@pytest.fixture
def sample_api_response():
    """Sample API response matching MISO Real-Time Total Load format."""
    return {
        "RefId": "2025-12-05T12:35:00Z",
        "ClearedMW": [
            {"Hour": "12:00", "Value": 32456},
            {"Hour": "13:00", "Value": 34123},
            {"Hour": "14:00", "Value": 35789}
        ],
        "MediumTermLoadForecast": [
            {"HourEnding": "14:00", "LoadForecast": 35678},
            {"HourEnding": "15:00", "LoadForecast": 36245},
            {"HourEnding": "16:00", "LoadForecast": 37012}
        ],
        "FiveMinTotalLoad": [
            {"Time": "12:30:00", "Value": 33123},
            {"Time": "12:35:00", "Value": 33456},
            {"Time": "12:40:00", "Value": 33789}
        ]
    }


@pytest.fixture
def sample_candidate():
    """Sample DownloadCandidate for testing."""
    return DownloadCandidate(
        identifier="realtime_total_load_20251205_120000.json",
        source_location="https://public-api.misoenergy.org/api/RealTimeTotalLoad",
        metadata={
            "data_type": "realtime_total_load",
            "source": "miso",
            "date": "2025-12-05",
            "date_formatted": "20251205",
            "timestamp": "20251205_120000",
            "market_type": "real_time_load",
        },
        collection_params={
            "headers": {
                "Accept": "application/json",
                "User-Agent": "MISO-Realtime-Total-Load-Collector/1.0",
            },
            "timeout": 30,
        },
        file_date=date(2025, 12, 5),
    )


class TestMisoRealtimeTotalLoadCollector:
    """Test suite for MisoRealtimeTotalLoadCollector."""

    def test_initialization(self, collector):
        """Test collector initialization."""
        assert collector.start_date == datetime(2025, 12, 5, 12, 0, 0)
        assert collector.end_date == datetime(2025, 12, 5, 12, 15, 0)
        assert collector.dgroup == "miso_realtime_total_load"
        assert collector.BASE_URL == "https://public-api.misoenergy.org/api/RealTimeTotalLoad"

    def test_generate_candidates(self, collector):
        """Test candidate generation for date range."""
        candidates = collector.generate_candidates()

        # Should generate 4 candidates: 12:00, 12:05, 12:10, 12:15 (inclusive)
        assert len(candidates) == 4

        # Check first candidate
        first = candidates[0]
        assert first.identifier == "realtime_total_load_20251205_120000.json"
        assert first.source_location == "https://public-api.misoenergy.org/api/RealTimeTotalLoad"
        assert first.metadata["data_type"] == "realtime_total_load"
        assert first.metadata["source"] == "miso"
        assert first.metadata["date"] == "2025-12-05"
        assert first.file_date == date(2025, 12, 5)

        # Check collection params
        assert "Accept" in first.collection_params["headers"]
        assert first.collection_params["headers"]["Accept"] == "application/json"
        assert first.collection_params["timeout"] == 30

        # Check second candidate (5 minutes later)
        second = candidates[1]
        assert second.identifier == "realtime_total_load_20251205_120500.json"
        assert second.metadata["timestamp"] == "20251205_120500"

    def test_generate_candidates_single_snapshot(self, mock_redis):
        """Test candidate generation for single snapshot."""
        collector = MisoRealtimeTotalLoadCollector(
            start_date=datetime(2025, 12, 5, 12, 0, 0),
            end_date=datetime(2025, 12, 5, 12, 0, 0),
            dgroup="miso_realtime_total_load",
            s3_bucket="test-bucket",
            s3_prefix="sourcing",
            redis_client=mock_redis,
            environment="dev",
        )

        candidates = collector.generate_candidates()

        # Should generate exactly 1 candidate
        assert len(candidates) == 1
        assert candidates[0].identifier == "realtime_total_load_20251205_120000.json"

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
        assert call_args[0][0] == "https://public-api.misoenergy.org/api/RealTimeTotalLoad"
        assert call_args[1]["headers"]["Accept"] == "application/json"
        assert call_args[1]["timeout"] == 30

        # Verify content
        result = json.loads(content.decode('utf-8'))
        assert "data" in result
        assert "collection_metadata" in result
        assert result["data"]["RefId"] == "2025-12-05T12:35:00Z"
        assert len(result["data"]["ClearedMW"]) == 3
        assert len(result["data"]["MediumTermLoadForecast"]) == 3
        assert len(result["data"]["FiveMinTotalLoad"]) == 3

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
                "source_url": "https://public-api.misoenergy.org/api/RealTimeTotalLoad"
            }
        }
        content = json.dumps(result).encode('utf-8')

        # Should validate successfully
        assert collector.validate_content(content, sample_candidate) is True

    def test_validate_content_missing_data_field(self, collector, sample_candidate):
        """Test validation failure when 'data' field is missing."""
        content = json.dumps({"wrong_field": {}}).encode('utf-8')

        assert collector.validate_content(content, sample_candidate) is False

    def test_validate_content_missing_refid(self, collector, sample_candidate):
        """Test validation failure when RefId is missing."""
        result = {
            "data": {
                "ClearedMW": [],
                "MediumTermLoadForecast": [],
                "FiveMinTotalLoad": []
            }
        }
        content = json.dumps(result).encode('utf-8')

        assert collector.validate_content(content, sample_candidate) is False

    def test_validate_content_missing_array(self, collector, sample_candidate):
        """Test validation failure when required array is missing."""
        result = {
            "data": {
                "RefId": "2025-12-05T12:35:00Z",
                "ClearedMW": [],
                "MediumTermLoadForecast": []
                # Missing FiveMinTotalLoad
            }
        }
        content = json.dumps(result).encode('utf-8')

        assert collector.validate_content(content, sample_candidate) is False

    def test_validate_content_array_not_list(self, collector, sample_candidate):
        """Test validation failure when array field is not a list."""
        result = {
            "data": {
                "RefId": "2025-12-05T12:35:00Z",
                "ClearedMW": "not_a_list",
                "MediumTermLoadForecast": [],
                "FiveMinTotalLoad": []
            }
        }
        content = json.dumps(result).encode('utf-8')

        assert collector.validate_content(content, sample_candidate) is False

    def test_validate_content_cleared_mw_invalid_structure(self, collector, sample_candidate):
        """Test validation failure for invalid ClearedMW structure."""
        result = {
            "data": {
                "RefId": "2025-12-05T12:35:00Z",
                "ClearedMW": [
                    {"Hour": "12:00"}  # Missing Value
                ],
                "MediumTermLoadForecast": [],
                "FiveMinTotalLoad": []
            }
        }
        content = json.dumps(result).encode('utf-8')

        assert collector.validate_content(content, sample_candidate) is False

    def test_validate_content_cleared_mw_non_numeric_value(self, collector, sample_candidate):
        """Test validation failure for non-numeric ClearedMW value."""
        result = {
            "data": {
                "RefId": "2025-12-05T12:35:00Z",
                "ClearedMW": [
                    {"Hour": "12:00", "Value": "not_a_number"}
                ],
                "MediumTermLoadForecast": [],
                "FiveMinTotalLoad": []
            }
        }
        content = json.dumps(result).encode('utf-8')

        assert collector.validate_content(content, sample_candidate) is False

    def test_validate_content_forecast_invalid_structure(self, collector, sample_candidate):
        """Test validation failure for invalid MediumTermLoadForecast structure."""
        result = {
            "data": {
                "RefId": "2025-12-05T12:35:00Z",
                "ClearedMW": [],
                "MediumTermLoadForecast": [
                    {"HourEnding": "14:00"}  # Missing LoadForecast
                ],
                "FiveMinTotalLoad": []
            }
        }
        content = json.dumps(result).encode('utf-8')

        assert collector.validate_content(content, sample_candidate) is False

    def test_validate_content_forecast_non_numeric_value(self, collector, sample_candidate):
        """Test validation failure for non-numeric LoadForecast value."""
        result = {
            "data": {
                "RefId": "2025-12-05T12:35:00Z",
                "ClearedMW": [],
                "MediumTermLoadForecast": [
                    {"HourEnding": "14:00", "LoadForecast": "invalid"}
                ],
                "FiveMinTotalLoad": []
            }
        }
        content = json.dumps(result).encode('utf-8')

        assert collector.validate_content(content, sample_candidate) is False

    def test_validate_content_five_min_load_invalid_structure(self, collector, sample_candidate):
        """Test validation failure for invalid FiveMinTotalLoad structure."""
        result = {
            "data": {
                "RefId": "2025-12-05T12:35:00Z",
                "ClearedMW": [],
                "MediumTermLoadForecast": [],
                "FiveMinTotalLoad": [
                    {"Time": "12:35:00"}  # Missing Value
                ]
            }
        }
        content = json.dumps(result).encode('utf-8')

        assert collector.validate_content(content, sample_candidate) is False

    def test_validate_content_five_min_load_non_numeric_value(self, collector, sample_candidate):
        """Test validation failure for non-numeric FiveMinTotalLoad value."""
        result = {
            "data": {
                "RefId": "2025-12-05T12:35:00Z",
                "ClearedMW": [],
                "MediumTermLoadForecast": [],
                "FiveMinTotalLoad": [
                    {"Time": "12:35:00", "Value": "not_numeric"}
                ]
            }
        }
        content = json.dumps(result).encode('utf-8')

        assert collector.validate_content(content, sample_candidate) is False

    def test_validate_content_empty_arrays_valid(self, collector, sample_candidate):
        """Test validation succeeds with empty arrays (valid scenario)."""
        result = {
            "data": {
                "RefId": "2025-12-05T12:35:00Z",
                "ClearedMW": [],
                "MediumTermLoadForecast": [],
                "FiveMinTotalLoad": []
            }
        }
        content = json.dumps(result).encode('utf-8')

        # Empty arrays are valid - API might return empty data
        assert collector.validate_content(content, sample_candidate) is True

    def test_validate_content_invalid_json(self, collector, sample_candidate):
        """Test validation failure for invalid JSON."""
        content = b"not valid json"

        assert collector.validate_content(content, sample_candidate) is False
