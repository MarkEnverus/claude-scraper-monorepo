"""Tests for MISO NSI Five-Minute Data Scraper."""

import json
from datetime import date, datetime
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
import requests

from sourcing.infrastructure.collection_framework import DownloadCandidate, ScrapingError
from sourcing.scraping.miso.nsi_five_minute.scraper_miso_nsi_five_minute import (
    MisoNsiFiveMinuteCollector,
)

# Fixtures directory
FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def sample_nsi_response():
    """Load sample NSI API response from fixture file."""
    fixture_path = FIXTURES_DIR / "sample_nsi_response.json"
    with open(fixture_path, 'r') as f:
        return json.load(f)


@pytest.fixture
def sample_nsi_response_bytes(sample_nsi_response):
    """Return sample response as bytes."""
    return json.dumps(sample_nsi_response).encode('utf-8')


@pytest.fixture
def collector():
    """Create a MisoNsiFiveMinuteCollector instance for testing."""
    mock_redis = Mock()
    mock_redis.ping.return_value = True

    return MisoNsiFiveMinuteCollector(
        dgroup="test_miso_nsi_five_minute",
        s3_bucket="test-bucket",
        s3_prefix="sourcing",
        redis_client=mock_redis,
        environment="dev",
    )


class TestMisoNsiFiveMinuteCollector:
    """Test suite for MisoNsiFiveMinuteCollector."""

    def test_initialization(self, collector):
        """Test collector initializes with correct attributes."""
        assert collector.dgroup == "test_miso_nsi_five_minute"
        assert collector.s3_bucket == "test-bucket"
        assert collector.s3_prefix == "sourcing"
        assert collector.environment == "dev"
        assert collector.API_URL == "https://public-api.misoenergy.org/api/Interchange/GetNsi/FiveMinute"
        assert collector.TIMEOUT_SECONDS == 30

    def test_generate_candidates(self, collector):
        """Test candidate generation creates single candidate for current data."""
        candidates = collector.generate_candidates()

        # Should generate exactly one candidate (API returns current data only)
        assert len(candidates) == 1

        candidate = candidates[0]
        assert isinstance(candidate, DownloadCandidate)
        assert candidate.source_location == collector.API_URL
        assert "nsi_five_minute" in candidate.identifier

        # Check metadata
        assert candidate.metadata["data_type"] == "nsi_five_minute"
        assert candidate.metadata["source"] == "miso"
        assert candidate.metadata["iso"] == "MISO"
        assert candidate.metadata["data_category"] == "interchange"
        assert candidate.metadata["interval"] == "5_minute"
        assert "collection_timestamp" in candidate.metadata

        # Check collection params
        assert "headers" in candidate.collection_params
        assert candidate.collection_params["headers"]["Accept"] == "application/json"
        assert candidate.collection_params["headers"]["User-Agent"] == "MISO-NSI-Collector/1.0"
        assert candidate.collection_params["timeout"] == 30

        # Check file_date is set
        assert isinstance(candidate.file_date, date)

    def test_generate_candidates_timestamp_format(self, collector):
        """Test candidate ID includes properly formatted timestamp."""
        candidates = collector.generate_candidates()
        candidate = candidates[0]

        # Format: nsi_five_minute_YYYYMMDD_HHMM.json
        assert candidate.identifier.startswith("nsi_five_minute_")
        assert candidate.identifier.endswith(".json")

    @patch('requests.get')
    def test_collect_content_success(self, mock_get, collector, sample_nsi_response_bytes):
        """Test successful content collection from API."""
        # Setup mock response
        mock_response = Mock()
        mock_response.content = sample_nsi_response_bytes
        mock_response.status_code = 200
        mock_response.elapsed.total_seconds.return_value = 0.5
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        # Create candidate
        candidate = DownloadCandidate(
            identifier="test_candidate_20250121_1430.json",
            source_location=collector.API_URL,
            metadata={"test": "data"},
            collection_params={
                "headers": {"Accept": "application/json"},
                "timeout": 30,
            },
            file_date=date.today(),
        )

        # Collect content
        content = collector.collect_content(candidate)

        # Verify
        assert content == sample_nsi_response_bytes
        mock_get.assert_called_once_with(
            collector.API_URL,
            headers={"Accept": "application/json"},
            timeout=30,
        )

    @patch('requests.get')
    def test_collect_content_timeout(self, mock_get, collector):
        """Test handling of timeout during collection."""
        mock_get.side_effect = requests.Timeout("Connection timeout")

        candidate = DownloadCandidate(
            identifier="test_candidate_timeout.json",
            source_location=collector.API_URL,
            metadata={},
            collection_params={"headers": {}, "timeout": 30},
            file_date=date.today(),
        )

        with pytest.raises(ScrapingError) as exc_info:
            collector.collect_content(candidate)

        assert "Timeout" in str(exc_info.value)

    @patch('requests.get')
    def test_collect_content_http_error(self, mock_get, collector):
        """Test handling of HTTP errors during collection."""
        mock_response = Mock()
        mock_response.status_code = 503
        mock_response.raise_for_status.side_effect = requests.HTTPError(response=mock_response)
        mock_get.return_value = mock_response

        candidate = DownloadCandidate(
            identifier="test_candidate_http_error.json",
            source_location=collector.API_URL,
            metadata={},
            collection_params={"headers": {}, "timeout": 30},
            file_date=date.today(),
        )

        with pytest.raises(ScrapingError) as exc_info:
            collector.collect_content(candidate)

        assert "HTTP error" in str(exc_info.value)
        assert "503" in str(exc_info.value)

    @patch('requests.get')
    def test_collect_content_request_exception(self, mock_get, collector):
        """Test handling of generic request exceptions."""
        mock_get.side_effect = requests.RequestException("Network error")

        candidate = DownloadCandidate(
            identifier="test_candidate_network_error.json",
            source_location=collector.API_URL,
            metadata={},
            collection_params={"headers": {}, "timeout": 30},
            file_date=date.today(),
        )

        with pytest.raises(ScrapingError) as exc_info:
            collector.collect_content(candidate)

        assert "Failed to fetch NSI data" in str(exc_info.value)

    def test_validate_content_valid_dict(self, collector, sample_nsi_response_bytes):
        """Test validation of valid JSON dictionary response."""
        candidate = DownloadCandidate(
            identifier="test.json",
            source_location=collector.API_URL,
            metadata={},
            collection_params={},
            file_date=date.today(),
        )

        is_valid = collector.validate_content(sample_nsi_response_bytes, candidate)
        assert is_valid is True

    def test_validate_content_valid_array(self, collector):
        """Test validation of valid JSON array response."""
        array_response = json.dumps([
            {"Value": -542.5, "EffectiveTime": "2025-01-21T14:35:00"},
            {"Value": -550.2, "EffectiveTime": "2025-01-21T14:30:00"}
        ]).encode('utf-8')

        candidate = DownloadCandidate(
            identifier="test.json",
            source_location=collector.API_URL,
            metadata={},
            collection_params={},
            file_date=date.today(),
        )

        is_valid = collector.validate_content(array_response, candidate)
        assert is_valid is True

    def test_validate_content_missing_value_field(self, collector):
        """Test validation fails when value field is missing."""
        invalid_response = json.dumps({
            "EffectiveTime": "2025-01-21T14:35:00",
            "Unit": "MW",
            # Missing Value/NSI field
        }).encode('utf-8')

        candidate = DownloadCandidate(
            identifier="test.json",
            source_location=collector.API_URL,
            metadata={},
            collection_params={},
            file_date=date.today(),
        )

        is_valid = collector.validate_content(invalid_response, candidate)
        assert is_valid is False

    def test_validate_content_empty_array(self, collector):
        """Test validation fails for empty array."""
        empty_array = json.dumps([]).encode('utf-8')

        candidate = DownloadCandidate(
            identifier="test.json",
            source_location=collector.API_URL,
            metadata={},
            collection_params={},
            file_date=date.today(),
        )

        is_valid = collector.validate_content(empty_array, candidate)
        assert is_valid is False

    def test_validate_content_invalid_json(self, collector):
        """Test validation fails for invalid JSON."""
        invalid_json = b"This is not JSON"

        candidate = DownloadCandidate(
            identifier="test.json",
            source_location=collector.API_URL,
            metadata={},
            collection_params={},
            file_date=date.today(),
        )

        is_valid = collector.validate_content(invalid_json, candidate)
        assert is_valid is False

    def test_validate_content_wrong_type(self, collector):
        """Test validation fails for non-dict/non-list types."""
        wrong_type = json.dumps("just a string").encode('utf-8')

        candidate = DownloadCandidate(
            identifier="test.json",
            source_location=collector.API_URL,
            metadata={},
            collection_params={},
            file_date=date.today(),
        )

        is_valid = collector.validate_content(wrong_type, candidate)
        assert is_valid is False

    def test_validate_content_number_type(self, collector):
        """Test validation fails for number type."""
        number_type = json.dumps(123.45).encode('utf-8')

        candidate = DownloadCandidate(
            identifier="test.json",
            source_location=collector.API_URL,
            metadata={},
            collection_params={},
            file_date=date.today(),
        )

        is_valid = collector.validate_content(number_type, candidate)
        assert is_valid is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
