"""Tests for MISO Public API Ex-Ante LMP scraper.

Test coverage:
- Candidate generation with and without query parameters
- Content collection and API interaction
- Comprehensive validation rules (structure, LMP arithmetic, data types)
- Error handling for various HTTP error codes
- Integration with BaseCollector framework
"""

import json
import os
from datetime import datetime, UTC
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

from sourcing.infrastructure.collection_framework import ScrapingError
from sourcing.scraping.miso.public_exante_lmp.scraper_miso_public_exante_lmp import (
    MisoPublicExAnteLMPCollector,
)


@pytest.fixture
def mock_redis():
    """Mock Redis client."""
    return MagicMock()


@pytest.fixture
def collector(mock_redis):
    """Create collector instance with mocked dependencies."""
    return MisoPublicExAnteLMPCollector(
        dgroup="miso_public_exante_lmp",
        s3_bucket="test-bucket",
        s3_prefix="sourcing",
        redis_client=mock_redis,
        environment="dev",
    )


@pytest.fixture
def collector_with_params(mock_redis):
    """Create collector with query parameters."""
    return MisoPublicExAnteLMPCollector(
        start_time="2025-12-05T14:00:00Z",
        end_time="2025-12-05T18:00:00Z",
        hub="CINERGY",
        dgroup="miso_public_exante_lmp",
        s3_bucket="test-bucket",
        s3_prefix="sourcing",
        redis_client=mock_redis,
        environment="dev",
    )


@pytest.fixture
def sample_response():
    """Load sample API response from fixture file."""
    fixture_path = Path(__file__).parent / "fixtures" / "sample_response.json"
    with open(fixture_path, "r") as f:
        return json.load(f)


@pytest.fixture
def sample_response_bytes(sample_response):
    """Sample response as bytes."""
    return json.dumps(sample_response).encode("utf-8")


class TestCandidateGeneration:
    """Test candidate generation logic."""

    def test_generate_candidates_no_params(self, collector):
        """Test candidate generation without query parameters."""
        candidates = collector.generate_candidates()

        assert len(candidates) == 1
        candidate = candidates[0]

        # Check identifier format
        assert candidate.identifier.startswith("public_exante_lmp_")
        assert candidate.identifier.endswith(".json")

        # Check source location
        assert candidate.source_location == MisoPublicExAnteLMPCollector.API_URL

        # Check metadata
        assert candidate.metadata["data_type"] == "public_exante_lmp"
        assert candidate.metadata["source"] == "miso"
        assert "collection_timestamp" in candidate.metadata
        assert candidate.metadata["query_params"] == {}

        # Check collection params
        assert "headers" in candidate.collection_params
        assert candidate.collection_params["headers"]["Accept"] == "application/json"
        assert candidate.collection_params["timeout"] == 30
        assert candidate.collection_params["query_params"] == {}

    def test_generate_candidates_with_params(self, collector_with_params):
        """Test candidate generation with query parameters."""
        candidates = collector_with_params.generate_candidates()

        assert len(candidates) == 1
        candidate = candidates[0]

        # Check identifier includes hub
        assert "hub_CINERGY" in candidate.identifier

        # Check query parameters are included
        query_params = candidate.collection_params["query_params"]
        assert query_params["startTime"] == "2025-12-05T14:00:00Z"
        assert query_params["endTime"] == "2025-12-05T18:00:00Z"
        assert query_params["hub"] == "CINERGY"

        # Check metadata includes query params
        assert candidate.metadata["query_params"] == query_params

    def test_generate_candidates_file_date(self, collector):
        """Test that file_date is set to current date."""
        candidates = collector.generate_candidates()
        candidate = candidates[0]

        # File date should be today
        assert candidate.file_date == datetime.now(UTC).date()


class TestContentCollection:
    """Test content collection from API."""

    @patch("requests.get")
    def test_collect_content_success(self, mock_get, collector, sample_response_bytes):
        """Test successful content collection."""
        # Mock successful API response
        mock_response = MagicMock()
        mock_response.content = sample_response_bytes
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        # Generate candidate
        candidates = collector.generate_candidates()
        candidate = candidates[0]

        # Collect content
        content = collector.collect_content(candidate)

        # Verify request was made correctly
        mock_get.assert_called_once()
        call_args = mock_get.call_args
        assert call_args[0][0] == MisoPublicExAnteLMPCollector.API_URL
        assert call_args[1]["timeout"] == 30

        # Verify content
        assert content == sample_response_bytes

    @patch("requests.get")
    def test_collect_content_with_query_params(
        self, mock_get, collector_with_params, sample_response_bytes
    ):
        """Test content collection with query parameters."""
        mock_response = MagicMock()
        mock_response.content = sample_response_bytes
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        candidates = collector_with_params.generate_candidates()
        candidate = candidates[0]

        collector_with_params.collect_content(candidate)

        # Verify query params were passed
        call_args = mock_get.call_args
        assert call_args[1]["params"]["startTime"] == "2025-12-05T14:00:00Z"
        assert call_args[1]["params"]["endTime"] == "2025-12-05T18:00:00Z"
        assert call_args[1]["params"]["hub"] == "CINERGY"

    @patch("requests.get")
    def test_collect_content_http_400(self, mock_get, collector):
        """Test handling of 400 Bad Request."""
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_get.return_value = mock_response
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            response=mock_response
        )

        candidates = collector.generate_candidates()
        candidate = candidates[0]

        with pytest.raises(ScrapingError, match="HTTP error"):
            collector.collect_content(candidate)

    @patch("requests.get")
    def test_collect_content_http_404(self, mock_get, collector):
        """Test handling of 404 Not Found (no data available)."""
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            response=mock_response
        )

        candidates = collector.generate_candidates()
        candidate = candidates[0]

        with pytest.raises(ScrapingError, match="HTTP error"):
            collector.collect_content(candidate)

    @patch("requests.get")
    def test_collect_content_timeout(self, mock_get, collector):
        """Test handling of request timeout."""
        mock_get.side_effect = requests.exceptions.Timeout("Request timed out")

        candidates = collector.generate_candidates()
        candidate = candidates[0]

        with pytest.raises(ScrapingError, match="Failed to fetch"):
            collector.collect_content(candidate)


class TestContentValidation:
    """Test content validation logic."""

    def test_validate_content_success(self, collector, sample_response_bytes):
        """Test validation of valid content."""
        candidates = collector.generate_candidates()
        candidate = candidates[0]

        is_valid = collector.validate_content(sample_response_bytes, candidate)
        assert is_valid is True

    def test_validate_content_missing_timestamp(self, collector):
        """Test validation fails with missing timestamp."""
        invalid_data = {
            "updateInterval": "5m",
            "hubs": []
        }
        content = json.dumps(invalid_data).encode("utf-8")

        candidates = collector.generate_candidates()
        candidate = candidates[0]

        is_valid = collector.validate_content(content, candidate)
        assert is_valid is False

    def test_validate_content_missing_update_interval(self, collector):
        """Test validation fails with missing updateInterval."""
        invalid_data = {
            "timestamp": "2025-12-05T14:30:00Z",
            "hubs": []
        }
        content = json.dumps(invalid_data).encode("utf-8")

        candidates = collector.generate_candidates()
        candidate = candidates[0]

        is_valid = collector.validate_content(content, candidate)
        assert is_valid is False

    def test_validate_content_missing_hubs(self, collector):
        """Test validation fails with missing hubs array."""
        invalid_data = {
            "timestamp": "2025-12-05T14:30:00Z",
            "updateInterval": "5m"
        }
        content = json.dumps(invalid_data).encode("utf-8")

        candidates = collector.generate_candidates()
        candidate = candidates[0]

        is_valid = collector.validate_content(content, candidate)
        assert is_valid is False

    def test_validate_content_invalid_timestamp(self, collector):
        """Test validation fails with invalid timestamp format."""
        invalid_data = {
            "timestamp": "not-a-timestamp",
            "updateInterval": "5m",
            "hubs": []
        }
        content = json.dumps(invalid_data).encode("utf-8")

        candidates = collector.generate_candidates()
        candidate = candidates[0]

        is_valid = collector.validate_content(content, candidate)
        assert is_valid is False

    def test_validate_content_hubs_not_array(self, collector):
        """Test validation fails when hubs is not an array."""
        invalid_data = {
            "timestamp": "2025-12-05T14:30:00Z",
            "updateInterval": "5m",
            "hubs": "not-an-array"
        }
        content = json.dumps(invalid_data).encode("utf-8")

        candidates = collector.generate_candidates()
        candidate = candidates[0]

        is_valid = collector.validate_content(content, candidate)
        assert is_valid is False

    def test_validate_content_empty_hubs_valid(self, collector):
        """Test validation passes with empty hubs array (no data available)."""
        valid_data = {
            "timestamp": "2025-12-05T14:30:00Z",
            "updateInterval": "5m",
            "hubs": []
        }
        content = json.dumps(valid_data).encode("utf-8")

        candidates = collector.generate_candidates()
        candidate = candidates[0]

        is_valid = collector.validate_content(content, candidate)
        assert is_valid is True

    def test_validate_content_hub_missing_name(self, collector):
        """Test validation fails when hub is missing name."""
        invalid_data = {
            "timestamp": "2025-12-05T14:30:00Z",
            "updateInterval": "5m",
            "hubs": [
                {
                    "lmp": 25.50,
                    "components": {
                        "energy": 24.80,
                        "congestion": 0.45,
                        "losses": 0.25
                    }
                }
            ]
        }
        content = json.dumps(invalid_data).encode("utf-8")

        candidates = collector.generate_candidates()
        candidate = candidates[0]

        is_valid = collector.validate_content(content, candidate)
        assert is_valid is False

    def test_validate_content_hub_invalid_name(self, collector):
        """Test validation fails with empty hub name."""
        invalid_data = {
            "timestamp": "2025-12-05T14:30:00Z",
            "updateInterval": "5m",
            "hubs": [
                {
                    "name": "",
                    "lmp": 25.50,
                    "components": {
                        "energy": 24.80,
                        "congestion": 0.45,
                        "losses": 0.25
                    }
                }
            ]
        }
        content = json.dumps(invalid_data).encode("utf-8")

        candidates = collector.generate_candidates()
        candidate = candidates[0]

        is_valid = collector.validate_content(content, candidate)
        assert is_valid is False

    def test_validate_content_lmp_not_numeric(self, collector):
        """Test validation fails when LMP is not numeric."""
        invalid_data = {
            "timestamp": "2025-12-05T14:30:00Z",
            "updateInterval": "5m",
            "hubs": [
                {
                    "name": "CINERGY",
                    "lmp": "not-a-number",
                    "components": {
                        "energy": 24.80,
                        "congestion": 0.45,
                        "losses": 0.25
                    }
                }
            ]
        }
        content = json.dumps(invalid_data).encode("utf-8")

        candidates = collector.generate_candidates()
        candidate = candidates[0]

        is_valid = collector.validate_content(content, candidate)
        assert is_valid is False

    def test_validate_content_missing_component(self, collector):
        """Test validation fails when component is missing."""
        invalid_data = {
            "timestamp": "2025-12-05T14:30:00Z",
            "updateInterval": "5m",
            "hubs": [
                {
                    "name": "CINERGY",
                    "lmp": 25.50,
                    "components": {
                        "energy": 24.80,
                        "congestion": 0.45
                        # Missing "losses"
                    }
                }
            ]
        }
        content = json.dumps(invalid_data).encode("utf-8")

        candidates = collector.generate_candidates()
        candidate = candidates[0]

        is_valid = collector.validate_content(content, candidate)
        assert is_valid is False

    def test_validate_content_component_not_numeric(self, collector):
        """Test validation fails when component is not numeric."""
        invalid_data = {
            "timestamp": "2025-12-05T14:30:00Z",
            "updateInterval": "5m",
            "hubs": [
                {
                    "name": "CINERGY",
                    "lmp": 25.50,
                    "components": {
                        "energy": "not-numeric",
                        "congestion": 0.45,
                        "losses": 0.25
                    }
                }
            ]
        }
        content = json.dumps(invalid_data).encode("utf-8")

        candidates = collector.generate_candidates()
        candidate = candidates[0]

        is_valid = collector.validate_content(content, candidate)
        assert is_valid is False

    def test_validate_content_lmp_arithmetic_correct(self, collector):
        """Test validation passes when LMP arithmetic is correct."""
        valid_data = {
            "timestamp": "2025-12-05T14:30:00Z",
            "updateInterval": "5m",
            "hubs": [
                {
                    "name": "CINERGY",
                    "lmp": 25.50,
                    "components": {
                        "energy": 24.80,
                        "congestion": 0.45,
                        "losses": 0.25  # 24.80 + 0.45 + 0.25 = 25.50 ✓
                    }
                }
            ]
        }
        content = json.dumps(valid_data).encode("utf-8")

        candidates = collector.generate_candidates()
        candidate = candidates[0]

        is_valid = collector.validate_content(content, candidate)
        assert is_valid is True

    def test_validate_content_lmp_arithmetic_within_tolerance(self, collector):
        """Test validation passes when LMP arithmetic is within tolerance (0.01)."""
        valid_data = {
            "timestamp": "2025-12-05T14:30:00Z",
            "updateInterval": "5m",
            "hubs": [
                {
                    "name": "CINERGY",
                    "lmp": 25.50,
                    "components": {
                        "energy": 24.80,
                        "congestion": 0.45,
                        "losses": 0.245  # 24.80 + 0.45 + 0.245 = 25.495 (diff=0.005) ✓
                    }
                }
            ]
        }
        content = json.dumps(valid_data).encode("utf-8")

        candidates = collector.generate_candidates()
        candidate = candidates[0]

        is_valid = collector.validate_content(content, candidate)
        assert is_valid is True

    def test_validate_content_lmp_arithmetic_fails(self, collector):
        """Test validation fails when LMP arithmetic is incorrect."""
        invalid_data = {
            "timestamp": "2025-12-05T14:30:00Z",
            "updateInterval": "5m",
            "hubs": [
                {
                    "name": "CINERGY",
                    "lmp": 30.00,  # Incorrect
                    "components": {
                        "energy": 24.80,
                        "congestion": 0.45,
                        "losses": 0.25  # 24.80 + 0.45 + 0.25 = 25.50 ≠ 30.00 ✗
                    }
                }
            ]
        }
        content = json.dumps(invalid_data).encode("utf-8")

        candidates = collector.generate_candidates()
        candidate = candidates[0]

        is_valid = collector.validate_content(content, candidate)
        assert is_valid is False

    def test_validate_content_negative_components(self, collector):
        """Test validation allows negative components (valid in energy markets)."""
        valid_data = {
            "timestamp": "2025-12-05T14:30:00Z",
            "updateInterval": "5m",
            "hubs": [
                {
                    "name": "CINERGY",
                    "lmp": 24.10,
                    "components": {
                        "energy": 24.80,
                        "congestion": -0.45,  # Negative congestion is valid
                        "losses": -0.25  # Negative losses can occur
                    }
                }
            ]
        }
        content = json.dumps(valid_data).encode("utf-8")

        candidates = collector.generate_candidates()
        candidate = candidates[0]

        is_valid = collector.validate_content(content, candidate)
        assert is_valid is True

    def test_validate_content_invalid_json(self, collector):
        """Test validation fails with invalid JSON."""
        invalid_content = b"not valid json{"

        candidates = collector.generate_candidates()
        candidate = candidates[0]

        is_valid = collector.validate_content(invalid_content, candidate)
        assert is_valid is False

    def test_validate_content_multiple_hubs(self, collector, sample_response_bytes):
        """Test validation succeeds with multiple hubs."""
        # sample_response has 5 hubs
        candidates = collector.generate_candidates()
        candidate = candidates[0]

        is_valid = collector.validate_content(sample_response_bytes, candidate)
        assert is_valid is True


class TestIntegration:
    """Integration tests with BaseCollector framework."""

    @patch("requests.get")
    def test_full_collection_workflow(
        self, mock_get, collector, sample_response_bytes, mock_redis
    ):
        """Test complete collection workflow."""
        # Mock API response
        mock_response = MagicMock()
        mock_response.content = sample_response_bytes
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        # Mock S3 client
        collector.s3_client = MagicMock()

        # Mock hash registry (not a duplicate)
        mock_redis.get.return_value = None

        # Generate candidates
        candidates = collector.generate_candidates()
        assert len(candidates) == 1

        # Collect content
        candidate = candidates[0]
        content = collector.collect_content(candidate)
        assert content == sample_response_bytes

        # Validate content
        is_valid = collector.validate_content(content, candidate)
        assert is_valid is True
