"""Tests for MISO Snapshot scraper."""

import json
import logging
from datetime import datetime, UTC
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

from sourcing.infrastructure.collection_framework import ScrapingError
from sourcing.scraping.miso.snapshot.scraper_miso_snapshot import MisoSnapshotCollector

logger = logging.getLogger("sourcing_app")


@pytest.fixture
def sample_snapshot_data():
    """Load sample snapshot response from fixtures."""
    fixture_path = Path(__file__).parent / "fixtures" / "sample_response.json"
    with open(fixture_path, "r") as f:
        return json.load(f)


@pytest.fixture
def mock_redis():
    """Create a mock Redis client."""
    redis_mock = MagicMock()
    redis_mock.ping.return_value = True
    # Mock exists to return integer (0 or 1)
    redis_mock.exists.return_value = 0
    return redis_mock


@pytest.fixture
def collector(mock_redis):
    """Create a MisoSnapshotCollector instance."""
    return MisoSnapshotCollector(
        dgroup="miso_snapshot",
        s3_bucket="test-bucket",
        s3_prefix="sourcing",
        redis_client=mock_redis,
        environment="dev",
    )


class TestCandidateGeneration:
    """Test candidate generation."""

    def test_generate_candidates_creates_single_candidate(self, collector):
        """Should generate exactly one candidate for current snapshot."""
        candidates = collector.generate_candidates()

        assert len(candidates) == 1

    def test_generate_candidates_has_correct_structure(self, collector):
        """Should generate candidate with proper structure."""
        candidates = collector.generate_candidates()
        candidate = candidates[0]

        assert candidate.source_location == MisoSnapshotCollector.API_URL
        assert candidate.metadata["data_type"] == "snapshot"
        assert candidate.metadata["source"] == "miso"
        assert "collection_timestamp" in candidate.metadata
        assert candidate.identifier.startswith("snapshot_")
        assert candidate.identifier.endswith(".json")

    def test_generate_candidates_has_proper_headers(self, collector):
        """Should include proper HTTP headers."""
        candidates = collector.generate_candidates()
        candidate = candidates[0]

        headers = candidate.collection_params["headers"]
        assert headers["Accept"] == "application/json"
        assert "User-Agent" in headers

    def test_generate_candidates_has_file_date(self, collector):
        """Should set file_date to current date."""
        candidates = collector.generate_candidates()
        candidate = candidates[0]

        assert candidate.file_date is not None
        assert candidate.file_date == datetime.now(UTC).date()


class TestContentCollection:
    """Test content collection."""

    @patch("requests.get")
    def test_collect_content_success(self, mock_get, collector, sample_snapshot_data):
        """Should successfully fetch snapshot data."""
        mock_response = MagicMock()
        mock_response.content = json.dumps(sample_snapshot_data).encode()
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        candidates = collector.generate_candidates()
        content = collector.collect_content(candidates[0])

        assert content == mock_response.content
        mock_get.assert_called_once()

    @patch("requests.get")
    def test_collect_content_timeout(self, mock_get, collector):
        """Should handle timeout errors."""
        mock_get.side_effect = requests.exceptions.Timeout("Timeout")

        candidates = collector.generate_candidates()
        with pytest.raises(ScrapingError, match="Failed to fetch snapshot"):
            collector.collect_content(candidates[0])

    @patch("requests.get")
    def test_collect_content_http_error(self, mock_get, collector):
        """Should handle HTTP errors."""
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404")
        mock_get.return_value = mock_response

        candidates = collector.generate_candidates()
        with pytest.raises(ScrapingError, match="Failed to fetch snapshot"):
            collector.collect_content(candidates[0])


class TestContentValidation:
    """Test content validation."""

    def test_validate_content_valid_response(self, collector, sample_snapshot_data):
        """Should pass validation for valid snapshot data."""
        content = json.dumps(sample_snapshot_data).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is True

    def test_validate_content_not_array(self, collector):
        """Should fail if response is not an array."""
        content = json.dumps({"error": "not an array"}).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is False

    def test_validate_content_wrong_count(self, collector):
        """Should fail if response doesn't have exactly 4 metrics."""
        content = json.dumps([
            {"t": "Test", "v": "100", "d": "2025-12-05T09:35:00-05:00", "id": "test"}
        ]).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is False

    def test_validate_content_missing_field(self, collector, sample_snapshot_data):
        """Should fail if any metric is missing required fields."""
        # Remove 'v' field from first metric
        invalid_data = sample_snapshot_data.copy()
        invalid_data[0] = {k: v for k, v in invalid_data[0].items() if k != "v"}
        content = json.dumps(invalid_data).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is False

    def test_validate_content_invalid_numeric_value(self, collector, sample_snapshot_data):
        """Should fail if value is not numeric."""
        invalid_data = sample_snapshot_data.copy()
        invalid_data[0]["v"] = "not-a-number"
        content = json.dumps(invalid_data).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is False

    def test_validate_content_wrong_metric_ids(self, collector):
        """Should fail if metric IDs don't match expected set."""
        invalid_data = [
            {"t": "Test1", "v": "100", "d": "2025-12-05T09:35:00-05:00", "id": "wrong_id_1"},
            {"t": "Test2", "v": "200", "d": "2025-12-05T09:35:00-05:00", "id": "wrong_id_2"},
            {"t": "Test3", "v": "300", "d": "2025-12-05T09:35:00-05:00", "id": "wrong_id_3"},
            {"t": "Test4", "v": "400", "d": "2025-12-05T09:35:00-05:00", "id": "wrong_id_4"},
        ]
        content = json.dumps(invalid_data).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is False

    def test_validate_content_invalid_json(self, collector):
        """Should fail if content is not valid JSON."""
        content = b"not valid json"
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is False


class TestIntegration:
    """Integration tests."""

    @patch("requests.get")
    def test_full_collection_flow(self, mock_get, collector, sample_snapshot_data, mock_redis):
        """Should complete full collection flow successfully."""
        # Mock HTTP response
        mock_response = MagicMock()
        mock_response.content = json.dumps(sample_snapshot_data).encode()
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        # Mock Redis to simulate no duplicate (exists returns 0 for no match)
        mock_redis.exists.return_value = 0
        mock_redis.setex.return_value = True

        # Mock S3 client with proper response
        mock_s3_response = {
            'VersionId': 'test-version-123',
            'ETag': '"test-etag"'
        }
        collector.s3_client = MagicMock()
        collector.s3_client.put_object.return_value = mock_s3_response

        # Run collection
        results = collector.run_collection()

        # Verify results
        assert results["total_candidates"] == 1
        assert results["collected"] == 1
        assert results["failed"] == 0

        # Verify Redis was checked for existence
        assert mock_redis.exists.called

        # Verify S3 upload was called
        assert collector.s3_client.put_object.called

    @patch("requests.get")
    def test_duplicate_detection(self, mock_get, collector, sample_snapshot_data, mock_redis):
        """Should skip duplicate content."""
        # Mock HTTP response
        mock_response = MagicMock()
        mock_response.content = json.dumps(sample_snapshot_data).encode()
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        # Mock Redis to simulate duplicate (exists returns 1 for match)
        mock_redis.exists.return_value = 1

        # Mock S3 client
        collector.s3_client = MagicMock()

        # Run collection
        results = collector.run_collection()

        # Verify duplicate was detected
        assert results["skipped_duplicate"] == 1
        assert results["collected"] == 0

        # Verify S3 was NOT called
        collector.s3_client.put_object.assert_not_called()
