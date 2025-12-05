"""Tests for MISO RSG Commitments scraper."""

import json
import logging
from datetime import datetime, UTC
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

from sourcing.infrastructure.collection_framework import ScrapingError
from sourcing.scraping.miso.rsg_commitments.scraper_miso_rsg_commitments import (
    MisoRSGCommitmentsCollector,
)

logger = logging.getLogger("sourcing_app")


@pytest.fixture
def sample_commitments_data():
    """Load sample RSG commitments response from fixtures."""
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
    """Create a MisoRSGCommitmentsCollector instance."""
    return MisoRSGCommitmentsCollector(
        dgroup="miso_rsg_commitments",
        s3_bucket="test-bucket",
        s3_prefix="sourcing",
        redis_client=mock_redis,
        environment="dev",
    )


@pytest.fixture
def collector_with_filters(mock_redis):
    """Create a MisoRSGCommitmentsCollector instance with filters."""
    return MisoRSGCommitmentsCollector(
        dgroup="miso_rsg_commitments",
        s3_bucket="test-bucket",
        s3_prefix="sourcing",
        redis_client=mock_redis,
        environment="dev",
        resource_type="WIND",
        commitment_reason="CONGESTION_RELIEF",
    )


class TestInitialization:
    """Test collector initialization."""

    def test_init_with_valid_filters(self, mock_redis):
        """Should initialize with valid filters."""
        collector = MisoRSGCommitmentsCollector(
            dgroup="miso_rsg_commitments",
            s3_bucket="test-bucket",
            s3_prefix="sourcing",
            redis_client=mock_redis,
            environment="dev",
            resource_type="WIND",
            commitment_reason="MUST_RUN",
        )
        assert collector.resource_type == "WIND"
        assert collector.commitment_reason == "MUST_RUN"

    def test_init_with_invalid_resource_type(self, mock_redis):
        """Should raise ValueError with invalid resource type."""
        with pytest.raises(ValueError, match="Invalid resource_type"):
            MisoRSGCommitmentsCollector(
                dgroup="miso_rsg_commitments",
                s3_bucket="test-bucket",
                s3_prefix="sourcing",
                redis_client=mock_redis,
                environment="dev",
                resource_type="INVALID_TYPE",
            )

    def test_init_with_invalid_commitment_reason(self, mock_redis):
        """Should raise ValueError with invalid commitment reason."""
        with pytest.raises(ValueError, match="Invalid commitment_reason"):
            MisoRSGCommitmentsCollector(
                dgroup="miso_rsg_commitments",
                s3_bucket="test-bucket",
                s3_prefix="sourcing",
                redis_client=mock_redis,
                environment="dev",
                commitment_reason="INVALID_REASON",
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

        assert candidate.source_location == MisoRSGCommitmentsCollector.API_URL
        assert candidate.metadata["data_type"] == "rsg_commitments"
        assert candidate.metadata["source"] == "miso"
        assert "collection_timestamp" in candidate.metadata
        assert candidate.identifier.startswith("rsg_commitments")
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

    def test_generate_candidates_with_filters(self, collector_with_filters):
        """Should include filters in query params and identifier."""
        candidates = collector_with_filters.generate_candidates()
        candidate = candidates[0]

        # Check query params include filters
        query_params = candidate.collection_params["query_params"]
        assert query_params["resourceType"] == "WIND"
        assert query_params["commitmentReason"] == "CONGESTION_RELIEF"

        # Check identifier includes filter info
        assert "type_wind" in candidate.identifier
        assert "reason_congestion_relief" in candidate.identifier

        # Check metadata includes filter info
        assert candidate.metadata["resource_type_filter"] == "WIND"
        assert candidate.metadata["commitment_reason_filter"] == "CONGESTION_RELIEF"

    def test_generate_candidates_without_filters(self, collector):
        """Should not include filters when none specified."""
        candidates = collector.generate_candidates()
        candidate = candidates[0]

        # Check query params are empty or don't have filters
        query_params = candidate.collection_params.get("query_params", {})
        assert "resourceType" not in query_params
        assert "commitmentReason" not in query_params

        # Check metadata indicates no filters
        assert candidate.metadata["resource_type_filter"] == "all"
        assert candidate.metadata["commitment_reason_filter"] == "all"


class TestContentCollection:
    """Test content collection."""

    @patch("requests.get")
    def test_collect_content_success(self, mock_get, collector, sample_commitments_data):
        """Should successfully fetch commitments data."""
        mock_response = MagicMock()
        mock_response.content = json.dumps(sample_commitments_data).encode()
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        candidates = collector.generate_candidates()
        content = collector.collect_content(candidates[0])

        assert content == mock_response.content
        mock_get.assert_called_once()

    @patch("requests.get")
    def test_collect_content_with_filters(self, mock_get, collector_with_filters, sample_commitments_data):
        """Should pass filters as query parameters."""
        mock_response = MagicMock()
        mock_response.content = json.dumps(sample_commitments_data).encode()
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        candidates = collector_with_filters.generate_candidates()
        collector_with_filters.collect_content(candidates[0])

        # Verify query params were passed
        call_kwargs = mock_get.call_args.kwargs
        assert "params" in call_kwargs
        assert call_kwargs["params"]["resourceType"] == "WIND"
        assert call_kwargs["params"]["commitmentReason"] == "CONGESTION_RELIEF"

    @patch("requests.get")
    def test_collect_content_timeout(self, mock_get, collector):
        """Should handle timeout errors."""
        mock_get.side_effect = requests.exceptions.Timeout("Timeout")

        candidates = collector.generate_candidates()
        with pytest.raises(ScrapingError, match="Failed to fetch RSG commitments"):
            collector.collect_content(candidates[0])

    @patch("requests.get")
    def test_collect_content_http_error(self, mock_get, collector):
        """Should handle HTTP errors."""
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404")
        mock_get.return_value = mock_response

        candidates = collector.generate_candidates()
        with pytest.raises(ScrapingError, match="Failed to fetch RSG commitments"):
            collector.collect_content(candidates[0])


class TestContentValidation:
    """Test content validation."""

    def test_validate_content_valid_response(self, collector, sample_commitments_data):
        """Should pass validation for valid commitments data."""
        content = json.dumps(sample_commitments_data).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is True

    def test_validate_content_not_dict(self, collector):
        """Should fail if response is not a dict."""
        content = json.dumps([{"error": "not a dict"}]).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is False

    def test_validate_content_missing_data_key(self, collector):
        """Should fail if response missing 'data' key."""
        content = json.dumps({"metadata": {}}).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is False

    def test_validate_content_missing_metadata_key(self, collector):
        """Should fail if response missing 'metadata' key."""
        content = json.dumps({"data": []}).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is False

    def test_validate_content_data_not_list(self, collector):
        """Should fail if 'data' is not a list."""
        content = json.dumps({"data": {}, "metadata": {}}).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is False

    def test_validate_content_missing_metadata_field(self, collector, sample_commitments_data):
        """Should fail if metadata missing required fields."""
        invalid_data = sample_commitments_data.copy()
        del invalid_data["metadata"]["timestamp"]
        content = json.dumps(invalid_data).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is False

    def test_validate_content_total_count_mismatch(self, collector, sample_commitments_data):
        """Should fail if totalCommitments doesn't match actual count."""
        invalid_data = sample_commitments_data.copy()
        invalid_data["metadata"]["totalCommitments"] = 999
        content = json.dumps(invalid_data).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is False

    def test_validate_content_invalid_json(self, collector):
        """Should fail if content is not valid JSON."""
        content = b"not valid json"
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is False


class TestCommitmentRecordValidation:
    """Test individual commitment record validation."""

    def test_validate_missing_required_field(self, collector, sample_commitments_data):
        """Should fail if commitment missing required field."""
        invalid_data = sample_commitments_data.copy()
        del invalid_data["data"][0]["resourceId"]
        content = json.dumps(invalid_data).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is False

    def test_validate_invalid_resource_type(self, collector, sample_commitments_data):
        """Should fail if resourceType is invalid."""
        invalid_data = sample_commitments_data.copy()
        invalid_data["data"][0]["resourceType"] = "INVALID_TYPE"
        content = json.dumps(invalid_data).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is False

    def test_validate_invalid_commitment_reason(self, collector, sample_commitments_data):
        """Should fail if commitmentReason is invalid."""
        invalid_data = sample_commitments_data.copy()
        invalid_data["data"][0]["commitmentReason"] = "INVALID_REASON"
        content = json.dumps(invalid_data).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is False

    def test_validate_invalid_numeric_field(self, collector, sample_commitments_data):
        """Should fail if numeric field is not numeric."""
        invalid_data = sample_commitments_data.copy()
        invalid_data["data"][0]["economicMaximum"] = "not-a-number"
        content = json.dumps(invalid_data).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is False

    def test_validate_invalid_integer_field(self, collector, sample_commitments_data):
        """Should fail if integer field is not an integer."""
        invalid_data = sample_commitments_data.copy()
        invalid_data["data"][0]["minimumRunTime"] = "not-an-integer"
        content = json.dumps(invalid_data).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is False

    def test_validate_economic_max_less_than_min(self, collector, sample_commitments_data):
        """Should fail if economicMaximum < economicMinimum."""
        invalid_data = sample_commitments_data.copy()
        invalid_data["data"][0]["economicMaximum"] = 10.0
        invalid_data["data"][0]["economicMinimum"] = 20.0
        content = json.dumps(invalid_data).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is False

    def test_validate_negative_economic_maximum(self, collector, sample_commitments_data):
        """Should fail if economicMaximum is negative."""
        invalid_data = sample_commitments_data.copy()
        invalid_data["data"][0]["economicMaximum"] = -10.0
        content = json.dumps(invalid_data).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is False

    def test_validate_negative_economic_minimum(self, collector, sample_commitments_data):
        """Should fail if economicMinimum is negative."""
        invalid_data = sample_commitments_data.copy()
        invalid_data["data"][0]["economicMinimum"] = -5.0
        content = json.dumps(invalid_data).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is False

    def test_validate_negative_actual_output(self, collector, sample_commitments_data):
        """Should fail if actualOutput is negative."""
        invalid_data = sample_commitments_data.copy()
        invalid_data["data"][0]["actualOutput"] = -25.0
        content = json.dumps(invalid_data).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is False

    def test_validate_actual_output_outside_bounds(self, collector, sample_commitments_data):
        """Should fail if actualOutput outside reasonable bounds (with tolerance)."""
        invalid_data = sample_commitments_data.copy()
        # economicMinimum=20, economicMaximum=100
        # With 10% tolerance: [18, 110]
        # Set actualOutput way outside bounds
        invalid_data["data"][0]["actualOutput"] = 200.0
        content = json.dumps(invalid_data).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is False

    def test_validate_actual_output_within_tolerance(self, collector, sample_commitments_data):
        """Should pass if actualOutput slightly outside bounds but within tolerance."""
        valid_data = sample_commitments_data.copy()
        # economicMinimum=20, economicMaximum=100
        # With 10% tolerance: [18, 110]
        # Set actualOutput just over max but within tolerance
        valid_data["data"][0]["actualOutput"] = 105.0
        content = json.dumps(valid_data).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is True

    def test_validate_zero_minimum_run_time(self, collector, sample_commitments_data):
        """Should fail if minimumRunTime is zero."""
        invalid_data = sample_commitments_data.copy()
        invalid_data["data"][0]["minimumRunTime"] = 0
        content = json.dumps(invalid_data).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is False

    def test_validate_negative_minimum_run_time(self, collector, sample_commitments_data):
        """Should fail if minimumRunTime is negative."""
        invalid_data = sample_commitments_data.copy()
        invalid_data["data"][0]["minimumRunTime"] = -30
        content = json.dumps(invalid_data).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is False

    def test_validate_commitment_end_before_start(self, collector, sample_commitments_data):
        """Should fail if commitmentEnd <= commitmentStart."""
        invalid_data = sample_commitments_data.copy()
        invalid_data["data"][0]["commitmentEnd"] = "2025-12-05T09:00:00-05:00"
        invalid_data["data"][0]["commitmentStart"] = "2025-12-05T12:00:00-05:00"
        content = json.dumps(invalid_data).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is False

    def test_validate_commitment_end_equals_start(self, collector, sample_commitments_data):
        """Should fail if commitmentEnd equals commitmentStart."""
        invalid_data = sample_commitments_data.copy()
        invalid_data["data"][0]["commitmentEnd"] = "2025-12-05T10:00:00-05:00"
        invalid_data["data"][0]["commitmentStart"] = "2025-12-05T10:00:00-05:00"
        content = json.dumps(invalid_data).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is False

    def test_validate_invalid_datetime_format(self, collector, sample_commitments_data):
        """Should fail if datetime fields have invalid format."""
        invalid_data = sample_commitments_data.copy()
        invalid_data["data"][0]["commitmentStart"] = "invalid-datetime"
        content = json.dumps(invalid_data).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is False

    def test_validate_all_resource_types(self, collector, sample_commitments_data):
        """Should validate all valid resource types."""
        valid_types = [
            "GENERATOR",
            "COMBINED_CYCLE",
            "WIND",
            "SOLAR",
            "ENERGY_STORAGE",
            "DEMAND_RESPONSE",
        ]

        for resource_type in valid_types:
            valid_data = sample_commitments_data.copy()
            valid_data["data"][0]["resourceType"] = resource_type
            content = json.dumps(valid_data).encode()
            candidates = collector.generate_candidates()

            assert collector.validate_content(content, candidates[0]) is True, \
                f"Should accept resource type {resource_type}"

    def test_validate_all_commitment_reasons(self, collector, sample_commitments_data):
        """Should validate all valid commitment reasons."""
        valid_reasons = [
            "MUST_RUN",
            "RELIABILITY_MUST_RUN",
            "OUT_OF_MERIT_ECONOMIC",
            "OUT_OF_MERIT_RELIABILITY",
            "CONGESTION_RELIEF",
            "LOCAL_VOLTAGE_SUPPORT",
        ]

        for reason in valid_reasons:
            valid_data = sample_commitments_data.copy()
            valid_data["data"][0]["commitmentReason"] = reason
            content = json.dumps(valid_data).encode()
            candidates = collector.generate_candidates()

            assert collector.validate_content(content, candidates[0]) is True, \
                f"Should accept commitment reason {reason}"


class TestEdgeCases:
    """Test edge cases."""

    def test_validate_empty_commitments_list(self, collector):
        """Should pass validation for empty commitments list."""
        valid_data = {
            "data": [],
            "metadata": {
                "timestamp": "2025-12-05T10:05:00-05:00",
                "totalCommitments": 0,
                "updateInterval": "PT5M"
            }
        }
        content = json.dumps(valid_data).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is True

    def test_validate_large_commitments_list(self, collector, sample_commitments_data):
        """Should handle large commitments list."""
        # Create a large list by repeating the sample data
        large_data = sample_commitments_data.copy()
        large_data["data"] = sample_commitments_data["data"] * 200  # 1000 commitments
        large_data["metadata"]["totalCommitments"] = len(large_data["data"])
        content = json.dumps(large_data).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is True

    def test_validate_zero_values(self, collector, sample_commitments_data):
        """Should accept zero values where appropriate."""
        valid_data = sample_commitments_data.copy()
        valid_data["data"][0]["economicMinimum"] = 0.0
        valid_data["data"][0]["actualOutput"] = 0.0
        valid_data["data"][0]["marginalCost"] = 0.0
        valid_data["data"][0]["startupCost"] = 0.0
        content = json.dumps(valid_data).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is True

    def test_validate_very_large_values(self, collector, sample_commitments_data):
        """Should accept very large but valid values."""
        valid_data = sample_commitments_data.copy()
        valid_data["data"][0]["economicMaximum"] = 10000.0
        valid_data["data"][0]["economicMinimum"] = 5000.0
        valid_data["data"][0]["actualOutput"] = 7500.0
        valid_data["data"][0]["marginalCost"] = 999.99
        valid_data["data"][0]["startupCost"] = 1000000.0
        valid_data["data"][0]["minimumRunTime"] = 480
        content = json.dumps(valid_data).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is True


class TestIntegration:
    """Integration tests."""

    @patch("requests.get")
    def test_full_collection_flow(self, mock_get, collector, sample_commitments_data, mock_redis):
        """Should complete full collection flow successfully."""
        # Mock HTTP response
        mock_response = MagicMock()
        mock_response.content = json.dumps(sample_commitments_data).encode()
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
    def test_duplicate_detection(self, mock_get, collector, sample_commitments_data, mock_redis):
        """Should skip duplicate content."""
        # Mock HTTP response
        mock_response = MagicMock()
        mock_response.content = json.dumps(sample_commitments_data).encode()
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

    @patch("requests.get")
    def test_validation_failure(self, mock_get, collector, mock_redis):
        """Should handle validation failures."""
        # Mock HTTP response with invalid data
        invalid_data = {"invalid": "structure"}
        mock_response = MagicMock()
        mock_response.content = json.dumps(invalid_data).encode()
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        # Mock S3 client
        collector.s3_client = MagicMock()

        # Run collection
        results = collector.run_collection()

        # Verify validation failure
        assert results["failed"] == 1
        assert results["collected"] == 0

        # Verify S3 was NOT called
        collector.s3_client.put_object.assert_not_called()
