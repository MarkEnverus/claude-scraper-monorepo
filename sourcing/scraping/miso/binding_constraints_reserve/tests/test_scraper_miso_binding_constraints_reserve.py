"""Tests for MISO Binding Constraints Reserve scraper."""

import json
import logging
from datetime import datetime, UTC
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

from sourcing.infrastructure.collection_framework import ScrapingError
from sourcing.scraping.miso.binding_constraints_reserve.scraper_miso_binding_constraints_reserve import (
    MisoBindingConstraintsReserveCollector
)

logger = logging.getLogger("sourcing_app")


@pytest.fixture
def sample_reserve_constraints_data():
    """Load sample reserve constraints response from fixtures."""
    fixture_path = Path(__file__).parent / "fixtures" / "sample_response.json"
    with open(fixture_path, "r") as f:
        return json.load(f)


@pytest.fixture
def sample_empty_response():
    """Load sample empty response from fixtures."""
    fixture_path = Path(__file__).parent / "fixtures" / "sample_response_empty.json"
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
    """Create a MisoBindingConstraintsReserveCollector instance."""
    return MisoBindingConstraintsReserveCollector(
        dgroup="miso_binding_constraints_reserve",
        s3_bucket="test-bucket",
        s3_prefix="sourcing",
        redis_client=mock_redis,
        environment="dev",
    )


class TestCandidateGeneration:
    """Test candidate generation."""

    def test_generate_candidates_creates_single_candidate(self, collector):
        """Should generate exactly one candidate for current reserve constraints snapshot."""
        candidates = collector.generate_candidates()

        assert len(candidates) == 1

    def test_generate_candidates_has_correct_structure(self, collector):
        """Should generate candidate with proper structure."""
        candidates = collector.generate_candidates()
        candidate = candidates[0]

        assert candidate.source_location == MisoBindingConstraintsReserveCollector.API_URL
        assert candidate.metadata["data_type"] == "binding_constraints_reserve"
        assert candidate.metadata["source"] == "miso"
        assert "collection_timestamp" in candidate.metadata
        assert candidate.identifier.startswith("binding_constraints_reserve_")
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
    def test_collect_content_success(self, mock_get, collector, sample_reserve_constraints_data):
        """Should successfully fetch reserve constraints data."""
        mock_response = MagicMock()
        mock_response.content = json.dumps(sample_reserve_constraints_data).encode()
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
        with pytest.raises(ScrapingError, match="Failed to fetch reserve constraints"):
            collector.collect_content(candidates[0])

    @patch("requests.get")
    def test_collect_content_http_error(self, mock_get, collector):
        """Should handle HTTP errors."""
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404")
        mock_get.return_value = mock_response

        candidates = collector.generate_candidates()
        with pytest.raises(ScrapingError, match="Failed to fetch reserve constraints"):
            collector.collect_content(candidates[0])


class TestContentValidation:
    """Test content validation."""

    def test_validate_content_valid_response(self, collector, sample_reserve_constraints_data):
        """Should pass validation for valid reserve constraints data."""
        content = json.dumps(sample_reserve_constraints_data).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is True

    def test_validate_content_empty_constraints(self, collector, sample_empty_response):
        """Should pass validation for empty Constraint array."""
        content = json.dumps(sample_empty_response).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is True

    def test_validate_content_not_dict(self, collector):
        """Should fail if response is not a dictionary."""
        content = json.dumps([{"error": "wrong type"}]).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is False

    def test_validate_content_missing_refid(self, collector):
        """Should fail if RefId is missing."""
        content = json.dumps({"Constraint": []}).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is False

    def test_validate_content_missing_constraint_field(self, collector):
        """Should fail if Constraint field is missing."""
        content = json.dumps({"RefId": "05-Dec-2025, 09:35 EST"}).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is False

    def test_validate_content_constraint_not_list(self, collector):
        """Should fail if Constraint is not a list."""
        content = json.dumps({
            "RefId": "05-Dec-2025, 09:35 EST",
            "Constraint": "not a list"
        }).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is False

    def test_validate_content_invalid_refid(self, collector):
        """Should fail if RefId is empty or invalid."""
        content = json.dumps({
            "RefId": "",
            "Constraint": []
        }).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is False

    def test_validate_content_missing_required_field(self, collector, sample_reserve_constraints_data):
        """Should fail if constraint is missing required field."""
        invalid_data = sample_reserve_constraints_data.copy()
        # Remove 'Name' field from first constraint
        invalid_data["Constraint"][0] = {
            k: v for k, v in invalid_data["Constraint"][0].items() if k != "Name"
        }
        content = json.dumps(invalid_data).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is False

    def test_validate_content_invalid_name(self, collector, sample_reserve_constraints_data):
        """Should fail if Name is empty."""
        invalid_data = sample_reserve_constraints_data.copy()
        invalid_data["Constraint"][0]["Name"] = ""
        content = json.dumps(invalid_data).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is False

    def test_validate_content_invalid_period(self, collector, sample_reserve_constraints_data):
        """Should fail if Period is empty."""
        invalid_data = sample_reserve_constraints_data.copy()
        invalid_data["Constraint"][0]["Period"] = ""
        content = json.dumps(invalid_data).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is False

    def test_validate_content_invalid_price(self, collector, sample_reserve_constraints_data):
        """Should fail if Price is not numeric."""
        invalid_data = sample_reserve_constraints_data.copy()
        invalid_data["Constraint"][0]["Price"] = "not-a-number"
        content = json.dumps(invalid_data).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is False

    def test_validate_content_invalid_quantity(self, collector, sample_reserve_constraints_data):
        """Should fail if Quantity is not numeric."""
        invalid_data = sample_reserve_constraints_data.copy()
        invalid_data["Constraint"][0]["Quantity"] = "invalid"
        content = json.dumps(invalid_data).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is False

    def test_validate_content_invalid_override(self, collector, sample_reserve_constraints_data):
        """Should fail if OVERRIDE is not boolean or int."""
        invalid_data = sample_reserve_constraints_data.copy()
        invalid_data["Constraint"][0]["OVERRIDE"] = "yes"
        content = json.dumps(invalid_data).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is False

    def test_validate_content_invalid_breakpoint(self, collector, sample_reserve_constraints_data):
        """Should fail if breakpoint parameter is not numeric."""
        invalid_data = sample_reserve_constraints_data.copy()
        invalid_data["Constraint"][0]["BP1"] = "invalid"
        content = json.dumps(invalid_data).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is False

    def test_validate_content_null_breakpoint(self, collector, sample_reserve_constraints_data):
        """Should pass validation if breakpoint parameter is null."""
        valid_data = sample_reserve_constraints_data.copy()
        valid_data["Constraint"][0]["BP1"] = None
        valid_data["Constraint"][0]["PC1"] = None
        content = json.dumps(valid_data).encode()
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is True

    def test_validate_content_unexpected_reserve_type(self, collector, sample_reserve_constraints_data):
        """Should log warning but pass for unexpected ReserveType."""
        valid_data = sample_reserve_constraints_data.copy()
        valid_data["Constraint"][0]["ReserveType"] = "NEW_TYPE"
        content = json.dumps(valid_data).encode()
        candidates = collector.generate_candidates()

        # Should pass but log warning
        assert collector.validate_content(content, candidates[0]) is True

    def test_validate_content_unexpected_direction(self, collector, sample_reserve_constraints_data):
        """Should log warning but pass for unexpected Direction."""
        valid_data = sample_reserve_constraints_data.copy()
        valid_data["Constraint"][0]["Direction"] = "SIDEWAYS"
        content = json.dumps(valid_data).encode()
        candidates = collector.generate_candidates()

        # Should pass but log warning
        assert collector.validate_content(content, candidates[0]) is True

    def test_validate_content_invalid_json(self, collector):
        """Should fail if content is not valid JSON."""
        content = b"not valid json"
        candidates = collector.generate_candidates()

        assert collector.validate_content(content, candidates[0]) is False

    def test_validate_content_all_reserve_types(self, collector):
        """Should validate all expected reserve types."""
        for reserve_type in ["SPIN", "NSPIN", "REG_UP", "REG_DOWN", "SUPPL"]:
            data = {
                "RefId": "05-Dec-2025, 09:35 EST",
                "Constraint": [
                    {
                        "Name": f"TEST_{reserve_type}",
                        "Period": "2025-12-05T14:35:00Z",
                        "Price": 10.0,
                        "ReserveType": reserve_type,
                        "Direction": "UP",
                        "Quantity": 100.0,
                        "OVERRIDE": 0,
                        "BP1": 50.0,
                        "PC1": 0.25,
                        "BP2": 100.0,
                        "PC2": 0.50
                    }
                ]
            }
            content = json.dumps(data).encode()
            candidates = collector.generate_candidates()

            assert collector.validate_content(content, candidates[0]) is True

    def test_validate_content_all_directions(self, collector):
        """Should validate all expected directions."""
        for direction in ["UP", "DOWN", "BOTH"]:
            data = {
                "RefId": "05-Dec-2025, 09:35 EST",
                "Constraint": [
                    {
                        "Name": f"TEST_{direction}",
                        "Period": "2025-12-05T14:35:00Z",
                        "Price": 10.0,
                        "ReserveType": "SPIN",
                        "Direction": direction,
                        "Quantity": 100.0,
                        "OVERRIDE": 0,
                        "BP1": 50.0,
                        "PC1": 0.25,
                        "BP2": 100.0,
                        "PC2": 0.50
                    }
                ]
            }
            content = json.dumps(data).encode()
            candidates = collector.generate_candidates()

            assert collector.validate_content(content, candidates[0]) is True


class TestIntegration:
    """Integration tests."""

    @patch("requests.get")
    def test_full_collection_flow(self, mock_get, collector, sample_reserve_constraints_data, mock_redis):
        """Should complete full collection flow successfully."""
        # Mock HTTP response
        mock_response = MagicMock()
        mock_response.content = json.dumps(sample_reserve_constraints_data).encode()
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
    def test_full_collection_flow_empty_constraints(self, mock_get, collector, sample_empty_response, mock_redis):
        """Should handle empty Constraint array successfully."""
        # Mock HTTP response with empty constraints
        mock_response = MagicMock()
        mock_response.content = json.dumps(sample_empty_response).encode()
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        # Mock Redis to simulate no duplicate
        mock_redis.exists.return_value = 0
        mock_redis.setex.return_value = True

        # Mock S3 client
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

    @patch("requests.get")
    def test_duplicate_detection(self, mock_get, collector, sample_reserve_constraints_data, mock_redis):
        """Should skip duplicate content."""
        # Mock HTTP response
        mock_response = MagicMock()
        mock_response.content = json.dumps(sample_reserve_constraints_data).encode()
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
