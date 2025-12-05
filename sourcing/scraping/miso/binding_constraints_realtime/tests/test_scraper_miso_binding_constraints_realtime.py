"""Tests for MISO Real-Time Binding Constraints Scraper."""

import json
from datetime import datetime, date, timezone
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
import requests

from sourcing.scraping.miso.binding_constraints_realtime.scraper_miso_binding_constraints_realtime import (
    MisoBindingConstraintsRealtimeCollector,
)
from sourcing.infrastructure.collection_framework import DownloadCandidate, ScrapingError


@pytest.fixture
def mock_redis():
    """Create a mock Redis client."""
    redis_mock = Mock()
    redis_mock.ping.return_value = True
    redis_mock.get.return_value = None
    redis_mock.set.return_value = True
    redis_mock.exists.return_value = False
    return redis_mock


@pytest.fixture
def mock_s3():
    """Create a mock S3 client."""
    s3_mock = Mock()
    s3_mock.list_objects_v2.return_value = {"Contents": []}
    s3_mock.upload_fileobj.return_value = None
    return s3_mock


@pytest.fixture
def collector(mock_redis, mock_s3):
    """Create a collector instance."""
    collector = MisoBindingConstraintsRealtimeCollector(
        dgroup="miso_binding_constraints_realtime",
        s3_bucket="test-bucket",
        s3_prefix="sourcing",
        redis_client=mock_redis,
        environment="dev",
    )
    collector.s3_client = mock_s3
    return collector


@pytest.fixture
def sample_api_response():
    """Load sample API response fixture."""
    fixture_path = Path(__file__).parent / "fixtures" / "sample_response.json"
    with open(fixture_path, 'r') as f:
        return json.load(f)


@pytest.fixture
def empty_api_response():
    """Load empty API response fixture."""
    fixture_path = Path(__file__).parent / "fixtures" / "empty_response.json"
    with open(fixture_path, 'r') as f:
        return json.load(f)


class TestCollectorInitialization:
    """Tests for collector initialization."""

    def test_init_success(self, mock_redis, mock_s3):
        """Test successful initialization."""
        collector = MisoBindingConstraintsRealtimeCollector(
            dgroup="test",
            s3_bucket="test-bucket",
            s3_prefix="sourcing",
            redis_client=mock_redis,
            environment="dev",
        )
        assert collector.dgroup == "test"
        assert collector.BASE_URL == "https://public-api.misoenergy.org/api/BindingConstraints/RealTime"
        assert collector.TIMEOUT_SECONDS == 30

    def test_init_with_s3_params(self, mock_redis, mock_s3):
        """Test initialization with S3 parameters."""
        collector = MisoBindingConstraintsRealtimeCollector(
            dgroup="test",
            s3_bucket="my-bucket",
            s3_prefix="data",
            redis_client=mock_redis,
            environment="prod",
        )
        assert collector.s3_bucket == "my-bucket"
        assert collector.s3_prefix == "data"
        assert collector.environment == "prod"


class TestCandidateGeneration:
    """Tests for candidate generation logic."""

    def test_single_candidate_generation(self, collector):
        """Test generation of a single real-time snapshot candidate."""
        candidates = collector.generate_candidates()

        assert len(candidates) == 1
        candidate = candidates[0]

        # Check identifier format
        assert candidate.identifier.startswith("binding_constraints_realtime_")
        assert candidate.identifier.endswith(".json")

        # Check source location
        assert candidate.source_location == collector.BASE_URL

        # Check metadata
        assert candidate.metadata["data_type"] == "binding_constraints_realtime"
        assert candidate.metadata["source"] == "miso"
        assert candidate.metadata["market_type"] == "real_time_transmission"
        assert candidate.metadata["snapshot"] is True
        assert "collection_timestamp" in candidate.metadata

        # Check file_date is today
        assert candidate.file_date == datetime.now(timezone.utc).date()

    def test_candidate_headers(self, collector):
        """Test that candidates include proper headers (no auth required)."""
        candidates = collector.generate_candidates()
        candidate = candidates[0]

        headers = candidate.collection_params["headers"]
        assert headers["Accept"] == "application/json"
        assert "User-Agent" in headers
        # No API key required for public API
        assert "Ocp-Apim-Subscription-Key" not in headers
        assert "Authorization" not in headers

    def test_candidate_timeout(self, collector):
        """Test that candidate includes timeout parameter."""
        candidates = collector.generate_candidates()
        candidate = candidates[0]

        assert candidate.collection_params["timeout"] == 30

    def test_identifier_uniqueness(self, collector):
        """Test that generated identifiers are unique (timestamp-based)."""
        candidates1 = collector.generate_candidates()
        # Small delay to ensure different timestamp
        import time
        time.sleep(1.1)
        candidates2 = collector.generate_candidates()

        # Different calls should generate different identifiers
        assert candidates1[0].identifier != candidates2[0].identifier


class TestDataCollection:
    """Tests for data collection logic."""

    def test_collect_with_constraints(self, collector, sample_api_response):
        """Test collection with active binding constraints."""
        candidate = DownloadCandidate(
            identifier="binding_constraints_realtime_20251205_143500.json",
            source_location=collector.BASE_URL,
            metadata={
                "collection_timestamp": "2025-12-05T19:35:00+00:00",
                "data_type": "binding_constraints_realtime",
            },
            collection_params={
                "headers": {"Accept": "application/json"},
                "timeout": 30,
            },
            file_date=date(2025, 12, 5),
        )

        # Mock API response with constraints
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "RefId": "05-Dec-2025, 14:35 EST",
            "Constraint": sample_api_response["Constraint"]
        }

        with patch('requests.get', return_value=mock_response):
            content = collector.collect_content(candidate)

        data = json.loads(content.decode('utf-8'))
        assert "RefId" in data
        assert "Constraint" in data
        assert len(data["Constraint"]) == 3
        assert "collection_metadata" in data
        assert data["collection_metadata"]["source"] == "miso_public_api"

    def test_collect_empty_constraints(self, collector, empty_api_response):
        """Test collection with no active binding constraints."""
        candidate = DownloadCandidate(
            identifier="binding_constraints_realtime_20251205_143500.json",
            source_location=collector.BASE_URL,
            metadata={
                "collection_timestamp": "2025-12-05T19:35:00+00:00",
                "data_type": "binding_constraints_realtime",
            },
            collection_params={
                "headers": {"Accept": "application/json"},
                "timeout": 30,
            },
            file_date=date(2025, 12, 5),
        )

        # Mock API response with empty constraints
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = empty_api_response

        with patch('requests.get', return_value=mock_response):
            content = collector.collect_content(candidate)

        data = json.loads(content.decode('utf-8'))
        assert "RefId" in data
        assert "Constraint" in data
        assert len(data["Constraint"]) == 0
        assert "collection_metadata" in data

    def test_collect_handles_404(self, collector):
        """Test that 404 responses return empty constraint data."""
        candidate = DownloadCandidate(
            identifier="binding_constraints_realtime_20251205_143500.json",
            source_location=collector.BASE_URL,
            metadata={
                "collection_timestamp": "2025-12-05T19:35:00+00:00",
                "data_type": "binding_constraints_realtime",
            },
            collection_params={
                "headers": {"Accept": "application/json"},
                "timeout": 30,
            },
            file_date=date(2025, 12, 5),
        )

        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_response)

        with patch('requests.get', return_value=mock_response):
            # 404 should return empty valid response
            content = collector.collect_content(candidate)

        data = json.loads(content.decode('utf-8'))
        assert "RefId" in data
        assert "Constraint" in data
        assert len(data["Constraint"]) == 0
        assert data["collection_metadata"]["note"] == "No binding constraints active"

    def test_collect_handles_rate_limit(self, collector):
        """Test that 429 rate limit responses are logged and raised."""
        candidate = DownloadCandidate(
            identifier="binding_constraints_realtime_20251205_143500.json",
            source_location=collector.BASE_URL,
            metadata={
                "collection_timestamp": "2025-12-05T19:35:00+00:00",
                "data_type": "binding_constraints_realtime",
            },
            collection_params={
                "headers": {"Accept": "application/json"},
                "timeout": 30,
            },
            file_date=date(2025, 12, 5),
        )

        mock_response = Mock()
        mock_response.status_code = 429
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_response)

        with patch('requests.get', return_value=mock_response):
            with pytest.raises(ScrapingError) as excinfo:
                collector.collect_content(candidate)
            assert "HTTP error" in str(excinfo.value)

    def test_collect_handles_network_error(self, collector):
        """Test that network errors are properly wrapped."""
        candidate = DownloadCandidate(
            identifier="binding_constraints_realtime_20251205_143500.json",
            source_location=collector.BASE_URL,
            metadata={
                "collection_timestamp": "2025-12-05T19:35:00+00:00",
                "data_type": "binding_constraints_realtime",
            },
            collection_params={
                "headers": {"Accept": "application/json"},
                "timeout": 30,
            },
            file_date=date(2025, 12, 5),
        )

        with patch('requests.get', side_effect=requests.exceptions.ConnectionError("Network error")):
            with pytest.raises(ScrapingError) as excinfo:
                collector.collect_content(candidate)
            assert "Failed to fetch" in str(excinfo.value)

    def test_collect_handles_invalid_json(self, collector):
        """Test that invalid JSON responses are handled."""
        candidate = DownloadCandidate(
            identifier="binding_constraints_realtime_20251205_143500.json",
            source_location=collector.BASE_URL,
            metadata={
                "collection_timestamp": "2025-12-05T19:35:00+00:00",
                "data_type": "binding_constraints_realtime",
            },
            collection_params={
                "headers": {"Accept": "application/json"},
                "timeout": 30,
            },
            file_date=date(2025, 12, 5),
        )

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.side_effect = json.JSONDecodeError("Invalid", "doc", 0)

        with patch('requests.get', return_value=mock_response):
            with pytest.raises(ScrapingError) as excinfo:
                collector.collect_content(candidate)
            assert "Invalid JSON" in str(excinfo.value)


class TestContentValidation:
    """Tests for content validation logic."""

    def test_validate_valid_content_with_constraints(self, collector, sample_api_response):
        """Test validation of valid binding constraints data."""
        candidate = DownloadCandidate(
            identifier="test.json",
            source_location=collector.BASE_URL,
            metadata={"collection_timestamp": "2025-12-05T19:35:00+00:00"},
            collection_params={},
            file_date=date(2025, 12, 5),
        )

        content = json.dumps(sample_api_response).encode('utf-8')
        assert collector.validate_content(content, candidate) is True

    def test_validate_valid_empty_constraints(self, collector, empty_api_response):
        """Test validation of empty constraints (no active constraints)."""
        candidate = DownloadCandidate(
            identifier="test.json",
            source_location=collector.BASE_URL,
            metadata={"collection_timestamp": "2025-12-05T19:35:00+00:00"},
            collection_params={},
            file_date=date(2025, 12, 5),
        )

        content = json.dumps(empty_api_response).encode('utf-8')
        assert collector.validate_content(content, candidate) is True

    def test_validate_missing_refid(self, collector):
        """Test validation fails when RefId is missing."""
        candidate = DownloadCandidate(
            identifier="test.json",
            source_location=collector.BASE_URL,
            metadata={"collection_timestamp": "2025-12-05T19:35:00+00:00"},
            collection_params={},
            file_date=date(2025, 12, 5),
        )

        data = {"Constraint": []}
        content = json.dumps(data).encode('utf-8')
        assert collector.validate_content(content, candidate) is False

    def test_validate_missing_constraint_field(self, collector):
        """Test validation fails when Constraint field is missing."""
        candidate = DownloadCandidate(
            identifier="test.json",
            source_location=collector.BASE_URL,
            metadata={"collection_timestamp": "2025-12-05T19:35:00+00:00"},
            collection_params={},
            file_date=date(2025, 12, 5),
        )

        data = {"RefId": "05-Dec-2025, 14:35 EST"}
        content = json.dumps(data).encode('utf-8')
        assert collector.validate_content(content, candidate) is False

    def test_validate_constraint_required_fields(self, collector):
        """Test validation of required constraint fields."""
        candidate = DownloadCandidate(
            identifier="test.json",
            source_location=collector.BASE_URL,
            metadata={"collection_timestamp": "2025-12-05T19:35:00+00:00"},
            collection_params={},
            file_date=date(2025, 12, 5),
        )

        # Missing required field "Period"
        data = {
            "RefId": "05-Dec-2025, 14:35 EST",
            "Constraint": [
                {
                    "Name": "TEST_CONSTRAINT",
                    "Price": 12.45
                    # Missing "Period"
                }
            ]
        }
        content = json.dumps(data).encode('utf-8')
        assert collector.validate_content(content, candidate) is False

    def test_validate_constraint_name_non_empty(self, collector):
        """Test validation fails for empty constraint name."""
        candidate = DownloadCandidate(
            identifier="test.json",
            source_location=collector.BASE_URL,
            metadata={"collection_timestamp": "2025-12-05T19:35:00+00:00"},
            collection_params={},
            file_date=date(2025, 12, 5),
        )

        data = {
            "RefId": "05-Dec-2025, 14:35 EST",
            "Constraint": [
                {
                    "Name": "",  # Empty name
                    "Period": "2025-12-05T14:35:00",
                    "Price": 12.45
                }
            ]
        }
        content = json.dumps(data).encode('utf-8')
        assert collector.validate_content(content, candidate) is False

    def test_validate_price_numeric(self, collector):
        """Test validation of shadow price field."""
        candidate = DownloadCandidate(
            identifier="test.json",
            source_location=collector.BASE_URL,
            metadata={"collection_timestamp": "2025-12-05T19:35:00+00:00"},
            collection_params={},
            file_date=date(2025, 12, 5),
        )

        # Valid numeric price
        data = {
            "RefId": "05-Dec-2025, 14:35 EST",
            "Constraint": [
                {
                    "Name": "TEST_CONSTRAINT",
                    "Period": "2025-12-05T14:35:00",
                    "Price": 12.45
                }
            ]
        }
        content = json.dumps(data).encode('utf-8')
        assert collector.validate_content(content, candidate) is True

        # Invalid non-numeric price
        data["Constraint"][0]["Price"] = "invalid"
        content = json.dumps(data).encode('utf-8')
        assert collector.validate_content(content, candidate) is False

    def test_validate_period_timestamp(self, collector):
        """Test validation of Period timestamp field."""
        candidate = DownloadCandidate(
            identifier="test.json",
            source_location=collector.BASE_URL,
            metadata={"collection_timestamp": "2025-12-05T19:35:00+00:00"},
            collection_params={},
            file_date=date(2025, 12, 5),
        )

        # Valid timestamp
        data = {
            "RefId": "05-Dec-2025, 14:35 EST",
            "Constraint": [
                {
                    "Name": "TEST_CONSTRAINT",
                    "Period": "2025-12-05T14:35:00",
                    "Price": 12.45
                }
            ]
        }
        content = json.dumps(data).encode('utf-8')
        assert collector.validate_content(content, candidate) is True

        # Non-string period
        data["Constraint"][0]["Period"] = 12345
        content = json.dumps(data).encode('utf-8')
        assert collector.validate_content(content, candidate) is False

    def test_validate_optional_fields(self, collector):
        """Test validation passes with optional fields present."""
        candidate = DownloadCandidate(
            identifier="test.json",
            source_location=collector.BASE_URL,
            metadata={"collection_timestamp": "2025-12-05T19:35:00+00:00"},
            collection_params={},
            file_date=date(2025, 12, 5),
        )

        data = {
            "RefId": "05-Dec-2025, 14:35 EST",
            "Constraint": [
                {
                    "Name": "TEST_CONSTRAINT",
                    "Period": "2025-12-05T14:35:00",
                    "Price": 12.45,
                    "OVERRIDE": 1,
                    "CURVETYPE": "MW",
                    "BP1": 100.0,
                    "PC1": 95.0,
                    "BP2": 120.0,
                    "PC2": 98.0
                }
            ]
        }
        content = json.dumps(data).encode('utf-8')
        assert collector.validate_content(content, candidate) is True

    def test_validate_breakpoint_numeric(self, collector):
        """Test validation of breakpoint parameters."""
        candidate = DownloadCandidate(
            identifier="test.json",
            source_location=collector.BASE_URL,
            metadata={"collection_timestamp": "2025-12-05T19:35:00+00:00"},
            collection_params={},
            file_date=date(2025, 12, 5),
        )

        # Valid numeric breakpoints
        data = {
            "RefId": "05-Dec-2025, 14:35 EST",
            "Constraint": [
                {
                    "Name": "TEST_CONSTRAINT",
                    "Period": "2025-12-05T14:35:00",
                    "Price": 12.45,
                    "BP1": 100.0,
                    "PC1": 95.0
                }
            ]
        }
        content = json.dumps(data).encode('utf-8')
        assert collector.validate_content(content, candidate) is True

        # Invalid non-numeric breakpoint
        data["Constraint"][0]["BP1"] = "invalid"
        content = json.dumps(data).encode('utf-8')
        assert collector.validate_content(content, candidate) is False

    def test_validate_shadow_price_range_logging(self, collector, sample_api_response):
        """Test that shadow price range is logged during validation."""
        candidate = DownloadCandidate(
            identifier="test.json",
            source_location=collector.BASE_URL,
            metadata={"collection_timestamp": "2025-12-05T19:35:00+00:00"},
            collection_params={},
            file_date=date(2025, 12, 5),
        )

        content = json.dumps(sample_api_response).encode('utf-8')
        # Should log shadow price range
        assert collector.validate_content(content, candidate) is True

    def test_validate_invalid_json(self, collector):
        """Test validation handles invalid JSON gracefully."""
        candidate = DownloadCandidate(
            identifier="test.json",
            source_location=collector.BASE_URL,
            metadata={"collection_timestamp": "2025-12-05T19:35:00+00:00"},
            collection_params={},
            file_date=date(2025, 12, 5),
        )

        content = b"not valid json {"
        assert collector.validate_content(content, candidate) is False


class TestEndToEnd:
    """End-to-end integration tests."""

    def test_full_collection_flow(self, collector, mock_redis, mock_s3, sample_api_response):
        """Test complete collection flow."""
        # Mock API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "RefId": sample_api_response["RefId"],
            "Constraint": sample_api_response["Constraint"]
        }

        with patch('requests.get', return_value=mock_response):
            with patch.object(collector, '_upload_to_s3', return_value=("version_123", "etag_abc")):
                results = collector.run_collection()

        # Check that results were returned
        assert results is not None
        assert isinstance(results, dict)

    def test_collection_with_no_constraints(self, collector, mock_redis, mock_s3, empty_api_response):
        """Test collection flow when no binding constraints are active."""
        # Mock API response with empty constraints
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = empty_api_response

        with patch('requests.get', return_value=mock_response):
            with patch.object(collector, '_upload_to_s3', return_value=("version_123", "etag_abc")):
                results = collector.run_collection()

        # Should still succeed with empty data
        assert results is not None
        assert isinstance(results, dict)
