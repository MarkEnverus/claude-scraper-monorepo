"""Tests for MISO Coordinated Transaction Scheduling (CTS) scraper."""

import json
import pytest
from datetime import date
from pathlib import Path
from unittest.mock import Mock, patch

from sourcing.scraping.miso.coordinated_transaction_scheduling.scraper_miso_coordinated_transaction_scheduling import (
    MisoCtsCollector,
)
from sourcing.infrastructure.collection_framework import DownloadCandidate, ScrapingError


@pytest.fixture
def sample_response():
    """Load sample CTS API response."""
    fixture_path = Path(__file__).parent / "fixtures" / "sample_response.json"
    with open(fixture_path, "rb") as f:
        return f.read()


@pytest.fixture
def mock_redis():
    """Mock Redis client."""
    redis_mock = Mock()
    redis_mock.ping.return_value = True
    return redis_mock


@pytest.fixture
def collector(mock_redis):
    """Create MisoCtsCollector instance with mocked dependencies."""
    with patch("boto3.client"):
        collector = MisoCtsCollector(
            dgroup="miso_coordinated_transaction_scheduling",
            s3_bucket="test-bucket",
            s3_prefix="sourcing",
            redis_client=mock_redis,
            environment="dev",
        )
        return collector


class TestCandidateGeneration:
    """Tests for generate_candidates method."""

    def test_generate_candidates_current_snapshot(self, collector):
        """Test generating candidate for current snapshot (no date parameters)."""
        candidates = collector.generate_candidates()

        assert len(candidates) == 1
        candidate = candidates[0]

        assert isinstance(candidate, DownloadCandidate)
        assert candidate.source_location == collector.API_URL
        assert candidate.identifier.startswith("cts_")
        assert candidate.identifier.endswith(".json")
        assert candidate.metadata["data_type"] == "coordinated_transaction_scheduling"
        assert candidate.metadata["source"] == "miso"
        assert candidate.metadata["operating_day"] == "current"
        assert candidate.metadata["interval"] == "all"
        assert candidate.metadata["node"] == "all"
        assert candidate.collection_params["query_params"] == {}

    def test_generate_candidates_specific_date(self, collector):
        """Test generating candidate for specific date parameter."""
        candidates = collector.generate_candidates(date_param="2025-01-20")

        assert len(candidates) == 1
        candidate = candidates[0]

        assert "20250120" in candidate.identifier
        assert candidate.metadata["operating_day"] == "2025-01-20"
        assert candidate.collection_params["query_params"]["date"] == "2025-01-20"
        assert candidate.file_date == date(2025, 1, 20)

    def test_generate_candidates_date_range(self, collector):
        """Test generating candidates for date range."""
        start = date(2025, 1, 20)
        end = date(2025, 1, 22)
        candidates = collector.generate_candidates(start_date=start, end_date=end)

        assert len(candidates) == 3

        # Check dates are sequential
        expected_dates = ["2025-01-20", "2025-01-21", "2025-01-22"]
        for candidate, expected_date in zip(candidates, expected_dates):
            assert candidate.collection_params["query_params"]["date"] == expected_date
            assert candidate.metadata["operating_day"] == expected_date

    def test_generate_candidates_with_interval(self, collector):
        """Test generating candidate with interval filter."""
        candidates = collector.generate_candidates(
            date_param="2025-01-20",
            interval="14:35"
        )

        assert len(candidates) == 1
        candidate = candidates[0]

        assert candidate.collection_params["query_params"]["interval"] == "14:35"
        assert candidate.metadata["interval"] == "14:35"
        assert "int1435" in candidate.identifier

    def test_generate_candidates_with_node(self, collector):
        """Test generating candidate with node filter."""
        candidates = collector.generate_candidates(
            date_param="2025-01-20",
            node="MISO.PJM.INTERFACE1"
        )

        assert len(candidates) == 1
        candidate = candidates[0]

        assert candidate.collection_params["query_params"]["node"] == "MISO.PJM.INTERFACE1"
        assert candidate.metadata["node"] == "MISO.PJM.INTERFACE1"
        assert "MISO_PJM_INTERFACE1" in candidate.identifier

    def test_generate_candidates_all_filters(self, collector):
        """Test generating candidate with all filters combined."""
        candidates = collector.generate_candidates(
            date_param="2025-01-20",
            interval="14:35",
            node="MISO.PJM.INTERFACE1"
        )

        assert len(candidates) == 1
        candidate = candidates[0]

        query_params = candidate.collection_params["query_params"]
        assert query_params["date"] == "2025-01-20"
        assert query_params["interval"] == "14:35"
        assert query_params["node"] == "MISO.PJM.INTERFACE1"
        assert "20250120" in candidate.identifier
        assert "int1435" in candidate.identifier
        assert "MISO_PJM_INTERFACE1" in candidate.identifier


class TestContentCollection:
    """Tests for collect_content method."""

    @patch("requests.get")
    def test_collect_content_success(self, mock_get, collector, sample_response):
        """Test successful content collection."""
        mock_response = Mock()
        mock_response.content = sample_response
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        candidate = DownloadCandidate(
            identifier="cts_20250120.json",
            source_location=collector.API_URL,
            metadata={"data_type": "coordinated_transaction_scheduling"},
            collection_params={
                "query_params": {"date": "2025-01-20"},
                "headers": {"Accept": "application/json"},
                "timeout": 30,
            },
            file_date=date(2025, 1, 20),
        )

        content = collector.collect_content(candidate)

        assert content == sample_response
        mock_get.assert_called_once_with(
            collector.API_URL,
            params={"date": "2025-01-20"},
            headers={"Accept": "application/json"},
            timeout=30,
        )

    @patch("requests.get")
    def test_collect_content_http_error(self, mock_get, collector):
        """Test handling HTTP errors during collection."""
        import requests
        mock_get.side_effect = requests.exceptions.HTTPError("404 Not Found")

        candidate = DownloadCandidate(
            identifier="cts_20250120.json",
            source_location=collector.API_URL,
            metadata={"data_type": "coordinated_transaction_scheduling"},
            collection_params={
                "query_params": {},
                "headers": {},
                "timeout": 30,
            },
            file_date=date(2025, 1, 20),
        )

        with pytest.raises(ScrapingError, match="Failed to fetch CTS data"):
            collector.collect_content(candidate)

    @patch("requests.get")
    def test_collect_content_timeout(self, mock_get, collector):
        """Test handling timeout errors."""
        import requests
        mock_get.side_effect = requests.exceptions.Timeout("Request timed out")

        candidate = DownloadCandidate(
            identifier="cts_20250120.json",
            source_location=collector.API_URL,
            metadata={"data_type": "coordinated_transaction_scheduling"},
            collection_params={
                "query_params": {},
                "headers": {},
                "timeout": 30,
            },
            file_date=date(2025, 1, 20),
        )

        with pytest.raises(ScrapingError, match="Failed to fetch CTS data"):
            collector.collect_content(candidate)


class TestContentValidation:
    """Tests for validate_content method."""

    def test_validate_content_success(self, collector, sample_response):
        """Test validation of valid CTS response."""
        candidate = DownloadCandidate(
            identifier="cts_20250120.json",
            source_location=collector.API_URL,
            metadata={"data_type": "coordinated_transaction_scheduling"},
            collection_params={"query_params": {"date": "2025-01-20"}},
            file_date=date(2025, 1, 20),
        )

        is_valid = collector.validate_content(sample_response, candidate)
        assert is_valid is True

    def test_validate_content_invalid_json(self, collector):
        """Test validation fails for invalid JSON."""
        candidate = DownloadCandidate(
            identifier="cts_20250120.json",
            source_location=collector.API_URL,
            metadata={"data_type": "coordinated_transaction_scheduling"},
            collection_params={"query_params": {}},
            file_date=date(2025, 1, 20),
        )

        is_valid = collector.validate_content(b"not valid json", candidate)
        assert is_valid is False

    def test_validate_content_missing_data_field(self, collector):
        """Test validation fails when 'data' field is missing."""
        response = json.dumps({
            "metadata": {
                "operatingDay": "2025-01-20",
                "retrievalTimestamp": "2025-01-20T09:12:00-05:00",
                "dataQuality": "FORECAST"
            }
        }).encode()

        candidate = DownloadCandidate(
            identifier="cts_20250120.json",
            source_location=collector.API_URL,
            metadata={"data_type": "coordinated_transaction_scheduling"},
            collection_params={"query_params": {}},
            file_date=date(2025, 1, 20),
        )

        is_valid = collector.validate_content(response, candidate)
        assert is_valid is False

    def test_validate_content_missing_metadata_field(self, collector):
        """Test validation fails when 'metadata' field is missing."""
        response = json.dumps({
            "data": []
        }).encode()

        candidate = DownloadCandidate(
            identifier="cts_20250120.json",
            source_location=collector.API_URL,
            metadata={"data_type": "coordinated_transaction_scheduling"},
            collection_params={"query_params": {}},
            file_date=date(2025, 1, 20),
        )

        is_valid = collector.validate_content(response, candidate)
        assert is_valid is False

    def test_validate_content_empty_data_array(self, collector):
        """Test validation fails for empty data array."""
        response = json.dumps({
            "data": [],
            "metadata": {
                "operatingDay": "2025-01-20",
                "retrievalTimestamp": "2025-01-20T09:12:00-05:00",
                "dataQuality": "FORECAST"
            }
        }).encode()

        candidate = DownloadCandidate(
            identifier="cts_20250120.json",
            source_location=collector.API_URL,
            metadata={"data_type": "coordinated_transaction_scheduling"},
            collection_params={"query_params": {}},
            file_date=date(2025, 1, 20),
        )

        is_valid = collector.validate_content(response, candidate)
        assert is_valid is False

    def test_validate_content_invalid_data_quality(self, collector):
        """Test validation fails for invalid dataQuality value."""
        response = json.dumps({
            "data": [
                {
                    "timestamp": "2025-01-20T09:00:00-05:00",
                    "interval": "09:00",
                    "node": "MISO.PJM.INTERFACE1",
                    "forecastedLMP": {
                        "total": 45.50,
                        "components": {"energy": 42.00, "congestion": 2.50, "losses": 1.00},
                        "direction": "export"
                    },
                    "transactionVolume": {"mwh": 250.5, "direction": "MISO_TO_PJM"}
                }
            ],
            "metadata": {
                "operatingDay": "2025-01-20",
                "retrievalTimestamp": "2025-01-20T09:12:00-05:00",
                "dataQuality": "INVALID"
            }
        }).encode()

        candidate = DownloadCandidate(
            identifier="cts_20250120.json",
            source_location=collector.API_URL,
            metadata={"data_type": "coordinated_transaction_scheduling"},
            collection_params={"query_params": {}},
            file_date=date(2025, 1, 20),
        )

        is_valid = collector.validate_content(response, candidate)
        assert is_valid is False

    def test_validate_content_invalid_interval_format(self, collector):
        """Test validation fails for invalid interval format."""
        response = json.dumps({
            "data": [
                {
                    "timestamp": "2025-01-20T09:00:00-05:00",
                    "interval": "09:03",  # Invalid - not a 5-minute interval
                    "node": "MISO.PJM.INTERFACE1",
                    "forecastedLMP": {
                        "total": 45.50,
                        "components": {"energy": 42.00, "congestion": 2.50, "losses": 1.00},
                        "direction": "export"
                    },
                    "transactionVolume": {"mwh": 250.5, "direction": "MISO_TO_PJM"}
                }
            ],
            "metadata": {
                "operatingDay": "2025-01-20",
                "retrievalTimestamp": "2025-01-20T09:12:00-05:00",
                "dataQuality": "FORECAST"
            }
        }).encode()

        candidate = DownloadCandidate(
            identifier="cts_20250120.json",
            source_location=collector.API_URL,
            metadata={"data_type": "coordinated_transaction_scheduling"},
            collection_params={"query_params": {}},
            file_date=date(2025, 1, 20),
        )

        is_valid = collector.validate_content(response, candidate)
        assert is_valid is False

    def test_validate_content_lmp_arithmetic_mismatch(self, collector):
        """Test validation fails when LMP components don't sum to total."""
        response = json.dumps({
            "data": [
                {
                    "timestamp": "2025-01-20T09:00:00-05:00",
                    "interval": "09:00",
                    "node": "MISO.PJM.INTERFACE1",
                    "forecastedLMP": {
                        "total": 100.00,  # Wrong - should be 45.50
                        "components": {"energy": 42.00, "congestion": 2.50, "losses": 1.00},
                        "direction": "export"
                    },
                    "transactionVolume": {"mwh": 250.5, "direction": "MISO_TO_PJM"}
                }
            ],
            "metadata": {
                "operatingDay": "2025-01-20",
                "retrievalTimestamp": "2025-01-20T09:12:00-05:00",
                "dataQuality": "FORECAST"
            }
        }).encode()

        candidate = DownloadCandidate(
            identifier="cts_20250120.json",
            source_location=collector.API_URL,
            metadata={"data_type": "coordinated_transaction_scheduling"},
            collection_params={"query_params": {}},
            file_date=date(2025, 1, 20),
        )

        is_valid = collector.validate_content(response, candidate)
        assert is_valid is False

    def test_validate_content_invalid_lmp_direction(self, collector):
        """Test validation fails for invalid LMP direction."""
        response = json.dumps({
            "data": [
                {
                    "timestamp": "2025-01-20T09:00:00-05:00",
                    "interval": "09:00",
                    "node": "MISO.PJM.INTERFACE1",
                    "forecastedLMP": {
                        "total": 45.50,
                        "components": {"energy": 42.00, "congestion": 2.50, "losses": 1.00},
                        "direction": "invalid_direction"
                    },
                    "transactionVolume": {"mwh": 250.5, "direction": "MISO_TO_PJM"}
                }
            ],
            "metadata": {
                "operatingDay": "2025-01-20",
                "retrievalTimestamp": "2025-01-20T09:12:00-05:00",
                "dataQuality": "FORECAST"
            }
        }).encode()

        candidate = DownloadCandidate(
            identifier="cts_20250120.json",
            source_location=collector.API_URL,
            metadata={"data_type": "coordinated_transaction_scheduling"},
            collection_params={"query_params": {}},
            file_date=date(2025, 1, 20),
        )

        is_valid = collector.validate_content(response, candidate)
        assert is_valid is False

    def test_validate_content_invalid_transaction_direction(self, collector):
        """Test validation fails for invalid transaction direction."""
        response = json.dumps({
            "data": [
                {
                    "timestamp": "2025-01-20T09:00:00-05:00",
                    "interval": "09:00",
                    "node": "MISO.PJM.INTERFACE1",
                    "forecastedLMP": {
                        "total": 45.50,
                        "components": {"energy": 42.00, "congestion": 2.50, "losses": 1.00},
                        "direction": "export"
                    },
                    "transactionVolume": {"mwh": 250.5, "direction": "INVALID"}
                }
            ],
            "metadata": {
                "operatingDay": "2025-01-20",
                "retrievalTimestamp": "2025-01-20T09:12:00-05:00",
                "dataQuality": "FORECAST"
            }
        }).encode()

        candidate = DownloadCandidate(
            identifier="cts_20250120.json",
            source_location=collector.API_URL,
            metadata={"data_type": "coordinated_transaction_scheduling"},
            collection_params={"query_params": {}},
            file_date=date(2025, 1, 20),
        )

        is_valid = collector.validate_content(response, candidate)
        assert is_valid is False

    def test_validate_content_negative_transaction_volume(self, collector):
        """Test validation fails for negative transaction volume."""
        response = json.dumps({
            "data": [
                {
                    "timestamp": "2025-01-20T09:00:00-05:00",
                    "interval": "09:00",
                    "node": "MISO.PJM.INTERFACE1",
                    "forecastedLMP": {
                        "total": 45.50,
                        "components": {"energy": 42.00, "congestion": 2.50, "losses": 1.00},
                        "direction": "export"
                    },
                    "transactionVolume": {"mwh": -250.5, "direction": "MISO_TO_PJM"}
                }
            ],
            "metadata": {
                "operatingDay": "2025-01-20",
                "retrievalTimestamp": "2025-01-20T09:12:00-05:00",
                "dataQuality": "FORECAST"
            }
        }).encode()

        candidate = DownloadCandidate(
            identifier="cts_20250120.json",
            source_location=collector.API_URL,
            metadata={"data_type": "coordinated_transaction_scheduling"},
            collection_params={"query_params": {}},
            file_date=date(2025, 1, 20),
        )

        is_valid = collector.validate_content(response, candidate)
        assert is_valid is False

    def test_validate_content_operating_day_mismatch(self, collector):
        """Test validation fails when operating day doesn't match date parameter."""
        response = json.dumps({
            "data": [
                {
                    "timestamp": "2025-01-20T09:00:00-05:00",
                    "interval": "09:00",
                    "node": "MISO.PJM.INTERFACE1",
                    "forecastedLMP": {
                        "total": 45.50,
                        "components": {"energy": 42.00, "congestion": 2.50, "losses": 1.00},
                        "direction": "export"
                    },
                    "transactionVolume": {"mwh": 250.5, "direction": "MISO_TO_PJM"}
                }
            ],
            "metadata": {
                "operatingDay": "2025-01-21",  # Mismatch with query param
                "retrievalTimestamp": "2025-01-20T09:12:00-05:00",
                "dataQuality": "FORECAST"
            }
        }).encode()

        candidate = DownloadCandidate(
            identifier="cts_20250120.json",
            source_location=collector.API_URL,
            metadata={"data_type": "coordinated_transaction_scheduling"},
            collection_params={"query_params": {"date": "2025-01-20"}},
            file_date=date(2025, 1, 20),
        )

        is_valid = collector.validate_content(response, candidate)
        assert is_valid is False


class TestIntervalValidation:
    """Tests for _validate_interval_format method."""

    def test_validate_interval_valid_formats(self, collector):
        """Test validation passes for all valid 5-minute intervals."""
        valid_intervals = [
            "00:00", "00:05", "00:10", "00:15", "00:20", "00:25",
            "00:30", "00:35", "00:40", "00:45", "00:50", "00:55",
            "14:00", "14:05", "14:10", "14:15", "14:20", "14:25",
            "14:30", "14:35", "14:40", "14:45", "14:50", "14:55",
            "23:00", "23:05", "23:10", "23:15", "23:20", "23:25",
            "23:30", "23:35", "23:40", "23:45", "23:50", "23:55",
        ]

        for interval in valid_intervals:
            assert collector._validate_interval_format(interval) is True

    def test_validate_interval_invalid_minutes(self, collector):
        """Test validation fails for invalid minute values."""
        invalid_intervals = [
            "00:01", "00:03", "00:07", "00:12", "00:59",
            "14:02", "14:08", "14:13", "14:47",
        ]

        for interval in invalid_intervals:
            assert collector._validate_interval_format(interval) is False

    def test_validate_interval_invalid_hour(self, collector):
        """Test validation fails for invalid hour values."""
        invalid_intervals = [
            "24:00", "25:00", "-1:00", "99:00"
        ]

        for interval in invalid_intervals:
            assert collector._validate_interval_format(interval) is False

    def test_validate_interval_invalid_format(self, collector):
        """Test validation fails for invalid format."""
        invalid_formats = [
            "00-00", "0000", "00:00:00", "00", ":00",
            "not a time", "", None, 123
        ]

        for interval in invalid_formats:
            assert collector._validate_interval_format(interval) is False


class TestEndToEnd:
    """End-to-end integration tests."""

    @patch("requests.get")
    def test_run_collection_success(self, mock_get, collector, sample_response):
        """Test complete collection workflow."""
        mock_response = Mock()
        mock_response.content = sample_response
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        # Mock S3 upload
        with patch.object(collector, "_upload_to_s3") as mock_upload:
            mock_upload.return_value = ("version123", "etag123")

            # Mock hash registry
            with patch.object(collector.hash_registry, "exists") as mock_exists:
                mock_exists.return_value = False

                with patch.object(collector.hash_registry, "register") as mock_register:
                    # Run collection
                    results = collector.run_collection()

                    # Verify results
                    assert results["total_candidates"] == 1
                    assert results["collected"] == 1
                    assert results["skipped_duplicate"] == 0
                    assert results["failed"] == 0

                    # Verify S3 upload was called
                    assert mock_upload.call_count == 1

                    # Verify hash registry was called
                    assert mock_register.call_count == 1

    @patch("requests.get")
    def test_run_collection_duplicate_detection(self, mock_get, collector, sample_response):
        """Test duplicate detection with hash registry."""
        mock_response = Mock()
        mock_response.content = sample_response
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        # Mock hash exists (duplicate)
        with patch.object(collector.hash_registry, "exists") as mock_exists:
            mock_exists.return_value = True

            # Run collection
            results = collector.run_collection()

            # Verify duplicate was skipped
            assert results["total_candidates"] == 1
            assert results["collected"] == 0
            assert results["skipped_duplicate"] == 1
            assert results["failed"] == 0
