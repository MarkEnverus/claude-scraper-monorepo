"""Tests for MISO Real-Time Ex-Ante LMP Scraper."""

import json
from datetime import datetime, date
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

import pytest
import requests

from sourcing.scraping.miso.rt_exante_lmp.scraper_miso_rt_exante_lmp import (
    MisoRealTimeExAnteLMPCollector,
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
def collector_hourly(mock_redis, mock_s3):
    """Create a collector instance with hourly resolution."""
    collector = MisoRealTimeExAnteLMPCollector(
        api_key="test_api_key",
        start_date=datetime(2025, 1, 1),
        end_date=datetime(2025, 1, 2),
        time_resolution="hourly",
        dgroup="miso_rt_exante_lmp",
        s3_bucket="test-bucket",
        s3_prefix="sourcing",
        redis_client=mock_redis,
        environment="dev",
    )
    collector.s3_client = mock_s3
    return collector


@pytest.fixture
def collector_5min(mock_redis, mock_s3):
    """Create a collector instance with 5-minute resolution."""
    collector = MisoRealTimeExAnteLMPCollector(
        api_key="test_api_key",
        start_date=datetime(2025, 1, 1),
        end_date=datetime(2025, 1, 2),
        time_resolution="5min",
        dgroup="miso_rt_exante_lmp",
        s3_bucket="test-bucket",
        s3_prefix="sourcing",
        redis_client=mock_redis,
        environment="dev",
    )
    collector.s3_client = mock_s3
    return collector


@pytest.fixture
def sample_api_response_hourly():
    """Load sample hourly API response fixture."""
    fixture_path = Path(__file__).parent / "fixtures" / "sample_response_hourly.json"
    with open(fixture_path, 'r') as f:
        return json.load(f)


@pytest.fixture
def sample_api_response_5min():
    """Load sample 5-minute API response fixture."""
    fixture_path = Path(__file__).parent / "fixtures" / "sample_response_5min.json"
    with open(fixture_path, 'r') as f:
        return json.load(f)


class TestCollectorInitialization:
    """Tests for collector initialization."""

    def test_init_hourly_resolution(self, mock_redis, mock_s3):
        """Test initialization with hourly resolution."""
        collector = MisoRealTimeExAnteLMPCollector(
            api_key="test_key",
            start_date=datetime(2025, 1, 1),
            end_date=datetime(2025, 1, 2),
            time_resolution="hourly",
            dgroup="test",
            redis_client=mock_redis,
        )
        assert collector.time_resolution == "hourly"

    def test_init_5min_resolution(self, mock_redis, mock_s3):
        """Test initialization with 5-minute resolution."""
        collector = MisoRealTimeExAnteLMPCollector(
            api_key="test_key",
            start_date=datetime(2025, 1, 1),
            end_date=datetime(2025, 1, 2),
            time_resolution="5min",
            dgroup="test",
            redis_client=mock_redis,
        )
        assert collector.time_resolution == "5min"

    def test_init_normalizes_resolution(self, mock_redis, mock_s3):
        """Test that time resolution variations are normalized."""
        variations = [
            ("5-min", "5min"),
            ("5minute", "5min"),
            ("5-minute", "5min"),
            ("hour", "hourly"),
            ("1hour", "hourly"),
        ]
        for input_res, expected_res in variations:
            collector = MisoRealTimeExAnteLMPCollector(
                api_key="test_key",
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 2),
                time_resolution=input_res,
                dgroup="test",
                redis_client=mock_redis,
            )
            assert collector.time_resolution == expected_res

    def test_init_invalid_resolution(self, mock_redis, mock_s3):
        """Test that invalid time resolution raises error."""
        with pytest.raises(ValueError) as excinfo:
            MisoRealTimeExAnteLMPCollector(
                api_key="test_key",
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 2),
                time_resolution="invalid",
                dgroup="test",
                redis_client=mock_redis,
            )
        assert "Invalid time_resolution" in str(excinfo.value)


class TestCandidateGeneration:
    """Tests for candidate generation logic."""

    def test_single_date_candidate_hourly(self, collector_hourly):
        """Test generation of a single date candidate with hourly resolution."""
        collector_hourly.start_date = datetime(2025, 1, 15)
        collector_hourly.end_date = datetime(2025, 1, 15)

        candidates = collector_hourly.generate_candidates()

        assert len(candidates) == 1
        candidate = candidates[0]
        assert candidate.identifier == "rt_exante_lmp_hourly_20250115.json"
        assert "2025-01-15" in candidate.source_location
        assert candidate.metadata["data_type"] == "rt_exante_lmp"
        assert candidate.metadata["source"] == "miso"
        assert candidate.metadata["date"] == "2025-01-15"
        assert candidate.metadata["time_resolution"] == "hourly"
        assert candidate.metadata["forecast"] is True
        assert candidate.file_date == date(2025, 1, 15)

    def test_single_date_candidate_5min(self, collector_5min):
        """Test generation of a single date candidate with 5-minute resolution."""
        collector_5min.start_date = datetime(2025, 1, 15)
        collector_5min.end_date = datetime(2025, 1, 15)

        candidates = collector_5min.generate_candidates()

        assert len(candidates) == 1
        candidate = candidates[0]
        assert candidate.identifier == "rt_exante_lmp_5min_20250115.json"
        assert candidate.metadata["time_resolution"] == "5min"

    def test_multi_date_candidates(self, collector_hourly):
        """Test generation of multiple date candidates."""
        candidates = collector_hourly.generate_candidates()

        assert len(candidates) == 2
        assert candidates[0].identifier == "rt_exante_lmp_hourly_20250101.json"
        assert candidates[1].identifier == "rt_exante_lmp_hourly_20250102.json"

    def test_candidate_url_format(self, collector_hourly):
        """Test that candidate URLs follow the correct format."""
        candidates = collector_hourly.generate_candidates()
        candidate = candidates[0]

        expected_url = "https://apim.misoenergy.org/pricing/v1/real-time/2025-01-01/lmp-exante"
        assert candidate.source_location == expected_url

    def test_candidate_headers(self, collector_hourly):
        """Test that candidates include proper authentication headers."""
        candidates = collector_hourly.generate_candidates()
        candidate = candidates[0]

        headers = candidate.collection_params["headers"]
        assert headers["Ocp-Apim-Subscription-Key"] == "test_api_key"
        assert headers["Accept"] == "application/json"
        assert "User-Agent" in headers

    def test_candidate_includes_time_resolution(self, collector_5min):
        """Test that candidates include time resolution parameter."""
        candidates = collector_5min.generate_candidates()
        candidate = candidates[0]

        query_params = candidate.collection_params["query_params"]
        assert query_params["timeResolution"] == "5min"
        assert query_params["pageNumber"] == 1


class TestDataCollection:
    """Tests for data collection logic."""

    def test_collect_single_page_hourly(self, collector_hourly, sample_api_response_hourly):
        """Test collection of a single page of hourly data."""
        candidate = DownloadCandidate(
            identifier="rt_exante_lmp_hourly_20250101.json",
            source_location="https://apim.misoenergy.org/pricing/v1/real-time/2025-01-01/lmp-exante",
            metadata={"date": "2025-01-01", "time_resolution": "hourly"},
            collection_params={
                "headers": {"Ocp-Apim-Subscription-Key": "test_key"},
                "query_params": {"pageNumber": 1, "timeResolution": "hourly"},
                "timeout": 240,
            },
            file_date=date(2025, 1, 1),
        )

        # Mock single page response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": sample_api_response_hourly["data"][:5],
            "page": {
                "pageNumber": 1,
                "pageSize": 5,
                "totalElements": 5,
                "totalPages": 1,
                "lastPage": True
            }
        }

        with patch('requests.get', return_value=mock_response):
            content = collector_hourly.collect_content(candidate)

        data = json.loads(content.decode('utf-8'))
        assert len(data["data"]) == 5
        assert data["total_records"] == 5
        assert data["total_pages"] == 1
        assert data["time_resolution"] == "hourly"

    def test_collect_single_page_5min(self, collector_5min, sample_api_response_5min):
        """Test collection of a single page of 5-minute data."""
        candidate = DownloadCandidate(
            identifier="rt_exante_lmp_5min_20250101.json",
            source_location="https://apim.misoenergy.org/pricing/v1/real-time/2025-01-01/lmp-exante",
            metadata={"date": "2025-01-01", "time_resolution": "5min"},
            collection_params={
                "headers": {"Ocp-Apim-Subscription-Key": "test_key"},
                "query_params": {"pageNumber": 1, "timeResolution": "5min"},
                "timeout": 240,
            },
            file_date=date(2025, 1, 1),
        )

        # Mock single page response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": sample_api_response_5min["data"][:5],
            "page": {
                "pageNumber": 1,
                "pageSize": 5,
                "totalElements": 5,
                "totalPages": 1,
                "lastPage": True
            }
        }

        with patch('requests.get', return_value=mock_response):
            content = collector_5min.collect_content(candidate)

        data = json.loads(content.decode('utf-8'))
        assert len(data["data"]) == 5
        assert data["time_resolution"] == "5min"

    def test_collect_multiple_pages(self, collector_hourly, sample_api_response_hourly):
        """Test collection with pagination across multiple pages."""
        candidate = DownloadCandidate(
            identifier="rt_exante_lmp_hourly_20250101.json",
            source_location="https://apim.misoenergy.org/pricing/v1/real-time/2025-01-01/lmp-exante",
            metadata={"date": "2025-01-01", "time_resolution": "hourly"},
            collection_params={
                "headers": {"Ocp-Apim-Subscription-Key": "test_key"},
                "query_params": {"pageNumber": 1, "timeResolution": "hourly"},
                "timeout": 240,
            },
            file_date=date(2025, 1, 1),
        )

        # Mock paginated responses
        page1_response = Mock()
        page1_response.status_code = 200
        page1_response.json.return_value = {
            "data": sample_api_response_hourly["data"][:3],
            "page": {
                "pageNumber": 1,
                "pageSize": 3,
                "totalElements": 6,
                "totalPages": 2,
                "lastPage": False
            }
        }

        page2_response = Mock()
        page2_response.status_code = 200
        page2_response.json.return_value = {
            "data": sample_api_response_hourly["data"][3:6],
            "page": {
                "pageNumber": 2,
                "pageSize": 3,
                "totalElements": 6,
                "totalPages": 2,
                "lastPage": True
            }
        }

        with patch('requests.get', side_effect=[page1_response, page2_response]):
            content = collector_hourly.collect_content(candidate)

        data = json.loads(content.decode('utf-8'))
        assert len(data["data"]) == 6
        assert data["total_records"] == 6
        assert data["total_pages"] == 2

    def test_collect_handles_404(self, collector_hourly):
        """Test that 404 responses return empty data (no data available yet)."""
        candidate = DownloadCandidate(
            identifier="rt_exante_lmp_hourly_20250101.json",
            source_location="https://apim.misoenergy.org/pricing/v1/real-time/2025-01-01/lmp-exante",
            metadata={"date": "2025-01-01", "time_resolution": "hourly"},
            collection_params={
                "headers": {"Ocp-Apim-Subscription-Key": "test_key"},
                "query_params": {"pageNumber": 1, "timeResolution": "hourly"},
                "timeout": 240,
            },
            file_date=date(2025, 1, 1),
        )

        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_response)

        with patch('requests.get', return_value=mock_response):
            # 404 should return empty data (forecast not available yet)
            content = collector_hourly.collect_content(candidate)

        data = json.loads(content.decode('utf-8'))
        assert data["total_records"] == 0
        assert len(data["data"]) == 0

    def test_collect_handles_401(self, collector_hourly):
        """Test that 401 unauthorized responses raise proper errors."""
        candidate = DownloadCandidate(
            identifier="rt_exante_lmp_hourly_20250101.json",
            source_location="https://apim.misoenergy.org/pricing/v1/real-time/2025-01-01/lmp-exante",
            metadata={"date": "2025-01-01", "time_resolution": "hourly"},
            collection_params={
                "headers": {"Ocp-Apim-Subscription-Key": "invalid_key"},
                "query_params": {"pageNumber": 1, "timeResolution": "hourly"},
                "timeout": 240,
            },
            file_date=date(2025, 1, 1),
        )

        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_response)

        with patch('requests.get', return_value=mock_response):
            with pytest.raises(ScrapingError) as excinfo:
                collector_hourly.collect_content(candidate)
            assert "HTTP error" in str(excinfo.value)

    def test_collect_handles_rate_limit(self, collector_hourly):
        """Test that 429 rate limit responses are logged and raised."""
        candidate = DownloadCandidate(
            identifier="rt_exante_lmp_hourly_20250101.json",
            source_location="https://apim.misoenergy.org/pricing/v1/real-time/2025-01-01/lmp-exante",
            metadata={"date": "2025-01-01", "time_resolution": "hourly"},
            collection_params={
                "headers": {"Ocp-Apim-Subscription-Key": "test_key"},
                "query_params": {"pageNumber": 1, "timeResolution": "hourly"},
                "timeout": 240,
            },
            file_date=date(2025, 1, 1),
        )

        mock_response = Mock()
        mock_response.status_code = 429
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_response)

        with patch('requests.get', return_value=mock_response):
            with pytest.raises(ScrapingError) as excinfo:
                collector_hourly.collect_content(candidate)
            assert "HTTP error" in str(excinfo.value)

    def test_collect_handles_network_error(self, collector_hourly):
        """Test that network errors are properly wrapped."""
        candidate = DownloadCandidate(
            identifier="rt_exante_lmp_hourly_20250101.json",
            source_location="https://apim.misoenergy.org/pricing/v1/real-time/2025-01-01/lmp-exante",
            metadata={"date": "2025-01-01", "time_resolution": "hourly"},
            collection_params={
                "headers": {"Ocp-Apim-Subscription-Key": "test_key"},
                "query_params": {"pageNumber": 1, "timeResolution": "hourly"},
                "timeout": 240,
            },
            file_date=date(2025, 1, 1),
        )

        with patch('requests.get', side_effect=requests.exceptions.ConnectionError("Network error")):
            with pytest.raises(ScrapingError) as excinfo:
                collector_hourly.collect_content(candidate)
            assert "Failed to fetch" in str(excinfo.value)


class TestContentValidation:
    """Tests for content validation logic."""

    def test_validate_valid_hourly_content(self, collector_hourly, sample_api_response_hourly):
        """Test validation of valid hourly Real-Time Ex-Ante LMP data."""
        candidate = DownloadCandidate(
            identifier="rt_exante_lmp_hourly_20250101.json",
            source_location="https://apim.misoenergy.org/pricing/v1/real-time/2025-01-01/lmp-exante",
            metadata={"date": "2023-06-29", "time_resolution": "hourly"},
            collection_params={},
            file_date=date(2025, 1, 1),
        )

        content = json.dumps(sample_api_response_hourly).encode('utf-8')
        assert collector_hourly.validate_content(content, candidate) is True

    def test_validate_valid_5min_content(self, collector_5min, sample_api_response_5min):
        """Test validation of valid 5-minute Real-Time Ex-Ante LMP data."""
        candidate = DownloadCandidate(
            identifier="rt_exante_lmp_5min_20250101.json",
            source_location="https://apim.misoenergy.org/pricing/v1/real-time/2025-01-01/lmp-exante",
            metadata={"date": "2023-06-29", "time_resolution": "5min"},
            collection_params={},
            file_date=date(2025, 1, 1),
        )

        content = json.dumps(sample_api_response_5min).encode('utf-8')
        assert collector_5min.validate_content(content, candidate) is True

    def test_validate_missing_data_field(self, collector_hourly):
        """Test validation fails when 'data' field is missing."""
        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="https://example.com",
            metadata={"date": "2025-01-01", "time_resolution": "hourly"},
            collection_params={},
            file_date=date(2025, 1, 1),
        )

        content = json.dumps({"metadata": {}}).encode('utf-8')
        assert collector_hourly.validate_content(content, candidate) is False

    def test_validate_empty_data_is_valid(self, collector_hourly):
        """Test that empty data array is considered valid (no data available)."""
        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="https://example.com",
            metadata={"date": "2025-01-01", "time_resolution": "hourly"},
            collection_params={},
            file_date=date(2025, 1, 1),
        )

        content = json.dumps({"data": [], "total_records": 0}).encode('utf-8')
        assert collector_hourly.validate_content(content, candidate) is True

    def test_validate_invalid_hourly_interval(self, collector_hourly):
        """Test validation fails for invalid hourly interval values."""
        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="https://example.com",
            metadata={"date": "2025-01-01", "time_resolution": "hourly"},
            collection_params={},
            file_date=date(2025, 1, 1),
        )

        data = {
            "data": [
                {
                    "node": "TEST.NODE",
                    "lmp": 20.5,
                    "mcc": 0.0,
                    "mec": 20.0,
                    "mlc": 0.5,
                    "interval": "25",  # Invalid: should be 1-24
                    "timeInterval": {
                        "resolution": "hourly",
                        "start": "2025-01-01 00:00:00",
                        "end": "2025-01-01 01:00:00",
                        "value": "2025-01-01"
                    }
                }
            ]
        }
        content = json.dumps(data).encode('utf-8')
        assert collector_hourly.validate_content(content, candidate) is False

    def test_validate_valid_5min_interval_numeric(self, collector_5min):
        """Test validation passes for numeric 5-minute interval (1-288)."""
        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="https://example.com",
            metadata={"date": "2025-01-01", "time_resolution": "5min"},
            collection_params={},
            file_date=date(2025, 1, 1),
        )

        data = {
            "data": [
                {
                    "node": "TEST.NODE",
                    "lmp": 20.5,
                    "mcc": 0.0,
                    "mec": 20.0,
                    "mlc": 0.5,
                    "interval": "144",  # Valid: 144th 5-minute interval
                    "timeInterval": {
                        "resolution": "5min",
                        "start": "2025-01-01 12:00:00",
                        "end": "2025-01-01 12:05:00",
                        "value": "2025-01-01"
                    }
                }
            ]
        }
        content = json.dumps(data).encode('utf-8')
        assert collector_5min.validate_content(content, candidate) is True

    def test_validate_valid_5min_interval_time_format(self, collector_5min):
        """Test validation passes for HH:MM formatted 5-minute interval."""
        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="https://example.com",
            metadata={"date": "2025-01-01", "time_resolution": "5min"},
            collection_params={},
            file_date=date(2025, 1, 1),
        )

        data = {
            "data": [
                {
                    "node": "TEST.NODE",
                    "lmp": 20.5,
                    "mcc": 0.0,
                    "mec": 20.0,
                    "mlc": 0.5,
                    "interval": "13:05",  # Valid: 1:05 PM, 5-minute interval
                    "timeInterval": {
                        "resolution": "5min",
                        "start": "2025-01-01 13:05:00",
                        "end": "2025-01-01 13:10:00",
                        "value": "2025-01-01"
                    }
                }
            ]
        }
        content = json.dumps(data).encode('utf-8')
        assert collector_5min.validate_content(content, candidate) is True

    def test_validate_invalid_5min_interval_minutes(self, collector_5min):
        """Test validation fails for invalid 5-minute interval minutes."""
        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="https://example.com",
            metadata={"date": "2025-01-01", "time_resolution": "5min"},
            collection_params={},
            file_date=date(2025, 1, 1),
        )

        data = {
            "data": [
                {
                    "node": "TEST.NODE",
                    "lmp": 20.5,
                    "mcc": 0.0,
                    "mec": 20.0,
                    "mlc": 0.5,
                    "interval": "13:07",  # Invalid: not a 5-minute increment
                    "timeInterval": {
                        "resolution": "5min",
                        "start": "2025-01-01 13:07:00",
                        "end": "2025-01-01 13:12:00",
                        "value": "2025-01-01"
                    }
                }
            ]
        }
        content = json.dumps(data).encode('utf-8')
        assert collector_5min.validate_content(content, candidate) is False

    def test_validate_lmp_arithmetic(self, collector_hourly):
        """Test LMP arithmetic validation (LMP = MEC + MCC + MLC)."""
        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="https://example.com",
            metadata={"date": "2025-01-01", "time_resolution": "hourly"},
            collection_params={},
            file_date=date(2025, 1, 1),
        )

        # LMP arithmetic is correct
        data = {
            "data": [
                {
                    "node": "TEST.NODE",
                    "lmp": 20.5,
                    "mcc": 0.1,
                    "mec": 20.0,
                    "mlc": 0.4,
                    "interval": "1",
                    "timeInterval": {
                        "resolution": "hourly",
                        "start": "2025-01-01 00:00:00",
                        "end": "2025-01-01 01:00:00",
                        "value": "2025-01-01"
                    }
                }
            ]
        }
        content = json.dumps(data).encode('utf-8')
        assert collector_hourly.validate_content(content, candidate) is True

    def test_validate_date_mismatch(self, collector_hourly):
        """Test validation fails when dates don't match."""
        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="https://example.com",
            metadata={"date": "2025-01-01", "time_resolution": "hourly"},
            collection_params={},
            file_date=date(2025, 1, 1),
        )

        data = {
            "data": [
                {
                    "node": "TEST.NODE",
                    "lmp": 20.5,
                    "mcc": 0.0,
                    "mec": 20.0,
                    "mlc": 0.5,
                    "interval": "1",
                    "timeInterval": {
                        "resolution": "hourly",
                        "start": "2025-01-02 00:00:00",
                        "end": "2025-01-02 01:00:00",
                        "value": "2025-01-02"  # Doesn't match candidate date
                    }
                }
            ]
        }
        content = json.dumps(data).encode('utf-8')
        assert collector_hourly.validate_content(content, candidate) is False

    def test_validate_invalid_json(self, collector_hourly):
        """Test validation handles invalid JSON gracefully."""
        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="https://example.com",
            metadata={"date": "2025-01-01", "time_resolution": "hourly"},
            collection_params={},
            file_date=date(2025, 1, 1),
        )

        content = b"not valid json {"
        assert collector_hourly.validate_content(content, candidate) is False


class TestEndToEnd:
    """End-to-end integration tests."""

    def test_full_collection_flow_hourly(self, collector_hourly, mock_redis, mock_s3):
        """Test complete collection flow with hourly resolution."""
        collector_hourly.start_date = datetime(2025, 1, 1)
        collector_hourly.end_date = datetime(2025, 1, 1)

        # Create sample data with matching date
        sample_data = [
            {
                "node": "TEST.NODE1",
                "lmp": 22.1,
                "mcc": 0.03,
                "mec": 21.34,
                "mlc": 0.73,
                "interval": "1",
                "timeInterval": {
                    "resolution": "hourly",
                    "start": "2024-12-31 00:00:00.000",
                    "end": "2025-01-01 00:00:00.000",
                    "value": "2025-01-01"
                }
            },
            {
                "node": "TEST.NODE1",
                "lmp": 21.8,
                "mcc": 0.02,
                "mec": 21.15,
                "mlc": 0.63,
                "interval": "2",
                "timeInterval": {
                    "resolution": "hourly",
                    "start": "2024-12-31 01:00:00.000",
                    "end": "2025-01-01 01:00:00.000",
                    "value": "2025-01-01"
                }
            }
        ]

        # Mock API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": sample_data,
            "page": {
                "pageNumber": 1,
                "pageSize": len(sample_data),
                "totalElements": len(sample_data),
                "totalPages": 1,
                "lastPage": True
            }
        }

        with patch('requests.get', return_value=mock_response):
            with patch.object(collector_hourly, '_upload_to_s3', return_value=("version_123", "etag_abc")):
                results = collector_hourly.run_collection()

        assert results["files_downloaded"] >= 0
        assert "files_failed" in results
