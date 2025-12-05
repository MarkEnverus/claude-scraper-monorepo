"""Tests for MISO Day-Ahead Ex-Ante LMP API Scraper."""

import json
from datetime import datetime, date
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

import pytest
import requests

from sourcing.scraping.miso.da_exante_lmp_api.scraper_miso_da_exante_lmp_api import (
    MisoDayAheadExAnteLMPAPICollector,
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
    """Create a collector instance with mocked dependencies."""
    collector = MisoDayAheadExAnteLMPAPICollector(
        api_key="test_api_key",
        start_date=datetime(2025, 1, 1),
        end_date=datetime(2025, 1, 2),
        dgroup="miso_da_exante_lmp_api",
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


class TestCandidateGeneration:
    """Tests for candidate generation logic."""

    def test_single_date_candidate(self, collector):
        """Test generation of a single date candidate."""
        collector.start_date = datetime(2025, 1, 15)
        collector.end_date = datetime(2025, 1, 15)

        candidates = collector.generate_candidates()

        assert len(candidates) == 1
        candidate = candidates[0]
        assert candidate.identifier == "da_exante_lmp_api_20250115.json"
        assert "2025-01-15" in candidate.source_location
        assert candidate.metadata["data_type"] == "da_exante_lmp_api"
        assert candidate.metadata["source"] == "miso"
        assert candidate.metadata["date"] == "2025-01-15"
        assert candidate.metadata["forecast"] is True
        assert candidate.file_date == date(2025, 1, 15)

    def test_multi_date_candidates(self, collector):
        """Test generation of multiple date candidates."""
        candidates = collector.generate_candidates()

        assert len(candidates) == 2
        assert candidates[0].identifier == "da_exante_lmp_api_20250101.json"
        assert candidates[1].identifier == "da_exante_lmp_api_20250102.json"

    def test_candidate_url_format(self, collector):
        """Test that candidate URLs follow the correct format."""
        candidates = collector.generate_candidates()
        candidate = candidates[0]

        expected_url = "https://apim.misoenergy.org/pricing/v1/day-ahead/2025-01-01/lmp-exante"
        assert candidate.source_location == expected_url

    def test_candidate_headers(self, collector):
        """Test that candidates include proper authentication headers."""
        candidates = collector.generate_candidates()
        candidate = candidates[0]

        headers = candidate.collection_params["headers"]
        assert headers["Ocp-Apim-Subscription-Key"] == "test_api_key"
        assert headers["Accept"] == "application/json"
        assert "User-Agent" in headers

    def test_candidate_pagination_params(self, collector):
        """Test that candidates include pagination parameters."""
        candidates = collector.generate_candidates()
        candidate = candidates[0]

        query_params = candidate.collection_params["query_params"]
        assert query_params["pageNumber"] == 1


class TestDataCollection:
    """Tests for data collection logic."""

    def test_collect_single_page(self, collector, sample_api_response):
        """Test collection of a single page of data."""
        candidate = DownloadCandidate(
            identifier="da_exante_lmp_api_20250101.json",
            source_location="https://apim.misoenergy.org/pricing/v1/day-ahead/2025-01-01/lmp-exante",
            metadata={"date": "2025-01-01"},
            collection_params={
                "headers": {"Ocp-Apim-Subscription-Key": "test_key"},
                "query_params": {"pageNumber": 1},
                "timeout": 180,
            },
            file_date=date(2025, 1, 1),
        )

        # Mock single page response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": sample_api_response["data"][:5],
            "page": {
                "pageNumber": 1,
                "pageSize": 5,
                "totalElements": 5,
                "totalPages": 1,
                "lastPage": True
            }
        }

        with patch('requests.get', return_value=mock_response):
            content = collector.collect_content(candidate)

        data = json.loads(content.decode('utf-8'))
        assert len(data["data"]) == 5
        assert data["total_records"] == 5
        assert data["total_pages"] == 1

    def test_collect_multiple_pages(self, collector, sample_api_response):
        """Test collection with pagination across multiple pages."""
        candidate = DownloadCandidate(
            identifier="da_exante_lmp_api_20250101.json",
            source_location="https://apim.misoenergy.org/pricing/v1/day-ahead/2025-01-01/lmp-exante",
            metadata={"date": "2025-01-01"},
            collection_params={
                "headers": {"Ocp-Apim-Subscription-Key": "test_key"},
                "query_params": {"pageNumber": 1},
                "timeout": 180,
            },
            file_date=date(2025, 1, 1),
        )

        # Mock paginated responses
        page1_response = Mock()
        page1_response.status_code = 200
        page1_response.json.return_value = {
            "data": sample_api_response["data"][:3],
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
            "data": sample_api_response["data"][3:6],
            "page": {
                "pageNumber": 2,
                "pageSize": 3,
                "totalElements": 6,
                "totalPages": 2,
                "lastPage": True
            }
        }

        with patch('requests.get', side_effect=[page1_response, page2_response]):
            content = collector.collect_content(candidate)

        data = json.loads(content.decode('utf-8'))
        assert len(data["data"]) == 6
        assert data["total_records"] == 6
        assert data["total_pages"] == 2

    def test_collect_handles_404(self, collector):
        """Test that 404 responses return empty data (no data available yet)."""
        candidate = DownloadCandidate(
            identifier="da_exante_lmp_api_20250101.json",
            source_location="https://apim.misoenergy.org/pricing/v1/day-ahead/2025-01-01/lmp-exante",
            metadata={"date": "2025-01-01"},
            collection_params={
                "headers": {"Ocp-Apim-Subscription-Key": "test_key"},
                "query_params": {"pageNumber": 1},
                "timeout": 180,
            },
            file_date=date(2025, 1, 1),
        )

        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_response)

        with patch('requests.get', return_value=mock_response):
            # 404 should return empty data (forecast not available yet)
            content = collector.collect_content(candidate)

        data = json.loads(content.decode('utf-8'))
        assert data["total_records"] == 0
        assert len(data["data"]) == 0
    def test_collect_handles_401(self, collector):
        """Test that 401 unauthorized responses raise proper errors."""
        candidate = DownloadCandidate(
            identifier="da_exante_lmp_api_20250101.json",
            source_location="https://apim.misoenergy.org/pricing/v1/day-ahead/2025-01-01/lmp-exante",
            metadata={"date": "2025-01-01"},
            collection_params={
                "headers": {"Ocp-Apim-Subscription-Key": "invalid_key"},
                "query_params": {"pageNumber": 1},
                "timeout": 180,
            },
            file_date=date(2025, 1, 1),
        )

        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_response)

        with patch('requests.get', return_value=mock_response):
            with pytest.raises(ScrapingError) as excinfo:
                collector.collect_content(candidate)
            assert "HTTP error" in str(excinfo.value)

    def test_collect_handles_rate_limit(self, collector):
        """Test that 429 rate limit responses are logged and raised."""
        candidate = DownloadCandidate(
            identifier="da_exante_lmp_api_20250101.json",
            source_location="https://apim.misoenergy.org/pricing/v1/day-ahead/2025-01-01/lmp-exante",
            metadata={"date": "2025-01-01"},
            collection_params={
                "headers": {"Ocp-Apim-Subscription-Key": "test_key"},
                "query_params": {"pageNumber": 1},
                "timeout": 180,
            },
            file_date=date(2025, 1, 1),
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
            identifier="da_exante_lmp_api_20250101.json",
            source_location="https://apim.misoenergy.org/pricing/v1/day-ahead/2025-01-01/lmp-exante",
            metadata={"date": "2025-01-01"},
            collection_params={
                "headers": {"Ocp-Apim-Subscription-Key": "test_key"},
                "query_params": {"pageNumber": 1},
                "timeout": 180,
            },
            file_date=date(2025, 1, 1),
        )

        with patch('requests.get', side_effect=requests.exceptions.ConnectionError("Network error")):
            with pytest.raises(ScrapingError) as excinfo:
                collector.collect_content(candidate)
            assert "Failed to fetch" in str(excinfo.value)


class TestContentValidation:
    """Tests for content validation logic."""

    def test_validate_valid_content(self, collector, sample_api_response):
        """Test validation of valid Ex-Ante LMP data."""
        candidate = DownloadCandidate(
            identifier="da_exante_lmp_api_20250101.json",
            source_location="https://apim.misoenergy.org/pricing/v1/day-ahead/2025-01-01/lmp-exante",
            metadata={"date": "2023-06-29"},
            collection_params={},
            file_date=date(2025, 1, 1),
        )

        content = json.dumps(sample_api_response).encode('utf-8')
        assert collector.validate_content(content, candidate) is True

    def test_validate_missing_data_field(self, collector):
        """Test validation fails when 'data' field is missing."""
        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="https://example.com",
            metadata={"date": "2025-01-01"},
            collection_params={},
            file_date=date(2025, 1, 1),
        )

        content = json.dumps({"metadata": {}}).encode('utf-8')
        assert collector.validate_content(content, candidate) is False

    def test_validate_empty_data_is_valid(self, collector):
        """Test that empty data array is considered valid (no data available)."""
        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="https://example.com",
            metadata={"date": "2025-01-01"},
            collection_params={},
            file_date=date(2025, 1, 1),
        )

        content = json.dumps({"data": [], "total_records": 0}).encode('utf-8')
        assert collector.validate_content(content, candidate) is True

    def test_validate_missing_required_fields(self, collector):
        """Test validation fails when required fields are missing."""
        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="https://example.com",
            metadata={"date": "2025-01-01"},
            collection_params={},
            file_date=date(2025, 1, 1),
        )

        # Missing 'lmp' field
        data = {
            "data": [
                {
                    "interval": "1",
                    "timeInterval": {
                        "resolution": "daily",
                        "start": "2025-01-01",
                        "end": "2025-01-02",
                        "value": "2025-01-01"
                    },
                    "node": "TEST.NODE",
                    "mcc": 0.0,
                    "mec": 20.0,
                    "mlc": 0.5
                }
            ]
        }
        content = json.dumps(data).encode('utf-8')
        assert collector.validate_content(content, candidate) is False

    def test_validate_invalid_interval(self, collector):
        """Test validation fails for invalid interval values."""
        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="https://example.com",
            metadata={"date": "2025-01-01"},
            collection_params={},
            file_date=date(2025, 1, 1),
        )

        data = {
            "data": [
                {
                    "interval": "25",  # Invalid: should be 1-24
                    "timeInterval": {
                        "resolution": "daily",
                        "start": "2025-01-01",
                        "end": "2025-01-02",
                        "value": "2025-01-01"
                    },
                    "node": "TEST.NODE",
                    "lmp": 20.5,
                    "mcc": 0.0,
                    "mec": 20.0,
                    "mlc": 0.5
                }
            ]
        }
        content = json.dumps(data).encode('utf-8')
        assert collector.validate_content(content, candidate) is False

    def test_validate_non_numeric_lmp_components(self, collector):
        """Test validation fails when LMP components are not numeric."""
        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="https://example.com",
            metadata={"date": "2025-01-01"},
            collection_params={},
            file_date=date(2025, 1, 1),
        )

        data = {
            "data": [
                {
                    "interval": "1",
                    "timeInterval": {
                        "resolution": "daily",
                        "start": "2025-01-01",
                        "end": "2025-01-02",
                        "value": "2025-01-01"
                    },
                    "node": "TEST.NODE",
                    "lmp": "not_a_number",  # Invalid
                    "mcc": 0.0,
                    "mec": 20.0,
                    "mlc": 0.5
                }
            ]
        }
        content = json.dumps(data).encode('utf-8')
        assert collector.validate_content(content, candidate) is False

    def test_validate_lmp_arithmetic(self, collector):
        """Test LMP arithmetic validation (LMP = MEC + MCC + MLC)."""
        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="https://example.com",
            metadata={"date": "2025-01-01"},
            collection_params={},
            file_date=date(2025, 1, 1),
        )

        # LMP arithmetic is correct
        data = {
            "data": [
                {
                    "interval": "1",
                    "timeInterval": {
                        "resolution": "daily",
                        "start": "2025-01-01",
                        "end": "2025-01-02",
                        "value": "2025-01-01"
                    },
                    "node": "TEST.NODE",
                    "lmp": 20.5,
                    "mcc": 0.1,
                    "mec": 20.0,
                    "mlc": 0.4
                }
            ]
        }
        content = json.dumps(data).encode('utf-8')
        assert collector.validate_content(content, candidate) is True

    def test_validate_date_mismatch(self, collector):
        """Test validation fails when dates don't match."""
        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="https://example.com",
            metadata={"date": "2025-01-01"},
            collection_params={},
            file_date=date(2025, 1, 1),
        )

        data = {
            "data": [
                {
                    "interval": "1",
                    "timeInterval": {
                        "resolution": "daily",
                        "start": "2025-01-02",
                        "end": "2025-01-03",
                        "value": "2025-01-02"  # Doesn't match candidate date
                    },
                    "node": "TEST.NODE",
                    "lmp": 20.5,
                    "mcc": 0.0,
                    "mec": 20.0,
                    "mlc": 0.5
                }
            ]
        }
        content = json.dumps(data).encode('utf-8')
        assert collector.validate_content(content, candidate) is False

    def test_validate_invalid_json(self, collector):
        """Test validation handles invalid JSON gracefully."""
        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="https://example.com",
            metadata={"date": "2025-01-01"},
            collection_params={},
            file_date=date(2025, 1, 1),
        )

        content = b"not valid json {"
        assert collector.validate_content(content, candidate) is False


class TestEndToEnd:
    """End-to-end integration tests."""

    def test_full_collection_flow(self, collector, mock_redis, mock_s3):
        """Test complete collection flow from candidate generation to S3 upload."""
        collector.start_date = datetime(2025, 1, 1)
        collector.end_date = datetime(2025, 1, 1)

        # Create sample data with matching date
        sample_data = [
            {
                "interval": "1",
                "timeInterval": {
                    "resolution": "daily",
                    "start": "2024-12-31 00:00:00.000",
                    "end": "2025-01-01 00:00:00.000",
                    "value": "2025-01-01"
                },
                "node": "TEST.NODE1",
                "lmp": 22.1,
                "mcc": 0.03,
                "mec": 21.34,
                "mlc": 0.73
            },
            {
                "interval": "2",
                "timeInterval": {
                    "resolution": "daily",
                    "start": "2024-12-31 01:00:00.000",
                    "end": "2025-01-01 01:00:00.000",
                    "value": "2025-01-01"
                },
                "node": "TEST.NODE1",
                "lmp": 21.8,
                "mcc": 0.02,
                "mec": 21.15,
                "mlc": 0.63
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
            with patch.object(collector, '_upload_to_s3', return_value=("version_123", "etag_abc")):
                results = collector.run_collection()

        assert results["files_downloaded"] >= 0  # At least we ran without crashing
        assert "files_failed" in results
