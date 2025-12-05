"""Tests for MISO Day-Ahead Ex-Post LMP Scraper."""

import json
from datetime import datetime, date
from unittest.mock import MagicMock, patch

import pytest
import requests

from sourcing.scraping.miso.da_expost_lmp.scraper_miso_da_expost_lmp import (
    MisoDayAheadExPostLMPCollector,
)
from sourcing.infrastructure.collection_framework import (
    DownloadCandidate,
    ScrapingError,
)


@pytest.fixture
def mock_redis_client():
    """Mock Redis client for testing."""
    redis_client = MagicMock()
    redis_client.ping.return_value = True
    redis_client.get.return_value = None
    redis_client.set.return_value = True
    return redis_client


@pytest.fixture
def collector(mock_redis_client):
    """Create collector instance for testing."""
    return MisoDayAheadExPostLMPCollector(
        api_key="test-api-key",
        start_date=datetime(2025, 1, 1),
        end_date=datetime(2025, 1, 2),
        dgroup="miso_da_expost_lmp",
        s3_bucket="test-bucket",
        s3_prefix="sourcing",
        redis_client=mock_redis_client,
        environment="dev",
    )


@pytest.fixture
def sample_api_response():
    """Sample API response with LMP data."""
    return {
        "data": [
            {
                "interval": "1",
                "timeInterval": {
                    "resolution": "daily",
                    "start": "2025-01-01 00:00:00.000",
                    "end": "2025-01-02 00:00:00.000",
                    "value": "2025-01-02"
                },
                "node": "ALTW.WELLS1",
                "lmp": 21.60,
                "mcc": 0.02,
                "mec": 20.94,
                "mlc": 0.64
            },
            {
                "interval": "2",
                "timeInterval": {
                    "resolution": "daily",
                    "start": "2025-01-01 00:00:00.000",
                    "end": "2025-01-02 00:00:00.000",
                    "value": "2025-01-02"
                },
                "node": "ALTW.WELLS1",
                "lmp": 22.15,
                "mcc": 0.05,
                "mec": 21.35,
                "mlc": 0.75
            },
        ],
        "page": {
            "pageNumber": 1,
            "pageSize": 2,
            "totalElements": 2,
            "totalPages": 1,
            "lastPage": True
        }
    }


@pytest.fixture
def sample_paginated_response_page1():
    """First page of paginated response."""
    return {
        "data": [
            {
                "interval": "1",
                "timeInterval": {
                    "resolution": "daily",
                    "start": "2025-01-01 00:00:00.000",
                    "end": "2025-01-02 00:00:00.000",
                    "value": "2025-01-02"
                },
                "node": "NODE1",
                "lmp": 25.00,
                "mcc": 1.00,
                "mec": 23.00,
                "mlc": 1.00
            }
        ],
        "page": {
            "pageNumber": 1,
            "pageSize": 1,
            "totalElements": 2,
            "totalPages": 2,
            "lastPage": False
        }
    }


@pytest.fixture
def sample_paginated_response_page2():
    """Second page of paginated response."""
    return {
        "data": [
            {
                "interval": "2",
                "timeInterval": {
                    "resolution": "daily",
                    "start": "2025-01-01 00:00:00.000",
                    "end": "2025-01-02 00:00:00.000",
                    "value": "2025-01-02"
                },
                "node": "NODE2",
                "lmp": 26.00,
                "mcc": 1.50,
                "mec": 23.50,
                "mlc": 1.00
            }
        ],
        "page": {
            "pageNumber": 2,
            "pageSize": 1,
            "totalElements": 2,
            "totalPages": 2,
            "lastPage": True
        }
    }


class TestMisoDayAheadExPostLMPCollector:
    """Test suite for MisoDayAheadExPostLMPCollector."""

    def test_init(self, collector):
        """Test collector initialization."""
        assert collector.api_key == "test-api-key"
        assert collector.start_date == datetime(2025, 1, 1)
        assert collector.end_date == datetime(2025, 1, 2)
        assert collector.BASE_URL == "https://apim.misoenergy.org/pricing/v1/day-ahead"

    def test_generate_candidates_single_day(self, collector):
        """Test candidate generation for a single day."""
        collector.start_date = datetime(2025, 1, 15)
        collector.end_date = datetime(2025, 1, 15)

        candidates = collector.generate_candidates()

        assert len(candidates) == 1
        candidate = candidates[0]

        assert candidate.identifier == "da_expost_lmp_20250115.json"
        assert candidate.source_location == "https://apim.misoenergy.org/pricing/v1/day-ahead/2025-01-15/lmp-expost"
        assert candidate.metadata["data_type"] == "da_expost_lmp"
        assert candidate.metadata["source"] == "miso"
        assert candidate.metadata["date"] == "2025-01-15"
        assert candidate.file_date == date(2025, 1, 15)

    def test_generate_candidates_date_range(self, collector):
        """Test candidate generation for a date range."""
        candidates = collector.generate_candidates()

        assert len(candidates) == 2

        # Check first candidate
        assert candidates[0].identifier == "da_expost_lmp_20250101.json"
        assert candidates[0].metadata["date"] == "2025-01-01"
        assert "2025-01-01/lmp-expost" in candidates[0].source_location

        # Check second candidate
        assert candidates[1].identifier == "da_expost_lmp_20250102.json"
        assert candidates[1].metadata["date"] == "2025-01-02"
        assert "2025-01-02/lmp-expost" in candidates[1].source_location

    def test_generate_candidates_auth_header(self, collector):
        """Test that candidates include proper authentication headers."""
        candidates = collector.generate_candidates()
        candidate = candidates[0]

        assert "Ocp-Apim-Subscription-Key" in candidate.collection_params["headers"]
        assert candidate.collection_params["headers"]["Ocp-Apim-Subscription-Key"] == "test-api-key"
        assert candidate.collection_params["headers"]["Accept"] == "application/json"

    @patch('sourcing.scraping.miso.da_expost_lmp.scraper_miso_da_expost_lmp.requests.get')
    def test_collect_content_single_page(self, mock_get, collector, sample_api_response):
        """Test content collection with single page response."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_api_response
        mock_get.return_value = mock_response

        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="https://apim.misoenergy.org/pricing/v1/day-ahead/2025-01-02/lmp-expost",
            metadata={"date": "2025-01-02"},
            collection_params={
                "headers": {"Ocp-Apim-Subscription-Key": "test-key"},
                "timeout": 180,
                "query_params": {"pageNumber": 1}
            },
            file_date=date(2025, 1, 2),
        )

        content = collector.collect_content(candidate)

        # Verify request was made
        mock_get.assert_called_once()
        call_args = mock_get.call_args
        assert "pageNumber" in call_args.kwargs["params"]
        assert call_args.kwargs["params"]["pageNumber"] == 1

        # Parse and verify content
        data = json.loads(content.decode('utf-8'))
        assert "data" in data
        assert len(data["data"]) == 2
        assert data["total_records"] == 2
        assert data["total_pages"] == 1

    @patch('sourcing.scraping.miso.da_expost_lmp.scraper_miso_da_expost_lmp.requests.get')
    def test_collect_content_pagination(
        self, mock_get, collector, sample_paginated_response_page1, sample_paginated_response_page2
    ):
        """Test content collection with multiple pages."""
        # Mock responses for pages 1 and 2
        mock_response_page1 = MagicMock()
        mock_response_page1.status_code = 200
        mock_response_page1.json.return_value = sample_paginated_response_page1

        mock_response_page2 = MagicMock()
        mock_response_page2.status_code = 200
        mock_response_page2.json.return_value = sample_paginated_response_page2

        mock_get.side_effect = [mock_response_page1, mock_response_page2]

        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="https://apim.misoenergy.org/pricing/v1/day-ahead/2025-01-02/lmp-expost",
            metadata={"date": "2025-01-02"},
            collection_params={
                "headers": {"Ocp-Apim-Subscription-Key": "test-key"},
                "timeout": 180,
                "query_params": {"pageNumber": 1}
            },
            file_date=date(2025, 1, 2),
        )

        content = collector.collect_content(candidate)

        # Verify both pages were requested
        assert mock_get.call_count == 2

        # Verify page numbers in requests
        first_call_params = mock_get.call_args_list[0].kwargs["params"]
        second_call_params = mock_get.call_args_list[1].kwargs["params"]
        assert first_call_params["pageNumber"] == 1
        assert second_call_params["pageNumber"] == 2

        # Verify combined content
        data = json.loads(content.decode('utf-8'))
        assert len(data["data"]) == 2
        assert data["data"][0]["node"] == "NODE1"
        assert data["data"][1]["node"] == "NODE2"
        assert data["total_records"] == 2
        assert data["total_pages"] == 2

    @patch('sourcing.scraping.miso.da_expost_lmp.scraper_miso_da_expost_lmp.requests.get')
    def test_collect_content_404_not_found(self, mock_get, collector):
        """Test handling of 404 (data not available).
        
        404 is treated as a warning (data not yet available), not an error.
        The scraper returns empty data and does not raise an exception.
        """
        mock_response = MagicMock()
        mock_response.status_code = 404
        
        http_error = requests.exceptions.HTTPError()
        http_error.response = mock_response
        mock_response.raise_for_status.side_effect = http_error
        mock_get.return_value = mock_response

        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="https://apim.misoenergy.org/pricing/v1/day-ahead/2025-12-31/lmp-expost",
            metadata={"date": "2025-12-31"},
            collection_params={
                "headers": {"Ocp-Apim-Subscription-Key": "test-key"},
                "timeout": 180,
                "query_params": {"pageNumber": 1}
            },
            file_date=date(2025, 12, 31),
        )

        # 404 should not raise an error, just return empty data
        content = collector.collect_content(candidate)
        data = json.loads(content.decode('utf-8'))
        
        assert data["data"] == []
        assert data["total_records"] == 0

    @patch('sourcing.scraping.miso.da_expost_lmp.scraper_miso_da_expost_lmp.requests.get')
    def test_collect_content_401_unauthorized(self, mock_get, collector):
        """Test handling of 401 (invalid API key)."""
        mock_response = MagicMock()
        mock_response.status_code = 401
        
        http_error = requests.exceptions.HTTPError()
        http_error.response = mock_response
        mock_response.raise_for_status.side_effect = http_error
        mock_get.return_value = mock_response

        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="https://apim.misoenergy.org/pricing/v1/day-ahead/2025-01-01/lmp-expost",
            metadata={"date": "2025-01-01"},
            collection_params={
                "headers": {"Ocp-Apim-Subscription-Key": "invalid-key"},
                "timeout": 180,
                "query_params": {"pageNumber": 1}
            },
            file_date=date(2025, 1, 1),
        )

        with pytest.raises(ScrapingError) as exc_info:
            collector.collect_content(candidate)

        assert "HTTP error" in str(exc_info.value)

    def test_validate_content_valid(self, collector, sample_api_response):
        """Test validation of valid content."""
        content = json.dumps({
            "data": sample_api_response["data"],
            "total_records": 2,
            "metadata": {"date": "2025-01-02"}
        }).encode('utf-8')

        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="test",
            metadata={"date": "2025-01-02"},
            collection_params={},
            file_date=date(2025, 1, 2),
        )

        assert collector.validate_content(content, candidate) is True

    def test_validate_content_missing_data_field(self, collector):
        """Test validation fails when data field is missing."""
        content = json.dumps({"total_records": 0}).encode('utf-8')

        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="test",
            metadata={"date": "2025-01-02"},
            collection_params={},
            file_date=date(2025, 1, 2),
        )

        assert collector.validate_content(content, candidate) is False

    def test_validate_content_empty_data(self, collector):
        """Test validation passes for empty data (no data available for date)."""
        content = json.dumps({"data": [], "total_records": 0}).encode('utf-8')

        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="test",
            metadata={"date": "2025-01-02"},
            collection_params={},
            file_date=date(2025, 1, 2),
        )

        assert collector.validate_content(content, candidate) is True

    def test_validate_content_missing_required_fields(self, collector):
        """Test validation fails when required fields are missing."""
        # Missing 'lmp' field
        content = json.dumps({
            "data": [
                {
                    "interval": "1",
                    "timeInterval": {
                        "resolution": "daily",
                        "start": "2025-01-01 00:00:00.000",
                        "end": "2025-01-02 00:00:00.000",
                        "value": "2025-01-02"
                    },
                    "node": "TEST",
                    "mcc": 0.02,
                    "mec": 20.94,
                    "mlc": 0.64
                    # Missing 'lmp'
                }
            ],
            "total_records": 1
        }).encode('utf-8')

        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="test",
            metadata={"date": "2025-01-02"},
            collection_params={},
            file_date=date(2025, 1, 2),
        )

        assert collector.validate_content(content, candidate) is False

    def test_validate_content_invalid_interval(self, collector):
        """Test validation fails for invalid interval value."""
        content = json.dumps({
            "data": [
                {
                    "interval": "25",  # Invalid - should be 1-24
                    "timeInterval": {
                        "resolution": "daily",
                        "start": "2025-01-01 00:00:00.000",
                        "end": "2025-01-02 00:00:00.000",
                        "value": "2025-01-02"
                    },
                    "node": "TEST",
                    "lmp": 21.60,
                    "mcc": 0.02,
                    "mec": 20.94,
                    "mlc": 0.64
                }
            ],
            "total_records": 1
        }).encode('utf-8')

        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="test",
            metadata={"date": "2025-01-02"},
            collection_params={},
            file_date=date(2025, 1, 2),
        )

        assert collector.validate_content(content, candidate) is False

    def test_validate_content_non_numeric_lmp(self, collector):
        """Test validation fails when LMP is not numeric."""
        content = json.dumps({
            "data": [
                {
                    "interval": "1",
                    "timeInterval": {
                        "resolution": "daily",
                        "start": "2025-01-01 00:00:00.000",
                        "end": "2025-01-02 00:00:00.000",
                        "value": "2025-01-02"
                    },
                    "node": "TEST",
                    "lmp": "invalid",  # Should be numeric
                    "mcc": 0.02,
                    "mec": 20.94,
                    "mlc": 0.64
                }
            ],
            "total_records": 1
        }).encode('utf-8')

        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="test",
            metadata={"date": "2025-01-02"},
            collection_params={},
            file_date=date(2025, 1, 2),
        )

        assert collector.validate_content(content, candidate) is False

    def test_validate_content_date_mismatch(self, collector):
        """Test validation fails when date doesn't match expected."""
        content = json.dumps({
            "data": [
                {
                    "interval": "1",
                    "timeInterval": {
                        "resolution": "daily",
                        "start": "2025-01-01 00:00:00.000",
                        "end": "2025-01-02 00:00:00.000",
                        "value": "2025-01-15"  # Doesn't match expected 2025-01-02
                    },
                    "node": "TEST",
                    "lmp": 21.60,
                    "mcc": 0.02,
                    "mec": 20.94,
                    "mlc": 0.64
                }
            ],
            "total_records": 1
        }).encode('utf-8')

        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="test",
            metadata={"date": "2025-01-02"},
            collection_params={},
            file_date=date(2025, 1, 2),
        )

        assert collector.validate_content(content, candidate) is False

    def test_validate_content_lmp_arithmetic_warning(self, collector, caplog):
        """Test validation warns about LMP arithmetic mismatch but still passes."""
        import logging
        caplog.set_level(logging.WARNING)

        # LMP doesn't equal MEC + MCC + MLC
        content = json.dumps({
            "data": [
                {
                    "interval": "1",
                    "timeInterval": {
                        "resolution": "daily",
                        "start": "2025-01-01 00:00:00.000",
                        "end": "2025-01-02 00:00:00.000",
                        "value": "2025-01-02"
                    },
                    "node": "TEST",
                    "lmp": 99.99,  # Should be 21.60 (20.94 + 0.02 + 0.64)
                    "mcc": 0.02,
                    "mec": 20.94,
                    "mlc": 0.64
                }
            ],
            "total_records": 1
        }).encode('utf-8')

        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="test",
            metadata={"date": "2025-01-02"},
            collection_params={},
            file_date=date(2025, 1, 2),
        )

        # Should still validate True but log warning
        assert collector.validate_content(content, candidate) is True
        assert "LMP arithmetic mismatch" in caplog.text

    def test_validate_content_invalid_json(self, collector):
        """Test validation fails for invalid JSON."""
        content = b"not valid json"

        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="test",
            metadata={"date": "2025-01-02"},
            collection_params={},
            file_date=date(2025, 1, 2),
        )

        assert collector.validate_content(content, candidate) is False
