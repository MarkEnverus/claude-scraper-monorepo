"""Tests for MISO Day-Ahead Ex-Post ASM MCP scraper."""

import json
import copy
import pytest
from datetime import datetime, date
from unittest.mock import Mock, patch

from sourcing.scraping.miso.da_expost_asm_mcp.scraper_miso_da_expost_asm_mcp import (
    MisoDayAheadExPostASMMCPCollector
)
from sourcing.infrastructure.collection_framework import DownloadCandidate


@pytest.fixture
def sample_api_response():
    """Sample MISO ASM MCP API response."""
    return {
        "data": [
            {
                "interval": "1",
                "timeInterval": {
                    "resolution": "daily",
                    "start": "2024-01-01T00:00:00.0000000+00:00",
                    "end": "2024-01-02T00:00:00.0000000+00:00",
                    "value": "2024-01-01"
                },
                "product": "Regulation",
                "zone": "Zone 1",
                "mcp": 6.48
            },
            {
                "interval": "1",
                "timeInterval": {
                    "resolution": "daily",
                    "start": "2024-01-01T00:00:00.0000000+00:00",
                    "end": "2024-01-02T00:00:00.0000000+00:00",
                    "value": "2024-01-01"
                },
                "product": "Spin",
                "zone": "Zone 2",
                "mcp": 4.23
            }
        ],
        "page": {
            "pageNumber": 1,
            "pageSize": 100,
            "totalElements": 2,
            "totalPages": 1,
            "lastPage": True
        }
    }


@pytest.fixture
def collector():
    """Create a test collector instance."""
    mock_redis = Mock()
    return MisoDayAheadExPostASMMCPCollector(
        api_key="test_api_key",
        start_date=datetime(2024, 1, 1),
        end_date=datetime(2024, 1, 1),  # Same day for single day test
        dgroup="miso_da_expost_asm_mcp",
        s3_bucket="test-bucket",
        s3_prefix="sourcing",
        redis_client=mock_redis,
        environment="dev"
    )


class TestGenerateCandidates:
    """Tests for candidate generation."""

    def test_generate_candidates_single_day(self, collector):
        """Test generating candidates for a single day."""
        candidates = collector.generate_candidates()

        assert len(candidates) == 1
        assert candidates[0].identifier == "da_expost_asm_mcp_20240101.json"
        assert "2024-01-01/asm-expost" in candidates[0].source_location
        assert candidates[0].file_date == date(2024, 1, 1)

    def test_generate_candidates_multiple_days(self):
        """Test generating candidates for multiple days."""
        mock_redis = Mock()
        collector = MisoDayAheadExPostASMMCPCollector(
            api_key="test_key",
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 1, 5),
            dgroup="test",
            s3_bucket="test-bucket",
            s3_prefix="sourcing",
            redis_client=mock_redis,
            environment="dev"
        )

        candidates = collector.generate_candidates()

        assert len(candidates) == 5
        dates = [c.file_date for c in candidates]
        assert dates == [
            date(2024, 1, 1),
            date(2024, 1, 2),
            date(2024, 1, 3),
            date(2024, 1, 4),
            date(2024, 1, 5)
        ]

    def test_candidate_includes_api_key_header(self, collector):
        """Test that candidates include API key in headers."""
        candidates = collector.generate_candidates()

        headers = candidates[0].collection_params["headers"]
        assert "Ocp-Apim-Subscription-Key" in headers
        assert headers["Ocp-Apim-Subscription-Key"] == "test_api_key"

    def test_candidate_metadata(self, collector):
        """Test that candidates include proper metadata."""
        candidates = collector.generate_candidates()

        metadata = candidates[0].metadata
        assert metadata["data_type"] == "da_expost_asm_mcp"
        assert metadata["source"] == "miso"
        assert metadata["date"] == "2024-01-01"
        assert metadata["market_type"] == "day_ahead_ancillary_services"
        assert metadata["price_type"] == "ex_post"


class TestCollectContent:
    """Tests for content collection."""

    def test_collect_content_single_page(self, collector, sample_api_response):
        """Test collecting content with single page response."""
        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="https://test.com/api",
            metadata={"date": "2024-01-01"},
            collection_params={
                "headers": {"Ocp-Apim-Subscription-Key": "test_key"},
                "query_params": {"pageNumber": 1},
                "timeout": 30
            },
            file_date=date(2024, 1, 1)
        )

        with patch("requests.get") as mock_get:
            mock_response = Mock()
            mock_response.json.return_value = sample_api_response
            mock_response.raise_for_status = Mock()
            mock_get.return_value = mock_response

            content = collector.collect_content(candidate)

            # Verify content is valid JSON
            data = json.loads(content)
            assert "data" in data
            assert len(data["data"]) == 2
            assert data["total_records"] == 2

    def test_collect_content_multiple_pages(self, collector, sample_api_response):
        """Test collecting content with pagination."""
        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="https://test.com/api",
            metadata={"date": "2024-01-01"},
            collection_params={
                "headers": {"Ocp-Apim-Subscription-Key": "test_key"},
                "query_params": {"pageNumber": 1},
                "timeout": 30
            },
            file_date=date(2024, 1, 1)
        )

        # Mock two pages - use deep copy to avoid shared dict references
        page1 = copy.deepcopy(sample_api_response)
        page1["page"]["lastPage"] = False
        page1["page"]["pageNumber"] = 1

        page2 = copy.deepcopy(sample_api_response)
        page2["page"]["pageNumber"] = 2
        page2["page"]["lastPage"] = True

        with patch("requests.get") as mock_get:
            mock_response1 = Mock()
            mock_response1.json.return_value = page1
            mock_response1.raise_for_status = Mock()

            mock_response2 = Mock()
            mock_response2.json.return_value = page2
            mock_response2.raise_for_status = Mock()

            mock_get.side_effect = [mock_response1, mock_response2]

            content = collector.collect_content(candidate)

            # Verify two requests were made
            assert mock_get.call_count == 2

            # Verify all data was collected
            data = json.loads(content)
            assert len(data["data"]) == 4  # 2 records per page


class TestValidateContent:
    """Tests for content validation."""

    def test_validate_valid_content(self, collector):
        """Test validation of valid content."""
        valid_data = {
            "data": [
                {
                    "interval": "1",
                    "timeInterval": {
                        "resolution": "daily",
                        "start": "2024-01-01T00:00:00Z",
                        "end": "2024-01-02T00:00:00Z",
                        "value": "2024-01-01"
                    },
                    "product": "Regulation",
                    "zone": "Zone 1",
                    "mcp": 5.5
                }
            ],
            "total_records": 1
        }
        content = json.dumps(valid_data).encode('utf-8')

        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="test",
            metadata={"date": "2024-01-01"},
            collection_params={},
            file_date=date(2024, 1, 1)
        )

        result = collector.validate_content(content, candidate)
        assert result is True

    def test_validate_empty_data(self, collector):
        """Test validation with empty data (valid case)."""
        empty_data = {"data": [], "total_records": 0}
        content = json.dumps(empty_data).encode('utf-8')

        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="test",
            metadata={"date": "2024-01-01"},
            collection_params={},
            file_date=date(2024, 1, 1)
        )

        result = collector.validate_content(content, candidate)
        assert result is True

    def test_validate_missing_data_field(self, collector):
        """Test validation with missing data field."""
        invalid_data = {"records": []}
        content = json.dumps(invalid_data).encode('utf-8')

        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="test",
            metadata={"date": "2024-01-01"},
            collection_params={},
            file_date=date(2024, 1, 1)
        )

        result = collector.validate_content(content, candidate)
        assert result is False

    def test_validate_missing_required_field(self, collector):
        """Test validation with missing required field."""
        invalid_data = {
            "data": [{
                "interval": "1",
                "timeInterval": {
                    "resolution": "daily",
                    "start": "2024-01-01T00:00:00Z",
                    "end": "2024-01-02T00:00:00Z",
                    "value": "2024-01-01"
                },
                "product": "Regulation",
                # Missing 'zone' and 'mcp'
            }],
            "total_records": 1
        }
        content = json.dumps(invalid_data).encode('utf-8')

        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="test",
            metadata={"date": "2024-01-01"},
            collection_params={},
            file_date=date(2024, 1, 1)
        )

        result = collector.validate_content(content, candidate)
        assert result is False

    def test_validate_invalid_json(self, collector):
        """Test validation with invalid JSON."""
        content = b"not valid json"

        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="test",
            metadata={"date": "2024-01-01"},
            collection_params={},
            file_date=date(2024, 1, 1)
        )

        result = collector.validate_content(content, candidate)
        assert result is False

    def test_validate_non_numeric_mcp(self, collector):
        """Test validation with non-numeric MCP value."""
        invalid_data = {
            "data": [{
                "interval": "1",
                "timeInterval": {
                    "resolution": "daily",
                    "start": "2024-01-01T00:00:00Z",
                    "end": "2024-01-02T00:00:00Z",
                    "value": "2024-01-01"
                },
                "product": "Regulation",
                "zone": "Zone 1",
                "mcp": "not a number"
            }],
            "total_records": 1
        }
        content = json.dumps(invalid_data).encode('utf-8')

        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="test",
            metadata={"date": "2024-01-01"},
            collection_params={},
            file_date=date(2024, 1, 1)
        )

        result = collector.validate_content(content, candidate)
        assert result is False
