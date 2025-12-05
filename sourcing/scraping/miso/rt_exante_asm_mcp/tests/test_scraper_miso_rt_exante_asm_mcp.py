"""Tests for MISO Real-Time Ex-Ante ASM MCP scraper."""

import json
import copy
import pytest
from datetime import datetime, date
from unittest.mock import Mock, patch

from sourcing.scraping.miso.rt_exante_asm_mcp.scraper_miso_rt_exante_asm_mcp import (
    MisoRealTimeExAnteASMMCPCollector
)
from sourcing.infrastructure.collection_framework import DownloadCandidate


@pytest.fixture
def sample_api_response_hourly():
    """Sample MISO RT Ex-Ante ASM MCP API response (hourly resolution)."""
    return {
        "data": [
            {
                "interval": "1",
                "timeInterval": {
                    "resolution": "hourly",
                    "start": "2024-01-01T00:00:00.0000000+00:00",
                    "end": "2024-01-01T01:00:00.0000000+00:00",
                    "value": "2024-01-01"
                },
                "product": "Regulation",
                "zone": "Zone 1",
                "mcp": 6.48
            },
            {
                "interval": "1",
                "timeInterval": {
                    "resolution": "hourly",
                    "start": "2024-01-01T00:00:00.0000000+00:00",
                    "end": "2024-01-01T01:00:00.0000000+00:00",
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
def sample_api_response_5min():
    """Sample MISO RT Ex-Ante ASM MCP API response (5-minute resolution)."""
    return {
        "data": [
            {
                "interval": "00:05",
                "timeInterval": {
                    "resolution": "5min",
                    "start": "2024-01-01T00:00:00.0000000+00:00",
                    "end": "2024-01-01T00:05:00.0000000+00:00",
                    "value": "2024-01-01"
                },
                "product": "Regulation",
                "zone": "Zone 1",
                "mcp": 6.48
            },
            {
                "interval": "00:10",
                "timeInterval": {
                    "resolution": "5min",
                    "start": "2024-01-01T00:05:00.0000000+00:00",
                    "end": "2024-01-01T00:10:00.0000000+00:00",
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
def collector_hourly():
    """Create a test collector instance with hourly resolution."""
    mock_redis = Mock()
    return MisoRealTimeExAnteASMMCPCollector(
        api_key="test_api_key",
        start_date=datetime(2024, 1, 1),
        end_date=datetime(2024, 1, 1),  # Same day for single day test
        time_resolution="hourly",
        dgroup="miso_rt_exante_asm_mcp",
        s3_bucket="test-bucket",
        s3_prefix="sourcing",
        redis_client=mock_redis,
        environment="dev"
    )


@pytest.fixture
def collector_5min():
    """Create a test collector instance with 5-minute resolution."""
    mock_redis = Mock()
    return MisoRealTimeExAnteASMMCPCollector(
        api_key="test_api_key",
        start_date=datetime(2024, 1, 1),
        end_date=datetime(2024, 1, 1),
        time_resolution="5min",
        dgroup="miso_rt_exante_asm_mcp",
        s3_bucket="test-bucket",
        s3_prefix="sourcing",
        redis_client=mock_redis,
        environment="dev"
    )


class TestInitialization:
    """Tests for collector initialization."""

    def test_init_hourly_resolution(self, collector_hourly):
        """Test initialization with hourly resolution."""
        assert collector_hourly.time_resolution == "hourly"
        assert collector_hourly.api_key == "test_api_key"

    def test_init_5min_resolution(self, collector_5min):
        """Test initialization with 5-minute resolution."""
        assert collector_5min.time_resolution == "5min"

    def test_init_invalid_resolution(self):
        """Test initialization with invalid resolution."""
        mock_redis = Mock()
        with pytest.raises(ValueError, match="Invalid time_resolution"):
            MisoRealTimeExAnteASMMCPCollector(
                api_key="test_key",
                start_date=datetime(2024, 1, 1),
                end_date=datetime(2024, 1, 1),
                time_resolution="invalid",
                dgroup="test",
                s3_bucket="test-bucket",
                s3_prefix="sourcing",
                redis_client=mock_redis,
                environment="dev"
            )


class TestGenerateCandidates:
    """Tests for candidate generation."""

    def test_generate_candidates_single_day_hourly(self, collector_hourly):
        """Test generating candidates for a single day with hourly resolution."""
        candidates = collector_hourly.generate_candidates()

        assert len(candidates) == 1
        assert candidates[0].identifier == "rt_exante_asm_mcp_hourly_20240101.json"
        assert "2024-01-01/asm-exante" in candidates[0].source_location
        assert candidates[0].file_date == date(2024, 1, 1)
        assert candidates[0].metadata["time_resolution"] == "hourly"

    def test_generate_candidates_single_day_5min(self, collector_5min):
        """Test generating candidates for a single day with 5-minute resolution."""
        candidates = collector_5min.generate_candidates()

        assert len(candidates) == 1
        assert candidates[0].identifier == "rt_exante_asm_mcp_5min_20240101.json"
        assert candidates[0].metadata["time_resolution"] == "5min"

    def test_generate_candidates_multiple_days(self):
        """Test generating candidates for multiple days."""
        mock_redis = Mock()
        collector = MisoRealTimeExAnteASMMCPCollector(
            api_key="test_key",
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 1, 5),
            time_resolution="hourly",
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

    def test_candidate_includes_api_key_header(self, collector_hourly):
        """Test that candidates include API key in headers."""
        candidates = collector_hourly.generate_candidates()

        headers = candidates[0].collection_params["headers"]
        assert "Ocp-Apim-Subscription-Key" in headers
        assert headers["Ocp-Apim-Subscription-Key"] == "test_api_key"

    def test_candidate_includes_time_resolution(self, collector_hourly):
        """Test that candidates include timeResolution query parameter."""
        candidates = collector_hourly.generate_candidates()

        params = candidates[0].collection_params["query_params"]
        assert "timeResolution" in params
        assert params["timeResolution"] == "hourly"

    def test_candidate_metadata(self, collector_hourly):
        """Test that candidates include proper metadata."""
        candidates = collector_hourly.generate_candidates()

        metadata = candidates[0].metadata
        assert metadata["data_type"] == "rt_exante_asm_mcp"
        assert metadata["source"] == "miso"
        assert metadata["date"] == "2024-01-01"
        assert metadata["market_type"] == "real_time_ancillary_services_exante"
        assert metadata["forecast"] is True


class TestCollectContent:
    """Tests for content collection."""

    def test_collect_content_single_page_hourly(self, collector_hourly, sample_api_response_hourly):
        """Test collecting content with single page response (hourly)."""
        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="https://test.com/api",
            metadata={"date": "2024-01-01", "time_resolution": "hourly"},
            collection_params={
                "headers": {"Ocp-Apim-Subscription-Key": "test_key"},
                "query_params": {"pageNumber": 1, "timeResolution": "hourly"},
                "timeout": 30
            },
            file_date=date(2024, 1, 1)
        )

        with patch("requests.get") as mock_get:
            mock_response = Mock()
            mock_response.json.return_value = sample_api_response_hourly
            mock_response.raise_for_status = Mock()
            mock_get.return_value = mock_response

            content = collector_hourly.collect_content(candidate)

            # Verify content is valid JSON
            data = json.loads(content)
            assert "data" in data
            assert len(data["data"]) == 2
            assert data["total_records"] == 2
            assert data["time_resolution"] == "hourly"

    def test_collect_content_single_page_5min(self, collector_5min, sample_api_response_5min):
        """Test collecting content with single page response (5-minute)."""
        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="https://test.com/api",
            metadata={"date": "2024-01-01", "time_resolution": "5min"},
            collection_params={
                "headers": {"Ocp-Apim-Subscription-Key": "test_key"},
                "query_params": {"pageNumber": 1, "timeResolution": "5min"},
                "timeout": 30
            },
            file_date=date(2024, 1, 1)
        )

        with patch("requests.get") as mock_get:
            mock_response = Mock()
            mock_response.json.return_value = sample_api_response_5min
            mock_response.raise_for_status = Mock()
            mock_get.return_value = mock_response

            content = collector_5min.collect_content(candidate)

            # Verify content is valid JSON
            data = json.loads(content)
            assert "data" in data
            assert len(data["data"]) == 2
            assert data["total_records"] == 2
            assert data["time_resolution"] == "5min"

    def test_collect_content_multiple_pages(self, collector_hourly, sample_api_response_hourly):
        """Test collecting content with pagination."""
        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="https://test.com/api",
            metadata={"date": "2024-01-01", "time_resolution": "hourly"},
            collection_params={
                "headers": {"Ocp-Apim-Subscription-Key": "test_key"},
                "query_params": {"pageNumber": 1, "timeResolution": "hourly"},
                "timeout": 30
            },
            file_date=date(2024, 1, 1)
        )

        # Mock two pages - use deep copy to avoid shared dict references
        page1 = copy.deepcopy(sample_api_response_hourly)
        page1["page"]["lastPage"] = False
        page1["page"]["pageNumber"] = 1
        page1["page"]["totalPages"] = 2

        page2 = copy.deepcopy(sample_api_response_hourly)
        page2["page"]["pageNumber"] = 2
        page2["page"]["lastPage"] = True
        page2["page"]["totalPages"] = 2

        with patch("requests.get") as mock_get:
            mock_response1 = Mock()
            mock_response1.json.return_value = page1
            mock_response1.raise_for_status = Mock()

            mock_response2 = Mock()
            mock_response2.json.return_value = page2
            mock_response2.raise_for_status = Mock()

            mock_get.side_effect = [mock_response1, mock_response2]

            content = collector_hourly.collect_content(candidate)

            # Verify two requests were made
            assert mock_get.call_count == 2

            # Verify all data was collected
            data = json.loads(content)
            assert len(data["data"]) == 4  # 2 records per page


class TestValidateContent:
    """Tests for content validation."""

    def test_validate_valid_content_hourly(self, collector_hourly):
        """Test validation of valid content (hourly resolution)."""
        valid_data = {
            "data": [
                {
                    "interval": "1",
                    "timeInterval": {
                        "resolution": "hourly",
                        "start": "2024-01-01T00:00:00Z",
                        "end": "2024-01-01T01:00:00Z",
                        "value": "2024-01-01"
                    },
                    "product": "Regulation",
                    "zone": "Zone 1",
                    "mcp": 5.5
                }
            ],
            "total_records": 1,
            "time_resolution": "hourly"
        }
        content = json.dumps(valid_data).encode('utf-8')

        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="test",
            metadata={"date": "2024-01-01", "time_resolution": "hourly"},
            collection_params={},
            file_date=date(2024, 1, 1)
        )

        result = collector_hourly.validate_content(content, candidate)
        assert result is True

    def test_validate_valid_content_5min(self, collector_5min):
        """Test validation of valid content (5-minute resolution)."""
        valid_data = {
            "data": [
                {
                    "interval": "00:05",
                    "timeInterval": {
                        "resolution": "5min",
                        "start": "2024-01-01T00:00:00Z",
                        "end": "2024-01-01T00:05:00Z",
                        "value": "2024-01-01"
                    },
                    "product": "Spin",
                    "zone": "Zone 3",
                    "mcp": 3.2
                }
            ],
            "total_records": 1,
            "time_resolution": "5min"
        }
        content = json.dumps(valid_data).encode('utf-8')

        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="test",
            metadata={"date": "2024-01-01", "time_resolution": "5min"},
            collection_params={},
            file_date=date(2024, 1, 1)
        )

        result = collector_5min.validate_content(content, candidate)
        assert result is True

    def test_validate_empty_data(self, collector_hourly):
        """Test validation with empty data (valid case)."""
        empty_data = {"data": [], "total_records": 0, "time_resolution": "hourly"}
        content = json.dumps(empty_data).encode('utf-8')

        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="test",
            metadata={"date": "2024-01-01", "time_resolution": "hourly"},
            collection_params={},
            file_date=date(2024, 1, 1)
        )

        result = collector_hourly.validate_content(content, candidate)
        assert result is True

    def test_validate_missing_data_field(self, collector_hourly):
        """Test validation with missing data field."""
        invalid_data = {"records": []}
        content = json.dumps(invalid_data).encode('utf-8')

        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="test",
            metadata={"date": "2024-01-01", "time_resolution": "hourly"},
            collection_params={},
            file_date=date(2024, 1, 1)
        )

        result = collector_hourly.validate_content(content, candidate)
        assert result is False

    def test_validate_missing_required_field(self, collector_hourly):
        """Test validation with missing required field."""
        invalid_data = {
            "data": [{
                "interval": "1",
                "timeInterval": {
                    "resolution": "hourly",
                    "start": "2024-01-01T00:00:00Z",
                    "end": "2024-01-01T01:00:00Z",
                    "value": "2024-01-01"
                },
                "product": "Regulation",
                # Missing 'zone' and 'mcp'
            }],
            "total_records": 1,
            "time_resolution": "hourly"
        }
        content = json.dumps(invalid_data).encode('utf-8')

        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="test",
            metadata={"date": "2024-01-01", "time_resolution": "hourly"},
            collection_params={},
            file_date=date(2024, 1, 1)
        )

        result = collector_hourly.validate_content(content, candidate)
        assert result is False

    def test_validate_invalid_json(self, collector_hourly):
        """Test validation with invalid JSON."""
        content = b"not valid json"

        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="test",
            metadata={"date": "2024-01-01", "time_resolution": "hourly"},
            collection_params={},
            file_date=date(2024, 1, 1)
        )

        result = collector_hourly.validate_content(content, candidate)
        assert result is False

    def test_validate_non_numeric_mcp(self, collector_hourly):
        """Test validation with non-numeric MCP value."""
        invalid_data = {
            "data": [{
                "interval": "1",
                "timeInterval": {
                    "resolution": "hourly",
                    "start": "2024-01-01T00:00:00Z",
                    "end": "2024-01-01T01:00:00Z",
                    "value": "2024-01-01"
                },
                "product": "Regulation",
                "zone": "Zone 1",
                "mcp": "not a number"
            }],
            "total_records": 1,
            "time_resolution": "hourly"
        }
        content = json.dumps(invalid_data).encode('utf-8')

        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="test",
            metadata={"date": "2024-01-01", "time_resolution": "hourly"},
            collection_params={},
            file_date=date(2024, 1, 1)
        )

        result = collector_hourly.validate_content(content, candidate)
        assert result is False

    def test_validate_hourly_interval_out_of_range(self, collector_hourly):
        """Test validation with hourly interval out of range."""
        invalid_data = {
            "data": [{
                "interval": "25",  # Invalid: should be 1-24
                "timeInterval": {
                    "resolution": "hourly",
                    "start": "2024-01-01T00:00:00Z",
                    "end": "2024-01-01T01:00:00Z",
                    "value": "2024-01-01"
                },
                "product": "Regulation",
                "zone": "Zone 1",
                "mcp": 5.0
            }],
            "total_records": 1,
            "time_resolution": "hourly"
        }
        content = json.dumps(invalid_data).encode('utf-8')

        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="test",
            metadata={"date": "2024-01-01", "time_resolution": "hourly"},
            collection_params={},
            file_date=date(2024, 1, 1)
        )

        result = collector_hourly.validate_content(content, candidate)
        assert result is False

    def test_validate_5min_interval_invalid_minute(self, collector_5min):
        """Test validation with 5-minute interval with invalid minute."""
        invalid_data = {
            "data": [{
                "interval": "00:07",  # Invalid: should be 00, 05, 10, 15, etc.
                "timeInterval": {
                    "resolution": "5min",
                    "start": "2024-01-01T00:00:00Z",
                    "end": "2024-01-01T00:05:00Z",
                    "value": "2024-01-01"
                },
                "product": "Regulation",
                "zone": "Zone 1",
                "mcp": 5.0
            }],
            "total_records": 1,
            "time_resolution": "5min"
        }
        content = json.dumps(invalid_data).encode('utf-8')

        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="test",
            metadata={"date": "2024-01-01", "time_resolution": "5min"},
            collection_params={},
            file_date=date(2024, 1, 1)
        )

        result = collector_5min.validate_content(content, candidate)
        assert result is False

    def test_validate_date_mismatch(self, collector_hourly):
        """Test validation with date mismatch."""
        invalid_data = {
            "data": [{
                "interval": "1",
                "timeInterval": {
                    "resolution": "hourly",
                    "start": "2024-01-02T00:00:00Z",
                    "end": "2024-01-02T01:00:00Z",
                    "value": "2024-01-02"  # Different from expected date
                },
                "product": "Regulation",
                "zone": "Zone 1",
                "mcp": 5.0
            }],
            "total_records": 1,
            "time_resolution": "hourly"
        }
        content = json.dumps(invalid_data).encode('utf-8')

        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="test",
            metadata={"date": "2024-01-01", "time_resolution": "hourly"},
            collection_params={},
            file_date=date(2024, 1, 1)
        )

        result = collector_hourly.validate_content(content, candidate)
        assert result is False
