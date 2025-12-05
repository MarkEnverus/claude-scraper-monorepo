"""Tests for MISO Real-Time Ex-Post ASM MCP scraper."""

import json
import copy
import pytest
from datetime import datetime, date
from unittest.mock import Mock, patch

from sourcing.scraping.miso.rt_expost_asm_mcp.scraper_miso_rt_expost_asm_mcp import (
    MisoRealTimeExPostASMMCPCollector
)
from sourcing.infrastructure.collection_framework import DownloadCandidate


@pytest.fixture
def sample_api_response():
    """Sample MISO RT Ex-Post ASM MCP API response."""
    return {
        "data": [
            {
                "preliminaryFinal": "Preliminary",
                "product": "Regulation",
                "zone": "Zone 1",
                "mcp": 6.48,
                "timeInterval": {
                    "resolution": "hourly",
                    "start": "2024-01-01 00:00:00.000",
                    "end": "2024-01-01 01:00:00.000",
                    "value": "2024-01-01"
                }
            },
            {
                "preliminaryFinal": "Final",
                "product": "Spin",
                "zone": "Zone 2",
                "mcp": 4.23,
                "timeInterval": {
                    "resolution": "hourly",
                    "start": "2024-01-01 00:00:00.000",
                    "end": "2024-01-01 01:00:00.000",
                    "value": "2024-01-01"
                }
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
    return MisoRealTimeExPostASMMCPCollector(
        api_key="test_api_key",
        start_date=datetime(2024, 1, 1),
        end_date=datetime(2024, 1, 1),  # Same day for single day test
        dgroup="miso_rt_expost_asm_mcp",
        s3_bucket="test-bucket",
        s3_prefix="sourcing",
        redis_client=mock_redis,
        environment="dev"
    )


class TestCollectorInitialization:
    """Tests for collector initialization and parameter validation."""

    def test_initialization_with_valid_parameters(self):
        """Test collector initializes with valid parameters."""
        mock_redis = Mock()
        collector = MisoRealTimeExPostASMMCPCollector(
            api_key="test_key",
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 1, 1),
            product="Regulation",
            zone="Zone 1",
            preliminary_final="Final",
            time_resolution="5min",
            dgroup="test",
            s3_bucket="test-bucket",
            s3_prefix="sourcing",
            redis_client=mock_redis,
            environment="dev"
        )
        assert collector.product == "Regulation"
        assert collector.zone == "Zone 1"
        assert collector.preliminary_final == "Final"
        assert collector.time_resolution == "5min"

    def test_invalid_product_raises_error(self):
        """Test that invalid product raises ValueError."""
        mock_redis = Mock()
        with pytest.raises(ValueError, match="Invalid product"):
            MisoRealTimeExPostASMMCPCollector(
                api_key="test_key",
                start_date=datetime(2024, 1, 1),
                end_date=datetime(2024, 1, 1),
                product="InvalidProduct",
                dgroup="test",
                s3_bucket="test-bucket",
                s3_prefix="sourcing",
                redis_client=mock_redis,
                environment="dev"
            )

    def test_invalid_zone_raises_error(self):
        """Test that invalid zone raises ValueError."""
        mock_redis = Mock()
        with pytest.raises(ValueError, match="Invalid zone"):
            MisoRealTimeExPostASMMCPCollector(
                api_key="test_key",
                start_date=datetime(2024, 1, 1),
                end_date=datetime(2024, 1, 1),
                zone="Zone 99",
                dgroup="test",
                s3_bucket="test-bucket",
                s3_prefix="sourcing",
                redis_client=mock_redis,
                environment="dev"
            )

    def test_invalid_preliminary_final_raises_error(self):
        """Test that invalid preliminaryFinal raises ValueError."""
        mock_redis = Mock()
        with pytest.raises(ValueError, match="Invalid preliminaryFinal"):
            MisoRealTimeExPostASMMCPCollector(
                api_key="test_key",
                start_date=datetime(2024, 1, 1),
                end_date=datetime(2024, 1, 1),
                preliminary_final="Invalid",
                dgroup="test",
                s3_bucket="test-bucket",
                s3_prefix="sourcing",
                redis_client=mock_redis,
                environment="dev"
            )

    def test_invalid_time_resolution_raises_error(self):
        """Test that invalid time resolution raises ValueError."""
        mock_redis = Mock()
        with pytest.raises(ValueError, match="Invalid time resolution"):
            MisoRealTimeExPostASMMCPCollector(
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

    def test_generate_candidates_single_day(self, collector):
        """Test generating candidates for a single day."""
        candidates = collector.generate_candidates()

        assert len(candidates) == 1
        assert candidates[0].identifier == "rt_expost_asm_mcp_20240101_hourly.json"
        assert "2024-01-01/asm-expost" in candidates[0].source_location
        assert candidates[0].file_date == date(2024, 1, 1)

    def test_generate_candidates_multiple_days(self):
        """Test generating candidates for multiple days."""
        mock_redis = Mock()
        collector = MisoRealTimeExPostASMMCPCollector(
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

    def test_generate_candidates_with_filters(self):
        """Test generating candidates with product and zone filters."""
        mock_redis = Mock()
        collector = MisoRealTimeExPostASMMCPCollector(
            api_key="test_key",
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 1, 1),
            product="Regulation",
            zone="Zone 1",
            preliminary_final="Final",
            time_resolution="5min",
            dgroup="test",
            s3_bucket="test-bucket",
            s3_prefix="sourcing",
            redis_client=mock_redis,
            environment="dev"
        )

        candidates = collector.generate_candidates()

        assert len(candidates) == 1
        assert "rt_expost_asm_mcp_20240101_final_regulation_zone1_5min.json" == candidates[0].identifier

        # Check query parameters
        query_params = candidates[0].collection_params["query_params"]
        assert query_params["product"] == "Regulation"
        assert query_params["zone"] == "Zone 1"
        assert query_params["preliminaryFinal"] == "Final"
        assert query_params["timeResolution"] == "5min"

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
        assert metadata["data_type"] == "rt_expost_asm_mcp"
        assert metadata["source"] == "miso"
        assert metadata["date"] == "2024-01-01"
        assert metadata["market_type"] == "real_time_ancillary_services_expost"
        assert metadata["time_resolution"] == "hourly"

    def test_candidate_5min_resolution(self):
        """Test generating candidates with 5-minute resolution."""
        mock_redis = Mock()
        collector = MisoRealTimeExPostASMMCPCollector(
            api_key="test_key",
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 1, 1),
            time_resolution="5min",
            dgroup="test",
            s3_bucket="test-bucket",
            s3_prefix="sourcing",
            redis_client=mock_redis,
            environment="dev"
        )

        candidates = collector.generate_candidates()

        assert candidates[0].identifier == "rt_expost_asm_mcp_20240101_5min.json"
        assert candidates[0].collection_params["query_params"]["timeResolution"] == "5min"
        assert candidates[0].metadata["time_resolution"] == "5min"


class TestCollectContent:
    """Tests for content collection."""

    def test_collect_content_single_page(self, collector, sample_api_response):
        """Test collecting content with single page response."""
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
            metadata={"date": "2024-01-01", "time_resolution": "hourly"},
            collection_params={
                "headers": {"Ocp-Apim-Subscription-Key": "test_key"},
                "query_params": {"pageNumber": 1, "timeResolution": "hourly"},
                "timeout": 30
            },
            file_date=date(2024, 1, 1)
        )

        # Mock two pages - use deep copy to avoid shared dict references
        page1 = copy.deepcopy(sample_api_response)
        page1["page"]["lastPage"] = False
        page1["page"]["pageNumber"] = 1
        page1["page"]["totalPages"] = 2

        page2 = copy.deepcopy(sample_api_response)
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

            content = collector.collect_content(candidate)

            # Verify two requests were made
            assert mock_get.call_count == 2

            # Verify all data was collected
            data = json.loads(content)
            assert len(data["data"]) == 4  # 2 records per page

    def test_collect_content_empty_response(self, collector):
        """Test collecting content when API returns no data (404)."""
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
            from requests.exceptions import HTTPError

            mock_response = Mock()
            mock_response.status_code = 404
            http_error = HTTPError()
            http_error.response = mock_response
            mock_get.return_value.raise_for_status.side_effect = http_error

            # 404 should not raise, just return empty data
            content = collector.collect_content(candidate)
            data = json.loads(content)
            assert data["data"] == []
            assert data["total_records"] == 0


class TestValidateContent:
    """Tests for content validation."""

    def test_validate_valid_content(self, collector):
        """Test validation of valid content."""
        valid_data = {
            "data": [
                {
                    "preliminaryFinal": "Preliminary",
                    "product": "Regulation",
                    "zone": "Zone 1",
                    "mcp": 5.5,
                    "timeInterval": {
                        "resolution": "hourly",
                        "start": "2024-01-01 00:00:00.000",
                        "end": "2024-01-01 01:00:00.000",
                        "value": "2024-01-01"
                    }
                }
            ],
            "total_records": 1
        }
        content = json.dumps(valid_data).encode('utf-8')

        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="test",
            metadata={"date": "2024-01-01", "time_resolution": "hourly"},
            collection_params={},
            file_date=date(2024, 1, 1)
        )

        result = collector.validate_content(content, candidate)
        assert result is True

    def test_validate_5min_resolution(self, collector):
        """Test validation with 5-minute resolution data."""
        valid_data = {
            "data": [
                {
                    "preliminaryFinal": "Final",
                    "product": "Spin",
                    "zone": "Zone 3",
                    "mcp": 8.25,
                    "timeInterval": {
                        "resolution": "5min",
                        "start": "2024-01-01 00:00:00.000",
                        "end": "2024-01-01 00:05:00.000",
                        "value": "2024-01-01"
                    }
                }
            ],
            "total_records": 1
        }
        content = json.dumps(valid_data).encode('utf-8')

        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="test",
            metadata={"date": "2024-01-01", "time_resolution": "5min"},
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
            metadata={"date": "2024-01-01", "time_resolution": "hourly"},
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
            metadata={"date": "2024-01-01", "time_resolution": "hourly"},
            collection_params={},
            file_date=date(2024, 1, 1)
        )

        result = collector.validate_content(content, candidate)
        assert result is False

    def test_validate_missing_required_field(self, collector):
        """Test validation with missing required field."""
        invalid_data = {
            "data": [{
                "preliminaryFinal": "Preliminary",
                "product": "Regulation",
                # Missing 'zone', 'mcp', and 'timeInterval'
            }],
            "total_records": 1
        }
        content = json.dumps(invalid_data).encode('utf-8')

        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="test",
            metadata={"date": "2024-01-01", "time_resolution": "hourly"},
            collection_params={},
            file_date=date(2024, 1, 1)
        )

        result = collector.validate_content(content, candidate)
        assert result is False

    def test_validate_missing_time_interval_field(self, collector):
        """Test validation with missing timeInterval field."""
        invalid_data = {
            "data": [{
                "preliminaryFinal": "Final",
                "product": "STR",
                "zone": "Zone 4",
                "mcp": 12.50,
                "timeInterval": {
                    "resolution": "hourly",
                    # Missing 'start', 'end', 'value'
                }
            }],
            "total_records": 1
        }
        content = json.dumps(invalid_data).encode('utf-8')

        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="test",
            metadata={"date": "2024-01-01", "time_resolution": "hourly"},
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
            metadata={"date": "2024-01-01", "time_resolution": "hourly"},
            collection_params={},
            file_date=date(2024, 1, 1)
        )

        result = collector.validate_content(content, candidate)
        assert result is False

    def test_validate_non_numeric_mcp(self, collector):
        """Test validation with non-numeric MCP value."""
        invalid_data = {
            "data": [{
                "preliminaryFinal": "Preliminary",
                "product": "Ramp-up",
                "zone": "Zone 5",
                "mcp": "not a number",
                "timeInterval": {
                    "resolution": "hourly",
                    "start": "2024-01-01 00:00:00.000",
                    "end": "2024-01-01 01:00:00.000",
                    "value": "2024-01-01"
                }
            }],
            "total_records": 1
        }
        content = json.dumps(invalid_data).encode('utf-8')

        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="test",
            metadata={"date": "2024-01-01", "time_resolution": "hourly"},
            collection_params={},
            file_date=date(2024, 1, 1)
        )

        result = collector.validate_content(content, candidate)
        assert result is False

    def test_validate_date_mismatch(self, collector):
        """Test validation with date mismatch."""
        invalid_data = {
            "data": [{
                "preliminaryFinal": "Final",
                "product": "Supplemental",
                "zone": "Zone 7",
                "mcp": 3.14,
                "timeInterval": {
                    "resolution": "hourly",
                    "start": "2024-01-02 00:00:00.000",
                    "end": "2024-01-02 01:00:00.000",
                    "value": "2024-01-02"  # Doesn't match expected date
                }
            }],
            "total_records": 1
        }
        content = json.dumps(invalid_data).encode('utf-8')

        candidate = DownloadCandidate(
            identifier="test.json",
            source_location="test",
            metadata={"date": "2024-01-01", "time_resolution": "hourly"},
            collection_params={},
            file_date=date(2024, 1, 1)
        )

        result = collector.validate_content(content, candidate)
        assert result is False

    def test_validate_all_products(self, collector):
        """Test validation accepts all valid product types."""
        products = ["Regulation", "Spin", "Supplemental", "STR", "Ramp-up", "Ramp-down"]

        for product in products:
            valid_data = {
                "data": [{
                    "preliminaryFinal": "Preliminary",
                    "product": product,
                    "zone": "Zone 1",
                    "mcp": 10.0,
                    "timeInterval": {
                        "resolution": "hourly",
                        "start": "2024-01-01 00:00:00.000",
                        "end": "2024-01-01 01:00:00.000",
                        "value": "2024-01-01"
                    }
                }],
                "total_records": 1
            }
            content = json.dumps(valid_data).encode('utf-8')

            candidate = DownloadCandidate(
                identifier="test.json",
                source_location="test",
                metadata={"date": "2024-01-01", "time_resolution": "hourly"},
                collection_params={},
                file_date=date(2024, 1, 1)
            )

            result = collector.validate_content(content, candidate)
            assert result is True, f"Validation failed for product: {product}"

    def test_validate_all_zones(self, collector):
        """Test validation accepts all valid zones."""
        zones = [f"Zone {i}" for i in range(1, 9)]

        for zone in zones:
            valid_data = {
                "data": [{
                    "preliminaryFinal": "Final",
                    "product": "Regulation",
                    "zone": zone,
                    "mcp": 7.5,
                    "timeInterval": {
                        "resolution": "hourly",
                        "start": "2024-01-01 00:00:00.000",
                        "end": "2024-01-01 01:00:00.000",
                        "value": "2024-01-01"
                    }
                }],
                "total_records": 1
            }
            content = json.dumps(valid_data).encode('utf-8')

            candidate = DownloadCandidate(
                identifier="test.json",
                source_location="test",
                metadata={"date": "2024-01-01", "time_resolution": "hourly"},
                collection_params={},
                file_date=date(2024, 1, 1)
            )

            result = collector.validate_content(content, candidate)
            assert result is True, f"Validation failed for zone: {zone}"

    def test_validate_both_preliminary_and_final(self, collector):
        """Test validation accepts both preliminary and final states."""
        for state in ["Preliminary", "Final"]:
            valid_data = {
                "data": [{
                    "preliminaryFinal": state,
                    "product": "Ramp-down",
                    "zone": "Zone 8",
                    "mcp": 2.75,
                    "timeInterval": {
                        "resolution": "5min",
                        "start": "2024-01-01 00:00:00.000",
                        "end": "2024-01-01 00:05:00.000",
                        "value": "2024-01-01"
                    }
                }],
                "total_records": 1
            }
            content = json.dumps(valid_data).encode('utf-8')

            candidate = DownloadCandidate(
                identifier="test.json",
                source_location="test",
                metadata={"date": "2024-01-01", "time_resolution": "5min"},
                collection_params={},
                file_date=date(2024, 1, 1)
            )

            result = collector.validate_content(content, candidate)
            assert result is True, f"Validation failed for state: {state}"
