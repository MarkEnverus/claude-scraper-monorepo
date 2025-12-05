"""Tests for MISO Public ASM MCP Scraper."""

import json
from datetime import date, datetime, UTC
from unittest.mock import Mock, patch, MagicMock

import pytest
import requests

from sourcing.infrastructure.collection_framework import DownloadCandidate, ScrapingError
from sourcing.scraping.miso.public_asm_mcp.scraper_miso_public_asm_mcp import (
    MisoPublicASMMCPCollector,
)


@pytest.fixture
def mock_redis():
    """Mock Redis client."""
    redis_mock = Mock()
    redis_mock.ping.return_value = True
    return redis_mock


@pytest.fixture
def collector(mock_redis):
    """Create collector instance with mocked dependencies."""
    return MisoPublicASMMCPCollector(
        dgroup="miso_public_asm_mcp",
        s3_bucket="test-bucket",
        s3_prefix="sourcing",
        redis_client=mock_redis,
        environment="dev",
    )


@pytest.fixture
def collector_with_filters(mock_redis):
    """Create collector instance with query filters."""
    return MisoPublicASMMCPCollector(
        product="Regulation",
        zone="Zone 1",
        dgroup="miso_public_asm_mcp",
        s3_bucket="test-bucket",
        s3_prefix="sourcing",
        redis_client=mock_redis,
        environment="dev",
    )


@pytest.fixture
def sample_api_response():
    """Sample API response matching MISO format."""
    return {
        "data": [
            {
                "datetime": "2025-12-05T10:30:00Z",
                "product": "Regulation",
                "zone": "Zone 1",
                "mcp": 5.23,
                "marketType": "real-time"
            },
            {
                "datetime": "2025-12-05T10:30:00Z",
                "product": "Regulation",
                "zone": "Zone 2",
                "mcp": 4.87,
                "marketType": "real-time"
            },
            {
                "datetime": "2025-12-05T10:30:00Z",
                "product": "Spin",
                "zone": "Zone 1",
                "mcp": 3.12,
                "marketType": "real-time"
            },
            {
                "datetime": "2025-12-05T11:00:00Z",
                "product": "Regulation",
                "zone": "Zone 1",
                "mcp": 6.45,
                "marketType": "day-ahead"
            }
        ],
        "metadata": {
            "totalRecords": 4,
            "updateTimestamp": "2025-12-05T10:35:00Z",
            "dataSource": "MISO Public Market Pricing"
        }
    }


@pytest.fixture
def sample_filtered_response():
    """Sample API response with single product/zone."""
    return {
        "data": [
            {
                "datetime": "2025-12-05T10:30:00Z",
                "product": "Regulation",
                "zone": "Zone 1",
                "mcp": 5.23,
                "marketType": "real-time"
            }
        ],
        "metadata": {
            "totalRecords": 1,
            "updateTimestamp": "2025-12-05T10:35:00Z",
            "dataSource": "MISO Public Market Pricing"
        }
    }


class TestMisoPublicASMMCPCollector:
    """Test suite for MisoPublicASMMCPCollector."""

    def test_initialization(self, mock_redis):
        """Test collector initialization."""
        collector = MisoPublicASMMCPCollector(
            dgroup="miso_public_asm_mcp",
            s3_bucket="test-bucket",
            s3_prefix="sourcing",
            redis_client=mock_redis,
            environment="dev",
        )

        assert collector.dgroup == "miso_public_asm_mcp"
        assert collector.s3_bucket == "test-bucket"
        assert collector.s3_prefix == "sourcing"
        assert collector.environment == "dev"
        assert collector.product is None
        assert collector.zone is None
        assert collector.specific_datetime is None

    def test_initialization_with_filters(self, mock_redis):
        """Test collector initialization with filters."""
        collector = MisoPublicASMMCPCollector(
            product="Regulation",
            zone="Zone 3",
            specific_datetime="2025-12-05T10:30:00Z",
            dgroup="miso_public_asm_mcp",
            s3_bucket="test-bucket",
            s3_prefix="sourcing",
            redis_client=mock_redis,
            environment="dev",
        )

        assert collector.product == "Regulation"
        assert collector.zone == "Zone 3"
        assert collector.specific_datetime == "2025-12-05T10:30:00Z"

    def test_initialization_invalid_product(self, mock_redis):
        """Test initialization fails with invalid product."""
        with pytest.raises(ValueError, match="Invalid product"):
            MisoPublicASMMCPCollector(
                product="InvalidProduct",
                dgroup="miso_public_asm_mcp",
                s3_bucket="test-bucket",
                s3_prefix="sourcing",
                redis_client=mock_redis,
                environment="dev",
            )

    def test_initialization_invalid_zone(self, mock_redis):
        """Test initialization fails with invalid zone."""
        with pytest.raises(ValueError, match="Invalid zone"):
            MisoPublicASMMCPCollector(
                zone="Zone 99",
                dgroup="miso_public_asm_mcp",
                s3_bucket="test-bucket",
                s3_prefix="sourcing",
                redis_client=mock_redis,
                environment="dev",
            )

    def test_valid_products(self):
        """Test VALID_PRODUCTS constant."""
        expected_products = {"Regulation", "Spin", "Supplemental", "STR", "Ramp-up", "Ramp-down"}
        assert MisoPublicASMMCPCollector.VALID_PRODUCTS == expected_products

    def test_valid_zones(self):
        """Test VALID_ZONES constant."""
        expected_zones = {f"Zone {i}" for i in range(1, 9)}
        assert MisoPublicASMMCPCollector.VALID_ZONES == expected_zones

    def test_generate_candidates_no_filters(self, collector):
        """Test candidate generation without filters."""
        candidates = collector.generate_candidates()

        assert len(candidates) == 1
        candidate = candidates[0]

        assert isinstance(candidate, DownloadCandidate)
        assert candidate.identifier.startswith("public_asm_mcp_")
        assert candidate.identifier.endswith(".json")
        assert candidate.source_location == collector.API_URL

        # Check metadata
        assert candidate.metadata["data_type"] == "public_asm_mcp"
        assert candidate.metadata["source"] == "miso"
        assert candidate.metadata["product_filter"] is None
        assert candidate.metadata["zone_filter"] is None

        # Check collection params
        assert "headers" in candidate.collection_params
        assert candidate.collection_params["headers"]["Accept"] == "application/json"
        assert "query_params" in candidate.collection_params
        assert len(candidate.collection_params["query_params"]) == 0

    def test_generate_candidates_with_filters(self, collector_with_filters):
        """Test candidate generation with filters."""
        candidates = collector_with_filters.generate_candidates()

        assert len(candidates) == 1
        candidate = candidates[0]

        # Check identifier includes filter info
        assert "regulation" in candidate.identifier.lower()
        assert "zone1" in candidate.identifier.lower()

        # Check metadata
        assert candidate.metadata["product_filter"] == "Regulation"
        assert candidate.metadata["zone_filter"] == "Zone 1"

        # Check query params
        query_params = candidate.collection_params["query_params"]
        assert query_params["product"] == "Regulation"
        assert query_params["zone"] == "Zone 1"

    def test_generate_candidates_with_datetime_filter(self, mock_redis):
        """Test candidate generation with datetime filter."""
        collector = MisoPublicASMMCPCollector(
            specific_datetime="2025-12-05T10:30:00Z",
            dgroup="miso_public_asm_mcp",
            s3_bucket="test-bucket",
            s3_prefix="sourcing",
            redis_client=mock_redis,
            environment="dev",
        )

        candidates = collector.generate_candidates()
        candidate = candidates[0]

        # Check datetime in identifier
        assert "20251205_1030" in candidate.identifier

        # Check query params
        assert candidate.collection_params["query_params"]["datetime"] == "2025-12-05T10:30:00Z"

    @patch("requests.get")
    def test_collect_content_success(self, mock_get, collector, sample_api_response):
        """Test successful content collection."""
        mock_response = Mock()
        mock_response.content = json.dumps(sample_api_response).encode("utf-8")
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        candidate = collector.generate_candidates()[0]
        content = collector.collect_content(candidate)

        assert content == mock_response.content
        mock_get.assert_called_once()

        # Verify request parameters
        call_args = mock_get.call_args
        assert call_args.args[0] == collector.API_URL
        assert "headers" in call_args.kwargs
        assert "timeout" in call_args.kwargs

    @patch("requests.get")
    def test_collect_content_with_filters(self, mock_get, collector_with_filters, sample_filtered_response):
        """Test content collection with query filters."""
        mock_response = Mock()
        mock_response.content = json.dumps(sample_filtered_response).encode("utf-8")
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        candidate = collector_with_filters.generate_candidates()[0]
        content = collector_with_filters.collect_content(candidate)

        assert content == mock_response.content

        # Verify query parameters were passed
        call_args = mock_get.call_args
        assert call_args.kwargs["params"]["product"] == "Regulation"
        assert call_args.kwargs["params"]["zone"] == "Zone 1"

    @patch("requests.get")
    def test_collect_content_http_error(self, mock_get, collector):
        """Test HTTP error handling."""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response
        mock_get.return_value.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_response)

        candidate = collector.generate_candidates()[0]

        with pytest.raises(ScrapingError, match="Failed to fetch ASM MCP data"):
            collector.collect_content(candidate)

    @patch("requests.get")
    def test_collect_content_bad_request(self, mock_get, collector):
        """Test bad request error handling."""
        mock_response = Mock()
        mock_response.status_code = 400
        mock_get.return_value = mock_response
        mock_get.return_value.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_response)

        candidate = collector.generate_candidates()[0]

        with pytest.raises(ScrapingError, match="Failed to fetch ASM MCP data"):
            collector.collect_content(candidate)

    @patch("requests.get")
    def test_collect_content_timeout(self, mock_get, collector):
        """Test timeout error handling."""
        mock_get.side_effect = requests.exceptions.Timeout()

        candidate = collector.generate_candidates()[0]

        with pytest.raises(ScrapingError, match="Failed to fetch ASM MCP data"):
            collector.collect_content(candidate)

    def test_validate_content_valid(self, collector, sample_api_response):
        """Test validation of valid content."""
        content = json.dumps(sample_api_response).encode("utf-8")
        candidate = collector.generate_candidates()[0]

        assert collector.validate_content(content, candidate) is True

    def test_validate_content_empty_data(self, collector):
        """Test validation with empty data array."""
        response = {
            "data": [],
            "metadata": {
                "totalRecords": 0,
                "updateTimestamp": "2025-12-05T10:35:00Z",
                "dataSource": "MISO Public Market Pricing"
            }
        }
        content = json.dumps(response).encode("utf-8")
        candidate = collector.generate_candidates()[0]

        # Empty data is valid
        assert collector.validate_content(content, candidate) is True

    def test_validate_content_missing_data_field(self, collector):
        """Test validation fails when 'data' field is missing."""
        response = {
            "metadata": {
                "totalRecords": 0,
                "updateTimestamp": "2025-12-05T10:35:00Z"
            }
        }
        content = json.dumps(response).encode("utf-8")
        candidate = collector.generate_candidates()[0]

        assert collector.validate_content(content, candidate) is False

    def test_validate_content_missing_metadata_field(self, collector):
        """Test validation fails when 'metadata' field is missing."""
        response = {
            "data": []
        }
        content = json.dumps(response).encode("utf-8")
        candidate = collector.generate_candidates()[0]

        assert collector.validate_content(content, candidate) is False

    def test_validate_content_invalid_product(self, collector):
        """Test validation fails with invalid product."""
        response = {
            "data": [
                {
                    "datetime": "2025-12-05T10:30:00Z",
                    "product": "InvalidProduct",
                    "zone": "Zone 1",
                    "mcp": 5.23,
                    "marketType": "real-time"
                }
            ],
            "metadata": {"totalRecords": 1}
        }
        content = json.dumps(response).encode("utf-8")
        candidate = collector.generate_candidates()[0]

        assert collector.validate_content(content, candidate) is False

    def test_validate_content_invalid_zone(self, collector):
        """Test validation fails with invalid zone."""
        response = {
            "data": [
                {
                    "datetime": "2025-12-05T10:30:00Z",
                    "product": "Regulation",
                    "zone": "Zone 99",
                    "mcp": 5.23,
                    "marketType": "real-time"
                }
            ],
            "metadata": {"totalRecords": 1}
        }
        content = json.dumps(response).encode("utf-8")
        candidate = collector.generate_candidates()[0]

        assert collector.validate_content(content, candidate) is False

    def test_validate_content_negative_mcp(self, collector):
        """Test validation fails with negative MCP."""
        response = {
            "data": [
                {
                    "datetime": "2025-12-05T10:30:00Z",
                    "product": "Regulation",
                    "zone": "Zone 1",
                    "mcp": -5.23,
                    "marketType": "real-time"
                }
            ],
            "metadata": {"totalRecords": 1}
        }
        content = json.dumps(response).encode("utf-8")
        candidate = collector.generate_candidates()[0]

        assert collector.validate_content(content, candidate) is False

    def test_validate_content_invalid_market_type(self, collector):
        """Test validation fails with invalid marketType."""
        response = {
            "data": [
                {
                    "datetime": "2025-12-05T10:30:00Z",
                    "product": "Regulation",
                    "zone": "Zone 1",
                    "mcp": 5.23,
                    "marketType": "invalid-type"
                }
            ],
            "metadata": {"totalRecords": 1}
        }
        content = json.dumps(response).encode("utf-8")
        candidate = collector.generate_candidates()[0]

        assert collector.validate_content(content, candidate) is False

    def test_validate_content_invalid_datetime_format(self, collector):
        """Test validation fails with invalid datetime format."""
        response = {
            "data": [
                {
                    "datetime": "not-a-datetime",
                    "product": "Regulation",
                    "zone": "Zone 1",
                    "mcp": 5.23,
                    "marketType": "real-time"
                }
            ],
            "metadata": {"totalRecords": 1}
        }
        content = json.dumps(response).encode("utf-8")
        candidate = collector.generate_candidates()[0]

        assert collector.validate_content(content, candidate) is False

    def test_validate_content_record_count_mismatch(self, collector):
        """Test validation fails when totalRecords doesn't match data length."""
        response = {
            "data": [
                {
                    "datetime": "2025-12-05T10:30:00Z",
                    "product": "Regulation",
                    "zone": "Zone 1",
                    "mcp": 5.23,
                    "marketType": "real-time"
                }
            ],
            "metadata": {
                "totalRecords": 999,  # Mismatch!
                "updateTimestamp": "2025-12-05T10:35:00Z"
            }
        }
        content = json.dumps(response).encode("utf-8")
        candidate = collector.generate_candidates()[0]

        assert collector.validate_content(content, candidate) is False

    def test_validate_content_missing_required_fields(self, collector):
        """Test validation fails when required fields are missing."""
        response = {
            "data": [
                {
                    "datetime": "2025-12-05T10:30:00Z",
                    "product": "Regulation",
                    # Missing 'zone' field
                    "mcp": 5.23,
                    "marketType": "real-time"
                }
            ],
            "metadata": {"totalRecords": 1}
        }
        content = json.dumps(response).encode("utf-8")
        candidate = collector.generate_candidates()[0]

        assert collector.validate_content(content, candidate) is False

    def test_validate_content_invalid_json(self, collector):
        """Test validation fails with invalid JSON."""
        content = b"not valid json"
        candidate = collector.generate_candidates()[0]

        assert collector.validate_content(content, candidate) is False

    def test_validate_content_all_products(self, collector):
        """Test validation with all valid products."""
        for product in MisoPublicASMMCPCollector.VALID_PRODUCTS:
            response = {
                "data": [
                    {
                        "datetime": "2025-12-05T10:30:00Z",
                        "product": product,
                        "zone": "Zone 1",
                        "mcp": 5.23,
                        "marketType": "real-time"
                    }
                ],
                "metadata": {"totalRecords": 1}
            }
            content = json.dumps(response).encode("utf-8")
            candidate = collector.generate_candidates()[0]

            assert collector.validate_content(content, candidate) is True

    def test_validate_content_all_zones(self, collector):
        """Test validation with all valid zones."""
        for zone in MisoPublicASMMCPCollector.VALID_ZONES:
            response = {
                "data": [
                    {
                        "datetime": "2025-12-05T10:30:00Z",
                        "product": "Regulation",
                        "zone": zone,
                        "mcp": 5.23,
                        "marketType": "real-time"
                    }
                ],
                "metadata": {"totalRecords": 1}
            }
            content = json.dumps(response).encode("utf-8")
            candidate = collector.generate_candidates()[0]

            assert collector.validate_content(content, candidate) is True

    def test_validate_content_both_market_types(self, collector):
        """Test validation with both market types."""
        for market_type in ["real-time", "day-ahead"]:
            response = {
                "data": [
                    {
                        "datetime": "2025-12-05T10:30:00Z",
                        "product": "Regulation",
                        "zone": "Zone 1",
                        "mcp": 5.23,
                        "marketType": market_type
                    }
                ],
                "metadata": {"totalRecords": 1}
            }
            content = json.dumps(response).encode("utf-8")
            candidate = collector.generate_candidates()[0]

            assert collector.validate_content(content, candidate) is True

    def test_validate_content_zero_mcp(self, collector):
        """Test validation allows zero MCP."""
        response = {
            "data": [
                {
                    "datetime": "2025-12-05T10:30:00Z",
                    "product": "Regulation",
                    "zone": "Zone 1",
                    "mcp": 0.0,
                    "marketType": "real-time"
                }
            ],
            "metadata": {"totalRecords": 1}
        }
        content = json.dumps(response).encode("utf-8")
        candidate = collector.generate_candidates()[0]

        assert collector.validate_content(content, candidate) is True
