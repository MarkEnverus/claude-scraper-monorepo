"""Tests for MISO LMP Consolidated Table Scraper."""

import json
from datetime import datetime, date, UTC
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
import requests

from sourcing.scraping.miso.lmp_consolidated.scraper_miso_lmp_consolidated import (
    MisoLmpConsolidatedCollector,
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
    collector = MisoLmpConsolidatedCollector(
        dgroup="miso_lmp_consolidated",
        s3_bucket="test-bucket",
        s3_prefix="sourcing",
        redis_client=mock_redis,
        environment="dev",
    )
    collector.s3_client = mock_s3
    return collector


@pytest.fixture
def sample_consolidated_response():
    """Load sample consolidated API response fixture."""
    fixture_path = Path(__file__).parent / "fixtures" / "sample_response.json"
    with open(fixture_path, 'r') as f:
        return json.load(f)


class TestCandidateGeneration:
    """Tests for candidate generation logic."""

    def test_single_candidate_generation(self, collector):
        """Test generation of a single candidate for current timestamp."""
        with patch('sourcing.scraping.miso.lmp_consolidated.scraper_miso_lmp_consolidated.datetime') as mock_dt:
            # Mock current time
            mock_now = datetime(2025, 12, 5, 14, 30, 45, tzinfo=UTC)
            mock_dt.now.return_value = mock_now

            candidates = collector.generate_candidates()

            assert len(candidates) == 1
            candidate = candidates[0]
            assert candidate.identifier == "lmp_consolidated_20251205_143045.json"
            assert candidate.source_location == "https://public-api.misoenergy.org/api/MarketPricing/GetLmpConsolidatedTable"
            assert candidate.metadata["data_type"] == "lmp_consolidated"
            assert candidate.metadata["source"] == "miso"
            assert candidate.metadata["real_time"] is True
            assert candidate.file_date == date(2025, 12, 5)

    def test_candidate_url_format(self, collector):
        """Test that candidate URL follows the correct format."""
        candidates = collector.generate_candidates()
        candidate = candidates[0]

        expected_url = "https://public-api.misoenergy.org/api/MarketPricing/GetLmpConsolidatedTable"
        assert candidate.source_location == expected_url

    def test_candidate_headers(self, collector):
        """Test that candidates include proper headers."""
        candidates = collector.generate_candidates()
        candidate = candidates[0]

        headers = candidate.collection_params["headers"]
        assert headers["Accept"] == "application/json"
        assert "User-Agent" in headers

    def test_candidate_market_types(self, collector):
        """Test that candidate metadata includes all market types."""
        candidates = collector.generate_candidates()
        candidate = candidates[0]

        market_types = candidate.metadata["market_types"]
        assert "five_min_realtime" in market_types
        assert "hourly_integrated" in market_types
        assert "day_ahead_exante" in market_types


class TestDataCollection:
    """Tests for data collection logic."""

    def test_collect_consolidated_data(self, collector, sample_consolidated_response):
        """Test collection of consolidated LMP data."""
        candidate = DownloadCandidate(
            identifier="lmp_consolidated_20251205_143045.json",
            source_location="https://public-api.misoenergy.org/api/MarketPricing/GetLmpConsolidatedTable",
            metadata={
                "data_type": "lmp_consolidated",
                "source": "miso",
                "real_time": True
            },
            collection_params={
                "headers": {"Accept": "application/json"},
                "timeout": 30,
            },
            file_date=date(2025, 12, 5),
        )

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_consolidated_response

        with patch('requests.get', return_value=mock_response):
            content = collector.collect_content(candidate)

        assert content is not None
        data = json.loads(content.decode('utf-8'))
        assert "FiveMinLMP" in data
        assert "HourlyIntegratedLMP" in data
        assert "DayAheadExAnteLMP" in data
        assert "collection_metadata" in data

    def test_collect_empty_response(self, collector):
        """Test handling of empty data response."""
        candidate = DownloadCandidate(
            identifier="lmp_consolidated_20251205_143045.json",
            source_location="https://public-api.misoenergy.org/api/MarketPricing/GetLmpConsolidatedTable",
            metadata={"data_type": "lmp_consolidated"},
            collection_params={
                "headers": {"Accept": "application/json"},
                "timeout": 30,
            },
            file_date=date(2025, 12, 5),
        )

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "FiveMinLMP": [],
            "HourlyIntegratedLMP": [],
            "DayAheadExAnteLMP": []
        }

        with patch('requests.get', return_value=mock_response):
            content = collector.collect_content(candidate)

        assert content is not None
        data = json.loads(content.decode('utf-8'))
        assert len(data["FiveMinLMP"]) == 0
        assert len(data["HourlyIntegratedLMP"]) == 0
        assert len(data["DayAheadExAnteLMP"]) == 0

    def test_collect_http_404_error(self, collector):
        """Test handling of 404 error (no data available)."""
        candidate = DownloadCandidate(
            identifier="lmp_consolidated_20251205_143045.json",
            source_location="https://public-api.misoenergy.org/api/MarketPricing/GetLmpConsolidatedTable",
            metadata={"data_type": "lmp_consolidated"},
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
            content = collector.collect_content(candidate)

        # Should return empty structure for 404
        data = json.loads(content.decode('utf-8'))
        assert data["FiveMinLMP"] == []
        assert data["HourlyIntegratedLMP"] == []
        assert data["DayAheadExAnteLMP"] == []
        assert data["note"] == "No data available"

    def test_collect_http_503_error(self, collector):
        """Test handling of 503 service unavailable error."""
        candidate = DownloadCandidate(
            identifier="lmp_consolidated_20251205_143045.json",
            source_location="https://public-api.misoenergy.org/api/MarketPricing/GetLmpConsolidatedTable",
            metadata={"data_type": "lmp_consolidated"},
            collection_params={
                "headers": {"Accept": "application/json"},
                "timeout": 30,
            },
            file_date=date(2025, 12, 5),
        )

        mock_response = Mock()
        mock_response.status_code = 503
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_response)

        with patch('requests.get', return_value=mock_response):
            with pytest.raises(ScrapingError) as exc_info:
                collector.collect_content(candidate)

            assert "HTTP error" in str(exc_info.value)

    def test_collect_invalid_json(self, collector):
        """Test handling of invalid JSON response."""
        candidate = DownloadCandidate(
            identifier="lmp_consolidated_20251205_143045.json",
            source_location="https://public-api.misoenergy.org/api/MarketPricing/GetLmpConsolidatedTable",
            metadata={"data_type": "lmp_consolidated"},
            collection_params={
                "headers": {"Accept": "application/json"},
                "timeout": 30,
            },
            file_date=date(2025, 12, 5),
        )

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.side_effect = json.JSONDecodeError("Invalid JSON", "", 0)

        with patch('requests.get', return_value=mock_response):
            with pytest.raises(ScrapingError) as exc_info:
                collector.collect_content(candidate)

            assert "Invalid JSON" in str(exc_info.value)


class TestContentValidation:
    """Tests for content validation logic."""

    def test_validate_complete_response(self, collector, sample_consolidated_response):
        """Test validation of complete consolidated response."""
        content = json.dumps(sample_consolidated_response).encode('utf-8')

        candidate = DownloadCandidate(
            identifier="lmp_consolidated_20251205_143045.json",
            source_location="https://public-api.misoenergy.org/api/MarketPricing/GetLmpConsolidatedTable",
            metadata={"data_type": "lmp_consolidated"},
            collection_params={},
            file_date=date(2025, 12, 5),
        )

        is_valid = collector.validate_content(content, candidate)
        assert is_valid is True

    def test_validate_missing_section(self, collector):
        """Test validation fails when required section is missing."""
        incomplete_data = {
            "FiveMinLMP": [],
            "HourlyIntegratedLMP": []
            # Missing DayAheadExAnteLMP
        }
        content = json.dumps(incomplete_data).encode('utf-8')

        candidate = DownloadCandidate(
            identifier="lmp_consolidated_20251205_143045.json",
            source_location="https://public-api.misoenergy.org/api/MarketPricing/GetLmpConsolidatedTable",
            metadata={"data_type": "lmp_consolidated"},
            collection_params={},
            file_date=date(2025, 12, 5),
        )

        is_valid = collector.validate_content(content, candidate)
        assert is_valid is False

    def test_validate_empty_sections(self, collector):
        """Test validation passes when all sections are empty."""
        empty_data = {
            "FiveMinLMP": [],
            "HourlyIntegratedLMP": [],
            "DayAheadExAnteLMP": []
        }
        content = json.dumps(empty_data).encode('utf-8')

        candidate = DownloadCandidate(
            identifier="lmp_consolidated_20251205_143045.json",
            source_location="https://public-api.misoenergy.org/api/MarketPricing/GetLmpConsolidatedTable",
            metadata={"data_type": "lmp_consolidated"},
            collection_params={},
            file_date=date(2025, 12, 5),
        )

        is_valid = collector.validate_content(content, candidate)
        assert is_valid is True

    def test_validate_missing_timestamp(self, collector):
        """Test validation fails when Timestamp is missing."""
        invalid_data = {
            "FiveMinLMP": [
                {
                    # Missing Timestamp
                    "Region": "South",
                    "PricingNodes": [
                        {"Name": "NODE1", "LMP": 25.5, "MLC": 2.3, "MCC": 5.7}
                    ]
                }
            ],
            "HourlyIntegratedLMP": [],
            "DayAheadExAnteLMP": []
        }
        content = json.dumps(invalid_data).encode('utf-8')

        candidate = DownloadCandidate(
            identifier="lmp_consolidated_20251205_143045.json",
            source_location="https://public-api.misoenergy.org/api/MarketPricing/GetLmpConsolidatedTable",
            metadata={"data_type": "lmp_consolidated"},
            collection_params={},
            file_date=date(2025, 12, 5),
        )

        is_valid = collector.validate_content(content, candidate)
        assert is_valid is False

    def test_validate_missing_pricing_nodes(self, collector):
        """Test validation fails when PricingNodes array is missing."""
        invalid_data = {
            "FiveMinLMP": [
                {
                    "Timestamp": "HE 9 INT 15",
                    "Region": "South",
                    # Missing PricingNodes
                }
            ],
            "HourlyIntegratedLMP": [],
            "DayAheadExAnteLMP": []
        }
        content = json.dumps(invalid_data).encode('utf-8')

        candidate = DownloadCandidate(
            identifier="lmp_consolidated_20251205_143045.json",
            source_location="https://public-api.misoenergy.org/api/MarketPricing/GetLmpConsolidatedTable",
            metadata={"data_type": "lmp_consolidated"},
            collection_params={},
            file_date=date(2025, 12, 5),
        )

        is_valid = collector.validate_content(content, candidate)
        assert is_valid is False

    def test_validate_missing_node_field(self, collector):
        """Test validation fails when required node field is missing."""
        invalid_data = {
            "FiveMinLMP": [
                {
                    "Timestamp": "HE 9 INT 15",
                    "Region": "South",
                    "PricingNodes": [
                        {
                            "Name": "NODE1",
                            "LMP": 25.5,
                            # Missing MLC
                            "MCC": 5.7
                        }
                    ]
                }
            ],
            "HourlyIntegratedLMP": [],
            "DayAheadExAnteLMP": []
        }
        content = json.dumps(invalid_data).encode('utf-8')

        candidate = DownloadCandidate(
            identifier="lmp_consolidated_20251205_143045.json",
            source_location="https://public-api.misoenergy.org/api/MarketPricing/GetLmpConsolidatedTable",
            metadata={"data_type": "lmp_consolidated"},
            collection_params={},
            file_date=date(2025, 12, 5),
        )

        is_valid = collector.validate_content(content, candidate)
        assert is_valid is False

    def test_validate_non_numeric_lmp(self, collector):
        """Test validation fails when LMP value is not numeric."""
        invalid_data = {
            "FiveMinLMP": [
                {
                    "Timestamp": "HE 9 INT 15",
                    "Region": "South",
                    "PricingNodes": [
                        {
                            "Name": "NODE1",
                            "LMP": "invalid",  # String instead of number
                            "MLC": 2.3,
                            "MCC": 5.7
                        }
                    ]
                }
            ],
            "HourlyIntegratedLMP": [],
            "DayAheadExAnteLMP": []
        }
        content = json.dumps(invalid_data).encode('utf-8')

        candidate = DownloadCandidate(
            identifier="lmp_consolidated_20251205_143045.json",
            source_location="https://public-api.misoenergy.org/api/MarketPricing/GetLmpConsolidatedTable",
            metadata={"data_type": "lmp_consolidated"},
            collection_params={},
            file_date=date(2025, 12, 5),
        )

        is_valid = collector.validate_content(content, candidate)
        assert is_valid is False

    def test_validate_lmp_arithmetic(self, collector):
        """Test validation of LMP arithmetic with MEC component."""
        valid_data = {
            "FiveMinLMP": [
                {
                    "Timestamp": "HE 9 INT 15",
                    "Region": "South",
                    "PricingNodes": [
                        {
                            "Name": "NODE1",
                            "LMP": 25.5,
                            "MEC": 17.4,
                            "MLC": 2.35,
                            "MCC": 5.75
                            # LMP (25.5) = MEC (17.4) + MLC (2.35) + MCC (5.75) = 25.5 ✓
                        }
                    ]
                }
            ],
            "HourlyIntegratedLMP": [],
            "DayAheadExAnteLMP": []
        }
        content = json.dumps(valid_data).encode('utf-8')

        candidate = DownloadCandidate(
            identifier="lmp_consolidated_20251205_143045.json",
            source_location="https://public-api.misoenergy.org/api/MarketPricing/GetLmpConsolidatedTable",
            metadata={"data_type": "lmp_consolidated"},
            collection_params={},
            file_date=date(2025, 12, 5),
        )

        is_valid = collector.validate_content(content, candidate)
        assert is_valid is True

    def test_validate_invalid_json(self, collector):
        """Test validation fails with invalid JSON."""
        invalid_json = b"not valid json {"

        candidate = DownloadCandidate(
            identifier="lmp_consolidated_20251205_143045.json",
            source_location="https://public-api.misoenergy.org/api/MarketPricing/GetLmpConsolidatedTable",
            metadata={"data_type": "lmp_consolidated"},
            collection_params={},
            file_date=date(2025, 12, 5),
        )

        is_valid = collector.validate_content(invalid_json, candidate)
        assert is_valid is False
