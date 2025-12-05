"""Tests for MISO CSAT Next-Day Short-Term Reserve Requirement Scraper."""

import json
from datetime import datetime, date
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
import requests

from sourcing.scraping.miso.csat_nextday_str.scraper_miso_csat_nextday_str import (
    MisoCsatNextDaySTRCollector,
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
    collector = MisoCsatNextDaySTRCollector(
        start_date=datetime(2025, 1, 1),
        end_date=datetime(2025, 1, 2),
        dgroup="miso_csat_nextday_str",
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
        assert candidate.identifier == "csat_nextday_str_20250115.json"
        assert candidate.source_location == "https://public-api.misoenergy.org/api/CsatNextDayShortTermReserveRequirement"
        assert candidate.metadata["data_type"] == "csat_nextday_str"
        assert candidate.metadata["source"] == "miso"
        assert candidate.metadata["date"] == "2025-01-15"
        assert candidate.file_date == date(2025, 1, 15)

    def test_multi_date_candidates(self, collector):
        """Test generation of multiple date candidates."""
        candidates = collector.generate_candidates()

        assert len(candidates) == 2
        assert candidates[0].identifier == "csat_nextday_str_20250101.json"
        assert candidates[1].identifier == "csat_nextday_str_20250102.json"

    def test_candidate_query_params(self, collector):
        """Test that candidate includes correct query parameters."""
        candidates = collector.generate_candidates()
        candidate = candidates[0]

        query_params = candidate.collection_params["query_params"]
        assert query_params["date"] == "2025-01-01"

    def test_candidate_headers(self, collector):
        """Test that candidates include proper headers."""
        candidates = collector.generate_candidates()
        candidate = candidates[0]

        headers = candidate.collection_params["headers"]
        assert headers["Accept"] == "application/json"
        assert "User-Agent" in headers

    def test_date_range_spanning_month(self, collector):
        """Test candidate generation across month boundary."""
        collector.start_date = datetime(2025, 1, 30)
        collector.end_date = datetime(2025, 2, 2)

        candidates = collector.generate_candidates()

        assert len(candidates) == 4
        assert candidates[0].metadata["date"] == "2025-01-30"
        assert candidates[-1].metadata["date"] == "2025-02-02"


class TestDataCollection:
    """Tests for data collection logic."""

    def test_collect_success(self, collector, sample_api_response):
        """Test successful data collection."""
        candidate = DownloadCandidate(
            identifier="csat_nextday_str_20250101.json",
            source_location="https://public-api.misoenergy.org/api/CsatNextDayShortTermReserveRequirement",
            metadata={"date": "2025-01-01"},
            collection_params={
                "headers": {"Accept": "application/json"},
                "query_params": {"date": "2025-01-01"},
                "timeout": 30,
            },
            file_date=date(2025, 1, 1),
        )

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = json.dumps(sample_api_response).encode('utf-8')

        with patch('requests.get', return_value=mock_response):
            content = collector.collect_content(candidate)

        assert content is not None
        assert len(content) > 0
        data = json.loads(content)
        assert data["operatingDay"] == "2025-01-01"

    def test_collect_404_error(self, collector):
        """Test handling of 404 error (no data available)."""
        candidate = DownloadCandidate(
            identifier="csat_nextday_str_20250101.json",
            source_location="https://public-api.misoenergy.org/api/CsatNextDayShortTermReserveRequirement",
            metadata={"date": "2025-01-01"},
            collection_params={
                "headers": {"Accept": "application/json"},
                "query_params": {"date": "2025-01-01"},
                "timeout": 30,
            },
            file_date=date(2025, 1, 1),
        )

        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_response)

        with patch('requests.get', return_value=mock_response):
            with pytest.raises(ScrapingError, match="No data available"):
                collector.collect_content(candidate)

    def test_collect_network_error(self, collector):
        """Test handling of network errors."""
        candidate = DownloadCandidate(
            identifier="csat_nextday_str_20250101.json",
            source_location="https://public-api.misoenergy.org/api/CsatNextDayShortTermReserveRequirement",
            metadata={"date": "2025-01-01"},
            collection_params={
                "headers": {"Accept": "application/json"},
                "query_params": {"date": "2025-01-01"},
                "timeout": 30,
            },
            file_date=date(2025, 1, 1),
        )

        with patch('requests.get', side_effect=requests.exceptions.ConnectionError("Network error")):
            with pytest.raises(ScrapingError, match="Failed to fetch"):
                collector.collect_content(candidate)


class TestContentValidation:
    """Tests for content validation logic."""

    def test_validate_valid_content(self, collector, sample_api_response):
        """Test validation of valid content."""
        candidate = DownloadCandidate(
            identifier="csat_nextday_str_20250101.json",
            source_location="https://public-api.misoenergy.org/api/CsatNextDayShortTermReserveRequirement",
            metadata={"date": "2025-01-01"},
            collection_params={},
            file_date=date(2025, 1, 1),
        )

        content = json.dumps(sample_api_response).encode('utf-8')
        assert collector.validate_content(content, candidate) is True

    def test_validate_missing_required_field(self, collector):
        """Test validation fails when required field is missing."""
        invalid_data = {
            "operatingDay": "2025-01-01",
            "publishedTimestamp": "2025-01-01T14:00:00Z",
            # Missing totalShortTermReserveRequirement
            "reserveComponents": [],
            "subregions": [],
            "qualitativeAssessment": {}
        }

        candidate = DownloadCandidate(
            identifier="csat_nextday_str_20250101.json",
            source_location="https://public-api.misoenergy.org/api/CsatNextDayShortTermReserveRequirement",
            metadata={"date": "2025-01-01"},
            collection_params={},
            file_date=date(2025, 1, 1),
        )

        content = json.dumps(invalid_data).encode('utf-8')
        assert collector.validate_content(content, candidate) is False

    def test_validate_date_mismatch(self, collector, sample_api_response):
        """Test validation fails when operatingDay doesn't match requested date."""
        candidate = DownloadCandidate(
            identifier="csat_nextday_str_20250101.json",
            source_location="https://public-api.misoenergy.org/api/CsatNextDayShortTermReserveRequirement",
            metadata={"date": "2025-01-05"},  # Different date
            collection_params={},
            file_date=date(2025, 1, 5),
        )

        content = json.dumps(sample_api_response).encode('utf-8')
        assert collector.validate_content(content, candidate) is False

    def test_validate_negative_total(self, collector, sample_api_response):
        """Test validation fails with negative total reserve requirement."""
        sample_api_response["totalShortTermReserveRequirement"] = -100

        candidate = DownloadCandidate(
            identifier="csat_nextday_str_20250101.json",
            source_location="https://public-api.misoenergy.org/api/CsatNextDayShortTermReserveRequirement",
            metadata={"date": "2025-01-01"},
            collection_params={},
            file_date=date(2025, 1, 1),
        )

        content = json.dumps(sample_api_response).encode('utf-8')
        assert collector.validate_content(content, candidate) is False

    def test_validate_components_sum_mismatch(self, collector, sample_api_response):
        """Test validation fails when reserve components don't sum to total."""
        # Modify component values so they don't sum to total
        sample_api_response["reserveComponents"][0]["value"] = 1000.0
        sample_api_response["reserveComponents"][1]["value"] = 1000.0
        # Total is still 2500.5, but components now sum to 2000

        candidate = DownloadCandidate(
            identifier="csat_nextday_str_20250101.json",
            source_location="https://public-api.misoenergy.org/api/CsatNextDayShortTermReserveRequirement",
            metadata={"date": "2025-01-01"},
            collection_params={},
            file_date=date(2025, 1, 1),
        )

        content = json.dumps(sample_api_response).encode('utf-8')
        assert collector.validate_content(content, candidate) is False

    def test_validate_subregions_sum_mismatch(self, collector, sample_api_response):
        """Test validation fails when subregions don't sum to total."""
        # Modify subregion values so they don't sum to total
        sample_api_response["subregions"][0]["shortTermReserveRequirement"] = 500.0
        sample_api_response["subregions"][1]["shortTermReserveRequirement"] = 500.0
        # Total is still 2500.5, but subregions now sum to 1000

        candidate = DownloadCandidate(
            identifier="csat_nextday_str_20250101.json",
            source_location="https://public-api.misoenergy.org/api/CsatNextDayShortTermReserveRequirement",
            metadata={"date": "2025-01-01"},
            collection_params={},
            file_date=date(2025, 1, 1),
        )

        content = json.dumps(sample_api_response).encode('utf-8')
        assert collector.validate_content(content, candidate) is False

    def test_validate_uncertainty_factor_out_of_range(self, collector, sample_api_response):
        """Test validation fails when uncertaintyFactor is outside 0.0-1.0 range."""
        sample_api_response["qualitativeAssessment"]["uncertaintyFactor"] = 1.5

        candidate = DownloadCandidate(
            identifier="csat_nextday_str_20250101.json",
            source_location="https://public-api.misoenergy.org/api/CsatNextDayShortTermReserveRequirement",
            metadata={"date": "2025-01-01"},
            collection_params={},
            file_date=date(2025, 1, 1),
        )

        content = json.dumps(sample_api_response).encode('utf-8')
        assert collector.validate_content(content, candidate) is False

    def test_validate_negative_component_value(self, collector, sample_api_response):
        """Test validation fails with negative reserve component value."""
        sample_api_response["reserveComponents"][0]["value"] = -100.0

        candidate = DownloadCandidate(
            identifier="csat_nextday_str_20250101.json",
            source_location="https://public-api.misoenergy.org/api/CsatNextDayShortTermReserveRequirement",
            metadata={"date": "2025-01-01"},
            collection_params={},
            file_date=date(2025, 1, 1),
        )

        content = json.dumps(sample_api_response).encode('utf-8')
        assert collector.validate_content(content, candidate) is False

    def test_validate_negative_subregion_deficit(self, collector, sample_api_response):
        """Test validation fails with negative reserve deficit."""
        sample_api_response["subregions"][0]["reserveDeficit"] = -50.0

        candidate = DownloadCandidate(
            identifier="csat_nextday_str_20250101.json",
            source_location="https://public-api.misoenergy.org/api/CsatNextDayShortTermReserveRequirement",
            metadata={"date": "2025-01-01"},
            collection_params={},
            file_date=date(2025, 1, 1),
        )

        content = json.dumps(sample_api_response).encode('utf-8')
        assert collector.validate_content(content, candidate) is False

    def test_validate_invalid_json(self, collector):
        """Test validation fails with invalid JSON."""
        candidate = DownloadCandidate(
            identifier="csat_nextday_str_20250101.json",
            source_location="https://public-api.misoenergy.org/api/CsatNextDayShortTermReserveRequirement",
            metadata={"date": "2025-01-01"},
            collection_params={},
            file_date=date(2025, 1, 1),
        )

        content = b"Not valid JSON"
        assert collector.validate_content(content, candidate) is False

    def test_validate_wrong_unit(self, collector, sample_api_response):
        """Test validation fails when reserve component unit is not MW."""
        sample_api_response["reserveComponents"][0]["unit"] = "kW"

        candidate = DownloadCandidate(
            identifier="csat_nextday_str_20250101.json",
            source_location="https://public-api.misoenergy.org/api/CsatNextDayShortTermReserveRequirement",
            metadata={"date": "2025-01-01"},
            collection_params={},
            file_date=date(2025, 1, 1),
        )

        content = json.dumps(sample_api_response).encode('utf-8')
        assert collector.validate_content(content, candidate) is False

    def test_validate_recommended_actions_not_list(self, collector, sample_api_response):
        """Test validation fails when recommendedActions is not a list."""
        sample_api_response["qualitativeAssessment"]["recommendedActions"] = "Not a list"

        candidate = DownloadCandidate(
            identifier="csat_nextday_str_20250101.json",
            source_location="https://public-api.misoenergy.org/api/CsatNextDayShortTermReserveRequirement",
            metadata={"date": "2025-01-01"},
            collection_params={},
            file_date=date(2025, 1, 1),
        )

        content = json.dumps(sample_api_response).encode('utf-8')
        assert collector.validate_content(content, candidate) is False


class TestArithmeticValidation:
    """Tests specifically for arithmetic validation logic."""

    def test_arithmetic_tolerance(self, collector):
        """Test that arithmetic validation allows small floating point differences."""
        data = {
            "operatingDay": "2025-01-01",
            "publishedTimestamp": "2025-01-01T14:00:00Z",
            "totalShortTermReserveRequirement": 2500.5,
            "reserveComponents": [
                {"type": "RegulationReserve", "value": 800.2, "unit": "MW"},
                {"type": "ContingencyReserve", "value": 1700.3, "unit": "MW"}
                # Sum is 2500.5, exact match
            ],
            "subregions": [
                {"name": "North", "shortTermReserveRequirement": 1250.25, "reserveDeficit": 0.0},
                {"name": "South", "shortTermReserveRequirement": 1250.25, "reserveDeficit": 0.0}
                # Sum is 2500.5, exact match
            ],
            "qualitativeAssessment": {
                "overallAdequacy": "Adequate",
                "uncertaintyFactor": 0.15,
                "recommendedActions": []
            }
        }

        candidate = DownloadCandidate(
            identifier="csat_nextday_str_20250101.json",
            source_location="https://public-api.misoenergy.org/api/CsatNextDayShortTermReserveRequirement",
            metadata={"date": "2025-01-01"},
            collection_params={},
            file_date=date(2025, 1, 1),
        )

        content = json.dumps(data).encode('utf-8')
        assert collector.validate_content(content, candidate) is True

    def test_boundary_uncertainty_factor(self, collector):
        """Test validation of boundary values for uncertaintyFactor."""
        data = {
            "operatingDay": "2025-01-01",
            "publishedTimestamp": "2025-01-01T14:00:00Z",
            "totalShortTermReserveRequirement": 2500.0,
            "reserveComponents": [
                {"type": "RegulationReserve", "value": 2500.0, "unit": "MW"}
            ],
            "subregions": [
                {"name": "North", "shortTermReserveRequirement": 2500.0, "reserveDeficit": 0.0}
            ],
            "qualitativeAssessment": {
                "overallAdequacy": "Adequate",
                "uncertaintyFactor": 0.0,  # Minimum boundary
                "recommendedActions": []
            }
        }

        candidate = DownloadCandidate(
            identifier="csat_nextday_str_20250101.json",
            source_location="https://public-api.misoenergy.org/api/CsatNextDayShortTermReserveRequirement",
            metadata={"date": "2025-01-01"},
            collection_params={},
            file_date=date(2025, 1, 1),
        )

        content = json.dumps(data).encode('utf-8')
        assert collector.validate_content(content, candidate) is True

        # Test maximum boundary
        data["qualitativeAssessment"]["uncertaintyFactor"] = 1.0
        content = json.dumps(data).encode('utf-8')
        assert collector.validate_content(content, candidate) is True


class TestIntegration:
    """Integration tests for the full collection workflow."""

    def test_full_collection_workflow(self, collector, sample_api_response, mock_s3):
        """Test the complete collection workflow from candidate generation to storage."""
        # Mock the HTTP request
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = json.dumps(sample_api_response).encode('utf-8')

        with patch('requests.get', return_value=mock_response):
            # Generate candidates
            candidates = collector.generate_candidates()
            assert len(candidates) == 2

            # Collect content for first candidate
            content = collector.collect_content(candidates[0])
            assert content is not None

            # Validate content
            is_valid = collector.validate_content(content, candidates[0])
            assert is_valid is True
