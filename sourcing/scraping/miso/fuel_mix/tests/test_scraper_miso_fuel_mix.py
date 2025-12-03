"""Tests for MISO Fuel Mix Scraper.

Test coverage:
    - Candidate generation
    - Content collection from API
    - Content validation (JSON structure, required fields)
    - S3 upload with gzip compression
    - Hash deduplication
    - Kafka notification publishing
    - Error handling
"""

import json
import gzip
from datetime import datetime, date, UTC
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import pytest
import redis
import requests

from sourcing.scraping.miso.fuel_mix.scraper_miso_fuel_mix import (
    MisoFuelMixCollector,
)
from sourcing.infrastructure.collection_framework import DownloadCandidate, ScrapingError


# Fixtures
@pytest.fixture
def mock_redis():
    """Mock Redis client."""
    mock = Mock(spec=redis.Redis)
    mock.ping.return_value = True
    return mock


@pytest.fixture
def collector(mock_redis):
    """Create collector instance without Kafka."""
    return MisoFuelMixCollector(
        dgroup="miso_fuel_mix",
        s3_bucket="test-bucket",
        s3_prefix="sourcing",
        redis_client=mock_redis,
        environment="dev",
        kafka_connection_string=None,
    )


@pytest.fixture
def collector_with_kafka(mock_redis):
    """Create collector instance with Kafka enabled."""
    return MisoFuelMixCollector(
        dgroup="miso_fuel_mix",
        s3_bucket="test-bucket",
        s3_prefix="sourcing",
        redis_client=mock_redis,
        environment="dev",
        kafka_connection_string="kafka://localhost:9092",
    )


@pytest.fixture
def sample_fuel_mix_data():
    """Load sample fuel mix data from fixtures."""
    fixture_path = Path(__file__).parent / "fixtures" / "sample_fuel_mix.json"
    with open(fixture_path, "r") as f:
        return json.load(f)


@pytest.fixture
def sample_fuel_mix_bytes(sample_fuel_mix_data):
    """Sample fuel mix data as bytes."""
    return json.dumps(sample_fuel_mix_data).encode("utf-8")


# Test: Candidate Generation
class TestCandidateGeneration:
    """Tests for generate_candidates method."""

    def test_generates_single_candidate(self, collector):
        """Should generate one candidate for current fuel mix."""
        candidates = collector.generate_candidates()

        assert len(candidates) == 1
        assert isinstance(candidates[0], DownloadCandidate)

    def test_candidate_has_correct_source_url(self, collector):
        """Should use correct MISO API endpoint."""
        candidates = collector.generate_candidates()
        candidate = candidates[0]

        assert candidate.source_location == "https://public-api.misoenergy.org/api/FuelMix"

    def test_candidate_has_metadata(self, collector):
        """Should include metadata fields."""
        candidates = collector.generate_candidates()
        candidate = candidates[0]

        assert "data_type" in candidate.metadata
        assert candidate.metadata["data_type"] == "fuel_mix"
        assert "source" in candidate.metadata
        assert candidate.metadata["source"] == "miso"
        assert "collection_timestamp" in candidate.metadata

    def test_candidate_has_collection_params(self, collector):
        """Should include HTTP headers and timeout."""
        candidates = collector.generate_candidates()
        candidate = candidates[0]

        assert "headers" in candidate.collection_params
        assert "Accept" in candidate.collection_params["headers"]
        assert "User-Agent" in candidate.collection_params["headers"]
        assert "timeout" in candidate.collection_params

    def test_candidate_identifier_format(self, collector):
        """Should have identifier with timestamp format."""
        candidates = collector.generate_candidates()
        candidate = candidates[0]

        assert candidate.identifier.startswith("fuel_mix_")
        assert candidate.identifier.endswith(".json")

    def test_candidate_file_date_is_today(self, collector):
        """Should set file_date to current date."""
        candidates = collector.generate_candidates()
        candidate = candidates[0]

        assert candidate.file_date == datetime.now(UTC).date()


# Test: Content Collection
class TestContentCollection:
    """Tests for collect_content method."""

    @patch("requests.get")
    def test_successful_collection(self, mock_get, collector, sample_fuel_mix_bytes):
        """Should fetch data from API successfully."""
        mock_response = Mock()
        mock_response.content = sample_fuel_mix_bytes
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        candidate = collector.generate_candidates()[0]
        content = collector.collect_content(candidate)

        assert content == sample_fuel_mix_bytes
        mock_get.assert_called_once()

    @patch("requests.get")
    def test_uses_correct_headers(self, mock_get, collector):
        """Should pass headers from candidate."""
        mock_response = Mock()
        mock_response.content = b'{"RefId": "", "TotalMW": "", "Fuel": {"Type": []}}'
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        candidate = collector.generate_candidates()[0]
        collector.collect_content(candidate)

        call_kwargs = mock_get.call_args[1]
        assert "headers" in call_kwargs
        assert "Accept" in call_kwargs["headers"]

    @patch("requests.get")
    def test_handles_http_error(self, mock_get, collector):
        """Should raise ScrapingError on HTTP failure."""
        mock_get.side_effect = requests.exceptions.RequestException("Connection timeout")

        candidate = collector.generate_candidates()[0]

        with pytest.raises(ScrapingError) as exc_info:
            collector.collect_content(candidate)

        assert "Failed to fetch fuel mix" in str(exc_info.value)

    @patch("requests.get")
    def test_handles_404_error(self, mock_get, collector):
        """Should raise ScrapingError on 404."""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Not Found", response=mock_response)
        mock_get.return_value = mock_response

        candidate = collector.generate_candidates()[0]

        with pytest.raises(ScrapingError):
            collector.collect_content(candidate)


# Test: Content Validation
class TestContentValidation:
    """Tests for validate_content method."""

    def test_valid_fuel_mix_data(self, collector, sample_fuel_mix_bytes):
        """Should validate correct fuel mix structure."""
        candidate = collector.generate_candidates()[0]
        is_valid = collector.validate_content(sample_fuel_mix_bytes, candidate)

        assert is_valid is True

    def test_missing_refid_key(self, collector):
        """Should reject data without 'RefId' key."""
        invalid_data = json.dumps({
            "TotalMW": "89681",
            "Fuel": {"Type": [{"INTERVALEST": "", "CATEGORY": "Coal", "ACT": "1000"}]}
        }).encode()
        candidate = collector.generate_candidates()[0]

        is_valid = collector.validate_content(invalid_data, candidate)

        assert is_valid is False

    def test_missing_totalmw_key(self, collector):
        """Should reject data without 'TotalMW' key."""
        invalid_data = json.dumps({
            "RefId": "03-Dec-2025",
            "Fuel": {"Type": [{"INTERVALEST": "", "CATEGORY": "Coal", "ACT": "1000"}]}
        }).encode()
        candidate = collector.generate_candidates()[0]

        is_valid = collector.validate_content(invalid_data, candidate)

        assert is_valid is False

    def test_missing_fuel_key(self, collector):
        """Should reject data without 'Fuel' key."""
        invalid_data = json.dumps({
            "RefId": "03-Dec-2025",
            "TotalMW": "89681"
        }).encode()
        candidate = collector.generate_candidates()[0]

        is_valid = collector.validate_content(invalid_data, candidate)

        assert is_valid is False

    def test_missing_fuel_type_array(self, collector):
        """Should reject data without 'Fuel.Type' array."""
        invalid_data = json.dumps({
            "RefId": "03-Dec-2025",
            "TotalMW": "89681",
            "Fuel": {}
        }).encode()
        candidate = collector.generate_candidates()[0]

        is_valid = collector.validate_content(invalid_data, candidate)

        assert is_valid is False

    def test_empty_fuel_type_array(self, collector):
        """Should reject empty Fuel.Type array."""
        invalid_data = json.dumps({
            "RefId": "03-Dec-2025",
            "TotalMW": "89681",
            "Fuel": {"Type": []}
        }).encode()
        candidate = collector.generate_candidates()[0]

        is_valid = collector.validate_content(invalid_data, candidate)

        assert is_valid is False

    def test_missing_required_fields_in_type(self, collector):
        """Should reject entries without required fields."""
        invalid_data = json.dumps({
            "RefId": "03-Dec-2025",
            "TotalMW": "89681",
            "Fuel": {
                "Type": [
                    {"INTERVALEST": "12/03/2025 12:55:00", "CATEGORY": "Coal"}  # Missing ACT
                ]
            }
        }).encode()
        candidate = collector.generate_candidates()[0]

        is_valid = collector.validate_content(invalid_data, candidate)

        assert is_valid is False

    def test_invalid_json(self, collector):
        """Should reject malformed JSON."""
        invalid_data = b"not json at all"
        candidate = collector.generate_candidates()[0]

        is_valid = collector.validate_content(invalid_data, candidate)

        assert is_valid is False


# Test: S3 Integration
class TestS3Integration:
    """Tests for S3 upload functionality."""

    def test_s3_path_format(self, collector):
        """Should build correct S3 path with date partitioning."""
        candidate = DownloadCandidate(
            identifier="fuel_mix_20251203_1400.json",
            source_location="https://example.com",
            metadata={},
            collection_params={},
            file_date=date(2025, 12, 3)
        )

        s3_path = collector._build_s3_path(candidate)

        assert "s3://test-bucket/sourcing/miso_fuel_mix/" in s3_path
        assert "year=2025/month=12/day=03/" in s3_path
        assert "fuel_mix_20251203_1400.json.gz" in s3_path

    @patch("boto3.client")
    def test_upload_compresses_content(self, mock_boto_client, collector, sample_fuel_mix_bytes):
        """Should gzip compress content before upload."""
        mock_s3 = Mock()
        mock_s3.put_object.return_value = {"VersionId": "v1", "ETag": "abc123"}
        collector.s3_client = mock_s3

        s3_path = "s3://test-bucket/sourcing/miso_fuel_mix/year=2025/month=12/day=03/test.json.gz"
        version_id, etag = collector._upload_to_s3(sample_fuel_mix_bytes, s3_path)

        # Verify compression happened
        call_kwargs = mock_s3.put_object.call_args[1]
        compressed_body = call_kwargs["Body"]
        decompressed = gzip.decompress(compressed_body)

        assert decompressed == sample_fuel_mix_bytes
        assert len(compressed_body) < len(sample_fuel_mix_bytes)

    @patch("boto3.client")
    def test_upload_returns_metadata(self, mock_boto_client, collector, sample_fuel_mix_bytes):
        """Should return version_id and etag."""
        mock_s3 = Mock()
        mock_s3.put_object.return_value = {"VersionId": "v1", "ETag": '"abc123"'}
        collector.s3_client = mock_s3

        s3_path = "s3://test-bucket/test.json.gz"
        version_id, etag = collector._upload_to_s3(sample_fuel_mix_bytes, s3_path)

        assert version_id == "v1"
        assert etag == "abc123"  # Should strip quotes


# Test: Kafka Integration
class TestKafkaIntegration:
    """Tests for Kafka notification publishing."""

    def test_kafka_disabled_by_default(self, collector, sample_fuel_mix_bytes):
        """Should not publish when Kafka not configured."""
        candidate = collector.generate_candidates()[0]

        # Should not raise error
        collector._publish_kafka_notification(
            candidate,
            "s3://bucket/key",
            "hash123",
            len(sample_fuel_mix_bytes),
            "etag123"
        )

    @patch("sourcing.infrastructure.kafka_utils.KafkaProducer")
    @patch("sourcing.infrastructure.kafka_utils.KafkaConfiguration")
    def test_kafka_notification_sent(self, mock_kafka_config, mock_kafka_producer, collector_with_kafka):
        """Should publish notification when Kafka enabled."""
        mock_producer_instance = MagicMock()
        mock_kafka_producer.return_value.__enter__.return_value = mock_producer_instance

        candidate = collector_with_kafka.generate_candidates()[0]
        collector_with_kafka._publish_kafka_notification(
            candidate,
            "s3://bucket/key",
            "hash123",
            1000,
            "etag123"
        )

        mock_producer_instance.publish.assert_called_once()

    @patch("sourcing.infrastructure.kafka_utils.KafkaProducer")
    @patch("sourcing.infrastructure.kafka_utils.KafkaConfiguration")
    def test_kafka_message_structure(self, mock_kafka_config, mock_kafka_producer, collector_with_kafka):
        """Should send message with correct structure."""
        mock_producer_instance = MagicMock()
        mock_kafka_producer.return_value.__enter__.return_value = mock_producer_instance

        candidate = collector_with_kafka.generate_candidates()[0]
        collector_with_kafka._publish_kafka_notification(
            candidate,
            "s3://bucket/key",
            "hash123",
            1000,
            "etag123"
        )

        # Get the message that was published
        published_message = mock_producer_instance.publish.call_args[0][0]

        assert published_message.dataset == "miso_fuel_mix"
        assert published_message.environment == "dev"
        assert published_message.location == "s3://bucket/key"
        assert published_message.etag == "etag123"

    @patch("sourcing.infrastructure.kafka_utils.KafkaProducer")
    @patch("sourcing.infrastructure.kafka_utils.KafkaConfiguration")
    def test_kafka_error_does_not_fail_collection(self, mock_kafka_config, mock_kafka_producer, collector_with_kafka):
        """Should log error but not raise on Kafka failure."""
        mock_kafka_producer.side_effect = Exception("Kafka unavailable")

        candidate = collector_with_kafka.generate_candidates()[0]

        # Should not raise
        collector_with_kafka._publish_kafka_notification(
            candidate,
            "s3://bucket/key",
            "hash123",
            1000,
            "etag123"
        )


# Test: End-to-End Collection
class TestEndToEndCollection:
    """Integration tests for full collection workflow."""

    @patch("requests.get")
    @patch("boto3.client")
    def test_full_collection_run(self, mock_boto_client, mock_get, collector, sample_fuel_mix_bytes):
        """Should complete full collection successfully."""
        # Mock HTTP response
        mock_response = Mock()
        mock_response.content = sample_fuel_mix_bytes
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        # Mock S3
        mock_s3 = Mock()
        mock_s3.put_object.return_value = {"VersionId": "v1", "ETag": "abc123"}
        collector.s3_client = mock_s3

        # Mock hash registry
        collector.hash_registry.exists = Mock(return_value=False)
        collector.hash_registry.register = Mock()

        # Run collection
        results = collector.run_collection()

        assert results["total_candidates"] == 1
        assert results["collected"] == 1
        assert results["failed"] == 0
        assert results["skipped_duplicate"] == 0

    @patch("requests.get")
    def test_skips_duplicate_content(self, mock_get, collector, sample_fuel_mix_bytes):
        """Should skip content with existing hash."""
        # Mock HTTP response
        mock_response = Mock()
        mock_response.content = sample_fuel_mix_bytes
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        # Mock hash registry to return existing hash
        collector.hash_registry.exists = Mock(return_value=True)

        # Run collection
        results = collector.run_collection()

        assert results["skipped_duplicate"] == 1
        assert results["collected"] == 0

    @patch("requests.get")
    def test_handles_collection_error(self, mock_get, collector):
        """Should record error and continue."""
        mock_get.side_effect = Exception("Network error")

        results = collector.run_collection()

        assert results["failed"] == 1
        assert len(results["errors"]) == 1
        assert "Network error" in results["errors"][0]["error"]
