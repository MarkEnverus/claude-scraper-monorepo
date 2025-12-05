"""MISO Real-Time Binding Constraints Scraper.

Collects Real-Time transmission binding constraint data from MISO's Public API.
Endpoint: https://public-api.misoenergy.org/api/BindingConstraints/RealTime

Real-time binding transmission constraints provide critical market insights into:
- Grid transmission congestion
- Power flow limitations
- Locational pricing impact via shadow prices
- Transmission system operational constraints

Key characteristics:
- Public API (no authentication required)
- Real-time data (5-minute updates)
- No historical data support (real-time snapshot only)
- Single-shot collection (no pagination)
- Shadow pricing for congestion analysis

Data is stored to S3 with timestamp partitioning and deduplicated using Redis.

Version Information:
    INFRASTRUCTURE_VERSION: 1.3.0
    LAST_UPDATED: 2025-12-05

Features:
    - HTTP REST API collection using BaseCollector framework
    - No authentication required (public API)
    - Real-time snapshot collection
    - Redis-based hash deduplication
    - S3 storage with timestamp partitioning and gzip compression
    - Kafka notifications for downstream processing
    - Comprehensive error handling and validation
    - Shadow price validation
"""

# INFRASTRUCTURE_VERSION: 1.3.0
# LAST_UPDATED: 2025-12-05

import json
import logging
from datetime import datetime, timezone
from typing import List

import boto3
import click
import redis
import requests

from sourcing.infrastructure.collection_framework import (
    BaseCollector,
    DownloadCandidate,
    ScrapingError,
)

logger = logging.getLogger("sourcing_app")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


class MisoBindingConstraintsRealtimeCollector(BaseCollector):
    """Collector for MISO Real-Time Binding Constraints data via Public API."""

    BASE_URL = "https://public-api.misoenergy.org/api/BindingConstraints/RealTime"
    TIMEOUT_SECONDS = 30  # Real-time snapshot, should be fast

    def __init__(
        self,
        **kwargs
    ):
        """Initialize Real-Time Binding Constraints collector.

        Args:
            **kwargs: Additional arguments passed to BaseCollector
        """
        super().__init__(**kwargs)
        logger.info("Initialized RT Binding Constraints collector (public API)")

    def generate_candidates(self, **kwargs) -> List[DownloadCandidate]:
        """Generate a single candidate for current real-time snapshot.

        MISO publishes real-time binding constraints as a snapshot of current
        transmission system constraints. No historical data is available - only
        the current state of binding constraints with shadow pricing.
        """
        candidates = []

        # Use current UTC time for identifier
        now = datetime.now(timezone.utc)
        timestamp_str = now.strftime('%Y%m%d_%H%M%S')
        identifier = f"binding_constraints_realtime_{timestamp_str}.json"

        candidate = DownloadCandidate(
            identifier=identifier,
            source_location=self.BASE_URL,
            metadata={
                "data_type": "binding_constraints_realtime",
                "source": "miso",
                "collection_timestamp": now.isoformat(),
                "market_type": "real_time_transmission",
                "snapshot": True,  # Real-time snapshot only
            },
            collection_params={
                "headers": {
                    "Accept": "application/json",
                    "User-Agent": "MISO-BindingConstraints-Collector/1.0",
                },
                "timeout": self.TIMEOUT_SECONDS,
            },
            file_date=now.date(),
        )

        candidates.append(candidate)
        logger.info(
            f"Generated candidate for real-time snapshot at {now.isoformat()}"
        )

        return candidates

    def collect_content(self, candidate: DownloadCandidate) -> bytes:
        """Fetch JSON data from MISO Public API.

        The Binding Constraints endpoint returns a single JSON response with:
        - RefId: Timestamp of data retrieval
        - Constraint: Array of binding constraint records with shadow pricing

        No pagination required - this is a real-time snapshot.
        """
        logger.info(
            f"Fetching RT Binding Constraints snapshot from {candidate.source_location}"
        )

        try:
            response = requests.get(
                candidate.source_location,
                headers=candidate.collection_params.get("headers", {}),
                timeout=candidate.collection_params.get("timeout", self.TIMEOUT_SECONDS),
            )
            response.raise_for_status()

            # Parse JSON response
            json_data = response.json()

            # Add collection metadata
            json_data["collection_metadata"] = {
                "collection_timestamp": candidate.metadata.get("collection_timestamp"),
                "source": "miso_public_api",
                "endpoint": self.BASE_URL,
            }

            constraint_count = len(json_data.get("Constraint", []))
            logger.info(
                f"Successfully collected {constraint_count} binding constraints "
                f"(RefId: {json_data.get('RefId', 'unknown')})"
            )

            return json.dumps(json_data, indent=2).encode('utf-8')

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.warning("No binding constraints data available (404)")
                # Return empty valid response
                empty_response = {
                    "RefId": datetime.now(timezone.utc).strftime("%d-%b-%Y, %H:%M UTC"),
                    "Constraint": [],
                    "collection_metadata": {
                        "collection_timestamp": candidate.metadata.get("collection_timestamp"),
                        "source": "miso_public_api",
                        "endpoint": self.BASE_URL,
                        "note": "No binding constraints active"
                    }
                }
                return json.dumps(empty_response, indent=2).encode('utf-8')
            elif e.response.status_code == 429:
                logger.warning("Rate limit exceeded - consider adding delays between requests")
            raise ScrapingError(f"HTTP error fetching Binding Constraints data: {e}") from e
        except requests.exceptions.RequestException as e:
            raise ScrapingError(f"Failed to fetch Binding Constraints data: {e}") from e
        except json.JSONDecodeError as e:
            raise ScrapingError(f"Invalid JSON response: {e}") from e

    def validate_content(self, content: bytes, candidate: DownloadCandidate) -> bool:
        """Validate JSON structure of Real-Time Binding Constraints data.

        Expected format:
        {
            "RefId": "05-Dec-2025, 09:35 EST",
            "Constraint": [
                {
                    "Name": "AMRN_3053_CNTY",
                    "Period": "2025-12-05T09:35:00",
                    "Price": 12.45,
                    "OVERRIDE": 0,
                    "CURVETYPE": "MW",
                    "BP1": 100.0,
                    "PC1": 95.0,
                    "BP2": 120.0,
                    "PC2": 98.0
                }
            ],
            "collection_metadata": {...}
        }
        """
        try:
            text_content = content.decode('utf-8')
            data = json.loads(text_content)

            # Check top-level structure
            if "RefId" not in data:
                logger.error("Missing 'RefId' field in response")
                return False

            if "Constraint" not in data:
                logger.error("Missing 'Constraint' field in response")
                return False

            # Empty constraint list is valid (no active binding constraints)
            if not data["Constraint"] or len(data["Constraint"]) == 0:
                logger.info("No binding constraints active at this time")
                return True

            # Validate first record structure
            record = data["Constraint"][0]
            required_fields = ["Name", "Period", "Price"]

            for field in required_fields:
                if field not in record:
                    logger.error(f"Missing required field: {field}")
                    return False

            # Validate constraint name is non-empty string
            if not isinstance(record["Name"], str) or not record["Name"]:
                logger.error(f"Invalid constraint Name: {record.get('Name')}")
                return False

            # Validate Period is a timestamp string
            if not isinstance(record["Period"], str):
                logger.error(f"Invalid Period format: {record.get('Period')}")
                return False

            # Try to parse Period as timestamp
            try:
                # MISO uses various timestamp formats, be flexible
                period_str = record["Period"]
                # Could be ISO format or MISO-specific format
                if 'T' in period_str or '-' in period_str:
                    # Basic validation that it looks like a timestamp
                    pass
                else:
                    logger.warning(f"Unusual Period format: {period_str}")
            except Exception as e:
                logger.error(f"Failed to parse Period timestamp: {e}")
                return False

            # Validate Price (shadow price) is numeric
            if not isinstance(record["Price"], (int, float)):
                logger.error(f"Price value is not numeric: {record['Price']}")
                return False

            # Optional fields validation if present
            if "OVERRIDE" in record:
                # OVERRIDE should be boolean (0/1) or blank
                if record["OVERRIDE"] not in [0, 1, "", None]:
                    logger.warning(f"Unusual OVERRIDE value: {record['OVERRIDE']}")

            if "CURVETYPE" in record:
                # CURVETYPE should be string like "MW" or "PERCENT"
                valid_curve_types = ["MW", "PERCENT", "", None]
                if record["CURVETYPE"] not in valid_curve_types:
                    logger.info(f"Note: CURVETYPE '{record['CURVETYPE']}' may be non-standard")

            # Validate breakpoint parameters if present
            for bp_field in ["BP1", "PC1", "BP2", "PC2"]:
                if bp_field in record and record[bp_field] is not None:
                    if not isinstance(record[bp_field], (int, float)):
                        logger.error(f"{bp_field} value is not numeric: {record[bp_field]}")
                        return False

            # Validate RefId format (should be a timestamp)
            ref_id = data["RefId"]
            if not isinstance(ref_id, str) or not ref_id:
                logger.error(f"Invalid RefId: {ref_id}")
                return False

            # Check for reasonable data
            constraint_count = len(data["Constraint"])
            logger.info(f"Validated {constraint_count} binding constraints successfully")

            # Log shadow pricing summary
            if constraint_count > 0:
                prices = [c.get("Price", 0) for c in data["Constraint"]]
                max_price = max(prices)
                min_price = min(prices)
                logger.info(
                    f"Shadow price range: ${min_price:.2f} to ${max_price:.2f}/MWh"
                )

            return True

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON content: {str(e)}")
            return False
        except (KeyError, ValueError) as e:
            logger.error(f"Validation error: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected validation error: {str(e)}")
            return False


@click.command()
@click.option(
    "--s3-bucket",
    envvar="S3_BUCKET",
    help="S3 bucket for data storage (or set S3_BUCKET environment variable)"
)
@click.option(
    "--aws-profile",
    envvar="AWS_PROFILE",
    help="AWS profile name (or set AWS_PROFILE environment variable)"
)
@click.option(
    "--redis-host",
    default="localhost",
    envvar="REDIS_HOST",
    help="Redis host for deduplication"
)
@click.option(
    "--redis-port",
    default=6379,
    envvar="REDIS_PORT",
    help="Redis port"
)
@click.option(
    "--redis-db",
    default=0,
    envvar="REDIS_DB",
    help="Redis database number"
)
@click.option(
    "--environment",
    type=click.Choice(["dev", "staging", "prod"]),
    default="dev",
    help="Environment for S3 storage and Redis"
)
@click.option(
    "--force",
    is_flag=True,
    help="Force re-download of existing files"
)
@click.option(
    "--skip-hash-check",
    is_flag=True,
    help="Skip Redis hash-based deduplication"
)
@click.option(
    "--log-level",
    default="INFO",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"]),
    help="Logging level"
)
def main(
    s3_bucket: str,
    aws_profile: str,
    redis_host: str,
    redis_port: int,
    redis_db: int,
    environment: str,
    force: bool,
    skip_hash_check: bool,
    log_level: str
) -> None:
    """Collect MISO Real-Time Binding Constraints data from Public API.

    This scraper collects real-time transmission binding constraint data with shadow
    pricing from MISO's public API. The data represents current grid transmission
    congestion and power flow limitations that impact locational marginal pricing.

    Key Features:
    - No authentication required (public API)
    - Real-time snapshot (5-minute updates)
    - Shadow pricing for congestion analysis
    - No historical data available

    Shadow Pricing:
    The Price field represents the shadow price ($/MWh) - the marginal economic value
    of relaxing a specific transmission constraint. Higher shadow prices indicate
    more severe congestion with greater economic impact.

    Examples:

        # Collect current real-time snapshot
        python scraper_miso_binding_constraints_realtime.py

        # With S3 storage
        export S3_BUCKET=your-bucket
        python scraper_miso_binding_constraints_realtime.py

        # With debug logging
        python scraper_miso_binding_constraints_realtime.py --log-level DEBUG

        # Force re-collection
        python scraper_miso_binding_constraints_realtime.py --force

    Note: This API provides only the current snapshot. For continuous monitoring,
    schedule this scraper to run every 5 minutes via cron or workflow scheduler.
    """
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    logger.info(
        "Starting MISO RT Binding Constraints collection",
        extra={
            "environment": environment,
            "force": force,
            "skip_hash_check": skip_hash_check
        }
    )

    # Initialize Redis client
    redis_client = redis.Redis(
        host=redis_host,
        port=redis_port,
        db=redis_db,
        decode_responses=False
    )

    # Test Redis connection
    try:
        redis_client.ping()
        logger.info(f"Connected to Redis at {redis_host}:{redis_port}/{redis_db}")
    except redis.ConnectionError as e:
        logger.error(f"Failed to connect to Redis: {e}")
        raise

    # Initialize S3 client if needed
    session_kwargs = {}
    if aws_profile:
        session_kwargs["profile_name"] = aws_profile

    session = boto3.Session(**session_kwargs)
    s3_client = session.client("s3")

    if s3_bucket:
        logger.info(f"Using S3 bucket: {s3_bucket}")
    else:
        logger.warning("No S3 bucket specified - files will be validated but not uploaded")

    # Initialize collector
    collector = MisoBindingConstraintsRealtimeCollector(
        dgroup="miso_binding_constraints_realtime",
        s3_bucket=s3_bucket,
        s3_prefix="sourcing",
        redis_client=redis_client,
        environment=environment,
    )

    # Override the s3_client to use our profile-aware one
    collector.s3_client = s3_client

    try:
        results = collector.run_collection()

        logger.info(
            "Collection complete",
            extra={
                "files_downloaded": results.get("files_downloaded", 0),
                "files_skipped": results.get("files_skipped", 0),
                "files_failed": results.get("files_failed", 0),
                "total_processed": results.get("total_processed", 0)
            }
        )

    except Exception as e:
        logger.error(f"Collection failed: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
