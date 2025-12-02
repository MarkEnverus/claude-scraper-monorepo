"""Kafka utilities for data collection notifications.

This module provides Kafka integration for publishing notifications when data
is collected and stored. It includes configuration parsing, producer management,
and message serialization.

Adapted from pr.prt.sourcing for generic use, removing PRT-specific references.

Example:
    >>> config = KafkaConfiguration("kafka://localhost:9092/my-topic")
    >>> message = ScraperNotificationMessage(
    ...     dataset="nyiso_load",
    ...     environment="dev",
    ...     urn="load_20250120.json",
    ...     location="s3://bucket/path/file.gz",
    ...     version="20250120T140000Z",
    ...     etag="abc123",
    ...     metadata={"source": "nyiso"}
    ... )
    >>> with KafkaProducer(config) as producer:
    ...     producer.publish(message)
"""

import json
import os
import logging
from urllib.parse import urlparse, parse_qs
from typing import Dict, Any, Optional

from pydantic import BaseModel, Field

logger = logging.getLogger("sourcing_app")


class KafkaConfiguration:
    """Kafka connection configuration parser.

    Parses Kafka connection strings and provides configuration for producers.
    Supports SASL authentication via credentials file or environment variables.

    Connection String Format:
        kafka://host:port/topic?param=value&...

    Query Parameters:
        - security_protocol: PLAINTEXT, SASL_PLAINTEXT, SASL_SSL (default: PLAINTEXT)
        - sasl_mechanism: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512 (default: PLAIN)
        - X_sasl_file: Path to JSON file with sasl.username and sasl.password

    Examples:
        Simple (no authentication):
            kafka://localhost:9092/my-topic

        With SASL authentication from file:
            kafka://localhost:9092/my-topic?security_protocol=SASL_PLAINTEXT&X_sasl_file=/path/to/creds.json

        With SASL authentication from environment:
            kafka://localhost:9092/my-topic?security_protocol=SASL_PLAINTEXT
            (requires SASL_USERNAME and SASL_PASSWORD environment variables)
    """

    def __init__(self, connection_string: str):
        """Initialize Kafka configuration from connection string.

        Args:
            connection_string: Kafka connection string

        Raises:
            ValueError: If connection string is invalid or credentials are missing
        """
        try:
            parsed = urlparse(connection_string)
            query_dict = parse_qs(parsed.query)

            # Validate schema
            if parsed.scheme != "kafka":
                raise ValueError(
                    f"Invalid connection string schema: {parsed.scheme}. Expected: kafka"
                )

            # Server configuration
            self.bootstrap_server = f"{parsed.hostname}:{parsed.port}"

            # Topic
            self.topic = parsed.path[1:]  # Remove leading '/'
            if not self.topic:
                raise ValueError("Topic is required in connection string")

            # Security protocol
            self.security_protocol = query_dict.get(
                "security_protocol", ["PLAINTEXT"]
            )[0]

            # SASL configuration
            self.sasl_mechanism = query_dict.get("sasl_mechanism", ["PLAIN"])[0]

            # Credentials
            sasl_file = query_dict.get("X_sasl_file", [""])[0]
            self.sasl_username, self.sasl_password = self._read_sasl_credentials(
                sasl_file
            )

        except Exception as e:
            raise ValueError(
                f"Error parsing Kafka connection string: {connection_string}"
            ) from e

    @property
    def producer_cfg_dict(self) -> Dict[str, str]:
        """Returns a dictionary with the Kafka producer configuration.

        Returns:
            Configuration dict for confluent_kafka.Producer
        """
        config = {
            "bootstrap.servers": self.bootstrap_server,
            "linger.ms": "100",
            "compression.type": "zstd",
        }

        # Add security configuration if not using PLAINTEXT
        if self.security_protocol != "PLAINTEXT":
            config["security.protocol"] = self.security_protocol
            config["sasl.mechanism"] = self.sasl_mechanism

            if self.sasl_username and self.sasl_password:
                config["sasl.username"] = self.sasl_username
                config["sasl.password"] = self.sasl_password

        return config

    @property
    def log_cfg_dict(self) -> Dict[str, str]:
        """Returns a dictionary with the Kafka configuration for logging purposes.

        Returns:
            Sanitized configuration dict (no credentials)
        """
        return {
            "bootstrap.servers": self.bootstrap_server,
            "topic": self.topic,
            "security.protocol": self.security_protocol,
            "sasl.mechanism": self.sasl_mechanism,
        }

    def _read_sasl_credentials(self, file_path: str) -> tuple[Optional[str], Optional[str]]:
        """Reads SASL credentials from a file or environment variables.

        The file should be JSON with keys:
            - sasl.username: The username for SASL authentication
            - sasl.password: The password for SASL authentication

        If the file is not found or empty, falls back to environment variables:
            - SASL_USERNAME
            - SASL_PASSWORD

        Args:
            file_path: Path to credentials JSON file (can be empty string)

        Returns:
            Tuple of (username, password) or (None, None) if not found

        Note:
            Missing credentials are only a warning, not an error. This allows
            unauthenticated Kafka for development environments.
        """
        sasl_username = None
        sasl_password = None

        # Try to read from file
        if file_path:
            try:
                with open(file_path) as f:
                    credentials = json.load(f)
                    sasl_password = credentials.get("sasl.password")
                    sasl_username = credentials.get("sasl.username")
                    logger.debug(f"Read SASL credentials from file: {file_path}")
            except Exception as e:
                logger.debug(
                    f"Error reading SASL credentials from file {file_path}: "
                    f"{type(e).__name__} - {e}. Trying environment variables."
                )

        # Fall back to environment variables
        if not sasl_username or not sasl_password:
            sasl_username = os.getenv("SASL_USERNAME")
            sasl_password = os.getenv("SASL_PASSWORD")

            if sasl_username and sasl_password:
                logger.debug("Read SASL credentials from environment variables")

        # Warn if credentials are missing but authentication is expected
        if not sasl_username or not sasl_password:
            if self.security_protocol != "PLAINTEXT":
                logger.warning(
                    f"SASL credentials not found in file or environment variables. "
                    f"Security protocol is {self.security_protocol} but no credentials available."
                )

        return sasl_username, sasl_password


class KafkaProducer:
    """Kafka producer for publishing notification messages.

    Context manager that wraps confluent_kafka.Producer with automatic
    flushing on exit and error handling.

    Example:
        >>> config = KafkaConfiguration("kafka://localhost:9092/topic")
        >>> with KafkaProducer(config) as producer:
        ...     producer.publish(message)
    """

    def __init__(self, cfg: KafkaConfiguration):
        """Initialize Kafka Producer.

        Args:
            cfg: KafkaConfiguration object with connection settings

        Raises:
            ImportError: If confluent-kafka is not installed
            RuntimeError: If producer initialization fails
        """
        try:
            from confluent_kafka import Producer
        except ImportError:
            raise ImportError(
                "confluent-kafka is required for Kafka support. "
                "Install it with: pip install confluent-kafka"
            )

        try:
            self.config = cfg
            self.producer = Producer(cfg.producer_cfg_dict)
            logger.debug(
                f"Kafka producer initialized",
                extra=cfg.log_cfg_dict
            )
        except Exception as e:
            raise RuntimeError(f"Failed to initialize Kafka producer: {e}") from e

    def __enter__(self):
        """Enter context manager."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Exit context manager and flush messages."""
        logger.info("Closing Kafka producer and flushing all messages")
        self.flush()
        logger.debug("Kafka producer closed")

    def __del__(self):
        """Destructor to ensure messages are flushed."""
        if hasattr(self, "producer") and self.producer is not None:
            logger.info("Kafka producer garbage collected, flushing messages")
            self.flush()

    def flush(self, timeout: int = 20) -> None:
        """Flush the producer queue.

        Args:
            timeout: Timeout for flushing the producer in seconds (default: 20)

        Raises:
            RuntimeError: If flush fails
        """
        if hasattr(self, "producer") and self.producer is not None:
            try:
                self.producer.flush(timeout)
            except Exception as e:
                raise RuntimeError(f"Error flushing producer: {e}") from e

    def _on_delivery(self, err, msg):
        """Delivery callback for producer.

        Args:
            err: Error if message delivery failed
            msg: Message object with delivery details

        Raises:
            RuntimeError: If message delivery failed
        """
        if err is not None:
            raise RuntimeError(f"Message delivery failed: {str(err)}")
        else:
            logger.debug(
                f"Message delivered to {msg.topic()} [{msg.partition()}]",
                extra={"topic": msg.topic(), "partition": msg.partition()}
            )

    def publish(self, message: "ScraperNotificationMessage", flush: bool = True) -> None:
        """Publish message to Kafka topic.

        Args:
            message: ScraperNotificationMessage to publish
            flush: Whether to flush the producer after sending (default: True)

        Raises:
            RuntimeError: If publishing fails
        """
        logger.debug(
            f"Publishing message to topic: {self.config.topic}",
            extra={
                "topic": self.config.topic,
                "dataset": message.dataset,
                "urn": message.urn
            }
        )

        try:
            # Serialize message to JSON
            message_str = message.model_dump_json(exclude_unset=True)
            message_key = f"{message.dataset}:{message.urn}"

            # Publish to Kafka
            self.producer.produce(
                self.config.topic,
                key=message_key.encode("utf-8"),
                value=message_str.encode("utf-8"),
                on_delivery=self._on_delivery,
            )

            if flush:
                self.flush()

        except Exception as e:
            raise RuntimeError(
                f"Error publishing message to topic {self.config.topic}: {e}"
            ) from e


class ScraperNotificationMessage(BaseModel):
    """Notification message for scraped data.

    Published to Kafka when a scraper successfully collects and stores data.

    Attributes:
        dataset: Dataset identifier (e.g., "nyiso_load_forecast")
        environment: Environment (dev/staging/prod)
        urn: Uniform Resource Name (unique file identifier)
        location: S3 location where file was stored
        version: Version identifier (typically timestamp)
        etag: S3 ETag of the stored file
        metadata: Additional metadata about the collection

    Example:
        >>> message = ScraperNotificationMessage(
        ...     dataset="nyiso_load_forecast",
        ...     environment="dev",
        ...     urn="load_20250120_14.json",
        ...     location="s3://bucket/sourcing/nyiso_load_forecast/year=2025/month=01/day=20/file.gz",
        ...     version="20250120T143000Z",
        ...     etag="abc123def456",
        ...     metadata={
        ...         "publish_dtm": "2025-01-20T14:30:00Z",
        ...         "s3_guid": "xyz789",
        ...         "url": "https://api.nyiso.com/v1/load",
        ...         "original_file_size": 12345,
        ...         "original_file_md5sum": "hash...",
        ...         "data_type": "load_forecast",
        ...         "source": "nyiso"
        ...     }
        ... )
    """

    dataset: str = Field(..., description="Dataset identifier")
    environment: str = Field(..., description="Environment (dev/staging/prod)")
    urn: str = Field(..., description="Uniform Resource Name for the file")
    location: str = Field(..., description="S3 location where file was stored")
    version: str = Field(..., description="Version identifier/timestamp")
    etag: str = Field(..., description="S3 ETag of the stored file")
    metadata: Dict[str, Any] = Field(..., description="Collection and file metadata")

    class Config:
        """Pydantic model configuration."""
        json_encoders = {
            # Add custom encoders if needed
        }
