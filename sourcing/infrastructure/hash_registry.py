"""Redis-based content hash registry for deduplication.

This module provides a Redis-backed registry for tracking downloaded content
using SHA256 hashes. It enables efficient deduplication across scraper runs
by storing content hashes with associated metadata and TTL.

Example:
    >>> import redis
    >>> from sourcing.scraping.commons.hash_registry import HashRegistry
    >>>
    >>> redis_client = redis.Redis(host='localhost', port=6379, db=0)
    >>> registry = HashRegistry(redis_client, environment='dev', ttl_days=365)
    >>>
    >>> content = b"sample data"
    >>> content_hash = registry.calculate_hash(content)
    >>>
    >>> if not registry.exists(content_hash, 'nyiso_load'):
    ...     # Process and store content
    ...     registry.register(content_hash, 'nyiso_load', 's3://...', {...})
"""

import hashlib
import json
from datetime import datetime
from typing import Any, Dict, Optional

import redis


class HashRegistry:
    """Redis-based content hash registry for deduplication.

    Stores SHA256 hashes of downloaded content with associated metadata in Redis.
    Uses environment namespacing to isolate dev/staging/prod data.

    Key Format:
        hash:{env}:{dgroup}:{sha256_hash}

    Example Keys:
        hash:dev:nyiso_load_forecast:abc123def456...
        hash:prod:aeso_wmrqh_report:789ghi012jkl...

    Attributes:
        redis: Redis client instance
        environment: Environment name (dev/staging/prod)
        ttl_seconds: Time-to-live in seconds for hash entries
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        environment: str,
        ttl_days: int = 365
    ):
        """Initialize hash registry.

        Args:
            redis_client: Redis client instance
            environment: Environment name (dev/staging/prod)
            ttl_days: Time-to-live in days for hash entries (default 365)

        Raises:
            ValueError: If environment is not one of dev/staging/prod
        """
        if environment not in ('dev', 'staging', 'prod'):
            raise ValueError(
                f"Invalid environment '{environment}'. "
                "Must be one of: dev, staging, prod"
            )

        self.redis = redis_client
        self.environment = environment
        self.ttl_seconds = ttl_days * 86400

    def calculate_hash(self, content: bytes) -> str:
        """Calculate SHA256 hash of content.

        Args:
            content: Raw content bytes to hash

        Returns:
            Hex-encoded SHA256 hash string (64 characters)

        Example:
            >>> registry = HashRegistry(redis_client, 'dev')
            >>> registry.calculate_hash(b"test data")
            'abc123def456...'
        """
        return hashlib.sha256(content).hexdigest()

    def _make_key(self, dgroup: str, content_hash: str) -> str:
        """Build Redis key with environment namespace.

        Format: hash:{env}:{dgroup}:{hash}

        Args:
            dgroup: Data group identifier (e.g., 'nyiso_load_forecast')
            content_hash: SHA256 hash string

        Returns:
            Fully qualified Redis key

        Example:
            >>> registry._make_key('nyiso_load', 'abc123')
            'hash:dev:nyiso_load:abc123'
        """
        return f"hash:{self.environment}:{dgroup}:{content_hash}"

    def exists(self, content_hash: str, dgroup: str) -> bool:
        """Check if content hash exists in registry.

        Args:
            content_hash: SHA256 hash to check
            dgroup: Data group identifier

        Returns:
            True if hash exists, False otherwise

        Example:
            >>> if registry.exists(content_hash, 'nyiso_load'):
            ...     print("Already downloaded, skipping")
        """
        key = self._make_key(dgroup, content_hash)
        return self.redis.exists(key) > 0

    def register(
        self,
        content_hash: str,
        dgroup: str,
        s3_path: str,
        metadata: Dict[str, Any]
    ) -> None:
        """Register hash with metadata and TTL.

        Stores the hash in Redis with associated metadata and expiration.
        The stored record includes the S3 location, registration timestamp,
        and any additional metadata provided.

        Args:
            content_hash: SHA256 hash of content
            dgroup: Data group identifier
            s3_path: S3 location where content is stored
            metadata: Additional metadata to store (serializable to JSON)

        Raises:
            redis.RedisError: If Redis operation fails
            TypeError: If metadata is not JSON serializable

        Example:
            >>> registry.register(
            ...     content_hash='abc123...',
            ...     dgroup='nyiso_load',
            ...     s3_path='s3://bucket/sourcing/nyiso_load/year=2025/...',
            ...     metadata={
            ...         'data_type': 'load_forecast',
            ...         'source': 'nyiso',
            ...         'version_id': 'v1',
            ...         'etag': 'def456'
            ...     }
            ... )
        """
        key = self._make_key(dgroup, content_hash)

        record = {
            "s3_path": s3_path,
            "registered_at": datetime.utcnow().isoformat() + "Z",
            "metadata": metadata
        }

        self.redis.setex(
            key,
            self.ttl_seconds,
            json.dumps(record)
        )

    def get_metadata(self, content_hash: str, dgroup: str) -> Optional[Dict[str, Any]]:
        """Retrieve metadata for a hash.

        Args:
            content_hash: SHA256 hash to lookup
            dgroup: Data group identifier

        Returns:
            Dictionary containing s3_path, registered_at, and metadata fields,
            or None if hash not found

        Example:
            >>> metadata = registry.get_metadata('abc123...', 'nyiso_load')
            >>> if metadata:
            ...     print(f"File at: {metadata['s3_path']}")
            ...     print(f"Registered: {metadata['registered_at']}")
        """
        key = self._make_key(dgroup, content_hash)
        data = self.redis.get(key)
        return json.loads(data) if data else None

    def delete(self, content_hash: str, dgroup: str) -> bool:
        """Delete a hash from the registry.

        Useful for cleanup or when files need to be re-downloaded.

        Args:
            content_hash: SHA256 hash to delete
            dgroup: Data group identifier

        Returns:
            True if hash was deleted, False if it didn't exist

        Example:
            >>> if registry.delete('abc123...', 'nyiso_load'):
            ...     print("Hash removed, file can be re-downloaded")
        """
        key = self._make_key(dgroup, content_hash)
        return self.redis.delete(key) > 0

    def count(self, dgroup: str) -> int:
        """Count total hashes for a data group.

        Note: This performs a SCAN operation which may be slow for large datasets.

        Args:
            dgroup: Data group identifier

        Returns:
            Number of hashes registered for this data group

        Example:
            >>> count = registry.count('nyiso_load')
            >>> print(f"Total files tracked: {count}")
        """
        pattern = f"hash:{self.environment}:{dgroup}:*"
        cursor = 0
        count = 0

        while True:
            cursor, keys = self.redis.scan(cursor, match=pattern, count=100)
            count += len(keys)
            if cursor == 0:
                break

        return count
