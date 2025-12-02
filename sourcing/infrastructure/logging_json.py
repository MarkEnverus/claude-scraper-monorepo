"""JSON-structured logging for Grafana/Loki compatibility.

This module extends the existing logging infrastructure with JSON formatting
support for better integration with log aggregation systems like Grafana Loki,
CloudWatch Logs Insights, and Elasticsearch.

Example:
    >>> from sourcing.common.logging_json import setup_logging
    >>>
    >>> logger = setup_logging(level='INFO', use_json=True)
    >>> logger.info("Processing started", extra={"file_count": 10, "source": "nyiso"})

    Output:
    {"timestamp": "2025-01-20T14:30:05Z", "level": "INFO", "message": "Processing started", "file_count": 10, "source": "nyiso"}
"""

import logging
import json
from datetime import datetime
from typing import Any, Dict


class JsonFormatter(logging.Formatter):
    """Format logs as JSON for structured logging.

    Converts log records to JSON format suitable for log aggregation systems.
    Includes standard fields (timestamp, level, message, etc.) and supports
    custom extra fields for contextual logging.

    Example Output:
        {
          "timestamp": "2025-01-20T14:30:05Z",
          "level": "INFO",
          "logger": "sourcing_app",
          "message": "Successfully collected",
          "module": "scraper_nyiso_load",
          "function": "run_collection",
          "line": 145,
          "candidate": "load_forecast_20250120_14.json",
          "hash": "abc123..."
        }
    """

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON.

        Args:
            record: Log record to format

        Returns:
            JSON-formatted log string

        Note:
            Extra fields passed to logger methods are automatically included
            in the output JSON.
        """
        log_data = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Add extra fields if present
        # Extra fields are added via: logger.info("message", extra={"key": "value"})
        if hasattr(record, "extra") and isinstance(record.extra, dict):
            log_data.update(record.extra)

        # Also check for any custom attributes added directly to record
        for key, value in record.__dict__.items():
            if key not in (
                'name', 'msg', 'args', 'created', 'filename', 'funcName',
                'levelname', 'levelno', 'lineno', 'module', 'msecs',
                'pathname', 'process', 'processName', 'relativeCreated',
                'thread', 'threadName', 'exc_info', 'exc_text', 'stack_info',
                'extra'
            ):
                # Add custom attributes
                log_data[key] = value

        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_data)


def setup_logging(level: str = "INFO", use_json: bool = True) -> logging.Logger:
    """Setup logging with optional JSON formatting.

    Configures the 'sourcing_app' logger with either JSON or traditional
    text formatting. The JSON format is recommended for production environments
    with log aggregation systems.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        use_json: Use JSON formatter (default True). Set to False for
                 backward compatibility with existing text-based logging.

    Returns:
        Configured logger instance

    Example:
        >>> # JSON logging (for production)
        >>> logger = setup_logging(level='INFO', use_json=True)
        >>> logger.info("Processing file", extra={"filename": "data.csv"})

        >>> # Text logging (for development/debugging)
        >>> logger = setup_logging(level='DEBUG', use_json=False)
        >>> logger.debug("Parsing row", extra={"row": 42})
    """
    handler = logging.StreamHandler()

    if use_json:
        handler.setFormatter(JsonFormatter())
    else:
        # Use existing format for backward compatibility
        formatter = logging.Formatter(
            "[%(levelname)s|%(module)s|L%(lineno)d] %(asctime)s: %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S%z"
        )
        handler.setFormatter(formatter)

    logger = logging.getLogger("sourcing_app")
    logger.handlers.clear()  # Remove existing handlers
    logger.addHandler(handler)
    logger.setLevel(level)

    return logger


# Convenience function for adding context to logs
def log_with_context(logger: logging.Logger, level: str, message: str, **context):
    """Log message with contextual fields.

    Helper function to easily add context to log messages without
    explicitly using the extra parameter.

    Args:
        logger: Logger instance
        level: Log level (debug, info, warning, error, critical)
        message: Log message
        **context: Key-value pairs to include in structured log

    Example:
        >>> from sourcing.common.logging_json import log_with_context
        >>> log_with_context(
        ...     logger, 'info', 'File processed',
        ...     filename='data.csv',
        ...     rows=1000,
        ...     duration=2.5
        ... )

        Output:
        {"timestamp": "...", "message": "File processed", "filename": "data.csv", "rows": 1000, "duration": 2.5}
    """
    log_method = getattr(logger, level.lower())
    log_method(message, extra=context)
