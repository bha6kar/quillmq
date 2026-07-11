# SPDX-License-Identifier: Apache-2.0
"""Logging configuration for QuillMQ, with an optional JSON formatter."""

from __future__ import annotations

import json
import logging
import sys

logger = logging.getLogger("quillmq")


class JsonFormatter(logging.Formatter):
    """Render log records as one JSON object per line for log aggregation."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload)


def configure_logging(level: str = "INFO", json_format: bool = False) -> None:
    handler = logging.StreamHandler(sys.stderr)
    if json_format:
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
        )
    logger.handlers = [handler]
    logger.setLevel(level.upper())
    logger.propagate = False
