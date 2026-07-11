# SPDX-License-Identifier: Apache-2.0
"""QuillMQ: a lightweight single-node message broker."""

from quillmq.client import Connection, connect

__version__ = "0.1.0"
__all__ = ["connect", "Connection", "__version__"]
