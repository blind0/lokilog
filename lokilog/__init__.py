from lokilog.handler import LokiQueueHandler, LokiQueueListener
from lokilog.emitter import LokiEmitter

__all__ = [
    "LokiQueueHandler",
    "LokiQueueListener",
    "LokiEmitter",
    "make_loki_handler",
]

import queue
from typing import Optional


def make_loki_handler(
    url: str,
    labels: Optional[dict] = None,
    extracted_labels: Optional[list] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    timeout: float = 10.0,
    batch_size: int = 1000,
    batch_timeout: float = 2.0,
    max_retries: int = 5,
    queue_maxsize: int = 10_000,
    extra_headers: Optional[dict] = None,
) -> tuple["LokiQueueHandler", "LokiQueueListener"]:
    q: queue.Queue = queue.Queue(maxsize=queue_maxsize)

    emitter = LokiEmitter(
        url=url,
        username=username,
        password=password,
        timeout=timeout,
        batch_size=batch_size,
        batch_timeout=batch_timeout,
        max_retries=max_retries,
        extra_headers=extra_headers or {},
    )

    handler = LokiQueueHandler(
        q,
        extracted_labels=extracted_labels or [],
        global_labels=labels or {},
    )
    listener = LokiQueueListener(q, emitter)

    return handler, listener
