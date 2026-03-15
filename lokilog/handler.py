from __future__ import annotations

import atexit
import json
import logging
import logging.handlers
import queue
import threading
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

from lokilog.emitter import LokiEmitter, LogEntry

_SKIP_RECORD_FIELDS = frozenset({
    "name", "msg", "args", "created", "relativeCreated",
    "levelname", "levelno", "pathname", "filename", "module",
    "exc_info", "exc_text", "stack_info", "lineno", "funcName",
    "msecs", "thread", "threadName", "processName", "process",
    "message", "taskName",
})

_mono_lock = threading.Lock()
_last_ns: int = 0

def _monotonic_ns_from_record(record: logging.LogRecord) -> int:
    global _last_ns
    ns = int(record.created * 1_000_000_000)
    with _mono_lock:
        if ns <= _last_ns:
            _last_ns += 1
            ns = _last_ns
        else:
            _last_ns = ns
    return ns

class LokiQueueHandler(logging.handlers.QueueHandler):
    def __init__(
        self,
        q: queue.Queue,
        extracted_labels: Optional[List[str]] = None,
        global_labels: Optional[Dict[str, str]] = None,
        fmt: Optional[logging.Formatter] = None,
    ) -> None:
        super().__init__(q)
        self._extracted_labels: List[str] = extracted_labels or []
        self._global_labels: Dict[str, str] = global_labels or {}
        self.formatter = fmt

    def enqueue(self, record: logging.LogRecord) -> None:
        try:
            self.queue.put_nowait(record)
        except queue.Full:
            try:
                self.queue.get_nowait()
            except queue.Empty:
                pass
            try:
                self.queue.put_nowait(record)
            except queue.Full:
                pass

    def prepare(self, record: logging.LogRecord) -> LogEntry:
        self.format(record)
        ts_ns = _monotonic_ns_from_record(record)
        labels: Dict[str, str] = {"level": record.levelname, **self._global_labels}
        for key in self._extracted_labels:
            val = getattr(record, key, None)
            if val is not None:
                labels[key] = str(val)

        payload: Dict[str, object] = {
            "message": record.getMessage(),
            "logger":  record.name,
            "level":   record.levelname,
            "module":  record.module,
            "file":    f"{record.filename}:{record.lineno}",
        }
        if record.exc_text:
            payload["exc_info"] = record.exc_text

        extracted_set = set(self._extracted_labels)
        for key, val in record.__dict__.items():
            if key not in _SKIP_RECORD_FIELDS and key not in extracted_set:
                try:
                    json.dumps(val)
                    payload[key] = val
                except (TypeError, ValueError):
                    payload[key] = str(val)

        line = json.dumps(payload, ensure_ascii=False, default=str)
        metadata = [
            (key, str(getattr(record, key, "")))
            for key in self._extracted_labels
            if getattr(record, key, None) is not None
        ]
        return LogEntry(timestamp=ts_ns, line=line, labels=labels, metadata=metadata)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            entry = self.prepare(record)
            self.enqueue(entry)
        except Exception:
            self.handleError(record)

class LokiQueueListener:
    _SENTINEL = object()

    def __init__(
        self,
        q: queue.Queue,
        emitter: LokiEmitter,
        poll_interval: float = 0.2,
    ) -> None:
        self._queue         = q
        self._emitter       = emitter
        self._poll_interval = poll_interval
        self._thread: Optional[threading.Thread] = None
        self._stop_event    = threading.Event()

    def start(self) -> None:
        if self._thread and self._thread.is_alive(): return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, name="LokiQueueListener", daemon=True)
        self._thread.start()
        atexit.register(self.stop)

    def stop(self, timeout: float = 10.0) -> None:
        if not self._thread or not self._thread.is_alive(): return
        self._stop_event.set()
        self._queue.put_nowait(self._SENTINEL)
        self._thread.join(timeout=timeout)

    def _run(self) -> None:
        while True:
            try:
                item = self._queue.get(timeout=self._poll_interval)
            except queue.Empty:
                if self._emitter.should_flush(): self._emitter.flush()
                continue

            if item is self._SENTINEL:
                while True:
                    try:
                        remaining = self._queue.get_nowait()
                        if remaining is not self._SENTINEL and isinstance(remaining, LogEntry):
                            self._emitter.add(remaining)
                    except queue.Empty: break
                self._emitter.flush()
                self._emitter.close()
                return

            if isinstance(item, LogEntry):
                self._emitter.add(item)
                if self._emitter.should_flush(): self._emitter.flush()
