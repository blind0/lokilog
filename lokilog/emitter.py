from __future__ import annotations

import logging
import time
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

import requests

from lokilog.proto import encode_entry, encode_push_request, encode_stream

_internal_logger = logging.getLogger(__name__)

try:
    import cramjam
    def _snappy_compress(data: bytes) -> bytes:
        return bytes(cramjam.snappy.compress_raw(data))
    _USE_PROTOBUF = True
except ImportError:
    _USE_PROTOBUF = False
    _snappy_compress = None

def _labels_to_str(labels: Dict[str, str]) -> str:
    parts = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
    return "{" + parts + "}"

class LogEntry:
    __slots__ = ("timestamp", "line", "labels", "metadata")
    def __init__(
        self,
        timestamp: int,
        line: str,
        labels: Dict[str, str],
        metadata: List[Tuple[str, str]],
    ) -> None:
        self.timestamp = timestamp
        self.line      = line
        self.labels    = labels
        self.metadata  = metadata

class LokiEmitter:
    PUSH_PATH = "/loki/api/v1/push"

    def __init__(
        self,
        url: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        timeout: float = 10.0,
        batch_size: int = 1000,
        batch_timeout: float = 2.0,
        max_retries: int = 5,
        extra_headers: Optional[Dict[str, str]] = None,
    ) -> None:
        self.url           = url.rstrip("/")
        self.timeout       = timeout
        self.batch_size    = batch_size
        self.batch_timeout = batch_timeout
        self.max_retries   = max_retries
        self._batch: Dict[str, List[bytes]] = defaultdict(list)
        self._batch_count = 0
        self._batch_first_ts: Optional[float] = None
        self._session = requests.Session()
        if username and password:
            self._session.auth = (username, password)
        default_headers: Dict[str, str] = {"User-Agent": "loki-logger-python/0.1"}
        if extra_headers:
            default_headers.update(extra_headers)
        self._session.headers.update(default_headers)

    def add(self, entry: LogEntry) -> None:
        label_str = _labels_to_str(entry.labels)
        if _USE_PROTOBUF:
            encoded = encode_entry(entry.timestamp, entry.line, entry.metadata or None)
            self._batch[label_str].append(encoded)
        else:
            self._batch[label_str].append({
                "ts": entry.timestamp,
                "line": entry.line,
                "metadata": entry.metadata,
            })
        self._batch_count += 1
        if self._batch_first_ts is None:
            self._batch_first_ts = time.monotonic()

    def should_flush(self) -> bool:
        if self._batch_count == 0:
            return False
        if self._batch_count >= self.batch_size:
            return True
        if self._batch_first_ts is not None and (time.monotonic() - self._batch_first_ts) >= self.batch_timeout:
            return True
        return False

    def flush(self) -> None:
        if not self._batch_count:
            return
        payload, content_type = self._build_payload()
        self._send_with_retry(payload, content_type)
        self._reset_batch()

    def close(self) -> None:
        self.flush()
        self._session.close()

    def _build_payload(self) -> Tuple[bytes, str]:
        if _USE_PROTOBUF:
            return self._build_protobuf_payload()
        return self._build_json_payload()

    def _build_protobuf_payload(self) -> Tuple[bytes, str]:
        streams_bytes = []
        for label_str, entries in self._batch.items():
            streams_bytes.append(encode_stream(label_str, entries))
        raw = encode_push_request(streams_bytes)
        return _snappy_compress(raw), "application/x-protobuf"

    def _build_json_payload(self) -> Tuple[bytes, str]:
        import json as _json
        streams = []
        for label_str, entries in self._batch.items():
            values = []
            for e in entries:
                row = [str(e["ts"]), e["line"]]
                if e["metadata"]:
                    row.append({k: v for k, v in e["metadata"]})
                values.append(row)
            streams.append({"stream": _parse_label_str(label_str), "values": values})
        return _json.dumps({"streams": streams}).encode("utf-8"), "application/json"

    def _send_with_retry(self, payload: bytes, content_type: str) -> None:
        url = self.url + self.PUSH_PATH
        delay = 0.5
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self._session.post(url, data=payload, headers={"Content-Type": content_type}, timeout=self.timeout)
                if resp.status_code in (200, 204):
                    return
                if resp.status_code in (429, 500, 502, 503, 504):
                    _internal_logger.warning("Loki push failed (%d/%d): %d", attempt, self.max_retries, resp.status_code)
                else:
                    _internal_logger.error("Loki push rejected: %d %s", resp.status_code, resp.text[:200])
                    return
            except requests.RequestException as exc:
                _internal_logger.warning("Loki push error (%d/%d): %s", attempt, self.max_retries, exc)
            time.sleep(delay)
            delay = min(delay * 2, 60.0)

    def _reset_batch(self) -> None:
        self._batch.clear()
        self._batch_count = 0
        self._batch_first_ts = None

def _parse_label_str(label_str: str) -> Dict[str, str]:
    result: Dict[str, str] = {}
    inner = label_str.strip("{}")
    if not inner: return result
    for pair in inner.split(","):
        k, _, v = pair.partition("=")
        result[k.strip()] = v.strip().strip('"')
    return result
