"""Confluent Schema Registry wire-format codec (0x00 + 4-byte schema ID + binary Avro)."""

from __future__ import annotations

import io
import json
import struct
import time
from pathlib import Path
from typing import Any

import fastavro
import requests

MAGIC_BYTE = 0x00
HEADER_STRUCT = struct.Struct(">bI")


class ConfluentAvroCodec:
    """Encodes and decodes Avro records using the Confluent wire framing expected by Flink."""

    def __init__(self, schema_dict: dict[str, Any], schema_id: int) -> None:
        self.schema_dict = schema_dict
        self.parsed_schema = fastavro.parse_schema(schema_dict)
        self.schema_id = schema_id
        self._header = HEADER_STRUCT.pack(MAGIC_BYTE, schema_id)

    @classmethod
    def from_schema_file(cls, schema_path: str | Path, schema_id: int = 1) -> "ConfluentAvroCodec":
        schema_dict = json.loads(Path(schema_path).read_text(encoding="utf-8"))
        return cls(schema_dict=schema_dict, schema_id=schema_id)

    def encode(self, record: dict[str, Any]) -> bytes:
        buf = io.BytesIO()
        buf.write(self._header)
        fastavro.schemaless_writer(buf, self.parsed_schema, record)
        return buf.getvalue()

    def decode(self, payload: bytes) -> tuple[int, dict[str, Any]]:
        if len(payload) < 5:
            raise ValueError(f"Payload too short for Confluent Avro wire header: {len(payload)} bytes")
        magic, schema_id = HEADER_STRUCT.unpack(payload[:5])
        if magic != MAGIC_BYTE:
            raise ValueError(f"Invalid Confluent magic byte: 0x{magic:02x}")
        buf = io.BytesIO(payload[5:])
        record = fastavro.schemaless_reader(buf, self.parsed_schema)
        return schema_id, dict(record)


def register_schema(
    registry_url: str,
    subject: str,
    schema_path: str | Path,
    compatibility: str = "BACKWARD",
    timeout_sec: float = 10.0,
) -> ConfluentAvroCodec:
    """Registers an Avro schema in Karapace / Confluent Schema Registry and returns a ready codec."""
    base = registry_url.rstrip("/")
    schema_text = Path(schema_path).read_text(encoding="utf-8")
    schema_dict = json.loads(schema_text)

    # Set subject compatibility mode (idempotent)
    requests.put(
        f"{base}/config/{subject}",
        json={"compatibility": compatibility},
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        timeout=timeout_sec,
    ).raise_for_status()

    resp = requests.post(
        f"{base}/subjects/{subject}/versions",
        json={"schema": json.dumps(schema_dict), "schemaType": "AVRO"},
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        timeout=timeout_sec,
    )
    resp.raise_for_status()
    schema_id = int(resp.json()["id"])
    return ConfluentAvroCodec(schema_dict=schema_dict, schema_id=schema_id)


def wait_for_registry(registry_url: str, max_wait_sec: float = 60.0) -> None:
    base = registry_url.rstrip("/")
    deadline = time.monotonic() + max_wait_sec
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        try:
            r = requests.get(f"{base}/subjects", timeout=3.0)
            if r.status_code == 200:
                return
        except Exception as exc:  # noqa: BLE001
            last_err = exc
        time.sleep(1.0)
    raise TimeoutError(f"Schema Registry at {base} not ready within {max_wait_sec}s: {last_err}")
