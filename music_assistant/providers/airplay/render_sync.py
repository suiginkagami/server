"""MQTT render-sync publisher for AirPlay timeline anchors."""

from __future__ import annotations

import asyncio
import json
from contextlib import suppress
from typing import Any
from uuid import uuid4


class RenderSyncMqttPublisher:
    """Small QoS0 MQTT 3.1.1 publisher for AirPlay render anchors."""

    def __init__(
        self,
        *,
        logger,
        enabled: bool,
        host: str,
        port: int,
        topic: str,
        username: str | None = None,
        password: str | None = None,
    ) -> None:
        """Initialize the render-sync MQTT publisher."""
        self.logger = logger
        self.enabled = enabled and bool(host) and bool(topic)
        self.host = host
        self.port = port
        self.topic = topic
        self.username = username
        self.password = password
        self._client_id = f"ma-airplay-render-sync-{uuid4().hex[:10]}"
        self._queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=256)
        self._runner_task: asyncio.Task[None] | None = None
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None

    def start(self) -> None:
        """Start the background MQTT sender if publishing is enabled."""
        if not self.enabled or self._runner_task is not None:
            return
        self._runner_task = asyncio.create_task(
            self._run(),
            name="airplay-render-sync-mqtt",
        )

    async def stop(self) -> None:
        """Stop the background MQTT sender."""
        task = self._runner_task
        self._runner_task = None
        if task is not None:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        await self._close_connection()

    def publish(self, payload: dict[str, Any]) -> None:
        """
        Queue a render-sync payload for MQTT publishing.

        :param payload: Render-sync payload to serialize and publish.
        """
        if not self.enabled:
            return
        self.start()
        message = json.dumps(payload, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
        if self._queue.full():
            with suppress(asyncio.QueueEmpty):
                self._queue.get_nowait()
        self._queue.put_nowait(message)

    async def _run(self) -> None:
        """Background sender task that keeps the MQTT connection alive on demand."""
        while True:
            message = await self._queue.get()
            try:
                await self._publish_message(message)
            except asyncio.CancelledError:
                raise
            except Exception as err:
                self.logger.warning(
                    "Failed to publish AirPlay render-sync MQTT message to %s:%s: %s",
                    self.host,
                    self.port,
                    err,
                )

    async def _publish_message(self, payload: bytes) -> None:
        """Publish one MQTT message, reconnecting once on failure."""
        for attempt in range(2):
            try:
                await self._ensure_connected()
                assert self._writer is not None
                self._writer.write(_encode_publish_packet(self.topic, payload))
                await self._writer.drain()
                return
            except asyncio.CancelledError:
                raise
            except Exception:
                await self._close_connection()
                if attempt == 1:
                    raise

    async def _ensure_connected(self) -> None:
        """Ensure the MQTT TCP session is connected and acknowledged."""
        if self._writer is not None and not self._writer.is_closing():
            return
        self._reader, self._writer = await asyncio.wait_for(
            asyncio.open_connection(self.host, self.port),
            timeout=5,
        )
        self._writer.write(
            _encode_connect_packet(
                client_id=self._client_id,
                username=self.username,
                password=self.password,
            )
        )
        await self._writer.drain()
        connack = await asyncio.wait_for(self._reader.readexactly(4), timeout=5)
        if connack[0] != 0x20 or connack[1] != 0x02:
            raise ConnectionError("Invalid MQTT CONNACK packet")
        if connack[3] != 0:
            raise ConnectionError(f"MQTT broker rejected connection with code {connack[3]}")

    async def _close_connection(self) -> None:
        """Close the MQTT TCP connection if it exists."""
        writer = self._writer
        self._reader = None
        self._writer = None
        if writer is None:
            return
        if not writer.is_closing():
            with suppress(OSError):
                writer.write(b"\xe0\x00")
                await writer.drain()
        writer.close()
        with suppress(OSError):
            await writer.wait_closed()


def _encode_connect_packet(
    *,
    client_id: str,
    username: str | None,
    password: str | None,
) -> bytes:
    """Encode an MQTT 3.1.1 CONNECT packet."""
    flags = 0x02
    payload = _encode_utf8(client_id)
    if username is not None:
        flags |= 0x80
        payload += _encode_utf8(username)
    if password is not None:
        flags |= 0x40
        payload += _encode_utf8(password)
    variable_header = _encode_utf8("MQTT") + bytes((4, flags)) + (30).to_bytes(2, "big")
    remaining_length = _encode_remaining_length(len(variable_header) + len(payload))
    return bytes((0x10,)) + remaining_length + variable_header + payload


def _encode_publish_packet(topic: str, payload: bytes) -> bytes:
    """Encode an MQTT QoS0 PUBLISH packet."""
    variable_header = _encode_utf8(topic)
    remaining_length = _encode_remaining_length(len(variable_header) + len(payload))
    return bytes((0x30,)) + remaining_length + variable_header + payload


def _encode_utf8(value: str) -> bytes:
    """Encode an MQTT UTF-8 string field."""
    raw = value.encode("utf-8")
    return len(raw).to_bytes(2, "big") + raw


def _encode_remaining_length(value: int) -> bytes:
    """Encode MQTT remaining length using base-128 continuation bytes."""
    encoded = bytearray()
    while True:
        digit = value % 128
        value //= 128
        if value:
            digit |= 0x80
        encoded.append(digit)
        if not value:
            return bytes(encoded)
