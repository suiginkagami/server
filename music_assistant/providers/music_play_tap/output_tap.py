"""Helpers for consuming music-play output_tap PCM packets."""

from __future__ import annotations

import asyncio
import json
import struct
from collections import deque
from dataclasses import dataclass
from typing import Any

from music_assistant_models.enums import ContentType
from music_assistant_models.errors import AudioError
from music_assistant_models.media_items import AudioFormat

OUTPUT_TAP_PROTOCOL_VERSION = 1
OUTPUT_TAP_PACKET_MAGIC = b"OTAP"
_PACKET_PREFIX = struct.Struct("!4sII")
_MAX_METADATA_BYTES = 64 * 1024
_MAX_PCM_BYTES = 16 * 1024 * 1024
_RETENTION_SECONDS = 180


@dataclass(frozen=True, slots=True)
class OutputTapPacket:
    """One decoded output_tap packet."""

    version: int
    event: str
    output_stream_epoch: str
    output_frame: int
    frame_count: int
    sample_rate: int
    channels: int
    sample_format: str
    frame_size: int
    pcm_bytes: int
    output_monotonic_ns: int
    published_monotonic_ns: int
    pcm: bytes


@dataclass(frozen=True, slots=True)
class OutputTapResolvedFrame:
    """Resolved output_tap coordinate for a rendered MA frame."""

    output_stream_epoch: str
    output_frame: int
    local_frame: int
    sample_rate: int
    output_monotonic_ns: int
    published_monotonic_ns: int


@dataclass(frozen=True, slots=True)
class _OutputTapFrameSpan:
    """Mapping span between MA-local frames and output_tap frames."""

    local_start_frame: int
    local_end_frame: int
    output_stream_epoch: str
    output_start_frame: int
    sample_rate: int
    output_monotonic_ns: int
    published_monotonic_ns: int


async def read_output_tap_packet(reader: asyncio.StreamReader) -> OutputTapPacket:
    """
    Read one output_tap packet from a stream.

    :param reader: The TCP stream reader connected to output_tap.
    :returns: Parsed packet metadata and PCM bytes.
    """
    prefix = await reader.readexactly(_PACKET_PREFIX.size)
    magic, metadata_size, pcm_size = _PACKET_PREFIX.unpack(prefix)
    if magic != OUTPUT_TAP_PACKET_MAGIC:
        raise AudioError("Invalid output_tap packet magic")
    if metadata_size > _MAX_METADATA_BYTES:
        raise AudioError("output_tap metadata length exceeds limit")
    if pcm_size > _MAX_PCM_BYTES:
        raise AudioError("output_tap PCM length exceeds limit")
    metadata_raw = await reader.readexactly(metadata_size)
    pcm = await reader.readexactly(pcm_size)
    metadata = json.loads(metadata_raw.decode("utf-8"))
    if not isinstance(metadata, dict):
        raise AudioError("output_tap metadata must be a JSON object")
    return _parse_output_tap_packet(metadata, pcm)


def _parse_output_tap_packet(metadata: dict[str, Any], pcm: bytes) -> OutputTapPacket:
    """Validate and normalize one output_tap metadata object."""
    version = int(metadata["version"])
    event = str(metadata["event"])
    output_stream_epoch = str(metadata["output_stream_epoch"])
    output_frame = int(metadata["output_frame"])
    frame_count = int(metadata["frame_count"])
    sample_rate = int(metadata["sample_rate"])
    channels = int(metadata["channels"])
    sample_format = str(metadata["sample_format"])
    frame_size = int(metadata["frame_size"])
    pcm_bytes = int(metadata["pcm_bytes"])
    output_monotonic_ns = int(metadata["output_monotonic_ns"])
    published_monotonic_ns = int(metadata["published_monotonic_ns"])
    if version != OUTPUT_TAP_PROTOCOL_VERSION:
        raise AudioError(f"Unsupported output_tap protocol version: {version}")
    if event != "output_pcm":
        raise AudioError(f"Unsupported output_tap event: {event}")
    if output_frame < 0 or frame_count < 0:
        raise AudioError("output_tap frame coordinates must be non-negative")
    if pcm_bytes != len(pcm):
        raise AudioError("output_tap PCM length does not match metadata")
    return OutputTapPacket(
        version=version,
        event=event,
        output_stream_epoch=output_stream_epoch,
        output_frame=output_frame,
        frame_count=frame_count,
        sample_rate=sample_rate,
        channels=channels,
        sample_format=sample_format,
        frame_size=frame_size,
        pcm_bytes=pcm_bytes,
        output_monotonic_ns=output_monotonic_ns,
        published_monotonic_ns=published_monotonic_ns,
        pcm=pcm,
    )


class OutputTapStreamSession:
    """Frame-coordinate tracker for one MA playback request consuming output_tap."""

    def __init__(self, audio_format: AudioFormat) -> None:
        """
        Initialize the output_tap session mapper.

        :param audio_format: The PCM format MA expects from output_tap.
        """
        self.audio_format = audio_format
        self.sample_format = _content_type_to_sample_format(audio_format.content_type)
        self.frame_size = (audio_format.bit_depth // 8) * audio_format.channels
        self.total_frames_received = 0
        self.latest_packet: OutputTapPacket | None = None
        self._spans: deque[_OutputTapFrameSpan] = deque()

    def record_packet(self, packet: OutputTapPacket) -> None:
        """
        Record a newly received output_tap packet.

        :param packet: The parsed output_tap packet.
        """
        self._validate_packet(packet)
        local_start_frame = self.total_frames_received
        local_end_frame = local_start_frame + packet.frame_count
        self._spans.append(
            _OutputTapFrameSpan(
                local_start_frame=local_start_frame,
                local_end_frame=local_end_frame,
                output_stream_epoch=packet.output_stream_epoch,
                output_start_frame=packet.output_frame,
                sample_rate=packet.sample_rate,
                output_monotonic_ns=packet.output_monotonic_ns,
                published_monotonic_ns=packet.published_monotonic_ns,
            )
        )
        self.total_frames_received = local_end_frame
        self.latest_packet = packet
        self._trim_history()

    def resolve_output_frame(self, render_elapsed_s: float) -> OutputTapResolvedFrame | None:
        """
        Resolve a rendered MA frame position onto output_tap coordinates.

        :param render_elapsed_s: Rendered elapsed time reported by AirPlay.
        :returns: Matching output_tap coordinate, if the frame is still in history.
        """
        if not self._spans:
            return None
        local_frame = max(0, int(render_elapsed_s * self.audio_format.sample_rate))
        for span in reversed(self._spans):
            if local_frame < span.local_start_frame:
                continue
            if local_frame < span.local_end_frame:
                return OutputTapResolvedFrame(
                    output_stream_epoch=span.output_stream_epoch,
                    output_frame=span.output_start_frame + (local_frame - span.local_start_frame),
                    local_frame=local_frame,
                    sample_rate=span.sample_rate,
                    output_monotonic_ns=span.output_monotonic_ns,
                    published_monotonic_ns=span.published_monotonic_ns,
                )
        last_span = self._spans[-1]
        if local_frame == last_span.local_end_frame:
            return OutputTapResolvedFrame(
                output_stream_epoch=last_span.output_stream_epoch,
                output_frame=last_span.output_start_frame
                + (last_span.local_end_frame - last_span.local_start_frame),
                local_frame=local_frame,
                sample_rate=last_span.sample_rate,
                output_monotonic_ns=last_span.output_monotonic_ns,
                published_monotonic_ns=last_span.published_monotonic_ns,
            )
        return None

    def _trim_history(self) -> None:
        """Keep only the recent frame mapping history needed for live anchors."""
        min_frame = max(
            0,
            self.total_frames_received - (self.audio_format.sample_rate * _RETENTION_SECONDS),
        )
        while self._spans and self._spans[0].local_end_frame < min_frame:
            self._spans.popleft()

    def _validate_packet(self, packet: OutputTapPacket) -> None:
        """Validate one packet against the expected PCM format."""
        if packet.sample_rate != self.audio_format.sample_rate:
            raise AudioError(
                f"output_tap sample rate mismatch: expected {self.audio_format.sample_rate}, "
                f"got {packet.sample_rate}"
            )
        if packet.channels != self.audio_format.channels:
            raise AudioError(
                f"output_tap channel mismatch: expected {self.audio_format.channels}, "
                f"got {packet.channels}"
            )
        if packet.frame_size != self.frame_size:
            raise AudioError(
                f"output_tap frame size mismatch: expected {self.frame_size}, got {packet.frame_size}"
            )
        if packet.sample_format != self.sample_format:
            raise AudioError(
                f"output_tap sample format mismatch: expected {self.sample_format}, "
                f"got {packet.sample_format}"
            )
        if packet.frame_count * packet.frame_size != packet.pcm_bytes:
            raise AudioError("output_tap packet is not frame-aligned")


def _content_type_to_sample_format(content_type: ContentType) -> str:
    """Translate MA PCM content type to the output_tap sample_format string."""
    sample_formats = {
        ContentType.PCM_S16LE: "s16le",
        ContentType.PCM_S24LE: "s24le",
        ContentType.PCM_S32LE: "s32le",
        ContentType.PCM_F32LE: "f32le",
        ContentType.PCM_F64LE: "f64le",
    }
    if content_type not in sample_formats:
        raise AudioError(f"Unsupported output_tap PCM format: {content_type}")
    return sample_formats[content_type]
