"""Tests for music-play output_tap frame mapping."""

from __future__ import annotations

import asyncio
import json
import struct
from unittest.mock import MagicMock

import pytest
from music_assistant_models.enums import ContentType
from music_assistant_models.errors import AudioError
from music_assistant_models.media_items import AudioFormat

from music_assistant.providers.music_play_tap import MusicPlayTapProvider
from music_assistant.providers.music_play_tap.output_tap import (
    OutputTapPacket,
    OutputTapStreamSession,
    read_output_tap_packet,
)

_PACKET_PREFIX = struct.Struct("!4sII")


def _audio_format() -> AudioFormat:
    """Build the PCM format used by output_tap."""
    return AudioFormat(
        content_type=ContentType.PCM_S16LE,
        codec_type=ContentType.PCM_S16LE,
        sample_rate=44100,
        bit_depth=16,
        channels=2,
    )


def _packet_bytes(metadata: dict[str, object], pcm: bytes) -> bytes:
    """Encode a raw output_tap packet for the reader test."""
    metadata_bytes = json.dumps(
        metadata,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return _PACKET_PREFIX.pack(b"OTAP", len(metadata_bytes), len(pcm)) + metadata_bytes + pcm


def _make_provider() -> MusicPlayTapProvider:
    """Create a minimal provider instance for unit tests."""
    mass = MagicMock()
    mass.cache = MagicMock()

    config_values = {
        "log_level": "GLOBAL",
        "tap_host": "127.0.0.1",
        "tap_port": 8766,
        "source_name": "music-play Output Tap",
    }
    config = MagicMock()
    config.get_value.side_effect = config_values.get
    config.instance_id = "music_play_tap_test"
    config.name = "music-play Output Tap"

    manifest = MagicMock()
    manifest.domain = "music_play_tap"

    return MusicPlayTapProvider(mass, manifest, config)


@pytest.mark.asyncio
async def test_read_output_tap_packet() -> None:
    """Reader should parse one complete output_tap packet."""
    pcm = b"\x00" * 16
    metadata = {
        "version": 1,
        "event": "output_pcm",
        "output_stream_epoch": "epoch-a",
        "output_frame": 0,
        "frame_count": 4,
        "sample_rate": 44100,
        "channels": 2,
        "sample_format": "s16le",
        "frame_size": 4,
        "pcm_bytes": len(pcm),
        "output_monotonic_ns": 100,
        "published_monotonic_ns": 120,
    }
    reader = asyncio.StreamReader()
    reader.feed_data(_packet_bytes(metadata, pcm))
    reader.feed_eof()

    packet = await read_output_tap_packet(reader)

    assert packet.output_stream_epoch == "epoch-a"
    assert packet.frame_count == 4
    assert packet.pcm == pcm


@pytest.mark.asyncio
async def test_provider_exposes_initiable_audio_source() -> None:
    """The output_tap source should be browsable/playable from the MA UI."""
    provider = _make_provider()

    sources = await provider.get_audio_sources()

    assert len(sources) == 1
    assert sources[0].can_initiate is True
    assert sources[0].allow_external_trigger is False


def test_output_tap_session_tracks_epoch_boundaries() -> None:
    """Frame mapping should survive an output_stream_epoch change mid-stream."""
    session = OutputTapStreamSession(_audio_format())
    pcm = b"\x00" * (4410 * 4)
    session.record_packet(
        OutputTapPacket(
            version=1,
            event="output_pcm",
            output_stream_epoch="epoch-a",
            output_frame=0,
            frame_count=4410,
            sample_rate=44100,
            channels=2,
            sample_format="s16le",
            frame_size=4,
            pcm_bytes=len(pcm),
            output_monotonic_ns=100,
            published_monotonic_ns=120,
            pcm=pcm,
        )
    )
    session.record_packet(
        OutputTapPacket(
            version=1,
            event="output_pcm",
            output_stream_epoch="epoch-a",
            output_frame=4410,
            frame_count=4410,
            sample_rate=44100,
            channels=2,
            sample_format="s16le",
            frame_size=4,
            pcm_bytes=len(pcm),
            output_monotonic_ns=200,
            published_monotonic_ns=220,
            pcm=pcm,
        )
    )
    session.record_packet(
        OutputTapPacket(
            version=1,
            event="output_pcm",
            output_stream_epoch="epoch-b",
            output_frame=0,
            frame_count=4410,
            sample_rate=44100,
            channels=2,
            sample_format="s16le",
            frame_size=4,
            pcm_bytes=len(pcm),
            output_monotonic_ns=300,
            published_monotonic_ns=320,
            pcm=pcm,
        )
    )

    first_epoch = session.resolve_output_frame(0.15)
    second_epoch = session.resolve_output_frame(0.25)

    assert first_epoch is not None
    assert first_epoch.output_stream_epoch == "epoch-a"
    assert first_epoch.output_frame == 6615
    assert second_epoch is not None
    assert second_epoch.output_stream_epoch == "epoch-b"
    assert second_epoch.output_frame == 2205


def test_output_tap_session_rejects_format_mismatch() -> None:
    """Unexpected PCM metadata should fail fast."""
    session = OutputTapStreamSession(_audio_format())
    with pytest.raises(AudioError):
        session.record_packet(
            OutputTapPacket(
                version=1,
                event="output_pcm",
                output_stream_epoch="epoch-a",
                output_frame=0,
                frame_count=1,
                sample_rate=48000,
                channels=2,
                sample_format="s16le",
                frame_size=4,
                pcm_bytes=4,
                output_monotonic_ns=100,
                published_monotonic_ns=120,
                pcm=b"\x00\x00\x00\x00",
            )
        )
