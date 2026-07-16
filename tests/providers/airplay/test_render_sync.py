"""Tests for AirPlay render-sync payload publication."""

from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from music_assistant_models.enums import ContentType, MediaType, PlaybackState, StreamType
from music_assistant_models.media_items import AudioFormat
from music_assistant_models.streamdetails import StreamDetails

from music_assistant.providers.airplay.constants import StreamingProtocol
from music_assistant.providers.airplay.provider import AirPlayProvider
from music_assistant.providers.music_play_tap.output_tap import (
    OutputTapPacket,
    OutputTapStreamSession,
)


def _audio_format() -> AudioFormat:
    """Build the PCM format used by output_tap."""
    return AudioFormat(
        content_type=ContentType.PCM_S16LE,
        codec_type=ContentType.PCM_S16LE,
        sample_rate=44100,
        bit_depth=16,
        channels=2,
    )


def test_publish_render_sync_anchor_resolves_output_frame() -> None:
    """AirPlay anchors should publish output_tap coordinates over MQTT."""
    tap_session = OutputTapStreamSession(_audio_format())
    pcm = b"\x00" * (4410 * 4)
    tap_session.record_packet(
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
            output_monotonic_ns=123456,
            published_monotonic_ns=123789,
            pcm=pcm,
        )
    )
    tap_session.record_packet(
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
            output_monotonic_ns=223456,
            published_monotonic_ns=223789,
            pcm=pcm,
        )
    )
    streamdetails = StreamDetails(
        provider="music_play_tap",
        item_id="main",
        audio_format=_audio_format(),
        media_type=MediaType.AUDIO_SOURCE,
        stream_type=StreamType.CUSTOM,
        data=tap_session,
    )
    active_queue = SimpleNamespace(current_item=SimpleNamespace(streamdetails=streamdetails))
    mass = MagicMock()
    mass.players.get_active_queue = MagicMock(return_value=active_queue)

    provider = AirPlayProvider.__new__(AirPlayProvider)
    provider.mass = mass
    provider.logger = logging.getLogger("tests.airplay.render_sync")
    provider._render_sync_publisher = MagicMock(enabled=True)

    player = MagicMock()
    player.player_id = "ap001122"
    player.protocol = StreamingProtocol.RAOP
    player.state.playback_state = PlaybackState.PLAYING
    player.config.get_value = MagicMock(return_value=25)
    protocol = SimpleNamespace(
        player=player,
        session=SimpleNamespace(start_ntp=987654321, wait_start=0.9),
    )

    with patch("music_assistant.providers.airplay.provider.time.monotonic_ns", return_value=555):
        provider.publish_render_sync_anchor(
            protocol,
            source="raop.elapsed",
            state=PlaybackState.PLAYING,
            elapsed_time=0.15,
            anchor_ts=200.0,
        )

    provider._render_sync_publisher.publish.assert_called_once()
    payload = provider._render_sync_publisher.publish.call_args.args[0]
    assert payload["event"] == "render_anchor"
    assert payload["source"] == "raop.elapsed"
    assert payload["output_stream_epoch"] == "epoch-a"
    assert payload["render_output_frame"] == 6615
    assert payload["sample_rate"] == 44100
    assert payload["wait_start_ms"] == 900
    assert payload["sync_adjust_ms"] == 25
    assert payload["anchor_monotonic_ns"] == 555
