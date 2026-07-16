"""Unit tests for AirPlay protocol timeline anchor handling."""

import logging
from collections.abc import AsyncIterator
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest
from music_assistant_models.enums import PlaybackState

from music_assistant.providers.airplay.constants import StreamingProtocol
from music_assistant.providers.airplay.protocols.airplay2 import AirPlay2Stream
from music_assistant.providers.airplay.protocols.raop import RaopStream


class _FakeCliProcess:
    """Minimal stderr-only process stub for protocol reader tests."""

    closed = False

    def __init__(self, lines: list[str]) -> None:
        """Initialize the fake stderr source."""
        self._lines = lines

    async def iter_stderr(self) -> AsyncIterator[str]:
        """Yield configured stderr lines."""
        for line in self._lines:
            yield line


def _make_protocol_player(protocol: StreamingProtocol) -> MagicMock:
    """Create a mock player with the fields required by AirPlayProtocol."""
    provider = MagicMock()
    provider.mass = MagicMock()
    provider.logger = logging.getLogger(f"tests.airplay.{protocol.value}")

    player = MagicMock()
    player.provider = provider
    player.player_id = "ap001122334455"
    player.display_name = "Test Player"
    player.name = "Test Player"
    player.logger = logging.getLogger(f"tests.airplay.{protocol.value}.player")
    player.device_info = SimpleNamespace(mac_address="00:11:22:33:44:55")
    player.protocol = protocol
    player.corrected_elapsed_time = None
    player.set_state_from_stream = MagicMock()
    player.session_establishment_latency_ms = 1500
    return player


@pytest.mark.asyncio
async def test_raop_reader_emits_session_aligned_timeline_anchor() -> None:
    """RAOP progress lines should carry the matching wall-clock anchor timestamp."""
    player = _make_protocol_player(StreamingProtocol.RAOP)
    stream = RaopStream(player)
    player.stream = stream
    stream.session = cast("Any", SimpleNamespace(start_time=95.0, start_ntp=123456789))
    stream._cli_proc = cast(
        "Any",
        _FakeCliProcess(
        [
            "connected to 10.0.0.5",
            "restarting w/o pause",
            "elapsed milliseconds: 2000",
            "end of stream reached",
        ]
    )
    )

    with patch(
        "music_assistant.providers.airplay.protocols.raop.time.time",
        side_effect=[100.0, 100.5],
    ):
        await stream._stderr_reader()

    call_args = player.set_state_from_stream.call_args_list
    assert call_args[0].kwargs["state"] == PlaybackState.PLAYING
    assert call_args[0].kwargs["elapsed_time"] == pytest.approx(5.0)
    assert call_args[0].kwargs["elapsed_time_last_updated"] == pytest.approx(100.0)
    assert call_args[0].kwargs["stream"] is stream

    assert call_args[1].kwargs["elapsed_time"] == pytest.approx(5.5)
    assert call_args[1].kwargs["elapsed_time_last_updated"] == pytest.approx(100.5)
    assert call_args[1].kwargs["stream"] is stream


@pytest.mark.asyncio
async def test_airplay2_reader_emits_session_aligned_start_anchor() -> None:
    """AirPlay 2 start events should be forwarded with an explicit anchor timestamp."""
    player = _make_protocol_player(StreamingProtocol.AIRPLAY2)
    stream = AirPlay2Stream(player)
    player.stream = stream
    stream.session = cast("Any", SimpleNamespace(start_time=95.0, start_ntp=987654321))
    stream._cli_proc = cast(
        "Any",
        _FakeCliProcess(
        [
            "main: DACP ID set to: 0123456789ABCDEF",
            "player: Callback from AirPlay 2 device Test Player to device_activate_cb (status 2)",
            "Starting at 0",
            "end of stream reached",
        ]
    )
    )

    with patch(
        "music_assistant.providers.airplay.protocols.airplay2.time.time",
        side_effect=[98.0, 99.0, 100.0],
    ):
        await stream._stderr_reader()

    call_args = player.set_state_from_stream.call_args_list
    assert call_args[0].kwargs["state"] == PlaybackState.PLAYING
    assert call_args[0].kwargs["elapsed_time"] == pytest.approx(5.0)
    assert call_args[0].kwargs["elapsed_time_last_updated"] == pytest.approx(100.0)
    assert call_args[0].kwargs["stream"] is stream
