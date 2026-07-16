"""Base protocol class for AirPlay streaming implementations."""

from __future__ import annotations

import asyncio
import time
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from music_assistant_models.enums import PlaybackState

from music_assistant.constants import VERBOSE_LOG_LEVEL
from music_assistant.helpers.images import player_image_url
from music_assistant.helpers.named_pipe import AsyncNamedPipeWriter
from music_assistant.providers.airplay.constants import AIRPLAY_PCM_FORMAT
from music_assistant.providers.airplay.helpers import generate_active_remote_id

if TYPE_CHECKING:
    from music_assistant_models.player import PlayerMedia

    from music_assistant.helpers.process import AsyncProcess
    from music_assistant.providers.airplay.player import AirPlayPlayer
    from music_assistant.providers.airplay.stream_session import AirPlayStreamSession


class AirPlayProtocol(ABC):
    """Base class for AirPlay streaming protocols (RAOP and AirPlay2).

    This class contains common logic shared between protocol implementations,
    with abstract methods for protocol-specific behavior.
    """

    _cli_proc: AsyncProcess | None  # reference to the (protocol-specific) CLI process
    session: AirPlayStreamSession | None = None  # reference to the active stream session (if any)

    # the pcm audio format used for streaming to this protocol
    pcm_format = AIRPLAY_PCM_FORMAT

    def __init__(
        self,
        player: AirPlayPlayer,
    ) -> None:
        """Initialize base AirPlay protocol.

        Args:
            player: The player to stream to
        """
        self.prov = player.provider
        self.mass = player.provider.mass
        self.player = player
        self.logger = player.provider.logger.getChild(f"protocol.{self.__class__.__name__}")
        mac_address = self.player.device_info.mac_address or self.player.player_id
        self.active_remote_id: str = generate_active_remote_id(mac_address)
        self.prevent_playback: bool = False
        self._cli_proc: AsyncProcess | None = None
        self.commands_pipe = AsyncNamedPipeWriter(
            f"/tmp/{self.player.protocol.value}-{self.player.player_id}-{self.active_remote_id}-cmd",  # noqa: S108
            owner_id=self.player.player_id,
        )
        self._stopped = False
        self._total_bytes_sent = 0
        self._stream_bytes_sent = 0
        self._connected = asyncio.Event()
        self._metadata_checksum = ""
        self._last_progress_sent: int = -1
        self._elapsed_time_offset: float | None = None
        self._cli_start_ts: float | None = None
        self._connected_ts: float | None = None

    @property
    def running(self) -> bool:
        """Return boolean if this stream is running."""
        return not self._stopped and self._cli_proc is not None and not self._cli_proc.closed

    @property
    def connected(self) -> bool:
        """Return boolean if the stream connection has been established."""
        return self._connected.is_set()

    @abstractmethod
    async def start(self, start_ntp: int) -> None:
        """Start the CLI process.

        :param start_ntp: NTP timestamp to start streaming.
        """

    def _session_elapsed_now(self, anchor_ts: float | None = None) -> float | None:
        """
        Return the session-based elapsed time for a wall-clock timestamp.

        :param anchor_ts: Unix timestamp to evaluate. Defaults to ``time.time()``.
        """
        if self.session is None:
            return None
        if anchor_ts is None:
            anchor_ts = time.time()
        return max(0.0, anchor_ts - self.session.start_time)

    def _current_elapsed_anchor(self, anchor_ts: float | None = None) -> float:
        """
        Return the best-known current elapsed time for this stream.

        :param anchor_ts: Unix timestamp used for session fallback calculations.
        """
        if (elapsed_time := self.player.corrected_elapsed_time) is not None:
            return elapsed_time
        if (session_elapsed := self._session_elapsed_now(anchor_ts)) is not None:
            return session_elapsed
        return 0.0

    def _update_timeline_anchor(
        self,
        source: str,
        *,
        state: PlaybackState | None = None,
        elapsed_time: float | None = None,
        anchor_ts: float | None = None,
    ) -> float:
        """
        Update the player's elapsed-time anchor from a CLI event.

        :param source: Short identifier for the CLI event that produced the anchor.
        :param state: Optional playback state to apply together with the anchor.
        :param elapsed_time: Optional elapsed time. Falls back to the best-known stream time.
        :param anchor_ts: Wall-clock timestamp that belongs to the anchor.
        :returns: The elapsed time that was forwarded to the player state.
        """
        if anchor_ts is None:
            anchor_ts = time.time()
        if elapsed_time is None:
            elapsed_time = self._current_elapsed_anchor(anchor_ts)
        session_elapsed = self._session_elapsed_now(anchor_ts)
        if session_elapsed is None:
            self.logger.debug(
                "AirPlay timeline anchor: player=%s protocol=%s source=%s elapsed=%.3f updated_at=%.6f",
                self.player.player_id,
                self.__class__.__name__,
                source,
                elapsed_time,
                anchor_ts,
            )
        else:
            drift = elapsed_time - session_elapsed
            log_level = VERBOSE_LOG_LEVEL if source == "raop.elapsed" else None
            log_msg = (
                "AirPlay timeline anchor: player=%s protocol=%s source=%s elapsed=%.3f "
                "session_elapsed=%.3f drift=%.3f updated_at=%.6f start_ntp=%s"
            )
            log_args = (
                self.player.player_id,
                self.__class__.__name__,
                source,
                elapsed_time,
                session_elapsed,
                drift,
                anchor_ts,
                getattr(self.session, "start_ntp", None),
            )
            if log_level is None:
                self.logger.debug(log_msg, *log_args)
            else:
                self.logger.log(log_level, log_msg, *log_args)
        self.player.set_state_from_stream(
            state=state,
            elapsed_time=elapsed_time,
            elapsed_time_last_updated=anchor_ts,
            stream=self,
        )
        self.prov.publish_render_sync_anchor(
            self,
            source=source,
            state=state,
            elapsed_time=elapsed_time,
            anchor_ts=anchor_ts,
        )
        return elapsed_time

    async def wait_for_connection(self) -> None:
        """Wait for device connection to be established."""
        if not self._cli_proc:
            return
        await asyncio.wait_for(self._connected.wait(), timeout=10)
        # repeat sending the volume level to the player because some players seem
        # to ignore it the first time
        # https://github.com/music-assistant/support/issues/3330
        volume = 0 if self.player.volume_muted else self.player.volume_level
        self.mass.call_later(2, self.send_cli_command(f"VOLUME={volume}"))
        # we also need to send the metadata after connection, because some players (e.g. Sonos)
        # simply won't start playback until they receive the metadata ?!
        # reset checksum so the resend isn't blocked by deduplication
        self._metadata_checksum = ""
        self.mass.call_later(2, self.player._on_player_media_updated)

    async def stop(self, force: bool = False) -> None:
        """
        Stop playback and cleanup.

        :param force: If True, immediately kill the process without graceful shutdown.
        """
        # Send STOP first and only flip ``_stopped`` once the write returns.
        # Flipping the flag too early lets concurrent cleanup coroutines that
        # check ``_stopped`` (e.g. ``send_cli_command`` short-circuit) assume
        # teardown is done before STOP has actually reached the CLI child.
        # Use try/finally so the flag still flips if the write raises.
        try:
            await self.send_cli_command("ACTION=STOP")
        finally:
            self._stopped = True
        await self.commands_pipe.remove()
        if force:
            # Kill immediately - skip write_eof() as it can block indefinitely
            # when the CLI stops reading from stdin after receiving STOP.
            if self._cli_proc and not self._cli_proc.closed:
                await self._cli_proc.kill()
        else:
            if self._cli_proc:
                await self._cli_proc.write_eof()
            if self._cli_proc and not self._cli_proc.closed:
                await self._cli_proc.close()
        self.player.set_state_from_stream(state=PlaybackState.IDLE, elapsed_time=0)

    async def write_audio(self, data: bytes) -> None:
        """Write raw audio data to the CLI process stdin.

        :param data: Raw audio bytes to send to the streaming process.
        """
        if self._stopped or not self._cli_proc or self._cli_proc.closed:
            return
        await self._cli_proc.write(data)

    async def write_audio_eof(self) -> None:
        """Signal end-of-stream to the CLI process stdin."""
        if self._stopped or not self._cli_proc or self._cli_proc.closed:
            return
        await self._cli_proc.write_eof()

    async def send_cli_command(self, command: str) -> None:
        """Send an interactive command to the running CLI binary."""
        if self._stopped or not self._cli_proc or self._cli_proc.closed:
            return
        if not self.commands_pipe:
            return
        self.player.last_command_sent = time.time()
        if not command.endswith("\n"):
            command += "\n"
        await self.commands_pipe.write(command.encode("utf-8"))

    async def send_metadata(self, progress: int | None, metadata: PlayerMedia | None) -> None:
        """Send metadata to player."""
        if self._stopped:
            return
        if metadata:
            duration = min(metadata.duration or 0, 3600)
            title = metadata.title or ""
            artist = metadata.artist or ""
            album = metadata.album or ""

            metadata_checksum = f"{title}|{artist}|{album}|{duration}|{metadata.image_url}"
            if metadata_checksum == self._metadata_checksum:
                return
            self._metadata_checksum = metadata_checksum

            cmd = f"TITLE={title}\nARTIST={artist}\nALBUM={album}\n"
            cmd += f"DURATION={duration}\nPROGRESS=0\nACTION=SENDMETA\n"

            await self.send_cli_command(cmd)
            self._last_progress_sent = 0
            if artwork_url := player_image_url(self.mass, metadata.image_url):
                await self.send_cli_command(f"ARTWORK={artwork_url}")
        if progress is not None and abs(progress - self._last_progress_sent) >= 2:
            self._last_progress_sent = progress
            await self.send_cli_command(f"PROGRESS={progress}")
