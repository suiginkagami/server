"""music-play output_tap plugin for Music Assistant."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import TYPE_CHECKING, cast

from music_assistant_models.config_entries import ConfigEntry
from music_assistant_models.enums import (
    ConfigEntryType,
    ContentType,
    MediaType,
    ProviderFeature,
    StreamType,
)
from music_assistant_models.errors import AudioError, MediaNotFoundError
from music_assistant_models.media_items import AudioFormat, AudioSource, ProviderMapping
from music_assistant_models.streamdetails import StreamDetails, StreamMetadata

from music_assistant.models.plugin import PluginProvider

from .output_tap import OutputTapStreamSession, read_output_tap_packet

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from music_assistant_models.config_entries import ConfigValueType, ProviderConfig
    from music_assistant_models.enums import SourceControl
    from music_assistant_models.provider import ProviderManifest

    from music_assistant.mass import MusicAssistant
    from music_assistant.models import ProviderInstanceType

CONF_TAP_HOST = "tap_host"
CONF_TAP_PORT = "tap_port"
CONF_SOURCE_NAME = "source_name"

DEFAULT_TAP_HOST = "127.0.0.1"
DEFAULT_TAP_PORT = 8766
AUDIO_SOURCE_ID = "main"
SUPPORTED_FEATURES = {ProviderFeature.AUDIO_SOURCE}


async def setup(
    mass: MusicAssistant, manifest: ProviderManifest, config: ProviderConfig
) -> ProviderInstanceType:
    """Initialize provider(instance) with given configuration."""
    return MusicPlayTapProvider(mass, manifest, config)


async def get_config_entries(
    mass: MusicAssistant,
    instance_id: str | None = None,  # noqa: ARG001
    action: str | None = None,  # noqa: ARG001
    values: dict[str, ConfigValueType] | None = None,  # noqa: ARG001
) -> tuple[ConfigEntry, ...]:
    """
    Return Config entries to setup this provider.

    instance_id: id of an existing provider instance (None if new instance setup).
    action: [optional] action key called from config entries UI.
    values: the (intermediate) raw values for config entries sent with the action.
    """
    del mass
    return (
        ConfigEntry(
            key=CONF_TAP_HOST,
            type=ConfigEntryType.STRING,
            label="output_tap host",
            default_value=DEFAULT_TAP_HOST,
            description="Host or IP address exposing the music-play output_tap TCP server.",
        ),
        ConfigEntry(
            key=CONF_TAP_PORT,
            type=ConfigEntryType.INTEGER,
            label="output_tap port",
            default_value=DEFAULT_TAP_PORT,
            range=(1, 65535),
            description="TCP port exposed by the music-play output_tap server.",
        ),
        ConfigEntry(
            key=CONF_SOURCE_NAME,
            type=ConfigEntryType.STRING,
            label="Source name",
            default_value="music-play Output Tap",
            description="Name shown under Music Assistant Live Inputs.",
        ),
    )


class MusicPlayTapProvider(PluginProvider):
    """Expose music-play output_tap as a MA AudioSource."""

    def __init__(
        self, mass: MusicAssistant, manifest: ProviderManifest, config: ProviderConfig
    ) -> None:
        """Initialize the provider instance."""
        super().__init__(mass, manifest, config, SUPPORTED_FEATURES)
        self._tap_host = cast("str", self.config.get_value(CONF_TAP_HOST)) or DEFAULT_TAP_HOST
        self._tap_port = int(cast("int", self.config.get_value(CONF_TAP_PORT)) or DEFAULT_TAP_PORT)
        source_name = (
            cast("str", self.config.get_value(CONF_SOURCE_NAME)) or "music-play Output Tap"
        )
        self._audio_format = AudioFormat(
            content_type=ContentType.PCM_S16LE,
            codec_type=ContentType.PCM_S16LE,
            sample_rate=44100,
            bit_depth=16,
            channels=2,
        )
        self._stream_metadata = StreamMetadata(
            title=source_name,
            description=f"music-play output_tap @ {self._tap_host}:{self._tap_port}",
        )
        self._audio_source = AudioSource(
            item_id=AUDIO_SOURCE_ID,
            provider=self.instance_id,
            name=source_name,
            provider_mappings={
                ProviderMapping(
                    item_id=AUDIO_SOURCE_ID,
                    provider_domain=self.domain,
                    provider_instance=self.instance_id,
                    audio_format=self._audio_format,
                )
            },
            can_play_pause=False,
            can_seek=False,
            can_next_previous=False,
            exclusive=True,
            allow_external_trigger=False,
            can_initiate=False,
        )
        self._in_use_by_queue: str | None = None
        self._active_session_id: str | None = None
        self._active_player_id: str | None = None

    async def get_audio_sources(self) -> list[AudioSource]:
        """Return the AudioSources this plugin currently exposes."""
        return [self._audio_source]

    async def get_stream_details(self, source_id: str, queue_id: str) -> StreamDetails:
        """Return StreamDetails for streaming music-play output_tap."""
        del queue_id
        if source_id != AUDIO_SOURCE_ID:
            raise MediaNotFoundError(f"Unknown AudioSource: {source_id}")
        return StreamDetails(
            provider=self.instance_id,
            item_id=source_id,
            audio_format=self._audio_format,
            media_type=MediaType.AUDIO_SOURCE,
            stream_type=StreamType.CUSTOM,
            stream_metadata=self._stream_metadata,
            data=OutputTapStreamSession(self._audio_format),
        )

    async def get_audio_stream(
        self, streamdetails: StreamDetails, seek_position: int = 0
    ) -> AsyncGenerator[bytes]:
        """Return the live PCM stream from music-play output_tap."""
        del seek_position
        streamdetails.data = session = OutputTapStreamSession(self._audio_format)
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(self._tap_host, self._tap_port),
                timeout=5,
            )
        except (OSError, TimeoutError) as err:
            raise AudioError(
                f"Unable to connect to music-play output_tap at {self._tap_host}:{self._tap_port}: {err}"
            ) from err
        try:
            while True:
                packet = await read_output_tap_packet(reader)
                session.record_packet(packet)
                yield packet.pcm
        except asyncio.IncompleteReadError:
            self.logger.info(
                "music-play output_tap connection closed on %s:%s",
                self._tap_host,
                self._tap_port,
            )
        finally:
            writer.close()
            with suppress(OSError):
                await writer.wait_closed()

    async def on_source_control(
        self,
        source_id: str,
        action: SourceControl,
        value: int | None = None,
    ) -> None:
        """Handle control commands for the output_tap source."""
        del source_id, action, value

    async def on_source_selected(
        self,
        source_id: str,
        player_id: str,
        queue_id: str,
        stream_session_id: str,
    ) -> None:
        """Claim the source for the queue currently streaming it."""
        del player_id
        if source_id != AUDIO_SOURCE_ID:
            return
        active_player_id = queue_id
        if self._active_player_id and self._active_player_id != active_player_id:
            prev_player_id = self._active_player_id
            self.logger.info(
                "music-play output_tap moved from %s to %s",
                prev_player_id,
                active_player_id,
            )
            try:
                await self.mass.players.cmd_stop(prev_player_id)
            except Exception as err:
                self.logger.debug(
                    "Failed to stop previous output_tap player %s: %s",
                    prev_player_id,
                    err,
                )
        self._in_use_by_queue = queue_id
        self._active_session_id = stream_session_id
        self._active_player_id = active_player_id

    async def on_source_unselected(
        self, source_id: str, queue_id: str, stream_session_id: str
    ) -> None:
        """Release the queue-scoped exclusive claim when MA tears down the stream."""
        if source_id != AUDIO_SOURCE_ID or self._active_session_id != stream_session_id:
            return
        self._active_session_id = None
        if self._in_use_by_queue == queue_id:
            self._in_use_by_queue = None
        if self._active_player_id == queue_id:
            self._active_player_id = None
