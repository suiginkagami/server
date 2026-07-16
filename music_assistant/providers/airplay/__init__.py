"""AirPlay Player provider for Music Assistant."""

from __future__ import annotations

from typing import TYPE_CHECKING

from music_assistant_models.config_entries import ConfigEntry
from music_assistant_models.enums import ConfigEntryType, ProviderFeature
from music_assistant_models.provider import ProviderManifest

from music_assistant.mass import MusicAssistant

from .constants import (
    CONF_RENDER_SYNC_MQTT_ENABLED,
    CONF_RENDER_SYNC_MQTT_HOST,
    CONF_RENDER_SYNC_MQTT_PASSWORD,
    CONF_RENDER_SYNC_MQTT_PORT,
    CONF_RENDER_SYNC_MQTT_TOPIC,
    CONF_RENDER_SYNC_MQTT_USERNAME,
)
from .provider import AirPlayProvider

if TYPE_CHECKING:
    from music_assistant_models.config_entries import ConfigValueType, ProviderConfig
    from music_assistant_models.provider import ProviderManifest

    from music_assistant.models import ProviderInstanceType

SUPPORTED_FEATURES = {
    ProviderFeature.SYNC_PLAYERS,
}


async def get_config_entries(
    mass: MusicAssistant,
    instance_id: str | None = None,
    action: str | None = None,
    values: dict[str, ConfigValueType] | None = None,
) -> tuple[ConfigEntry, ...]:
    """
    Return Config entries to setup this provider.

    instance_id: id of an existing provider instance (None if new instance setup).
    action: [optional] action key called from config entries UI.
    values: the (intermediate) raw values for config entries sent with the action.
    """
    del mass, instance_id, action, values
    return (
        ConfigEntry(
            key=CONF_RENDER_SYNC_MQTT_ENABLED,
            type=ConfigEntryType.BOOLEAN,
            label="Publish render-sync anchors over MQTT",
            default_value=False,
            description="Publish AirPlay render anchors for output_tap playback so external displays can align to the rendered audio timeline.",
            advanced=True,
        ),
        ConfigEntry(
            key=CONF_RENDER_SYNC_MQTT_HOST,
            type=ConfigEntryType.STRING,
            label="MQTT host",
            default_value="127.0.0.1",
            depends_on=CONF_RENDER_SYNC_MQTT_ENABLED,
            depends_on_value=True,
            advanced=True,
        ),
        ConfigEntry(
            key=CONF_RENDER_SYNC_MQTT_PORT,
            type=ConfigEntryType.INTEGER,
            label="MQTT port",
            default_value=1883,
            range=(1, 65535),
            depends_on=CONF_RENDER_SYNC_MQTT_ENABLED,
            depends_on_value=True,
            advanced=True,
        ),
        ConfigEntry(
            key=CONF_RENDER_SYNC_MQTT_TOPIC,
            type=ConfigEntryType.STRING,
            label="MQTT topic",
            default_value="music-assistant/render-sync",
            depends_on=CONF_RENDER_SYNC_MQTT_ENABLED,
            depends_on_value=True,
            advanced=True,
        ),
        ConfigEntry(
            key=CONF_RENDER_SYNC_MQTT_USERNAME,
            type=ConfigEntryType.STRING,
            label="MQTT username",
            default_value="",
            depends_on=CONF_RENDER_SYNC_MQTT_ENABLED,
            depends_on_value=True,
            advanced=True,
        ),
        ConfigEntry(
            key=CONF_RENDER_SYNC_MQTT_PASSWORD,
            type=ConfigEntryType.SECURE_STRING,
            label="MQTT password",
            default_value="",
            depends_on=CONF_RENDER_SYNC_MQTT_ENABLED,
            depends_on_value=True,
            advanced=True,
        ),
    )


async def setup(
    mass: MusicAssistant, manifest: ProviderManifest, config: ProviderConfig
) -> ProviderInstanceType:
    """Initialize provider(instance) with given configuration."""
    return AirPlayProvider(mass, manifest, config, SUPPORTED_FEATURES)
