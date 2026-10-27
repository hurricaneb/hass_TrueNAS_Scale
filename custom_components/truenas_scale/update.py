"""Update platform for TrueNAS Scale integration."""

import asyncio
import logging
from typing import Any, Optional

from homeassistant.components.update import (
    UpdateEntity,
    UpdateEntityFeature,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DATA_APPS, DATA_SYSTEM_INFO, DOMAIN

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up TrueNAS Scale update entities."""
    data = hass.data[DOMAIN][entry.entry_id]
    coordinator = data["coordinator"]

    entities = []

    # App updates
    apps = coordinator.data.get(DATA_APPS, {})
    for app_name in apps:
        entities.append(TrueNASAppUpdateEntity(coordinator, entry, app_name))

    async_add_entities(entities)


class TrueNASAppUpdateEntity(CoordinatorEntity, UpdateEntity):
    """Update entity for TrueNAS applications."""

    _attr_has_entity_name = True

    def __init__(self, coordinator, entry: ConfigEntry, app_name: str) -> None:
        super().__init__(coordinator)
        self.entry = entry
        self.app_name = app_name
        display_name = self._get_display_name(coordinator, app_name)
        self._attr_name = display_name
        # "title" is the software's own name, as opposed to the device/entity
        # name ("TrueNAS (host)"). The frontend (update overview, dialogs and
        # notifications) shows this instead of the entity name when set.
        self._attr_title = display_name
        self._attr_unique_id = f"{entry.entry_id}_app_update_{app_name}"
        self._upgrade_task = None
        self._attr_supported_features = UpdateEntityFeature.INSTALL | UpdateEntityFeature.PROGRESS

    @staticmethod
    def _get_display_name(coordinator, app_name: str) -> str:
        """Return a human-friendly app name, preferring catalog metadata."""
        apps = coordinator.data.get(DATA_APPS, {})
        app = apps.get(app_name, {})
        metadata = app.get("metadata")
        title = app.get("title") or (
            metadata.get("title") if isinstance(metadata, dict) else None
        )
        if isinstance(title, str) and title.strip():
            return title.strip()
        return app_name.replace("_", " ").title()

    @property
    def device_info(self) -> DeviceInfo:
        sys_info = self.coordinator.data.get(DATA_SYSTEM_INFO, {})
        hostname = sys_info.get("hostname", self.entry.data["host"])
        return {
            "identifiers": {(DOMAIN, self.entry.entry_id)},
            "name": f"TrueNAS ({hostname})",
            "manufacturer": "iXsystems",
            "model": sys_info.get("model", "TrueNAS Scale"),
            "sw_version": sys_info.get("version", "26.0"),
        }

    @property
    def icon(self) -> str:
        """Return default icon for app updates."""
        return "mdi:cube-outline"

    @property
    def entity_picture(self) -> Optional[str]:
        """Return app logo URL if available."""
        apps = self.coordinator.data.get(DATA_APPS, {})
        app = apps.get(self.app_name, {})
        icon_url = app.get("icon_url") or app.get("icon") or app.get("logo")
        if not icon_url and isinstance(app.get("metadata"), dict):
            icon_url = app["metadata"].get("icon")
        if isinstance(icon_url, str) and (icon_url.startswith("http://") or icon_url.startswith("https://")):
            return icon_url
        return None

    @property
    def installed_version(self) -> Optional[str]:
        apps = self.coordinator.data.get(DATA_APPS, {})
        app = apps.get(self.app_name, {})
        return app.get("version") or app.get("human_version")

    @property
    def latest_version(self) -> Optional[str]:
        apps = self.coordinator.data.get(DATA_APPS, {})
        app = apps.get(self.app_name, {})
        upgrade_avail = app.get("upgrade_available") is True or app.get("upgrade") is True
        upgrade_version = (
            app.get("upgrade_available_version")
            or app.get("available_version")
            or app.get("latest_version")
            or app.get("human_available_version")
            or app.get("catalog_version")
        )
        inst = self.installed_version

        if upgrade_version and str(upgrade_version) != str(inst):
            return str(upgrade_version)

        if upgrade_avail:
            return f"{inst} (Update)" if inst else "Update Available"

        return inst

    @property
    def release_summary(self) -> Optional[str]:
        apps = self.coordinator.data.get(DATA_APPS, {})
        app = apps.get(self.app_name, {})
        latest = self.latest_version
        if app.get("upgrade_available") is True or latest != self.installed_version:
            return f"An update for {self._attr_title} is available in TrueNAS SCALE."
        return None

    @property
    def in_progress(self) -> bool:
        """Track this app's job independently of HA's service-call lifecycle."""
        return self._upgrade_task is not None and not self._upgrade_task.done()

    async def async_install(self, version: Optional[str], backup: bool, **kwargs: Any) -> None:
        """Submit a tracked task; caller cancellation must not cancel the job watcher."""
        if self.in_progress:
            raise HomeAssistantError("An upgrade is already in progress for this app")
        self._upgrade_task = self.entry.async_create_background_task(
            self.hass, self._async_watch_upgrade(version),
            f"TrueNAS upgrade {self.app_name}",
        )
        self.async_write_ha_state()
        await asyncio.shield(self._upgrade_task)

    async def _async_watch_upgrade(self, version: Optional[str]) -> None:
        """Keep progress active until the TrueNAS job succeeds or fails."""
        _LOGGER.info("Installing update for TrueNAS App '%s'", self.app_name)
        try:
            await self.coordinator.client.async_upgrade_app(self.app_name, version)
        except Exception as err:
            _LOGGER.error("Failed to upgrade TrueNAS App '%s': %s", self.app_name, err)
            raise HomeAssistantError(f"Could not upgrade {self._attr_title}: {err}") from err
        finally:
            self._upgrade_task = None
            self.async_write_ha_state()
            await self.coordinator.async_request_refresh()
