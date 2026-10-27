"""Tests for TrueNAS Scale update platform."""

import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock

from homeassistant.components.update import UpdateEntityFeature
from homeassistant.exceptions import HomeAssistantError

from custom_components.truenas_scale.const import DATA_APPS, DATA_SYSTEM_INFO, DOMAIN
from custom_components.truenas_scale.update import (
    TrueNASAppUpdateEntity,
    async_setup_entry,
)


class TestTrueNASUpdateEntities(unittest.IsolatedAsyncioTestCase):
    """Test Update Entities."""

    def setUp(self):
        self.coordinator = MagicMock()
        self.coordinator.client = MagicMock()
        self.coordinator.client.async_upgrade_app = AsyncMock()
        self.coordinator.async_request_refresh = AsyncMock()
        self.coordinator.data = {
            DATA_APPS: {
                "plex": {
                    "name": "plex",
                    "version": "1.32.0",
                    "human_version": "1.32.0",
                    "upgrade_available": True,
                    "available_version": "1.33.0",
                    "icon_url": "https://plex.tv/logo.png",
                }
            }
        }
        self.entry = MagicMock()
        self.entry.async_create_background_task.side_effect = (
            lambda hass, coro, name: asyncio.create_task(coro, name=name)
        )
        self.entry.entry_id = "test_entry"
        self.entry.data = {"host": "192.168.1.100"}

    def test_app_update_entity(self):
        update_entity = TrueNASAppUpdateEntity(self.coordinator, self.entry, "plex")

        self.assertEqual(update_entity.installed_version, "1.32.0")
        self.assertEqual(update_entity.latest_version, "1.33.0")
        self.assertEqual(update_entity.entity_picture, "https://plex.tv/logo.png")

    async def test_app_update_install(self):
        update_entity = TrueNASAppUpdateEntity(self.coordinator, self.entry, "plex")
        update_entity.async_write_ha_state = MagicMock()
        await update_entity.async_install(version="1.33.0", backup=False)

        self.coordinator.client.async_upgrade_app.assert_called_with("plex", "1.33.0")
        self.assertTrue(self.coordinator.async_request_refresh.called)

    def test_display_name_falls_back_to_slug(self):
        # No "title" anywhere in the app data: fall back to a title-cased
        # version of the app's slug/name.
        update_entity = TrueNASAppUpdateEntity(self.coordinator, self.entry, "plex")

        self.assertEqual(update_entity.name, "Plex")
        self.assertEqual(update_entity._attr_title, "Plex")

    def test_display_name_prefers_catalog_metadata_title(self):
        # TrueNAS catalog apps commonly expose a nicer name under
        # metadata.title (e.g. "Plex Media Server" instead of "plex").
        self.coordinator.data[DATA_APPS]["plex"]["metadata"] = {
            "title": "Plex Media Server"
        }
        update_entity = TrueNASAppUpdateEntity(self.coordinator, self.entry, "plex")

        self.assertEqual(update_entity.name, "Plex Media Server")
        self.assertEqual(update_entity._attr_title, "Plex Media Server")

    def test_display_name_prefers_top_level_title_over_metadata(self):
        self.coordinator.data[DATA_APPS]["plex"]["title"] = "Plex (top-level)"
        self.coordinator.data[DATA_APPS]["plex"]["metadata"] = {
            "title": "Plex Media Server"
        }
        update_entity = TrueNASAppUpdateEntity(self.coordinator, self.entry, "plex")

        self.assertEqual(update_entity.name, "Plex (top-level)")

    def test_title_attribute_used_by_frontend_distinguishes_from_device_name(self):
        # UpdateEntity.title is what the frontend shows instead of the
        # entity/device name; it must reflect the app, not "TrueNAS".
        update_entity = TrueNASAppUpdateEntity(self.coordinator, self.entry, "plex")

        self.assertEqual(update_entity.title, "Plex")
        self.assertNotIn("TrueNAS", update_entity.title)

    def test_release_summary_uses_app_display_name(self):
        update_entity = TrueNASAppUpdateEntity(self.coordinator, self.entry, "plex")

        self.assertEqual(
            update_entity.release_summary,
            "An update for Plex is available in TrueNAS SCALE.",
        )

    def test_release_summary_none_when_up_to_date(self):
        self.coordinator.data[DATA_APPS]["plex"]["upgrade_available"] = False
        self.coordinator.data[DATA_APPS]["plex"]["available_version"] = "1.32.0"
        update_entity = TrueNASAppUpdateEntity(self.coordinator, self.entry, "plex")

        self.assertIsNone(update_entity.release_summary)

    def test_supported_features_does_not_claim_release_notes(self):
        # release_notes()/async_release_notes() are not implemented, so the
        # RELEASE_NOTES feature must not be advertised (the base UpdateEntity
        # raises NotImplementedError if the frontend requests them).
        update_entity = TrueNASAppUpdateEntity(self.coordinator, self.entry, "plex")

        self.assertEqual(update_entity.supported_features, UpdateEntityFeature.INSTALL | UpdateEntityFeature.PROGRESS)

    def test_icon(self):
        update_entity = TrueNASAppUpdateEntity(self.coordinator, self.entry, "plex")
        self.assertEqual(update_entity.icon, "mdi:cube-outline")

    def test_device_info(self):
        self.coordinator.data[DATA_SYSTEM_INFO] = {
            "hostname": "truenas-scale",
            "model": "TRUENAS-CUSTOM",
            "version": "26.0.0",
        }
        update_entity = TrueNASAppUpdateEntity(self.coordinator, self.entry, "plex")

        info = update_entity.device_info
        self.assertEqual(info["identifiers"], {(DOMAIN, "test_entry")})
        self.assertEqual(info["name"], "TrueNAS (truenas-scale)")
        self.assertEqual(info["manufacturer"], "iXsystems")
        self.assertEqual(info["model"], "TRUENAS-CUSTOM")
        self.assertEqual(info["sw_version"], "26.0.0")

    def test_device_info_falls_back_when_system_info_missing(self):
        # No DATA_SYSTEM_INFO in coordinator.data: fall back to the config
        # entry's host and the hardcoded defaults.
        update_entity = TrueNASAppUpdateEntity(self.coordinator, self.entry, "plex")

        info = update_entity.device_info
        self.assertEqual(info["name"], "TrueNAS (192.168.1.100)")
        self.assertEqual(info["model"], "TrueNAS Scale")
        self.assertEqual(info["sw_version"], "26.0")

    def test_entity_picture_falls_back_to_metadata_icon(self):
        self.coordinator.data[DATA_APPS]["plex"].pop("icon_url", None)
        self.coordinator.data[DATA_APPS]["plex"]["metadata"] = {
            "icon": "https://plex.tv/metadata-icon.png"
        }
        update_entity = TrueNASAppUpdateEntity(self.coordinator, self.entry, "plex")

        self.assertEqual(
            update_entity.entity_picture, "https://plex.tv/metadata-icon.png"
        )

    def test_entity_picture_none_when_no_icon_available(self):
        self.coordinator.data[DATA_APPS]["plex"].pop("icon_url", None)
        update_entity = TrueNASAppUpdateEntity(self.coordinator, self.entry, "plex")

        self.assertIsNone(update_entity.entity_picture)

    def test_entity_picture_none_for_non_http_icon(self):
        self.coordinator.data[DATA_APPS]["plex"]["icon_url"] = "data:image/png;base64,xxx"
        update_entity = TrueNASAppUpdateEntity(self.coordinator, self.entry, "plex")

        self.assertIsNone(update_entity.entity_picture)

    def test_latest_version_update_available_without_explicit_version(self):
        # upgrade_available is True but no version fields are provided at
        # all: still surface something meaningful rather than nothing.
        self.coordinator.data[DATA_APPS]["plex"] = {
            "name": "plex",
            "upgrade_available": True,
        }
        update_entity = TrueNASAppUpdateEntity(self.coordinator, self.entry, "plex")

        self.assertEqual(update_entity.latest_version, "Update Available")

    async def test_async_install_error_still_refreshes_and_reraises(self):
        self.coordinator.client.async_upgrade_app = AsyncMock(
            side_effect=RuntimeError("Upgrade failed")
        )
        update_entity = TrueNASAppUpdateEntity(self.coordinator, self.entry, "plex")

        update_entity.async_write_ha_state = MagicMock()
        with self.assertRaisesRegex(HomeAssistantError, "Upgrade failed"):
            await update_entity.async_install(version=None, backup=False)

        self.assertTrue(self.coordinator.async_request_refresh.called)

    async def test_progress_is_independent_and_survives_caller_cancellation(self):
        releases = {"plex": asyncio.Event(), "sonarr": asyncio.Event()}
        async def upgrade(app, version):
            await releases[app].wait()
        self.coordinator.client.async_upgrade_app.side_effect = upgrade
        entities = [TrueNASAppUpdateEntity(self.coordinator, self.entry, name) for name in releases]
        for entity in entities:
            entity.async_write_ha_state = MagicMock()
        callers = [asyncio.create_task(entity.async_install(None, False)) for entity in entities]
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        self.assertTrue(all(entity.in_progress for entity in entities))
        callers[0].cancel()
        with self.assertRaises(asyncio.CancelledError):
            await callers[0]
        entities[0]._attr_in_progress = False  # HA's service wrapper clears this.
        self.assertTrue(entities[0].in_progress)
        with self.assertRaisesRegex(HomeAssistantError, "already in progress"):
            await entities[0].async_install(None, False)
        watcher = entities[0]._upgrade_task
        releases["plex"].set()
        await watcher
        self.assertFalse(entities[0].in_progress)
        self.assertTrue(entities[1].in_progress)
        releases["sonarr"].set()
        await callers[1]
        self.assertFalse(entities[1].in_progress)


class TestTrueNASUpdateSetupEntry(unittest.IsolatedAsyncioTestCase):
    """Test the update platform's async_setup_entry."""

    async def test_async_setup_entry_creates_entity_per_app(self):
        coordinator = MagicMock()
        coordinator.data = {
            DATA_APPS: {
                "plex": {"name": "plex"},
                "sonarr": {"name": "sonarr"},
            }
        }
        entry = MagicMock()
        entry.entry_id = "test_entry"
        hass = MagicMock()
        hass.data = {DOMAIN: {"test_entry": {"coordinator": coordinator}}}

        added_entities = []
        async_add_entities = MagicMock(side_effect=lambda entities: added_entities.extend(entities))

        await async_setup_entry(hass, entry, async_add_entities)

        self.assertEqual(len(added_entities), 2)
        self.assertTrue(async_add_entities.called)

    async def test_async_setup_entry_no_apps(self):
        coordinator = MagicMock()
        coordinator.data = {DATA_APPS: {}}
        entry = MagicMock()
        entry.entry_id = "test_entry"
        hass = MagicMock()
        hass.data = {DOMAIN: {"test_entry": {"coordinator": coordinator}}}

        async_add_entities = MagicMock()

        await async_setup_entry(hass, entry, async_add_entities)

        async_add_entities.assert_called_once_with([])
