from __future__ import annotations

import asyncio
import json
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any

from app.core.errors import invalid_request


def utcnow_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


class VehicleContentRegistry:
    def __init__(self, storage_dir: str) -> None:
        self.root_dir = Path(storage_dir).resolve()
        self.latest_dir = self.root_dir / "vehicles" / "latest"
        self.history_dir = self.root_dir / "vehicles" / "history"
        self.lock = asyncio.Lock()
        self._latest: dict[str, dict[str, Any]] = {}
        self.latest_dir.mkdir(parents=True, exist_ok=True)
        self.history_dir.mkdir(parents=True, exist_ok=True)
        self._load_latest()

    def list_summaries(self) -> list[dict[str, Any]]:
        items = [self._to_summary(manifest) for manifest in self._latest.values()]
        items.sort(key=lambda item: (item["display_name"].lower(), item["vehicle_id"]))
        return items

    def get_manifest(self, vehicle_id: str) -> dict[str, Any] | None:
        manifest = self._latest.get(vehicle_id)
        return deepcopy(manifest) if manifest is not None else None

    async def publish(self, payload: dict[str, Any]) -> dict[str, Any]:
        vehicle_id = str(payload.get("vehicle_id") or "").strip()
        if not vehicle_id:
            raise invalid_request("vehicle_id is required")

        async with self.lock:
            previous = self._latest.get(vehicle_id)
            current_version = int(payload.get("content_version") or 0)
            current_hash = str(payload.get("content_hash") or "").strip()
            if current_version < 1:
                raise invalid_request("content_version must be >= 1")
            if not current_hash:
                raise invalid_request("content_hash is required")

            if previous is not None:
                previous_version = int(previous.get("content_version") or 0)
                previous_hash = str(previous.get("content_hash") or "")
                if current_version < previous_version:
                    raise invalid_request(
                        f"content_version {current_version} is older than published version {previous_version}"
                    )
                if current_version == previous_version and previous_hash != current_hash:
                    raise invalid_request(
                        "content_version matches the published version but content_hash differs; bump content_version first"
                    )
                if current_version == previous_version and previous_hash == current_hash:
                    return {
                        "published": False,
                        "created": False,
                        "updated": False,
                        "unchanged": True,
                        "previous_content_version": previous_version,
                        "current": self._to_summary(previous),
                        "changes": [],
                    }

            stored_manifest = deepcopy(payload)
            stored_manifest["published_at"] = utcnow_iso()
            changes = self._build_diff(previous, stored_manifest)
            self._write_manifest_files(vehicle_id, stored_manifest)
            self._latest[vehicle_id] = stored_manifest

            return {
                "published": True,
                "created": previous is None,
                "updated": previous is not None,
                "unchanged": False,
                "previous_content_version": int(previous.get("content_version")) if previous is not None else None,
                "current": self._to_summary(stored_manifest),
                "changes": changes,
            }

    def _load_latest(self) -> None:
        self._latest.clear()
        for manifest_path in sorted(self.latest_dir.glob("*.json")):
            with manifest_path.open("r", encoding="utf-8") as handle:
                manifest = json.load(handle)
            vehicle_id = str(manifest.get("vehicle_id") or "").strip()
            if vehicle_id:
                self._latest[vehicle_id] = manifest

    def _write_manifest_files(self, vehicle_id: str, manifest: dict[str, Any]) -> None:
        latest_path = self.latest_dir / f"{vehicle_id}.json"
        history_vehicle_dir = self.history_dir / vehicle_id
        history_vehicle_dir.mkdir(parents=True, exist_ok=True)
        history_path = history_vehicle_dir / self._build_history_file_name(manifest)

        serialized = json.dumps(manifest, ensure_ascii=False, indent=2)
        latest_path.write_text(serialized, encoding="utf-8")
        history_path.write_text(serialized, encoding="utf-8")

    @staticmethod
    def _build_history_file_name(manifest: dict[str, Any]) -> str:
        version = int(manifest.get("content_version") or 0)
        content_hash = str(manifest.get("content_hash") or "hashless")
        hash_prefix = content_hash[:12] if content_hash else "hashless"
        return f"v{version}_{hash_prefix}.json"

    @staticmethod
    def _to_summary(manifest: dict[str, Any]) -> dict[str, Any]:
        domains = manifest.get("domains") or []
        option_count = 0
        for domain in domains:
            values = domain.get("values") or []
            option_count += len(values)

        return {
            "vehicle_id": manifest.get("vehicle_id"),
            "display_name": manifest.get("display_name") or manifest.get("vehicle_id"),
            "content_version": int(manifest.get("content_version") or 0),
            "content_hash": manifest.get("content_hash") or "",
            "published_at": manifest.get("published_at") or "",
            "domain_count": len(domains),
            "option_count": option_count,
        }

    @staticmethod
    def _build_diff(previous: dict[str, Any] | None, current: dict[str, Any]) -> list[dict[str, Any]]:
        if previous is None:
            changes: list[dict[str, Any]] = []
            for domain in current.get("domains") or []:
                domain_id = domain.get("domain_id")
                changes.append({"change_type": "domain_added", "domain_id": domain_id, "value_id": None})
                for value in domain.get("values") or []:
                    changes.append(
                        {
                            "change_type": "value_added",
                            "domain_id": domain_id,
                            "value_id": value.get("value_id"),
                        }
                    )
            return changes

        previous_domains = VehicleContentRegistry._index_domains(previous.get("domains") or [])
        current_domains = VehicleContentRegistry._index_domains(current.get("domains") or [])
        changes = []

        for domain_id in sorted(current_domains.keys()):
            if domain_id not in previous_domains:
                changes.append({"change_type": "domain_added", "domain_id": domain_id, "value_id": None})
            previous_values = previous_domains.get(domain_id, {})
            current_values = current_domains.get(domain_id, {})
            for value_id in sorted(current_values.keys()):
                if value_id not in previous_values:
                    changes.append({"change_type": "value_added", "domain_id": domain_id, "value_id": value_id})
            for value_id in sorted(previous_values.keys()):
                if value_id not in current_values:
                    changes.append({"change_type": "value_removed", "domain_id": domain_id, "value_id": value_id})

        for domain_id in sorted(previous_domains.keys()):
            if domain_id not in current_domains:
                changes.append({"change_type": "domain_removed", "domain_id": domain_id, "value_id": None})

        return changes

    @staticmethod
    def _index_domains(domains: list[dict[str, Any]]) -> dict[str, dict[str, dict[str, Any]]]:
        indexed: dict[str, dict[str, dict[str, Any]]] = {}
        for domain in domains:
            domain_id = str(domain.get("domain_id") or "").strip()
            if not domain_id:
                continue

            values_index: dict[str, dict[str, Any]] = {}
            for value in domain.get("values") or []:
                value_id = str(value.get("value_id") or "").strip()
                if not value_id:
                    continue
                values_index[value_id] = value

            indexed[domain_id] = values_index
        return indexed
