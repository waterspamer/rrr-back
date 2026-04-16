from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any

from app.core.errors import invalid_request


VALID_STATES = {"draft", "published", "deprecated"}


def utcnow_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


class VehicleOfferRegistry:
    def __init__(self, storage_dir: str) -> None:
        self.root_dir = Path(storage_dir).resolve()
        self.store_dir = self.root_dir / "store"
        self.store_dir.mkdir(parents=True, exist_ok=True)

    def list_public_offers(self, vehicle_id: str) -> list[dict[str, Any]]:
        payload = self._load_vehicle(vehicle_id)
        items = []
        for offer in payload["items"]:
            if offer["state"] == "published":
                items.append(deepcopy(offer))
        return items

    def list_admin_offers(self, vehicle_id: str) -> list[dict[str, Any]]:
        payload = self._load_vehicle(vehicle_id)
        return deepcopy(payload["items"])

    def sync_from_manifest(self, manifest: dict[str, Any]) -> dict[str, Any]:
        vehicle_id = str(manifest.get("vehicle_id") or "").strip()
        if not vehicle_id:
            raise invalid_request("vehicle_id is required")

        payload = self._load_vehicle(vehicle_id)
        items_by_offer_id = {item["offer_id"]: item for item in payload["items"]}
        defaults = self._index_defaults(manifest.get("defaults") or [])

        created_count = 0
        updated_count = 0
        deprecated_count = 0
        seen_offer_ids: set[str] = set()

        for domain in manifest.get("domains") or []:
            domain_id = str(domain.get("domain_id") or "").strip()
            domain_display_name = str(domain.get("display_name") or domain_id).strip()
            values = domain.get("values") or []
            for value in values:
                value_id = str(value.get("value_id") or "").strip()
                if not domain_id or not value_id:
                    continue

                offer_id = self._build_offer_id(vehicle_id, domain_id, value_id)
                seen_offer_ids.add(offer_id)
                is_default = defaults.get(domain_id) == value_id
                display_name = f"{domain_display_name}: {str(value.get('display_name') or value_id).strip()}"

                if offer_id in items_by_offer_id:
                    offer = items_by_offer_id[offer_id]
                    offer["vehicle_id"] = vehicle_id
                    offer["domain_id"] = domain_id
                    offer["value_id"] = value_id
                    offer["display_name"] = display_name
                    offer["value_display_name"] = str(value.get("display_name") or value_id).strip()
                    offer["source_name"] = str(value.get("source_name") or "").strip()
                    offer["is_default"] = is_default
                    offer["bundle_id"] = str((manifest.get("bundle") or {}).get("bundle_id") or "").strip()
                    offer["last_content_version"] = int(manifest.get("content_version") or 0)
                    offer["last_content_hash"] = str(manifest.get("content_hash") or "").strip()
                    offer["updated_at"] = utcnow_iso()
                    if offer["state"] == "deprecated":
                        offer["state"] = "published" if is_default else "draft"
                    updated_count += 1
                else:
                    offer = {
                        "offer_id": offer_id,
                        "vehicle_id": vehicle_id,
                        "domain_id": domain_id,
                        "value_id": value_id,
                        "display_name": display_name,
                        "value_display_name": str(value.get("display_name") or value_id).strip(),
                        "source_name": str(value.get("source_name") or "").strip(),
                        "is_default": is_default,
                        "state": "published" if is_default else "draft",
                        "soft_price": 0,
                        "premium_price": 0,
                        "bundle_id": str((manifest.get("bundle") or {}).get("bundle_id") or "").strip(),
                        "last_content_version": int(manifest.get("content_version") or 0),
                        "last_content_hash": str(manifest.get("content_hash") or "").strip(),
                        "created_at": utcnow_iso(),
                        "updated_at": utcnow_iso(),
                    }
                    payload["items"].append(offer)
                    items_by_offer_id[offer_id] = offer
                    created_count += 1

        for offer in payload["items"]:
            if offer["offer_id"] in seen_offer_ids:
                continue
            if offer["state"] != "deprecated":
                offer["state"] = "deprecated"
                offer["updated_at"] = utcnow_iso()
                deprecated_count += 1

        payload["vehicle_id"] = vehicle_id
        payload["updated_at"] = utcnow_iso()
        payload["items"].sort(key=lambda item: (item["domain_id"], item["value_id"]))
        self._save_vehicle(vehicle_id, payload)

        return {
            "vehicle_id": vehicle_id,
            "created_count": created_count,
            "updated_count": updated_count,
            "deprecated_count": deprecated_count,
            "items": deepcopy(payload["items"]),
        }

    def update_offers(self, vehicle_id: str, updates: list[dict[str, Any]]) -> dict[str, Any]:
        payload = self._load_vehicle(vehicle_id)
        items_by_offer_id = {item["offer_id"]: item for item in payload["items"]}

        changed_count = 0
        for update in updates:
            offer_id = str(update.get("offer_id") or "").strip()
            if not offer_id or offer_id not in items_by_offer_id:
                raise invalid_request(f"Unknown offer_id '{offer_id}'")

            offer = items_by_offer_id[offer_id]
            state = str(update.get("state") or offer["state"]).strip().lower()
            if state not in VALID_STATES:
                raise invalid_request(f"Invalid offer state '{state}'")

            offer["display_name"] = str(update.get("display_name") or offer["display_name"]).strip()
            offer["soft_price"] = max(0, int(update.get("soft_price") if update.get("soft_price") is not None else offer["soft_price"]))
            offer["premium_price"] = max(0, int(update.get("premium_price") if update.get("premium_price") is not None else offer["premium_price"]))
            offer["state"] = state
            offer["updated_at"] = utcnow_iso()
            changed_count += 1

        payload["updated_at"] = utcnow_iso()
        self._save_vehicle(vehicle_id, payload)
        return {
            "vehicle_id": vehicle_id,
            "updated_count": changed_count,
            "items": deepcopy(payload["items"]),
        }

    def _load_vehicle(self, vehicle_id: str) -> dict[str, Any]:
        path = self.store_dir / f"{vehicle_id}.json"
        if not path.exists():
            return {"vehicle_id": vehicle_id, "updated_at": "", "items": []}
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    def _save_vehicle(self, vehicle_id: str, payload: dict[str, Any]) -> None:
        path = self.store_dir / f"{vehicle_id}.json"
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    @staticmethod
    def _build_offer_id(vehicle_id: str, domain_id: str, value_id: str) -> str:
        return f"{vehicle_id}:{domain_id}:{value_id}"

    @staticmethod
    def _index_defaults(defaults: list[dict[str, Any]]) -> dict[str, str]:
        indexed: dict[str, str] = {}
        for item in defaults:
            domain_id = str(item.get("domain_id") or "").strip()
            value_id = str(item.get("value_id") or "").strip()
            if domain_id and value_id:
                indexed[domain_id] = value_id
        return indexed
