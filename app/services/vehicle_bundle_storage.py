from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any
import hashlib


def utcnow_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


class VehicleBundleStorage:
    def __init__(self, storage_dir: str) -> None:
        self.root_dir = Path(storage_dir).resolve()
        self.files_dir = self.root_dir / "bundles" / "files"
        self.meta_dir = self.root_dir / "bundles" / "meta"
        self.files_dir.mkdir(parents=True, exist_ok=True)
        self.meta_dir.mkdir(parents=True, exist_ok=True)

    def upload(
        self,
        vehicle_id: str,
        file_name: str,
        file_bytes: bytes,
        content_type: str | None = None,
    ) -> dict[str, Any]:
        safe_vehicle_id = str(vehicle_id or "").strip()
        original_name = Path(file_name or "vehicle.bundle").name
        suffix = Path(original_name).suffix or ".bundle"
        bundle_hash = hashlib.sha256(file_bytes).hexdigest()
        bundle_id = f"{safe_vehicle_id}_{bundle_hash[:12]}"

        file_path = self.files_dir / f"{bundle_id}{suffix}"
        meta_path = self.meta_dir / f"{bundle_id}.json"

        file_path.write_bytes(file_bytes)
        metadata = {
            "vehicle_id": safe_vehicle_id,
            "bundle_id": bundle_id,
            "file_name": original_name,
            "content_type": content_type or "application/octet-stream",
            "file_size_bytes": len(file_bytes),
            "bundle_hash": bundle_hash,
            "uploaded_at": utcnow_iso(),
            "file_path": str(file_path),
        }
        meta_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
        return deepcopy(metadata)

    def get_metadata(self, bundle_id: str) -> dict[str, Any] | None:
        meta_path = self.meta_dir / f"{bundle_id}.json"
        if not meta_path.exists():
            return None
        with meta_path.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    def get_file_path(self, bundle_id: str) -> Path | None:
        metadata = self.get_metadata(bundle_id)
        if metadata is None:
            return None

        file_path = Path(str(metadata.get("file_path") or ""))
        return file_path if file_path.exists() else None
