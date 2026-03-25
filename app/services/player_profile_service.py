from __future__ import annotations

from datetime import datetime
from typing import Any

from app.models import PlayerGarageCar, PlayerProfile


class PlayerProfileService:
    def __init__(self) -> None:
        self._profiles_by_player_id: dict[str, PlayerProfile] = {}

    def ensure_guest_profile(self, player_id: str, display_name: str) -> PlayerProfile:
        profile = self._profiles_by_player_id.get(player_id)
        if profile is None:
            now = datetime.utcnow()
            profile = PlayerProfile(
                account_id=player_id,
                player_id=player_id,
                display_name=(display_name or player_id).strip() or player_id,
                is_guest=True,
                created_at=now,
                updated_at=now,
                owned_cars=[
                    PlayerGarageCar(
                        car_id="cooper",
                        display_name="Cooper",
                        acquired_at=now,
                        acquisition_source="starter",
                        favorite=True,
                        tags=["starter", "owned"],
                    )
                ],
                public_flags={"starter_pack": True, "guest_account": True},
                private_data={"schema_version": 1, "notes": "guest bootstrap profile"},
            )
            self._profiles_by_player_id[player_id] = profile
            return profile

        normalized_name = (display_name or "").strip()
        if normalized_name and profile.display_name != normalized_name:
            profile.display_name = normalized_name
            profile.updated_at = datetime.utcnow()
        return profile

    def get_profile(self, player_id: str) -> PlayerProfile | None:
        if not player_id:
            return None
        return self._profiles_by_player_id.get(player_id)

    def list_profiles(self) -> list[PlayerProfile]:
        profiles = list(self._profiles_by_player_id.values())
        profiles.sort(key=lambda item: (item.created_at, item.player_id))
        return profiles

    def update_profile(self, player_id: str, patch: dict[str, Any]) -> PlayerProfile | None:
        profile = self.get_profile(player_id)
        if profile is None:
            return None

        normalized_name = str(patch.get("display_name", "") or "").strip()
        if normalized_name:
            profile.display_name = normalized_name

        if "balance_soft" in patch and patch.get("balance_soft") is not None:
            profile.balance_soft = max(0, int(patch["balance_soft"]))
        if "balance_premium" in patch and patch.get("balance_premium") is not None:
            profile.balance_premium = max(0, int(patch["balance_premium"]))
        if "level" in patch and patch.get("level") is not None:
            profile.level = max(1, int(patch["level"]))
        if "experience" in patch and patch.get("experience") is not None:
            profile.experience = max(0, int(patch["experience"]))

        if isinstance(patch.get("public_flags"), dict):
            profile.public_flags = dict(patch["public_flags"])
        if isinstance(patch.get("private_data"), dict):
            profile.private_data = dict(patch["private_data"])

        selected_car_id = str(patch.get("selected_car_id", "") or "").strip()
        selected_car_display_name = str(patch.get("selected_car_display_name", "") or "").strip()
        if selected_car_id:
            profile.selected_car_id = selected_car_id
        if selected_car_display_name:
            profile.selected_car_display_name = selected_car_display_name

        owned_cars_payload = patch.get("owned_cars")
        if isinstance(owned_cars_payload, list):
            updated_owned_cars: list[PlayerGarageCar] = []
            now = datetime.utcnow()
            for item in owned_cars_payload:
                if not isinstance(item, dict):
                    continue
                car_id = str(item.get("car_id", "") or "").strip()
                if not car_id:
                    continue
                display_name = str(item.get("display_name", "") or car_id).strip() or car_id
                acquisition_source = str(item.get("acquisition_source", "") or "manual_update").strip() or "manual_update"
                tuning_preset_ids = [str(entry).strip() for entry in item.get("tuning_preset_ids", []) if str(entry).strip()]
                tags = [str(entry).strip() for entry in item.get("tags", []) if str(entry).strip()]
                updated_owned_cars.append(
                    PlayerGarageCar(
                        car_id=car_id,
                        display_name=display_name,
                        acquired_at=now,
                        acquisition_source=acquisition_source,
                        favorite=bool(item.get("favorite", False)),
                        tuning_preset_ids=tuning_preset_ids,
                        tags=tags,
                    )
                )
            if updated_owned_cars:
                profile.owned_cars = updated_owned_cars
                if not selected_car_id:
                    profile.selected_car_id = updated_owned_cars[0].car_id
                if not selected_car_display_name:
                    profile.selected_car_display_name = updated_owned_cars[0].display_name

        profile.updated_at = datetime.utcnow()
        return profile

    def record_car_config_selection(self, player_id: str, car_config: dict[str, Any] | None) -> None:
        if not player_id or not isinstance(car_config, dict):
            return

        profile = self.get_profile(player_id)
        if profile is None:
            return

        car_id = str(car_config.get("loadout_name", "") or "").strip()
        display_name = str(car_config.get("loadout_display_name", "") or car_id).strip() or car_id
        if not car_id:
            return

        profile.selected_car_id = car_id
        profile.selected_car_display_name = display_name or car_id

        existing = None
        for owned_car in profile.owned_cars:
            if owned_car.car_id == car_id:
                existing = owned_car
                break

        if existing is None:
            profile.owned_cars.append(
                PlayerGarageCar(
                    car_id=car_id,
                    display_name=display_name or car_id,
                    acquired_at=datetime.utcnow(),
                    acquisition_source="selection_unlock",
                    tags=["owned", "selected"],
                )
            )
        else:
            existing.display_name = display_name or existing.display_name or car_id

        profile.updated_at = datetime.utcnow()

    def build_public_payload(
        self,
        player_id: str,
        fallback_name: str,
        *,
        is_server_controlled: bool = False,
    ) -> dict[str, Any]:
        profile = self.get_profile(player_id)
        if profile is None:
            return self._build_fallback_public_payload(player_id, fallback_name, is_server_controlled=is_server_controlled)
        return self.serialize_public(profile)

    def serialize_public(self, profile: PlayerProfile) -> dict[str, Any]:
        owned_cars = [self._serialize_owned_car(item) for item in profile.owned_cars]
        return {
            "player_id": profile.player_id,
            "account_id": profile.account_id,
            "display_name": profile.display_name,
            "is_guest": profile.is_guest,
            "balance": {
                "soft": profile.balance_soft,
                "premium": profile.balance_premium,
            },
            "progression": {
                "level": profile.level,
                "experience": profile.experience,
            },
            "garage": {
                "selected_car_id": profile.selected_car_id,
                "selected_car_display_name": profile.selected_car_display_name,
                "owned_car_count": len(owned_cars),
                "owned_cars": owned_cars,
            },
            "public_flags": dict(profile.public_flags),
        }

    def serialize_private(self, profile: PlayerProfile) -> dict[str, Any]:
        return {
            **self.serialize_public(profile),
            "created_at": profile.created_at.isoformat() + "Z",
            "updated_at": profile.updated_at.isoformat() + "Z",
            "private_data": dict(profile.private_data),
        }

    @staticmethod
    def _serialize_owned_car(car: PlayerGarageCar) -> dict[str, Any]:
        return {
            "car_id": car.car_id,
            "display_name": car.display_name,
            "acquired_at": car.acquired_at.isoformat() + "Z",
            "acquisition_source": car.acquisition_source,
            "favorite": car.favorite,
            "tuning_preset_ids": list(car.tuning_preset_ids),
            "tags": list(car.tags),
        }

    @staticmethod
    def _build_fallback_public_payload(
        player_id: str,
        fallback_name: str,
        *,
        is_server_controlled: bool,
    ) -> dict[str, Any]:
        display_name = (fallback_name or player_id).strip() or player_id
        car_display_name = "Idle Server Car" if is_server_controlled else "Cooper"
        car_id = "server_bot" if is_server_controlled else "cooper"
        return {
            "player_id": player_id,
            "account_id": player_id,
            "display_name": display_name,
            "is_guest": not is_server_controlled,
            "balance": {"soft": 0, "premium": 0},
            "progression": {"level": 1, "experience": 0},
            "garage": {
                "selected_car_id": car_id,
                "selected_car_display_name": car_display_name,
                "owned_car_count": 1,
                "owned_cars": [
                    {
                        "car_id": car_id,
                        "display_name": car_display_name,
                        "acquired_at": datetime.utcnow().isoformat() + "Z",
                        "acquisition_source": "fallback",
                        "favorite": True,
                        "tuning_preset_ids": [],
                        "tags": ["fallback"],
                    }
                ],
            },
            "public_flags": {
                "server_controlled": is_server_controlled,
                "fallback_profile": True,
            },
        }
