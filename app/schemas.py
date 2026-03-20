from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class SessionGuestCreateRequest(BaseModel):
    player_name: str = Field(min_length=3, max_length=32)


class SessionResponse(BaseModel):
    session_id: str
    player_id: str
    player_name: str
    session_token: str
    created_at: str
    expires_at: str


class CarCustomization(BaseModel):
    model_config = ConfigDict(extra="forbid")

    selector_path: str = Field(max_length=128)
    variant_name: str = Field(max_length=128)


class PaintPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    r: float
    g: float
    b: float
    a: float


class CarConfigPayload(BaseModel):
    model_config = ConfigDict(extra="allow")

    version: int
    loadout_name: str = Field(max_length=128)
    loadout_display_name: str | None = Field(default=None, max_length=128)
    body_set_option_index: int | None = None
    engine_index: int | None = None
    suspension_index: int | None = None
    paint_index: int | None = None
    handling_name: str | None = Field(default=None, max_length=128)
    body_set_name: str | None = Field(default=None, max_length=128)
    engine_name: str | None = Field(default=None, max_length=128)
    suspension_name: str | None = Field(default=None, max_length=128)
    paint_name: str | None = Field(default=None, max_length=128)
    has_paint: bool | None = None
    paint: PaintPayload | None = None
    customizations: list[CarCustomization] = Field(default_factory=list, max_length=128)

    @field_validator("customizations")
    @classmethod
    def validate_customizations(cls, value: list[CarCustomization]) -> list[CarCustomization]:
        if len(value) > 128:
            raise ValueError("customizations exceeds 128 entries")
        return value

    @model_validator(mode="after")
    def check_json_size(self) -> "CarConfigPayload":
        encoded = json.dumps(self.model_dump(mode="json")).encode("utf-8")
        if len(encoded) > 32 * 1024:
            raise ValueError("car_config exceeds 32 KB")
        return self


class LobbyCreateRequest(BaseModel):
    name: str = Field(min_length=3, max_length=32)
    map_id: str = Field(min_length=1, max_length=64)
    max_players: int = Field(ge=2, le=8)
    car_config: CarConfigPayload


class LobbyJoinRequest(BaseModel):
    car_config: CarConfigPayload


class LobbyCarConfigUpdateRequest(BaseModel):
    car_config: CarConfigPayload


class LobbyPlayerResponse(BaseModel):
    player_id: str
    player_name: str
    connection_state: str
    joined_at: str | None = None
    car_config: dict[str, Any] | None = None


class LobbySummaryResponse(BaseModel):
    lobby_id: str
    name: str
    status: str
    map_id: str
    max_players: int
    current_players: int


class LobbyDetailResponse(LobbySummaryResponse):
    owner_player_id: str
    players: list[LobbyPlayerResponse]
    created_at: str
    match_id: str | None = None


class LobbyCreateResponse(BaseModel):
    lobby_id: str
    status: str


class LobbyJoinResponse(BaseModel):
    lobby_id: str
    player_id: str
    joined: bool


class SimpleSuccessResponse(BaseModel):
    updated: bool | None = None
    left: bool | None = None


class PaginatedLobbiesResponse(BaseModel):
    items: list[LobbySummaryResponse]
    total: int


class Vec3Response(BaseModel):
    x: float
    y: float
    z: float


class MatchPlayerInfoResponse(BaseModel):
    player_id: str
    player_name: str
    connection_state: str
    authority_order: int
    spawn_point_id: str
    spawn_position: Vec3Response
    spawn_rotation: Vec3Response
    car_config: dict[str, Any]


class MatchInfoResponse(BaseModel):
    match_id: str
    lobby_id: str
    status: str
    map_id: str
    tick_rate: int
    players: list[MatchPlayerInfoResponse]


class HealthResponse(BaseModel):
    status: str
    lobbies: int
    matches: int
    sessions: int


class AdminLobbyPlayerResponse(BaseModel):
    player_id: str
    player_name: str
    connection_state: str
    joined_at: str
    car_config: dict[str, Any]
    loadout_display_name: str | None = None
    paint_name: str | None = None
    customizations: list[dict[str, Any]] = Field(default_factory=list)


class AdminLobbyResponse(BaseModel):
    lobby_id: str
    name: str
    status: str
    map_id: str
    max_players: int
    current_players: int
    owner_player_id: str
    created_at: str
    match_id: str | None = None
    players: list[AdminLobbyPlayerResponse]


class AdminLobbiesResponse(BaseModel):
    items: list[AdminLobbyResponse]


class AdminMatchPlayerResponse(BaseModel):
    player_id: str
    player_name: str
    connection_state: str
    authority_order: int
    spawn_point_id: str
    spawn_position: dict[str, float]
    spawn_rotation: dict[str, float]
    position: dict[str, float]
    rotation: dict[str, float]
    velocity: dict[str, float]
    speed: float
    last_snapshot_at: str
    client_time_ms: int = 0
    server_received_time_ms: int = 0
    car_config: dict[str, Any]
    wheel_state_count: int = 0
    damage_revision: int = 0
    damage_width: int = 0
    damage_height: int = 0
    damage_map_bytes: int = 0
    damage_map_b64: str | None = None
    last_damage_at: str | None = None


class AdminMatchSummaryResponse(BaseModel):
    match_id: str
    lobby_id: str
    status: str
    map_id: str
    player_count: int
    server_tick: int


class AdminMatchesResponse(BaseModel):
    items: list[AdminMatchSummaryResponse]


class AdminMatchDetailResponse(BaseModel):
    match_id: str
    lobby_id: str
    status: str
    map_id: str
    tick_rate: int
    server_tick: int
    players: list[AdminMatchPlayerResponse]
    raw_snapshot: dict[str, Any]
    telemetry: dict[str, Any]
