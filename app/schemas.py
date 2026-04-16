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
    player_profile: "PlayerProfileResponse"


class PlayerGarageCarPayload(BaseModel):
    car_id: str = Field(min_length=1, max_length=128)
    display_name: str | None = Field(default=None, max_length=128)
    acquisition_source: str | None = Field(default=None, max_length=64)
    favorite: bool = False
    tuning_preset_ids: list[str] = Field(default_factory=list, max_length=32)
    tags: list[str] = Field(default_factory=list, max_length=32)


class PlayerBalanceResponse(BaseModel):
    soft: int
    premium: int


class PlayerProgressionResponse(BaseModel):
    level: int
    experience: int


class PlayerGarageCarResponse(BaseModel):
    car_id: str
    display_name: str
    acquired_at: str
    acquisition_source: str
    favorite: bool = False
    tuning_preset_ids: list[str] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)


class PlayerGarageResponse(BaseModel):
    selected_car_id: str
    selected_car_display_name: str
    owned_car_count: int
    owned_cars: list[PlayerGarageCarResponse] = Field(default_factory=list)


class PlayerPublicProfileResponse(BaseModel):
    player_id: str
    account_id: str
    display_name: str
    is_guest: bool = True
    balance: PlayerBalanceResponse
    progression: PlayerProgressionResponse
    garage: PlayerGarageResponse
    public_flags: dict[str, Any] = Field(default_factory=dict)


class PlayerProfileResponse(PlayerPublicProfileResponse):
    created_at: str
    updated_at: str
    private_data: dict[str, Any] = Field(default_factory=dict)


class PlayerProfileUpdateRequest(BaseModel):
    display_name: str | None = Field(default=None, min_length=3, max_length=32)
    balance_soft: int | None = Field(default=None, ge=0)
    balance_premium: int | None = Field(default=None, ge=0)
    level: int | None = Field(default=None, ge=1)
    experience: int | None = Field(default=None, ge=0)
    selected_car_id: str | None = Field(default=None, max_length=128)
    selected_car_display_name: str | None = Field(default=None, max_length=128)
    owned_cars: list[PlayerGarageCarPayload] | None = None
    public_flags: dict[str, Any] | None = None
    private_data: dict[str, Any] | None = None


class AdminPlayersResponse(BaseModel):
    items: list[PlayerProfileResponse] = Field(default_factory=list)


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
    is_server_controlled: bool = False
    joined_at: str | None = None
    car_config: dict[str, Any] | None = None
    player_profile: PlayerPublicProfileResponse | None = None


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
    expires_at: str | None = None
    match_id: str | None = None


class LobbyCreateResponse(BaseModel):
    lobby_id: str
    status: str


class LobbyJoinResponse(BaseModel):
    lobby_id: str
    player_id: str
    joined: bool


class LobbyStartSoloResponse(BaseModel):
    started: bool
    match_id: str
    server_player_id: str


class SimpleSuccessResponse(BaseModel):
    updated: bool | None = None
    left: bool | None = None
    closed: bool | None = None


class AdminLobbyCloseResponse(BaseModel):
    lobby_id: str
    match_id: str | None = None
    closed: bool
    reason: str


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
    is_server_controlled: bool = False
    authority_order: int
    spawn_point_id: str
    spawn_position: Vec3Response
    spawn_rotation: Vec3Response
    car_config: dict[str, Any]
    player_profile: PlayerPublicProfileResponse | None = None


class MatchInfoResponse(BaseModel):
    match_id: str
    lobby_id: str
    status: str
    map_id: str
    tick_rate: int
    room_id: str | None = None
    room_status: str | None = None
    room_http_url: str | None = None
    room_ws_url: str | None = None
    room_token: str | None = None
    players: list[MatchPlayerInfoResponse]


class HealthResponse(BaseModel):
    status: str
    lobbies: int
    matches: int
    sessions: int


class VehicleContentValuePayload(BaseModel):
    value_id: str = Field(min_length=1, max_length=128)
    display_name: str | None = Field(default=None, max_length=128)
    source_name: str | None = Field(default=None, max_length=128)


class VehicleContentDomainPayload(BaseModel):
    domain_id: str = Field(min_length=1, max_length=128)
    display_name: str | None = Field(default=None, max_length=128)
    source_type: str = Field(default="visual_slot", max_length=64)
    selector_path: str | None = Field(default=None, max_length=128)
    values: list[VehicleContentValuePayload] = Field(default_factory=list, max_length=256)


class VehicleContentSelectionPayload(BaseModel):
    domain_id: str = Field(min_length=1, max_length=128)
    value_id: str = Field(min_length=1, max_length=128)


class VehicleBundlePayload(BaseModel):
    bundle_id: str | None = Field(default=None, max_length=128)
    bundle_hash: str | None = Field(default=None, max_length=256)
    bundle_url: str | None = Field(default=None, max_length=512)


class VehicleContentManifestPayload(BaseModel):
    schema_version: int = Field(default=1, ge=1)
    vehicle_id: str = Field(min_length=1, max_length=128)
    display_name: str = Field(min_length=1, max_length=128)
    manufacturer: str | None = Field(default=None, max_length=128)
    description: str | None = Field(default=None, max_length=2048)
    content_version: int = Field(ge=1)
    content_hash: str = Field(min_length=1, max_length=256)
    prefab_name: str | None = Field(default=None, max_length=128)
    generated_at: str | None = Field(default=None, max_length=64)
    bundle: VehicleBundlePayload | None = None
    defaults: list[VehicleContentSelectionPayload] = Field(default_factory=list, max_length=128)
    domains: list[VehicleContentDomainPayload] = Field(default_factory=list, max_length=256)


class VehicleContentManifestResponse(VehicleContentManifestPayload):
    published_at: str


class VehicleContentSummaryResponse(BaseModel):
    vehicle_id: str
    display_name: str
    content_version: int
    content_hash: str
    published_at: str
    domain_count: int
    option_count: int


class VehicleContentListResponse(BaseModel):
    items: list[VehicleContentSummaryResponse] = Field(default_factory=list)


class VehicleContentPublishChangeResponse(BaseModel):
    change_type: str
    domain_id: str | None = None
    value_id: str | None = None


class VehicleContentPublishResponse(BaseModel):
    published: bool
    created: bool
    updated: bool
    unchanged: bool
    previous_content_version: int | None = None
    current: VehicleContentSummaryResponse
    changes: list[VehicleContentPublishChangeResponse] = Field(default_factory=list)


class VehicleBundleUploadResponse(BaseModel):
    vehicle_id: str
    bundle_id: str
    file_name: str
    content_type: str
    file_size_bytes: int
    bundle_hash: str
    bundle_url: str
    uploaded_at: str


class VehicleOfferResponse(BaseModel):
    offer_id: str
    vehicle_id: str
    domain_id: str
    value_id: str
    display_name: str
    value_display_name: str
    source_name: str = ""
    is_default: bool = False
    state: str
    soft_price: int = 0
    premium_price: int = 0
    bundle_id: str = ""
    last_content_version: int = 0
    last_content_hash: str = ""
    created_at: str | None = None
    updated_at: str | None = None


class VehicleOfferListResponse(BaseModel):
    vehicle_id: str
    items: list[VehicleOfferResponse] = Field(default_factory=list)


class VehicleOfferSyncResponse(BaseModel):
    vehicle_id: str
    created_count: int
    updated_count: int
    deprecated_count: int
    items: list[VehicleOfferResponse] = Field(default_factory=list)


class VehicleOfferUpdatePayload(BaseModel):
    offer_id: str = Field(min_length=1, max_length=256)
    display_name: str | None = Field(default=None, max_length=256)
    state: str | None = Field(default=None, max_length=32)
    soft_price: int | None = Field(default=None, ge=0)
    premium_price: int | None = Field(default=None, ge=0)


class VehicleOfferUpdateRequest(BaseModel):
    items: list[VehicleOfferUpdatePayload] = Field(default_factory=list, max_length=512)


class VehicleOfferUpdateResponse(BaseModel):
    vehicle_id: str
    updated_count: int
    items: list[VehicleOfferResponse] = Field(default_factory=list)


class AdminLobbyPlayerResponse(BaseModel):
    player_id: str
    player_name: str
    connection_state: str
    is_server_controlled: bool = False
    joined_at: str
    car_config: dict[str, Any]
    loadout_display_name: str | None = None
    paint_name: str | None = None
    customizations: list[dict[str, Any]] = Field(default_factory=list)
    player_profile: PlayerPublicProfileResponse | None = None


class AdminLobbyResponse(BaseModel):
    lobby_id: str
    name: str
    status: str
    map_id: str
    max_players: int
    current_players: int
    owner_player_id: str
    created_at: str
    expires_at: str | None = None
    match_id: str | None = None
    players: list[AdminLobbyPlayerResponse]


class AdminLobbiesResponse(BaseModel):
    items: list[AdminLobbyResponse]


class AdminMatchPlayerResponse(BaseModel):
    player_id: str
    player_name: str
    connection_state: str
    is_server_controlled: bool = False
    authority_order: int
    spawn_point_id: str
    spawn_position: dict[str, float]
    spawn_rotation: dict[str, float]
    position: dict[str, float]
    rotation: dict[str, float]
    velocity: dict[str, float]
    angular_velocity: dict[str, float]
    speed: float
    last_snapshot_at: str
    client_time_ms: int = 0
    server_received_time_ms: int = 0
    last_input_seq: int = 0
    throttle: float = 0.0
    steer: float = 0.0
    brake: bool = False
    handbrake: bool = False
    nitro: bool = False
    car_config: dict[str, Any]
    wheel_state_count: int = 0
    damage_revision: int = -1
    damage_width: int = 0
    damage_height: int = 0
    damage_map_bytes: int = 0
    damage_map_b64: str | None = None
    last_damage_at: str | None = None
    debug: dict[str, Any] = Field(default_factory=dict)
    player_profile: PlayerPublicProfileResponse | None = None


class AdminMatchSummaryResponse(BaseModel):
    match_id: str
    lobby_id: str
    status: str
    map_id: str
    player_count: int
    server_tick: int
    room_id: str | None = None
    room_status: str | None = None
    source: str = "backend_runtime"
    debug_summary: dict[str, Any] = Field(default_factory=dict)


class AdminMatchesResponse(BaseModel):
    items: list[AdminMatchSummaryResponse]


class AdminMatchDetailResponse(BaseModel):
    match_id: str
    lobby_id: str
    status: str
    map_id: str
    tick_rate: int
    server_tick: int
    room_id: str | None = None
    room_status: str | None = None
    room_http_url: str | None = None
    room_ws_url: str | None = None
    room_token: str | None = None
    source: str = "backend_runtime"
    debug_summary: dict[str, Any] = Field(default_factory=dict)
    players: list[AdminMatchPlayerResponse]
    recent_collisions: list[dict[str, Any]] = Field(default_factory=list)
    raw_snapshot: dict[str, Any]
    telemetry: dict[str, Any]


class AdminGameSettingsSectionRequest(BaseModel):
    section_id: str
    fields: dict[str, Any] = Field(default_factory=dict)


class AdminGameSettingsUpdateRequest(BaseModel):
    sections: list[AdminGameSettingsSectionRequest] = Field(default_factory=list)


class AdminGameSettingsSectionResponse(BaseModel):
    section_id: str
    title: str
    description: str | None = None
    editable: bool = True
    meta: dict[str, Any] = Field(default_factory=dict)
    fields: dict[str, Any] = Field(default_factory=dict)


class AdminGameSettingsResponse(BaseModel):
    scope: str = "global"
    source: str = "backend_runtime"
    note: str | None = None
    sections: list[AdminGameSettingsSectionResponse] = Field(default_factory=list)


SessionResponse.model_rebuild()
