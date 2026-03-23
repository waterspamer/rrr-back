from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class LobbyStatus(str, Enum):
    waiting = "waiting"
    starting = "starting"
    in_game = "in_game"
    closed = "closed"


class ConnectionState(str, Enum):
    connected = "connected"
    disconnected = "disconnected"
    loading = "loading"
    in_game = "in_game"


class MatchStatus(str, Enum):
    starting = "starting"
    running = "running"
    finished = "finished"
    aborted = "aborted"


@dataclass(slots=True)
class Session:
    session_id: str
    player_id: str
    player_name: str
    session_token: str
    created_at: datetime
    expires_at: datetime


@dataclass(slots=True)
class Vec3:
    x: float = 0.0
    y: float = 0.0
    z: float = 0.0

    def as_dict(self) -> dict[str, float]:
        return {"x": round(self.x, 3), "y": round(self.y, 3), "z": round(self.z, 3)}


@dataclass(slots=True)
class LobbyPlayer:
    player_id: str
    player_name: str
    connection_state: ConnectionState
    joined_at: datetime
    car_config: dict[str, Any]
    is_server_controlled: bool = False


@dataclass(slots=True)
class SpawnPoint:
    spawn_point_id: str
    position: Vec3
    rotation: Vec3


@dataclass(slots=True)
class Lobby:
    lobby_id: str
    name: str
    status: LobbyStatus
    map_id: str
    max_players: int
    owner_player_id: str
    players: dict[str, LobbyPlayer]
    created_at: datetime
    expires_at: datetime
    match_id: str | None = None


@dataclass(slots=True)
class MatchPlayer:
    player_id: str
    player_name: str
    car_config: dict[str, Any]
    authority_order: int
    spawn_point_id: str
    spawn_position: Vec3
    spawn_rotation: Vec3
    is_server_controlled: bool = False
    position: Vec3 = field(default_factory=Vec3)
    rotation: Vec3 = field(default_factory=Vec3)
    velocity: Vec3 = field(default_factory=Vec3)
    angular_velocity: Vec3 = field(default_factory=Vec3)
    wheel_states: list[dict[str, Vec3]] = field(default_factory=list)
    client_time_ms: int = 0
    server_received_time_ms: int = 0
    loaded: bool = False
    last_state_seq: int = -1
    last_input_seq: int = -1
    throttle: float = 0.0
    steer: float = 0.0
    brake: bool = False
    handbrake: bool = False
    nitro: bool = False
    last_snapshot_at: datetime = field(default_factory=datetime.utcnow)
    damage_revision: int = 0
    damage_width: int = 0
    damage_height: int = 0
    damage_map_b64: str | None = None
    last_damage_at: datetime | None = None
    debug_state: dict[str, Any] = field(default_factory=dict)
    disconnected_announced: bool = False


@dataclass(slots=True)
class Match:
    match_id: str
    lobby_id: str
    status: MatchStatus
    map_id: str
    tick_rate: int
    broadcast_rate: int
    players: dict[str, MatchPlayer]
    created_at: datetime
    server_tick: int = 0
    load_deadline: datetime | None = None
    room_id: str | None = None
    room_status: str = "backend_fallback"
    room_http_url: str | None = None
    room_ws_url: str | None = None
    room_token: str | None = None
    last_simulation_snapshot: dict[str, Any] | None = None
    recent_collisions: list[dict[str, Any]] = field(default_factory=list)
    last_authoritative_collision_seq: int = 0
