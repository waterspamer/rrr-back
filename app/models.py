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
class InputState:
    throttle: float = 0.0
    brake: float = 0.0
    steer: float = 0.0
    handbrake: bool = False
    nitro: bool = False


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
    match_id: str | None = None


@dataclass(slots=True)
class MatchPlayer:
    player_id: str
    player_name: str
    car_config: dict[str, Any]
    input_state: InputState = field(default_factory=InputState)
    position: Vec3 = field(default_factory=Vec3)
    rotation: Vec3 = field(default_factory=Vec3)
    velocity: Vec3 = field(default_factory=Vec3)
    loaded: bool = False
    last_input_seq: int = -1
    last_packet_at: datetime = field(default_factory=datetime.utcnow)
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
