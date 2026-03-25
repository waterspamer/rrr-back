from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta
from secrets import token_urlsafe
from typing import Any
from uuid import uuid4

from fastapi import WebSocket
from starlette.websockets import WebSocketDisconnect

from app.core.config import Settings
from app.core.errors import (
    invalid_request,
    lobby_already_started,
    lobby_full,
    lobby_not_found,
    match_not_found,
    player_already_in_lobby,
    player_not_in_lobby,
    unauthorized,
)
from app.models import ConnectionState, Lobby, LobbyPlayer, LobbyStatus, Match, MatchPlayer, MatchStatus, Session, SpawnPoint, Vec3
from app.services.direct_observer_service import DirectObserverClient
from app.services.simulation_service import SimulationServiceClient


logger = logging.getLogger("rrr.runtime")

MAP_SPAWN_POINTS: dict[str, list[SpawnPoint]] = {
    "city_default": [
        SpawnPoint("sp_01", Vec3(0.0, 0.5, 0.0), Vec3(0.0, 0.0, 0.0)),
        SpawnPoint("sp_02", Vec3(4.5, 0.5, 0.0), Vec3(0.0, 0.0, 0.0)),
        SpawnPoint("sp_03", Vec3(-4.5, 0.5, 0.0), Vec3(0.0, 0.0, 0.0)),
        SpawnPoint("sp_04", Vec3(9.0, 0.5, -2.5), Vec3(0.0, 8.0, 0.0)),
        SpawnPoint("sp_05", Vec3(-9.0, 0.5, -2.5), Vec3(0.0, -8.0, 0.0)),
        SpawnPoint("sp_06", Vec3(13.5, 0.5, -5.0), Vec3(0.0, 12.0, 0.0)),
        SpawnPoint("sp_07", Vec3(-13.5, 0.5, -5.0), Vec3(0.0, -12.0, 0.0)),
        SpawnPoint("sp_08", Vec3(18.0, 0.5, -7.5), Vec3(0.0, 15.0, 0.0)),
    ],
    "duel_test": [
        SpawnPoint("sp_01", Vec3(-2.5, 0.5, 0.0), Vec3(0.0, 0.0, 0.0)),
        SpawnPoint("sp_02", Vec3(2.5, 0.5, 0.0), Vec3(0.0, 0.0, 0.0)),
    ],
}


def utcnow() -> datetime:
    return datetime.utcnow()


class SlidingWindowRateLimiter:
    def __init__(self, limit: int, window_sec: int = 60) -> None:
        self.limit = limit
        self.window_sec = window_sec
        self._buckets: dict[str, deque[float]] = defaultdict(deque)

    def hit(self, key: str) -> bool:
        now = time.time()
        bucket = self._buckets[key]
        while bucket and bucket[0] <= now - self.window_sec:
            bucket.popleft()
        if len(bucket) >= self.limit:
            return False
        bucket.append(now)
        return True


class RollingMetricWindow:
    def __init__(self, window_sec: float = 10.0) -> None:
        self.window_sec = window_sec
        self.events: deque[tuple[float, int]] = deque()
        self.total_bytes = 0
        self.total_messages = 0

    def add(self, byte_count: int) -> None:
        now = time.time()
        self.events.append((now, max(0, byte_count)))
        self.total_bytes += max(0, byte_count)
        self.total_messages += 1
        self._prune(now)

    def snapshot(self) -> dict[str, float | int]:
        now = time.time()
        self._prune(now)
        span = max(1.0, self.window_sec)
        bytes_sum = sum(item[1] for item in self.events)
        messages = len(self.events)
        return {
            "window_sec": round(self.window_sec, 1),
            "messages_per_sec": round(messages / span, 2),
            "bytes_per_sec": round(bytes_sum / span, 2),
            "kbps": round((bytes_sum * 8.0) / span / 1000.0, 2),
            "total_messages": self.total_messages,
            "total_bytes": self.total_bytes,
        }

    def _prune(self, now: float) -> None:
        threshold = now - self.window_sec
        while self.events and self.events[0][0] < threshold:
            self.events.popleft()


class MatchRuntimeMetrics:
    def __init__(self) -> None:
        self.player_input_in = RollingMetricWindow()
        self.player_state_in = RollingMetricWindow()
        self.simulation_input_out = RollingMetricWindow()
        self.simulation_snapshot_in = RollingMetricWindow()
        self.damage_state_in = RollingMetricWindow()
        self.collision_event_in = RollingMetricWindow()
        self.match_state_out = RollingMetricWindow()
        self.damage_state_out = RollingMetricWindow()
        self.collision_event_out = RollingMetricWindow()
        self.admin_state_out = RollingMetricWindow()
        self.last_match_snapshot_bytes = 0
        self.last_simulation_snapshot_bytes = 0
        self.last_damage_payload_bytes = 0
        self.last_collision_payload_bytes = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "player_input_in": self.player_input_in.snapshot(),
            "player_state_in": self.player_state_in.snapshot(),
            "simulation_input_out": self.simulation_input_out.snapshot(),
            "simulation_snapshot_in": self.simulation_snapshot_in.snapshot(),
            "damage_state_in": self.damage_state_in.snapshot(),
            "collision_event_in": self.collision_event_in.snapshot(),
            "match_state_out": self.match_state_out.snapshot(),
            "damage_state_out": self.damage_state_out.snapshot(),
            "collision_event_out": self.collision_event_out.snapshot(),
            "admin_state_out": self.admin_state_out.snapshot(),
            "last_match_snapshot_bytes": self.last_match_snapshot_bytes,
            "last_simulation_snapshot_bytes": self.last_simulation_snapshot_bytes,
            "last_damage_payload_bytes": self.last_damage_payload_bytes,
            "last_collision_payload_bytes": self.last_collision_payload_bytes,
        }


class RuntimeState:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.simulation_service = SimulationServiceClient(
            base_url=settings.simulation_service_url,
            secret=settings.simulation_service_secret,
            timeout_sec=settings.simulation_service_request_timeout_sec,
        )
        self.direct_observer = DirectObserverClient(
            base_url=settings.direct_observer_url,
            secret=settings.direct_observer_secret,
            timeout_sec=settings.direct_observer_request_timeout_sec,
        )
        self.lock = asyncio.Lock()
        self.sessions_by_token: dict[str, Session] = {}
        self.lobbies_by_id: dict[str, Lobby] = {}
        self.player_to_lobby: dict[str, str] = {}
        self.matches_by_id: dict[str, Match] = {}
        self.player_connections: dict[str, WebSocket] = {}
        self.admin_connections: dict[int, WebSocket] = {}
        self.lobby_subscribers: dict[str, set[str]] = defaultdict(set)
        self.countdown_tasks: dict[str, asyncio.Task[None]] = {}
        self.loading_tasks: dict[str, asyncio.Task[None]] = {}
        self.match_tasks: dict[str, asyncio.Task[None]] = {}
        self.match_metrics: dict[str, MatchRuntimeMetrics] = {}
        self.recent_collision_pairs: dict[str, float] = {}
        self.maintenance_task: asyncio.Task[None] | None = asyncio.create_task(
            self._maintenance_loop(), name="maintenance"
        )
        self.guest_rate_limit = SlidingWindowRateLimiter(settings.guest_session_rate_limit)
        self.lobby_rate_limit = SlidingWindowRateLimiter(settings.lobby_action_rate_limit)

    async def shutdown(self) -> None:
        tasks = [
            *self.countdown_tasks.values(),
            *self.loading_tasks.values(),
            *self.match_tasks.values(),
        ]
        if self.maintenance_task is not None:
            tasks.append(self.maintenance_task)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await self.simulation_service.close()
        await self.direct_observer.close()

    def stats(self) -> dict[str, int]:
        return {
            "sessions": len(self.sessions_by_token),
            "lobbies": len(self.lobbies_by_id),
            "matches": len(self.matches_by_id),
        }

    def validate_admin_token(self, token: str | None) -> None:
        if self.settings.admin_token and token != self.settings.admin_token:
            raise unauthorized("Invalid admin token")

    async def create_guest_session(self, player_name: str, client_host: str) -> Session:
        if not self.guest_rate_limit.hit(client_host or "unknown"):
            raise invalid_request("Guest session rate limit exceeded")
        created_at = utcnow()
        session = Session(
            session_id=f"sess_{uuid4().hex[:12]}",
            player_id=f"player_{uuid4().hex[:12]}",
            player_name=player_name,
            session_token=token_urlsafe(32),
            created_at=created_at,
            expires_at=created_at + timedelta(hours=self.settings.session_ttl_hours),
        )
        async with self.lock:
            self.sessions_by_token[session.session_token] = session
        logger.info("session_created session_id=%s player_id=%s", session.session_id, session.player_id)
        return session

    async def resolve_session(self, token: str) -> Session:
        async with self.lock:
            session = self.sessions_by_token.get(token)
        if session is None or session.expires_at <= utcnow():
            raise unauthorized()
        return session

    async def list_lobbies(
        self,
        *,
        status: str | None,
        map_id: str | None,
        page: int,
        page_size: int,
    ) -> tuple[list[dict[str, Any]], int]:
        async with self.lock:
            lobbies = list(self.lobbies_by_id.values())
        if status:
            lobbies = [lobby for lobby in lobbies if lobby.status.value == status]
        if map_id:
            lobbies = [lobby for lobby in lobbies if lobby.map_id == map_id]
        total = len(lobbies)
        start = (page - 1) * page_size
        end = start + page_size
        return [self._serialize_lobby_summary(item) for item in lobbies[start:end]], total

    async def get_lobby(self, lobby_id: str) -> dict[str, Any]:
        async with self.lock:
            lobby = self.lobbies_by_id.get(lobby_id)
            if lobby is None:
                raise lobby_not_found()
            return self._serialize_lobby_detail(lobby)

    async def create_lobby(
        self,
        *,
        session_token: str,
        name: str,
        map_id: str,
        max_players: int,
        car_config: dict[str, Any],
    ) -> dict[str, Any]:
        session = await self.resolve_session(session_token)
        self._check_lobby_rate_limit(session.player_id)
        spawn_points = self._get_map_spawn_points(map_id)
        if max_players > len(spawn_points):
            raise invalid_request(f"Map '{map_id}' supports at most {len(spawn_points)} players")
        created_at = utcnow()
        async with self.lock:
            if len([lobby for lobby in self.lobbies_by_id.values() if lobby.status == LobbyStatus.waiting]) >= self.settings.max_waiting_lobbies:
                raise invalid_request("Maximum waiting lobbies reached")
            if session.player_id in self.player_to_lobby:
                raise player_already_in_lobby()
            lobby_id = f"lobby_{uuid4().hex[:12]}"
            player = LobbyPlayer(
                player_id=session.player_id,
                player_name=session.player_name,
                connection_state=self._player_connection_state(session.player_id),
                joined_at=utcnow(),
                car_config=car_config,
            )
            lobby = Lobby(
                lobby_id=lobby_id,
                name=name,
                status=LobbyStatus.waiting,
                map_id=map_id,
                max_players=max_players,
                owner_player_id=session.player_id,
                players={session.player_id: player},
                created_at=created_at,
                expires_at=created_at + timedelta(seconds=self.settings.lobby_ttl_seconds),
            )
            self.lobbies_by_id[lobby_id] = lobby
            self.player_to_lobby[session.player_id] = lobby_id
        logger.info("lobby_created lobby_id=%s player_id=%s", lobby_id, session.player_id)
        await self._broadcast_lobby_snapshot(lobby_id)
        await self._broadcast_admin_lobby_change(lobby_id)
        await self._maybe_start_lobby(lobby_id)
        return {"lobby_id": lobby_id, "status": lobby.status.value}

    async def join_lobby(self, *, session_token: str, lobby_id: str, car_config: dict[str, Any]) -> dict[str, Any]:
        session = await self.resolve_session(session_token)
        self._check_lobby_rate_limit(session.player_id)
        async with self.lock:
            lobby = self.lobbies_by_id.get(lobby_id)
            if lobby is None:
                raise lobby_not_found()
            if lobby.status != LobbyStatus.waiting:
                raise lobby_already_started()
            if session.player_id in self.player_to_lobby and self.player_to_lobby[session.player_id] != lobby_id:
                raise player_already_in_lobby()
            if session.player_id in lobby.players:
                raise player_already_in_lobby()
            if len(lobby.players) >= lobby.max_players:
                raise lobby_full()
            player = LobbyPlayer(
                player_id=session.player_id,
                player_name=session.player_name,
                connection_state=self._player_connection_state(session.player_id),
                joined_at=utcnow(),
                car_config=car_config,
            )
            lobby.players[session.player_id] = player
            self.player_to_lobby[session.player_id] = lobby_id
            lobby_snapshot = self._serialize_lobby_detail(lobby)
        logger.info("lobby_join lobby_id=%s player_id=%s", lobby_id, session.player_id)
        await self._broadcast_lobby_event(
            lobby_id,
            {
                "type": "lobby_player_joined",
                "lobby_id": lobby_id,
                "player": self._serialize_lobby_player(player, include_car_config=False),
            },
        )
        await self._broadcast_lobby_snapshot(lobby_id, precomputed=lobby_snapshot)
        await self._broadcast_admin_lobby_change(lobby_id)
        await self._maybe_start_lobby(lobby_id)
        return {"lobby_id": lobby_id, "player_id": session.player_id, "joined": True}

    async def leave_lobby(self, *, session_token: str, lobby_id: str) -> dict[str, Any]:
        session = await self.resolve_session(session_token)
        self._check_lobby_rate_limit(session.player_id)
        deleted_lobby = False
        countdown_cancelled = False
        async with self.lock:
            lobby = self.lobbies_by_id.get(lobby_id)
            if lobby is None:
                raise lobby_not_found()
            if session.player_id not in lobby.players:
                raise player_not_in_lobby()
            del lobby.players[session.player_id]
            self.player_to_lobby.pop(session.player_id, None)
            if not lobby.players:
                self.lobbies_by_id.pop(lobby_id, None)
                self.lobby_subscribers.pop(lobby_id, None)
                task = self.countdown_tasks.pop(lobby_id, None)
                if task is not None:
                    task.cancel()
                    countdown_cancelled = True
                deleted_lobby = True
            else:
                if lobby.owner_player_id == session.player_id:
                    lobby.owner_player_id = next(iter(lobby.players))
                if self._cancel_countdown_if_not_ready_locked(lobby):
                    countdown_cancelled = True
        logger.info("lobby_leave lobby_id=%s player_id=%s", lobby_id, session.player_id)
        await self._broadcast_lobby_event(
            lobby_id,
            {"type": "lobby_player_left", "lobby_id": lobby_id, "player_id": session.player_id},
        )
        if not deleted_lobby:
            await self._broadcast_lobby_snapshot(lobby_id)
        await self._broadcast_admin_lobbies_snapshot()
        if not deleted_lobby:
            await self._broadcast_admin_lobby_change(lobby_id)
            if countdown_cancelled:
                await self._broadcast_lobby_snapshot(lobby_id)
        return {"left": True}

    async def update_car_config(self, *, session_token: str, lobby_id: str, car_config: dict[str, Any]) -> dict[str, Any]:
        session = await self.resolve_session(session_token)
        self._check_lobby_rate_limit(session.player_id)
        countdown_cancelled = False
        async with self.lock:
            lobby = self.lobbies_by_id.get(lobby_id)
            if lobby is None:
                raise lobby_not_found()
            player = lobby.players.get(session.player_id)
            if player is None:
                raise player_not_in_lobby()
            player.car_config = car_config
            countdown_cancelled = self._cancel_countdown_if_not_ready_locked(lobby)
        logger.info("lobby_car_config_updated lobby_id=%s player_id=%s", lobby_id, session.player_id)
        await self._broadcast_lobby_snapshot(lobby_id)
        await self._broadcast_admin_lobby_change(lobby_id)
        await self._maybe_start_lobby(lobby_id)
        return {"updated": True}

    async def start_solo(self, *, session_token: str, lobby_id: str) -> dict[str, Any]:
        session = await self.resolve_session(session_token)
        self._check_lobby_rate_limit(session.player_id)

        async with self.lock:
            lobby = self.lobbies_by_id.get(lobby_id)
            if lobby is None:
                raise lobby_not_found()
            if lobby.status != LobbyStatus.waiting:
                raise lobby_already_started()
            if session.player_id not in lobby.players:
                raise player_not_in_lobby()
            if lobby.owner_player_id != session.player_id:
                raise invalid_request("Only the lobby owner can start solo")

            human_players = [player for player in lobby.players.values() if not player.is_server_controlled]
            if len(human_players) != 1:
                raise invalid_request("Start Solo requires exactly one human player in the lobby")
            if any(player.is_server_controlled for player in lobby.players.values()):
                raise invalid_request("Solo starter car already exists in this lobby")

            owner_player = lobby.players[session.player_id]
            if not owner_player.car_config:
                raise invalid_request("Owner car config is required before solo start")

            server_player = LobbyPlayer(
                player_id=f"server_bot_{uuid4().hex[:12]}",
                player_name="Idle Server Car",
                connection_state=ConnectionState.connected,
                joined_at=utcnow(),
                car_config=json.loads(json.dumps(owner_player.car_config)),
                is_server_controlled=True,
            )
            lobby.players[server_player.player_id] = server_player
            lobby.status = LobbyStatus.starting
            lobby_snapshot = self._serialize_lobby_detail(lobby)
            match = self._create_match_locked(lobby)

        logger.info("lobby_start_solo lobby_id=%s owner_player_id=%s match_id=%s", lobby_id, session.player_id, match.match_id)
        await self._broadcast_lobby_event(
            lobby_id,
            {
                "type": "lobby_starting",
                "lobby_id": lobby_id,
                "countdown_sec": 0,
            },
        )
        await self._broadcast_lobby_snapshot(lobby_id, precomputed=lobby_snapshot)
        await self._finish_match_creation(lobby_id, match)
        return {
            "started": True,
            "match_id": match.match_id,
            "server_player_id": server_player.player_id,
        }

    async def get_match(self, match_id: str) -> dict[str, Any]:
        async with self.lock:
            match = self.matches_by_id.get(match_id)
            if match is None:
                raise match_not_found()
            return self._serialize_match_info(match)

    async def list_admin_lobbies(self) -> dict[str, Any]:
        async with self.lock:
            items = [self._serialize_admin_lobby(lobby) for lobby in self._sorted_lobbies()]
        return {"items": items}

    async def get_admin_lobby(self, lobby_id: str) -> dict[str, Any]:
        async with self.lock:
            lobby = self.lobbies_by_id.get(lobby_id)
            if lobby is None:
                raise lobby_not_found()
            return self._serialize_admin_lobby(lobby)

    async def list_admin_matches(self) -> dict[str, Any]:
        async with self.lock:
            items = [self._serialize_admin_match_summary(match) for match in self._sorted_matches()]
        items.extend(await self._list_direct_admin_matches())
        items.sort(key=self._admin_match_sort_key)
        return {"items": items}

    async def get_admin_match(self, match_id: str) -> dict[str, Any]:
        async with self.lock:
            match = self.matches_by_id.get(match_id)
            if match is not None:
                return self._serialize_admin_match_detail(match)

        direct_match = await self._get_direct_admin_match(match_id)
        if direct_match is not None:
            return direct_match
        raise match_not_found()

    async def get_admin_game_settings(self) -> dict[str, Any]:
        if not self.direct_observer.enabled:
            return self._build_admin_game_settings_response(
                source="backend_runtime",
                damage_config=None,
                note="Direct observer is not configured for global runtime settings.",
            )

        note: str | None = None
        damage_config: dict[str, Any] | None = None

        try:
            damage_config = await self.direct_observer.get_global_damage_config()
        except Exception:
            logger.warning("direct_observer_global_damage_config_get_failed", exc_info=True)

        if damage_config is None:
            fallback_room_id = await self._get_direct_observer_fallback_room_id()
            if fallback_room_id:
                try:
                    damage_config = await self.direct_observer.get_damage_config(fallback_room_id)
                    note = f"Global settings are currently proxied through active room {fallback_room_id}."
                except Exception:
                    logger.warning(
                        "direct_observer_room_damage_config_get_failed room_id=%s",
                        fallback_room_id,
                        exc_info=True,
                    )
            else:
                note = "Dedicated observer does not expose global settings yet and there is no active direct room for fallback."

        return self._build_admin_game_settings_response(
            source="purrnet_direct",
            damage_config=damage_config,
            note=note,
        )

    async def update_admin_game_settings(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not self.direct_observer.enabled:
            raise invalid_request("Direct observer is not configured")

        damage_fields = self._extract_game_settings_section_fields(payload, "damage")
        if damage_fields is None:
            raise invalid_request("Game settings payload must include a damage section")

        note: str | None = None
        damage_config: dict[str, Any] | None = None

        try:
            damage_config = await self.direct_observer.update_global_damage_config(damage_fields)
        except Exception:
            logger.warning("direct_observer_global_damage_config_update_failed", exc_info=True)

        if damage_config is None:
            fallback_room_id = await self._get_direct_observer_fallback_room_id()
            if not fallback_room_id:
                raise invalid_request(
                    "Dedicated observer global settings endpoint is unavailable and there is no active direct room for fallback"
                )

            try:
                damage_config = await self.direct_observer.update_damage_config(fallback_room_id, damage_fields)
                note = f"Global settings were applied through active room {fallback_room_id}."
            except Exception as exc:
                logger.warning(
                    "direct_observer_room_damage_config_update_failed room_id=%s",
                    fallback_room_id,
                    exc_info=True,
                )
                raise invalid_request(f"Failed to update damage settings: {exc}") from exc

        return self._build_admin_game_settings_response(
            source="purrnet_direct",
            damage_config=damage_config,
            note=note,
        )

    async def close_lobby(self, lobby_id: str, *, reason: str) -> dict[str, Any]:
        async with self.lock:
            close_result = self._close_lobby_locked(lobby_id, reason)
        if close_result is None:
            raise lobby_not_found()
        await self._finalize_lobby_close(close_result)
        logger.info(
            "lobby_closed lobby_id=%s match_id=%s reason=%s",
            close_result["lobby_id"],
            close_result["match_id"],
            reason,
        )
        return {
            "lobby_id": close_result["lobby_id"],
            "match_id": close_result["match_id"],
            "closed": True,
            "reason": reason,
        }

    async def register_connection(self, token: str, websocket: WebSocket) -> Session:
        session = await self.resolve_session(token)
        await websocket.accept()
        async with self.lock:
            old_socket = self.player_connections.get(session.player_id)
            self.player_connections[session.player_id] = websocket
            lobby_id = self.player_to_lobby.get(session.player_id)
            if lobby_id and lobby_id in self.lobbies_by_id and session.player_id in self.lobbies_by_id[lobby_id].players:
                self.lobbies_by_id[lobby_id].players[session.player_id].connection_state = ConnectionState.connected
                self._cancel_countdown_if_not_ready_locked(self.lobbies_by_id[lobby_id])
        if old_socket is not None and old_socket is not websocket:
            await self._safe_close(old_socket, code=4001)
        await websocket.send_json(
            {
                "type": "welcome",
                "player_id": session.player_id,
                "server_time": int(time.time() * 1000),
            }
        )
        logger.info("ws_connected player_id=%s", session.player_id)
        if lobby_id:
            await self._broadcast_lobby_snapshot(lobby_id)
            await self._broadcast_admin_lobby_change(lobby_id)
            await self._maybe_start_lobby(lobby_id)
        return session

    async def unregister_connection(self, player_id: str) -> None:
        close_result: dict[str, Any] | None = None
        async with self.lock:
            self.player_connections.pop(player_id, None)
            lobby_id = self.player_to_lobby.get(player_id)
            match_id = None
            if lobby_id and lobby_id in self.lobbies_by_id and player_id in self.lobbies_by_id[lobby_id].players:
                lobby = self.lobbies_by_id[lobby_id]
                lobby.players[player_id].connection_state = ConnectionState.disconnected
                match_id = lobby.match_id
                self._cancel_countdown_if_not_ready_locked(lobby)
                if lobby.status in {LobbyStatus.waiting, LobbyStatus.starting} and self._all_human_players_disconnected_locked(lobby):
                    close_result = self._close_lobby_locked(lobby_id, "all_players_disconnected")
                    lobby_id = None
                    match_id = close_result["match_id"] if close_result is not None else match_id
            for subscribers in self.lobby_subscribers.values():
                subscribers.discard(player_id)
        logger.info("ws_disconnected player_id=%s", player_id)
        if close_result is not None:
            await self._finalize_lobby_close(close_result)
            return
        if lobby_id:
            await self._broadcast_lobby_snapshot(lobby_id)
            await self._broadcast_admin_lobby_change(lobby_id)
        if match_id:
            await self._broadcast_match_players(
                match_id,
                {"type": "player_disconnected", "match_id": match_id, "player_id": player_id},
            )
            await self._broadcast_admin_match_change(match_id)

    async def register_admin_connection(self, token: str | None, websocket: WebSocket) -> None:
        self.validate_admin_token(token)
        await websocket.accept()
        async with self.lock:
            self.admin_connections[id(websocket)] = websocket
        logger.info("admin_ws_connected")
        await self._send_to_admin(
            websocket,
            {
                "type": "admin_connected",
                "connected": True,
                "server_time": int(time.time() * 1000),
            },
        )
        await self._send_to_admin(websocket, {"type": "admin_lobbies_snapshot", **(await self.list_admin_lobbies())})
        await self._send_to_admin(websocket, {"type": "admin_matches_snapshot", **(await self.list_admin_matches())})

    async def unregister_admin_connection(self, websocket: WebSocket) -> None:
        async with self.lock:
            self.admin_connections.pop(id(websocket), None)
        logger.info("admin_ws_disconnected")

    async def admin_websocket_loop(self, websocket: WebSocket) -> None:
        while True:
            try:
                raw = await websocket.receive_text()
            except WebSocketDisconnect:
                break
            if not raw:
                continue
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if payload.get("type") == "ping":
                await self._send_to_admin(websocket, {"type": "pong", "time": int(time.time() * 1000)})

    async def websocket_loop(self, player_id: str, websocket: WebSocket) -> None:
        while True:
            try:
                raw = await websocket.receive_text()
            except WebSocketDisconnect:
                break
            if len(raw.encode("utf-8")) > self.settings.websocket_message_max_bytes:
                await self._send_ws_error(player_id, "INVALID_REQUEST", "WebSocket payload too large")
                continue
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                await self._send_ws_error(player_id, "INVALID_REQUEST", "Invalid JSON")
                continue
            await self._handle_ws_message(player_id, payload)

    async def _handle_ws_message(self, player_id: str, payload: dict[str, Any]) -> None:
        message_type = payload.get("type")
        if message_type == "subscribe_lobby":
            await self.subscribe_lobby(player_id, str(payload.get("lobby_id", "")))
            return
        if message_type == "unsubscribe_lobby":
            await self.unsubscribe_lobby(player_id, str(payload.get("lobby_id", "")))
            return
        if message_type == "match_loaded":
            await self.mark_match_loaded(player_id, str(payload.get("match_id", "")))
            return
        if message_type == "player_input":
            await self.apply_player_input(player_id, payload)
            return
        if message_type == "player_state":
            await self.apply_player_state(player_id, payload)
            return
        if message_type == "damage_state":
            await self.apply_damage_state(player_id, payload)
            return
        if message_type == "collision_event":
            await self.apply_collision_event(player_id, payload)
            return
        if message_type == "ping":
            return
        await self._send_ws_error(player_id, "INVALID_REQUEST", "Unsupported message type")

    async def subscribe_lobby(self, player_id: str, lobby_id: str) -> None:
        async with self.lock:
            lobby = self.lobbies_by_id.get(lobby_id)
            if lobby is None:
                raise lobby_not_found()
            self.lobby_subscribers[lobby_id].add(player_id)
            snapshot = self._serialize_lobby_detail(lobby)
        await self._send_to_player(player_id, {"type": "lobby_snapshot", "lobby": snapshot})

    async def unsubscribe_lobby(self, player_id: str, lobby_id: str) -> None:
        async with self.lock:
            self.lobby_subscribers[lobby_id].discard(player_id)

    async def mark_match_loaded(self, player_id: str, match_id: str) -> None:
        lobby_id: str | None = None
        async with self.lock:
            match = self.matches_by_id.get(match_id)
            if match is None:
                raise match_not_found()
            player = match.players.get(player_id)
            if player is None:
                raise invalid_request("Player is not part of the match")
            player.loaded = True
            player.last_snapshot_at = utcnow()
            lobby = self.lobbies_by_id.get(match.lobby_id)
            if lobby and player_id in lobby.players:
                lobby.players[player_id].connection_state = ConnectionState.loading
                lobby_id = lobby.lobby_id
        logger.info("match_loaded match_id=%s player_id=%s", match_id, player_id)
        if lobby_id:
            await self._broadcast_admin_lobby_change(lobby_id)

    async def apply_player_state(self, player_id: str, payload: dict[str, Any]) -> None:
        input_payload = payload.get("input") or {}
        if not input_payload:
            input_payload = {
                "throttle": 0.0,
                "steer": 0.0,
                "brake": False,
                "handbrake": False,
                "nitro": False,
            }
        await self._apply_player_input_payload(
            player_id,
            payload,
            input_payload=input_payload,
            state_payload=payload.get("state") or {},
            metric_key="player_state",
        )

    async def apply_player_input(self, player_id: str, payload: dict[str, Any]) -> None:
        await self._apply_player_input_payload(
            player_id,
            payload,
            input_payload=payload.get("input") or {},
            state_payload=payload.get("state") or {},
            metric_key="player_input",
        )

    async def _apply_player_input_payload(
        self,
        player_id: str,
        payload: dict[str, Any],
        *,
        input_payload: dict[str, Any],
        state_payload: dict[str, Any],
        metric_key: str,
    ) -> None:
        match_id = str(payload.get("match_id", ""))
        seq = int(payload.get("seq", -1))
        client_time_ms = int(payload.get("client_time", 0) or 0)
        async with self.lock:
            match = self.matches_by_id.get(match_id)
            if match is None:
                raise match_not_found()
            player = match.players.get(player_id)
            if player is None:
                raise invalid_request("Player is not part of the match")
            if match.status != MatchStatus.running or seq <= player.last_input_seq:
                return
            player.last_input_seq = seq
            authoritative_room = self._is_authoritative_room_status(match.room_status)
            if state_payload:
                player.last_state_seq = max(player.last_state_seq, seq)
            player.last_snapshot_at = utcnow()
            player.client_time_ms = client_time_ms
            player.server_received_time_ms = int(time.time() * 1000)
            player.throttle = max(-1.0, min(1.0, float(input_payload.get("throttle", player.throttle) or 0.0)))
            player.steer = max(-1.0, min(1.0, float(input_payload.get("steer", player.steer) or 0.0)))
            player.brake = bool(input_payload.get("brake", player.brake))
            player.handbrake = bool(input_payload.get("handbrake", player.handbrake))
            player.nitro = bool(input_payload.get("nitro", player.nitro))
            if state_payload and not authoritative_room:
                player.position = self._coerce_vec3(state_payload.get("position"), player.position)
                player.rotation = self._coerce_vec3(state_payload.get("rotation"), player.rotation)
                player.velocity = self._coerce_vec3(state_payload.get("velocity"), player.velocity)
                player.angular_velocity = self._coerce_vec3(state_payload.get("angular_velocity"), player.angular_velocity)
                player.wheel_states = self._coerce_wheel_states(state_payload.get("wheel_states"), player.wheel_states)
            player.disconnected_announced = False
            metrics = self._get_match_metrics(match_id)
            payload_size = self._estimate_payload_size(payload)
            metrics.player_input_in.add(payload_size)
            if metric_key == "player_state":
                metrics.player_state_in.add(payload_size)

    async def apply_damage_state(self, player_id: str, payload: dict[str, Any]) -> None:
        match_id = str(payload.get("match_id", ""))
        revision = int(payload.get("revision", 0))
        metrics: MatchRuntimeMetrics | None = None
        payload_size = 0
        async with self.lock:
            match = self.matches_by_id.get(match_id)
            if match is None:
                raise match_not_found()
            player = match.players.get(player_id)
            if player is None:
                raise invalid_request("Player is not part of the match")
            if self._is_authoritative_room_status(match.room_status):
                return
            if match.status != MatchStatus.running or revision <= player.damage_revision:
                return

            width = max(0, int(payload.get("width", 0)))
            height = max(0, int(payload.get("height", 0)))
            map_b64 = str(payload.get("map_b64", "") or "")
            player.damage_revision = revision
            player.damage_width = width
            player.damage_height = height
            player.damage_map_b64 = map_b64
            player.last_damage_at = utcnow()
            metrics = self._get_match_metrics(match_id)
            payload_size = self._estimate_payload_size(payload)
            metrics.damage_state_in.add(payload_size)
            metrics.last_damage_payload_bytes = payload_size

        await self._broadcast_match_players(match_id, payload)
        if metrics is not None:
            metrics.damage_state_out.add(payload_size)
        await self._broadcast_admin_match_change(match_id)

    async def apply_collision_event(self, player_id: str, payload: dict[str, Any]) -> None:
        match_id = str(payload.get("match_id", ""))
        primary_player_id = str(payload.get("primary_player_id", ""))
        secondary_player_id = str(payload.get("secondary_player_id", ""))
        collision_pair_key = self._build_collision_pair_key(match_id, primary_player_id, secondary_player_id)
        now = time.time()
        metrics: MatchRuntimeMetrics | None = None
        async with self.lock:
            match = self.matches_by_id.get(match_id)
            if match is None:
                raise match_not_found()
            primary = match.players.get(primary_player_id)
            secondary = match.players.get(secondary_player_id)
            if primary is None or secondary is None:
                raise invalid_request("Collision participants are not part of the match")
            if self._is_authoritative_room_status(match.room_status):
                return
            if primary.player_id == secondary.player_id:
                raise invalid_request("Collision participants must be different players")
            if player_id != primary_player_id:
                raise invalid_request("Collision report sender must match primary_player_id")
            last_collision_at = self.recent_collision_pairs.get(collision_pair_key, 0.0)
            if now - last_collision_at < 0.12:
                return
            self.recent_collision_pairs[collision_pair_key] = now
            metrics = self._get_match_metrics(match_id)

        world_point = self._coerce_vec3(payload.get("world_point"), Vec3())
        world_normal = self._coerce_vec3(payload.get("world_normal"), Vec3(0.0, 1.0, 0.0))
        relative_velocity = self._coerce_vec3(payload.get("relative_velocity"), Vec3())
        impulse_vector = self._coerce_vec3(payload.get("impulse_vector"), Vec3())
        impulse_magnitude = float(payload.get("impulse_magnitude", 0.0) or 0.0)
        payload_size = self._estimate_payload_size(payload)
        if metrics is not None:
            metrics.collision_event_in.add(payload_size)
            metrics.last_collision_payload_bytes = payload_size

        await self._broadcast_match_players(
            match_id,
            {
                "type": "collision_event",
                "match_id": match_id,
                "primary_player_id": primary_player_id,
                "secondary_player_id": secondary_player_id,
                "world_point": world_point.as_dict(),
                "world_normal": world_normal.as_dict(),
                "relative_velocity": relative_velocity.as_dict(),
                "impulse_vector": impulse_vector.as_dict(),
                "impulse_magnitude": impulse_magnitude,
            },
        )
        if metrics is not None:
            metrics.collision_event_out.add(payload_size)
        await self._broadcast_match_players(
            match_id,
            {
                "type": "collision_event",
                "match_id": match_id,
                "primary_player_id": secondary_player_id,
                "secondary_player_id": primary_player_id,
                "world_point": world_point.as_dict(),
                "world_normal": Vec3(-world_normal.x, -world_normal.y, -world_normal.z).as_dict(),
                "relative_velocity": Vec3(-relative_velocity.x, -relative_velocity.y, -relative_velocity.z).as_dict(),
                "impulse_vector": Vec3(-impulse_vector.x, -impulse_vector.y, -impulse_vector.z).as_dict(),
                "impulse_magnitude": impulse_magnitude,
            },
        )
        if metrics is not None:
            metrics.collision_event_out.add(payload_size)

    async def _maintenance_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(max(1, self.settings.maintenance_interval_sec))
                await self._expire_lobbies()
        except asyncio.CancelledError:
            raise

    async def _expire_lobbies(self) -> None:
        expired: list[dict[str, Any]] = []
        async with self.lock:
            now = utcnow()
            for lobby_id, lobby in list(self.lobbies_by_id.items()):
                if lobby.status not in {LobbyStatus.waiting, LobbyStatus.starting}:
                    continue
                if lobby.expires_at > now:
                    continue
                close_result = self._close_lobby_locked(lobby_id, "timeout")
                if close_result is not None:
                    expired.append(close_result)

        for close_result in expired:
            logger.info("lobby_expired lobby_id=%s", close_result["lobby_id"])
            await self._finalize_lobby_close(close_result)

    @staticmethod
    def _all_human_players_disconnected_locked(lobby: Lobby) -> bool:
        human_players = [player for player in lobby.players.values() if not player.is_server_controlled]
        if not human_players:
            return False
        return all(player.connection_state == ConnectionState.disconnected for player in human_players)

    def _close_lobby_locked(self, lobby_id: str, reason: str) -> dict[str, Any] | None:
        lobby = self.lobbies_by_id.pop(lobby_id, None)
        if lobby is None:
            return None

        countdown_task = self.countdown_tasks.pop(lobby_id, None)
        loading_task = None
        match_task = None
        match_id = lobby.match_id
        match_player_ids: set[str] = set()
        server_tick = 0
        if match_id:
            loading_task = self.loading_tasks.pop(match_id, None)
            match_task = self.match_tasks.pop(match_id, None)
            match = self.matches_by_id.pop(match_id, None)
            if match is not None:
                match_player_ids = set(match.players.keys())
                server_tick = match.server_tick
                self.match_metrics.pop(match_id, None)
        for player_id in lobby.players:
            self.player_to_lobby.pop(player_id, None)
        self.lobby_subscribers.pop(lobby_id, None)
        if match_id:
            prefix = f"{match_id}:"
            self.recent_collision_pairs = {
                key: value for key, value in self.recent_collision_pairs.items() if not key.startswith(prefix)
            }
        return {
            "lobby_id": lobby_id,
            "match_id": match_id,
            "reason": reason,
            "player_ids": set(lobby.players.keys()),
            "match_player_ids": match_player_ids,
            "server_tick": server_tick,
            "countdown_task": countdown_task,
            "loading_task": loading_task,
            "match_task": match_task,
        }

    async def _finalize_lobby_close(self, close_result: dict[str, Any]) -> None:
        countdown_task = close_result.get("countdown_task")
        loading_task = close_result.get("loading_task")
        match_task = close_result.get("match_task")
        current_task = asyncio.current_task()
        for task in (countdown_task, loading_task, match_task):
            if task is not None and task is not current_task:
                task.cancel()

        match_id = close_result.get("match_id")
        if match_id:
            try:
                await self.simulation_service.release_room(match_id)
            except Exception:
                logger.warning("simulation_room_release_failed match_id=%s", match_id, exc_info=True)

        player_ids = set(close_result.get("player_ids") or set())
        match_player_ids = set(close_result.get("match_player_ids") or set())
        reason = str(close_result.get("reason", "closed") or "closed")

        if match_id and match_player_ids:
            await self._broadcast_to_players(
                match_player_ids,
                {
                    "type": "match_finished",
                    "match_id": match_id,
                    "reason": reason,
                    "server_tick": int(close_result.get("server_tick", 0) or 0),
                },
            )

        if player_ids:
            await self._broadcast_to_players(
                player_ids,
                {"type": "lobby_closed", "lobby_id": close_result["lobby_id"], "reason": reason},
            )

        await self._broadcast_admin_lobbies_snapshot()
        await self._broadcast_admin_matches_snapshot()

    def _cancel_countdown_if_not_ready_locked(self, lobby: Lobby) -> bool:
        if lobby is None or lobby.status != LobbyStatus.starting:
            return False
        if self._is_lobby_ready_to_start_locked(lobby):
            return False

        lobby.status = LobbyStatus.waiting
        task = self.countdown_tasks.pop(lobby.lobby_id, None)
        if task is not None:
            task.cancel()
        return task is not None

    def _is_lobby_ready_to_start_locked(self, lobby: Lobby) -> bool:
        if lobby is None or lobby.status not in {LobbyStatus.waiting, LobbyStatus.starting}:
            return False
        if len(lobby.players) != lobby.max_players:
            return False
        if not all(player.car_config for player in lobby.players.values()):
            return False
        return all(
            player.is_server_controlled or player.player_id in self.player_connections
            for player in lobby.players.values()
        )

    @staticmethod
    def _coerce_vec3(payload: dict[str, Any] | None, fallback: Vec3) -> Vec3:
        if not isinstance(payload, dict):
            return Vec3(fallback.x, fallback.y, fallback.z)

        def read(key: str, default: float) -> float:
            try:
                return float(payload.get(key, default))
            except (TypeError, ValueError):
                return default

        return Vec3(
            x=read("x", fallback.x),
            y=read("y", fallback.y),
            z=read("z", fallback.z),
        )

    @classmethod
    def _coerce_wheel_states(cls, payload: Any, fallback: list[dict[str, Vec3]]) -> list[dict[str, Vec3]]:
        if not isinstance(payload, list):
            return list(fallback or [])

        states: list[dict[str, Vec3]] = []
        for item in payload[:4]:
            if not isinstance(item, dict):
                continue
            position = cls._coerce_vec3(item.get("position"), Vec3())
            rotation = cls._coerce_vec3(item.get("rotation"), Vec3())
            states.append({"position": position, "rotation": rotation})
        return states

    async def _maybe_start_lobby(self, lobby_id: str) -> None:
        async with self.lock:
            lobby = self.lobbies_by_id.get(lobby_id)
            if lobby is None or lobby.status != LobbyStatus.waiting:
                return
            if not self._is_lobby_ready_to_start_locked(lobby):
                return
            if lobby_id in self.countdown_tasks:
                return
            lobby.status = LobbyStatus.starting
            lobby_snapshot = self._serialize_lobby_detail(lobby)
            task = asyncio.create_task(self._countdown_to_match(lobby_id), name=f"countdown:{lobby_id}")
            self.countdown_tasks[lobby_id] = task
        logger.info("lobby_autostart lobby_id=%s", lobby_id)
        await self._broadcast_lobby_event(
            lobby_id,
            {
                "type": "lobby_starting",
                "lobby_id": lobby_id,
                "countdown_sec": self.settings.auto_start_countdown_sec,
            },
        )
        await self._broadcast_lobby_snapshot(lobby_id, precomputed=lobby_snapshot)
        await self._broadcast_admin_lobby_change(lobby_id)

    async def _countdown_to_match(self, lobby_id: str) -> None:
        try:
            await asyncio.sleep(self.settings.auto_start_countdown_sec)
            match: Match | None = None
            async with self.lock:
                lobby = self.lobbies_by_id.get(lobby_id)
                if lobby is None or lobby.status != LobbyStatus.starting:
                    return
                match = self._create_match_locked(lobby)
            if match is not None:
                logger.info("match_created lobby_id=%s match_id=%s", lobby_id, match.match_id)
                await self._finish_match_creation(lobby_id, match)
        finally:
            async with self.lock:
                self.countdown_tasks.pop(lobby_id, None)

    def _create_match_locked(self, lobby: Lobby) -> Match:
        spawn_assignments = self._build_spawn_assignments(lobby)
        match_id = f"match_{uuid4().hex[:12]}"
        players = {
            player_id: MatchPlayer(
                player_id=player.player_id,
                player_name=player.player_name,
                car_config=json.loads(json.dumps(player.car_config)),
                authority_order=index,
                spawn_point_id=spawn_assignments[player_id].spawn_point_id,
                spawn_position=Vec3(
                    spawn_assignments[player_id].position.x,
                    spawn_assignments[player_id].position.y,
                    spawn_assignments[player_id].position.z,
                ),
                spawn_rotation=Vec3(
                    spawn_assignments[player_id].rotation.x,
                    spawn_assignments[player_id].rotation.y,
                    spawn_assignments[player_id].rotation.z,
                ),
                is_server_controlled=player.is_server_controlled,
                position=Vec3(
                    spawn_assignments[player_id].position.x,
                    spawn_assignments[player_id].position.y,
                    spawn_assignments[player_id].position.z,
                ),
                rotation=Vec3(
                    spawn_assignments[player_id].rotation.x,
                    spawn_assignments[player_id].rotation.y,
                    spawn_assignments[player_id].rotation.z,
                ),
                loaded=player.is_server_controlled,
            )
            for index, (player_id, player) in enumerate(sorted(lobby.players.items(), key=lambda item: (item[1].joined_at, item[0])))
        }
        match = Match(
            match_id=match_id,
            lobby_id=lobby.lobby_id,
            status=MatchStatus.starting,
            map_id=lobby.map_id,
            tick_rate=self.settings.match_tick_rate,
            broadcast_rate=self.settings.match_broadcast_rate,
            players=players,
            created_at=utcnow(),
            load_deadline=utcnow() + timedelta(seconds=self.settings.match_load_timeout_sec),
        )
        self.matches_by_id[match_id] = match
        self.match_metrics[match_id] = MatchRuntimeMetrics()
        lobby.match_id = match_id
        lobby.status = LobbyStatus.in_game
        return match

    async def _finish_match_creation(self, lobby_id: str, match: Match) -> None:
        await self._assign_simulation_room(match.match_id)
        await self._broadcast_lobby_event(
            lobby_id,
            {
                "type": "match_created",
                "match_id": match.match_id,
                "lobby_id": lobby_id,
                "map_id": match.map_id,
                "room_id": match.room_id,
                "room_status": match.room_status,
                "room_http_url": match.room_http_url,
                "room_ws_url": match.room_ws_url,
                "room_token": match.room_token,
                "players": [self._serialize_match_player_info(match, player) for player in match.players.values()],
            },
        )
        await self._broadcast_admin_lobby_change(lobby_id)
        await self._broadcast_admin_match_change(match.match_id)
        loading_task = asyncio.create_task(self._await_match_loaded(match.match_id), name=f"load:{match.match_id}")
        async with self.lock:
            self.loading_tasks[match.match_id] = loading_task

    async def _await_match_loaded(self, match_id: str) -> None:
        try:
            while True:
                await asyncio.sleep(0.1)
                async with self.lock:
                    match = self.matches_by_id.get(match_id)
                    if match is None or match.status != MatchStatus.starting:
                        return
                    deadline_reached = bool(match.load_deadline and utcnow() >= match.load_deadline)
                    all_loaded = all(player.loaded for player in match.players.values())
                    if not all_loaded and not deadline_reached:
                        continue
                    match.status = MatchStatus.running
                    lobby = self.lobbies_by_id.get(match.lobby_id)
                    if lobby:
                        for player_id in match.players:
                            if player_id in lobby.players:
                                lobby.players[player_id].connection_state = ConnectionState.in_game
                        lobby_id = lobby.lobby_id
                    else:
                        lobby_id = None
                logger.info("match_started match_id=%s", match_id)
                await self._broadcast_match_players(
                    match_id,
                    {"type": "match_started", "match_id": match_id, "server_tick": 0},
                )
                if lobby_id:
                    await self._broadcast_admin_lobby_change(lobby_id)
                await self._broadcast_admin_match_change(match_id)
                task = asyncio.create_task(self._run_match_loop(match_id), name=f"match:{match_id}")
                async with self.lock:
                    self.match_tasks[match_id] = task
                return
        finally:
            async with self.lock:
                self.loading_tasks.pop(match_id, None)

    async def _run_match_loop(self, match_id: str) -> None:
        async with self.lock:
            match = self.matches_by_id.get(match_id)
            if match is None:
                return
            tick_rate = max(1, int(match.tick_rate or self.settings.match_tick_rate))
            broadcast_rate = max(1, int(match.broadcast_rate or self.settings.match_broadcast_rate))

        tick_interval = 1 / tick_rate
        broadcast_every = max(1, round(tick_rate / max(1, broadcast_rate)))
        loop = asyncio.get_running_loop()
        next_tick_at = loop.time()
        try:
            while True:
                next_tick_at += tick_interval
                delay = next_tick_at - loop.time()
                if delay > 0:
                    await asyncio.sleep(delay)
                else:
                    next_tick_at = loop.time()
                snapshot = None
                close_result: dict[str, Any] | None = None
                disconnected_events: list[dict[str, Any]] = []
                authoritative_damage_payloads: list[dict[str, Any]] = []
                authoritative_collision_payloads: list[dict[str, Any]] = []
                simulation_match_id: str | None = None
                simulation_input_payload: dict[str, Any] | None = None
                use_simulation = False
                metrics = self._get_match_metrics(match_id)
                async with self.lock:
                    match = self.matches_by_id.get(match_id)
                    if match is None or match.status != MatchStatus.running:
                        return
                    if self._is_authoritative_room_status(match.room_status) and self.simulation_service.enabled:
                        simulation_match_id = match.match_id
                        simulation_input_payload = self._build_simulation_input_batch(match)
                        use_simulation = True
                    else:
                        match.server_tick += 1

                if use_simulation and simulation_match_id and simulation_input_payload is not None:
                    try:
                        input_bytes = self._estimate_payload_size(simulation_input_payload)
                        metrics.simulation_input_out.add(input_bytes)
                        await self.simulation_service.apply_inputs(simulation_match_id, simulation_input_payload)
                        simulation_snapshot = await self.simulation_service.get_snapshot(simulation_match_id)
                        snapshot_bytes = self._estimate_payload_size(simulation_snapshot or {})
                        metrics.simulation_snapshot_in.add(snapshot_bytes)
                        metrics.last_simulation_snapshot_bytes = snapshot_bytes
                        async with self.lock:
                            match = self.matches_by_id.get(match_id)
                            if match is None or match.status != MatchStatus.running:
                                return
                            (
                                authoritative_damage_payloads,
                                authoritative_collision_payloads,
                            ) = self._apply_simulation_snapshot_locked(match, simulation_snapshot or {})
                    except Exception:
                        logger.warning("simulation_snapshot_pull_failed match_id=%s", match_id, exc_info=True)
                        async with self.lock:
                            match = self.matches_by_id.get(match_id)
                            if match is not None and match.status == MatchStatus.running:
                                match.room_status = "simulation_unavailable"
                                match.server_tick += 1

                async with self.lock:
                    match = self.matches_by_id.get(match_id)
                    if match is None or match.status != MatchStatus.running:
                        return
                    now = utcnow()
                    all_players_disconnected = True
                    for player in match.players.values():
                        if (
                            now - player.last_snapshot_at >= timedelta(seconds=self.settings.disconnect_timeout_sec)
                            and not player.disconnected_announced
                        ):
                            player.disconnected_announced = True
                            disconnected_events.append(
                                {"type": "player_disconnected", "match_id": match_id, "player_id": player.player_id}
                            )
                        if (
                            player.player_id in self.player_connections
                            or now - player.last_snapshot_at < timedelta(seconds=self.settings.match_abandon_timeout_sec)
                        ):
                            all_players_disconnected = False
                    if all_players_disconnected:
                        close_result = self._close_lobby_locked(match.lobby_id, "abandoned")
                        logger.info("match_finished match_id=%s reason=abandoned", match_id)
                    elif match.server_tick % broadcast_every == 0:
                        snapshot = self._serialize_match_state(match)
                for event in disconnected_events:
                    await self._broadcast_match_players(match_id, event)
                for damage_payload in authoritative_damage_payloads:
                    damage_bytes = self._estimate_payload_size(damage_payload)
                    metrics.damage_state_in.add(damage_bytes)
                    metrics.damage_state_out.add(damage_bytes)
                    metrics.last_damage_payload_bytes = damage_bytes
                    await self._broadcast_match_players(match_id, damage_payload)
                for collision_payload in authoritative_collision_payloads:
                    collision_bytes = self._estimate_payload_size(collision_payload)
                    metrics.collision_event_in.add(collision_bytes)
                    metrics.collision_event_out.add(collision_bytes)
                    metrics.last_collision_payload_bytes = collision_bytes
                    await self._broadcast_match_players(match_id, collision_payload)
                if close_result is not None:
                    await self._finalize_lobby_close(close_result)
                    return
                if snapshot:
                    snapshot_bytes = self._estimate_payload_size(snapshot)
                    metrics.match_state_out.add(snapshot_bytes)
                    metrics.last_match_snapshot_bytes = snapshot_bytes
                    await self._broadcast_match_players(match_id, snapshot)
                    await self._broadcast_admin_match_state(match_id, snapshot)
        finally:
            async with self.lock:
                self.match_tasks.pop(match_id, None)
            try:
                await self.simulation_service.release_room(match_id)
            except Exception:
                logger.warning("simulation_room_release_failed match_id=%s", match_id, exc_info=True)

    async def _assign_simulation_room(self, match_id: str) -> None:
        async with self.lock:
            match = self.matches_by_id.get(match_id)
            if match is None:
                return
            payload = self._build_simulation_room_request(match)

        if not self.simulation_service.enabled:
            async with self.lock:
                match = self.matches_by_id.get(match_id)
                if match is not None:
                    match.room_status = "backend_fallback"
            return

        try:
            response = await self.simulation_service.reserve_room(payload)
        except Exception:
            logger.warning("simulation_room_reserve_failed match_id=%s", match_id, exc_info=True)
            async with self.lock:
                match = self.matches_by_id.get(match_id)
                if match is not None:
                    match.room_status = "allocation_failed"
            return

        if not response:
            async with self.lock:
                match = self.matches_by_id.get(match_id)
                if match is not None:
                    match.room_status = "allocation_failed"
            return

        async with self.lock:
            match = self.matches_by_id.get(match_id)
            if match is None:
                return
            match.room_id = str(response.get("room_id", "") or match_id)
            match.room_status = str(response.get("status", "allocated") or "allocated")
            match.room_http_url = str(response.get("room_http_url", "") or "") or None
            match.room_ws_url = str(response.get("room_ws_url", "") or "") or None
            match.room_token = str(response.get("room_token", "") or "") or None

    def _build_simulation_room_request(self, match: Match) -> dict[str, Any]:
        return {
            "match_id": match.match_id,
            "map_id": match.map_id,
            "tick_rate": match.tick_rate,
            "broadcast_rate": match.broadcast_rate,
            "players": [
                {
                    "player_id": player.player_id,
                    "player_name": player.player_name,
                    "authority_order": player.authority_order,
                    "spawn_point_id": player.spawn_point_id,
                    "spawn_position": player.spawn_position.as_dict(),
                    "spawn_rotation": player.spawn_rotation.as_dict(),
                    "car_config": player.car_config,
                }
                for player in match.players.values()
            ],
        }

    def _build_simulation_input_batch(self, match: Match) -> dict[str, Any]:
        return {
            "players": [
                {
                    "player_id": player.player_id,
                    "seq": player.last_input_seq,
                    "client_time": player.client_time_ms,
                    "input": {
                        "throttle": round(player.throttle, 4),
                        "steer": round(player.steer, 4),
                        "brake": player.brake,
                        "handbrake": player.handbrake,
                        "nitro": player.nitro,
                    },
                }
                for player in match.players.values()
            ]
        }

    @staticmethod
    def _is_authoritative_room_status(room_status: str | None) -> bool:
        return (room_status or "").lower() in {"allocated", "ready", "reserved", "simulating"}

    def _apply_simulation_snapshot_locked(self, match: Match, snapshot: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        damage_payloads: list[dict[str, Any]] = []
        collision_payloads: list[dict[str, Any]] = []
        if not snapshot:
            return damage_payloads, collision_payloads

        match.last_simulation_snapshot = snapshot

        try:
            match.server_tick = max(match.server_tick, int(snapshot.get("server_tick", match.server_tick) or match.server_tick))
        except (TypeError, ValueError):
            pass

        snapshot_status = str(snapshot.get("status", "") or "").strip()
        if snapshot_status:
            match.room_status = snapshot_status
        elif self._is_authoritative_room_status(match.room_status):
            match.room_status = "simulating"

        players_payload = snapshot.get("players")
        if isinstance(players_payload, list):
            for item in players_payload:
                if not isinstance(item, dict):
                    continue
                player_id = str(item.get("player_id", "") or "")
                if not player_id:
                    continue
                player = match.players.get(player_id)
                if player is None:
                    continue

                try:
                    player.last_input_seq = max(player.last_input_seq, int(item.get("ack_input_seq", player.last_input_seq)))
                except (TypeError, ValueError):
                    pass
                try:
                    player.client_time_ms = int(item.get("client_time", player.client_time_ms) or player.client_time_ms)
                except (TypeError, ValueError):
                    pass
                try:
                    player.server_received_time_ms = int(
                        item.get("server_received_time", player.server_received_time_ms) or player.server_received_time_ms
                    )
                except (TypeError, ValueError):
                    pass

                input_payload = item.get("input")
                if isinstance(input_payload, dict):
                    player.throttle = max(-1.0, min(1.0, float(input_payload.get("throttle", player.throttle) or 0.0)))
                    player.steer = max(-1.0, min(1.0, float(input_payload.get("steer", player.steer) or 0.0)))
                    player.brake = bool(input_payload.get("brake", player.brake))
                    player.handbrake = bool(input_payload.get("handbrake", player.handbrake))
                    player.nitro = bool(input_payload.get("nitro", player.nitro))

                player.position = self._coerce_vec3(item.get("position"), player.position)
                player.rotation = self._coerce_vec3(item.get("rotation"), player.rotation)
                player.velocity = self._coerce_vec3(item.get("velocity"), player.velocity)
                player.angular_velocity = self._coerce_vec3(item.get("angular_velocity"), player.angular_velocity)
                player.wheel_states = self._coerce_wheel_states(item.get("wheel_states"), player.wheel_states)
                player.debug_state = item.get("debug") if isinstance(item.get("debug"), dict) else {}

        damage_states_payload = snapshot.get("damage_states")
        if isinstance(damage_states_payload, list):
            for item in damage_states_payload:
                if not isinstance(item, dict):
                    continue
                player_id = str(item.get("player_id", "") or "")
                if not player_id:
                    continue
                player = match.players.get(player_id)
                if player is None:
                    continue

                try:
                    revision = int(item.get("revision", 0) or 0)
                except (TypeError, ValueError):
                    revision = 0
                if revision <= player.damage_revision:
                    continue

                try:
                    width = max(0, int(item.get("width", 0) or 0))
                except (TypeError, ValueError):
                    width = 0
                try:
                    height = max(0, int(item.get("height", 0) or 0))
                except (TypeError, ValueError):
                    height = 0
                map_b64 = str(item.get("map_b64", "") or "")
                player.damage_revision = revision
                player.damage_width = width
                player.damage_height = height
                player.damage_map_b64 = map_b64
                player.last_damage_at = utcnow()

                world_point = item.get("world_point")
                world_normal = item.get("world_normal")
                damage_payloads.append(
                    {
                        "type": "damage_state",
                        "match_id": match.match_id,
                        "player_id": player_id,
                        "revision": revision,
                        "width": width,
                        "height": height,
                        "map_b64": map_b64,
                        "world_point": self._coerce_vec3(world_point, Vec3()).as_dict() if isinstance(world_point, dict) else None,
                        "world_normal": self._coerce_vec3(world_normal, Vec3(0.0, 1.0, 0.0)).as_dict()
                        if isinstance(world_normal, dict)
                        else None,
                    }
                )

        collisions_payload = snapshot.get("collisions")
        if isinstance(collisions_payload, list):
            snapshot_server_time = int(snapshot.get("server_time", 0) or 0)
            for item in collisions_payload:
                if not isinstance(item, dict):
                    continue
                primary_player_id = str(item.get("primary_player_id", "") or "")
                secondary_player_id = str(item.get("secondary_player_id", "") or "")
                if not primary_player_id or not secondary_player_id:
                    continue
                if primary_player_id not in match.players or secondary_player_id not in match.players:
                    continue
                if primary_player_id == secondary_player_id:
                    continue

                try:
                    sequence = int(item.get("sequence", 0) or 0)
                except (TypeError, ValueError):
                    sequence = 0
                if sequence > 0 and sequence <= match.last_authoritative_collision_seq:
                    continue

                pair_key = self._build_collision_pair_key(match.match_id, primary_player_id, secondary_player_id)
                event_server_time = int(item.get("server_time", snapshot_server_time) or snapshot_server_time or 0)
                event_time_sec = (event_server_time / 1000.0) if event_server_time > 0 else time.time()
                last_collision_at = self.recent_collision_pairs.get(pair_key, 0.0)
                if event_time_sec - last_collision_at < 0.12:
                    continue
                self.recent_collision_pairs[pair_key] = event_time_sec

                if sequence > 0:
                    match.last_authoritative_collision_seq = max(match.last_authoritative_collision_seq, sequence)

                world_point = self._coerce_vec3(item.get("world_point"), Vec3())
                world_normal = self._coerce_vec3(item.get("world_normal"), Vec3(0.0, 1.0, 0.0))
                relative_velocity = self._coerce_vec3(item.get("relative_velocity"), Vec3())
                impulse_vector = self._coerce_vec3(item.get("impulse_vector"), Vec3())
                try:
                    impulse_magnitude = float(item.get("impulse_magnitude", 0.0) or 0.0)
                except (TypeError, ValueError):
                    impulse_magnitude = 0.0
                base_payload = {
                    "type": "collision_event",
                    "match_id": match.match_id,
                    "primary_player_id": primary_player_id,
                    "secondary_player_id": secondary_player_id,
                    "world_point": world_point.as_dict(),
                    "world_normal": world_normal.as_dict(),
                    "relative_velocity": relative_velocity.as_dict(),
                    "impulse_vector": impulse_vector.as_dict(),
                    "impulse_magnitude": impulse_magnitude,
                }
                match.recent_collisions.append(
                    {
                        **base_payload,
                        "sequence": sequence,
                        "server_time": event_server_time,
                        "server_tick": match.server_tick,
                    }
                )
                if len(match.recent_collisions) > 24:
                    match.recent_collisions = match.recent_collisions[-24:]

                collision_payloads.append(base_payload)
                collision_payloads.append(
                    {
                        "type": "collision_event",
                        "match_id": match.match_id,
                        "primary_player_id": secondary_player_id,
                        "secondary_player_id": primary_player_id,
                        "world_point": world_point.as_dict(),
                        "world_normal": Vec3(-world_normal.x, -world_normal.y, -world_normal.z).as_dict(),
                        "relative_velocity": Vec3(-relative_velocity.x, -relative_velocity.y, -relative_velocity.z).as_dict(),
                        "impulse_vector": Vec3(-impulse_vector.x, -impulse_vector.y, -impulse_vector.z).as_dict(),
                        "impulse_magnitude": impulse_magnitude,
                    }
                )

        return damage_payloads, collision_payloads

    def _check_lobby_rate_limit(self, player_id: str) -> None:
        if not self.lobby_rate_limit.hit(player_id):
            raise invalid_request("Lobby action rate limit exceeded")

    def _get_match_metrics(self, match_id: str) -> MatchRuntimeMetrics:
        metrics = self.match_metrics.get(match_id)
        if metrics is None:
            metrics = MatchRuntimeMetrics()
            self.match_metrics[match_id] = metrics
        return metrics

    @staticmethod
    def _estimate_payload_size(payload: dict[str, Any]) -> int:
        try:
            return len(json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
        except Exception:
            return 0

    def _player_connection_state(self, player_id: str) -> ConnectionState:
        return ConnectionState.connected if player_id in self.player_connections else ConnectionState.disconnected

    def _serialize_session(self, session: Session) -> dict[str, Any]:
        return {
            "session_id": session.session_id,
            "player_id": session.player_id,
            "player_name": session.player_name,
            "session_token": session.session_token,
            "created_at": session.created_at.isoformat() + "Z",
            "expires_at": session.expires_at.isoformat() + "Z",
        }

    def _serialize_lobby_player(self, player: LobbyPlayer, *, include_car_config: bool = True) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "player_id": player.player_id,
            "player_name": player.player_name,
            "connection_state": player.connection_state.value,
            "is_server_controlled": player.is_server_controlled,
            "joined_at": player.joined_at.isoformat() + "Z",
        }
        if include_car_config:
            payload["car_config"] = player.car_config
        return payload

    def _serialize_lobby_summary(self, lobby: Lobby) -> dict[str, Any]:
        return {
            "lobby_id": lobby.lobby_id,
            "name": lobby.name,
            "status": lobby.status.value,
            "map_id": lobby.map_id,
            "max_players": lobby.max_players,
            "current_players": len(lobby.players),
        }

    def _serialize_lobby_detail(self, lobby: Lobby) -> dict[str, Any]:
        return {
            **self._serialize_lobby_summary(lobby),
            "owner_player_id": lobby.owner_player_id,
            "players": [self._serialize_lobby_player(player) for player in lobby.players.values()],
            "created_at": lobby.created_at.isoformat() + "Z",
            "expires_at": lobby.expires_at.isoformat() + "Z",
            "match_id": lobby.match_id,
        }

    def _serialize_match_info(self, match: Match) -> dict[str, Any]:
        return {
            "match_id": match.match_id,
            "lobby_id": match.lobby_id,
            "status": match.status.value,
            "map_id": match.map_id,
            "tick_rate": match.tick_rate,
            "room_id": match.room_id,
            "room_status": match.room_status,
            "room_http_url": match.room_http_url,
            "room_ws_url": match.room_ws_url,
            "room_token": match.room_token,
            "players": [self._serialize_match_player_info(match, player) for player in match.players.values()],
        }

    def _serialize_match_player_info(self, match: Match, player: MatchPlayer) -> dict[str, Any]:
        lobby = self.lobbies_by_id.get(match.lobby_id)
        connection_state = ConnectionState.disconnected.value
        if lobby and player.player_id in lobby.players:
            connection_state = lobby.players[player.player_id].connection_state.value
        return {
            "player_id": player.player_id,
            "player_name": player.player_name,
            "connection_state": connection_state,
            "is_server_controlled": player.is_server_controlled,
            "authority_order": player.authority_order,
            "spawn_point_id": player.spawn_point_id,
            "spawn_position": player.spawn_position.as_dict(),
            "spawn_rotation": player.spawn_rotation.as_dict(),
            "car_config": player.car_config,
        }

    def _serialize_admin_lobby_player(self, player: LobbyPlayer) -> dict[str, Any]:
        return {
            "player_id": player.player_id,
            "player_name": player.player_name,
            "connection_state": player.connection_state.value,
            "is_server_controlled": player.is_server_controlled,
            "joined_at": player.joined_at.isoformat() + "Z",
            "car_config": player.car_config,
            "loadout_display_name": player.car_config.get("loadout_display_name"),
            "paint_name": player.car_config.get("paint_name"),
            "customizations": player.car_config.get("customizations", []),
        }

    def _serialize_admin_lobby(self, lobby: Lobby) -> dict[str, Any]:
        return {
            **self._serialize_lobby_summary(lobby),
            "owner_player_id": lobby.owner_player_id,
            "created_at": lobby.created_at.isoformat() + "Z",
            "expires_at": lobby.expires_at.isoformat() + "Z",
            "match_id": lobby.match_id,
            "players": [self._serialize_admin_lobby_player(player) for player in lobby.players.values()],
        }

    def _serialize_match_state(self, match: Match) -> dict[str, Any]:
        server_time = int(time.time() * 1000)
        if isinstance(match.last_simulation_snapshot, dict):
            try:
                server_time = int(match.last_simulation_snapshot.get("server_time", server_time) or server_time)
            except (TypeError, ValueError):
                server_time = int(time.time() * 1000)

        return {
            "type": "match_state",
            "match_id": match.match_id,
            "server_tick": match.server_tick,
            "server_time": server_time,
            "players": [
                {
                    "player_id": player.player_id,
                    "ack_input_seq": player.last_input_seq,
                    "client_time": player.client_time_ms,
                    "server_received_time": player.server_received_time_ms,
                    "input": {
                        "throttle": round(player.throttle, 3),
                        "steer": round(player.steer, 3),
                        "brake": player.brake,
                        "handbrake": player.handbrake,
                        "nitro": player.nitro,
                    },
                    "position": player.position.as_dict(),
                    "rotation": player.rotation.as_dict(),
                    "velocity": player.velocity.as_dict(),
                    "angular_velocity": player.angular_velocity.as_dict(),
                    "wheel_states": [
                        {
                            "position": state["position"].as_dict(),
                            "rotation": state["rotation"].as_dict(),
                        }
                        for state in player.wheel_states
                    ],
                }
                for player in match.players.values()
            ],
        }

    def _serialize_admin_match_player(self, match: Match, player: MatchPlayer) -> dict[str, Any]:
        lobby = self.lobbies_by_id.get(match.lobby_id)
        connection_state = ConnectionState.disconnected.value
        if lobby and player.player_id in lobby.players:
            connection_state = lobby.players[player.player_id].connection_state.value
        speed = (player.velocity.x**2 + player.velocity.y**2 + player.velocity.z**2) ** 0.5
        return {
            "player_id": player.player_id,
            "player_name": player.player_name,
            "connection_state": connection_state,
            "is_server_controlled": player.is_server_controlled,
            "authority_order": player.authority_order,
            "spawn_point_id": player.spawn_point_id,
            "spawn_position": player.spawn_position.as_dict(),
            "spawn_rotation": player.spawn_rotation.as_dict(),
            "position": player.position.as_dict(),
            "rotation": player.rotation.as_dict(),
            "velocity": player.velocity.as_dict(),
            "angular_velocity": player.angular_velocity.as_dict(),
            "speed": round(speed, 3),
            "last_snapshot_at": player.last_snapshot_at.isoformat() + "Z",
            "client_time_ms": player.client_time_ms,
            "server_received_time_ms": player.server_received_time_ms,
            "last_input_seq": player.last_input_seq,
            "throttle": round(player.throttle, 3),
            "steer": round(player.steer, 3),
            "brake": player.brake,
            "handbrake": player.handbrake,
            "nitro": player.nitro,
            "car_config": player.car_config,
            "wheel_state_count": len(player.wheel_states),
            "damage_revision": player.damage_revision,
            "damage_width": player.damage_width,
            "damage_height": player.damage_height,
            "damage_map_bytes": len(player.damage_map_b64.encode("utf-8")) if player.damage_map_b64 else 0,
            "damage_map_b64": player.damage_map_b64,
            "last_damage_at": player.last_damage_at.isoformat() + "Z" if player.last_damage_at else None,
            "debug": player.debug_state,
        }

    def _serialize_admin_match_summary(self, match: Match) -> dict[str, Any]:
        return {
            "match_id": match.match_id,
            "lobby_id": match.lobby_id,
            "status": match.status.value,
            "map_id": match.map_id,
            "player_count": len(match.players),
            "server_tick": match.server_tick,
            "room_id": match.room_id,
            "room_status": match.room_status,
            "source": "backend_runtime",
            "debug_summary": {},
        }

    def _serialize_admin_match_detail(self, match: Match) -> dict[str, Any]:
        raw_snapshot = match.last_simulation_snapshot or self._serialize_match_state(match)
        return {
            "match_id": match.match_id,
            "lobby_id": match.lobby_id,
            "status": match.status.value,
            "map_id": match.map_id,
            "tick_rate": match.tick_rate,
            "server_tick": match.server_tick,
            "room_id": match.room_id,
            "room_status": match.room_status,
            "room_http_url": match.room_http_url,
            "room_ws_url": match.room_ws_url,
            "room_token": match.room_token,
            "source": "backend_runtime",
            "debug_summary": {},
            "players": [self._serialize_admin_match_player(match, player) for player in match.players.values()],
            "recent_collisions": list(match.recent_collisions),
            "raw_snapshot": raw_snapshot,
            "telemetry": self._get_match_metrics(match.match_id).as_dict(),
        }

    async def _list_direct_admin_matches(self) -> list[dict[str, Any]]:
        if not self.direct_observer.enabled:
            return []

        try:
            rooms = await self.direct_observer.list_rooms()
        except Exception:
            logger.warning("direct_observer_list_failed", exc_info=True)
            return []

        room_items = [room for room in rooms if isinstance(room, dict)]
        snapshot_results = await asyncio.gather(
            *[
                self.direct_observer.get_snapshot(str(room.get("match_id", "") or room.get("room_id", "") or ""))
                for room in room_items
            ],
            return_exceptions=True,
        )

        items: list[dict[str, Any]] = []
        for room, snapshot_result in zip(room_items, snapshot_results):
            snapshot = snapshot_result if isinstance(snapshot_result, dict) else None
            items.append(self._serialize_direct_admin_match_summary(room, snapshot))
        return items

    async def _get_direct_admin_match(self, match_id: str) -> dict[str, Any] | None:
        if not self.direct_observer.enabled or not match_id:
            return None

        room_result, snapshot_result = await asyncio.gather(
            self.direct_observer.get_room(match_id),
            self.direct_observer.get_snapshot(match_id),
            return_exceptions=True,
        )
        room = room_result if isinstance(room_result, dict) else {}
        snapshot = snapshot_result if isinstance(snapshot_result, dict) else {}

        if isinstance(room_result, Exception) and isinstance(snapshot_result, Exception):
            logger.warning("direct_observer_match_failed match_id=%s", match_id, exc_info=True)
            return None
        if not room and not snapshot:
            return None

        return self._serialize_direct_admin_match_detail(match_id, room, snapshot)

    def _serialize_direct_admin_match_summary(
        self,
        room: dict[str, Any],
        snapshot: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        match_id = str(room.get("match_id", "") or room.get("room_id", "") or "")
        server_tick = self._safe_int(
            snapshot.get("server_tick") if isinstance(snapshot, dict) else None,
            self._safe_int(room.get("server_tick"), 0),
        )
        debug_summary = self._build_direct_admin_debug_summary(room, snapshot)
        return {
            "match_id": match_id,
            "lobby_id": "direct_purrnet",
            "status": str(
                (snapshot.get("status") if isinstance(snapshot, dict) else None)
                or room.get("status", "running")
                or "running"
            ),
            "map_id": str(
                (snapshot.get("map_id") if isinstance(snapshot, dict) else None)
                or room.get("map_id", "city_default")
                or "city_default"
            ),
            "player_count": self._safe_int(room.get("player_count"), debug_summary.get("player_count", 0)),
            "server_tick": server_tick,
            "room_id": str(room.get("room_id", match_id) or match_id),
            "room_status": str(
                (snapshot.get("status") if isinstance(snapshot, dict) else None)
                or room.get("status", "running")
                or "running"
            ),
            "source": "purrnet_direct",
            "debug_summary": debug_summary,
        }

    def _serialize_direct_admin_match_detail(
        self,
        match_id: str,
        room: dict[str, Any],
        snapshot: dict[str, Any],
    ) -> dict[str, Any]:
        observer = snapshot.get("observer") if isinstance(snapshot.get("observer"), dict) else {}
        tracked_players = observer.get("tracked_players") if isinstance(observer.get("tracked_players"), list) else []
        tracked_by_id = {
            str(item.get("player_id", "") or ""): item
            for item in tracked_players
            if isinstance(item, dict) and str(item.get("player_id", "") or "")
        }
        damage_by_player = {
            str(item.get("player_id", "") or ""): item
            for item in (snapshot.get("damage_states") if isinstance(snapshot.get("damage_states"), list) else [])
            if isinstance(item, dict) and str(item.get("player_id", "") or "")
        }
        server_time = self._safe_int(snapshot.get("server_time"), int(time.time() * 1000))
        active_ids: set[str] = set()
        players: list[dict[str, Any]] = []

        snapshot_players = snapshot.get("players") if isinstance(snapshot.get("players"), list) else []
        for index, item in enumerate(snapshot_players):
            if not isinstance(item, dict):
                continue
            player_id = str(item.get("player_id", "") or "")
            if not player_id:
                continue
            active_ids.add(player_id)
            players.append(
                self._serialize_direct_admin_match_player(
                    player_payload=item,
                    tracked_payload=tracked_by_id.get(player_id),
                    damage_payload=damage_by_player.get(player_id),
                    index=index,
                    server_time_ms=server_time,
                )
            )

        for tracked in tracked_players:
            if not isinstance(tracked, dict):
                continue
            player_id = str(tracked.get("player_id", "") or "")
            if not player_id or player_id in active_ids:
                continue
            players.append(
                self._serialize_direct_admin_match_player(
                    player_payload=None,
                    tracked_payload=tracked,
                    damage_payload=damage_by_player.get(player_id),
                    index=len(players),
                    server_time_ms=server_time,
                )
            )

        tick_rate = self._safe_int(room.get("tick_rate"), self._safe_int((observer.get("network") or {}).get("tick_rate"), 30))
        status = str(snapshot.get("status", "") or room.get("status", "") or "running")
        raw_snapshot = snapshot or room
        return {
            "match_id": match_id,
            "lobby_id": "direct_purrnet",
            "status": status,
            "map_id": str(snapshot.get("map_id", "") or room.get("map_id", "") or "city_default"),
            "tick_rate": tick_rate,
            "server_tick": self._safe_int(snapshot.get("server_tick"), self._safe_int(room.get("server_tick"), 0)),
            "room_id": str(room.get("room_id", "") or snapshot.get("room_id", "") or match_id),
            "room_status": status,
            "room_http_url": str(room.get("room_http_url", "") or "") or None,
            "room_ws_url": str(room.get("room_ws_url", "") or "") or None,
            "room_token": str(room.get("room_token", "") or "") or None,
            "source": "purrnet_direct",
            "debug_summary": self._build_direct_admin_debug_summary(room, snapshot),
            "players": players,
            "recent_collisions": snapshot.get("collisions") if isinstance(snapshot.get("collisions"), list) else [],
            "raw_snapshot": raw_snapshot,
            "telemetry": self._build_direct_admin_telemetry(raw_snapshot),
        }

    @staticmethod
    def _extract_game_settings_section_fields(payload: dict[str, Any], section_id: str) -> dict[str, Any] | None:
        sections = payload.get("sections") if isinstance(payload, dict) else None
        if not isinstance(sections, list):
            return None

        for item in sections:
            if not isinstance(item, dict):
                continue
            if str(item.get("section_id", "") or "") != section_id:
                continue
            fields = item.get("fields")
            return fields if isinstance(fields, dict) else None

        return None

    async def _get_direct_observer_fallback_room_id(self) -> str | None:
        try:
            rooms = await self.direct_observer.list_rooms()
        except Exception:
            logger.warning("direct_observer_list_rooms_failed_for_game_settings", exc_info=True)
            return None

        for room in rooms:
            if not isinstance(room, dict):
                continue
            match_id = str(room.get("match_id", "") or room.get("room_id", "") or "")
            if match_id:
                return match_id

        return None

    @staticmethod
    def _build_admin_game_settings_response(
        source: str,
        damage_config: dict[str, Any] | None,
        note: str | None = None,
    ) -> dict[str, Any]:
        sections: list[dict[str, Any]] = []
        if isinstance(damage_config, dict):
            meta: dict[str, Any] = {}
            fields: dict[str, Any] = {}
            for key, value in damage_config.items():
                if key in {"version", "revision", "updated_at_unix_ms", "source"}:
                    meta[key] = value
                else:
                    fields[key] = value

            sections.append(
                {
                    "section_id": "damage",
                    "title": "Damage",
                    "description": "Runtime collision and deformation tuning pulled from the dedicated observer.",
                    "editable": source == "purrnet_direct",
                    "meta": meta,
                    "fields": fields,
                }
            )

        return {
            "scope": "global",
            "source": source or "backend_runtime",
            "note": note,
            "sections": sections,
        }

    def _serialize_direct_admin_match_player(
        self,
        *,
        player_payload: dict[str, Any] | None,
        tracked_payload: dict[str, Any] | None,
        damage_payload: dict[str, Any] | None,
        index: int,
        server_time_ms: int,
    ) -> dict[str, Any]:
        player_payload = player_payload or {}
        tracked_payload = tracked_payload or {}
        player_id = str(player_payload.get("player_id", "") or tracked_payload.get("player_id", "") or f"direct_{index}")
        connection_state = str(
            player_payload.get("connection_state", "")
            or ("queued" if tracked_payload.get("queued") and not tracked_payload.get("spawned") else "in_game")
        )
        spawn_position_vec = self._coerce_vec3(
            player_payload.get("spawn_position") if isinstance(player_payload.get("spawn_position"), dict) else tracked_payload.get("spawn_position"),
            Vec3(),
        )
        spawn_rotation_vec = self._coerce_vec3(
            player_payload.get("spawn_rotation") if isinstance(player_payload.get("spawn_rotation"), dict) else tracked_payload.get("spawn_rotation"),
            Vec3(),
        )
        position_vec = self._coerce_vec3(player_payload.get("position"), spawn_position_vec)
        rotation_vec = self._coerce_vec3(player_payload.get("rotation"), spawn_rotation_vec)
        velocity_vec = self._coerce_vec3(player_payload.get("velocity"), Vec3())
        angular_velocity_vec = self._coerce_vec3(player_payload.get("angular_velocity"), Vec3())
        speed = (velocity_vec.x**2 + velocity_vec.y**2 + velocity_vec.z**2) ** 0.5
        input_payload = player_payload.get("input") if isinstance(player_payload.get("input"), dict) else {}
        car_config = player_payload.get("car_config") if isinstance(player_payload.get("car_config"), dict) else {}
        if not car_config and isinstance(tracked_payload.get("car_config"), dict):
            car_config = tracked_payload.get("car_config")

        debug = dict(player_payload.get("debug") if isinstance(player_payload.get("debug"), dict) else {})
        if tracked_payload:
            debug["tracked_is_bot"] = bool(tracked_payload.get("is_bot", False))
            debug["tracked_queued"] = bool(tracked_payload.get("queued", False))
            debug["tracked_spawned"] = bool(tracked_payload.get("spawned", False))
            debug["tracked_spawn_slot"] = self._safe_int(tracked_payload.get("spawn_slot"), -1)
            if tracked_payload.get("last_spawn_failure_reason"):
                debug["last_spawn_failure_reason"] = tracked_payload.get("last_spawn_failure_reason")
        if car_config:
            debug.setdefault("loadout_name", car_config.get("loadout_name"))
            debug.setdefault("loadout_display_name", car_config.get("loadout_display_name"))

        damage_revision = self._safe_int(damage_payload.get("revision") if damage_payload else None, -1)
        damage_width = self._safe_int(damage_payload.get("width") if damage_payload else None, 0)
        damage_height = self._safe_int(damage_payload.get("height") if damage_payload else None, 0)
        damage_map_b64 = str(damage_payload.get("map_b64", "") or "") if damage_payload else None
        last_snapshot_at = datetime.utcfromtimestamp(max(0, server_time_ms) / 1000.0).isoformat() + "Z"

        return {
            "player_id": player_id,
            "player_name": str(player_payload.get("player_name", "") or player_id),
            "connection_state": connection_state,
            "is_server_controlled": bool(player_payload.get("is_server_controlled", tracked_payload.get("is_bot", False))),
            "authority_order": self._safe_int(player_payload.get("authority_order"), self._safe_int(tracked_payload.get("spawn_slot"), index)),
            "spawn_point_id": str(
                player_payload.get("spawn_point_id", "")
                or tracked_payload.get("spawn_point_id", "")
                or f"purr_slot_{index}"
            ),
            "spawn_position": spawn_position_vec.as_dict(),
            "spawn_rotation": spawn_rotation_vec.as_dict(),
            "position": position_vec.as_dict(),
            "rotation": rotation_vec.as_dict(),
            "velocity": velocity_vec.as_dict(),
            "angular_velocity": angular_velocity_vec.as_dict(),
            "speed": round(speed, 3),
            "last_snapshot_at": last_snapshot_at,
            "client_time_ms": self._safe_int(player_payload.get("client_time"), 0),
            "server_received_time_ms": self._safe_int(player_payload.get("server_received_time"), server_time_ms),
            "last_input_seq": self._safe_int(player_payload.get("ack_input_seq"), 0),
            "throttle": round(float(input_payload.get("throttle", 0.0) or 0.0), 3),
            "steer": round(float(input_payload.get("steer", 0.0) or 0.0), 3),
            "brake": bool(input_payload.get("brake", False)),
            "handbrake": bool(input_payload.get("handbrake", False)),
            "nitro": bool(input_payload.get("nitro", False)),
            "car_config": car_config,
            "wheel_state_count": len(player_payload.get("wheel_states", [])) if isinstance(player_payload.get("wheel_states"), list) else 0,
            "damage_revision": damage_revision,
            "damage_width": damage_width,
            "damage_height": damage_height,
            "damage_map_bytes": len(damage_map_b64.encode("utf-8")) if damage_map_b64 else 0,
            "damage_map_b64": damage_map_b64,
            "last_damage_at": last_snapshot_at if damage_revision >= 0 else None,
            "debug": debug,
        }

    def _build_direct_admin_debug_summary(self, room: dict[str, Any], snapshot: dict[str, Any] | None) -> dict[str, Any]:
        observer = snapshot.get("observer") if isinstance(snapshot, dict) and isinstance(snapshot.get("observer"), dict) else {}
        counts = observer.get("counts") if isinstance(observer.get("counts"), dict) else {}
        spawner = observer.get("spawner") if isinstance(observer.get("spawner"), dict) else {}
        return {
            "player_count": self._safe_int(room.get("player_count"), self._safe_int(counts.get("active_player_cars"), 0)),
            "tracked_players": self._safe_int(counts.get("tracked_players"), 0),
            "queued_players": self._safe_int(spawner.get("queued_players"), 0),
            "spawned_players": self._safe_int(spawner.get("spawned_players"), 0),
            "bot_target": self._safe_int(spawner.get("solo_bot_target"), 0),
            "tracked_bot_players": self._safe_int(spawner.get("tracked_bot_players"), 0),
            "transient_cleanup_enabled": bool(spawner.get("transient_solo_cleanup_enabled", False)),
            "solo_session_active": bool(spawner.get("solo_session_active", False)),
            "solo_session_human_player_id": str(spawner.get("solo_session_human_player_id", "") or ""),
            "solo_session_status": str(spawner.get("solo_session_status", "") or ""),
            "solo_idle_timeout_sec": self._safe_float(spawner.get("solo_idle_timeout_sec"), 0.0),
            "seconds_since_last_human_seen": self._safe_float(spawner.get("seconds_since_last_human_seen"), -1.0),
            "seconds_since_last_input": self._safe_float(spawner.get("seconds_since_last_meaningful_input"), -1.0),
            "seconds_until_idle_close": self._safe_float(spawner.get("seconds_until_idle_close"), -1.0),
            "last_close_reason": str(spawner.get("last_solo_session_close_reason", "") or ""),
            "seconds_since_last_close": self._safe_float(spawner.get("seconds_since_last_solo_session_close"), -1.0),
            "room_visible": bool(observer.get("room_visible", True)),
            "scene_name": str(room.get("scene_name", "") or observer.get("scene_name", "") or ""),
            "mode": str(observer.get("mode", "") or ""),
        }

    def _build_direct_admin_telemetry(self, payload: dict[str, Any]) -> dict[str, Any]:
        observer = payload.get("observer") if isinstance(payload.get("observer"), dict) else {}
        return {
            "observer": {
                "source": observer.get("source", "purrnet_direct"),
                "mode": observer.get("mode", "Server"),
                "scene_name": observer.get("scene_name"),
                "started_at_utc": observer.get("started_at_utc"),
                "uptime_sec": observer.get("uptime_sec"),
            },
            "network": observer.get("network", {}) if isinstance(observer.get("network"), dict) else {},
            "prediction": observer.get("prediction", {}) if isinstance(observer.get("prediction"), dict) else {},
            "spawner": observer.get("spawner", {}) if isinstance(observer.get("spawner"), dict) else {},
            "counts": observer.get("counts", {}) if isinstance(observer.get("counts"), dict) else {},
        }

    @staticmethod
    def _admin_match_sort_key(match: dict[str, Any]) -> tuple[int, int, str]:
        priority = {
            "running": 0,
            "starting": 1,
            "finished": 2,
            "aborted": 3,
        }
        status = str(match.get("status", "") or "")
        server_tick = 0
        try:
            server_tick = int(match.get("server_tick", 0) or 0)
        except (TypeError, ValueError):
            server_tick = 0
        return (priority.get(status, 99), -server_tick, str(match.get("match_id", "") or ""))

    @staticmethod
    def _safe_int(value: Any, fallback: int) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return fallback

    @staticmethod
    def _safe_float(value: Any, fallback: float) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return fallback

    def _get_map_spawn_points(self, map_id: str) -> list[SpawnPoint]:
        spawn_points = MAP_SPAWN_POINTS.get(map_id)
        if not spawn_points:
            raise invalid_request(f"Map '{map_id}' has no configured spawn points")
        return spawn_points

    def _build_spawn_assignments(self, lobby: Lobby) -> dict[str, SpawnPoint]:
        spawn_points = self._get_map_spawn_points(lobby.map_id)
        sorted_players = sorted(lobby.players.values(), key=lambda player: (player.joined_at, player.player_id))
        if len(sorted_players) > len(spawn_points):
            raise invalid_request(
                f"Map '{lobby.map_id}' provides only {len(spawn_points)} spawn points for {len(sorted_players)} players"
            )
        return {
            player.player_id: spawn_points[index]
            for index, player in enumerate(sorted_players)
        }

    @staticmethod
    def _build_collision_pair_key(match_id: str, primary_player_id: str, secondary_player_id: str) -> str:
        ordered = sorted([primary_player_id, secondary_player_id])
        return f"{match_id}:{ordered[0]}|{ordered[1]}"

    def _sorted_lobbies(self) -> list[Lobby]:
        priority = {
            LobbyStatus.waiting: 0,
            LobbyStatus.starting: 1,
            LobbyStatus.in_game: 2,
            LobbyStatus.closed: 3,
        }
        return sorted(self.lobbies_by_id.values(), key=lambda lobby: (priority.get(lobby.status, 99), lobby.created_at))

    def _sorted_matches(self) -> list[Match]:
        priority = {
            MatchStatus.running: 0,
            MatchStatus.starting: 1,
            MatchStatus.finished: 2,
            MatchStatus.aborted: 3,
        }
        return sorted(
            self.matches_by_id.values(),
            key=lambda match: (priority.get(match.status, 99), match.created_at, match.match_id),
        )

    async def _broadcast_lobby_snapshot(self, lobby_id: str, precomputed: dict[str, Any] | None = None) -> None:
        async with self.lock:
            lobby = self.lobbies_by_id.get(lobby_id)
            if lobby is None:
                return
            payload = {"type": "lobby_snapshot", "lobby": precomputed or self._serialize_lobby_detail(lobby)}
        await self._broadcast_lobby_event(lobby_id, payload)

    async def _broadcast_lobby_event(self, lobby_id: str, payload: dict[str, Any]) -> None:
        async with self.lock:
            lobby = self.lobbies_by_id.get(lobby_id)
            player_ids = set(self.lobby_subscribers.get(lobby_id, set()))
            if lobby:
                player_ids.update(lobby.players.keys())
        await self._broadcast_to_players(player_ids, payload)

    async def _broadcast_match_players(self, match_id: str, payload: dict[str, Any]) -> None:
        async with self.lock:
            match = self.matches_by_id.get(match_id)
            if match is None:
                return
            player_ids = set(match.players.keys())
        await self._broadcast_to_players(player_ids, payload)

    async def _broadcast_admin_lobbies_snapshot(self) -> None:
        await self._broadcast_to_admins({"type": "admin_lobbies_snapshot", **(await self.list_admin_lobbies())})

    async def _broadcast_admin_lobby_change(self, lobby_id: str) -> None:
        try:
            payload = await self.get_admin_lobby(lobby_id)
            await self._broadcast_to_admins({"type": "admin_lobby_updated", "lobby": payload})
        except Exception:
            await self._broadcast_admin_lobbies_snapshot()
            return
        await self._broadcast_admin_lobbies_snapshot()

    async def _broadcast_admin_matches_snapshot(self) -> None:
        await self._broadcast_to_admins({"type": "admin_matches_snapshot", **(await self.list_admin_matches())})

    async def _broadcast_admin_match_change(self, match_id: str) -> None:
        try:
            payload = await self.get_admin_match(match_id)
            await self._broadcast_to_admins({"type": "admin_match_updated", "match": payload})
        except Exception:
            await self._broadcast_admin_matches_snapshot()
            return
        await self._broadcast_admin_matches_snapshot()

    async def _broadcast_admin_match_state(self, match_id: str, snapshot: dict[str, Any] | None = None) -> None:
        if snapshot is None:
            async with self.lock:
                match = self.matches_by_id.get(match_id)
                if match is None:
                    return
                snapshot = match.last_simulation_snapshot or self._serialize_match_state(match)
        async with self.lock:
            match = self.matches_by_id.get(match_id)
            if match is None:
                return
            payload = {
                "type": "admin_match_state",
                "match_id": match_id,
                "server_tick": snapshot["server_tick"],
                "room_id": match.room_id,
                "room_status": match.room_status,
                "source": "backend_runtime",
                "players": [self._serialize_admin_match_player(match, player) for player in match.players.values()],
                "recent_collisions": list(match.recent_collisions),
                "telemetry": self._get_match_metrics(match_id).as_dict(),
                "raw_snapshot": snapshot,
            }
        self._get_match_metrics(match_id).admin_state_out.add(self._estimate_payload_size(payload))
        await self._broadcast_to_admins(payload)

    async def _broadcast_to_players(self, player_ids: set[str], payload: dict[str, Any]) -> None:
        for player_id in player_ids:
            await self._send_to_player(player_id, payload)

    async def _send_to_player(self, player_id: str, payload: dict[str, Any]) -> None:
        async with self.lock:
            websocket = self.player_connections.get(player_id)
        if websocket is None:
            return
        try:
            await websocket.send_json(payload)
        except RuntimeError:
            await self.unregister_connection(player_id)
        except WebSocketDisconnect:
            await self.unregister_connection(player_id)

    async def _send_ws_error(self, player_id: str, code: str, message: str) -> None:
        await self._send_to_player(player_id, {"type": "error", "code": code, "message": message})

    async def _safe_close(self, websocket: WebSocket, code: int) -> None:
        try:
            await websocket.close(code=code)
        except RuntimeError:
            return

    async def _broadcast_to_admins(self, payload: dict[str, Any]) -> None:
        async with self.lock:
            admin_sockets = list(self.admin_connections.values())
        stale: list[WebSocket] = []
        for websocket in admin_sockets:
            try:
                await websocket.send_json(payload)
            except (RuntimeError, WebSocketDisconnect):
                stale.append(websocket)
        for websocket in stale:
            await self.unregister_admin_connection(websocket)

    async def _send_to_admin(self, websocket: WebSocket, payload: dict[str, Any]) -> None:
        try:
            await websocket.send_json(payload)
        except (RuntimeError, WebSocketDisconnect):
            await self.unregister_admin_connection(websocket)

    def serialize_public_session(self, session: Session) -> dict[str, Any]:
        return self._serialize_session(session)
