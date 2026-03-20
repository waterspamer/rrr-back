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
        self.player_state_in = RollingMetricWindow()
        self.damage_state_in = RollingMetricWindow()
        self.match_state_out = RollingMetricWindow()
        self.damage_state_out = RollingMetricWindow()
        self.admin_state_out = RollingMetricWindow()
        self.last_match_snapshot_bytes = 0
        self.last_damage_payload_bytes = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "player_state_in": self.player_state_in.snapshot(),
            "damage_state_in": self.damage_state_in.snapshot(),
            "match_state_out": self.match_state_out.snapshot(),
            "damage_state_out": self.damage_state_out.snapshot(),
            "admin_state_out": self.admin_state_out.snapshot(),
            "last_match_snapshot_bytes": self.last_match_snapshot_bytes,
            "last_damage_payload_bytes": self.last_damage_payload_bytes,
        }


class RuntimeState:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
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
        return {"items": items}

    async def get_admin_match(self, match_id: str) -> dict[str, Any]:
        async with self.lock:
            match = self.matches_by_id.get(match_id)
            if match is None:
                raise match_not_found()
            return self._serialize_admin_match_detail(match)

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
        async with self.lock:
            self.player_connections.pop(player_id, None)
            lobby_id = self.player_to_lobby.get(player_id)
            match_id = None
            if lobby_id and lobby_id in self.lobbies_by_id and player_id in self.lobbies_by_id[lobby_id].players:
                self.lobbies_by_id[lobby_id].players[player_id].connection_state = ConnectionState.disconnected
                match_id = self.lobbies_by_id[lobby_id].match_id
                self._cancel_countdown_if_not_ready_locked(self.lobbies_by_id[lobby_id])
            for subscribers in self.lobby_subscribers.values():
                subscribers.discard(player_id)
        logger.info("ws_disconnected player_id=%s", player_id)
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
        if message_type == "player_state":
            await self.apply_player_state(player_id, payload)
            return
        if message_type == "damage_state":
            await self.apply_damage_state(player_id, payload)
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
        match_id = str(payload.get("match_id", ""))
        seq = int(payload.get("seq", -1))
        state_payload = payload.get("state") or {}
        async with self.lock:
            match = self.matches_by_id.get(match_id)
            if match is None:
                raise match_not_found()
            player = match.players.get(player_id)
            if player is None:
                raise invalid_request("Player is not part of the match")
            if match.status != MatchStatus.running or seq <= player.last_state_seq:
                return
            player.last_state_seq = seq
            player.last_snapshot_at = utcnow()
            player.position = self._coerce_vec3(state_payload.get("position"), player.position)
            player.rotation = self._coerce_vec3(state_payload.get("rotation"), player.rotation)
            player.velocity = self._coerce_vec3(state_payload.get("velocity"), player.velocity)
            player.wheel_states = self._coerce_wheel_states(state_payload.get("wheel_states"), player.wheel_states)
            player.disconnected_announced = False
            metrics = self._get_match_metrics(match_id)
            metrics.player_state_in.add(self._estimate_payload_size(payload))

    async def apply_damage_state(self, player_id: str, payload: dict[str, Any]) -> None:
        match_id = str(payload.get("match_id", ""))
        revision = int(payload.get("revision", 0))
        async with self.lock:
            match = self.matches_by_id.get(match_id)
            if match is None:
                raise match_not_found()
            player = match.players.get(player_id)
            if player is None:
                raise invalid_request("Player is not part of the match")
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
        metrics.damage_state_out.add(payload_size)
        await self._broadcast_admin_match_change(match_id)

    async def _maintenance_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(max(1, self.settings.maintenance_interval_sec))
                await self._expire_lobbies()
        except asyncio.CancelledError:
            raise

    async def _expire_lobbies(self) -> None:
        expired: list[tuple[str, set[str]]] = []
        async with self.lock:
            now = utcnow()
            for lobby_id, lobby in list(self.lobbies_by_id.items()):
                if lobby.status not in {LobbyStatus.waiting, LobbyStatus.starting}:
                    continue
                if lobby.expires_at > now:
                    continue

                task = self.countdown_tasks.pop(lobby_id, None)
                if task is not None:
                    task.cancel()
                player_ids = set(lobby.players.keys())
                for player_id in player_ids:
                    self.player_to_lobby.pop(player_id, None)
                self.lobby_subscribers.pop(lobby_id, None)
                self.lobbies_by_id.pop(lobby_id, None)
                expired.append((lobby_id, player_ids))

        for lobby_id, player_ids in expired:
            logger.info("lobby_expired lobby_id=%s", lobby_id)
            await self._broadcast_to_players(
                player_ids,
                {"type": "lobby_closed", "lobby_id": lobby_id, "reason": "timeout"},
            )
        if expired:
            await self._broadcast_admin_lobbies_snapshot()

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
        return all(player_id in self.player_connections for player_id in lobby.players)

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
            async with self.lock:
                lobby = self.lobbies_by_id.get(lobby_id)
                if lobby is None or lobby.status != LobbyStatus.starting:
                    return
                spawn_assignments = self._build_spawn_assignments(lobby)
                match_id = f"match_{uuid4().hex[:12]}"
                players = {
                    player_id: MatchPlayer(
                        player_id=player.player_id,
                        player_name=player.player_name,
                        car_config=player.car_config,
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
                    )
                    for player_id, player in lobby.players.items()
                }
                match = Match(
                    match_id=match_id,
                    lobby_id=lobby_id,
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
            logger.info("match_created lobby_id=%s match_id=%s", lobby_id, match_id)
            await self._broadcast_lobby_event(
                lobby_id,
                {
                    "type": "match_created",
                    "match_id": match_id,
                    "lobby_id": lobby_id,
                    "map_id": match.map_id,
                    "players": [self._serialize_match_player_info(match, player) for player in match.players.values()],
                },
            )
            await self._broadcast_admin_lobby_change(lobby_id)
            await self._broadcast_admin_match_change(match_id)
            loading_task = asyncio.create_task(self._await_match_loaded(match_id), name=f"load:{match_id}")
            async with self.lock:
                self.loading_tasks[match_id] = loading_task
        finally:
            async with self.lock:
                self.countdown_tasks.pop(lobby_id, None)

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
        tick_interval = 1 / self.settings.match_tick_rate
        broadcast_every = max(1, round(self.settings.match_tick_rate / max(1, self.settings.match_broadcast_rate)))
        try:
            while True:
                await asyncio.sleep(tick_interval)
                snapshot = None
                disconnected_events: list[dict[str, Any]] = []
                async with self.lock:
                    match = self.matches_by_id.get(match_id)
                    if match is None or match.status != MatchStatus.running:
                        return
                    match.server_tick += 1
                    now = utcnow()
                    for player in match.players.values():
                        if (
                            now - player.last_snapshot_at >= timedelta(seconds=self.settings.disconnect_timeout_sec)
                            and not player.disconnected_announced
                        ):
                            player.disconnected_announced = True
                            disconnected_events.append(
                                {"type": "player_disconnected", "match_id": match_id, "player_id": player.player_id}
                            )
                    if match.server_tick % broadcast_every == 0:
                        snapshot = self._serialize_match_state(match)
                for event in disconnected_events:
                    await self._broadcast_match_players(match_id, event)
                if snapshot:
                    snapshot_bytes = self._estimate_payload_size(snapshot)
                    metrics = self._get_match_metrics(match_id)
                    metrics.match_state_out.add(snapshot_bytes)
                    metrics.last_match_snapshot_bytes = snapshot_bytes
                    await self._broadcast_match_players(match_id, snapshot)
                    await self._broadcast_admin_match_state(match_id, snapshot)
        finally:
            async with self.lock:
                self.match_tasks.pop(match_id, None)

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
        return {
            "type": "match_state",
            "match_id": match.match_id,
            "server_tick": match.server_tick,
            "server_time": int(time.time() * 1000),
            "players": [
                {
                    "player_id": player.player_id,
                    "position": player.position.as_dict(),
                    "rotation": player.rotation.as_dict(),
                    "velocity": player.velocity.as_dict(),
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
            "spawn_point_id": player.spawn_point_id,
            "spawn_position": player.spawn_position.as_dict(),
            "spawn_rotation": player.spawn_rotation.as_dict(),
            "position": player.position.as_dict(),
            "rotation": player.rotation.as_dict(),
            "velocity": player.velocity.as_dict(),
            "speed": round(speed, 3),
            "last_snapshot_at": player.last_snapshot_at.isoformat() + "Z",
            "car_config": player.car_config,
            "wheel_state_count": len(player.wheel_states),
            "damage_revision": player.damage_revision,
            "damage_map_bytes": len(player.damage_map_b64.encode("utf-8")) if player.damage_map_b64 else 0,
        }

    def _serialize_admin_match_summary(self, match: Match) -> dict[str, Any]:
        return {
            "match_id": match.match_id,
            "lobby_id": match.lobby_id,
            "status": match.status.value,
            "map_id": match.map_id,
            "player_count": len(match.players),
            "server_tick": match.server_tick,
        }

    def _serialize_admin_match_detail(self, match: Match) -> dict[str, Any]:
        raw_snapshot = self._serialize_match_state(match)
        return {
            "match_id": match.match_id,
            "lobby_id": match.lobby_id,
            "status": match.status.value,
            "map_id": match.map_id,
            "tick_rate": match.tick_rate,
            "server_tick": match.server_tick,
            "players": [self._serialize_admin_match_player(match, player) for player in match.players.values()],
            "raw_snapshot": raw_snapshot,
            "telemetry": self._get_match_metrics(match.match_id).as_dict(),
        }

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
                snapshot = self._serialize_match_state(match)
        async with self.lock:
            match = self.matches_by_id.get(match_id)
            if match is None:
                return
            payload = {
                "type": "admin_match_state",
                "match_id": match_id,
                "server_tick": snapshot["server_tick"],
                "players": [self._serialize_admin_match_player(match, player) for player in match.players.values()],
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
