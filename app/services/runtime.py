from __future__ import annotations

import asyncio
import json
import logging
import math
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
from app.models import ConnectionState, InputState, Lobby, LobbyPlayer, LobbyStatus, Match, MatchPlayer, MatchStatus, Session


logger = logging.getLogger("rrr.runtime")


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


class RuntimeState:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.lock = asyncio.Lock()
        self.sessions_by_token: dict[str, Session] = {}
        self.lobbies_by_id: dict[str, Lobby] = {}
        self.player_to_lobby: dict[str, str] = {}
        self.matches_by_id: dict[str, Match] = {}
        self.player_connections: dict[str, WebSocket] = {}
        self.lobby_subscribers: dict[str, set[str]] = defaultdict(set)
        self.countdown_tasks: dict[str, asyncio.Task[None]] = {}
        self.loading_tasks: dict[str, asyncio.Task[None]] = {}
        self.match_tasks: dict[str, asyncio.Task[None]] = {}
        self.guest_rate_limit = SlidingWindowRateLimiter(settings.guest_session_rate_limit)
        self.lobby_rate_limit = SlidingWindowRateLimiter(settings.lobby_action_rate_limit)

    async def shutdown(self) -> None:
        tasks = [
            *self.countdown_tasks.values(),
            *self.loading_tasks.values(),
            *self.match_tasks.values(),
        ]
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
                created_at=utcnow(),
            )
            self.lobbies_by_id[lobby_id] = lobby
            self.player_to_lobby[session.player_id] = lobby_id
        logger.info("lobby_created lobby_id=%s player_id=%s", lobby_id, session.player_id)
        await self._broadcast_lobby_snapshot(lobby_id)
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
        await self._maybe_start_lobby(lobby_id)
        return {"lobby_id": lobby_id, "player_id": session.player_id, "joined": True}

    async def leave_lobby(self, *, session_token: str, lobby_id: str) -> dict[str, Any]:
        session = await self.resolve_session(session_token)
        self._check_lobby_rate_limit(session.player_id)
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
                deleted_lobby = True
            else:
                deleted_lobby = False
                if lobby.owner_player_id == session.player_id:
                    lobby.owner_player_id = next(iter(lobby.players))
        logger.info("lobby_leave lobby_id=%s player_id=%s", lobby_id, session.player_id)
        await self._broadcast_lobby_event(
            lobby_id,
            {"type": "lobby_player_left", "lobby_id": lobby_id, "player_id": session.player_id},
        )
        if not deleted_lobby:
            await self._broadcast_lobby_snapshot(lobby_id)
        return {"left": True}

    async def update_car_config(self, *, session_token: str, lobby_id: str, car_config: dict[str, Any]) -> dict[str, Any]:
        session = await self.resolve_session(session_token)
        self._check_lobby_rate_limit(session.player_id)
        async with self.lock:
            lobby = self.lobbies_by_id.get(lobby_id)
            if lobby is None:
                raise lobby_not_found()
            player = lobby.players.get(session.player_id)
            if player is None:
                raise player_not_in_lobby()
            player.car_config = car_config
        logger.info("lobby_car_config_updated lobby_id=%s player_id=%s", lobby_id, session.player_id)
        await self._broadcast_lobby_snapshot(lobby_id)
        await self._maybe_start_lobby(lobby_id)
        return {"updated": True}

    async def get_match(self, match_id: str) -> dict[str, Any]:
        async with self.lock:
            match = self.matches_by_id.get(match_id)
            if match is None:
                raise match_not_found()
            return {
                "match_id": match.match_id,
                "lobby_id": match.lobby_id,
                "status": match.status.value,
                "map_id": match.map_id,
                "tick_rate": match.tick_rate,
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
            for subscribers in self.lobby_subscribers.values():
                subscribers.discard(player_id)
        logger.info("ws_disconnected player_id=%s", player_id)
        if lobby_id:
            await self._broadcast_lobby_snapshot(lobby_id)
        if match_id:
            await self._broadcast_match_players(
                match_id,
                {"type": "player_disconnected", "match_id": match_id, "player_id": player_id},
            )

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
        async with self.lock:
            match = self.matches_by_id.get(match_id)
            if match is None:
                raise match_not_found()
            player = match.players.get(player_id)
            if player is None:
                raise invalid_request("Player is not part of the match")
            player.loaded = True
            lobby = self.lobbies_by_id.get(match.lobby_id)
            if lobby and player_id in lobby.players:
                lobby.players[player_id].connection_state = ConnectionState.loading
        logger.info("match_loaded match_id=%s player_id=%s", match_id, player_id)

    async def apply_player_input(self, player_id: str, payload: dict[str, Any]) -> None:
        match_id = str(payload.get("match_id", ""))
        seq = int(payload.get("seq", -1))
        input_payload = payload.get("input") or {}
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
            player.last_packet_at = utcnow()
            player.input_state = InputState(
                throttle=max(-1.0, min(1.0, float(input_payload.get("throttle", 0.0)))),
                brake=max(0.0, min(1.0, float(input_payload.get("brake", 0.0)))),
                steer=max(-1.0, min(1.0, float(input_payload.get("steer", 0.0)))),
                handbrake=bool(input_payload.get("handbrake", False)),
                nitro=bool(input_payload.get("nitro", False)),
            )

    async def _maybe_start_lobby(self, lobby_id: str) -> None:
        async with self.lock:
            lobby = self.lobbies_by_id.get(lobby_id)
            if lobby is None or lobby.status != LobbyStatus.waiting:
                return
            if len(lobby.players) != lobby.max_players:
                return
            if not all(player.car_config for player in lobby.players.values()):
                return
            if not all(player_id in self.player_connections for player_id in lobby.players):
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

    async def _countdown_to_match(self, lobby_id: str) -> None:
        try:
            await asyncio.sleep(self.settings.auto_start_countdown_sec)
            async with self.lock:
                lobby = self.lobbies_by_id.get(lobby_id)
                if lobby is None or lobby.status != LobbyStatus.starting:
                    return
                match_id = f"match_{uuid4().hex[:12]}"
                players = {
                    player_id: MatchPlayer(
                        player_id=player.player_id,
                        player_name=player.player_name,
                        car_config=player.car_config,
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
                lobby.match_id = match_id
                lobby.status = LobbyStatus.in_game
            logger.info("match_created lobby_id=%s match_id=%s", lobby_id, match_id)
            await self._broadcast_lobby_event(
                lobby_id,
                {"type": "match_created", "match_id": match_id, "lobby_id": lobby_id, "map_id": match.map_id},
            )
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
                logger.info("match_started match_id=%s", match_id)
                await self._broadcast_match_players(
                    match_id,
                    {"type": "match_started", "match_id": match_id, "server_tick": 0},
                )
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
                        self._simulate_player(player, tick_interval)
                        if (
                            now - player.last_packet_at >= timedelta(seconds=self.settings.disconnect_timeout_sec)
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
                    await self._broadcast_match_players(match_id, snapshot)
        finally:
            async with self.lock:
                self.match_tasks.pop(match_id, None)

    def _simulate_player(self, player: MatchPlayer, dt: float) -> None:
        yaw = player.rotation.y
        speed = math.sqrt(player.velocity.x**2 + player.velocity.z**2)
        acceleration = (player.input_state.throttle * 14.0) - (player.input_state.brake * 18.0) - (speed * 0.4)
        if player.input_state.nitro:
            acceleration += 8.0
        if player.input_state.handbrake:
            acceleration -= 6.0
        speed = max(0.0, min(65.0, speed + acceleration * dt))
        turn_multiplier = 0.7 if player.input_state.handbrake else 1.0
        yaw += player.input_state.steer * 120.0 * turn_multiplier * dt
        radians = math.radians(yaw)
        player.velocity.x = math.sin(radians) * speed
        player.velocity.z = math.cos(radians) * speed
        player.position.x += player.velocity.x * dt
        player.position.z += player.velocity.z * dt
        player.rotation.y = yaw % 360

    def _check_lobby_rate_limit(self, player_id: str) -> None:
        if not self.lobby_rate_limit.hit(player_id):
            raise invalid_request("Lobby action rate limit exceeded")

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
            "match_id": lobby.match_id,
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
                    "car_config": player.car_config,
                }
                for player in match.players.values()
            ],
        }

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

    def serialize_public_session(self, session: Session) -> dict[str, Any]:
        return self._serialize_session(session)
