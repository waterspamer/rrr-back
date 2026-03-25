from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Depends, FastAPI, Query, Request, Response, WebSocket
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from app.api.deps import get_admin_token, get_bearer_token, get_runtime
from app.core.config import Settings, get_settings
from app.core.errors import ApiError
from app.schemas import (
    AdminLobbyCloseResponse,
    AdminGameSettingsResponse,
    AdminGameSettingsUpdateRequest,
    AdminLobbiesResponse,
    AdminLobbyResponse,
    AdminPlayersResponse,
    AdminMatchDetailResponse,
    AdminMatchesResponse,
    HealthResponse,
    LobbyCarConfigUpdateRequest,
    LobbyCreateRequest,
    LobbyCreateResponse,
    LobbyDetailResponse,
    LobbyJoinRequest,
    LobbyJoinResponse,
    LobbyStartSoloResponse,
    MatchInfoResponse,
    PaginatedLobbiesResponse,
    PlayerProfileResponse,
    PlayerProfileUpdateRequest,
    SessionGuestCreateRequest,
    SessionResponse,
    SimpleSuccessResponse,
)
from app.services.runtime import RuntimeState


STATIC_DIR = Path(__file__).resolve().parent / "static"


def configure_logging(settings: Settings) -> None:
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    configure_logging(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.runtime = RuntimeState(settings)
        yield
        await app.state.runtime.shutdown()

    app = FastAPI(
        title=settings.app_name,
        version="0.1.0",
        docs_url=settings.docs_url,
        redoc_url=settings.redoc_url,
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"] if settings.cors_origin == "*" else [settings.cors_origin],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.mount("/admin-assets", StaticFiles(directory=STATIC_DIR), name="admin-assets")

    @app.middleware("http")
    async def admin_no_cache(request: Request, call_next):
        response: Response = await call_next(request)
        if request.url.path == "/admin" or request.url.path == "/admin/" or request.url.path.startswith("/admin-assets/"):
            response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"
        return response

    @app.exception_handler(ApiError)
    async def api_error_handler(_: Request, exc: ApiError) -> JSONResponse:
        return JSONResponse(status_code=exc.status_code, content=exc.detail)

    @app.exception_handler(RequestValidationError)
    async def request_validation_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=400,
            content={"code": "INVALID_REQUEST", "message": "Request validation failed", "details": exc.errors()},
        )

    @app.get(f"{settings.api_prefix}/health", response_model=HealthResponse)
    async def health(runtime: RuntimeState = Depends(get_runtime)) -> dict[str, int | str]:
        return {"status": "ok", **runtime.stats()}

    @app.get("/admin", include_in_schema=False)
    @app.get("/admin/", include_in_schema=False)
    async def admin_panel() -> FileResponse:
        return FileResponse(STATIC_DIR / "admin" / "index.html")

    @app.post(f"{settings.api_prefix}/sessions/guest", response_model=SessionResponse, status_code=201)
    async def create_guest_session(
        payload: SessionGuestCreateRequest,
        request: Request,
        runtime: RuntimeState = Depends(get_runtime),
    ) -> dict[str, object]:
        session = await runtime.create_guest_session(payload.player_name, request.client.host if request.client else "unknown")
        return runtime.serialize_public_session(session)

    @app.get(f"{settings.api_prefix}/players/me", response_model=PlayerProfileResponse)
    async def get_player_profile(
        token: str = Depends(get_bearer_token),
        runtime: RuntimeState = Depends(get_runtime),
    ) -> dict[str, object]:
        return await runtime.get_player_profile(session_token=token)

    @app.put(f"{settings.api_prefix}/players/me", response_model=PlayerProfileResponse)
    async def update_player_profile(
        payload: PlayerProfileUpdateRequest,
        token: str = Depends(get_bearer_token),
        runtime: RuntimeState = Depends(get_runtime),
    ) -> dict[str, object]:
        return await runtime.update_player_profile(session_token=token, payload=payload.model_dump(mode="json", exclude_none=True))

    @app.get(f"{settings.api_prefix}/lobbies", response_model=PaginatedLobbiesResponse)
    async def list_lobbies(
        status: str | None = Query(default=None),
        map_id: str | None = Query(default=None),
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=settings.pagination_default_size, ge=1, le=settings.pagination_max_size),
        runtime: RuntimeState = Depends(get_runtime),
    ) -> dict[str, object]:
        items, total = await runtime.list_lobbies(status=status, map_id=map_id, page=page, page_size=page_size)
        return {"items": items, "total": total}

    @app.post(f"{settings.api_prefix}/lobbies", response_model=LobbyCreateResponse, status_code=201)
    async def create_lobby(
        payload: LobbyCreateRequest,
        token: str = Depends(get_bearer_token),
        runtime: RuntimeState = Depends(get_runtime),
    ) -> dict[str, object]:
        return await runtime.create_lobby(
            session_token=token,
            name=payload.name,
            map_id=payload.map_id,
            max_players=payload.max_players,
            car_config=payload.car_config.model_dump(mode="json"),
        )

    @app.get(f"{settings.api_prefix}/lobbies/{{lobby_id}}", response_model=LobbyDetailResponse)
    async def get_lobby(lobby_id: str, runtime: RuntimeState = Depends(get_runtime)) -> dict[str, object]:
        return await runtime.get_lobby(lobby_id)

    @app.post(f"{settings.api_prefix}/lobbies/{{lobby_id}}/join", response_model=LobbyJoinResponse)
    async def join_lobby(
        lobby_id: str,
        payload: LobbyJoinRequest,
        token: str = Depends(get_bearer_token),
        runtime: RuntimeState = Depends(get_runtime),
    ) -> dict[str, object]:
        return await runtime.join_lobby(
            session_token=token,
            lobby_id=lobby_id,
            car_config=payload.car_config.model_dump(mode="json"),
        )

    @app.post(f"{settings.api_prefix}/lobbies/{{lobby_id}}/start-solo", response_model=LobbyStartSoloResponse)
    async def start_lobby_solo(
        lobby_id: str,
        token: str = Depends(get_bearer_token),
        runtime: RuntimeState = Depends(get_runtime),
    ) -> dict[str, object]:
        return await runtime.start_solo(session_token=token, lobby_id=lobby_id)

    @app.post(f"{settings.api_prefix}/lobbies/{{lobby_id}}/leave", response_model=SimpleSuccessResponse)
    async def leave_lobby(
        lobby_id: str,
        token: str = Depends(get_bearer_token),
        runtime: RuntimeState = Depends(get_runtime),
    ) -> dict[str, object]:
        return await runtime.leave_lobby(session_token=token, lobby_id=lobby_id)

    @app.put(f"{settings.api_prefix}/lobbies/{{lobby_id}}/car-config", response_model=SimpleSuccessResponse)
    async def update_lobby_car_config(
        lobby_id: str,
        payload: LobbyCarConfigUpdateRequest,
        token: str = Depends(get_bearer_token),
        runtime: RuntimeState = Depends(get_runtime),
    ) -> dict[str, object]:
        return await runtime.update_car_config(
            session_token=token,
            lobby_id=lobby_id,
            car_config=payload.car_config.model_dump(mode="json"),
        )

    @app.get(f"{settings.api_prefix}/matches/{{match_id}}", response_model=MatchInfoResponse)
    async def get_match(match_id: str, runtime: RuntimeState = Depends(get_runtime)) -> dict[str, object]:
        return await runtime.get_match(match_id)

    @app.get(f"{settings.api_prefix}/admin/lobbies", response_model=AdminLobbiesResponse)
    async def get_admin_lobbies(
        admin_token: str | None = Depends(get_admin_token),
        runtime: RuntimeState = Depends(get_runtime),
    ) -> dict[str, object]:
        runtime.validate_admin_token(admin_token)
        return await runtime.list_admin_lobbies()

    @app.get(f"{settings.api_prefix}/admin/lobbies/{{lobby_id}}", response_model=AdminLobbyResponse)
    async def get_admin_lobby(
        lobby_id: str,
        admin_token: str | None = Depends(get_admin_token),
        runtime: RuntimeState = Depends(get_runtime),
    ) -> dict[str, object]:
        runtime.validate_admin_token(admin_token)
        return await runtime.get_admin_lobby(lobby_id)

    @app.get(f"{settings.api_prefix}/admin/players", response_model=AdminPlayersResponse)
    async def get_admin_players(
        admin_token: str | None = Depends(get_admin_token),
        runtime: RuntimeState = Depends(get_runtime),
    ) -> dict[str, object]:
        runtime.validate_admin_token(admin_token)
        return await runtime.list_admin_players()

    @app.get(f"{settings.api_prefix}/admin/players/{{player_id}}", response_model=PlayerProfileResponse)
    async def get_admin_player(
        player_id: str,
        admin_token: str | None = Depends(get_admin_token),
        runtime: RuntimeState = Depends(get_runtime),
    ) -> dict[str, object]:
        runtime.validate_admin_token(admin_token)
        return await runtime.get_admin_player(player_id)

    @app.delete(f"{settings.api_prefix}/admin/lobbies/{{lobby_id}}", response_model=AdminLobbyCloseResponse)
    async def close_admin_lobby(
        lobby_id: str,
        admin_token: str | None = Depends(get_admin_token),
        runtime: RuntimeState = Depends(get_runtime),
    ) -> dict[str, object]:
        runtime.validate_admin_token(admin_token)
        return await runtime.close_lobby(lobby_id, reason="admin_killed")

    @app.get(f"{settings.api_prefix}/admin/matches", response_model=AdminMatchesResponse)
    async def get_admin_matches(
        admin_token: str | None = Depends(get_admin_token),
        runtime: RuntimeState = Depends(get_runtime),
    ) -> dict[str, object]:
        runtime.validate_admin_token(admin_token)
        return await runtime.list_admin_matches()

    @app.get(f"{settings.api_prefix}/admin/matches/{{match_id}}", response_model=AdminMatchDetailResponse)
    async def get_admin_match(
        match_id: str,
        admin_token: str | None = Depends(get_admin_token),
        runtime: RuntimeState = Depends(get_runtime),
    ) -> dict[str, object]:
        runtime.validate_admin_token(admin_token)
        return await runtime.get_admin_match(match_id)

    @app.get(f"{settings.api_prefix}/admin/game-settings", response_model=AdminGameSettingsResponse)
    async def get_admin_game_settings(
        admin_token: str | None = Depends(get_admin_token),
        runtime: RuntimeState = Depends(get_runtime),
    ) -> dict[str, object]:
        runtime.validate_admin_token(admin_token)
        return await runtime.get_admin_game_settings()

    @app.put(f"{settings.api_prefix}/admin/game-settings", response_model=AdminGameSettingsResponse)
    async def update_admin_game_settings(
        request: AdminGameSettingsUpdateRequest,
        admin_token: str | None = Depends(get_admin_token),
        runtime: RuntimeState = Depends(get_runtime),
    ) -> dict[str, object]:
        runtime.validate_admin_token(admin_token)
        return await runtime.update_admin_game_settings(request.model_dump(mode="json"))

    @app.websocket(f"{settings.api_prefix}/ws")
    async def websocket_endpoint(websocket: WebSocket) -> None:
        token = websocket.query_params.get("session_token")
        runtime: RuntimeState = websocket.app.state.runtime
        if not token:
            await websocket.accept()
            await websocket.send_json({"type": "error", "code": "UNAUTHORIZED", "message": "session_token is required"})
            await websocket.close(code=4401)
            return
        try:
            session = await runtime.register_connection(token, websocket)
            await runtime.websocket_loop(session.player_id, websocket)
        except ApiError as exc:
            if websocket.client_state.name == "CONNECTING":
                await websocket.accept()
            await websocket.send_json({"type": "error", **exc.detail})
            await websocket.close(code=4401)
        except Exception as exc:  # pragma: no cover
            logging.getLogger("rrr.ws").exception("websocket_error %s", exc)
            try:
                await websocket.send_json({"type": "error", "code": "INTERNAL_ERROR", "message": "Internal error"})
            except RuntimeError:
                pass
        finally:
            if "session" in locals():
                await runtime.unregister_connection(session.player_id)

    @app.websocket(f"{settings.api_prefix}/admin/ws")
    async def admin_websocket_endpoint(websocket: WebSocket) -> None:
        token = websocket.query_params.get("token")
        runtime: RuntimeState = websocket.app.state.runtime
        try:
            await runtime.register_admin_connection(token, websocket)
            await runtime.admin_websocket_loop(websocket)
        except ApiError as exc:
            if websocket.client_state.name == "CONNECTING":
                await websocket.accept()
            await websocket.send_json({"type": "error", **exc.detail})
            await websocket.close(code=4401)
        finally:
            await runtime.unregister_admin_connection(websocket)

    return app


app = create_app()
