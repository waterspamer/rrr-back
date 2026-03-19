from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Query, Request, WebSocket
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.api.deps import get_bearer_token, get_runtime
from app.core.config import Settings, get_settings
from app.core.errors import ApiError
from app.schemas import (
    HealthResponse,
    LobbyCarConfigUpdateRequest,
    LobbyCreateRequest,
    LobbyCreateResponse,
    LobbyDetailResponse,
    LobbyJoinRequest,
    LobbyJoinResponse,
    MatchInfoResponse,
    PaginatedLobbiesResponse,
    SessionGuestCreateRequest,
    SessionResponse,
    SimpleSuccessResponse,
)
from app.services.runtime import RuntimeState


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

    @app.post(f"{settings.api_prefix}/sessions/guest", response_model=SessionResponse, status_code=201)
    async def create_guest_session(
        payload: SessionGuestCreateRequest,
        request: Request,
        runtime: RuntimeState = Depends(get_runtime),
    ) -> dict[str, object]:
        session = await runtime.create_guest_session(payload.player_name, request.client.host if request.client else "unknown")
        return runtime.serialize_public_session(session)

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

    return app


app = create_app()
