from fastapi import Header, Request

from app.core.errors import unauthorized
from app.services.runtime import RuntimeState


def get_runtime(request: Request) -> RuntimeState:
    return request.app.state.runtime


def get_bearer_token(authorization: str | None = Header(default=None)) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise unauthorized()
    token = authorization.split(" ", 1)[1].strip()
    if not token:
        raise unauthorized()
    return token
