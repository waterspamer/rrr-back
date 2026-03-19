from dataclasses import dataclass

from fastapi import HTTPException, status


@dataclass(slots=True)
class ErrorPayload:
    code: str
    message: str


class ApiError(HTTPException):
    def __init__(self, status_code: int, code: str, message: str) -> None:
        super().__init__(status_code=status_code, detail=ErrorPayload(code=code, message=message).__dict__)


def unauthorized(message: str = "Invalid or expired session token") -> ApiError:
    return ApiError(status.HTTP_401_UNAUTHORIZED, "UNAUTHORIZED", message)


def invalid_request(message: str) -> ApiError:
    return ApiError(status.HTTP_400_BAD_REQUEST, "INVALID_REQUEST", message)


def lobby_not_found() -> ApiError:
    return ApiError(status.HTTP_404_NOT_FOUND, "LOBBY_NOT_FOUND", "Lobby not found")


def lobby_full() -> ApiError:
    return ApiError(status.HTTP_409_CONFLICT, "LOBBY_FULL", "Lobby is full")


def lobby_already_started() -> ApiError:
    return ApiError(status.HTTP_409_CONFLICT, "LOBBY_ALREADY_STARTED", "Lobby already started")


def player_already_in_lobby() -> ApiError:
    return ApiError(status.HTTP_409_CONFLICT, "PLAYER_ALREADY_IN_LOBBY", "Player already in a lobby")


def player_not_in_lobby() -> ApiError:
    return ApiError(status.HTTP_409_CONFLICT, "PLAYER_NOT_IN_LOBBY", "Player is not in the lobby")


def match_not_found() -> ApiError:
    return ApiError(status.HTTP_404_NOT_FOUND, "MATCH_NOT_FOUND", "Match not found")

