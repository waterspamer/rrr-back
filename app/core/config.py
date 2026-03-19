from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "RRR Backend"
    api_prefix: str = "/api/v1"
    host: str = "0.0.0.0"
    port: int = 8080
    node_env: str = "production"
    log_level: str = "INFO"
    cors_origin: str = "*"
    session_ttl_hours: int = 24
    auto_start_countdown_sec: int = 3
    match_tick_rate: int = 20
    match_broadcast_rate: int = 10
    match_load_timeout_sec: int = 10
    disconnect_timeout_sec: int = 10
    admin_token: str = ""
    guest_session_rate_limit: int = 20
    lobby_action_rate_limit: int = 60
    websocket_message_max_bytes: int = 32768
    max_waiting_lobbies: int = 100
    max_lobby_name_len: int = 32
    min_lobby_name_len: int = 3
    max_players_per_lobby: int = 8
    min_players_per_lobby: int = 2
    car_config_max_bytes: int = 32 * 1024
    car_config_max_customizations: int = 128
    pagination_default_size: int = 50
    pagination_max_size: int = 100
    docs_url: str | None = Field(default="/docs")
    redoc_url: str | None = Field(default="/redoc")


@lru_cache
def get_settings() -> Settings:
    return Settings()
