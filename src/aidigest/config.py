from __future__ import annotations

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    tg_api_id: int | None = Field(default=None, alias="TG_API_ID")
    tg_api_hash: str | None = Field(default=None, alias="TG_API_HASH")
    tg_session_path: str = Field(default="./data/telethon.session", alias="TG_SESSION_PATH")
    bot_token: str | None = Field(default=None, alias="BOT_TOKEN")
    digest_channel_id: str | None = Field(default=None, alias="DIGEST_CHANNEL_ID")

    timezone: str = Field(default="Europe/Riga", alias="TIMEZONE")
    window_start_hour: int = Field(default=13, alias="WINDOW_START_HOUR")
    window_end_hour: int = Field(default=13, alias="WINDOW_END_HOUR")
    run_at_hour: int = Field(default=13, alias="RUN_AT_HOUR")
    run_at_minute: int = Field(default=10, alias="RUN_AT_MINUTE")

    database_url: str = Field(
        default="postgresql+psycopg://aidigest:aidigest@localhost:5432/aidigest",
        alias="DATABASE_URL",
    )

    yandex_folder_id: str | None = Field(default=None, alias="YANDEX_FOLDER_ID")
    yandex_api_key: str | None = Field(default=None, alias="YANDEX_API_KEY")
    yandex_model_uri: str | None = Field(default=None, alias="YANDEX_MODEL_URI")
    yandex_embed_model_uri: str | None = Field(default=None, alias="YANDEX_EMBED_MODEL_URI")

    embed_dim: int = Field(default=256, alias="EMBED_DIM")
    dedup_threshold: float = Field(default=0.88, alias="DEDUP_THRESHOLD")

    @field_validator("run_at_hour", "window_start_hour", "window_end_hour")
    @classmethod
    def validate_hours(cls, value: int) -> int:
        if not 0 <= value <= 23:
            raise ValueError("hour must be in range 0..23")
        return value

    @field_validator("run_at_minute")
    @classmethod
    def validate_minutes(cls, value: int) -> int:
        if not 0 <= value <= 59:
            raise ValueError("minute must be in range 0..59")
        return value

    @field_validator("embed_dim")
    @classmethod
    def validate_embed_dim(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("embed_dim must be greater than 0")
        return value

    @field_validator("database_url")
    @classmethod
    def validate_database_url(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("database_url must not be empty")
        return value


def get_settings() -> Settings:
    return Settings()
