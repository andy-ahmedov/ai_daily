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
    admin_tg_user_id: int | None = Field(default=None, alias="ADMIN_TG_USER_ID")
    allowed_user_ids: list[int] = Field(default_factory=list, alias="ALLOWED_USER_IDS")

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
    top_k_per_channel: int = Field(default=5, alias="TOP_K_PER_CHANNEL")
    min_importance_channel: int = Field(default=3, alias="MIN_IMPORTANCE_CHANNEL")
    top_k_global: int = Field(default=10, alias="TOP_K_GLOBAL")
    min_importance_global: int = Field(default=4, alias="MIN_IMPORTANCE_GLOBAL")

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

    @field_validator("top_k_per_channel", "top_k_global")
    @classmethod
    def validate_top_k(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("top_k must be greater than 0")
        return value

    @field_validator("min_importance_channel", "min_importance_global")
    @classmethod
    def validate_importance_thresholds(cls, value: int) -> int:
        if not 1 <= value <= 5:
            raise ValueError("importance threshold must be in range 1..5")
        return value

    @field_validator("allowed_user_ids", mode="before")
    @classmethod
    def parse_allowed_user_ids(cls, value: object) -> list[int]:
        if value is None:
            return []
        if isinstance(value, list):
            return [int(item) for item in value]
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return []
            return [int(item.strip()) for item in stripped.split(",") if item.strip()]
        return []

    @field_validator("database_url")
    @classmethod
    def validate_database_url(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("database_url must not be empty")
        return value


def get_settings() -> Settings:
    return Settings()
