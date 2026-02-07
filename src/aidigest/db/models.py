from __future__ import annotations

from datetime import datetime
from typing import Any

from pgvector.sqlalchemy import Vector
import sqlalchemy as sa
from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Identity,
    Index,
    Integer,
    BigInteger,
    SmallInteger,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Channel(Base):
    __tablename__ = "channels"

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    tg_peer_id: Mapped[int] = mapped_column(BigInteger, unique=True, nullable=False)
    username: Mapped[str | None] = mapped_column(Text, nullable=True)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, server_default=sa.text("true"), nullable=False)
    added_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
    )
    last_fetched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class Post(Base):
    __tablename__ = "posts"
    __table_args__ = (
        UniqueConstraint("channel_id", "message_id", name="posts_channel_message_uidx"),
        Index("posts_time_idx", "posted_at"),
        Index("posts_hash_idx", "content_hash"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    channel_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("channels.id", ondelete="CASCADE"), nullable=False
    )
    message_id: Mapped[int] = mapped_column(Integer, nullable=False)
    posted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    edited_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    text: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    has_media: Mapped[bool] = mapped_column(Boolean, server_default=sa.text("false"), nullable=False)
    views: Mapped[int | None] = mapped_column(Integer, nullable=True)
    forwards: Mapped[int | None] = mapped_column(Integer, nullable=True)
    reactions: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    permalink: Mapped[str | None] = mapped_column(Text, nullable=True)
    content_hash: Mapped[str] = mapped_column(Text, nullable=False)
    lang: Mapped[str | None] = mapped_column(Text, nullable=True)
    embedding: Mapped[list[float] | None] = mapped_column(Vector(256), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
    )


class PostSummary(Base):
    __tablename__ = "post_summaries"

    post_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("posts.id", ondelete="CASCADE"), primary_key=True
    )
    key_point: Mapped[str] = mapped_column(Text, nullable=False)
    why_it_matters: Mapped[str | None] = mapped_column(Text, nullable=True)
    tags: Mapped[list[str]] = mapped_column(
        ARRAY(Text), server_default=sa.text("'{}'"), nullable=False
    )
    importance: Mapped[int] = mapped_column(
        SmallInteger, server_default=sa.text("3"), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
    )


class Window(Base):
    __tablename__ = "windows"
    __table_args__ = (
        UniqueConstraint("start_at", "end_at", name="windows_range_uidx"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    start_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    end_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    status: Mapped[str] = mapped_column(Text, server_default=sa.text("'new'"), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
    )


class DedupCluster(Base):
    __tablename__ = "dedup_clusters"

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    window_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("windows.id", ondelete="CASCADE"), nullable=False
    )
    label: Mapped[str | None] = mapped_column(Text, nullable=True)
    representative_post_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("posts.id"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
    )


class DedupClusterPost(Base):
    __tablename__ = "dedup_cluster_posts"

    cluster_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("dedup_clusters.id", ondelete="CASCADE"), primary_key=True
    )
    post_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("posts.id", ondelete="CASCADE"), primary_key=True
    )
    similarity: Mapped[float | None] = mapped_column(Float, nullable=True)


class Digest(Base):
    __tablename__ = "digests"

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    window_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("windows.id", ondelete="CASCADE"), unique=True, nullable=False
    )
    channel_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    message_ids: Mapped[list[int]] = mapped_column(
        ARRAY(Integer), server_default=sa.text("'{}'"), nullable=False
    )
    content: Mapped[str] = mapped_column(Text, nullable=False)
    stats: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class Setting(Base):
    __tablename__ = "settings"

    key: Mapped[str] = mapped_column(Text, primary_key=True)
    value: Mapped[str] = mapped_column(Text, nullable=False)


Index("channels_active_idx", Channel.is_active)
