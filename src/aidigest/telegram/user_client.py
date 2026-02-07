from __future__ import annotations

import re
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from loguru import logger
from telethon import TelegramClient
from telethon.errors import (
    ChannelPrivateError,
    InviteHashExpiredError,
    InviteHashInvalidError,
    UserAlreadyParticipantError,
    UsernameInvalidError,
    UsernameNotOccupiedError,
)
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.tl.types import Channel, Chat, User
from telethon.utils import get_peer_id, resolve_id

_INVITE_RE = re.compile(r"(?:^|/)joinchat/(?P<hash>[\w-]+)$", re.IGNORECASE)


def _extract_invite_hash(ref: str) -> str | None:
    cleaned = ref.strip()
    if cleaned.startswith("https://") or cleaned.startswith("http://"):
        parsed = urlparse(cleaned)
        path = parsed.path.strip("/")
    else:
        if cleaned.startswith("t.me/"):
            parsed = urlparse(f"https://{cleaned}")
            path = parsed.path.strip("/")
        else:
            path = cleaned.strip("/")

    if path.startswith("+"):
        return path[1:]

    match = _INVITE_RE.search(path)
    if match:
        return match.group("hash")

    return None


def _extract_username(ref: str) -> str:
    cleaned = ref.strip()
    if cleaned.startswith("@"):
        return cleaned

    if cleaned.startswith("https://") or cleaned.startswith("http://"):
        parsed = urlparse(cleaned)
        path = parsed.path.strip("/")
        if not path:
            raise ValueError("cannot resolve: empty reference")
        return path.split("/")[0]

    if cleaned.startswith("t.me/"):
        parsed = urlparse(f"https://{cleaned}")
        path = parsed.path.strip("/")
        if not path:
            raise ValueError("cannot resolve: empty reference")
        return path.split("/")[0]

    if not cleaned:
        raise ValueError("cannot resolve: empty reference")

    return cleaned


class UserTelegramClient:
    def __init__(self, api_id: int, api_hash: str, session_path: str) -> None:
        self.api_id = api_id
        self.api_hash = api_hash
        self.session_path = Path(session_path)
        self._client = TelegramClient(str(self.session_path), api_id, api_hash)

    @property
    def client(self) -> TelegramClient:
        return self._client

    async def connect(self, allow_interactive_login: bool = True) -> None:
        self.session_path.parent.mkdir(parents=True, exist_ok=True)
        await self._client.connect()
        if not await self._client.is_user_authorized():
            if not allow_interactive_login:
                await self._client.disconnect()
                raise RuntimeError(
                    "Telethon session is not authorized. Run `aidigest tg:whoami` first."
                )
            logger.info("Telethon session not authorized. Starting interactive login.")
            await self._client.start()

    async def disconnect(self) -> None:
        await self._client.disconnect()

    async def whoami(self) -> str:
        me: User = await self._client.get_me()
        if me.username:
            return f"@{me.username}"
        if me.phone:
            return me.phone
        return str(me.id)

    async def resolve_entity(self, ref: str) -> Any:
        invite_hash = _extract_invite_hash(ref)
        if invite_hash:
            return await self._import_invite(invite_hash)

        username = _extract_username(ref)
        try:
            return await self._client.get_entity(username)
        except UsernameInvalidError as exc:
            logger.warning("Invalid username: {}", ref)
            raise RuntimeError("cannot resolve: invalid username") from exc
        except UsernameNotOccupiedError as exc:
            logger.warning("Username not found: {}", ref)
            raise RuntimeError("cannot resolve: username not found") from exc
        except Exception as exc:
            logger.warning("Failed to resolve entity: {} ({})", ref, exc)
            raise RuntimeError("cannot resolve: failed to resolve entity") from exc

    async def resolve_entity_by_peer_id(self, tg_peer_id: int) -> Any:
        try:
            peer_id, peer_type = resolve_id(tg_peer_id)
            peer = peer_type(peer_id)
            return await self._client.get_entity(peer)
        except Exception as exc:
            logger.warning("Failed to resolve entity by peer id: {} ({})", tg_peer_id, exc)
            raise RuntimeError("cannot resolve: failed to resolve entity") from exc

    async def ensure_join(self, entity_or_ref: Any) -> Any:
        if isinstance(entity_or_ref, str):
            invite_hash = _extract_invite_hash(entity_or_ref)
            if invite_hash:
                return await self._import_invite(invite_hash)
            entity = await self.resolve_entity(entity_or_ref)
        else:
            entity = entity_or_ref

        if isinstance(entity, Channel):
            try:
                await self._client(JoinChannelRequest(entity))
            except UserAlreadyParticipantError:
                return entity
            except ChannelPrivateError as exc:
                raise RuntimeError("private channel requires access") from exc
            except Exception as exc:
                logger.warning("Failed to join channel: {} ({})", entity, exc)
                raise RuntimeError("cannot join channel") from exc

        return entity

    async def get_channel_info(self, ref: str) -> dict[str, Any]:
        entity = await self.resolve_entity(ref)
        return self._entity_info(entity)

    async def _import_invite(self, invite_hash: str) -> Any:
        try:
            updates = await self._client(ImportChatInviteRequest(invite_hash))
        except InviteHashInvalidError as exc:
            raise RuntimeError("invite invalid") from exc
        except InviteHashExpiredError as exc:
            raise RuntimeError("invite expired") from exc
        except ChannelPrivateError as exc:
            raise RuntimeError("private channel requires access") from exc
        except Exception as exc:
            logger.warning("Failed to import invite: {} ({})", invite_hash, exc)
            raise RuntimeError("invite invalid") from exc

        chats = getattr(updates, "chats", [])
        if chats:
            return chats[0]
        raise RuntimeError("invite invalid")

    def _entity_info(self, entity: Any) -> dict[str, Any]:
        if not isinstance(entity, (Channel, Chat)):
            raise RuntimeError("entity is not a channel or chat")

        title = getattr(entity, "title", None)
        if not title and getattr(entity, "username", None):
            title = entity.username
        if not title:
            raise RuntimeError("cannot resolve: missing channel title")

        return {
            "tg_peer_id": int(get_peer_id(entity)),
            "username": getattr(entity, "username", None),
            "title": title,
        }
