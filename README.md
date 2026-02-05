# aidigest

Bootstrap проекта для будущего Telegram-дайджеста.

## Быстрый старт

### 1) Создать и активировать venv

```bash
python -m venv .venv
source .venv/bin/activate
```

### 2) Установить зависимости

```bash
pip install -U pip
pip install -e .
```

### 3) Поднять Postgres (pgvector)

```bash
docker compose up -d
```

### 4) Создать .env

```bash
cp .env.example .env
```

### 5) Применить миграции

```bash
alembic upgrade head
```

Для создания новых миграций можно использовать:

```bash
alembic revision --autogenerate -m "describe change"
```

### 6) Проверить окружение

```bash
aidigest doctor
```

## Telethon login

1) Заполнить `TG_API_ID` и `TG_API_HASH` в `.env`.
2) Запустить:

```bash
aidigest tg:whoami
```

3) Ввести код подтверждения из Telegram. После этого сессия сохранится в `data/telethon.session` (или в путь из `TG_SESSION_PATH`).

Примеры:

```bash
aidigest tg:add @somechannel
aidigest tg:list
```
