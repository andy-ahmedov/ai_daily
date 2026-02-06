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

## Ingest posts

1) Добавить каналы (через bot `/add ...` или CLI `aidigest tg:add ...`).
2) Запустить ingest за окно `вчера 13:00 -> сегодня 13:00` (TIMEZONE из `.env`):

```bash
aidigest ingest
```

3) Запустить за конкретную дату:

```bash
aidigest ingest --date 2026-02-07
```

4) Прогон без записи в БД:

```bash
aidigest ingest --dry-run
```

Проверка в БД:

```bash
psql "$DATABASE_URL" -c "SELECT count(*) FROM posts;"
```

## Exact dedup by content_hash

`content_hash` строится из нормализованного текста поста. Если текст пустой, но есть медиа, хэш строится по маркеру `media-only` + `posted_at` + `permalink`.

Отчет по exact-дублям за ingest-окно:

```bash
aidigest dedup:report
```

За конкретную дату окна:

```bash
aidigest dedup:report --date 2026-02-07
```

Команда ничего не изменяет в БД и показывает top-10 групп с одинаковым `content_hash`, количеством дублей и каналами, где они встретились.

## Telegram bot

1) Заполнить `BOT_TOKEN` и `ADMIN_TG_USER_ID` (или `ALLOWED_USER_IDS`) в `.env`.
2) Запустить:

```bash
aidigest bot:run
```

Примеры команд в личке:

```text
/start
/add @telegram
/list
/remove @telegram
/list_all
/status
```
