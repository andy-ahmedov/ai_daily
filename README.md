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

## Summarize (Alice AI LLM)

Нужные переменные в `.env`:

- `YANDEX_API_KEY` (API-ключ Yandex AI Studio)
- `YANDEX_FOLDER_ID` (ID каталога в Yandex Cloud)
- `YANDEX_MODEL_URI` (например: `gpt://<folder_id>/aliceai-llm`)

Где взять значения:

- API ключ: Yandex Cloud Console -> сервисный аккаунт/AI Studio -> создать API key.
- Folder ID: карточка каталога в Yandex Cloud Console.
- Model URI: собрать как `gpt://<folder_id>/aliceai-llm`.

После ingest можно запустить суммаризацию:

```bash
aidigest ingest && aidigest summarize --limit 50
```

За конкретную дату окна:

```bash
aidigest summarize --date 2026-02-07 --limit 50
```

Команда сначала переиспользует exact-dedup summary по `content_hash`, и только для оставшихся постов вызывает LLM.

## Embeddings

Нужные переменные в `.env`:

- `YANDEX_API_KEY`
- `YANDEX_FOLDER_ID`
- `YANDEX_EMBED_MODEL_URI` (например: `emb://<folder_id>/text-search-doc/latest`)
- `EMBED_DIM=256`

После ingest можно посчитать эмбеддинги для постов без `embedding`:

```bash
aidigest embed --limit 200
```

Пример с размером батча:

```bash
aidigest embed --limit 20 --batch-size 10
```

Проверка в БД:

```bash
psql "$DATABASE_URL" -c "SELECT count(*) FROM posts WHERE embedding IS NOT NULL;"
```

Повторный запуск не переэмбеддит посты, у которых `embedding` уже заполнен.

## Semantic dedup (pgvector)

Рекомендуемый порядок запуска:

```bash
aidigest ingest
aidigest summarize --limit 100   # опционально
aidigest embed --limit 200
aidigest dedup
```

Команда `aidigest dedup` строит кластеры семантически похожих постов за окно `13:00 -> 13:00`
и заполняет `dedup_clusters`/`dedup_cluster_posts`.

Примеры:

```bash
aidigest dedup --threshold 0.88 --top-k 80
aidigest dedup --date 2026-02-07 --dry-run
```

Повторный запуск за то же окно пересчитывает результат: старые кластеры удаляются и создаются заново.

## Build digest

Последовательность для дневного прогона:

```bash
aidigest ingest
aidigest summarize --limit 100
aidigest embed --limit 200
aidigest dedup
aidigest digest --top 10
```

`aidigest digest` не отправляет сообщение в Telegram, а печатает HTML-блоки в stdout:

```text
----- MESSAGE 1/3 -----
<b>AI Digest</b> ...
```

Каждый блок уже подготовлен под Telegram HTML и ограничен по длине.

## Publish

Для публикации нужны переменные:

- `BOT_TOKEN`
- `DIGEST_CHANNEL_ID` (chat_id канала, обычно вида `-100...`)

Публикация:

```bash
aidigest publish
```

Повторный запуск для того же окна без `--force` не отправляет повторно:

```bash
aidigest publish
# Already published for window ...
```

## Scheduler

Ежедневный запуск pipeline по расписанию (`RUN_AT_HOUR`, `RUN_AT_MINUTE`, `TIMEZONE`):

```bash
aidigest scheduler:run
```

Для теста и ручного прогона есть разовый запуск:

```bash
aidigest run-once --date 2026-02-07
```

Планировщик работает как сервисный процесс (systemd/docker можно добавить отдельно).

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
/digest-now
```

## Bot operations

- `/status` показывает состояние системы: каналы, окно, посты без summary/embedding, кластеры, последний published digest и расписание.
- `/digest-now` запускает полный pipeline для текущего окна вручную.
  Если digest уже опубликован, бот сообщает время публикации и message ids (и ссылку на первое сообщение, если возможно).
