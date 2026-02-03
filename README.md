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
