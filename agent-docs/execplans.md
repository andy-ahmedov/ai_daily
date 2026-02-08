# Execution Plans

Use these command sequences for predictable pipeline runs.

## Daily publish run (recommended)
1. `aidigest doctor`
2. `aidigest ingest`
3. `aidigest summarize --limit 100`
4. `aidigest embed --limit 200`
5. `aidigest dedup`
6. `aidigest publish`

Notes:
- Window is computed in `Europe/Riga` (`13:00 previous day -> 13:00 current day` by default).
- `publish` is idempotent for a window unless `--force` is used.

## One-shot pipeline run
- `aidigest run-once --date YYYY-MM-DD`
- Uses the same stage order as scheduler: ingest -> summarize -> embed -> dedup -> publish.
- Marks window status in DB (`ingested`, `summarized`, `embedded`, `deduped`, `published`, `failed`).

## Backfill or safe validation
- Dry-run ingest: `aidigest ingest --date YYYY-MM-DD --dry-run`
- Dry-run dedup: `aidigest dedup --date YYYY-MM-DD --dry-run`
- Validate exact duplicates without writes: `aidigest dedup:report --date YYYY-MM-DD`

## Failure handling
- Re-run the same command first; retries and idempotency are built into several stages.
- If run-once fails, inspect logs and window status, fix config/data issue, then re-run `aidigest run-once --date YYYY-MM-DD`.
