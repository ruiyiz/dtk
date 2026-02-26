# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install / sync dependencies (dev group included)
uv sync --all-groups
# After uv sync, editable install must be refreshed if _dtk.pth is empty:
uv pip install -e . --reinstall

# Run all tests
uv run pytest

# Run a single test
uv run pytest tests/test_store.py::test_schema_initialized

# Lint
uv run ruff check src/ tests/

# Verify import works
uv run python -c "from dtk import Store"
```

## Architecture

`dtk` is a DuckDB-backed data access layer for financial data (pricing, security master, field snapshots, dividends, corporate events, adjustment factors). It is a Python port of the R package `ryzmisc`.

### Core objects

**`Store`** (`store.py`) is the sole public entry point. It owns a `Backend` and a `FieldRegistry` and exposes four families of methods:

- `cdp(...)` -- point-in-time cross-sectional snapshot
- `cdh(...)` -- historical time series
- `cds*(...)` -- event data sets (dividend, event, adjfactor)
- `cdu*(...)` -- data upload / upsert

`Store.from_duckdb(path)` is the factory. It calls `backend.init_schema()` **before** constructing the `Store` instance (which loads `FieldRegistry`, which queries `FieldDef`).

**`DuckDBBackend`** (`_duckdb_backend.py`) wraps a `duckdb.Connection`. `query()` returns `pl.DataFrame` built via `fetchall()` + `description` (no pyarrow). `init_schema()` executes `src/dtk/sql/init.sql` statement-by-statement, stripping comment lines before executing each fragment to avoid silently dropping CREATE TABLE statements preceded by `--` blocks.

**`FieldRegistry`** (`_field_registry.py`) loads the entire `FieldDef` table into memory once on `Store.__init__`. No refresh/TTL -- create a new Store if field definitions change. Provides `map_to_storage(flds)` which returns a dict keyed by `StorageMode` (`"wide"` or `"long"`).

### Storage model (mirrors the SQL schema)

- **Wide tables** (`Pricing`, `WeeklyData`, `MonthlyData`): regular time series with one row per (SecurityId, ValueDate). Field mnemonics map to column names via `WIDE_COLUMN_MAP` in `_constants.py`.
- **Long tables** (`FieldSnapshot`, `SecuritySnapshot`): EAV with revision history. Each row has `(SecurityId, FieldId, ValueDate, AsOfDate, LastFlag, Val{Chr,Dbl,Int,Date})`. Point-in-time queries use `LastFlag=TRUE` (as_seen) or a ranked CTE on `AsOfDate` (as_of).
- **Event tables** (`Dividend`, `CorpEvent`, `AdjFactor`): irregularly-spaced, accessed via `cds*`.

### Field routing

`cdp`/`cdh`/`cdu` call `FieldRegistry.map_to_storage(flds)` to split requested fields into wide vs. long subsets, then query/write each storage type separately and join results on `(SecurityId[, ValueDate])`.

### Date utilities

`date_utils.py` replaces RQuantLib with `exchange-calendars`. `EXCHANGE_CALENDAR_MAP` in `_constants.py` maps ryzmisc exchange codes (US, LN, AU, ...) to MIC codes (XNYS, XLON, XASX, ...). `seq_date()` is the central generator used by `prev_date`, `next_date`, and the period-scaling logic in `cdh`.

### Implementation status

Fully implemented: `errors`, `_types`, `_constants`, `_backend`, `_duckdb_backend`, `_field_registry`, `store`, `convert`, `security`, `date_utils`, `cds`.

Stubs raising `NotImplementedError`: `cdp`, `cdh`, `cdu`.
