# dtk

Data Toolkit: a DuckDB-backed data access layer for financial data. Python port of the R package `ryzmisc`.

Pairs with [qtk](../qtk) (Quant Toolkit): dtk handles storage and retrieval, qtk handles time series analytics.

## Installation

```bash
uv sync --all-groups
uv pip install -e . --reinstall
```

## Usage

```python
import dtk

# Local DuckDB file
store = dtk.Store.from_duckdb("path/to/data.duckdb")

# MotherDuck (set MOTHERDUCK_TOKEN env var)
store = dtk.Store.from_duckdb("md:my_database")
```

### Point-in-time snapshot (cdp)

```python
from datetime import date

df = store.cdp(["SPY", "AAPL"], ["PX_CLOSE", "PX_OPEN"])
df = store.cdp(["SPY"], ["PX_CLOSE"], dt=date(2024, 6, 30), date_mode="as_of")
```

### Historical time series (cdh)

```python
df = store.cdh(["SPY"], ["PX_CLOSE"], start_dt=date(2024, 1, 1))
df = store.cdh(["SPY"], ["PX_CLOSE"], start_dt=date(2023, 1, 1), period="M")
```

### Data sets (cds)

```python
divs   = store.cds_dividend(["SPY"], start_dt=date(2024, 1, 1))
events = store.cds_event(["AAPL"], event_type="Split")
adj    = store.cds_adjfactor(["SPY"])
```

### Upload (cdu)

```python
store.cdu_pricing(df)          # upsert to Pricing table
store.cdu_security(df)         # upsert to SecurityMaster
store.cdu_dataset(df, "dividend")
store.cdu(df, flds=["PX_CLOSE"])   # generic upload, routes to wide/long tables
```

## API reference

All methods live on `Store`. All outputs are Polars DataFrames.

| Method                                         | Description                               |
| ---------------------------------------------- | ----------------------------------------- |
| `cdp(x, flds, dt, ...)`                        | Cross-sectional snapshot at a single date |
| `cdh(x, flds, start_dt, end_dt, period, ...)`  | Time series over a date range             |
| `cds(x, dataset, start_dt, end_dt, ...)`       | Event-style data set query                |
| `cds_dividend(x, ...)`                         | Dividend history                          |
| `cds_event(x, ...)`                            | Corporate events                          |
| `cds_adjfactor(x, ...)`                        | Adjustment factors                        |
| `cdu(df, flds, ...)`                           | Generic field upload                      |
| `cdu_pricing(df)`                              | Pricing upsert                            |
| `cdu_security(df)`                             | Security master upsert                    |
| `cdu_dataset(df, dataset)`                     | Event data set insert                     |
| `cdu_override(security_id, field, value, ...)` | Set a field override                      |

### Common parameters

| Parameter   | Values                                                 | Default    |
| ----------- | ------------------------------------------------------ | ---------- |
| `x`         | list of tickers or integer IDs                         |            |
| `id_type`   | `"ticker"`, `"id"`, `"blp"`                            | `"ticker"` |
| `date_mode` | `"as_of"`, `"as_seen"`                                 | `"as_of"`  |
| `fill`      | `"NA"`, `"P"` (forward-fill)                           | `"NA"`     |
| `period`    | `"D"`, `"W"`, `"M"`, `"Q"`, `"Y"`                      | `"D"`      |
| `days`      | `"N"` (non-trading), `"C"` (calendar), `"T"` (trading) | `"N"`      |
| `exch_code` | `"US"`, `"LN"`, `"AU"`, `"CN"`, ...                    | `"US"`     |
| `fx`        | target currency string, e.g. `"USD"`                   | `None`     |

## Storage model

The DuckDB schema has three storage tiers:

- **Wide tables** (`Pricing`, `WeeklyData`, `MonthlyData`): one row per (SecurityId, ValueDate), used for regular time series fields.
- **Long tables** (`FieldSnapshot`, `SecuritySnapshot`): EAV format with full revision history via `(AsOfDate, LastFlag)`. Supports both `as_of` (latest value known up to a date) and `as_seen` (value recorded on that exact date) queries.
- **Event tables** (`Dividend`, `CorpEvent`, `AdjFactor`): irregularly-spaced event data.

Field definitions in `FieldDef` specify which tier each field uses (`StorageMode = "wide"` or `"long"`) and which table (`StorageTable`).

## Dependencies

- `duckdb >= 1.0`
- `polars >= 1.0`
- `exchange-calendars >= 4.0`
