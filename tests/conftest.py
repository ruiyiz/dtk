"""Shared test fixtures."""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from dtk import Store


@pytest.fixture
def store() -> Store:
    """In-memory DuckDB store with initialized schema."""
    return Store.from_duckdb(":memory:")


@pytest.fixture
def seeded_store(store: Store) -> Store:
    """Store with minimal seed data for testing queries."""
    # Insert two securities
    store._backend.execute(
        """
        INSERT INTO SecurityMaster (Id, Ticker, SecurityType, Currency, ExchangeCode, IsActive)
        VALUES
          (1, 'SPY', 'ETF', 'USD', 'US', TRUE),
          (2, 'AAPL', 'ETF', 'USD', 'US', TRUE)
        """
    )

    # Insert field definitions
    store._backend.execute(
        """
        INSERT INTO FieldDef
          (FieldId, FieldMnemonic, FieldName, DataType, StorageMode, StorageTable,
           IsCdp, IsCdh, IsCds, IsCdu)
        VALUES
          (1, 'PX_CLOSE', 'Close Price', 'dbl', 'wide', 'Pricing',
           TRUE, TRUE, FALSE, TRUE),
          (2, 'PX_OPEN',  'Open Price',  'dbl', 'wide', 'Pricing',
           TRUE, TRUE, FALSE, TRUE)
        """
    )

    # Reload field registry
    from dtk._field_registry import FieldRegistry

    store._fields = FieldRegistry(store._backend)

    # Insert pricing rows
    store._backend.execute(
        """
        INSERT INTO Pricing (SecurityId, ValueDate, PxClose, PxOpen)
        VALUES
          (1, '2024-01-02', 450.0, 448.0),
          (1, '2024-01-03', 452.0, 450.5),
          (2, '2024-01-02', 185.0, 184.0),
          (2, '2024-01-03', 186.0, 185.5)
        """
    )

    return store
