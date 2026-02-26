-- DuckDB Schema for cefsys Data Access Layer
-- Run this script to initialize the database schema

-- =============================================================================
-- REFERENCE TABLES
-- =============================================================================

CREATE TABLE IF NOT EXISTS SecurityMaster (
    Id INTEGER PRIMARY KEY,
    Ticker VARCHAR NOT NULL,
    SecurityType VARCHAR NOT NULL,
    Currency VARCHAR DEFAULT 'USD',
    ExchangeCode VARCHAR DEFAULT 'US',
    BlpTicker VARCHAR,
    TiingoTicker VARCHAR,
    InceptionDate DATE,
    TerminationDate DATE,
    IsActive BOOLEAN DEFAULT TRUE,
    CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UpdatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_security_ticker ON SecurityMaster(Ticker);
CREATE INDEX IF NOT EXISTS idx_security_type ON SecurityMaster(SecurityType);
CREATE INDEX IF NOT EXISTS idx_security_active ON SecurityMaster(IsActive);

CREATE TABLE IF NOT EXISTS FieldDef (
    FieldId INTEGER PRIMARY KEY,
    FieldMnemonic VARCHAR NOT NULL UNIQUE,
    FieldName VARCHAR,
    DataType VARCHAR NOT NULL,  -- chr, dbl, int, date, json, lgl
    StorageMode VARCHAR NOT NULL,  -- wide, long
    StorageTable VARCHAR,  -- target table for this field
    Periodicity VARCHAR DEFAULT 'D',  -- D, W, M, Q, Y
    FxMode VARCHAR,  -- NULL (no conversion), 'money' (divide by FX), 'return' (return formula)
    IsCdp BOOLEAN DEFAULT FALSE,
    IsCdh BOOLEAN DEFAULT FALSE,
    IsCds BOOLEAN DEFAULT FALSE,
    IsCdu BOOLEAN DEFAULT FALSE,
    Description VARCHAR
);

CREATE INDEX IF NOT EXISTS idx_field_mnemonic ON FieldDef(FieldMnemonic);
CREATE INDEX IF NOT EXISTS idx_field_storage ON FieldDef(StorageMode, StorageTable);

CREATE TABLE IF NOT EXISTS FieldMapping (
    FieldId INTEGER NOT NULL,
    SecurityType VARCHAR NOT NULL,
    SourceTable VARCHAR NOT NULL,
    SourceColumn VARCHAR NOT NULL,
    Priority INTEGER DEFAULT 1,
    PRIMARY KEY (FieldId, SecurityType),
    FOREIGN KEY (FieldId) REFERENCES FieldDef(FieldId)
);

-- =============================================================================
-- LONG-FORMAT TABLES (time-varying fields with revision history)
-- =============================================================================

CREATE TABLE IF NOT EXISTS FieldSnapshot (
    SecurityId INTEGER NOT NULL,
    FieldId INTEGER NOT NULL,
    ValueDate DATE NOT NULL,
    AsOfDate DATE NOT NULL,
    LastFlag BOOLEAN DEFAULT TRUE,
    ValChr VARCHAR,
    ValDbl DOUBLE,
    ValInt INTEGER,
    ValDate DATE,
    PRIMARY KEY (SecurityId, FieldId, ValueDate, AsOfDate),
    FOREIGN KEY (SecurityId) REFERENCES SecurityMaster(Id),
    FOREIGN KEY (FieldId) REFERENCES FieldDef(FieldId)
);

CREATE INDEX IF NOT EXISTS idx_fieldsnapshot_latest
    ON FieldSnapshot(SecurityId, FieldId, LastFlag);
CREATE INDEX IF NOT EXISTS idx_fieldsnapshot_asof
    ON FieldSnapshot(SecurityId, FieldId, AsOfDate DESC);
CREATE INDEX IF NOT EXISTS idx_fieldsnapshot_valuedate
    ON FieldSnapshot(SecurityId, ValueDate);

CREATE TABLE IF NOT EXISTS SecuritySnapshot (
    SecurityId INTEGER NOT NULL,
    AttrName VARCHAR NOT NULL,
    ValueDate DATE NOT NULL,
    AsOfDate DATE NOT NULL,
    LastFlag BOOLEAN DEFAULT TRUE,
    ValChr VARCHAR,
    ValDbl DOUBLE,
    ValInt INTEGER,
    ValDate DATE,
    ValJson VARCHAR,
    PRIMARY KEY (SecurityId, AttrName, ValueDate, AsOfDate),
    FOREIGN KEY (SecurityId) REFERENCES SecurityMaster(Id)
);

CREATE INDEX IF NOT EXISTS idx_securitysnapshot_latest
    ON SecuritySnapshot(SecurityId, AttrName, LastFlag);
CREATE INDEX IF NOT EXISTS idx_securitysnapshot_asof
    ON SecuritySnapshot(SecurityId, AttrName, AsOfDate DESC);

-- =============================================================================
-- WIDE-FORMAT TABLES (regular time series)
-- =============================================================================

CREATE TABLE IF NOT EXISTS Pricing (
    SecurityId INTEGER NOT NULL,
    ValueDate DATE NOT NULL,
    PxClose DOUBLE,
    PxHigh DOUBLE,
    PxLow DOUBLE,
    PxOpen DOUBLE,
    PxLast DOUBLE,
    Volume DOUBLE,
    NavClose DOUBLE,
    NavLast DOUBLE,
    TotalReturn DOUBLE,
    DividendAmount DOUBLE,
    AdjFactor DOUBLE DEFAULT 1.0,
    PRIMARY KEY (SecurityId, ValueDate),
    FOREIGN KEY (SecurityId) REFERENCES SecurityMaster(Id)
);

CREATE INDEX IF NOT EXISTS idx_pricing_date ON Pricing(ValueDate);
CREATE INDEX IF NOT EXISTS idx_pricing_security ON Pricing(SecurityId, ValueDate);

CREATE TABLE IF NOT EXISTS WeeklyData (
    SecurityId INTEGER NOT NULL,
    ValueDate DATE NOT NULL,
    PxClose DOUBLE,
    PxHigh DOUBLE,
    PxLow DOUBLE,
    Volume DOUBLE,
    NavClose DOUBLE,
    TotalReturn DOUBLE,
    PRIMARY KEY (SecurityId, ValueDate),
    FOREIGN KEY (SecurityId) REFERENCES SecurityMaster(Id)
);

CREATE INDEX IF NOT EXISTS idx_weekly_date ON WeeklyData(ValueDate);

CREATE TABLE IF NOT EXISTS MonthlyData (
    SecurityId INTEGER NOT NULL,
    ValueDate DATE NOT NULL,
    PxClose DOUBLE,
    PxHigh DOUBLE,
    PxLow DOUBLE,
    Volume DOUBLE,
    NavClose DOUBLE,
    TotalReturn DOUBLE,
    DistributionRate DOUBLE,
    Discount DOUBLE,
    ZScore DOUBLE,
    PRIMARY KEY (SecurityId, ValueDate),
    FOREIGN KEY (SecurityId) REFERENCES SecurityMaster(Id)
);

CREATE INDEX IF NOT EXISTS idx_monthly_date ON MonthlyData(ValueDate);

-- =============================================================================
-- DATA SET TABLES (irregularly-spaced event data)
-- =============================================================================

CREATE TABLE IF NOT EXISTS Dividend (
    DividendId INTEGER PRIMARY KEY,
    SecurityId INTEGER NOT NULL,
    ExDate DATE NOT NULL,
    RecordDate DATE,
    PayableDate DATE,
    DeclaredDate DATE,
    Amount DOUBLE NOT NULL,
    DividendType VARCHAR DEFAULT 'Regular',
    Frequency VARCHAR,
    Currency VARCHAR DEFAULT 'USD',
    TaxRate DOUBLE,
    SpecialFlag BOOLEAN DEFAULT FALSE,
    AsOfDate DATE DEFAULT CURRENT_DATE,
    FOREIGN KEY (SecurityId) REFERENCES SecurityMaster(Id)
);

CREATE INDEX IF NOT EXISTS idx_dividend_security ON Dividend(SecurityId, ExDate);
CREATE INDEX IF NOT EXISTS idx_dividend_exdate ON Dividend(ExDate);
CREATE INDEX IF NOT EXISTS idx_dividend_type ON Dividend(DividendType);

CREATE TABLE IF NOT EXISTS CorpEventRef (
    EventTypeId INTEGER PRIMARY KEY,
    EventType VARCHAR NOT NULL UNIQUE,
    EventCategory VARCHAR,
    Description VARCHAR
);

CREATE TABLE IF NOT EXISTS CorpEvent (
    EventId INTEGER PRIMARY KEY,
    SecurityId INTEGER NOT NULL,
    EventTypeId INTEGER NOT NULL,
    AnnouncementDate DATE,
    EffectiveDate DATE NOT NULL,
    ExpirationDate DATE,
    Description VARCHAR,
    Data VARCHAR,  -- JSON storage for event-specific data
    Status VARCHAR DEFAULT 'Active',
    AsOfDate DATE DEFAULT CURRENT_DATE,
    FOREIGN KEY (SecurityId) REFERENCES SecurityMaster(Id),
    FOREIGN KEY (EventTypeId) REFERENCES CorpEventRef(EventTypeId)
);

CREATE INDEX IF NOT EXISTS idx_corpevent_security ON CorpEvent(SecurityId, EffectiveDate);
CREATE INDEX IF NOT EXISTS idx_corpevent_type ON CorpEvent(EventTypeId);
CREATE INDEX IF NOT EXISTS idx_corpevent_date ON CorpEvent(EffectiveDate);

CREATE TABLE IF NOT EXISTS AdjFactor (
    AdjFactorId INTEGER PRIMARY KEY,
    SecurityId INTEGER NOT NULL,
    EffectiveDate DATE NOT NULL,
    Factor DOUBLE NOT NULL,
    AdjType VARCHAR NOT NULL,  -- Split, Merger, SpinOff, etc.
    CumulativeFactor DOUBLE,
    Description VARCHAR,
    AsOfDate DATE DEFAULT CURRENT_DATE,
    FOREIGN KEY (SecurityId) REFERENCES SecurityMaster(Id)
);

CREATE INDEX IF NOT EXISTS idx_adjfactor_security ON AdjFactor(SecurityId, EffectiveDate);
CREATE INDEX IF NOT EXISTS idx_adjfactor_type ON AdjFactor(AdjType);

-- =============================================================================
-- OVERRIDE TABLE (for user-specified value overrides)
-- =============================================================================

CREATE TABLE IF NOT EXISTS FieldOverride (
    SecurityId INTEGER NOT NULL,
    FieldId INTEGER NOT NULL,
    ValueDate DATE NOT NULL,
    ValChr VARCHAR,
    ValDbl DOUBLE,
    ValInt INTEGER,
    ValDate DATE,
    Reason VARCHAR,
    CreatedBy VARCHAR,
    CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (SecurityId, FieldId, ValueDate),
    FOREIGN KEY (SecurityId) REFERENCES SecurityMaster(Id),
    FOREIGN KEY (FieldId) REFERENCES FieldDef(FieldId)
);
