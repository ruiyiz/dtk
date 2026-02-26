from dataclasses import dataclass

# Maps ryzmisc exchange codes to exchange_calendars MIC codes
EXCHANGE_CALENDAR_MAP: dict[str, str] = {
    "US": "XNYS",
    "LN": "XLON",
    "AU": "XASX",
    "CN": "XTSE",
    "SS": "XSTO",
    "SM": "XMAD",
    "IM": "XMIL",
    "NO": "XOSL",
    "FP": "XPAR",
    "BB": "XBRU",
    "SW": "XSWX",
    "WD": "XWBO",  # weekends-only fallback
}


@dataclass(frozen=True)
class DataSetDef:
    table: str
    date_col: str
    fields: tuple[str, ...]


DATA_SETS: dict[str, DataSetDef] = {
    "dividend": DataSetDef(
        table="Dividend",
        date_col="ExDate",
        fields=(
            "ExDate",
            "RecordDate",
            "PayableDate",
            "DeclaredDate",
            "Amount",
            "DividendType",
            "Frequency",
            "Currency",
            "TaxRate",
            "SpecialFlag",
        ),
    ),
    "event": DataSetDef(
        table="CorpEvent",
        date_col="EffectiveDate",
        fields=(
            "EventTypeId",
            "AnnouncementDate",
            "EffectiveDate",
            "ExpirationDate",
            "Description",
            "Data",
            "Status",
        ),
    ),
    "adjfactor": DataSetDef(
        table="AdjFactor",
        date_col="EffectiveDate",
        fields=(
            "EffectiveDate",
            "Factor",
            "AdjType",
            "CumulativeFactor",
            "Description",
        ),
    ),
}

# Maps field mnemonics to wide-table column names
WIDE_COLUMN_MAP: dict[str, str] = {
    "PX_CLOSE": "PxClose",
    "PX_HIGH": "PxHigh",
    "PX_LOW": "PxLow",
    "PX_OPEN": "PxOpen",
    "PX_LAST": "PxLast",
    "VOLUME": "Volume",
    "NAV_CLOSE": "NavClose",
    "NAV_LAST": "NavLast",
    "TOTAL_RETURN": "TotalReturn",
    "DIVIDEND_AMOUNT": "DividendAmount",
    "ADJ_FACTOR": "AdjFactor",
}
