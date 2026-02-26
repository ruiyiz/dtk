from enum import StrEnum


class SecurityType(StrEnum):
    CEF = "CEF"
    ETF = "ETF"
    INDEX = "Index"
    FX = "FX"
    WARRANT = "Warrant"
    CASH = "Cash"
    COMMON_STOCK = "Common Stock"
    MF = "MF"
    PRIVATE_FUND = "Private Fund"
    MM = "MM"


class Periodicity(StrEnum):
    D = "D"
    W = "W"
    M = "M"
    Q = "Q"
    HY = "HY"
    Y = "Y"


class BDC(StrEnum):
    Following = "Following"
    ModifiedFollowing = "ModifiedFollowing"
    Preceding = "Preceding"
    ModifiedPreceding = "ModifiedPreceding"
    Unadjusted = "Unadjusted"


class Days(StrEnum):
    NonTradingDay = "N"
    CalendarDay = "C"
    TradingDay = "T"


class Endpoint(StrEnum):
    LastOf = "last_of"
    FirstOf = "first_of"


class ExchangeCode(StrEnum):
    US = "US"
    LN = "LN"
    AU = "AU"
    CN = "CN"
    SS = "SS"
    SM = "SM"
    IM = "IM"
    NO = "NO"
    FP = "FP"
    BB = "BB"
    SW = "SW"
    WD = "WD"
    NA = "NA"


class DateMode(StrEnum):
    AsOf = "as_of"
    AsSeen = "as_seen"


class FillMode(StrEnum):
    NA = "NA"
    Previous = "P"


class IdType(StrEnum):
    Id = "id"
    Ticker = "ticker"
    Blp = "blp"


class FxMode(StrEnum):
    Money = "money"
    Return = "return"
