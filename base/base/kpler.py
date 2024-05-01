from enum import Enum


class FlowsSplit(Enum):
    """"""

    Total = "total"  #:
    Grades = "grades"  #:
    Products = "products"  #:
    OriginCountries = "origin countries"  #:
    OriginSubcontinents = "origin subcontinents"  #:
    OriginContinents = "origin continents"  #:
    OriginTradingRegions = "origin trading regions"  #:
    OriginPorts = "origin ports"
    DestinationTradingRegions = "destination trading regions"  #:
    DestinationCountries = "destination countries"  #:
    DestinationSubcontinents = "destination subcontinents"  #:
    DestinationContinents = "destination continents"  #:
    OriginInstallations = "origin installations"  #:
    DestinationInstallations = "destination installations"  #:
    DestinationPorts = "destination ports"
    OriginPadds = "origin padds"  #:
    DestinationPadds = "destination padds"  #:
    VesselType = "vessel type"  #:
    TradeStatus = "trade status"  #:
    Sources = "sources"  #:
    Charterers = "charterers"  #:
    Routes = "routes"  #:
    Buyers = "buyer"  #:
    Sellers = "seller"  #:
    VesselTypeOil = "vessel type oil"  #:
    VesselTypeCpp = "vessel type cpp"  #:
    LongHaulVesselType = "long haul vessel type"  #:
    LongHaulVesselTypeOil = "long haul vessel type oil"  #:
    LongHaulVesselTypeCpp = "long haul vessel type cpp"  #:
    CrudeQuality = "crude quality"  #:


class FlowsPeriod(Enum):
    """"""

    Annually = "annually"  #:
    Monthly = "monthly"  #:
    Weekly = "weekly"  #:
    EiaWeekly = "eia-weekly"  #:
    Daily = "daily"  #:


class FlowsMeasurementUnit(Enum):
    """"""

    KBD = "kbd"  #:
    BBL = "bbl"  #:
    KB = "kb"  #:
    MMBBL = "mmbbl"  #:
    MT = "mt"  #:
    KT = "kt"  #:
    T = "t"  #:
    CM = "cm"  #:


class FlowsDirection(Enum):
    """"""

    Import = "import"  #:
    Export = "export"  #:
    NetImport = "netimport"  #:
    NetExport = "netexport"  #:
