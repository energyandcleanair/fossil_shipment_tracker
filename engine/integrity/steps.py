from enum import Enum

from integrity.check_kpler_trade_computed import test_sample_computed
from integrity.checks import *
from integrity.check_kpler_trade import test_kpler_trades, KplerCheckerProducts
from integrity.integrity_check_definition import IntegrityCheckDefinition


KPLER_CHECKER_DATE_FROM = "2021-01-01"


class IntegrityStep(Enum):
    PRICING = IntegrityCheckDefinition("pricing positive", test_counter_pricing_positive)
    INSURER_UNKNOWNS = IntegrityCheckDefinition(
        "insurer no unexpected unknowns", test_insurers_no_unexpected_unknown
    )
    INSURER_FIRST_DATE_NOT_NULL = IntegrityCheckDefinition(
        "insurer first date not null", test_insurers_no_null_date_from
    )

    OVERLAND_TRADE_HAS_VALUES = IntegrityCheckDefinition(
        "overland trade has values for each month", test_overland_trade_has_values
    )

    CHINA_RUSSIA_SPREADSHEET_UP_TO_DATE = IntegrityCheckDefinition(
        "China Russia spreadsheet up to date", check_china_russia_source
    )

    KPLER_TRADE_COMPUTED_SAMPLE_CORRECT = IntegrityCheckDefinition(
        "Kpler trade computed is correct", test_sample_computed
    )

    KPLER_TRADES_WITHOUT_PRICES = IntegrityCheckDefinition(
        "Kpler trades without prices", test_kpler_trades_without_prices
    )

    SHIP_INSPECTIONS_HAVE_DATES = IntegrityCheckDefinition(
        "Ship inspections have dates", check_ship_inspections_report_date_filled
    )

    KPLER_TRADE_CRUDE = IntegrityCheckDefinition(
        "Kpler trade crude",
        lambda: test_kpler_trades(
            date_from=KPLER_CHECKER_DATE_FROM,
            product=KplerCheckerProducts.CRUDE,
            origin_iso2="RU",
        ),
    )
    KPLER_TRADE_LNG = IntegrityCheckDefinition(
        "Kpler trade LNG",
        lambda: test_kpler_trades(
            date_from=KPLER_CHECKER_DATE_FROM,
            product=KplerCheckerProducts.LNG,
            origin_iso2="RU",
        ),
    )
    KPLER_TRADE_GASOIL_DIESEL = IntegrityCheckDefinition(
        "Kpler trade Gasoil/Diesel",
        lambda: test_kpler_trades(
            date_from=KPLER_CHECKER_DATE_FROM,
            product=KplerCheckerProducts.GASOIL_DIESEL,
            origin_iso2="RU",
        ),
    )
    KPLER_TRADE_METALLURGICAL_COAL = IntegrityCheckDefinition(
        "Kpler trade Coal",
        lambda: test_kpler_trades(
            date_from=KPLER_CHECKER_DATE_FROM,
            product=KplerCheckerProducts.METALLURGICAL_COAL,
            origin_iso2="RU",
        ),
    )
    KPLER_TRADE_THERMAL_COAL = IntegrityCheckDefinition(
        "Kpler trade Coal",
        lambda: test_kpler_trades(
            date_from=KPLER_CHECKER_DATE_FROM,
            product=KplerCheckerProducts.THERMAL_COAL,
            origin_iso2="RU",
        ),
    )

    def run_test(self):
        return self.value.run_test()

    @classmethod
    def get_kpler_checks(cls):
        return [step for step in IntegrityStep if step.name.startswith("KPLER_TRADE_")]
