from enum import Enum
import traceback
from api.app import app
from base.logger import logger_slack, logger, notify_engineers

from api.tests import test_counter

from .checks import *


class IntegrityCheckResult:
    def __init__(self, step, error=None, tb=None):
        self.step = step
        self.error = error
        self.tb = tb

    @property
    def success(self):
        return self.error == None

    @property
    def name(self):
        return self.step.name

    def format_error(self):
        return f"Integrity check {self.name} failed with error: {self.tb}"


class IntegrityCheckDefinition:
    def __init__(self, name, test):
        self.name = name
        self.test = test

    def run_test(self):
        logger.info(f"Checking integrity: {self.name}")
        try:
            self.test()
            return IntegrityCheckResult(self)
        except AssertionError as e:
            logger.info(f"Checking integrity {self.name} - failure")
            tb = traceback.format_exc()
            return IntegrityCheckResult(self, error=e, tb=tb)


KPLER_CHECKER_DATE_FROM = "2021-01-01"


class IntegrityStep(Enum):
    SHIPMENTS = IntegrityCheckDefinition("shipments", test_shipment_table)
    SHIPMENT_PORTCALL = IntegrityCheckDefinition(
        "shipment portcall", test_shipment_portcall_integrity
    )
    PORTCALL_RELATIONSHIP = IntegrityCheckDefinition(
        "portcall relationship", test_portcall_relationship
    )
    BERTHS = IntegrityCheckDefinition("berths", test_berths)
    COUNTER = IntegrityCheckDefinition(
        "counter", lambda: test_counter.test_counter_against_voyage(app)
    )
    PRICING = IntegrityCheckDefinition(
        "pricing positive", lambda: test_counter.test_pricing_gt0(app)
    )
    INSURER_UNKNOWNS = IntegrityCheckDefinition(
        "insurer no unexpected unknowns", test_insurers_no_unexpected_unknown
    )
    INSURER_FIRST_DATE_NOT_NULL = IntegrityCheckDefinition(
        "insurer first date not null", test_insurers_no_null_date_from
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


def check(steps=[step for step in IntegrityStep]):
    logger_slack.info("Checking integrity")

    results = [step.run_test() for step in steps]
    failed_results = [result for result in results if not result.success]

    if len(failed_results) > 0:
        failures = "\n------------\n".join([result.format_error() for result in failed_results])
        logger_slack.error(f"Integrity checks failed: {failures}")
        notify_engineers("Please check error")
    else:
        logger_slack.info(f"All integrity checks passed")
