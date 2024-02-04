import base

from argparse import ArgumentParser

from base.logger import logger_slack

from update_for_sanctioners_laundering import ALL_SANCTIONING_COUNTRIES, get_countries_to_update

from integrity.checker import (
    IntegrityCheckDefinition,
    test_kpler_trades,
    KplerCheckerProducts,
    check,
)


def check_integrity_sanctioners(date_from=None, date_to=None):
    countries_to_check = get_countries_to_update(date_from, date_to)

    # Don't do list comprehension otherwise the lambda will use the last value of country because
    # the closure is the same for all lambdas.
    crude_checks = list(
        map(
            lambda country: IntegrityCheckDefinition(
                "Crude for %s" % country,
                lambda: test_kpler_trades(
                    date_from=date_from,
                    date_to=date_to,
                    product=KplerCheckerProducts.CRUDE,
                    origin_iso2=country,
                ),
            ),
            countries_to_check,
        )
    )

    oil_checks = list(
        map(
            lambda country: IntegrityCheckDefinition(
                "Gasoil/Diesel for %s" % country,
                lambda: test_kpler_trades(
                    date_from=date_from,
                    date_to=date_to,
                    product=KplerCheckerProducts.GASOIL_DIESEL,
                    origin_iso2=country,
                ),
            ),
            countries_to_check,
        )
    )

    checks = crude_checks + oil_checks

    check(checks)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--date-from", type=str, default=None)
    parser.add_argument("--date-to", type=str, default=None)

    args = parser.parse_args()

    if args.date_from is None:
        print("Please specify --date-from")
        exit(1)

    if args.date_to is None:
        print("Please specify --date-to")
        exit(1)

    logger_slack.info(
        "=== Check integrity for report: using %s environment ===" % (base.db.environment,)
    )
    try:
        check_integrity_sanctioners(date_from=args.date_from, date_to=args.date_to)
        logger_slack.info("=== Check integrity for report complete ===")
    except BaseException as e:
        logger_slack.error("=== Check integrity for report failed", stack_info=True, exc_info=True)
        raise e
