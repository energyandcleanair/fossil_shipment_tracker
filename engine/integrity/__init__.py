from integrity.steps import IntegrityStep
from integrity.integrity_check_definition import IntegrityCheckDefinition

from base.logger import logger_slack, notify_engineers


def check(steps=[step for step in IntegrityStep]):
    logger_slack.info("Checking integrity")

    results = [step.run_test() for step in steps]
    failed_results = [result for result in results if not result.success]

    if len(failed_results) > 0:
        separator = "\n------------\n"
        failures = separator.join([result.format_error() for result in failed_results])
        logger_slack.error(f"Integrity checks failed:{separator}{failures}")
        notify_engineers("Please check error")
    else:
        logger_slack.info(f"All integrity checks passed")
