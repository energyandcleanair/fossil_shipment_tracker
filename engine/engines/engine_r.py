# Some parts of the counter scripts
# are still written in R, waiting to be migrated here
# We use our Python/R capacity in the air pollution containers. Not ideal...

import requests
from base.logger import logger_slack, logger, slacker, notify_engineers

from google.cloud.run_v2 import JobsClient, RunJobRequest


def update(rebuild_prices=False):
    logger_slack.info("=== Pricing and misc flows update ===")

    try:
        client = JobsClient()

        project = "fossil-shipment-tracker"
        location = "europe-west1"
        job = "engine-r"

        def run_job():
            request = RunJobRequest(name=f"projects/{project}/locations/{location}/jobs/{job}")
            operation = client.run_job(request=request)
            # Wait for the operation to complete
            result = operation.result()
            logger.info("Job result: ", result)
            return result.succeeded_count > 0

        itry = 0
        maxtries = 2
        success = False

        while not success and itry < maxtries:
            success = run_job()

        if not success:
            logger_slack.error("R script failed")
            notify_engineers("Please check error")
        else:
            logger_slack.info("R script succeeded")
    except Exception:
        logger_slack.error("R script failed", stack_info=True, exc_info=True)
        notify_engineers("Please check error")
