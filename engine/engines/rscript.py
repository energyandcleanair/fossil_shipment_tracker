# Some parts of the counter scripts
# are still written in R, waiting to be migrated here
# We use our Python/R capacity in the air pollution containers. Not ideal...

import requests
from ...base.logger import logger_slack, slacker, notify_engineers


def update():
    logger_slack.info("=== RScript update ===")
    payload = {
        "command": "run_script",
        "rscript": {
            "content": [
                "library(remotes)",
                "library(rcrea)",
                "remotes::install_github('energyandcleanair/fossil_shipment_tracker_r', upgrade=F, force=F)",
                # To ensure latest version is being used
                "if('russiacounter' %in% (.packages())){detach('package:russiacounter', unload=T)}",
                "library(russiacounter)",
                "russiacounter::update_counter()",
            ]
        },
        "environment_variables": [
            "FOSSIL_DB_DEVELOPMENT",
            "FOSSIL_DB_PRODUCTION",
            "GITHUB_PAT",
            "CREA_MONGODB_URL",
            "EIA_KEY",
        ],
    }

    itry = 0
    maxtries = 2
    success = False

    while not success and itry < maxtries:
        url = "http://engine.crea-aq-data.appspot.com"
        res = requests.post(url=url, json=payload)
        itry += 1
        success = res.status_code == 200

    if not success:
        logger_slack.error("R script failed")
        notify_engineers("Please check error")
    else:
        logger_slack.info("R script succeeded")
