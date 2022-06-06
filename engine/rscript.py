# Some parts of the counter scripts
# are still written in R, waiting to be migrated here
# We use our Python/R capacity in the air pollution containers. Not ideal...

import requests
from base.logger import logger_slack

def update():
    logger_slack.info("=== RScript update ===")
    payload = {
        "command": "run_script",
        "rscript": {
            "content": [
                "install.packages('tidyverse',repos='http://cran.us.r-project.org')",
                "library(tidyverse)",
                "library(remotes)",
                "remotes::install_github('energyandcleanair/entsog', upgrade=F)",
                "remotes::install_github('energyandcleanair/202203_russian_gas', upgrade=F, force=T)",
                "library(russiacounter)",
                "print(packageVersion('russiacounter'))",
                "print(russiacounter::update_counter2)",
                "library(tidyverse);library(lubridate);library(magrittr);library(countrycode)",
                "russiacounter::price.update_portprices(production=T)"
                # "russiacounter::update_counter2()"
            ]
        },
        "environment_variables": [
            "FOSSIL_DB_DEVELOPMENT",
            "FOSSIL_DB_PRODUCTION",
            "GITHUB_PAT",
            "CREA_MONGODB_URL"
        ]
    }

    url = "http://engine.crea-aq-data.appspot.com"
    # url = "http://localhost:8080"
    res = requests.post(url=url, json=payload)

    if res.status_code != 200:
        logger_slack.error("R script failed")
    else:
        logger_slack.info("R script succeeded")