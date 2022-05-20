# Some parts of the counter scripts
# are still written in R, waiting to be migrated here
# We use our Python/R capacity in the air pollution containers. Not ideal...

import requests


def update():
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
                "russiacounter::update_counter2()"
            ]
        },
        "environment_variables": [
            "FOSSIL_DB_DEVELOPMENT",
            "FOSSIL_DB_PRODUCTION",
            "GITHUB_PAT"
        ]
    }

    url = "https://engine.crea-aq-data.appspot.com"

    res = requests.post(url=url,
                  data=payload)
