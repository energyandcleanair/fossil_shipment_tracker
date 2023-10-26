import requests
import argparse
import csv
import time
from typing import Dict, Any
import os
from decouple import config
from tqdm import tqdm


def get_trades(from_: int, size: int = 1000) -> Dict[str, Any]:
    url = f"https://terminal.kpler.com/api/trades?from={from_}&size={size}&view=kpler&withForecasted=false&withFreightView=false&withProductEstimation=false&operationalFilter=shipToShip"
    headers = {
        "Accept": "text/csv",
        "Authorization": f'Bearer {config("KPLER_TOKEN_BRUTE")}',
        "DNT": "1",
        "Referer": "https://terminal.kpler.com/map/search/trades?fields=location&locations=z757",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36",
        "sec-ch-ua": '"Google Chrome";v="113", "Chromium";v="113", "Not-A.Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "x-web-application-version": "v21.482.11",
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.text


def write_to_csv(content: str, filename: str):
    with open(filename, "a") as f:
        f.write(content)


def main():
    parser = argparse.ArgumentParser(description="A CLI to get and save trades data.")
    parser.add_argument(
        "-n", "--number", type=int, default=1, help="Number of times the query should be run."
    )
    args = parser.parse_args()

    for i in tqdm(range(args.number)):
        data = get_trades(from_=i * 1000)
        write_to_csv(data, f"trades_{i}.csv")
        time.sleep(5)  # to prevent overwhelming the server


if __name__ == "__main__":
    main()
