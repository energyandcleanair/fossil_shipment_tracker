import requests
import urllib
import geopandas as gpd
import io
import numpy as np
import pandas as pd
import base
import json
import sqlalchemy as sa
from base.models import Position, ShipmentArrivalBerth, Price
from base.db import session
from base import PRICING_DEFAULT


PRICING_PRICECAP = "usd40"


def test_voyage_pricing(app):
    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        date_from = "2022-10-01"
        commodities = [base.CRUDE_OIL]

        # Default and cap pricing should be similar before 2022-07-01
        params = {
            "format": "json",
            "departure_date_from": date_from,
            "commodity": ",".join(commodities),
            "pricing_scenario": PRICING_DEFAULT,
            "commodity_origin_iso2": "RU",
            "currency": "EUR",
        }

        response = test_client.get("/v0/voyage?" + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = pd.DataFrame(response.json["data"])
        assert len(data) > 0

        # Check that the match is done properlyz
        query_price = Price.query.filter(
            Price.date >= date_from,
            Price.commodity.in_(commodities),
            Price.scenario == PRICING_DEFAULT,
        )

        price_given = pd.read_sql(query_price.statement, session.bind)
        price_given = price_given.explode("departure_port_ids")
        price_given = price_given.explode("destination_iso2s")
        price_given = price_given.explode("ship_owner_iso2s")
        price_given = price_given.explode("ship_insurer_iso2s")
        price_given["date"] = pd.to_datetime(price_given.date).dt.date
        price_given = price_given.rename(
            columns={
                "departure_port_ids": "departure_port_id",
                "destination_iso2s": "destination_iso2",
                "ship_owner_iso2s": "ship_owner_iso2",
                "ship_insurer_iso2s": "ship_insurer_iso2",
            }
        )
        price_given["departure_port_id"] = price_given.departure_port_id.astype("Int64")
        price_given["given_index"] = price_given.index

        # Checking there is no duplicate
        assert len(data.drop_duplicates(subset=["id", "arrival_ship_imo"])) == len(data)
        data["eur_per_tonne"] = data["value_eur_unweighted"] / data["ship_dwt"]
        data["date"] = pd.to_datetime(data.departure_date_utc).dt.date
        data["data_index"] = data.index

        # Checking different matching
        max_match = [
            "departure_port_id",
            "destination_iso2",
            "ship_owner_iso2",
            "ship_insurer_iso2",
        ]
        matches = [
            [
                "departure_port_id",
                "destination_iso2",
                "ship_owner_iso2",
                "ship_insurer_iso2",
            ],
            ["departure_port_id", "destination_iso2", "ship_owner_iso2"],
            ["departure_port_id", "destination_iso2", "ship_insurer_iso2"],
            ["departure_port_id", "ship_owner_iso2", "ship_insurer_iso2"],
            ["destination_iso2", "ship_owner_iso2", "ship_insurer_iso2"],
            ["departure_port_id", "destination_iso2"],
            ["departure_port_id", "ship_insurer_iso2"],
            ["departure_port_id", "ship_owner_iso2"],
            ["destination_iso2", "ship_owner_iso2"],
            ["destination_iso2", "ship_insurer_iso2"],
            ["ship_owner_iso2", "ship_insurer_iso2"],
            ["departure_port_id"],
            ["destination_iso2"],
            ["ship_insurer_iso2"],
            ["ship_owner_iso2"],
            [],
        ]

        idx_already_compared = []

        for match in matches:
            price_obtained = data.copy()[
                [
                    "data_index",
                    "id",
                    "departure_port_id",
                    "destination_iso2",
                    "ship_owner_iso2",
                    "ship_insurer_iso2",
                    "commodity",
                    "date",
                    "eur_per_tonne",
                ]
            ]
            price_obtained = price_obtained[~price_obtained.data_index.isin(idx_already_compared)]
            match += ["commodity", "date"]
            price_obtained["departure_port_id"] = price_obtained.departure_port_id.astype("Int64")

            price_comparison = price_obtained.merge(
                price_given, on=match, suffixes=["_obtained", "_given"], how="inner"
            ).drop_duplicates()

            unmatching_columns = [x for x in max_match if x not in match]
            for col in unmatching_columns:
                price_comparison = price_comparison[
                    price_comparison[col + "_given"].isnull()
                    | price_comparison[col + "_given"].isna()
                ]

            idx_already_compared += list(price_comparison.data_index)

            assert all(
                np.isclose(
                    price_comparison.eur_per_tonne_given,
                    price_comparison.eur_per_tonne_obtained,
                )
            )

            assert len(set(idx_already_compared)) == len(idx_already_compared)

        # Check that all have been compared
        assert len(set(idx_already_compared)) == len(data)


def test_price_cap(app):
    with app.test_client() as test_client:
        date_from = "2022-12-01"
        commodities = [base.CRUDE_OIL]
        bbl_per_tonne = 1 / 0.138

        # Default and cap pricing should be similar before 2022-07-01
        params = {
            "format": "json",
            "date_from": date_from,
            "commodity": ",".join(commodities),
            "pricing_scenario": base.PRICING_DEFAULT,
            "commodity_origin_iso2": "RU",
            "currency": "EUR,USD",
        }

        response = test_client.get("/v0/voyage?" + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = pd.DataFrame(response.json["data"])
        assert len(data) > 0
        data["usd_per_tonne"] = data.value_usd / data.value_tonne
        data["eur_per_usd"] = data.value_eur / data.value_usd

        # Check that no crude oil is about cap when conditions are matched
        data = data[data.departure_date_utc >= "2022-12-01"]
        data = data[data.departure_date_utc <= "2022-12-31"]
        # There is some discrepancy in exchange rate, hence the 1.1
        data[data.ship_owner_region == "EU"].usd_per_tonne.max() / bbl_per_tonne < 60 * 1.1
        data[data.ship_manager_region == "EU"].usd_per_tonne.max() / bbl_per_tonne < 60 * 1.1
        data[data.ship_insurer_region == "EU"].usd_per_tonne.max() / bbl_per_tonne < 60 * 1.1

        data = data[data.arrival_date_utc >= "2022-12-01"]
        # Check that those not covered aren't affected
        data[
            (data.ship_owner_region != "EU")
            & (data.ship_manager_region != "EU")
            & (data.ship_insurer_region != "EU")
            & (data.commodity_destination_region != "EU")
        ].usd_per_tonne.max() / bbl_per_tonne > 60 * 1.1

        data["covered"] = (
            (data.ship_owner_region == "EU")
            | (data.ship_manager_region == "EU")
            | (data.ship_manager_region == "EU")
            | (data.destination_region == "EU")
        )
        data.groupby(data.covered)["value_tonne"].sum()
        # Default and cap pricing should be similar before put in place
        params = {
            "format": "json",
            "date_from": date_from,
            "commodity": ",".join(commodities),
            "pricing_scenario": base.PRICING_DEFAULT,
            "commodity_origin_iso2": "RU",
            "currency": "EUR",
        }

        response = test_client.get("/v0/voyage?" + urllib.parse.urlencode(params))
        assert response.status_code == 200

        data = pd.DataFrame(response.json["data"])
        assert len(data) > 0

        # Check that the match is done properly
        query_price = Price.query.filter(
            Price.date >= "2022-11-01",
            Price.commodity.in_(commodities),
            Price.scenario == PRICING_DEFAULT,
        )

        price_given = pd.read_sql(query_price.statement, session.bind)
        price_given = price_given.explode("departure_port_ids")
        price_given = price_given.explode("destination_iso2s")
        price_given = price_given.explode("ship_owner_iso2s")
        price_given = price_given.explode("ship_insurer_iso2s")
        price_given["date"] = pd.to_datetime(price_given.date).dt.date
        price_given = price_given.rename(
            columns={
                "departure_port_id": "departure_port_ids",
                "destination_iso2": "destination_iso2",
                "ship_owner_iso2s": "ship_owner_iso2",
                "ship_insurer_iso2s": "ship_insurer_iso2",
            }
        )

        price_obtained = data
        price_obtained["date"] = pd.to_datetime(price_obtained.departure_date_utc).dt.date
        price_obtained["eur_per_tonne"] = price_obtained.value_eur / price_obtained.value_tonne
        price_obtained = price_obtained[
            [
                "ship_owner_iso2",
                "commodity",
                "date",
                "eur_per_tonne",
                "departure_port_id",
                "destination_iso2",
            ]
        ]

        price_comparison = price_obtained.merge(
            price_given,
            on=["ship_owner_iso2", "commodity", "date"],
            suffixes=["_obtained", "_given"],
            how="inner",
        )
        assert len(price_comparison) > 0
        assert all(
            np.isclose(
                price_comparison.eur_per_tonne_given,
                price_comparison.eur_per_tonne_obtained,
            )
        )

        params["pricing_scenario"] = ",".join([PRICING_DEFAULT, PRICING_PRICECAP])
        response = test_client.get("/v0/voyage?" + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0
        both_df = pd.DataFrame(data)
        assert default_sum == both_df[both_df.pricing_scenario == PRICING_DEFAULT].value_eur.sum()
        assert capped_sum == both_df[both_df.pricing_scenario == PRICING_PRICECAP].value_eur.sum()


def test_coal_pricing(app):
    # Trying to figure out why numbers are different

    with app.test_client() as test_client:
        # Default and cap pricing should be similar before 2022-07-01
        params = {
            "format": "json",
            "date_from": "2022-03-01",
            "date_to": "2022-03-05",
            # "aggregate_by": ','.join(["commodity_origin_iso2", "commodity_destination_iso2", "commodity", "date"]),
            "commodity": base.COAL,
            "commodity_origin_iso2": "RU",
            "currency": "EUR",
        }

        params["pricing_scenario"] = ",".join([PRICING_DEFAULT, PRICING_PRICECAP])
        response = test_client.get("/v0/voyage?" + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0
        both_df = pd.DataFrame(data)
        default_sum = both_df[both_df.pricing_scenario == PRICING_DEFAULT].value_eur.sum()
        capped_sum = both_df[both_df.pricing_scenario == PRICING_PRICECAP].value_eur.sum()
        assert default_sum == capped_sum
