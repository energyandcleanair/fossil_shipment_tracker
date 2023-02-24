import requests
import urllib
import geopandas as gpd
import io
import pandas as pd
import base
import json
import numpy as np
import datetime as dt

from base.models import Position, ShipmentArrivalBerth
from base.db import session
from base.utils import to_datetime
from base import PRICING_DEFAULT, PRICING_PRICECAP


def test_counter_last(app):
    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        response = test_client.get('/v0/counter')
        assert response.status_code == 200

        params = {"format": "json"}
        response = test_client.get('/v0/counter_last?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) >= 4
        data_df = pd.DataFrame(data)
        expected_columns = set(['destination_region', 'date', 'commodity',
                                'commodity_group', 'eur_per_sec', 'total_eur'])
        assert set(data_df.columns) >= expected_columns

        params = {"aggregate_by": "commodity_group", "format": "json"}
        response = test_client.get('/v0/counter_last?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) == 4  # 3 main commodities plus total
        data_df = pd.DataFrame(data)
        expected_columns = set(
            ['date', 'commodity_group', 'eur_per_sec', 'total_eur'])
        assert set(data_df.columns) >= expected_columns

        params = {"aggregate_by": "destination_region,commodity_group", "format": "json"}
        response = test_client.get('/v0/counter_last?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) >= 4
        data_df = pd.DataFrame(data)
        expected_columns = set(
            ['destination_region', 'date', 'commodity_group', 'eur_per_sec', 'total_eur'])
        assert set(data_df.columns) >= expected_columns

        params = {"aggregate_by": "destination_country,commodity_group", "format": "json"}
        response = test_client.get('/v0/counter_last?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        data_df = pd.DataFrame(data)
        expected_columns = set(
            ['destination_iso2', 'destination_country', 'commodity_group', 'eur_per_sec', 'total_eur'])
        assert set(data_df.columns) >= expected_columns


def test_counter(app):
    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        response = test_client.get('/v0/counter')
        assert response.status_code == 200

        params = {"format": "json"}
        response = test_client.get('/v0/counter_last?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) >= 4
        data_df = pd.DataFrame(data)

        expected_columns = set(['destination_region',
                                'date', 'commodity_group', 'eur_per_sec', 'total_eur'])
        assert set(data_df.columns) >= expected_columns

        params = {"format": "json"}
        response = test_client.get('/v0/counter?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0
        data_df = pd.DataFrame(data)

        expected_columns = set(['commodity', 'commodity_group', 'commodity_group_name', 'date',
                                'destination_iso2', 'destination_country', 'destination_region',
                                'pricing_scenario_name', 'pricing_scenario', 'value_tonne', 'value_eur', 'value_usd'])
        assert set(data_df.columns) == expected_columns

        params = {"format": "json", "aggregate_by": "destination_country,month", "date_from": "2022-02-24"}
        response = test_client.get('/v0/counter?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0
        data_df = pd.DataFrame(data)

        expected_columns = set(['month',
                                'destination_iso2', 'destination_country', 'destination_region',
                                'value_tonne', 'value_eur', 'value_usd'])
        assert len([c for c in expected_columns if c in data_df.columns]) == len(expected_columns)


def test_counter_use_eu(app):
    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        params = {"format": "json", "use_eu": True}
        response = test_client.get('/v0/counter?' + urllib.parse.urlencode(params))
        data = response.json["data"]
        data_df = pd.DataFrame(data)
        assert 'EU' in data_df.destination_region.unique()
        assert 'EU28' not in data_df.destination_region.unique()

        response = test_client.get('/v0/counter_last?' + urllib.parse.urlencode(params))
        data = response.json["data"]
        data_df = pd.DataFrame(data)
        assert 'EU' in data_df.destination_region.unique()
        assert 'EU28' not in data_df.destination_region.unique()

        params = {"format": "json", "use_eu": False}
        response = test_client.get('/v0/counter?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        data_df = pd.DataFrame(data)

        assert 'EU' not in data_df.destination_region.unique()
        assert 'EU28' in data_df.destination_region.unique()

        response = test_client.get('/v0/counter_last?' + urllib.parse.urlencode(params))
        data = response.json["data"]
        data_df = pd.DataFrame(data)
        assert 'EU' not in data_df.destination_region.unique()
        assert 'EU28' in data_df.destination_region.unique()


def test_counter_cumulate(app):
    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        params = {"format": "json", "cumulate": True}
        response = test_client.get('/v0/counter?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0
        data_df = pd.DataFrame(data)


def test_counter_rolling(app):
    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        response = test_client.get('/v0/counter')
        assert response.status_code == 200

        params = {"format": "json", "rolling_days": 7}
        response = test_client.get('/v0/counter?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0
        data_df = pd.DataFrame(data)

        params = {"format": "json", "rolling_days": 7, "aggregate_by": "date,destination_region,commodity_group"}
        response = test_client.get('/v0/counter?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0
        data_df = pd.DataFrame(data)


def test_counter_aggregation(app):
    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:

        aggregate_bys = [
            [],
            ['destination_country', 'commodity'],
            ['destination_region', 'commodity_group'],
            ['destination_region', 'commodity'],
            ['destination_region'],
        ]

        for aggregate_by in aggregate_bys:
            params = {"format": "json", "aggregate_by": ','.join(aggregate_by)}
            response = test_client.get('/v0/counter?' + urllib.parse.urlencode(params))
            assert response.status_code == 200
            data = response.json["data"]
            assert len(data) > 0
            data_df = pd.DataFrame(data)

            expected_columns = set(aggregate_by + ['value_tonne', 'value_eur', 'value_usd']) if aggregate_by \
                else set(['commodity', 'commodity_group', 'commodity_group_name', 'destination_region',
                          'destination_iso2', 'destination_country',
                          'date', 'value_tonne', 'value_eur', 'value_usd'])

            if "commodity" in aggregate_by:
                expected_columns.update(["commodity_group", 'commodity_group_name'])

            if "commodity_group" in aggregate_by:
                expected_columns.update(['commodity_group_name'])

            if "destination_iso2" in aggregate_by or 'destination_country' in aggregate_by:
                expected_columns.update(["destination_country", 'destination_iso2', 'destination_region'])

            assert set(data_df.columns) == expected_columns

        # Test that sum countries is conserved
        params = {"format": "json"}
        response = test_client.get('/v0/counter_last?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data1 = response.json["data"]
        assert len(data1) >= 4
        data1_df = pd.DataFrame(data1)
        sum1 = data1_df.total_eur.sum()

        params = {"format": "json", "aggregate_by": "destination_country,commodity"}
        response = test_client.get('/v0/counter_last?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data2 = response.json["data"]
        data2_df = pd.DataFrame(data2)
        sum2 = data2_df.total_eur.sum()

        assert round(sum1, 2) == round(sum2, 2)
        assert sum1 > 1e9


def test_counter_matches_shipments(app):
    # We take a country without overland, or a commodity that is only traded through shipments
    with app.test_client() as test_client:
        params = {"format": "json",
                  "destination_iso2": "JP",
                  "commodity": "lng",
                  "aggregate_by": "commodity,destination_country,date"}
        response = test_client.get('/v0/counter?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0
        counter_df = pd.DataFrame(data)

        params = {"format": "json",
                  "destination_iso2": "JP",
                  "commodity_origin_iso2": 'RU',
                  "commodity": "lng", "aggregate_by": "commodity,destination_country,arrival_date"}
        response = test_client.get('/v0/voyage?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0
        shipments_df = pd.DataFrame(data)

        date_from = to_datetime('2022-03-01')
        date_to = to_datetime('2022-06-01')
        counter_sum = counter_df[(pd.to_datetime(counter_df.date) >= date_from) \
                                 & (pd.to_datetime(counter_df.date) <= date_to)] \
            .groupby(['commodity', 'destination_country'])['value_eur'].sum()

        shipments_sum = shipments_df[(pd.to_datetime(shipments_df.arrival_date) >= date_from) \
                                     & (pd.to_datetime(shipments_df.arrival_date) <= date_to)] \
            .groupby(['commodity', 'destination_country'])['value_eur'].sum()

        comparison = pd.merge(counter_sum.reset_index(), shipments_sum.reset_index(),
                              on=['commodity', 'destination_country'])

        assert all(comparison.value_eur_x == comparison.value_eur_y)


# TODO agree on sorting specification
def test_counter_sorting(app):
    # We take a country without overland, or a commodity that is only traded through shipments
    with app.test_client() as test_client:
        params = {"format": "json",
                  "destination_iso2": "JP",
                  "aggregate_by": "commodity,destination_country,date",
                  "sort_by": "commodity,desc(date)"}
        response = test_client.get('/v0/counter?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0
        counter_df = pd.DataFrame(data)

        list(counter_df.commodity) == list(counter_df.commodity.sort_values(ascending=True))

        for c in counter_df.commodity.unique():
            assert list(counter_df[counter_df.commodity == c].date) == list(
                counter_df[counter_df.commodity == c].date.sort_values(ascending=False))


def test_counter_against_voyage(app):
    with app.test_client() as test_client:
        response = test_client.get('/v0/counter')
        assert response.status_code == 200
        data = response.json["data"]
        counter_df = pd.DataFrame(data).sort_values(['date'], ascending=False)

        response = test_client.get('/v0/overland')
        assert response.status_code == 200
        data = response.json["data"]
        pipeline_df = pd.DataFrame(data).sort_values(['date'], ascending=False)

        response = test_client.get('/v0/voyage')
        assert response.status_code == 200
        data = response.json["data"]
        voyage_df = pd.DataFrame(data)

        counter2 = pd.concat([
            voyage_df.loc[(voyage_df.arrival_date_utc >= '2022-02-24') & (voyage_df.departure_iso2 == 'RU')][
                ['destination_region', 'commodity_group', 'value_eur']],
            pipeline_df.loc[(pipeline_df.date >= '2022-02-24') &
                            (pipeline_df.departure_iso2.isin(['TR', 'RU', 'BY']))][
                ['destination_region', 'commodity_group', 'value_eur']]]) \
            .groupby(['destination_region', 'commodity_group']) \
            .agg(value_eur=('value_eur', lambda x: np.nansum(x) / 1e9))

        counter1 = counter_df.groupby(['destination_region', 'commodity_group']) \
            .agg(value_eur=('value_eur', lambda x: np.nansum(x) / 1e9))

        # make sure total values are within a tolerance
        assert np.isclose(counter1['value_eur'].sum(), counter2['value_eur'].sum(), rtol=0.05)


def test_pricing_gt0(app):
    with app.test_client() as test_client:
        response = test_client.get('/v0/counter')
        assert response.status_code == 200
        data = response.json["data"]
        counter_df = pd.DataFrame(data).sort_values(['date'], ascending=False)

        response = test_client.get('/v0/overland')
        assert response.status_code == 200
        data = response.json["data"]
        pipeline_df = pd.DataFrame(data).sort_values(['date'], ascending=False)

        pipeline_notgas = pipeline_df.loc[pipeline_df.commodity != 'natural_gas']

        assert pd.to_datetime(pipeline_df.date).max().date() > dt.date.today() - dt.timedelta(days=3)
        assert all(pipeline_df.value_eur > 0)

        assert pd.to_datetime(pipeline_notgas.date).max().date() > dt.date.today() - dt.timedelta(days=3)
        assert all(pipeline_notgas.value_eur > 0)


def test_pricing_scenario(app):
    with app.test_client() as test_client:
        response = test_client.get('/v0/counter')
        assert response.status_code == 200
        data = response.json["data"]
        counter_df = pd.DataFrame(data)
        assert list(counter_df.pricing_scenario.unique()) == [PRICING_DEFAULT]
        default_sum = counter_df.value_eur.sum()

        params = {'pricing_scenario': PRICING_PRICECAP}
        response = test_client.get('/v0/counter' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        counter_df = pd.DataFrame(data)
        assert list(counter_df.pricing_scenario.unique()) == [PRICING_DEFAULT]
        default_sum = counter_df.value_eur.sum()
