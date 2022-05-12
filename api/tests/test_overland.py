import requests
import urllib
import geopandas as gpd
import io
import pandas as pd
import base
import json
from base.models import Position, ShipmentArrivalBerth
from base.db import session
import datetime as dt


def test_pipelineflow_pricing(app):
    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        params = {"format": "json"}
        response = test_client.get('/v0/overland?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0
        assert all([x["value_tonne"] == 0 or x["value_tonne"]/x["value_eur"] > 0 for x in data])
        assert len(set([x['id'] for x in data])) == len(data)

        params = {"format": "json", "date_from": "2021-01-01"}
        response = test_client.get('/v0/overland?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0
        assert all([x["value_tonne"] == 0 or x["value_tonne"] / x["value_eur"] > 0 for x in data])
        assert len(set([x['id'] for x in data])) == len(data)


# def test_pipelineflow_ukraine(app):
#     # We assume gas is transiting through Ukraine,
#     # So Ukraine must be considered as part of EU
#     # Create a test client using the Flask application configured for testing
#     with app.test_client() as test_client:
#         params = {"format": "json", "destination_iso2": "UA", "aggregate_by": "destination_region"}
#         response = test_client.get('/v0/overland?' + urllib.parse.urlencode(params))
#         assert response.status_code == 200
#         data = response.json["data"]
#         assert len(data) == 1
#         assert all([x["value_eur"] > 0 for x in data])
#         assert list(set([x["destination_region"] for x in data])) == ["EU28"]
#
#         params = {"format": "json", "destination_region": "EU28", "aggregate_by": "destination_region"}
#         response = test_client.get('/v0/overland?' + urllib.parse.urlencode(params))
#         assert response.status_code == 200
#         data_eu = response.json["data"]
#         assert len(data_eu) == 1
#         sum([x["value_eur"] for x in data_eu]) > sum([x["value_eur"] for x in data])
#         assert list(set([x["destination_region"] for x in data_eu])) == ["EU28"]


def test_pipelineflow_aggregation(app):

    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        aggregate_bys = [
            [],
            ['destination_region', 'commodity'],
            ['destination_region', 'commodity_group'],
            ['destination_region', 'commodity', 'date'],
        ]

        for aggregate_by in aggregate_bys:
            params = {"format": "json", "aggregate_by": ','.join(aggregate_by)}
            response = test_client.get('/v0/overland?' + urllib.parse.urlencode(params))
            assert response.status_code == 200
            data = response.json["data"]
            assert len(data) > 0
            data_df = pd.DataFrame(data)

            expected_columns = set(aggregate_by + ['value_tonne', 'value_eur', 'value_m3']) if aggregate_by \
                else set(['id', 'commodity', 'commodity_group',
                          'departure_iso2', 'departure_country', 'departure_region',
                          'destination_iso2', 'destination_country', 'destination_region',
                          'date', 'value_tonne', 'value_eur', 'value_m3'])

            if "commodity" in aggregate_by:
                expected_columns.update(["commodity_group"])

            assert set(data_df.columns) == expected_columns