import requests
import urllib
import geopandas as gpd
import io
import pandas as pd
import base
import json
from base.models import Position, FlowArrivalBerth
from base.db import session



def test_ship(app):

    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        params = {}
        response = test_client.get('/v0/ship?'+ urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0
        assert all([x['imo'] is not None for x in data])

        params = {'imo': data[0]["imo"]}
        response = test_client.get('/v0/ship?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data2 = response.json["data"]
        assert len(data2) == 1

        params = {'imo': ",".join([x["imo"] for x in data[0:3]])}
        response = test_client.get('/v0/ship?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data3 = response.json["data"]
        assert len(data3) == 3


def test_port(app):

    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        params = {}
        response = test_client.get('/v0/port?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0
        # assert all([x['unlocode'] is not None for x in data])

        params = {'unlocode': data[0]["unlocode"]}
        response = test_client.get('/v0/port?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data2 = response.json["data"]
        assert len(data2) == 1

        params = {'unlocode': ",".join([x["unlocode"] for x in data[0:3]])}
        response = test_client.get('/v0/port?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data3 = response.json["data"]
        assert len(data3) == 3


def test_voyage(app):

    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        params = {"format": "json"}
        response = test_client.get('/v0/voyage?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0
        assert base.ONGOING in set([x['status'] for x in data])
        assert base.COMPLETED in set([x['status'] for x in data])
        assert all([x['arrival_date_utc'] is None or x['arrival_date_utc'] > x['departure_date_utc'] for x in data])

        # Test commodity parameter
        params = {"format": "json", "commodity": "crude_oil"}
        response = test_client.get('/v0/voyage?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert all([x['commodity'] == "crude_oil" for x in data])

        # Test id parameter
        params = {"format": "json", "id": data[0]["id"]}
        response = test_client.get('/v0/voyage?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) == 1


        params = {"format": "geojson"}
        response = test_client.get('/v0/voyage?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0
        gdf = gpd.read_file(io.StringIO(json.dumps(data)))
        assert len(gdf) > 0
        assert "geometry" in gdf.columns

        # Test cutting trail works
        voyage_id, date_berthing = session.query(FlowArrivalBerth.flow_id, Position.date_utc) \
            .join(Position, FlowArrivalBerth.position_id == Position.id) \
            .first()

        params = {"format": "geojson", "id": voyage_id}
        response = test_client.get('/v0/voyage?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0
        gdf = gpd.read_file(io.StringIO(json.dumps(data)))
        assert len(gdf) == 1
        assert "geometry" in gdf.columns

        from http import HTTPStatus
        params = {"format": "geojson", "id": -9999}
        response = test_client.get('/v0/voyage?' + urllib.parse.urlencode(params))
        assert response.status_code == HTTPStatus.NO_CONTENT


def test_position(app):
    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        ship_imo = session.query(Position.ship_imo).first()[0]
        params = {"ship_imo": ship_imo}
        response = test_client.get('/v0/position?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0


        # Test data required for finding missing berth
        params = {"has_arrival_berth": "false", "speed_max": "0.1", "status": "completed"}
        response = test_client.get('/v0/position?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0

def test_berth(app):

    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        params = {"format": "json"}
        response = test_client.get('/v0/berth?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0

        # Test commodity parameter
        params = {"format": "geojson"}
        response = test_client.get('/v0/berth?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        gdf = gpd.read_file(io.StringIO(json.dumps(data)))
        assert len(gdf) > 0
        assert "geometry" in gdf.columns



def test_aggregated(app):

    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:

        aggregate_bys = [
            [],
            ['destination_country', 'departure_date'],
            ['departure_port', 'departure_date', 'commodity', 'status'],
            ['departure_port', 'arrival_date', 'commodity', 'status'],
            ['departure_country', 'departure_date', 'commodity', 'status'],
            ['destination_country', 'departure_date', 'commodity', 'status']
        ]

        for aggregate_by in aggregate_bys:

            params = {"format": "json", "aggregate_by": ','.join(aggregate_by)}

            response = test_client.get('/v0/voyage?' + urllib.parse.urlencode(params))
            assert response.status_code == 200
            data = response.json["data"]
            assert len(data) > 0
            data_df = pd.DataFrame(data)

            expected_columns = set(aggregate_by + ['commodity', 'unit', 'quantity', 'ship_dwt'])

            if "departure_port" in aggregate_by:
                expected_columns.update(["departure_port_name", "departure_unlocode", "departure_iso2", "departure_country"])
                expected_columns.discard("departure_port")

            if "departure_country" in aggregate_by:
                expected_columns.update(["departure_iso2"])

            if "destination_country" in aggregate_by:
                expected_columns.update(["destination_iso2"])

            if "arrival_port" in aggregate_by:
                expected_columns.update(["destination_port_name", "destination_unlocode", "destination_iso2", "destination_country"])
                expected_columns.discard("destination_port")

            assert set(data_df.columns) == expected_columns

            assert set(data_df.commodity.unique()) < set([base.OIL_PRODUCTS,
                                                           base.CRUDE_OIL,
                                                           base.COAL,
                                                           base.BULK,
                                                           base.BULK_NOT_COAL,
                                                           base.OIL_OR_CHEMICAL,
                                                           base.OIL_OR_ORE,
                                                           base.LPG,
                                                           base.LNG,
                                                           base.UNKNOWN_COMMODITY
                                                           ])
