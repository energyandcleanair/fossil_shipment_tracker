import requests
import urllib
import geopandas as gpd
import io
import pandas as pd
import base
import json
from base.models import Position, ShipmentArrivalBerth
from base.db import session



def test_voyage_pricing(app):

    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        params = {"format": "json", "commodity": ",".join([base.COAL, base.CRUDE_OIL,
                                                          base.LNG, base.OIL_PRODUCTS, base.OIL_OR_CHEMICAL])}

        response = test_client.get('/v0/voyage?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0
        assert all([x["value_eur"] > 0 for x in data])
        assert len(set([x['id'] for x in data])) == len(data)

        #check discount was applied ( we need two shipments on the same day for a same country)
        data_df = pd.DataFrame(data)
        data_df["eur_per_tonne"] = round(data_df.value_eur / data_df.value_tonne)
        data_df["date"] = pd.to_datetime(data_df.departure_date_utc).dt.date
        prices = data_df.loc[data_df.commodity==base.CRUDE_OIL][["destination_iso2", "commodity", "date", "eur_per_tonne"]]
        prices = prices.drop_duplicates()
        unique_prices = prices.groupby(["destination_iso2", "commodity", "date"]).eur_per_tonne.nunique().reset_index()
        #TODO add a test that is not relying on
        # having two arrivals on same day... assert max(unique_prices.eur_per_tonne) > 1


def test_voyage_aggregated(app):

    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:

        aggregate_bys = [
            [],
            ['destination_country', 'departure_date'],
            ['departure_port', 'departure_date', 'commodity', 'status'],
            ['departure_port', 'arrival_date', 'commodity_group', 'status'],
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

            expected_columns = set(aggregate_by + ['value_tonne', 'value_m3', 'ship_dwt', 'value_eur'])

            if "departure_port" in aggregate_by:
                expected_columns.update(["departure_port_name", "departure_unlocode", "departure_iso2", "departure_country", "departure_region"])
                expected_columns.discard("departure_port")

            if "departure_country" in aggregate_by:
                expected_columns.update(["departure_iso2", "departure_country", "departure_region"])

            if "destination_country" in aggregate_by:
                expected_columns.update(["destination_iso2", "destination_country", "destination_region"])

            if "arrival_port" in aggregate_by:
                expected_columns.update(["destination_port_name", "destination_unlocode", "destination_iso2", "destination_country", "destination_region"])
                expected_columns.discard("destination_port")

            if "commodity" in aggregate_by:
                expected_columns.update(["commodity_group"])

            if aggregate_by:
                assert set(data_df.columns) == expected_columns

            if "commodity" in aggregate_by:
                assert set(data_df.commodity.unique()) <= set([base.OIL_PRODUCTS,
                                                               base.CRUDE_OIL,
                                                               base.COAL,
                                                               # base.BULK,
                                                               base.BULK_NOT_COAL,
                                                               base.OIL_OR_CHEMICAL,
                                                               base.OIL_OR_ORE,
                                                               base.LPG,
                                                               base.LNG,
                                                               base.UNKNOWN_COMMODITY,
                                                               base.GENERAL_CARGO
                                                               ])




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
        assert all([x['arrival_date_utc'] is not None for x in data if x['status'] == base.COMPLETED])
        assert all([x['arrival_date_utc'] is None for x in data if x['status'] == base.ONGOING])
        assert len(set([x['id'] for x in data])) == len(data)


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

        # Test destination_iso2 parameter
        params = {"format": "json", "destination_iso2": "IT"}
        response = test_client.get('/v0/voyage?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0
        assert all([x['destination_iso2'] == "IT" for x in data])


        params = {"format": "geojson"}
        response = test_client.get('/v0/voyage?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0
        gdf = gpd.read_file(io.StringIO(json.dumps(data)))
        assert len(gdf) > 0
        assert "geometry" in gdf.columns

        # Test cutting trail works
        voyage_id, date_berthing = session.query(ShipmentArrivalBerth.shipment_id, Position.date_utc) \
            .join(Position, ShipmentArrivalBerth.position_id == Position.id) \
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


def test_voyage_rolling(app):

    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        #TODO find a relevant test
        params = {"format": "json", "rolling_days": 7, "aggregate_by": "departure_date,destination_country,commodity,status"}
        response = test_client.get('/v0/voyage?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0
        assert base.ONGOING in set([x['status'] for x in data])
        assert base.COMPLETED in set([x['status'] for x in data])
        assert all(["departure_date" in x.keys() for x in data])
