import requests
import urllib
import geopandas as gpd
import io
import pandas as pd
import base
import json
from base.models import Position, ShipmentArrivalBerth
from base.db import session
from base import PRICING_DEFAULT, PRICING_PRICECAP


def test_voyage_pricing(app):

    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:

        # Default and cap pricing should be similar before 2022-07-01
        params = {"format": "json",
                  "date_from": "2021-01-01",
                  "aggregate_by": ','.join(["commodity_origin_iso2", "commodity_destination_iso2", "commodity", "date"]),
                  "commodity": ",".join([base.COAL, base.CRUDE_OIL,
                                                          base.LNG, base.OIL_PRODUCTS, base.OIL_OR_CHEMICAL]),
                  "pricing_scenario": PRICING_DEFAULT,
                  "commodity_origin_iso2": 'RU',
                  "currency": 'EUR'
                  }

        response = test_client.get('/v0/voyage?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0
        default_df = pd.DataFrame(data)
        default_sum = default_df.value_eur.sum()

        params['pricing_scenario'] = PRICING_PRICECAP
        response = test_client.get('/v0/voyage?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0
        capped_df = pd.DataFrame(data)
        capped_sum = capped_df.value_eur.sum()

        assert abs((default_sum - capped_sum) / default_sum) < 0.1

        params['pricing_scenario'] = ','.join([PRICING_DEFAULT, PRICING_PRICECAP])
        response = test_client.get('/v0/voyage?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0
        both_df = pd.DataFrame(data)
        assert default_sum == both_df[both_df.pricing_scenario == PRICING_DEFAULT].value_eur.sum()
        assert capped_sum == both_df[both_df.pricing_scenario == PRICING_PRICECAP].value_eur.sum()
