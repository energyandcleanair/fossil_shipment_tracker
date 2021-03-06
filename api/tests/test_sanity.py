import requests
import urllib
import pandas as pd
import json
import pandas as pd
import base


def test_voyage(app):

    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        params = {"format": "json"}
        response = test_client.get('/v0/voyage?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0
        assert all([x['status'] != 'completed' or x['arrival_date_utc'] > x['departure_date_utc'] for x in data])

        voyages = pd.DataFrame(data)
        assert voyages.duplicated(subset=['id']).any() == False
        voyages['former_arrival_date_utc'] = voyages.sort_values(['ship_imo', 'arrival_date_utc']).groupby("ship_imo")['arrival_date_utc'].shift(1)
        v = voyages.sort_values(['ship_imo', 'arrival_date_utc'])

        problematic = v.loc[(v.former_arrival_date_utc > v.departure_date_utc) & (v.status != base.UNDETECTED_ARRIVAL)]
        assert len(problematic) == 0


def test_no_coal_in_tanker(app):
    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        params = {"format": "json", "commodity": "coal"}
        response = test_client.get('/v0/voyage?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0


        voyages = pd.DataFrame(data)
        assert voyages.duplicated(subset=['id']).any() == False
        assert not any(['tanker' in y for y in voyages.ship_type.str.lower().unique()])
