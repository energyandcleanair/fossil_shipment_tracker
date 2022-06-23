import requests
import urllib
import pandas as pd
import json
import pandas as pd
import base


def test_currency(app):

    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        endpoints = ['counter', 'voyage', 'overland', 'entsogflow']
        aggregate_bys = ['date', 'destination_region', 'commodity,destination_region']

        for endpoint in endpoints:
            for aggregate_by in aggregate_bys:
                params = {"format": "json", "date_from": '2022-06-01',
                          'aggregate_by': aggregate_by.split(','),
                          'currency': 'EUR'}
                response = test_client.get('/v0/' + endpoint + '?' + urllib.parse.urlencode(params))
                data1 = pd.DataFrame(response.json["data"])
                value_cols = [x for x in data1.columns.to_list() if x.startswith('value_')]
                assert set(value_cols + ['value_m3']) == set(['value_m3', 'value_tonne', 'value_eur'])

                params['currency'] = 'EUR,USD,JPY,CNY'
                response = test_client.get('/v0/' + endpoint + '?' + urllib.parse.urlencode(params))
                data2 = pd.DataFrame(response.json["data"])
                value_cols = [x for x in data2.columns.to_list() if x.startswith('value_')]
                assert set(value_cols + ['value_m3']) == set(['value_m3', 'value_tonne', 'value_eur',
                                               'value_usd', 'value_jpy', 'value_cny'])

                assert data1.value_eur.sum() > 0
                assert data1.value_eur.sum() == data2.value_eur.sum()

                usd_per_eur = data2.value_usd.sum() / data1.value_eur.sum()
                assert usd_per_eur > 1
                assert usd_per_eur < 1.2

                cny_per_eur = data2.value_cny.sum() / data1.value_eur.sum()
                assert cny_per_eur > 6.5
                assert cny_per_eur < 9

