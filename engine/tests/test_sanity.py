import pandas as pd
from engine.entsog import *
from base.models import Shipment, Departure



# def imos_are_matching():
#     imos = session.query(Arrival.)


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

        assert pd.to_datetime(pipeline_df.date).max() > dt.date.today() - dt.timedelta(days=3)
        assert all(pipeline_df.value_eur > 0)

        assert pd.to_datetime(pipeline_notgas.date).max() > dt.date.today() - dt.timedelta(days=3)
        assert all(pipeline_notgas.value_eur > 0)


def test_pipeline_gas():
    return


def test_manual_shipments():
    Shipment.query.join(Departure, Departure.id == Shipment.departure_id)