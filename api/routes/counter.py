from . import routes_api

import json
import pandas as pd
import pytz
import datetime as dt
from flask import Response
from flask_restx import Resource, reqparse
import pymongo



def get_counter_prices():
    from base.db import mongo_client
    db = mongo_client['russian_fossil']
    prices = db.get_collection("counter_prices").find().sort("date", pymongo.DESCENDING)
    prices_df = pd.DataFrame(list(prices))
    prices_df = prices_df[["commodity", "date", "eur_per_tonne", "value", "unit"]]
    return prices_df


@routes_api.route('/v1/russia_counter', strict_slashes=False)
class RussiaCounterResource(Resource):

    parser = reqparse.RequestParser()

    @routes_api.expect(parser)
    def get(self):

        from base.db import mongo_client
        db = mongo_client['russian_fossil']
        last_item = db.get_collection("counter").find().sort("date", pymongo.DESCENDING).limit(1)[0]
        timezone = pytz.timezone("UTC")
        try:
            last_date = timezone.localize(dt.datetime.strptime(last_item['date'], "%Y-%m-%d %H:%M:%S"))
        except:
            last_date = timezone.localize(dt.datetime.strptime(last_item['date'], "%Y-%m-%d"))
        n_days = dt.datetime.now(dt.timezone.utc) - last_date
        n_days = n_days.total_seconds() / 24 / 3600

        result = {
            'date': str(dt.datetime.now(dt.timezone.utc))
        }

        commodities = ['coal_eur', 'gas_eur', 'oil_eur', 'total_eur']
        for c in commodities:
            result[c] = last_item['cumulated_' + c] + last_item[c] * n_days
            result[c + '_per_sec'] = last_item[c] / 24 / 3600

        response = Response(
            response=json.dumps(result),
            status=200,
            mimetype='application/json'
        )
        return response


@routes_api.route('/v1/russia_counter_prices', strict_slashes=False)
class RussiaCounterPriceResource(Resource):

    parser = reqparse.RequestParser()
    parser.add_argument('format', type=str, help='format of returned results (json or csv)',
                        required=False, default="json")

    @routes_api.expect(parser)
    def get(self):

        params = RussiaCounterPriceResource.parser.parse_args()
        format = params.get("format")
        prices_df = get_counter_prices()

        if format == "json":
            response = Response(
                response=json.dumps(prices_df.to_dict(orient='records')),
                status=200,
                mimetype='application/json'
            )
            return response

        if format == "csv":
            response = Response(
                response=prices_df.to_csv(),
                mimetype="text/csv",
                headers={"Content-disposition":
                             "attachment; filename=counter_prices.csv"})

            return response