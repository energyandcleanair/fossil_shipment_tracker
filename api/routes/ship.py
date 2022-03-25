import simplejson as json

from . import routes_api
# from base.encoder import JsonEncoder
from timeit import default_timer as timer
from http import HTTPStatus
from flask import Response
from flask_restx import Resource, reqparse


@routes_api.route('/ship', strict_slashes=False)
class ShipResource(Resource):

    parser = reqparse.RequestParser()
    parser.add_argument('imo', required=False, action='split')
    parser.add_argument('format', type=str, help='format of returned results (json or csv)',
                        required=False, default="json")

    @routes_api.expect(parser)
    def get(self):

        params = VoyageResource.parser.parse_args()
        location_id = params.get("location")
        city_id = params.get("city")
        city_name = params.get("city_name")

        if format == "json":
            end_processing = timer()
            start_todict = timer()
            meas = meas_df.to_dict(orient="records")
            end_todict = timer()

            performance = {
                "query": round(end_query - start_query, 2),
                "processing": round(end_processing - start_processing, 2),
                "todict": round(end_todict - start_todict, 2)
            }

            resp = json.dumps({"data": meas,
                               "performance": performance},
                              cls=JsonEncoder)

            return Response(response=resp,
                            status=200,
                            mimetype='application/json')

        if format == "csv":
            return Response(
                response=meas_df.to_csv(),
                mimetype="text/csv",
                headers={"Content-disposition":
                             "attachment; filename=measurements.csv"})

        return Response(response="Unknown format. Should be either csv or json",
                        status=HTTPStatus.BAD_REQUEST,
                        mimetype='application/json')

