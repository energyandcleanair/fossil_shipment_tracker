from http import HTTPStatus
from flask import Response
from flask_restx import Resource, reqparse
from flask_restx import inputs

from base.utils import update_geometry_from_wkb, to_datetime, df_to_json
from . import routes_api


@routes_api.route('/v0/alert_test', strict_slashes=False)
class AlertTestResource(Resource):

    parser = reqparse.RequestParser()

    parser.add_argument('destination_iso2', help='What new destination country code(s) e.g. IT,IN',
                        action='split',
                        required=False,
                        default=None)

    parser.add_argument('destination_name_pattern', help='What new destination name pattern(s)',
                        action='split',
                        required=False,
                        default=None)

    parser.add_argument('commodity', help='Commodity(ies) of interest',
                        action='split',
                        required=False,
                        default=None)

    parser.add_argument('min_dwt', help='Minimal tonnage of ship',
                        type=float,
                        required=False,
                        default=None)

    parser.add_argument('date_from', help='Starting date. Can be an integer e.g. -3 for 3 days before now',
                        type=str,
                        required=False,
                        default='-7')

    parser.add_argument('format', type=str, help='format of returned results (json, csv, or geojson)',
                        required=False, default="json")

    parser.add_argument('nest_in_data', help='Whether to nest the json content in a data key.',
                        type=inputs.boolean, default=True)


    @routes_api.expect(parser)
    def get(self):
        from engine.alert import manual_alert

        params = AlertTestResource.parser.parse_args()
        destination_iso2 = params.get('destination_iso2')
        destination_name_pattern = params.get('destination_name_pattern')
        commodity = params.get('commodity')
        min_dwt = params.get('min_dwt')
        date_from = params.get('date_from')
        format = params.get("format")
        nest_in_data = params.get('nest_in_data')

        # Retool adds empty arguments
        if destination_iso2 and '' in destination_iso2:
            destination_iso2.remove('')

        if destination_name_pattern and '' in destination_name_pattern:
            destination_name_pattern.remove('')

        if commodity and '' in commodity:
            commodity.remove('')


        alerts_df = manual_alert(destination_name_pattern=destination_name_pattern,
                              destination_iso2=destination_iso2,
                              date_from=to_datetime(date_from),
                              commodity=commodity,
                              min_dwt=min_dwt)


        if format == "csv":
            return Response(
                response=alerts_df.to_csv(index=False),
                mimetype="text/csv",
                headers={"Content-disposition":
                             "attachment; filename=alert_shipments.csv"})

        if format == "json":
            return Response(
                response=df_to_json(alerts_df, nest_in_data=nest_in_data),
                status=200,
                mimetype='application/json')

        return Response(response="Unknown format. Should be either csv, json or geojson",
                        status=HTTPStatus.BAD_REQUEST,
                        mimetype='application/json')