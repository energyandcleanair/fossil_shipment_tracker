from http import HTTPStatus
from flask import Response
from flask_restx import Resource, reqparse
from flask_restx import inputs
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import func
from sqlalchemy.orm import aliased

from base.utils import df_to_json
from . import routes_api, ns_alerts
from base.db import session
from base.models import KplerVessel, Country, Shipment, Commodity, Port, Departure, Arrival
from base.utils import to_list, to_datetime


@ns_alerts.route("/v0/alert_test", strict_slashes=False)
class AlertTestResource(Resource):

    parser = reqparse.RequestParser()

    parser.add_argument(
        "destination_iso2",
        help="What new destination country code(s) e.g. IT,IN",
        action="split",
        required=False,
        default=None,
    )

    parser.add_argument(
        "destination_name_pattern",
        help="What new destination name pattern(s)",
        action="split",
        required=False,
        default=None,
    )

    parser.add_argument(
        "commodity",
        help="Commodity(ies) of interest",
        action="split",
        required=False,
        default=None,
    )

    parser.add_argument(
        "departure_port_id",
        help="Departure port id(s)",
        action="split",
        required=False,
        default=None,
    )

    parser.add_argument(
        "min_dwt",
        help="Minimal tonnage of ship",
        type=float,
        required=False,
        default=None,
    )

    parser.add_argument(
        "date_from",
        help="Starting date. Can be an integer e.g. -3 for 3 days before now",
        type=str,
        required=False,
        default="-7",
    )

    parser.add_argument(
        "format",
        type=str,
        help="format of returned results (json, csv, or geojson)",
        required=False,
        default="json",
    )

    parser.add_argument(
        "nest_in_data",
        help="Whether to nest the json content in a data key.",
        type=inputs.boolean,
        default=True,
    )

    @routes_api.expect(parser)
    def get(self):
        params = AlertTestResource.parser.parse_args()
        destination_iso2 = params.get("destination_iso2")
        destination_name_pattern = params.get("destination_name_pattern")
        departure_port_id = params.get("departure_port_id")
        min_dwt = params.get("min_dwt")
        date_from = params.get("date_from")
        format = params.get("format")
        nest_in_data = params.get("nest_in_data")

        # Retool adds empty arguments
        if destination_iso2 and "" in destination_iso2:
            destination_iso2.remove("")

        if destination_name_pattern and "" in destination_name_pattern:
            destination_name_pattern.remove("")

        if departure_port_id and "" in departure_port_id:
            departure_port_id.remove("")

        alerts_df = self.manual_alert(
            destination_name_pattern=destination_name_pattern,
            destination_iso2=destination_iso2,
            date_from=to_datetime(date_from),
            min_dwt=min_dwt,
            departure_port_id=departure_port_id,
        )

        if format == "csv":
            return Response(
                response=alerts_df.to_csv(index=False),
                mimetype="text/csv",
                headers={"Content-disposition": "attachment; filename=alert_shipments.csv"},
            )

        if format == "json":
            return Response(
                response=df_to_json(alerts_df, nest_in_data=nest_in_data),
                status=200,
                mimetype="application/json",
            )

        return Response(
            response="Unknown format. Should be either csv, json or geojson",
            status=HTTPStatus.BAD_REQUEST,
            mimetype="application/json",
        )

    def manual_alert(
        self,
        destination_iso2=None,
        destination_name_pattern=None,
        min_dwt=None,
        date_from=None,
        departure_port_id=None,
    ):
        """
        A function to get what would be the resuts from an alert,
        without actually adding the alert_config and criteria in the db.
        Used to test alert on the frontend, for user to know roughly how many ships it would return.

        It should match the results of the build_alerts function below.

        :param destination_iso2s:
        :param delta_time:
        :return:
        """

        DeparturePort = aliased(Port)
        ArrivalPort = aliased(Port)

        destination_iso2_field = func.unnest(Shipment.destination_iso2s).label("destination_iso2")
        destination_name_field = func.unnest(Shipment.destination_names).label("destination_name")
        destination_date_field = func.unnest(Shipment.destination_dates).label("destination_date")

        query = (
            session.query(
                Shipment.id.label("shipment_id"),
                Shipment.status,
                KplerVessel.imo,
                KplerVessel.name,
                KplerVessel.dwt,
                Departure.port_id.label("departure_port_id"),
                DeparturePort.name.label("departure_port_name"),
                destination_iso2_field,
                destination_name_field,
                destination_date_field,
                ArrivalPort.iso2.label("arrival_iso2"),
            )
            .join(Departure, Departure.id == Shipment.departure_id)
            .join(DeparturePort, DeparturePort.id == Departure.port_id)
            .outerjoin(Arrival, Arrival.id == Shipment.arrival_id)
            .outerjoin(ArrivalPort, Arrival.port_id == ArrivalPort.id)
            .join(KplerVessel, KplerVessel.imo == Departure.ship_imo)
            .subquery()
        )

        prev_destination_iso2_field = (
            func.lag(query.c.destination_iso2)
            .over(partition_by=query.c.shipment_id, order_by=query.c.destination_date)
            .label("previous_destination_iso2")
        )

        prev_destination_name_field = (
            func.lag(query.c.destination_name)
            .over(partition_by=query.c.shipment_id, order_by=query.c.destination_date)
            .label("previous_destination_name")
        )

        query2 = (
            session.query(
                query,
                prev_destination_iso2_field,
                prev_destination_name_field,
                Country.name.label("destination_country"),
            )
            .outerjoin(Country, Country.iso2 == query.c.destination_iso2)
            .subquery()
        )

        previous_country = aliased(Country)

        query3 = (
            session.query(query2, previous_country.name.label("previous_country"))
            .outerjoin(
                previous_country, previous_country.iso2 == query2.c.previous_destination_iso2
            )
            .filter(
                sa.or_(
                    query2.c.destination_iso2 != query2.c.previous_destination_iso2,
                    query2.c.destination_name != query2.c.previous_destination_name,
                )
            )
        )

        if destination_iso2:
            query3 = query3.filter(
                sa.or_(
                    sa.and_(
                        query2.c.destination_iso2 != query2.c.previous_destination_iso2,
                        query2.c.destination_iso2.in_(to_list(destination_iso2)),
                    ),
                    query2.c.arrival_iso2.in_(to_list(destination_iso2)),
                )
            )

        if destination_name_pattern:
            query3 = query3.filter(
                sa.and_(
                    query2.c.destination_name != query2.c.previous_destination_name,
                    query2.c.destination_name.in_(to_list(destination_name_pattern)),
                )  # TODO use pattern
            )

        if date_from:
            query3 = query3.filter(query2.c.destination_date >= to_datetime(date_from))

        if min_dwt:
            query3 = query3.filter(query2.c.dwt >= min_dwt)

        if departure_port_id:
            query3 = query3.filter(query2.c.departure_port_id.in_(to_list(departure_port_id)))

        query3 = query3.order_by(query2.c.shipment_id, sa.desc(query2.c.destination_date)).distinct(
            query2.c.shipment_id
        )

        res = pd.read_sql(query3.statement, session.bind)
        return res
