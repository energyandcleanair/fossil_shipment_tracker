import json
import pandas as pd
import datetime as dt
import numpy as np
from sqlalchemy.dialects.postgresql import array
from flask import Response
from flask_restx import Resource, reqparse, inputs
from sqlalchemy import func, BigInteger
from sqlalchemy.sql.expression import cast


import base
from base.models import Price, Port, Currency
from base.encoder import JsonEncoder
from base.db import session
from base.utils import to_list, to_datetime
from base import PRICING_DEFAULT
from . import routes_api


@routes_api.route("/v0/price", methods=["GET"], strict_slashes=False)
class PriceResource(Resource):

    parser = reqparse.RequestParser()
    parser.add_argument(
        "commodity",
        help="commodity(ies) of interest. Default: returns all of them",
        default=None,
        action="split",
        required=False,
    )
    parser.add_argument(
        "date_from",
        help="start date (format 2020-01-15)",
        default="2022-01-01",
        required=False,
    )

    parser.add_argument(
        "date_to",
        type=str,
        help="end date (format 2020-01-15)",
        required=False,
        default=dt.datetime.today().strftime("%Y-%m-%d"),
    )

    parser.add_argument(
        "scenario",
        help="Pricing scenario (standard or pricecap)",
        default=PRICING_DEFAULT,
        required=False,
    )

    parser.add_argument(
        "port_id", help="id(s) of port(s)", default=None, required=False, action="split"
    )

    parser.add_argument(
        "nest_in_data",
        help="Whether to nest the json content in a data key.",
        type=inputs.boolean,
        default=True,
    )
    parser.add_argument(
        "format",
        type=str,
        help="format of returned results (json or csv)",
        required=False,
        default="json",
    )

    @routes_api.expect(parser)
    def get(self):

        params = PriceResource.parser.parse_args()
        commodity = params.get("commodity")
        date_from = params.get("date_from")
        scenario = params.get("scenario")
        date_to = params.get("date_to")
        format = params.get("format")
        port_id = params.get("port_id")
        nest_in_data = params.get("nest_in_data")

        query = Price.query.filter(Price.scenario == scenario)

        if commodity is not None:
            query = query.filter(Price.commodity.in_(to_list(commodity)))

        if date_from is not None:
            query = query.filter(Price.date >= to_datetime(date_from))

        if date_to is not None:
            query = query.filter(Price.date <= to_datetime(date_to))

        if port_id is not None:
            query = query.filter(
                Price.departure_port_ids.contains(
                    array([cast(int(x), BigInteger) for x in to_list(port_id)])
                )
            )

        price_df = pd.read_sql(query.statement, session.bind)
        price_df.replace({np.nan: None}, inplace=True)

        if format == "csv":
            return Response(
                response=price_df.to_csv(index=False),
                mimetype="text/csv",
                headers={"Content-disposition": "attachment; filename=prices.csv"},
            )

        if format == "json":
            if nest_in_data:
                resp_content = json.dumps(
                    {"data": price_df.to_dict(orient="records")}, cls=JsonEncoder
                )
            else:
                resp_content = json.dumps(price_df.to_dict(orient="records"), cls=JsonEncoder)

            return Response(response=resp_content, status=200, mimetype="application/json")


@routes_api.route("/v0/portprice", methods=["GET"], strict_slashes=False, doc=False)
class PortPriceResource(Resource):

    parser = reqparse.RequestParser()
    parser.add_argument("unlocode", help="UNLOCODE", default=None, action="split", required=False)
    parser.add_argument(
        "commodity",
        help="commodity(ies) of interest. Default: returns all of them",
        default=None,
        action="split",
        required=False,
    )
    parser.add_argument(
        "date_from",
        help="start date (format 2020-01-15)",
        default="2022-01-01",
        required=False,
    )
    parser.add_argument(
        "date_to",
        type=str,
        help="end date (format 2020-01-15)",
        required=False,
        default=dt.datetime.today().strftime("%Y-%m-%d"),
    )
    parser.add_argument(
        "ship_owner_iso2",
        action="split",
        help="iso2(s) of ship owner",
        required=False,
        default=base.PRICE_NULLARRAY_CHAR,
    )
    parser.add_argument(
        "ship_insurer_iso2",
        action="split",
        help="iso2(s) of ship insurer",
        required=False,
        default=base.PRICE_NULLARRAY_CHAR,
    )
    parser.add_argument(
        "destination_iso2",
        action="split",
        help="iso2(s) of destination",
        required=False,
        default=base.PRICE_NULLARRAY_CHAR,
    )
    parser.add_argument(
        "scenario",
        help="Pricing scenario (standard or pricecap)",
        default=PRICING_DEFAULT,
        required=False,
    )
    parser.add_argument(
        "nest_in_data",
        help="Whether to nest the json content in a data key.",
        type=inputs.boolean,
        default=True,
    )
    parser.add_argument(
        "format",
        type=str,
        help="format of returned results (json or csv)",
        required=False,
        default="json",
    )

    @routes_api.expect(parser)
    def get(self):

        params = PortPriceResource.parser.parse_args()
        unlocode = params.get("unlocode")
        commodity = params.get("commodity")
        date_from = params.get("date_from")
        date_to = params.get("date_to")
        scenario = params.get("scenario")
        format = params.get("format")
        nest_in_data = params.get("nest_in_data")
        ship_owner_iso2 = params.get("ship_owner_iso2")
        ship_insurer_iso2 = params.get("ship_insurer_iso2")
        destination_iso2 = params.get("destination_iso2")

        unnested_query = (
            session.query(
                Price.commodity,
                Price.date,
                Price.scenario,
                Price.destination_iso2s,
                Price.ship_owner_iso2s,
                Price.ship_insurer_iso2s,
                func.unnest(Price.departure_port_ids).label("port_id"),
                (Currency.per_eur * Price.eur_per_tonne).label("usd_per_tonne"),
                (Currency.per_eur * Price.eur_per_tonne * 0.138).label("usd_per_barrel"),
            )
            .join(Currency, Currency.date == Price.date)
            .filter(Price.scenario.in_(to_list(scenario)), Currency.currency == "USD")
            .subquery()
        )

        query = session.query(unnested_query).join(Port, Port.id == unnested_query.c.port_id)

        if unlocode is not None:
            query = query.filter(Port.unlocode.in_(to_list(unlocode)))

        if commodity is not None:
            query = query.filter(unnested_query.c.commodity.in_(to_list(commodity)))

        if date_from is not None:
            query = query.filter(unnested_query.c.date >= to_datetime(date_from))

        if date_to is not None:
            query = query.filter(unnested_query.c.date <= to_datetime(date_to))

        if ship_owner_iso2 is not None:
            if ship_owner_iso2 != base.PRICE_NULLARRAY_CHAR:
                query = query.filter(
                    unnested_query.c.ship_owner_iso2s.overlap(to_list(ship_owner_iso2))
                )
            else:
                query = query.filter(unnested_query.c.ship_owner_iso2s == ship_owner_iso2)

        if ship_insurer_iso2 is not None:
            if ship_insurer_iso2 != base.PRICE_NULLARRAY_CHAR:
                query = query.filter(
                    unnested_query.c.ship_insurer_iso2s.overlap(to_list(ship_insurer_iso2))
                )
            else:
                query = query.filter(unnested_query.c.ship_insurer_iso2s == ship_insurer_iso2)

        if destination_iso2 is not None:
            if destination_iso2 != base.PRICE_NULLARRAY_CHAR:
                query = query.filter(
                    unnested_query.c.destination_iso2s.overlap(to_list(destination_iso2))
                )
            else:
                query = query.filter(unnested_query.c.destination_iso2s == destination_iso2)

        price_df = pd.read_sql(query.statement, session.bind)
        price_df.replace({np.nan: None}, inplace=True)
        price_df.sort_values(["date"], inplace=True)
        price_df["date"] = pd.to_datetime(price_df["date"]).dt.date

        if format == "csv":
            return Response(
                response=price_df.to_csv(index=False),
                mimetype="text/csv",
                headers={"Content-disposition": "attachment; filename=portprices.csv"},
            )

        if format == "json":
            if nest_in_data:
                resp_content = json.dumps(
                    {"data": price_df.to_dict(orient="records")}, cls=JsonEncoder
                )
            else:
                resp_content = json.dumps(price_df.to_dict(orient="records"), cls=JsonEncoder)

            return Response(response=resp_content, status=200, mimetype="application/json")
