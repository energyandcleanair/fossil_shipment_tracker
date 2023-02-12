import datetime as dt
import json
from sqlalchemy.exc import IntegrityError
from collections import OrderedDict

from base.encoder import JsonEncoder
from base.models import EndpointCache
from base.db import session


class EndpointCacher:
    @classmethod
    def get_cache(cls, endpoint, params, max_age_minutes=60):
        # Sort params to ensure that the same params in a different order
        # will still be found in cache

        cached = (
            session.query(EndpointCache.response)
            .filter(
                EndpointCache.endpoint == endpoint,
                EndpointCache.params == cls.sort_params(params),
                EndpointCache.updated_on
                >= dt.datetime.utcnow() - dt.timedelta(minutes=max_age_minutes),
            )
            .first()
        )
        if cached:
            return json.loads(cached[0])
        else:
            return None

    @classmethod
    def set_cache(cls, endpoint, params, response):
        new_cache = EndpointCache(
            endpoint=endpoint,
            params=cls.sort_params(params),
            response=json.dumps(response, cls=JsonEncoder),
            # updated_on=dt.datetime.utcnow(),
        )
        session.merge(new_cache)

        try:
            session.commit()
        except IntegrityError:
            session.rollback()
            existing = (
                session.query(EndpointCache)
                .filter(
                    EndpointCache.endpoint == endpoint,
                    EndpointCache.params == cls.sort_params(params),
                )
                .first()
            )
            existing.response = json.dumps(response, cls=JsonEncoder)
            # existing.updated_on = dt.datetime.utcnow()
            session.commit()

    @classmethod
    def sort_params(cls, params):
        # sort values of params if they are lists
        for key, value in params.items():
            if isinstance(value, list):
                params[key] = sorted(value)

        # Sort dictionary keys
        params = OrderedDict(sorted(params.items()))
        return params
