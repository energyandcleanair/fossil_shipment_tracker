import datetime as dt
import json
from sqlalchemy.exc import IntegrityError

from base.encoder import JsonEncoder
from base.models import EndpointCache
from base.db import session


class EndpointCacher:
    @classmethod
    def get_cache(cls, endpoint, params, max_age_minutes=60):
        cached = (
            session.query(EndpointCache.response)
            .filter(
                EndpointCache.endpoint == endpoint,
                EndpointCache.params == params,
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
            params=params,
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
                    EndpointCache.endpoint == endpoint, EndpointCache.params == params
                )
                .first()
            )
            existing.response = json.dumps(response, cls=JsonEncoder)
            # existing.updated_on = dt.datetime.utcnow()
            session.commit()
