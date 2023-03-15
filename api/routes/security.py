import functools
from hmac import compare_digest
from flask import request
import sqlalchemy as sa

from base.models import ApiKey
from base.db import session


def is_valid(api_key, endpoint):
    n_found = (
        session.query(ApiKey)
        .filter(
            ApiKey.key == api_key,
            sa.or_(ApiKey.endpoints == sa.null(), ApiKey.endpoints.contains([endpoint])),
        )
        .count()
    )
    return n_found > 0


def key_required(func):
    @functools.wraps(func)
    def decorator(*args, **kwargs):
        api_key = request.args.get("api_key")
        if api_key is None:
            return {"message": "Please provide an API key"}, 400
        # Check if API key is correct and valid
        if is_valid(api_key, request.path):
            return func(*args, **kwargs)
        else:
            return {"message": "The provided API key is not valid"}, 403

    return decorator
