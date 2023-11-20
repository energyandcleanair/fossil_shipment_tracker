from base.models import GlobalCache
from base.db import session


class GlobalCacher:
    cache = None

    def __init__(self, cache_name):
        self.cache_name = cache_name

    def add(self, value):
        self._load_if_empty()

        session.add(GlobalCache(name=self.cache_name, value=value))
        session.commit()
        self.cache.append(value)

    def get(self, filter):
        self._load_if_empty()

        if filter == None:
            return [x for x in self.cache]
        else:
            return [x for x in self.cache if filter(x)]

    def _load_if_empty(self):
        if not self.cache:
            cached_items = (
                session.query(GlobalCache).filter(GlobalCache.name == self.cache_name).all()
            )
            self.cache = [o.value for o in cached_items]
