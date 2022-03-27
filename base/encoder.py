import datetime
import json
import decimal


class JsonEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, (datetime.date, datetime.datetime)):
            return o.isoformat()
        if isinstance(o, (decimal.Decimal)):
            return float(o)
        return super(JsonEncoder, self).default(o)