import datetime
import json
import decimal
import pandas as pd

class JsonEncoder(json.JSONEncoder):
    def default(self, o):
        if pd.isnull(o):
            return None
        if isinstance(o, (datetime.date, datetime.datetime)):
            return o.isoformat()
        if isinstance(o, (decimal.Decimal)):
            return float(o)
        return super(JsonEncoder, self).default(o)