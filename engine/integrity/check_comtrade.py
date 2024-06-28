from base.models import ComtradeHsTradeRecord

from engines.comtrade_client.comtrade import ComtradeCommodities, ComtradeClient

from base.db import session
import random


def test_sample_comtrade():

    all_reporters = (
        session.query(ComtradeHsTradeRecord.reporter_iso2)
        .group_by(ComtradeHsTradeRecord.reporter_iso2)
        .all()
    )
    sample_reporters = random.sample(all_reporters, 5)

    commodities = [commodity for commodity in ComtradeCommodities]
    sample_commodities = random.sample(commodities, 5)

    comtrade_client = ComtradeClient.from_env()

    comtrade_client.get_monthly_trades_for_periods()
