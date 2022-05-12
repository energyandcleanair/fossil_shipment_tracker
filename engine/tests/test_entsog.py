import pandas as pd
from engine.entsog import *


def test_interconnections():
    ic = get_interconnections()
    assert isinstance(ic, pd.DataFrame)
    assert set(["pointKey", "pointKey"]) < set(ic.columns)
    return


def test_get_crossborder_flows(test_db):
    flows = get_crossborder_flows(date_from='2020-01-01',
                                  # date_to='2020-01-10',
                                  save_to_file=True,
                                  # country_iso2='DE',
                                  # partner_iso2='BY'
                                  )

    total = flows.groupby(['to_country', 'from_country']).agg(value=('value', np.nansum)) \
        .reset_index()

    assert abs(np.sum(total.loc[total.from_country == 'Norway'].value)/1e9 - 42) < 4
    assert abs(np.sum(total.loc[total.from_country == 'Russia'].value) / 1e9 - 53) < 3

    return
