import pandas as pd
from engine.entsog import *




def test_interconnections():
    ic = get_interconnections()
    assert isinstance(ic, pd.DataFrame)
    assert set(["pointKey", "pointKey"]) < set(ic.columns)
    return


def test_germany():
    date_from = '2020-02-01'
    date_to = '2020-02-28'

    (flows_import_raw,
     flows_import_lng_raw,
     flows_export_raw,
     flows_export_lng_raw,
     flows_production_raw) = get_crossborder_flows_raw(date_from=date_from,
                                                       date_to=date_to,
                                                       country_iso2='DE')

    a=2

    import_de = flows_import_raw.groupby(['pointKey', 'operatorKey', 'pointLabel', 'country', 'partner']) \
        .agg(value_bcm=('value', lambda x: np.nansum(x) / base.GCV_KWH_PER_M3 / 1e9)) \
        .reset_index() \
        .sort_values(['value_bcm'], ascending=False)


    opd = get_operator_point_directions()
    poi1 = opd.loc[opd.pointKey.isin(['ITP-00069',
                                        'ITP-00538'])]
    poi1 = opd.loc[opd.pointLabel.isin(['VIP Waidhaus NCG',
                                       'Waidhaus (OGE)'])]
    poi1b = opd.loc[opd.pointLabel.str.contains('Waidhaus')]
    poi2 = opd.loc[opd.pointLabel.isin(['Dornum / NETRA (OGE)',
                                        'Dornum GASPOOL'])]

def test_get_crossborder_flows():

    flows = get_crossborder_flows(date_from='2020-01-01',
                                  date_to='2020-01-31',
                                  save_to_file=True,
                                  filename='entsog_flows_202001.csv')


    total = flows.groupby(['to_country', 'from_country']) \
        .agg(value_bcm=('value', lambda x: np.nansum(x) / base.GCV_KWH_PER_M3 / 1e9)) \
        .reset_index()

    # Some numbers taken from IEA
    iea = [
        {'from_country': 'RU', 'to_country': 'DE', 'value_bcm': 5.075},
        {'from_country': 'DE', 'to_country': 'CZ', 'value_bcm': 3.954},
        {'from_country': 'CZ', 'to_country': 'DE', 'value_bcm': 2.102},
        {'from_country': 'NO', 'to_country': 'DE', 'value_bcm': 3.574},
    ]


    comp = total.merge(pd.DataFrame(iea),
                       on=['to_country', 'from_country'],
                       suffixes=['_entsog', '_iea'])

    assert abs(np.sum(total.loc[total.from_country == 'Norway'].value)/1e9 - 42) < 4
    assert abs(np.sum(total.loc[total.from_country == 'RU'].value) / 1e9 - 53) < 3

    return
