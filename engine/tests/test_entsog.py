import pandas as pd
from engine.entsog import *




def test_austria():

    gcv_kwh_per_m3 = 11.259
    date_from = '2020-01-01'
    date_to = '2020-01-31'

    (flows_import_raw,
     flows_import_lng_raw,
     flows_export_raw,
     flows_export_lng_raw,
     flows_production_raw) = get_crossborder_flows_raw(date_from=date_from,
                                                       date_to=date_to,
                                                       country_iso2=['AT', 'SK'])

    import_flows = flows_import_raw \
            .groupby(['pointLabel', 'directionKey', 'country', 'partner']) \
            .agg(value=('value', lambda x: np.nansum(x) / gcv_kwh_per_m3 / 1e9)) \
            .sort_values(['value'], ascending=False)

    export_at = flows_export_raw \
            .groupby(['pointLabel', 'directionKey', 'country', 'partner']) \
            .agg(value=('value', lambda x: np.nansum(x) / gcv_kwh_per_m3 / 1e9)) \
            .sort_values(['value'], ascending=False)

    ic = get_interconnections()
    opd = get_operator_point_directions()

    flows = process_crossborder_flows_raw(ic=ic,
                                          opd=opd,
                                          flows_import_raw=flows_import_raw,
                                          flows_import_lng_raw=flows_import_lng_raw,
                                          flows_export_raw=flows_export_raw,
                                          flows_export_lng_raw=flows_export_lng_raw,
                                          flows_production_raw=flows_production_raw)

    flows_agg = flows.groupby(['from_country', 'to_country']) \
                    .agg(value_bcm=('value', lambda x: np.nansum(x) / gcv_kwh_per_m3 / 1e9)) \
                    .sort_values(['value_bcm'], ascending=False)



def test_processing():

    gcv_kwh_per_m3 = 11.259
    date_from = '2020-01-01'
    date_to = '2020-01-03'


    (flows_import_raw,
     flows_import_lng_raw,
     flows_export_raw,
     flows_export_lng_raw,
     flows_production_raw) = get_crossborder_flows_raw(date_from=date_from,
                                                       date_to=date_to)

    import_flows = flows_import_raw \
        .groupby(['pointLabel', 'directionKey', 'country', 'partner']) \
        .agg(value=('value', lambda x: np.nansum(x) / gcv_kwh_per_m3 / 1e9)) \
        .sort_values(['value'], ascending=False) \
        .reset_index()


    sk_to_at1 = import_flows.loc[(import_flows.country=='AT') & (import_flows.partner=='SK')].value.sum()

    export_flows = flows_export_raw \
        .groupby(['pointLabel', 'directionKey', 'country', 'partner']) \
        .agg(value=('value', lambda x: np.nansum(x) / gcv_kwh_per_m3 / 1e9)) \
        .sort_values(['value'], ascending=False) \
        .reset_index()

    sk_to_at2 = export_flows.loc[(export_flows.country=='SK') & (export_flows.partner=='AT')].value.sum()

    # Process flows and see if something remains
    ic = get_interconnections()
    opd = get_operator_point_directions()

    flows_agg = process_crossborder_flows_raw(ic=ic,
                                              opd=opd,
                                              flows_import_raw=flows_import_raw,
                                              flows_import_lng_raw=flows_import_lng_raw,
                                              flows_export_raw=flows_export_raw,
                                              flows_export_lng_raw=flows_export_lng_raw,
                                              flows_production_raw=flows_production_raw)

    flows_agg_grouped = flows_agg.groupby(['from_country', 'to_country']) \
        .agg(value_bcm=('value', lambda x: np.nansum(x) / gcv_kwh_per_m3 / 1e9)) \
        .reset_index()

    sk_to_at3 = flows_agg_grouped.loc[(flows_agg_grouped.to_country=='AT') \
                                      & (flows_agg_grouped.from_country=='SK')].value_bcm.sum()


    assert round(sk_to_at1,3) == round(sk_to_at2,3)
    assert round(sk_to_at2, 3) == round(sk_to_at3, 3)

    flows_all = get_crossborder_flows(date_from=date_from,
                                      date_to=date_to,
                                      save_to_file=True,
                                      filename='entsog_flows_202001.csv')

    sk_to_at4 = flows_all.loc[]

def test_italy():

    gcv_kwh_per_m3 = 11.259
    date_from = '2020-01-01'
    date_to = '2020-01-31'

    (flows_import_raw,
     flows_import_lng_raw,
     flows_export_raw,
     flows_export_lng_raw,
     flows_production_raw) = get_crossborder_flows_raw(date_from=date_from,
                                                       date_to=date_to,
                                                       country_iso2=['IT'])


    import_it = flows_import_raw.groupby(['pointLabel', 'directionKey']) \
        .agg(value=('value', lambda x: np.nansum(x) / gcv_kwh_per_m3 / 1e9)) \
        .sort_values(['value'], ascending=False)

    production_it =  flows_production_raw.groupby(['pointLabel', 'directionKey']) \
        .agg(value=('value', lambda x: np.nansum(x) / gcv_kwh_per_m3 / 1e9)) \
        .sort_values(['value'], ascending=False)

    import_it_lng = flows_import_lng_raw \
        .groupby(['pointLabel', 'directionKey']) \
        .agg(value=('value', lambda x: np.nansum(x) / gcv_kwh_per_m3 / 1e9)) \
        .sort_values(['value'], ascending=False)


    # IEA 5bcm without LNG, 0.5 of LNG
    assert import_it.value.sum() > 4.3
    assert len(import_it.loc[import_it.value > 0]) == 5
    assert import_it_lng.value.sum() > 0.55


    # Process them
    ic = get_interconnections()
    opd = get_operator_point_directions()

    flows_agg = process_crossborder_flows_raw(ic=ic,
                                              opd=opd,
                                              flows_import_raw=flows_import_raw,
                                              flows_import_lng_raw=flows_import_lng_raw,
                                              flows_export_raw=flows_export_raw,
                                              flows_export_lng_raw=flows_export_lng_raw,
                                              flows_production_raw=flows_production_raw)

    import_it_agg = flows_agg.groupby(['from_country', 'to_country']) \
        .agg(value_bcm=('value', lambda x: np.nansum(x) / gcv_kwh_per_m3 / 1e9)) \
        .reset_index()

    assert round(import_it_agg.value_bcm.sum(),3) == round(import_it.value.sum() \
                                                + production_it.value.sum() \
                                                + import_it_lng.value.sum(), 3)




def test_germany_czechia():

    gcv_kwh_per_m3 = 11.259
    date_from = '2020-01-01'
    date_to = '2020-01-31'

    (flows_import_de_raw,
     flows_import_de_lng_raw,
     flows_export_cz_raw,
     flows_export_cz_lng_raw,
     flows_production_cz_raw) = get_crossborder_flows_raw(date_from=date_from,
                                            date_to=date_to,
                                            country_iso2=['CZ'],
                                            partner_iso2=['DE'])

    (flows_import_cz_raw,
     flows_import_cz_lng_raw,
     flows_export_de_raw,
     flows_export_de_lng_raw,
     flows_production_de_raw) = get_crossborder_flows_raw(date_from=date_from,
                                                          date_to=date_to,
                                                          country_iso2=['DE'],
                                                          partner_iso2=['CZ'])

    export_de = flows_export_de_raw.groupby(['pointLabel', 'directionKey']) \
             .agg(value=('value', lambda x: np.nansum(x) / gcv_kwh_per_m3 / 1e9 )) \
            .sort_values(['value'], ascending=False)

    import_cz = flows_import_cz_raw.groupby(['pointLabel', 'directionKey']) \
        .agg(value=('value', lambda x: np.nansum(x) / gcv_kwh_per_m3 / 1e9)) \
        .sort_values(['value'], ascending=False)

    import_de = flows_import_de_raw.groupby(['pointLabel', 'directionKey']) \
        .agg(value=('value', lambda x: np.nansum(x) / gcv_kwh_per_m3 / 1e9)) \
        .sort_values(['value'], ascending=False)


    flows_export_de_raw.value.sum() / gcv_kwh_per_m3 / 1e9
    de_to_cz_bcm_raw = flows_import_cz_raw.value.sum() / gcv_kwh_per_m3 / 1e9
    flows_import_de_raw.value.sum() / gcv_kwh_per_m3 / 1e9
    flows_import_cz_raw.value.sum() / gcv_kwh_per_m3 / 1e9
    flows_export_de_raw.value.sum() / gcv_kwh_per_m3 / 1e9

    ic = get_interconnections()
    opd = get_operator_point_directions()

    # connections = ic.loc[(ic.fromCountryKey=='DE') & (ic.toCountryKey=='CZ')]
    # day1 = flows_export_raw.loc[flows_export_raw.date==dt.date(2020,1,1)]
    #
    # export_total = flows_export_raw.groupby(['pointLabel', 'directionKey']) \
    #     .agg(value=('value', lambda x: np.nansum(x) / gcv_kwh_per_m3 / 1e9 )).reset_index()
    # import_total = flows_import_raw.groupby(['pointLabel', 'directionKey']) \
    #     .agg(value=('value', lambda x: np.nansum(x) / gcv_kwh_per_m3 / 1e9))
    #
    # export_total.value.sum()


    # 3.9 bcm from IEA
    # with our ETNSOG data we have 5.7, that includes some VIP Gaspool
    # assert import_total.value.sum() > 3.9

    flows_agg = process_crossborder_flows_raw(ic=ic,
                                              opd=opd,
                                              flows_import_raw=flows_import_cz_raw,
                                              flows_import_lng_raw=None,
                                              flows_export_raw=flows_export_de_raw,
                                              flows_export_lng_raw=None,
                                              flows_production_raw=None)

    de_cz_bcm = flows_agg.groupby(['from_country', 'to_country']) \
        .agg(value_bcm=('value', lambda x: np.nansum(x) / gcv_kwh_per_m3 / 1e9)) \
        .reset_index()
    de_to_cz_bcm = de_cz_bcm.loc[(de_cz_bcm.from_country == 'Germany') & (de_cz_bcm.to_country == 'Czechia')] \
        .value_bcm.sum()

    assert de_to_cz_bcm == de_to_cz_bcm_raw


    # Now try with all countries
    flows_agg_all = get_crossborder_flows(date_from=date_from,
                                          date_to=date_to,
                                          filename='flows_entsog_2020.csv',
                                          save_to_file=True)
    de_cz_bcm_all = flows_agg_all.groupby(['from_country', 'to_country']) \
        .agg(value_bcm=('value', lambda x: np.nansum(x) / gcv_kwh_per_m3 / 1e9)) \
        .reset_index()
    de_to_cz_bcm_all = de_cz_bcm_all.loc[(de_cz_bcm_all.from_country == 'Germany') & (de_cz_bcm_all.to_country == 'Czechia')] \
        .value_bcm.sum()

    assert de_to_cz_bcm_all == de_to_cz_bcm


def test_interconnections():
    ic = get_interconnections()
    assert isinstance(ic, pd.DataFrame)
    assert set(["pointKey", "pointKey"]) < set(ic.columns)
    return


def test_get_crossborder_flows():

    gcv_kwh_per_m3 = 11.259

    flows = get_crossborder_flows(date_from='2020-01-01',
                                  date_to='2020-01-31',
                                  save_to_file=True,
                                  filename='entsog_flows_202001.csv')


    total = flows.groupby(['to_country', 'from_country']) \
        .agg(value_bcm=('value', lambda x: np.nansum(x) / gcv_kwh_per_m3 / 1e9)) \
        .reset_index()

    # Some numbers taken from IEA
    iea = [
        {'from_country': 'RU', 'to_country': 'DE', 'value_bcm': 5.075},
        {'from_country': 'DE', 'to_country': 'CZ', 'value_bcm': 3.954},
        {'from_country': 'CZ', 'to_country': 'DE', 'value_bcm': 2.102}]


    comp = total.merge(pd.DataFrame(iea),
                       on=['to_country', 'from_country'],
                       suffixes=['_entsog', '_iea'])

    assert abs(np.sum(total.loc[total.from_country == 'Norway'].value)/1e9 - 42) < 4
    assert abs(np.sum(total.loc[total.from_country == 'RU'].value) / 1e9 - 53) < 3

    return
