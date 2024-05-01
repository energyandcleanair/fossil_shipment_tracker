import pandas as pd
import pytest
from engines.entsog import *


@pytest.mark.system
def test_interconnections():
    ic = get_interconnections()
    assert isinstance(ic, pd.DataFrame)
    assert set(["pointKey", "pointKey"]) < set(ic.columns)
    return


@pytest.mark.system
def test_bulgaria_greece():
    date_from = "2020-01-01"
    date_to = "2020-01-31"

    flows_db = get_flows_raw(
        date_from=date_from, date_to=date_to, country_iso2=["BG", "TR"], use_db=True
    )

    import_de = (
        flows_import_raw.groupby(["pointKey", "operatorKey", "pointLabel", "country", "partner"])
        .agg(value_bcm=("value", lambda x: np.nansum(x) / base.GCV_KWH_PER_M3 / 1e9))
        .reset_index()
        .sort_values(["value_bcm"], ascending=False)
    )

    opd = get_operator_point_directions()
    poi1 = opd.loc[opd.pointKey.isin(["ITP-00069", "ITP-00538"])]
    poi1 = opd.loc[opd.pointLabel.isin(["VIP Waidhaus NCG", "Waidhaus (OGE)"])]
    poi1b = opd.loc[opd.pointLabel.str.contains("Waidhaus")]
    poi2 = opd.loc[opd.pointLabel.isin(["Dornum / NETRA (OGE)", "Dornum GASPOOL"])]


@pytest.mark.system
def test_get_crossborder_flows():
    # flows = update(date_from='2018-01-01',
    #                date_to='2019-12-31',
    #                country_iso2=['AT'])

    flows = update(
        date_from="2020-01-01",
        date_to="2020-02-01",
        country_iso2=["NL", "BE"],
        remove_pipe_in_pipe=True,
        delete_before_upload=True,
    )

    # country=['BE'],)

    # for year in range(2022, 2023):
    #     for filter in [False]:
    #         filter_str = 'filtered' if filter else 'unfiltered'
    #         flows = get_flows(date_from='%s-01-01'%(year),
    #                           date_to='%s-12-31'%(year),
    #                           save_intermediary_to_file=True,
    #                           use_csv_selection=not filter,
    #                           intermediary_filename='entsog/local/entsog_%s_%s_intermediary.csv'%(year, filter_str),
    #                           save_to_file=True,
    #                           filename='entsog/local/entsog_%s_%s.csv' % (year, filter_str))

    for year in range(2021, 2022):
        get_flows(
            date_from="%s-01-01" % (year),
            date_to="%s-12-31" % (year),
            save_intermediary_to_file=True,
            intermediary_filename="entsog/local/entsog_%s_uploaded_intermediary.csv" % (year),
            save_to_file=True,
            remove_pipe_in_pipe=True,
            filename="entsog/local/entsog_%s_uploaded.csv" % (year),
        )

    flows["month"] = pd.to_datetime(flows["date"]).dt.strftime("%Y-%m-01")
    total = (
        flows.groupby(["departure_iso2", "destination_iso2", "month"])
        .agg(value_bcm=("value_m3", lambda x: np.nansum(x) / 1e9))
        .reset_index()
    )
    total["departure_iso2"] = total.departure_iso2.replace({"DZ": "Maghreb"})
    total["departure_iso2"] = total.departure_iso2.replace({"AZ": "TR"})

    iea = (
        pd.read_csv("engine/tests/assets/iea.csv")
        .groupby(["departure_iso2", "destination_iso2", "month"])
        .agg(value_bcm=("value_m3", lambda x: np.nansum(x) / 1e9))
        .reset_index()
    )
    iea = iea[iea.month == "2022-01-01"]
    iea["departure_iso2"] = iea.departure_iso2.replace({"TN": "Maghreb"})

    comp = total.merge(
        iea,
        how="left",
        on=["departure_iso2", "destination_iso2", "month"],
        suffixes=["_entsog", "_iea"],
    )
    comp = comp.fillna(0)
    comp["pct"] = (comp.value_bcm_entsog - comp.value_bcm_iea) / comp.value_bcm_iea
    comp["abs"] = comp.value_bcm_entsog - comp.value_bcm_iea

    comp = comp.sort_values(["abs"], ascending=True)
    comp = comp[comp.departure_iso2 != comp.destination_iso2]
    comp = comp[comp.departure_iso2 != "lng"]
    comp = comp[(comp.departure_iso2 != "RU") | (comp.destination_iso2 != "UA")]

    error = comp[~np.isnan(comp["abs"])]["abs"].abs().sum() / comp.value_bcm_iea.sum()
    assert error < 0.25

    assert abs(np.sum(total.loc[total.departure_iso2 == "NO"].value_bcm) - 42) < 4
    assert abs(np.sum(total.loc[total.departure_iso2 == "RU"].value_bcm) - 53) < 3
