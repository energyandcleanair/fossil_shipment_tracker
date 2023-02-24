def get_postcompute_fn(postcompute):
    fns = {"thousandtonne_millioneur": postcompute_thousandtonne_millioneur}
    return fns.get(postcompute)


def postcompute_none(result, params=None):
    return result


def postcompute_thousandtonne_millioneur(result, params=None):
    if "variable" in result.columns:
        num_columns = result.select_dtypes(include=["float64", "int64"]).columns

        idx_eur = result.variable == "value_eur"
        result.loc[idx_eur, num_columns] = result.loc[idx_eur, num_columns] / 1e6
        result.loc[idx_eur, "variable"] = "Million EUR"

        idx_tonne = result.variable == "value_tonne"
        result.loc[idx_tonne, num_columns] = result.loc[idx_tonne, num_columns] / 1000
        result.loc[idx_tonne, "variable"] = "Thousand tonnes"

    return result
