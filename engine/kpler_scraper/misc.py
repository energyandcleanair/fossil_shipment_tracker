import sqlalchemy as sa

import base


def get_split_name(split):
    if "countries" in split.value:
        return "country"
    elif "ports" in split.value:
        return "port"
    elif "installations" in split.value:
        return "installation"
    else:
        raise ValueError(f"Unknown split: {split}")


def get_product_id(name):
    # Replace " " or "/" by "_"
    id = "kpler_" + name.replace(" ", "_").replace("/", "_").lower()
    return id


def get_product_id_field(name_field):
    # Replace " " or "/" by "_" in the sqlalchemy field
    id = "kpler_" + sa.func.replace(sa.func.replace(sa.func.lower(name_field), " ", "_"), "/", "_")
    return id


def get_commodity_group(row):
    """

    :param row:
    :return:
    """
    if row["family"] in ["Dirty"] and row["name"] != "Condensate":
        return base.CRUDE_OIL
    elif (
        row["group"] in ["Fuel Oils"] or row["family"] in ["Light Ends", "Middle Distillates"]
    ) and row["name"] != "Clean Condensate":
        return base.OIL_PRODUCTS
    elif row["name"] == "Fuel Oils":
        return base.OIL_PRODUCTS
    elif row["name"] == "lng":
        return base.LNG
    elif row["name"] in ["Coal", "Thermal", "Metallurgical"]:
        return base.COAL
    else:
        return None
