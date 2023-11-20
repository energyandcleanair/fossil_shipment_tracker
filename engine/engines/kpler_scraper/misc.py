import sqlalchemy as sa
from base.logger import logger
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


def get_commodity_equivalent(row):
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


def get_commodity_pricing(row):
    id = row["id"]
    equivalent_id = row["equivalent_id"]
    name = row["name"]

    if name in ["Crude/Co", "Crude", "Condensate"]:
        return base.CRUDE_OIL
    elif equivalent_id == base.LNG:
        return base.LNG
    elif equivalent_id == base.COAL:
        return base.COAL
    else:
        return id


def get_nested(x, *keys, warn=True):
    """
    Get a nested value from a dict
    :param x:
    :param keys:
    :return:
    """
    if keys:
        try:
            return get_nested(x.get(keys[0]), *keys[1:], warn=warn)
        except AttributeError:
            if warn:
                logger.warning(f"Error while getting nested value {keys} in {x}")
            return None
    return x
