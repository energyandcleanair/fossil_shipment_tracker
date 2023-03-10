from kpler.sdk import FlowsDirection, FlowsSplit, FlowsPeriod, FlowsMeasurementUnit


def get_split_name(split):
    if "countries" in split.value:
        return "country"
    elif "ports" in split.value:
        return "port"
    elif "installations" in split.value:
        return "installation"
    else:
        raise ValueError(f"Unknown split: {split}")
