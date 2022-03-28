from engine.datalastic import Datalastic


def get(imo, date_from, date_to):
    positions = Datalastic.get_positions(imo=imo, date_from=date_from, date_to=date_to)
    return positions


