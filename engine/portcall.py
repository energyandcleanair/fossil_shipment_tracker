from models import PortCall


def initial_fill():
    """
    Fill PortCall table with manually downloaded data (from MarimeTraffic interface)
    Files are in assets/marinetraffic
    :return:
    """
    #TODO

    return



def update():
    """
    Fill port calls for ports of interest, for dates not yet queried in database
    :return:
    """
    #TODO
    # - query MarineTraffic for portcalls at ports of interest (assets/departure_port_of_interest.csv)
    # - upsert in db
    # Things to pay attention to:
    # - we want to minimize credits use from MarineTraffic. So need to only query from last date available.
    # - port call have foreignkeys towards Port and Ship tables. If Port or Ship are missing, we need to find
    # information and insert them in their respective tables (probably using Datalastic)

    return



def get(date_from=None):
    """

    :param date_from:
    :return: Pandas dataframe of portcalls
    """
    return