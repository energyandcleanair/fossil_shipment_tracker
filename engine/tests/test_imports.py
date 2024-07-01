from .mock_db_module import *


def test_engine_imports(mocker):
    import engines.alert
    import engines.arrival
    import engines.backuper
    import engines.berth
    import engines.cache
    import engines.commodity
    import engines.company
    import engines.currency
    import engines.datalastic
    import engines.departure
    import engines.engine_r
    import engines.flaring
    import engines.global_cache
    import engines.insurance
    import engines.kpler_trade_computed
    import engines.marinetraffic
    import engines.mtevents
    import engines.port
    import engines.portcall
    import engines.position
    import engines.ship
    import engines.shipment
    import engines.sts
    import engines.terminal
    import engines.trajectory
