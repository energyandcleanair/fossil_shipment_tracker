from .mock_db_module import *


def test_engine_imports(mocker):
    import engines.alert
    import engines.backuper
    import engines.company
    import engines.commodity
    import engines.comtrade
    import engines.counter
    import engines.currency
    import engines.engine_r
    import engines.entsog
    import engines.flaring
    import engines.global_cache
    import engines.insurance
    import engines.kpler_trade_computed
    import engines.ship


def test_update_lite(mocker):
    import run.update_lite
    import run.update_main
