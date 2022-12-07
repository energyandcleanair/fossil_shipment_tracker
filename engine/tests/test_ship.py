from engine import ship


def test_ship_fill():
    imos, mmsis = [], []

    ship.fill()


def test_convert_mmsis_to_array():
    ship.fix_duplicate_imo(imo='9901037')
