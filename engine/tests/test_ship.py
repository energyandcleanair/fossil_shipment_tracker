import pytest
from engines import ship


@pytest.mark.system
def test_ship_fill():
    imos, mmsis = ["9640499", "9831828"], ["538009877", "538009877"]

    ship.fill(imos=imos, mmsis=mmsis)


@pytest.mark.system
def test_fix_imo():
    ship.fix_duplicate_imo(imo="9402249", handle_versioned=True, handle_not_found=False)

    ship.fix_duplicate_imo(imo="NOTFOUND_239984000", handle_versioned=False, handle_not_found=True)
