from engine import ship


def test_ship_fill():
    imos, mmsis = [], ['248554000', '355461000', '636022397', '636014354', '636022144', '240626000', '273350170', '248895000', '341576000', '273214860', '249865000', '215777000', '273355410', '477118700', '538004215', '229911000', '577165000', '636022259', '636020228', '273426280', '372371000', '273426280', '577165000', '273394890', '538008157', '636022397', '352001148', '538002565', '636017647', '636014354', '273353580', '351588000', '249865000', '636022504', '248895000', '273355410', '341815000', '636015736', '273426280', '215925000', '352001148', '636017647', '370863000', '538004215', '538008157', '636022256', '341903000', '341600000', '341903000', '341815000', '636022504', '229911000', '341815000', '229902000', '341815000', '636022267', '273353580', '240626000', '215247000', '341815000', '341815000']

    ship.fill()


def test_convert_mmsis_to_array():
    ship.fix_duplicate_imo(imo='9402249', handle_not_found=False)


def test_convert_cache():
    ship.convert_mminame_cache_to_array()