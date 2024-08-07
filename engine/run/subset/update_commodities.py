from engines import commodity

import base


def update():
    commodity.fill()


if __name__ == "__main__":
    print("=== Filling commodities: using %s environment ===" % (base.db.environment,))
    update()
    print("=== Filling commodities complete ===")
