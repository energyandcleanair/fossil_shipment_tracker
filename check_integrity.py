import integrity
import base

import datetime as dt

def update():
    integrity.check()
    return

if __name__ == "__main__":
    print("=== Using %s environment ===" % (base.db.environment,))
    update()
