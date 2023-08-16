from engine import company
import integrity
import base

def update():
    company.update()
    integrity.check()
    return

if __name__ == "__main__":
    print("=== Using %s environment ===" % (base.db.environment,))
    update()
