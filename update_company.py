from engine import company
import base

def update():
    company.update()
    return

if __name__ == "__main__":
    print("=== Using %s environment ===" % (base.db.environment,))
    update()
