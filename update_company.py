from engine import company, insurance
import integrity
import base


def update():
    company.update()
    insurance.update()
    integrity.check()
    return


if __name__ == "__main__":
    print("=== Using %s environment ===" % (base.db.environment,))
    update()
