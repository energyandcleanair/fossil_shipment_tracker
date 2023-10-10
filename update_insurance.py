from engine import insurance
import integrity
import base


def update():
    insurance.update()
    return


if __name__ == "__main__":
    print("=== Using %s environment ===" % (base.db.environment,))
    update()
