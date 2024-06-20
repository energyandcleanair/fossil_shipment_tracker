from base.models.kpler import KplerVessel

from base.db import session
from base.logger import logger

import datetime as dt

from tqdm import tqdm


def main():

    vessels_without_class: list[KplerVessel] = (
        session.query(KplerVessel)
        .filter((KplerVessel.type_class_name == None) | (KplerVessel.capacity_cm == None))
        .all()
    )

    for vessel in tqdm(vessels_without_class):
        vessel.type_name = vessel.others.get("vesselType")
        vessel.class_name = vessel.others.get("vesselClass")
        vessel.type_class_name = vessel.others.get("vesselTypeClass")
        vessel.capacity_cm = vessel.others.get("capacity")

    session.commit()


if __name__ == "__main__":
    main()
