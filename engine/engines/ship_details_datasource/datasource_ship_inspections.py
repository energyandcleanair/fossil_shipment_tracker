import datetime as dt
from base.db import session, engine
from base.models import (
    ShipInspection,
)
from base.models.table_names import DB_TABLE_SHIP_INSPECTIONS


def update_ships_inspections(imo, inspection_info):
    inspection_df = convert_inspections_to_df(imo, inspection_info)
    inspection_df["updated_on"] = dt.datetime.now()

    with session.begin_nested():
        # Delete existing inspections for ship
        session.query(ShipInspection).filter(ShipInspection.ship_imo == imo).delete()
        # Insert updated inspections
        inspection_df.to_sql(DB_TABLE_SHIP_INSPECTIONS, con=engine, if_exists="append", index=False)
    session.commit()


def convert_inspections_to_df(imo, inspection_info):
    inspection_df = inspection_info["inspections"]
    inspection_df["ship_imo"] = imo
    inspection_df = inspection_df.rename(
        columns={
            "Authority": "authority",
            "Port of inspection": "port_of_inspection",
            "Date of report": "date_of_report",
            "Detention": "detention",
            "PSC Organisation": "psc_organisation",
            "Type of inspection": "type_of_inspection",
            "Duration (days)": "duration_days",
            "Number of deficiencies": "number_of_deficiencies",
        }
    )

    return inspection_df
