import datetime as dt
from decouple import config
from sqlalchemy import func
from sqlalchemy import or_
import sqlalchemy as sa
from geoalchemy2.functions import (
    ST_MakeLine,
    ST_Multi,
    ST_Union,
    ST_Distance,
    ST_ClusterDBSCAN,
    ST_Centroid,
    ST_IsEmpty,
)
from base.logger import logger_slack, logger
from sqlalchemy.orm import aliased
from sqlalchemy.sql.expression import text
from sqlalchemy import union
from tqdm import tqdm

from skimage.graph import MCP_Geometric
import rasterio as rio
import shapely
from shapely.geometry import LineString, MultiLineString
from base.utils import wkb_to_shape
import matplotlib.pyplot as plt
import matplotlib
import numpy as np


import base
from base.models import (
    Position,
    Trajectory,
    Port,
    Departure,
    Arrival,
    Ship,
    ShipmentArrivalBerth,
    ShipmentDepartureBerth,
    ShipmentWithSTS,
)
from engines import position
from base.db import session
from base.db import engine
from base.utils import to_list, to_datetime
from base.utils import wkb_to_shape, update_geometry_from_wkb
from base.db_utils import upsert
from base.models import DB_TABLE_TRAJECTORY
import pandas as pd
import geopandas as gpd
from geoalchemy2 import Geometry
from engines.shipment import return_combined_shipments


def update(
    shipment_id=None,
    rebuild_all=False,
    do_cluster=True,
    cluster_deg=0.001,
    extend_beyond=False,
    add_port_location_if_need_be=True,
    arrival_port_iso2=None,
    date_from=None,
):
    logger_slack.info("=== Trajectory update ===")

    try:
        create_new(
            shipment_id=shipment_id,
            rebuild_all=rebuild_all,
            do_cluster=do_cluster,
            cluster_deg=cluster_deg,
            extend_beyond=extend_beyond,
            add_port_location_if_need_be=add_port_location_if_need_be,
            arrival_port_iso2=arrival_port_iso2,
        )

        reroute(date_from=date_from, shipment_id=shipment_id)

    except Exception as e:
        logger.info(
            "Failed to update trajectories",
            stack_info=True,
            exc_info=True,
        )
        pass


def create_new(
    shipment_id=None,
    rebuild_all=False,
    do_cluster=True,
    cluster_deg=0.001,
    extend_beyond=False,
    add_port_location_if_need_be=False,
    arrival_port_iso2=None,
):
    DepartureBerthPosition = aliased(Position)
    ArrivalBerthPosition = aliased(Position)
    ArrivalPort = aliased(Port)

    buffer_before_hours = base.QUERY_POSITION_HOURS_BEFORE_DEPARTURE if extend_beyond else 0
    buffer_after_hours = base.QUERY_POSITION_HOURS_AFTER_ARRIVAL if extend_beyond else 0

    shipments_all = return_combined_shipments(session)

    shipments_to_update = (
        session.query(
            shipments_all.c.shipment_id,
            sa.func.greatest(
                Departure.date_utc - dt.timedelta(hours=buffer_before_hours),
                DepartureBerthPosition.date_utc,
            ).label("departure_date"),
            Departure.ship_imo.label("ship_imo"),
            sa.func.least(
                Arrival.date_utc + dt.timedelta(hours=buffer_after_hours),
                ArrivalBerthPosition.date_utc,
            ).label("arrival_date"),
            Trajectory.shipment_id,
            ArrivalPort.geometry.label("arrival_port_geometry"),
        )
        .outerjoin(Trajectory, Trajectory.shipment_id == shipments_all.c.shipment_id)
        .join(Departure, shipments_all.c.shipment_departure_id == Departure.id)
        .outerjoin(Arrival, shipments_all.c.shipment_arrival_id == Arrival.id)
        .outerjoin(ArrivalPort, Arrival.port_id == ArrivalPort.id)
        .outerjoin(
            ShipmentDepartureBerth,
            ShipmentDepartureBerth.shipment_id == shipments_all.c.shipment_id,
        )
        .outerjoin(
            ShipmentArrivalBerth,
            ShipmentArrivalBerth.shipment_id == shipments_all.c.shipment_id,
        )
        .outerjoin(
            DepartureBerthPosition,
            DepartureBerthPosition.id == ShipmentDepartureBerth.position_id,
        )
        .outerjoin(
            ArrivalBerthPosition,
            ArrivalBerthPosition.id == ShipmentArrivalBerth.position_id,
        )
        .filter(
            sa.or_(
                rebuild_all,
                Trajectory.shipment_id.is_(None),
                Trajectory.geometry.is_(None),
            ),
            shipments_all.c.shipment_status.in_([base.COMPLETED, base.ONGOING]),
        )
    )

    if shipment_id is not None:
        shipments_to_update = shipments_to_update.filter(
            shipments_all.c.shipment_id.in_(to_list(shipment_id))
        )

    if arrival_port_iso2 is not None:
        shipments_to_update = shipments_to_update.filter(
            ArrivalPort.iso2.in_(to_list(arrival_port_iso2))
        )

    shipments_to_update = shipments_to_update.subquery()
    ordered_positions = (
        session.query(
            shipments_to_update.c.shipment_id,
            Position.date_utc,
            Position.geometry
            # func.lag(Position.date_utc).over(
            #     Position.ship_imo,
            #     Position.date_utc
            # ).label('previous_date_utc'),
            # func.lag(Position.geometry).over(
            #     Position.ship_imo,
            #     Position.date_utc
            # ).label('previous_geometry')
        )
        .join(Position, Position.ship_imo == shipments_to_update.c.ship_imo)
        .filter(
            sa.and_(
                Position.date_utc >= shipments_to_update.c.departure_date,
                sa.or_(
                    Position.date_utc <= shipments_to_update.c.arrival_date,
                    shipments_to_update.c.arrival_date == sa.null(),
                ),
            )
        )
        .order_by(shipments_to_update.c.shipment_id, Position.date_utc)
    )

    if add_port_location_if_need_be:
        ordered_positions = union(
            ordered_positions.statement,
            session.query(
                shipments_to_update.c.shipment_id,
                shipments_to_update.c.arrival_date.label("date_utc"),
                shipments_to_update.c.arrival_port_geometry.label("geometry"),
            ).filter(
                shipments_to_update.c.arrival_port_geometry != sa.null(),
                sa.not_(ST_IsEmpty(shipments_to_update.c.arrival_port_geometry)),
            ),
        ).alias()

        ordered_positions = session.query(ordered_positions).order_by(
            ordered_positions.c.shipment_id, ordered_positions.c.date_utc
        )

    ordered_positions = ordered_positions.subquery()

    if do_cluster:
        trajectories = cluster(ordered_positions, buffer_deg=cluster_deg)
    else:
        trajectories = session.query(
            ordered_positions.c.shipment_id.label("shipment_id"),
            ST_Multi(ST_MakeLine(ordered_positions.c.geometry)).label("geometry"),
        ).group_by(ordered_positions.c.shipment_id)

    # We split in different segments if two points are two distant (timewise for now)
    # max_hours = 48
    # max_deg = 5
    # segmented_positions = session.query(ordered_positions,
    #                                     ST_Distance(ordered_positions.c.geometry, ordered_positions.c.previous_geometry).label('distance'),
    #                                     sa.and_(
    #                                     ordered_positions.c.date_utc - ordered_positions.c.previous_date_utc > dt.timedelta(hours=max_hours),
    #                                     ST_Distance(ordered_positions.c.geometry, ordered_positions.c.previous_geometry) > max_deg
    #                                      ).label('new_segment')
    #
    #                                     ) \
    # .subquery()
    #
    # from sqlalchemy.sql.expression import text
    # segmented_positions2 = session.query(segmented_positions,
    #                                     text("""sum(new_segment::integer) over
    #                                     (order by shipment_id, date_utc
    #                                     rows between unbounded preceding and current row) as segment""")) \
    #     .subquery()

    # .group_by(segmented_positions2.c.shipment_id, text("coalesce(segment, -1)")) \

    #
    # trajectories_combined = session.query(trajectories.c.shipment_id,
    #                                       ST_Multi(ST_Union(trajectories.c.geometry)).label('geometry')) \
    #     .group_by(trajectories.c.shipment_id)

    trajectories_df = pd.read_sql(trajectories.statement, session.bind)
    trajectories_df = update_geometry_from_wkb(trajectories_df, to="shape")
    trajectories_df = gpd.GeoDataFrame(trajectories_df, geometry="geometry")
    trajectories_df = trajectories_df.loc[~trajectories_df.is_empty]
    trajectories_df = pd.DataFrame(trajectories_df)
    trajectories_df = update_geometry_from_wkb(trajectories_df, to="wkt")
    upsert(
        df=trajectories_df,
        table=DB_TABLE_TRAJECTORY,
        constraint_name="trajectory_shipment_id_key",
        dtype=({"geometry": Geometry("MULTILINESTRING", 4326)}),
    )


def cluster(ordered_positions, buffer_deg=0.005):
    # buffer_deg=0.005 roughly divide the number of points by 2
    clustered_points = session.query(
        ordered_positions.c.shipment_id,
        ordered_positions.c.date_utc,
        ordered_positions.c.geometry,
        ST_ClusterDBSCAN(ordered_positions.c.geometry, buffer_deg, 1)
        .over(partition_by=ordered_positions.c.shipment_id)
        .label("cluster"),
    ).subquery()

    # Cluster can only happen with consecutive points
    # we force another cluster if this is not the case
    clustered_points2 = session.query(
        clustered_points.c.shipment_id,
        clustered_points.c.geometry,
        clustered_points.c.date_utc,
        sa.case(
            [
                (
                    func.lag(clustered_points.c.cluster).over(
                        partition_by=clustered_points.c.shipment_id,
                        order_by=clustered_points.c.date_utc,
                    )
                    <= clustered_points.c.cluster,
                    clustered_points.c.cluster,
                )
            ],
            else_=-1 * clustered_points.c.cluster,
        ).label("cluster"),
    ).subquery()

    clustered_points3 = (
        session.query(
            clustered_points2.c.shipment_id,
            func.min(clustered_points2.c.date_utc).label("date_utc"),
            ST_Centroid(ST_Union(clustered_points2.c.geometry)).label("geometry"),
        )
        .group_by(clustered_points2.c.shipment_id, clustered_points2.c.cluster)
        .subquery()
    )

    clustered_points4 = (
        session.query(clustered_points3)
        .order_by(clustered_points3.c.shipment_id, clustered_points3.c.date_utc)
        .subquery()
    )

    # text('ST_Multi(st_makeline(geometry ORDER BY date_utc)) as geometry')

    trajectories = session.query(
        clustered_points4.c.shipment_id.label("shipment_id"),
        ST_Multi(ST_MakeLine(clustered_points4.c.geometry)).label("geometry"),
    ).group_by(clustered_points4.c.shipment_id)

    return trajectories


def get_trajectories_over_land(
    date_from=-31, min_land_distance=2, ignore_recent_hours=24, shipment_id=None, commodity=None
):
    if isinstance(date_from, int):
        last_date = session.query(sa.func.max(Arrival.date_utc)).first()[0]
        date_from = to_datetime(last_date) + dt.timedelta(days=date_from)
        date_from = date_from.strftime("%Y-%m-%d")

    with engine.connect() as con:
        data = {
            "date_from": date_from,
            "min_land_distance": min_land_distance,
            "ignore_recent_hours": ignore_recent_hours,
            "commodity": commodity,
        }

        # if not commodity is None:
        #     import json
        #     commodity = json.dumps(to_list(commodity))
        # else:
        #     commodity = 'NULL'

        statement = text(
            """WITH lines as (SELECT t.shipment_id,
                                     sh.commodity,
                            SUM(ST_LENGTH(ST_Intersection(t.geometry, l.geom))) AS length
                            FROM ne_110m_land l, trajectory t
                            LEFT JOIN shipment s ON t.shipment_id = s.id
                            INNER JOIN departure d ON s.departure_id = d.id
                            LEFT JOIN ship sh ON d.ship_imo = sh.imo
                            WHERE ST_Intersects(l.geom, t.geometry)
                            AND (
                                (routing_date IS NULL) OR ((NOW() - routing_date) > ':ignore_recent_hours hours')
                                )
                            AND d.date_utc >= :date_from
                            AND (:commodity IS NULL OR sh.commodity = any(ARRAY[:commodity]))
                            GROUP BY 1,2)

                            SELECT shipment_id FROM lines
                            WHERE length > :min_land_distance
                            ;
                            """
        )

        rs = con.execute(statement, **data)
        overland_shipment_ids = [row[0] for row in rs]
        trajs = Trajectory.query.filter(Trajectory.shipment_id.in_(overland_shipment_ids))
        if shipment_id:
            trajs = trajs.filter(Trajectory.shipment_id.in_(to_list(shipment_id)))

        return trajs.all()


def get_splitted_traj(trajectory_id):
    """
    Return a trajecgtory with its splitted segments,
    as well as the length of each that goes overland.
    :param trajectory_id:
    :return:
    """

    with engine.connect() as con:
        data = {"trajectory_id": trajectory_id}
        statement = text(
            """
                            WITH splitted as (
                                SELECT t.id, t.shipment_id, (ST_Dump(st_union(t.geometry,t.geometry))).*
                                FROM trajectory t
                                WHERE t.id=:trajectory_id
                            ),
                            overlap as (
                                SELECT s.id, s.path, SUM(ST_LENGTH(ST_Intersection(s.geom, l.geom))) AS length
                                FROM ne_110m_land l, splitted s
                                WHERE ST_Intersects(l.geom, s.geom)
                                GROUP BY 1, 2
                            )
                            SELECT s.id, s.shipment_id, s.path, st_astext(s.geom) as geometry, o.length
                            FROM splitted s
                            LEFT JOIN overlap o
                            ON s.id=o.id and s.path=o.path
                            """
        )
        rs = con.execute(statement, **data)

    segments_df = pd.DataFrame(rs)
    segments_df.columns = [
        "trajectory_id",
        "shipment_id",
        "path",
        "geometry",
        "land_distance",
    ]
    import shapely

    segments_df["geometry"] = segments_df.geometry.apply(shapely.wkt.loads)
    return segments_df


def get_routing_cost():
    routing_cost_file = "assets/routing_cost_3857.tif"
    dataset = rio.open(routing_cost_file)
    img = dataset.read(1)
    return dataset, img


def to_3857(coords):
    from pyproj import Proj, Transformer, datadir

    if config("PROJ_DIR", default=None):
        datadir.set_data_dir(config("PROJ_DIR"))

    transformer = Transformer.from_crs(4326, 3857)
    return list(transformer.itransform([[x[1], x[0]] for x in coords]))


def to_4326(coords):
    from pyproj import Proj, Transformer, datadir

    if config("PROJ_DIR", default=None):
        datadir.set_data_dir(config("PROJ_DIR"))

    transformer = Transformer.from_crs(3857, 4326)
    return [[x[1], x[0]] for x in list(transformer.itransform(coords))]


def remove_intermediary_points(coords):
    """
    Route path algorithm will return pixel by pixel route,
    but a lot of them are aligned... removing intermediary points
    :param coords:
    :return:
    """
    from math import isclose

    def lined_up(bef, p, aft):
        if p[1] - bef[1] == 0:
            if aft[1] - p[1] == 0:
                # matching signs
                return p[0] - bef[0] < 0 == p[0] - bef[0] < 0
            else:
                return False
        elif aft[1] - p[1] == 0:
            return False
        else:
            return isclose(
                (p[0] - bef[0]) / (p[1] - bef[1]),
                (aft[0] - p[0]) / (aft[1] - p[1]),
                rel_tol=1e-6,
            )

    def remove_redundant_points(pts):
        # remove redundant end points overlapping with the starting point
        while len(pts) > 1 and pts[-1] == pts[0]:
            pts = pts[:-1]
        return (
            [pts[0]]
            + [p for bef, p, aft in zip(pts[0:-2], pts[1:-1], pts[2:]) if not lined_up(bef, p, aft)]
            + [pts[-1]]
        )

    return remove_redundant_points(coords)


def cut_at_globe_side(route):
    return route


def reroute(
    date_from=-30,
    min_land_distance=2,
    min_segment_land_distance=3,
    max_segment_land_distance=90,  # Use to remove segmenta that cross the +180/-180 line
    shipment_id=None,
    commodity=None,
):
    trajs = get_trajectories_over_land(
        date_from=date_from,
        min_land_distance=min_land_distance,
        shipment_id=shipment_id,
        commodity=commodity,
    )

    dataset, img = get_routing_cost()
    # plt.imshow(img)

    for traj in tqdm(trajs):
        try:
            # Split segments and only deal with overlapping ones
            segments_df = get_splitted_traj(trajectory_id=traj.id)

            # Remove those that cross globe extremity
            for index, segment in segments_df.iterrows():
                coords_4326 = wkb_to_shape(segment.geometry).coords
                if coords_4326[0][0] * coords_4326[1][0] < -120 * 120:
                    # if crossing globe 'extremity' (e.g. Japan to US)
                    segments_df.loc[index, "geometry"] = None
                    continue

            overland_segments_df = segments_df.loc[
                (segments_df.land_distance > min_segment_land_distance)
                & ~segments_df.geometry.isnull()
            ]

            for index, segment in overland_segments_df.iterrows():
                geom = wkb_to_shape(segment.geometry)
                coords_3857 = to_3857(geom.coords)
                coords_4326 = to_4326(coords_3857)
                (x_i, y_i) = coords_3857[0]
                (x_f, y_f) = coords_3857[-1]
                row_i, col_i = dataset.index(x_i, y_i)
                row_f, col_f = dataset.index(x_f, y_f)

                starts = [[row_i, col_i]]
                ends = [[row_f, col_f]]

                # Pass full set of start and end points to `MCP.find_costs`
                # from skimage.graph import _mcp
                # offsets = _mcp.make_offsets(2, True)
                # offsets4 = np.array([[x,y] for x in range(-3,4) for y in range(-3,4) if x != 0 or y != 0])
                # # print(offsets)

                # m = MCP_Geometric(img, fully_connected=True)
                # cost_array, tracebacks_array = m.find_costs(starts, ends)
                # #
                # # # Transpose `ends` so can be used to index in NumPy
                # # ends_idx = tuple(np.asarray(ends).T.tolist())
                # # costs = cost_array[ends_idx]
                # #
                # # # Compute exact minimum cost path to each endpoint
                # tracebacks = [m.traceback(end) for end in ends]
                # tracebacks_3857 = [[dataset.xy(rowcol[0], rowcol[1]) for rowcol in t] for t in tracebacks]
                # tracebacks_4326 = [to_4326(t) for t in tracebacks_3857]
                # #
                # for t in tracebacks_4326:
                #     ax.plot([pt[0] for pt in t],
                #              [pt[1] for pt in t], label='MCP')
                # fig.legend()

                from skimage.graph import route_through_array

                route, weight = route_through_array(img, starts[0], ends[0], geometric=True)
                route_simplified = remove_intermediary_points(coords=route)
                route_3857 = [dataset.xy(rowcol[0], rowcol[1]) for rowcol in route_simplified]
                route_4326 = to_4326(route_3857)

                geometry_routed = LineString(route_4326)

                if False:
                    fig, ax = plt.subplots()
                    ax.plot(
                        [pt[0] for pt in coords_4326],
                        [pt[1] for pt in coords_4326],
                        label="Original",
                    )
                    ax.plot(
                        [pt[0] for pt in route_4326],
                        [pt[1] for pt in route_4326],
                        label="route_through_array",
                    )
                    fig.legend()
                    ax.set_title(traj.shipment_id)
                    fig.show()

                segments_df.loc[index, "geometry"] = geometry_routed

            geometry_routed = MultiLineString(
                segments_df.loc[~segments_df.geometry.isnull()].geometry.to_list()
            )
            traj.geometry_routed = "SRID=4326;" + geometry_routed.wkt
            traj.routing_date = dt.datetime.now()
            session.commit()

        except ValueError as e:
            logger.warning(
                "Failed to reroute traj",
                stack_info=True,
                exc_info=True,
            )
            continue


def push_ne_10m_land():
    # On server only, just run once
    "shp2pgsql -s 4326 ne_110m_land public.ne_110m_land | psql $DB_URL_PRODUCTION"
    return
