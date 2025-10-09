from datetime import datetime, timedelta
from sqlalchemy import func, or_, true, false, literal, tuple_
from model.model import LatestStatus, Stores, Status
from collections import defaultdict


def fetch_store_status_for_report(db):
    cameras_status_result = db.query(
        Stores.id.label("store_id"),
        Stores.name.label("store_name"),
        LatestStatus.camera_no.label("camera_number"),
        literal("not pinging").label("status"),
        LatestStatus.updated_at.label("checked_on")
    ).join(
        Stores, Stores.id == LatestStatus.store_id
    ).filter(
        LatestStatus.current_status == 0,
        LatestStatus.camera_no.is_not(None),
        Stores.region_id.is_not(None),
        Stores.area_id.is_not(None),
        Stores.store_active == 1
    ).all()

    system_status_result = db.query(
        Stores.id.label("store_id"),
        Stores.name.label("store_name"),
        literal("not pinging").label("status"),
        LatestStatus.updated_at.label("checked_on"),
    ).join(
        Stores, Stores.id == LatestStatus.store_id
    ).filter(
        LatestStatus.current_status == 0,
        LatestStatus.camera_no.is_(None),
        Stores.store_active == 1,
        Stores.region_id.is_not(None),
        Stores.area_id.is_not(None)
    ).all()

    offline_stores = [record.store_id for record in system_status_result]
    cameras_status_result = [record for record in cameras_status_result if record.store_id not in offline_stores]
    ids_to_check = [(record.store_id, record.camera_number) for record in cameras_status_result]

    cameras_last_online_result = db.query(
        Status.store_id.label("store_id"),
        Status.camera_no.label("camera_number"),
        func.max(Status.updated_at).label("last_scene")
    ).filter(
        Status.current_status == 1,
        Status.camera_no.is_not(None),
        tuple_(Status.store_id, Status.camera_no).in_(ids_to_check)
    ).group_by(Status.store_id, Status.camera_no).all()

    camera_last_online_dict = defaultdict(lambda: {})
    for record in cameras_last_online_result:
        store_id = record.store_id
        camera_no = record.camera_number
        last_scene = record.last_scene
        camera_last_online_dict[store_id][camera_no] = last_scene

    camera_result = []
    for record in cameras_status_result:
        store_id = record.store_id
        camera_no = record.camera_number
        if store_id in camera_last_online_dict and camera_no in camera_last_online_dict[store_id]:
            last_scene = camera_last_online_dict[store_id][camera_no]
        else:
            last_scene = "Never"

        camera_result.append({
            "Store ID": store_id,
            "Store Name": record.store_name,
            "Camera Number": record.camera_number,
            "Status": record.status,
            "Checked On": record.checked_on,
            "Last Seen": last_scene
        })

    store_ids_to_check = [record.store_id for record in system_status_result]

    system_last_online_result = db.query(
        LatestStatus.store_id.label("store_id"),
        func.max(LatestStatus.last_active).label("last_scene")
    ).filter(
        # LatestStatus.current_status == 1,
        LatestStatus.camera_no.is_(None),
        LatestStatus.store_id.in_(store_ids_to_check)
    ).group_by(LatestStatus.store_id).all()

    system_last_online_dict = {
        record.store_id: record.last_scene for record in system_last_online_result
    }
    system_result = []
    
    for record in system_status_result:
        store_id = record.store_id
        print(store_id)
        if store_id in system_last_online_dict:
            last_scene = system_last_online_dict[store_id]
        else:
            last_scene = "Never"

        system_result.append({
            "Store ID": store_id,
            "Store Name": record.store_name,
            "Status": record.status,
            "Checked On": record.checked_on,
            "Last Seen": last_scene
        })

    return camera_result, system_result


def fetch_camera_status_for_report(db, store_id: int = None, region_id: int = None, area_id: int = None, page: int = 1,
                                   per_page: int = 10):
    result_camera_record = db.query(
        LatestStatus.store_id,
        Stores.name.label("store_name"),
        literal("not pinging").label("status"),
        LatestStatus.updated_at.label("checked_on"),
        LatestStatus.camera_no.label("camera_no")
    ).join(
        Stores, Stores.id == LatestStatus.store_id
    ).filter(
        LatestStatus.current_status == 0,
        LatestStatus.camera_no.is_not(None),
        Stores.region_id.is_not(None),
        Stores.area_id.is_not(None),
        Stores.store_active == 1,
        or_(LatestStatus.store_id == store_id, true() if not store_id else false()),
        or_(Stores.region_id == region_id, true() if not region_id else false()),
        or_(Stores.area_id == area_id, true() if not area_id else false())
    ).order_by(Stores.name).all()

    result_store_record = db.query(
        LatestStatus.store_id,
        Stores.name.label("store_name"),
        literal("not pinging").label("status"),
        LatestStatus.updated_at.label("checked_on"),
    ).join(
        Stores, Stores.id == LatestStatus.store_id
    ).filter(
        LatestStatus.current_status == 0,
        LatestStatus.camera_no.is_(None),
        Stores.region_id.is_not(None),
        Stores.area_id.is_not(None),
        Stores.store_active == 1,
        or_(LatestStatus.store_id == store_id, true() if not store_id else false()),
        or_(Stores.region_id == region_id, true() if not region_id else false()),
        or_(Stores.area_id == area_id, true() if not area_id else false())
    ).order_by(Stores.name).all()

    offline_stores = [record.store_id for record in result_store_record]
    result_camera_record = [record for record in result_camera_record if record.store_id not in offline_stores]

    count = len(result_camera_record)
    result_camera_record = result_camera_record[(page - 1) * per_page: page * per_page]

    stores = set([record.store_id for record in result_camera_record])
    cameras = set([record.camera_no for record in result_camera_record])

    last_seen = db.query(
        Status.store_id, Status.camera_no, func.max(Status.updated_at).label("updated_at")
    ).filter(
        Status.store_id.in_(stores),
        Status.camera_no.in_(cameras),
        Status.current_status == 1
    ).group_by(Status.store_id, Status.camera_no).all()

    last_seen_dict = defaultdict(lambda: defaultdict(str))
    for record in last_seen:
        last_seen_dict[record.store_id][record.camera_no] = record.updated_at

    result_camera = []
    for record in result_camera_record:
        record = dict(record)
        record["last_seen"] = last_seen_dict.get(record["store_id"], {}).get(record["camera_no"], "Never")

        result_camera.append(record)

    result_camera = sorted(result_camera, key=lambda record: record["store_name"])

    return result_camera, count


def fetch_system_status_for_report(db, store_id: int = None, region_id: int = None, area_id: int = None, page: int = 1,
                                   per_page: int = 10):
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(minutes=5)

    result_store_record = db.query(
        LatestStatus.store_id,
        Stores.name.label("store_name"),
        literal("not pinging").label("status"),
        LatestStatus.updated_at.label("checked_on"),
    ).join(
        Stores, Stores.id == LatestStatus.store_id
    ).filter(
        LatestStatus.current_status == 0,
        LatestStatus.camera_no.is_(None),
        Stores.region_id.is_not(None),
        Stores.area_id.is_not(None),
        Stores.store_active == 1,
        or_(LatestStatus.store_id == store_id, true() if not store_id else false()),
        or_(Stores.region_id == region_id, true() if not region_id else false()),
        or_(Stores.area_id == area_id, true() if not area_id else false())
    ).order_by(Stores.name).all()

    count = len(result_store_record)
    result_store_record = result_store_record[(page - 1) * per_page: page * per_page]
    stores = set([record.store_id for record in result_store_record])

    last_seen = db.query(
        Status.store_id, func.max(Status.updated_at).label("updated_at")
    ).filter(
        Status.store_id.in_(stores),
        Status.camera_no.is_(None),
        Status.current_status == 1,
        Status.updated_at <= start_time
    ).group_by(Status.store_id).all()

    last_seen_dict = defaultdict(lambda: defaultdict(str))
    for record in last_seen:
        last_seen_dict[record.store_id] = record.updated_at

    result_store = []
    for record in result_store_record:
        record = dict(record)
        record["last_seen"] = last_seen_dict.get(record["store_id"], "Never")
        result_store.append(record)

    return result_store, count