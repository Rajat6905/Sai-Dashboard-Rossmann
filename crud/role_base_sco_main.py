from sqlalchemy import func, and_, distinct, desc, or_, true, false, asc, extract, text
from model.model import Transactions, Stores, Transaction_items, Outage,Sources, Operators, Comments
from sqlalchemy.orm import Session, aliased
from datetime import datetime, timedelta
import itertools
import pandas as pd
from utils import (current_date_time,
                   get_last_3_months_from_current_date,
                   add_values_stats,
                   sort_by_year_month,
                   sort_by_year_month_week,
                   sort_by_year_month_day,
                   merge_dicts)
from operator import itemgetter
from utils import all_store_count_aisle, all_store_count_sco_main

def store_region_area_wise_data_sco_main(db, type, store_id, region_id, area_id, start_time, end_time):
    if start_time == None or start_time == "":
        start_time, end_time = current_date_time()

    print(start_time, end_time)

    Transactions1 = aliased(Transactions)
    Transaction_items1 = aliased(Transaction_items)
    # ----------------- For Region -------------------
    if type == "admin":
        in_region_theft_count = db.query(Stores.region_id.label("value"),
                                         func.count(distinct(Transactions1.transaction_id)).label(
                                             'count'),
                                         func.count(distinct(Stores.name)).label('store_count')
                                         ).select_from(Transactions1) \
            .join(Stores, or_(Stores.region_id == region_id, true() if region_id is None else false())) \
            .filter(Transactions1.store_id == Stores.id) \
            .filter(Transactions1.source_id==2,Transactions1.hidden == 0, Transactions1.missed_scan == 1) \
            .filter(func.date(Transactions1.begin_date) >= start_time) \
            .filter(func.date(Transactions1.begin_date) <= end_time) \
            .group_by(Stores.region_id).order_by(
            desc('count'))

        in_region_theft_price = db.query(Stores.region_id.label("value"),
                                         func.count(distinct(Transactions1.transaction_id)).label(
                                             'count'),
                                         func.ifnull(
                                             func.sum(
                                                 Transaction_items1.regular_sales_unit_price * Transaction_items1.quantity),
                                             0).label(
                                             'Total')
                                         ).select_from(Transactions1) \
            .join(Stores, or_(Stores.region_id == region_id, true() if region_id is None else false())) \
            .filter(
            Transactions1.store_id == Stores.id) \
            .join(Transaction_items1,
                  and_(Transaction_items1.transaction_id == Transactions1.transaction_id, Transaction_items1.missed == 1,
                       Transactions1.hidden == 0, Transactions1.missed_scan == 1, Transactions1.source_id==2)) \
            .filter(func.date(Transactions1.begin_date) >= start_time) \
            .filter(func.date(Transactions1.begin_date) <= end_time) \
            .group_by(Stores.region_id).order_by(
            desc('count'))
        in_region_theft_count = in_region_theft_count.all()
        in_region_theft_price = in_region_theft_price.all()
        in_region_theft = [(*count, price[-1]) for count, price in zip(in_region_theft_count, in_region_theft_price)]
        keys_in_region_theft = ["value","count", "store_count", "Total"]
        in_region_theft = [dict(zip(keys_in_region_theft, tup)) for tup in in_region_theft]
        in_region_theft = all_store_count_sco_main(type=type, data=in_region_theft, db=db, store_id=store_id, region_id=region_id, area_id=area_id)

        # eeee
        in_region_non_theft_count = db.query(Stores.region_id.label("value"),
                                             func.count(distinct(Transactions1.transaction_id)).label(
                                                 'count'),
                                             func.count(distinct(Stores.name)).label('store_count')
                                             ).select_from(Transactions1) \
            .join(Stores, or_(Stores.region_id == region_id, true() if region_id is None else false())) \
            .filter(Transactions1.store_id == Stores.id) \
            .filter(Transactions1.source_id==1,Transactions1.hidden == 0, Transactions1.missed_scan == 1) \
            .filter(func.date(Transactions1.begin_date) >= start_time) \
            .filter(func.date(Transactions1.begin_date) <= end_time) \
            .group_by(Stores.region_id).order_by(
            asc('count'))

        in_region_non_theft_price = db.query(Stores.region_id.label("value"),
                                             func.count(distinct(Transactions1.transaction_id)).label(
                                                 'count'),
                                             func.ifnull(
                                                 func.sum(
                                                     Transaction_items1.regular_sales_unit_price * Transaction_items1.quantity),
                                                 0).label(
                                                 'Total')
                                             ).select_from(Transactions1) \
            .join(Stores, or_(Stores.region_id == region_id, true() if region_id is None else false())) \
            .filter(
            Transactions1.store_id == Stores.id) \
            .join(Transaction_items1,
                  and_(Transaction_items1.transaction_id == Transactions1.transaction_id, Transaction_items1.missed == 1,
                       Transactions1.hidden == 0, Transactions1.missed_scan == 1, Transactions1.source_id==1)) \
            .filter(func.date(Transactions1.begin_date) >= start_time) \
            .filter(func.date(Transactions1.begin_date) <= end_time) \
            .group_by(Stores.region_id).order_by(
            asc('count'))

        in_region_non_theft_count = in_region_non_theft_count.all()
        in_region_non_theft_price = in_region_non_theft_price.all()
        in_region_non_theft = [(*count, price[-1]) for count, price in
                               zip(in_region_non_theft_count, in_region_non_theft_price)]
        keys_in_region_theft = ["value","count",'store_count', "Total"]
        in_region_non_theft = [dict(zip(keys_in_region_theft, tup)) for tup in in_region_non_theft]
        in_region_non_theft = all_store_count_sco_main(type=type, data=in_region_non_theft, db=db, store_id=store_id,
                                                   region_id=region_id, area_id=area_id)

        return {"data_theft": in_region_theft, "data_non_theft": in_region_non_theft, "type": type}

    # ----------------- For Area -------------------
    elif type == "region":
        in_region_theft_count = db.query(Stores.area_id.label("value"),
                                         # Stores.area_id.label("area_id"),
                                         func.count(distinct(Transactions1.transaction_id)).label(
                                             'count'),
                                         func.count(distinct(Stores.name)).label('store_count')
                                         ).select_from(Transactions1) \
            .join(Stores, or_(Stores.region_id == region_id, true() if region_id is None else false())) \
            .filter(Transactions1.store_id == Stores.id) \
            .filter(Transactions1.source_id==2,Transactions1.hidden == 0, Transactions1.missed_scan == 1) \
            .filter(func.date(Transactions1.begin_date) >= start_time) \
            .filter(func.date(Transactions1.begin_date) <= end_time) \
            .group_by(Stores.area_id).order_by(
            desc('count'))

        in_region_theft_price = db.query(Stores.area_id.label("value"),
                                         # Stores.area_id.label("area_id"),
                                         func.count(distinct(Transactions1.transaction_id)).label(
                                             'count'),
                                         func.ifnull(
                                             func.sum(
                                                 Transaction_items1.regular_sales_unit_price * Transaction_items1.quantity),
                                             0).label(
                                             'Total')
                                         ).select_from(Transactions1) \
            .join(Stores, or_(Stores.region_id == region_id, true() if region_id is None else false())) \
            .filter(
            Transactions1.store_id == Stores.id) \
            .join(Transaction_items1,
                  and_(Transaction_items1.transaction_id == Transactions1.transaction_id,
                       Transaction_items1.missed == 1,
                       Transactions1.hidden == 0, Transactions1.missed_scan == 1, Transactions1.source_id==2)) \
            .filter(func.date(Transactions1.begin_date) >= start_time) \
            .filter(func.date(Transactions1.begin_date) <= end_time) \
            .group_by(Stores.area_id).order_by(
            desc('count'))
        in_region_theft_count = in_region_theft_count.all()
        in_region_theft_price = in_region_theft_price.all()
        in_region_theft = [(*count, price[-1]) for count, price in zip(in_region_theft_count, in_region_theft_price)]
        keys_in_region_theft = ["value", "count",'store_count', "Total"]
        in_region_theft = [dict(zip(keys_in_region_theft, tup)) for tup in in_region_theft]
        in_region_theft = all_store_count_sco_main(type=type, data=in_region_theft, db=db, store_id=store_id,
                                                   region_id=region_id, area_id=area_id)


        # eeee
        in_region_non_theft_count = db.query(Stores.area_id.label("value"),
                                             # Stores.area_id.label("area_id"),
                                             func.count(distinct(Transactions1.transaction_id)).label(
                                                 'count'),
                                             func.count(distinct(Stores.name)).label('store_count')
                                             ).select_from(Transactions1) \
            .join(Stores, or_(Stores.region_id == region_id, true() if region_id is None else false())) \
            .filter(Transactions1.store_id == Stores.id) \
            .filter(Transactions1.source_id==1, Transactions1.hidden == 0, Transactions1.missed_scan == 1) \
            .filter(func.date(Transactions1.begin_date) >= start_time) \
            .filter(func.date(Transactions1.begin_date) <= end_time) \
            .group_by(Stores.area_id).order_by(
            asc('count'))

        in_region_non_theft_price = db.query(Stores.area_id.label("value"),
                                             # Stores.area_id.label("area_id"),
                                             func.count(distinct(Transactions1.transaction_id)).label(
                                                 'count'),
                                             func.ifnull(
                                                 func.sum(
                                                     Transaction_items1.regular_sales_unit_price * Transaction_items1.quantity),
                                                 0).label(
                                                 'Total')
                                             ).select_from(Transactions1) \
            .join(Stores, or_(Stores.region_id == region_id, true() if region_id is None else false())) \
            .filter(
            Transactions1.store_id == Stores.id) \
            .join(Transaction_items1,
                  and_(Transaction_items1.transaction_id == Transactions1.transaction_id,
                       Transaction_items1.missed == 1,
                       Transactions1.hidden == 0, Transactions1.missed_scan == 1, Transactions1.source_id==1)) \
            .filter(func.date(Transactions1.begin_date) >= start_time) \
            .filter(func.date(Transactions1.begin_date) <= end_time) \
            .group_by(Stores.area_id).order_by(
            asc('count'))

        in_region_non_theft_count = in_region_non_theft_count.all()
        in_region_non_theft_price = in_region_non_theft_price.all()
        in_region_non_theft = [(*count, price[-1]) for count, price in
                               zip(in_region_non_theft_count, in_region_non_theft_price)]
        keys_in_region_theft = ["value", "count",'store_count' ,"Total"]
        in_region_non_theft = [dict(zip(keys_in_region_theft, tup)) for tup in in_region_non_theft]
        in_region_non_theft = all_store_count_sco_main(type=type, data=in_region_non_theft, db=db, store_id=store_id,
                                                   region_id=region_id, area_id=area_id)

        return {"data_theft":in_region_theft, "data_non_theft": in_region_non_theft,  "type":type}

    elif type == "area":
        in_region_theft_count = db.query(Stores.name.label("value"),
                                         # Stores.id.label("store_id"),
                                         func.count(distinct(Transactions1.transaction_id)).label(
                                             'count'),
                                         func.count(distinct(Stores.name)).label('store_count')
                                         ).select_from(Transactions1) \
            .join(Stores, or_(Stores.area_id == area_id, true() if area_id is None else false())) \
            .filter(Transactions1.store_id == Stores.id) \
            .filter(Transactions1.source_id==2,Transactions1.hidden == 0, Transactions1.missed_scan == 1) \
            .filter(func.date(Transactions1.begin_date) >= start_time) \
            .filter(func.date(Transactions1.begin_date) <= end_time) \
            .group_by(Stores.area_id, Stores.id).order_by(
            desc('count'))

        in_region_theft_price = db.query(Stores.name.label("value"),
                                         # Stores.id.label("store_id"),
                                         func.count(distinct(Transactions1.transaction_id)).label(
                                             'count'),
                                         func.ifnull(
                                             func.sum(
                                                 Transaction_items1.regular_sales_unit_price * Transaction_items1.quantity),
                                             0).label(
                                             'Total')
                                         ).select_from(Transactions1) \
            .join(Stores, or_(Stores.area_id == area_id, true() if area_id is None else false())) \
            .filter(
            Transactions1.store_id == Stores.id) \
            .join(Transaction_items1,
                  and_(Transaction_items1.transaction_id == Transactions1.transaction_id,
                       Transaction_items1.missed == 1,
                       Transactions1.hidden == 0, Transactions1.missed_scan == 1,Transactions1.source_id==2)) \
            .filter(func.date(Transactions1.begin_date) >= start_time) \
            .filter(func.date(Transactions1.begin_date) <= end_time) \
            .group_by(Stores.area_id, Stores.id).order_by(
            desc('count'))
        in_region_theft_count = in_region_theft_count.all()
        in_region_theft_price = in_region_theft_price.all()
        in_region_theft = [(*count, price[-1]) for count, price in zip(in_region_theft_count, in_region_theft_price)]
        keys_in_region_theft = ["value","count",'store_count', "Total"]
        in_region_theft = [dict(zip(keys_in_region_theft, tup)) for tup in in_region_theft]

        # eeee
        in_region_non_theft_count = db.query(Stores.name.label("value"),
                                             # Stores.id.label("store_id"),
                                             func.count(distinct(Transactions1.transaction_id)).label(
                                                 'count'),
                                             func.count(distinct(Stores.name)).label('store_count')
                                             ).select_from(Transactions1) \
            .join(Stores, or_(Stores.area_id == area_id, true() if area_id is None else false())) \
            .filter(Transactions1.store_id == Stores.id) \
            .filter(Transactions1.source_id == 1,Transactions1.hidden == 0, Transactions1.missed_scan == 1) \
            .filter(func.date(Transactions1.begin_date) >= start_time) \
            .filter(func.date(Transactions1.begin_date) <= end_time) \
            .group_by(Stores.area_id, Stores.id).order_by(
            asc('count'))

        in_region_non_theft_price = db.query(Stores.name.label("value"),
                                             # Stores.id.label("store_id"),
                                             func.count(distinct(Transactions1.transaction_id)).label(
                                                 'count'),
                                             func.ifnull(
                                                 func.sum(
                                                     Transaction_items1.regular_sales_unit_price * Transaction_items1.quantity),
                                                 0).label(
                                                 'Total')
                                             ).select_from(Transactions1) \
            .join(Stores, or_(Stores.area_id == area_id, true() if area_id is None else false())) \
            .filter(
            Transactions1.store_id == Stores.id) \
            .join(Transaction_items1,
                  and_(Transactions1.source_id == 1, Transaction_items1.transaction_id == Transactions1.transaction_id,
                       Transaction_items1.missed == 1,
                       Transactions1.hidden == 0, Transactions1.missed_scan == 1)) \
            .filter(func.date(Transactions1.begin_date) >= start_time) \
            .filter(func.date(Transactions1.begin_date) <= end_time) \
            .group_by(Stores.area_id, Stores.id).order_by(
            asc('count'))

        in_region_non_theft_count = in_region_non_theft_count.all()
        in_region_non_theft_price = in_region_non_theft_price.all()
        in_region_non_theft = [(*count, price[-1]) for count, price in
                               zip(in_region_non_theft_count, in_region_non_theft_price)]
        keys_in_region_theft = ["value", "count", 'store_count', "Total"]
        in_region_non_theft = [dict(zip(keys_in_region_theft, tup)) for tup in in_region_non_theft]

        return {"data_theft": in_region_theft, "data_non_theft": in_region_non_theft, "type": type}

    else:
        return {"message": "Invalid Type"}


def store_region_area_wise_data_aisle(db, type, store_id, region_id, area_id, start_time, end_time):
    if start_time == None or start_time == "":
        start_time, end_time = current_date_time()

    if type == "admin":
        in_region = db.query(Stores.region_id.label("value"),
                                         func.count(distinct(Transactions.transaction_id)).label('count'),
                                         func.sum(Transactions.bag_price).label('aisle_total'),
                                         func.count(distinct(Stores.name)).label('store_count')
                                         ).select_from(Transactions) \
            .join(Stores, or_(Stores.region_id == region_id, true() if region_id is None else false())) \
            .filter(or_(Stores.id == store_id, true() if store_id is None else false()),
                    or_(Stores.region_id == region_id, true() if region_id is None else false()),
                    or_(Stores.area_id == area_id, true() if area_id is None else false())
                    )\
            .filter(
                    Transactions.store_id == Stores.id,
                    Transactions.source_id == 3,
                    Transactions.hidden == 0,
                    Transactions.triggers == 1,
                    func.date(Transactions.begin_date) >= start_time,
                    func.date(Transactions.begin_date) <= end_time
                    ) \
            .group_by(Stores.region_id)
        in_region_theft = in_region.order_by(desc('count')).all()
        print(in_region_theft)
        # print(in_region_theft[0]._asdict())
        in_region_theft = all_store_count_aisle(type=type, data=in_region_theft, db=db, store_id=store_id, region_id=region_id, area_id=area_id)

        in_region_non =db.query(Stores.region_id.label("value"),
                                         func.count(distinct(Transactions.transaction_id)).label('count'),
                                         func.sum(Transactions.bag_price).label('aisle_total'),
                                         func.count(distinct(Stores.name)).label('store_count')
                                         ).select_from(Transactions) \
            .join(Stores, or_(Stores.region_id == region_id, true() if region_id is None else false())) \
            .filter(or_(Stores.id == store_id, true() if store_id is None else false()),
                    or_(Stores.region_id == region_id, true() if region_id is None else false()),
                    or_(Stores.area_id == area_id, true() if area_id is None else false())
                    )\
            .filter(
                    Transactions.store_id == Stores.id,
                    Transactions.source_id == 3,
                    Transactions.hidden == 0,
                    Transactions.triggers == 1,
                    Transactions.intervention == 1,
                    func.date(Transactions.begin_date) >= start_time,
                    func.date(Transactions.begin_date) <= end_time
                    ) \
            .group_by(Stores.region_id)
        in_region_non_theft = in_region_non.order_by(desc('count')).all()
        in_region_non_theft = all_store_count_aisle(type=type, data=in_region_non_theft, db=db, store_id=store_id, region_id=region_id,
                        area_id=area_id)
        return {"data_aisle_theft": in_region_theft, "data_aisle_non_theft":in_region_non_theft, "type":type}

    elif type == "region":
        in_region = db.query(Stores.area_id.label("value"),
                             func.count(distinct(Transactions.transaction_id)).label('count'),
                             func.sum(Transactions.bag_price).label('aisle_total'),
                             func.count(distinct(Stores.name)).label('store_count')
                             ).select_from(Transactions) \
            .join(Stores, or_(Stores.region_id == region_id, true() if region_id is None else false())) \
            .filter(or_(Stores.id == store_id, true() if store_id is None else false()),
                    or_(Stores.region_id == region_id, true() if region_id is None else false()),
                    or_(Stores.area_id == area_id, true() if area_id is None else false())
                    )\
            .filter(
            Transactions.store_id == Stores.id,
            Transactions.source_id == 3,
            Transactions.hidden == 0,
            Transactions.triggers == 1,
            func.date(Transactions.begin_date) >= start_time,
            func.date(Transactions.begin_date) <= end_time
        ) \
            .group_by(Stores.area_id)
        in_region_theft = in_region.order_by(desc('count')).all()
        in_region_theft = all_store_count_aisle(type=type, data=in_region_theft, db=db, store_id=store_id,
                                                    region_id=region_id,
                                                    area_id=area_id)


        in_region_non = db.query(Stores.area_id.label("value"),
                             func.count(distinct(Transactions.transaction_id)).label('count'),
                             func.sum(Transactions.bag_price).label('aisle_total'),
                             func.count(distinct(Stores.name)).label('store_count')
                             ).select_from(Transactions) \
            .join(Stores, or_(Stores.region_id == region_id, true() if region_id is None else false())) \
            .filter(or_(Stores.id == store_id, true() if store_id is None else false()),
                    or_(Stores.region_id == region_id, true() if region_id is None else false()),
                    or_(Stores.area_id == area_id, true() if area_id is None else false())
                    )\
            .filter(
            Transactions.store_id == Stores.id,
            Transactions.source_id == 3,
            Transactions.hidden == 0,
            Transactions.triggers == 1,
            Transactions.intervention == 1,
            func.date(Transactions.begin_date) >= start_time,
            func.date(Transactions.begin_date) <= end_time
        ) \
            .group_by(Stores.area_id)
        in_region_non_theft = in_region_non.order_by(desc('count')).all()
        in_region_non_theft = all_store_count_aisle(type=type, data=in_region_non_theft, db=db, store_id=store_id,
                                                region_id=region_id,
                                                area_id=area_id)
        return {"data_aisle_theft": in_region_theft, "data_aisle_non_theft": in_region_non_theft, "type":type}

    elif type == "area":
        in_region = db.query(Stores.name.label("value"),
                             Stores.area_id.label("area_id"),
                             func.count(distinct(Transactions.transaction_id)).label('count'),
                             func.sum(Transactions.bag_price).label('aisle_total'),
                             func.count(distinct(Stores.name)).label('store_count')
                             ).select_from(Transactions) \
            .join(Stores, or_(Stores.area_id == area_id, true() if area_id is None else false())) \
            .filter(or_(Stores.id == store_id, true() if store_id is None else false()),
                    or_(Stores.region_id == region_id, true() if region_id is None else false()),
                    or_(Stores.area_id == area_id, true() if area_id is None else false())
                    )\
            .filter(
            Transactions.store_id == Stores.id,
            Transactions.source_id == 3,
            Transactions.hidden == 0,
            Transactions.triggers == 1,
            func.date(Transactions.begin_date) >= start_time,
            func.date(Transactions.begin_date) <= end_time
        ) \
            .group_by(Stores.area_id, Stores.id)
        in_region_theft = in_region.order_by(desc('count')).all()
        from pprint import pprint
        pprint(in_region_theft)
        in_region_theft = all_store_count_aisle(type=type, data=in_region_theft, db=db, store_id=store_id,
                                                    region_id=region_id,
                                                    area_id=area_id)

        in_region_non = db.query(Stores.name.label("value"),
                                 Stores.area_id.label("area_id"),
                             func.count(distinct(Transactions.transaction_id)).label('count'),
                             func.sum(Transactions.bag_price).label('aisle_total'),
                             func.count(distinct(Stores.name)).label('store_count')
                             ).select_from(Transactions) \
            .join(Stores, or_(Stores.area_id == area_id, true() if area_id is None else false())) \
            .filter(or_(Stores.id == store_id, true() if store_id is None else false()),
                    or_(Stores.region_id == region_id, true() if region_id is None else false()),
                    or_(Stores.area_id == area_id, true() if area_id is None else false())
                    )\
            .filter(
            Transactions.store_id == Stores.id,
            Transactions.source_id == 3,
            Transactions.hidden == 0,
            Transactions.triggers == 1,
            Transactions.intervention == 1,
            func.date(Transactions.begin_date) >= start_time,
            func.date(Transactions.begin_date) <= end_time
        ) \
            .group_by(Stores.area_id, Stores.id)
        in_region_non_theft = in_region_non.order_by(desc('count')).all()
        return {"data_aisle_theft": in_region_theft, "data_aisle_non_theft": in_region_non_theft, "type": type}

    else:
        return {"message": "Invalid Type"}