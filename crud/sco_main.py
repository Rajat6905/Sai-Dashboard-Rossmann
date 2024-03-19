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
                   merge_dicts,
                   all_store_count_list_of_sco_main)
from operator import itemgetter

def get_count_sco_main(db: Session, store_id, region_id, area_id, start_time, end_time):
    if start_time == None or start_time == "":
        start_time, end_time = current_date_time()

    main_count = db.query(
        func.count(distinct(Transactions.transaction_id)).label("main_count"))\
        .join(Stores, Transactions.store_id == Stores.id).filter(
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.region_id == region_id, true() if region_id is None else false()),
        or_(Stores.area_id == area_id, true() if area_id is None else false())
        ).filter(Transactions.source_id == 1, Transactions.hidden == 0, Transactions.missed_scan == 1) \
        .filter(func.date(Transactions.begin_date) >= start_time) \
        .filter(func.date(Transactions.begin_date) <= end_time) \
        .first()

    main_price = db.query(
        func.count(distinct(Transactions.transaction_id)).label("main_count"),
        func.ifnull(func.sum(Transaction_items.regular_sales_unit_price * Transaction_items.quantity), 0).label(
            'main_total')).join(Stores, Transactions.store_id == Stores.id).filter(
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.region_id == region_id, true() if region_id is None else false()),
        or_(Stores.area_id == area_id, true() if area_id is None else false())
        ).join(Transaction_items,
               and_(Transaction_items.transaction_id == Transactions.transaction_id, Transaction_items.missed == 1)) \
        .filter(Transactions.source_id == 1, Transactions.hidden == 0, Transactions.missed_scan == 1) \
        .filter(func.date(Transactions.begin_date) >= start_time) \
        .filter(func.date(Transactions.begin_date) <= end_time) \
        .first()

    sco_count = db.query(
        func.count(distinct(Transactions.transaction_id)).label("sco_count"))\
        .join(Stores, Transactions.store_id == Stores.id).filter(
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.region_id == region_id, true() if region_id is None else false()),
        or_(Stores.area_id == area_id, true() if area_id is None else false())
        ).filter(Transactions.source_id == 2, Transactions.hidden == 0, Transactions.missed_scan == 1) \
        .filter(func.date(Transactions.begin_date) >= start_time) \
        .filter(func.date(Transactions.begin_date) <= end_time) \
        .first()

    sco_price = db.query(func.count(distinct(Transactions.transaction_id)).label('sco_count'),
                   func.ifnull(func.sum(Transaction_items.regular_sales_unit_price * Transaction_items.quantity),
                               0).label(
                       'sco_total')).join(Stores, Transactions.store_id == Stores.id).filter(
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.region_id == region_id, true() if region_id is None else false()),
        or_(Stores.area_id == area_id, true() if area_id is None else false())
        ).join(Transaction_items,
               and_(Transaction_items.transaction_id == Transactions.transaction_id, Transaction_items.missed == 1)) \
        .filter(Transactions.source_id == 2, Transactions.hidden == 0, Transactions.missed_scan == 1) \
        .filter(func.date(Transactions.begin_date) >= start_time) \
        .filter(func.date(Transactions.begin_date) <= end_time) \
        .first()

    return {"main_count": int(main_count.main_count), "main_total": int(main_price.main_total), "sco_count": int(sco_count.sco_count),
            "sco_total": int(sco_price.sco_total)}


def get_main_count_data(db: Session, store_id, region_id, area_id, type, start_time, end_time, page, seen_status):
    if start_time == None or start_time == "":
        start_time, end_time = current_date_time()
    per_page = 10
    main_sco_data = db.query(Stores.name.label("store"), Transactions.transaction_id,Transactions.sequence_no,Transactions.counter_no,Transactions.staffcard, Operators.operator_id,
             Transactions.description,Transactions.missed_scan,Transactions.video_link, Transactions.begin_date)\
        .filter(
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.region_id == region_id, true() if region_id is None else false()),
        or_(Stores.area_id == area_id, true() if area_id is None else false())
        )\
        .join(Stores, Transactions.store_id == Stores.id) \
        .join(Operators, Transactions.operator_id == Operators.id)        \
        .filter(Transactions.source_id == type, Transactions.hidden == 0, Transactions.description != "", Transactions.missed_scan == 1, Transactions.seen_status==seen_status)\
        .filter(func.date(Transactions.begin_date) >= start_time) \
        .filter(func.date(Transactions.begin_date) <= end_time) \
        .order_by(desc(func.date(Transactions.begin_date)))
        # .limit(2).offset((page - 1) * per_page) # For pagination
        # .all()
    # main_sco_data.paginate(page, 10, error_out=False)
    count = {"total_count": main_sco_data.count()}
    main_sco_data = main_sco_data.order_by(desc(Transactions.begin_date))
    return main_sco_data.limit(10).offset((page - 1) * per_page).all(), count


def get_transaction_data(db: Session, transaction_id):

    info = db.query(Stores.name.label("store"), Transactions.transaction_id, Transactions.sequence_no,
                    Transactions.counter_no, Transactions.operator_id, Transactions.operator_id,
                    Transactions.description, Transactions.missed_scan, Transactions.video_link, Transactions.bag_price,
                    Transactions.begin_date, Transactions.first_item_at).filter(Transactions.transaction_id == transaction_id).join(Stores,
                                                                                            Transactions.store_id == Stores.id).all()



    items = db.query(Transaction_items.name, Transaction_items.quantity, Transaction_items.missed, Transaction_items.id,
                    Transaction_items.begin_date_time, Transaction_items.transaction_type, Transaction_items.regular_sales_unit_price).select_from(Transactions).join(Transaction_items,
                                                                                      Transaction_items.transaction_id == Transactions.transaction_id) \
        .filter(Transactions.transaction_id == transaction_id).order_by(
        asc('begin_date_time')).all()
    # db.query(Transactions).filter_by(transaction_id=transaction_id).update({Transactions.seen_status: 1})
    # db.commit()
    return {"info":info,"items":items}



def get_drop_down_info_stores(db: Session, region_id, area_id):
    # for value in db.query(Stores.name).distinct():
    #     print(value)
    store = db.query(Stores.id, Stores.name)\
        .filter(or_(Stores.region_id == region_id, true() if region_id is None else false()),
                or_(Stores.area_id == area_id, true() if area_id is None else false())).order_by(asc(Stores.name)).all()
    region_data = db.query(Stores.name, func.group_concat(Stores.region_id)).group_by(Stores.name).all()
    region = db.query(distinct(Stores.region_id).label("region")).all()
    area = db.query(distinct(Stores.area_id).label("area")).all()
    # return {"store": store, "region": region, "area": area}
    print(region_data)
    return {"store_data": store}


def get_drop_down_info_region(db: Session, store_id, area_id):
    # if store_name:
        # region = db.query(Stores.region_id).filter(Stores.name == store_name).all()
    region = db.query(Stores.region_id).filter(
        # or_(Stores.name == store_name, true() if store_name is None else false()),
        or_(Stores.id == store_id, true() if store_id is None else false()),
        or_(Stores.area_id == area_id, true() if area_id is None else false())).all()
    region = list(set(list(itertools.chain(*region))))
    region.sort()
    if len(region) > 0:
        return {"region": region}
    else:
        return {"message": "No Data Found!!"}
    return {"region": region}

def get_drop_down_info_area(db: Session, store_id, region_id):
    # area = db.query(Stores.area_id)#.filter(Stores.name == store_name, Stores.region_id == region).all()
    area = db.query(Stores.area_id).filter(
        # or_(Stores.name == store_name, true() if store_name is None else false()),
        or_(Stores.id == store_id, true() if store_id is None else false()),
        or_(Stores.region_id == region_id, true() if region_id is None else false()),)
    area = sorted(list(set(itertools.chain(*area))))
    return {"area":area}



def get_top5_store_area(db: Session, area_id, start_time, end_time):
    if start_time == None or start_time == "":
        start_time, end_time = current_date_time()

    Transactions1 = aliased(Transactions)
    Transaction_items1 = aliased(Transaction_items)
    in_area_theft_count = db.query(Stores.name,
                             func.count(distinct(Transactions1.transaction_id)).label(
                                 'count')
                             ).select_from(Transactions1) \
        .join(Stores, or_(Stores.area_id == area_id, true() if area_id is None else false()))\
        .filter(Transactions1.store_id == Stores.id) \
        .filter(Transactions1.hidden == 0, Transactions1.missed_scan == 1) \
        .filter(func.date(Transactions1.begin_date) >= start_time) \
        .filter(func.date(Transactions1.begin_date) <= end_time) \
        .group_by(Transactions1.store_id).order_by(
        desc('count')).limit(5)

    in_area_theft_price = db.query(Stores.name,
                             func.count(distinct(Transactions1.transaction_id)).label(
                                 'count'),
                             func.ifnull(
                                 func.sum(Transaction_items1.regular_sales_unit_price * Transaction_items1.quantity),
                                 0).label(
                                 'Total')
                             ).select_from(Transactions1) \
        .join(Stores, or_(Stores.area_id == area_id, true() if area_id is None else false())).filter(
        Transactions1.store_id == Stores.id) \
        .join(Transaction_items1,
              and_(Transaction_items1.transaction_id == Transactions1.transaction_id, Transaction_items1.missed == 1,
                   Transactions1.hidden == 0, Transactions1.missed_scan == 1)) \
        .filter(func.date(Transactions1.begin_date) >= start_time) \
        .filter(func.date(Transactions1.begin_date) <= end_time) \
        .group_by(Transactions1.store_id).order_by(
        desc('count')).limit(5)
    in_area_theft_count = in_area_theft_count.all()
    in_area_theft_price = in_area_theft_price.all()
    in_area_theft = [(*count, price[-1]) for count, price in zip(in_area_theft_count, in_area_theft_price)]
    keys_in_area_theft = ["name", "count", "Total"]
    in_area_theft = [dict(zip(keys_in_area_theft, tup)) for tup in in_area_theft]


    in_area_non_theft_count = db.query(Stores.name,
                             func.count(distinct(Transactions1.transaction_id)).label(
                                 'count')
                             ).select_from(Transactions1) \
        .join(Stores, or_(Stores.area_id == area_id, true() if area_id is None else false()))\
        .filter(Transactions1.store_id == Stores.id) \
        .filter(Transactions1.hidden == 0, Transactions1.missed_scan == 1) \
        .filter(func.date(Transactions1.begin_date) >= start_time) \
        .filter(func.date(Transactions1.begin_date) <= end_time) \
        .group_by(Transactions1.store_id).order_by(
        asc('count')).limit(5)

    in_area_non_theft_price = db.query(Stores.name,
                             func.count(distinct(Transactions1.transaction_id)).label(
                                 'count'),
                             func.ifnull(
                                 func.sum(Transaction_items1.regular_sales_unit_price * Transaction_items1.quantity),
                                 0).label(
                                 'Total')
                             ).select_from(Transactions1) \
        .join(Stores, or_(Stores.area_id == area_id, true() if area_id is None else false())).filter(
        Transactions1.store_id == Stores.id) \
        .join(Transaction_items1,
              and_(Transaction_items1.transaction_id == Transactions1.transaction_id, Transaction_items1.missed == 1,
                   Transactions1.hidden == 0, Transactions1.missed_scan == 1)) \
        .filter(func.date(Transactions1.begin_date) >= start_time) \
        .filter(func.date(Transactions1.begin_date) <= end_time) \
        .group_by(Transactions1.store_id).order_by(
        asc('count')).limit(5)

    in_area_non_theft_count = in_area_non_theft_count.all()
    in_area_non_theft_price = in_area_non_theft_price.all()
    in_area_non_theft = [(*count, price[-1]) for count, price in zip(in_area_non_theft_count, in_area_non_theft_price)]
    keys_in_area_theft = ["name", "count", "Total"]
    in_area_non_theft = [dict(zip(keys_in_area_theft, tup)) for tup in in_area_non_theft]

    return {"top5_store_in_area_theft": in_area_theft, "top5_store_in_area_non_theft": in_area_non_theft}


def get_top5_store_region(db: Session, region_id, start_time, end_time):
    if start_time == None or start_time == "":
        start_time, end_time = current_date_time()

    Transactions1 = aliased(Transactions)
    Transaction_items1 = aliased(Transaction_items)

    in_region_theft_count = db.query(Stores.name,
                                   func.count(distinct(Transactions1.transaction_id)).label(
                                       'count')
                                   ).select_from(Transactions1) \
        .join(Stores, or_(Stores.region_id == region_id, true() if region_id is None else false())) \
        .filter(Transactions1.store_id == Stores.id) \
        .filter(Transactions1.hidden == 0, Transactions1.missed_scan == 1) \
        .filter(func.date(Transactions1.begin_date) >= start_time) \
        .filter(func.date(Transactions1.begin_date) <= end_time) \
        .group_by(Transactions1.store_id).order_by(
        desc('count')).limit(5)

    in_region_theft_price = db.query(Stores.name,
                                   func.count(distinct(Transactions1.transaction_id)).label(
                                       'count'),
                                   func.ifnull(
                                       func.sum(
                                           Transaction_items1.regular_sales_unit_price * Transaction_items1.quantity),
                                       0).label(
                                       'Total')
                                   ).select_from(Transactions1) \
        .join(Stores, or_(Stores.region_id == region_id, true() if region_id is None else false())).filter(
        Transactions1.store_id == Stores.id) \
        .join(Transaction_items1,
              and_(Transaction_items1.transaction_id == Transactions1.transaction_id, Transaction_items1.missed == 1,
                   Transactions1.hidden == 0, Transactions1.missed_scan == 1)) \
        .filter(func.date(Transactions1.begin_date) >= start_time) \
        .filter(func.date(Transactions1.begin_date) <= end_time) \
        .group_by(Transactions1.store_id).order_by(
        desc('count')).limit(5)
    in_region_theft_count = in_region_theft_count.all()
    in_region_theft_price = in_region_theft_price.all()
    in_region_theft = [(*count, price[-1]) for count, price in zip(in_region_theft_count, in_region_theft_price)]
    keys_in_region_theft = ["name", "count", "Total"]
    in_region_theft = [dict(zip(keys_in_region_theft, tup)) for tup in in_region_theft]

    #ssss
    in_region_non_theft = db.query(Stores.name,
                                   func.count(distinct(Transactions1.transaction_id)).label(
                                       'count'),
                                   func.ifnull(func.sum(
                                       Transaction_items1.regular_sales_unit_price * Transaction_items1.quantity),
                                               0).label(
                                       'Total')
                                   ).select_from(Transactions1)\
        .join(Stores,or_(Stores.region_id == region_id, true() if region_id is None else false())).filter(
        Transactions1.store_id == Stores.id) \
        .join(Transaction_items1,
              and_(Transaction_items1.transaction_id == Transactions1.transaction_id, Transaction_items1.missed == 1,
                   Transactions1.hidden == 0, Transactions1.missed_scan == 1)) \
        .filter(func.date(Transactions1.begin_date) >= start_time) \
        .filter(func.date(Transactions1.begin_date) <= end_time) \
        .group_by(Transactions1.store_id).order_by(
        asc('count')).limit(5).all()
    #eeee
    in_region_non_theft_count = db.query(Stores.name,
                                       func.count(distinct(Transactions1.transaction_id)).label(
                                           'count')
                                       ).select_from(Transactions1) \
        .join(Stores, or_(Stores.region_id == region_id, true() if region_id is None else false())) \
        .filter(Transactions1.store_id == Stores.id) \
        .filter(Transactions1.hidden == 0, Transactions1.missed_scan == 1) \
        .filter(func.date(Transactions1.begin_date) >= start_time) \
        .filter(func.date(Transactions1.begin_date) <= end_time) \
        .group_by(Transactions1.store_id).order_by(
        asc('count')).limit(5)

    in_region_non_theft_price = db.query(Stores.name,
                                       func.count(distinct(Transactions1.transaction_id)).label(
                                           'count'),
                                       func.ifnull(
                                           func.sum(
                                               Transaction_items1.regular_sales_unit_price * Transaction_items1.quantity),
                                           0).label(
                                           'Total')
                                       ).select_from(Transactions1) \
        .join(Stores, or_(Stores.region_id == region_id, true() if region_id is None else false())).filter(
        Transactions1.store_id == Stores.id) \
        .join(Transaction_items1,
              and_(Transaction_items1.transaction_id == Transactions1.transaction_id, Transaction_items1.missed == 1,
                   Transactions1.hidden == 0, Transactions1.missed_scan == 1)) \
        .filter(func.date(Transactions1.begin_date) >= start_time) \
        .filter(func.date(Transactions1.begin_date) <= end_time) \
        .group_by(Transactions1.store_id).order_by(
        asc('count')).limit(5)

    in_region_non_theft_count = in_region_non_theft_count.all()
    in_region_non_theft_price = in_region_non_theft_price.all()
    in_region_non_theft = [(*count, price[-1]) for count, price in zip(in_region_non_theft_count, in_region_non_theft_price)]
    keys_in_region_theft = ["name", "count", "Total"]
    in_region_non_theft = [dict(zip(keys_in_region_theft, tup)) for tup in in_region_non_theft]

    return {"top5_store_in_region_theft": in_region_theft, "top5_store_in_region_non_theft": in_region_non_theft}


def get_count_sco_main_by_month(db: Session, store_id, region_id, area_id, start_time, end_time):
    if start_time == None or start_time == "":
        start_time, end_time = get_last_3_months_from_current_date()
        start_time_current, end_time_current = current_date_time()
    else:
        start_time_current = start_time
        end_time_current = end_time

    keys_for_main_month = ["Year", "Month", "main_count", "main_total"]
    keys_for_main_week = ["Year", "Month", "Week", "main_count", "main_total"]
    keys_for_main_day = ["Year", "Month", "Day", "main_count", "main_total"]


    keys_for_sco_month = ["Year", "Month", "sco_count", "sco_total"]
    keys_for_sco_week = ["Year", "Month", "Week", "sco_count", "sco_total"]
    keys_for_sco_day = ["Year", "Month", "Day", "sco_count", "sco_total"]


    # ------------------- For Month Data Start -----------------------------
    main_count = db.query(
        extract('year', Transactions.begin_date).label("Year"),
        extract('month', Transactions.begin_date).label("Month"),
        func.count(distinct(Transactions.transaction_id)).label("main_count"))\
        .join(Stores, Transactions.store_id == Stores.id).filter(
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.region_id == region_id, true() if region_id is None else false()),
        or_(Stores.area_id == area_id, true() if area_id is None else false())
    ) \
        .filter(Transactions.source_id == 1, Transactions.hidden == 0, Transactions.missed_scan == 1) \
        .filter(func.date(Transactions.begin_date) >= start_time) \
        .filter(func.date(Transactions.begin_date) <= end_time) \
        .group_by(func.month(Transactions.begin_date)).order_by(
        desc(extract('month', Transactions.begin_date))).all()

    main_price = db.query(
        extract('year', Transactions.begin_date).label("Year"),
        extract('month', Transactions.begin_date).label("Month"),
        func.ifnull(func.sum(Transaction_items.regular_sales_unit_price * Transaction_items.quantity), 0).label(
            'main_total')).join(Stores, Transactions.store_id == Stores.id).filter(
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.region_id == region_id, true() if region_id is None else false()),
        or_(Stores.area_id == area_id, true() if area_id is None else false())
    ).join(Transaction_items,
           and_(Transaction_items.transaction_id == Transactions.transaction_id, Transaction_items.missed == 1)) \
        .filter(Transactions.source_id == 1, Transactions.hidden == 0, Transactions.missed_scan == 1) \
        .filter(func.date(Transactions.begin_date) >= start_time) \
        .filter(func.date(Transactions.begin_date) <= end_time) \
        .group_by(func.month(Transactions.begin_date)).order_by(
        desc(extract('month', Transactions.begin_date))).all()


    sco_count = db.query(
        extract('year', Transactions.begin_date).label("Year"),
        extract('month', Transactions.begin_date).label("Month"),
        func.count(distinct(Transactions.transaction_id)).label('sco_count'))\
        .join(Stores, Transactions.store_id == Stores.id).filter(
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.region_id == region_id, true() if region_id is None else false()),
        or_(Stores.area_id == area_id, true() if area_id is None else false())
    ) \
        .filter(Transactions.source_id == 2, Transactions.hidden == 0, Transactions.missed_scan == 1) \
        .filter(func.date(Transactions.begin_date) >= start_time) \
        .filter(func.date(Transactions.begin_date) <= end_time) \
        .group_by(func.month(Transactions.begin_date)).order_by(
        desc(extract('month', Transactions.begin_date))).all()

    sco_price = db.query(
        extract('year', Transactions.begin_date).label("Year"),
        extract('month', Transactions.begin_date).label("Month"),
        func.ifnull(func.sum(Transaction_items.regular_sales_unit_price * Transaction_items.quantity), 0).label(
            'sco_total')).join(Stores, Transactions.store_id == Stores.id).filter(
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.region_id == region_id, true() if region_id is None else false()),
        or_(Stores.area_id == area_id, true() if area_id is None else false())
    ).join(Transaction_items,
           and_(Transaction_items.transaction_id == Transactions.transaction_id, Transaction_items.missed == 1)) \
        .filter(Transactions.source_id == 2, Transactions.hidden == 0, Transactions.missed_scan == 1) \
        .filter(func.date(Transactions.begin_date) >= start_time) \
        .filter(func.date(Transactions.begin_date) <= end_time) \
        .group_by(func.month(Transactions.begin_date)).order_by(
        desc(extract('month', Transactions.begin_date))).all()

    main = [(*count, price[-1]) for count, price in zip(main_count, main_price)]
    main = [dict(zip(keys_for_main_month, tup)) for tup in main]

    sco = [(*count, price[-1]) for count, price in zip(sco_count, sco_price)]
    sco = [dict(zip(keys_for_sco_month, tup)) for tup in sco]

    # ------------------- For Month Data End -----------------------------

    # ------------------- For Week Data Start -----------------------------
    main_week_count = (db.query(
        extract('year', Transactions.begin_date).label("Year"),
        extract('month', Transactions.begin_date).label("Month"),
        extract('week', Transactions.begin_date).label("Week"),
        func.count(distinct(Transactions.transaction_id)).label("main_count"))
    .join(Stores, Transactions.store_id == Stores.id)
    .filter(
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.region_id == region_id, true() if region_id is None else false()),
        or_(Stores.area_id == area_id, true() if area_id is None else false())
    )
        .filter(Transactions.source_id == 1, Transactions.hidden == 0, Transactions.missed_scan == 1)
        .filter(func.date(Transactions.begin_date) >= start_time_current)
        .filter(func.date(Transactions.begin_date) <= end_time_current)
        .group_by(
        func.week(Transactions.begin_date)).order_by(desc(extract('week', Transactions.begin_date))).limit(
        4)).all()

    main_week_price = (db.query(
        extract('year', Transactions.begin_date).label("Year"),
        extract('month', Transactions.begin_date).label("Month"),
        extract('week', Transactions.begin_date).label("Week"),
        func.ifnull(func.sum(Transaction_items.regular_sales_unit_price * Transaction_items.quantity), 0).label(
            'main_total')).join(Stores, Transactions.store_id == Stores.id).filter(
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.region_id == region_id, true() if region_id is None else false()),
        or_(Stores.area_id == area_id, true() if area_id is None else false())
    ).join(Transaction_items,
           and_(Transaction_items.transaction_id == Transactions.transaction_id, Transaction_items.missed == 1))
    .filter(Transactions.source_id == 1, Transactions.hidden == 0, Transactions.missed_scan == 1)
    .filter(func.date(Transactions.begin_date) >= start_time_current)
    .filter(func.date(Transactions.begin_date) <= end_time_current)
    .group_by(
        func.week(Transactions.begin_date)).order_by(desc(extract('week', Transactions.begin_date))).limit(
        4)).all()

    sco_week_count = (db.query(
        extract('year', Transactions.begin_date).label("Year"),
        extract('month', Transactions.begin_date).label("Month"),
        extract('week', Transactions.begin_date).label("Week"),
        func.count(distinct(Transactions.transaction_id)).label("sco_count"))
    .join(Stores, Transactions.store_id == Stores.id).filter(
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.region_id == region_id, true() if region_id is None else false()),
        or_(Stores.area_id == area_id, true() if area_id is None else false())
    )\
        .filter(Transactions.source_id == 2, Transactions.hidden == 0, Transactions.missed_scan == 1)
        .filter(func.date(Transactions.begin_date) >= start_time_current)
        .filter(func.date(Transactions.begin_date) <= end_time_current)
        .group_by(
        func.week(Transactions.begin_date)).order_by(desc(extract('week', Transactions.begin_date))).limit(
        4)).all()

    sco_week_price = (db.query(
        extract('year', Transactions.begin_date).label("Year"),
        extract('month', Transactions.begin_date).label("Month"),
        extract('week', Transactions.begin_date).label("Week"),
        func.ifnull(func.sum(Transaction_items.regular_sales_unit_price * Transaction_items.quantity), 0).label(
            'sco_total')).join(Stores, Transactions.store_id == Stores.id).filter(
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.region_id == region_id, true() if region_id is None else false()),
        or_(Stores.area_id == area_id, true() if area_id is None else false())
    ).join(Transaction_items,
           and_(Transaction_items.transaction_id == Transactions.transaction_id, Transaction_items.missed == 1)) \
        .filter(Transactions.source_id == 2, Transactions.hidden == 0, Transactions.missed_scan == 1)
        .filter(func.date(Transactions.begin_date) >= start_time_current)
        .filter(func.date(Transactions.begin_date) <= end_time_current)
        .group_by(
        func.week(Transactions.begin_date)).order_by(desc(extract('week', Transactions.begin_date))).limit(
        4)).all()

    main_week = [(*count, price[-1]) for count, price in zip(main_week_count, main_week_price)]
    main_week = [dict(zip(keys_for_main_week, tup)) for tup in main_week]

    sco_week = [(*count, price[-1]) for count, price in zip(sco_week_count, sco_week_price)]
    sco_week = [dict(zip(keys_for_sco_week, tup)) for tup in sco_week]
    # ------------------- For Week Data END -----------------------------

    # ------------------- For Days Data Start -----------------------------
    main_days_count = (db.query(
        extract('year', Transactions.begin_date).label("Year"),
        extract('month', Transactions.begin_date).label("Month"),
        extract('day', Transactions.begin_date).label("Day"),
        func.count(distinct(Transactions.transaction_id)).label("main_count"))
    .join(Stores, Transactions.store_id == Stores.id).filter(
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.region_id == region_id, true() if region_id is None else false()),
        or_(Stores.area_id == area_id, true() if area_id is None else false())
    ) \
        .filter(Transactions.source_id == 1, Transactions.hidden == 0, Transactions.missed_scan == 1)
        .filter(func.date(Transactions.begin_date) >= start_time_current)
        .filter(func.date(Transactions.begin_date) <= end_time_current)
        .group_by(
        func.day(Transactions.begin_date)).order_by(desc(extract('month', Transactions.begin_date)),
                                                    desc(extract('day', Transactions.begin_date))).limit(
        7)).all()

    main_days_price = (db.query(
        extract('year', Transactions.begin_date).label("Year"),
        extract('month', Transactions.begin_date).label("Month"),
        extract('day', Transactions.begin_date).label("Day"),
        func.ifnull(func.sum(Transaction_items.regular_sales_unit_price * Transaction_items.quantity), 0).label(
            'main_total')).join(Stores, Transactions.store_id == Stores.id).filter(
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.region_id == region_id, true() if region_id is None else false()),
        or_(Stores.area_id == area_id, true() if area_id is None else false())
    ).join(Transaction_items,
           and_(Transaction_items.transaction_id == Transactions.transaction_id, Transaction_items.missed == 1)) \
        .filter(Transactions.source_id == 1, Transactions.hidden == 0, Transactions.missed_scan == 1)
        .filter(func.date(Transactions.begin_date) >= start_time_current)
        .filter(func.date(Transactions.begin_date) <= end_time_current)
        .group_by(
        func.day(Transactions.begin_date)).order_by(desc(extract('month', Transactions.begin_date)),
                                                    desc(extract('day', Transactions.begin_date))).limit(
        7)).all()

    sco_days_count = (db.query(
        extract('year', Transactions.begin_date).label("Year"),
        extract('month', Transactions.begin_date).label("Month"),
        # extract('week', Transactions.begin_date).label("Week"),
        extract('day', Transactions.begin_date).label("Day"),
        func.count(distinct(Transactions.transaction_id)).label("sco_count"))
        .join(Stores, Transactions.store_id == Stores.id)
    .filter(
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.region_id == region_id, true() if region_id is None else false()),
        or_(Stores.area_id == area_id, true() if area_id is None else false())
    ).join(Transaction_items,
           and_(Transaction_items.transaction_id == Transactions.transaction_id, Transaction_items.missed == 1)) \
        .filter(Transactions.source_id == 2, Transactions.hidden == 0, Transactions.missed_scan == 1)
        .filter(func.date(Transactions.begin_date) >= start_time_current)
        .filter(func.date(Transactions.begin_date) <= end_time_current)
        .group_by(
        func.day(Transactions.begin_date)).order_by(desc(extract('month', Transactions.begin_date)),
                                                    desc(extract('day', Transactions.begin_date))).limit(
        7)).all()

    sco_days_price = (db.query(
        extract('year', Transactions.begin_date).label("Year"),
        extract('month', Transactions.begin_date).label("Month"),
        # extract('week', Transactions.begin_date).label("Week"),
        extract('day', Transactions.begin_date).label("Day"),
        func.ifnull(func.sum(Transaction_items.regular_sales_unit_price * Transaction_items.quantity), 0).label(
            'sco_total')).join(Stores, Transactions.store_id == Stores.id).filter(
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.region_id == region_id, true() if region_id is None else false()),
        or_(Stores.area_id == area_id, true() if area_id is None else false())
    ).join(Transaction_items,
           and_(Transaction_items.transaction_id == Transactions.transaction_id, Transaction_items.missed == 1)) \
        .filter(Transactions.source_id == 2, Transactions.hidden == 0, Transactions.missed_scan == 1)
        .filter(func.date(Transactions.begin_date) >= start_time_current)
        .filter(func.date(Transactions.begin_date) <= end_time_current)
        .group_by(
        func.day(Transactions.begin_date)).order_by(desc(extract('month', Transactions.begin_date)),
                                                    desc(extract('day', Transactions.begin_date))).limit(
        7)).all()

    main_days = [(*count, price[-1]) for count, price in zip(main_days_count, main_days_price)]
    main_days = [dict(zip(keys_for_main_day, tup)) for tup in main_days]

    sco_days = [(*count, price[-1]) for count, price in zip(sco_days_count, sco_days_price)]
    sco_days = [dict(zip(keys_for_sco_day, tup)) for tup in sco_days]
    # ------------------- For Days Data END -----------------------------
    main = sorted(main, key=sort_by_year_month)
    sco = sorted(sco, key=sort_by_year_month)
    main_week = sorted(main_week, key = sort_by_year_month_week)
    sco_week = sorted(sco_week, key = sort_by_year_month_week)
    main_days = sorted(main_days, key=sort_by_year_month_day)
    sco_days = sorted(sco_days, key=sort_by_year_month_day)
    # main_week = sorted(main_week, key=sort_by_year_month)
    # main_month_sort = sorted(main, key=itemgetter(0,1))
    # print(main_month_sort)
    # sco_month_sort = sorted(sco, key=itemgetter(0,1))
    # main_week_sort = sorted(main_week, key=itemgetter(0,1))
    # sco_week_sort = sorted(sco_week, key=itemgetter(0,1))
    # main_days_sort = sorted(main_days, key=itemgetter(0,1))
    # sco_days_sort = sorted(sco_days, key=itemgetter(0,1))
    # data_for_month = add_values_stats(main_month_sort, sco_month_sort)
    # data_for_week = add_values_stats(main_week_sort, sco_week_sort)
    # data_for_days = add_values_stats(main_days_sort, sco_days_sort)

    return {"Month": {"main": main, "sco": sco},
            "Week": {"main": main_week, "sco": sco_week},
            "Days": {"main": main_days, "sco": sco_days},
            # "data_for_month": data_for_month,
            # "data_for_week": data_for_week,
            # "data_for_days": data_for_days,
            }
            # "data_for_month":data_for_month}

# def get_count_sco_main_by_month(db: Session, store_id, region_id, area_id, start_time, end_time):
#     main = db.query(
#         extract('year', Transactions.begin_date).label("Year"),
#         extract('month', Transactions.begin_date).label("Month"),
#         func.count(distinct(Transactions.transaction_id)).label("main_count"),
#         func.ifnull(func.sum(Transaction_items.regular_sales_unit_price * Transaction_items.quantity), 0).label(
#             'main_total')).join(Stores, Transactions.store_id == Stores.id).filter(
#         or_(Transactions.store_id == store_id, true() if store_id is None else false()),
#         or_(Stores.region_id == region_id, true() if region_id is None else false()),
#         or_(Stores.area_id == area_id, true() if area_id is None else false())
#         ).join(Transaction_items,
#                and_(Transaction_items.transaction_id == Transactions.transaction_id, Transaction_items.missed == 1)) \
#         .filter(Transactions.source_id == 1, Transactions.hidden == 0, Transactions.missed_scan == 1,
#                 or_(Transactions.begin_date.between(start_time, end_time), true() if start_time is None else false())
#                 ).group_by(func.month(Transactions.begin_date)).order_by(
#         desc(extract('month', Transactions.begin_date))).limit(3).all()
#
#     sco = db.query(
#         extract('year', Transactions.begin_date).label("Year"),
#         extract('month', Transactions.begin_date).label("Month"),
#         func.count(distinct(Transactions.transaction_id)).label('sco_count'),
#         func.ifnull(func.sum(Transaction_items.regular_sales_unit_price * Transaction_items.quantity), 0).label(
#             'sco_total')).join(Stores, Transactions.store_id == Stores.id).filter(
#         or_(Transactions.store_id == store_id, true() if store_id is None else false()),
#         or_(Stores.region_id == region_id, true() if region_id is None else false()),
#         or_(Stores.area_id == area_id, true() if area_id is None else false())
#         ).join(Transaction_items,
#                and_(Transaction_items.transaction_id == Transactions.transaction_id, Transaction_items.missed == 1)) \
#         .filter(Transactions.source_id == 2, Transactions.hidden == 0, Transactions.missed_scan == 1,
#                 or_(Transactions.begin_date.between(start_time, end_time), true() if start_time is None else false())
#                 ).group_by(func.month(Transactions.begin_date)).order_by(
#         desc(extract('month', Transactions.begin_date))).limit(3).all()
#
#     main_week = (db.query(
#         extract('month', Transactions.begin_date).label("Month"),
#         extract('week', Transactions.begin_date).label("Week"),
#         func.count(distinct(Transactions.transaction_id)).label("main_count"),
#         func.ifnull(func.sum(Transaction_items.regular_sales_unit_price * Transaction_items.quantity), 0).label(
#             'main_total')).join(Stores, Transactions.store_id == Stores.id).filter(
#         or_(Transactions.store_id == store_id, true() if store_id is None else false()),
#         or_(Stores.region_id == region_id, true() if region_id is None else false()),
#         or_(Stores.area_id == area_id, true() if area_id is None else false())
#     ).join(Transaction_items,
#            and_(Transaction_items.transaction_id == Transactions.transaction_id, Transaction_items.missed == 1)) \
#         .filter(Transactions.source_id == 1, Transactions.hidden == 0, Transactions.missed_scan == 1,
#                 or_(Transactions.begin_date.between(start_time, end_time),
#                     true() if start_time is None else false())).group_by(
#         func.week(Transactions.begin_date)).order_by(desc(extract('week', Transactions.begin_date))).limit(
#         4)).all()
#
#     sco_week = (db.query(
#         extract('month', Transactions.begin_date).label("Month"),
#         extract('week', Transactions.begin_date).label("Week"),
#         func.count(distinct(Transactions.transaction_id)).label("sco_count"),
#         func.ifnull(func.sum(Transaction_items.regular_sales_unit_price * Transaction_items.quantity), 0).label(
#             'sco_total')).join(Stores, Transactions.store_id == Stores.id).filter(
#         or_(Transactions.store_id == store_id, true() if store_id is None else false()),
#         or_(Stores.region_id == region_id, true() if region_id is None else false()),
#         or_(Stores.area_id == area_id, true() if area_id is None else false())
#     ).join(Transaction_items,
#            and_(Transaction_items.transaction_id == Transactions.transaction_id, Transaction_items.missed == 1)) \
#         .filter(Transactions.source_id == 2, Transactions.hidden == 0, Transactions.missed_scan == 1,
#                 or_(Transactions.begin_date.between(start_time, end_time),
#                     true() if start_time is None else false())).group_by(
#         func.week(Transactions.begin_date)).order_by(desc(extract('week', Transactions.begin_date))).limit(
#         4)).all()
#     main_days = (db.query(
#         extract('month', Transactions.begin_date).label("Month"),
#         extract('day', Transactions.begin_date).label("Day"),
#         func.count(distinct(Transactions.transaction_id)).label("main_count"),
#         func.ifnull(func.sum(Transaction_items.regular_sales_unit_price * Transaction_items.quantity), 0).label(
#             'main_total')).join(Stores, Transactions.store_id == Stores.id).filter(
#         or_(Transactions.store_id == store_id, true() if store_id is None else false()),
#         or_(Stores.region_id == region_id, true() if region_id is None else false()),
#         or_(Stores.area_id == area_id, true() if area_id is None else false())
#     ).join(Transaction_items,
#            and_(Transaction_items.transaction_id == Transactions.transaction_id, Transaction_items.missed == 1)) \
#         .filter(Transactions.source_id == 1, Transactions.hidden == 0, Transactions.missed_scan == 1,
#                 or_(Transactions.begin_date.between(start_time, end_time),
#                     true() if start_time is None else false())).group_by(
#         func.day(Transactions.begin_date)).order_by(desc(extract('month', Transactions.begin_date)),
#                                                     desc(extract('day', Transactions.begin_date))).limit(
#         7)).all()
#     sco_days = (db.query(
#         extract('month', Transactions.begin_date).label("Month"),
#         extract('week', Transactions.begin_date).label("Week"),
#         extract('day', Transactions.begin_date).label("Day"),
#         func.count(distinct(Transactions.transaction_id)).label("sco_count"),
#         func.ifnull(func.sum(Transaction_items.regular_sales_unit_price * Transaction_items.quantity), 0).label(
#             'sco_total')).join(Stores, Transactions.store_id == Stores.id).filter(
#         or_(Transactions.store_id == store_id, true() if store_id is None else false()),
#         or_(Stores.region_id == region_id, true() if region_id is None else false()),
#         or_(Stores.area_id == area_id, true() if area_id is None else false())
#     ).join(Transaction_items,
#            and_(Transaction_items.transaction_id == Transactions.transaction_id, Transaction_items.missed == 1)) \
#         .filter(Transactions.source_id == 2, Transactions.hidden == 0, Transactions.missed_scan == 1,
#                 or_(Transactions.begin_date.between(start_time, end_time),
#                     true() if start_time is None else false())).group_by(
#         func.day(Transactions.begin_date)).order_by(desc(extract('month', Transactions.begin_date)),
#                                                     desc(extract('day', Transactions.begin_date))).limit(
#         7)).all()
#     return {"Month": {"main": main, "sco": sco}, "Week": {"main": main_week, "sco": sco_week},
#             "Days": {"main": main_days, "sco": sco_days}}


def get_sco_main_active_status(db: Session, store_id):
    current_date = datetime.utcnow()
    three_days_ago = current_date - timedelta(days=3)
    data = db.query(Outage.date).filter(
        Outage.date > three_days_ago, Outage.store_id == store_id).all()
    data = [str(i.date) for i in data]
    three_days = [str((current_date - timedelta(days=1)).date()), str((current_date - timedelta(days=2)).date()),
                  str((current_date - timedelta(days=3)).date())]
    return_data = {}
    for i in three_days:
        if i in data:
            return_data[i] = "Inactive"
        else:
            return_data[i] = "Active"

    return return_data


def performance_comparison(db: Session, store_id, start_time, end_time):
    if start_time == None or start_time == "":
        start_time, end_time = current_date_time()
    print(start_time, end_time)
    main = db.query(Transactions.counter_no,
                    func.count(distinct(Transactions.transaction_id)).label("count")) \
        .join(Stores, Stores.id == Transactions.store_id).join(Transaction_items,
                                                               and_(
                                                                   Transaction_items.transaction_id == Transactions.transaction_id,
                                                                   Transaction_items.missed == 1)) \
        .filter(Transactions.source_id == 1, Transactions.hidden == 0, Transactions.missed_scan == 1,
                Transactions.store_id == store_id) \
        .filter(func.date(Transactions.begin_date) >= start_time) \
        .filter(func.date(Transactions.begin_date) <= end_time) \
        .group_by(Transactions.counter_no).all()

    sco = db.query(Transactions.counter_no,
                   func.count(distinct(Transactions.transaction_id)).label("count")) \
        .join(Stores, Stores.id == Transactions.store_id).join(Transaction_items,
                                                               and_(
                                                                   Transaction_items.transaction_id == Transactions.transaction_id,
                                                                   Transaction_items.missed == 1)) \
        .filter(Transactions.source_id == 2, Transactions.hidden == 0, Transactions.missed_scan == 1,
                Transactions.store_id == store_id) \
        .filter(func.date(Transactions.begin_date) >= start_time) \
        .filter(func.date(Transactions.begin_date) <= end_time) \
        .group_by(Transactions.counter_no).all()

    return {"main": main, "sco": sco}

# def get_sco_main_stats_for_month(db: Session, store_id, start_date, end_date):
#     if store_id == None:
#         query_txt = "SELECT DISTINCT(month(begin_date)),count(transaction_id) FROM transactions where begin_date >= last_day(now()) + interval 1 day - interval 3 month and hidden=0 and missed_scan=1 group by month(begin_date);"
#     else:
#         if start_date:
#             df = pd.DataFrame(
#                 columns=["datetime"],
#                 data=pd.date_range(start_date, end_date, freq='D'))
#             df["month_number"] = df["datetime"].dt.month
#             months_ext = df["month_number"].unique()
#             data = {}
#             for month in months_ext:
#                 grouped = df.groupby(['month_number'])
#                 df1 = grouped.get_group(month)
#                 min_date = df1["datetime"].min().strftime("%Y-%m-%d")
#                 max_date = df1["datetime"].max().strftime("%Y-%m-%d")
#                 query_txt = f"SELECT count(transaction_id) From transactions where Date(begin_date) between('{min_date}') and ('{max_date}') and missed_scan = 1 and hidden = 0 and store_id = 1 and source_id = {store_id}"
#                 sql = text(query_txt)
#                 month_data = db.execute(sql).all()
#                 val = list(*month_data)[0]
#                 data[datetime.strptime(min_date, '%Y-%m-%d').strftime("%m")] = val
#             return {"data": data}
#         else:
#             query_txt = f"SELECT DISTINCT(month(begin_date)),count(transaction_id) FROM transactions where begin_date >= last_day(now()) + interval 1 day - interval 3 month and hidden=0 and missed_scan=1 and store_id = {store_id} group by month(begin_date);"
#
#     sql = text(query_txt)
#     month_data = db.execute(sql).all()
#     print(month_data)
#     months = list(list(zip(*month_data))[0])
#     counts = list(list(zip(*month_data))[1])
#     if datetime.now().month in months:
#         return {"data": {"months":months, "count":counts}}
#     months.append(datetime.now().month)
#     counts.append(0)
#     return {"data": {"months":months, "count":counts}}

def get_top5_main_theft_data(db: Session, store_id, region_id, area_id, start_time, end_time):
    if start_time == None or start_time == "":
        start_time, end_time = current_date_time()

    query = (db.query(
            extract("year", Transactions.begin_date).label("year"),
            extract("month", Transactions.begin_date).label("month"),
            extract('day', Transactions.begin_date).label('day'),
            extract('hour', Transactions.begin_date).label('hour'),
            extract('minute', Transactions.begin_date).label('minute'),
            extract('second', Transactions.begin_date).label("second"),
            Transactions.transaction_id,
            Transactions.video_link,
            func.sum(Transaction_items.regular_sales_unit_price * Transaction_items.quantity).label('ss'))
         .join(Transaction_items, Transactions.transaction_id == Transaction_items.transaction_id)
         .filter(Transactions.hidden == 0)
         .filter(Transactions.missed_scan == 1)
         .filter(Transaction_items.missed == 1)
         .filter(or_(Transactions.begin_date.between(start_time, end_time), true() if start_time is None else false()))
         .filter(
            or_(Transactions.store_id == store_id, true() if store_id is None else false()),
            or_(Stores.region_id == region_id, true() if region_id is None else false()),
            or_(Stores.area_id == area_id, true() if area_id is None else false())
            )
         .filter(func.date(Transactions.begin_date) >= start_time) \
         .filter(func.date(Transactions.begin_date) <= end_time) \
         .group_by(Transactions.transaction_id)
         .order_by(desc('ss'))
         .limit(5)).all()

    data = [dict(date=f"{i.day}/{i.month}/{i.year}",
                 time=f"{i.hour}:{i.minute}:{i.second}",
                 transaction_id=i.transaction_id,
                 total=i.ss,
                 video_link=i.video_link,
                 )
            for i in query]

    return dict(data=data)

def get_top5_theft_sco_res(db, start_time, end_time):
    if start_time == None or start_time == "":
        start_time, end_time = current_date_time()

    Transactions1 = aliased(Transactions)
    Transaction_items1 = aliased(Transaction_items)

    theft = db.query(Stores.name,
                               func.count(distinct(Transactions1.transaction_id)).label(
                                   'count'),
                               func.ifnull(
                                   func.sum(Transaction_items1.regular_sales_unit_price * Transaction_items1.quantity),
                                   0).label(
                                   'Total')
                               ).select_from(Transactions1)\
        .filter(
        Transactions1.store_id == Stores.id) \
        .join(Transaction_items1,
              and_(Transaction_items1.transaction_id == Transactions1.transaction_id, Transaction_items1.missed == 1,
                   Transactions1.hidden == 0, Transactions1.missed_scan == 1, Transactions1.source_id == 2)) \
        .filter(func.date(Transactions1.begin_date) >= start_time) \
        .filter(func.date(Transactions1.begin_date) <= end_time) \
        .group_by(Transactions1.store_id).order_by(
        desc('count')).limit(5).all()
    return theft

def get_top5_theft_main_res(db, start_time, end_time):
    if start_time == None or start_time == "":
        start_time, end_time = current_date_time()

    Transactions1 = aliased(Transactions)
    Transaction_items1 = aliased(Transaction_items)

    theft = db.query(Stores.name,
                               func.count(distinct(Transactions1.transaction_id)).label(
                                   'count'),
                               func.ifnull(
                                   func.sum(Transaction_items1.regular_sales_unit_price * Transaction_items1.quantity),
                                   0).label(
                                   'Total')
                               ).select_from(Transactions1)\
        .filter(
        Transactions1.store_id == Stores.id) \
        .join(Transaction_items1,
              and_(Transaction_items1.transaction_id == Transactions1.transaction_id, Transaction_items1.missed == 1,
                   Transactions1.hidden == 0, Transactions1.missed_scan == 1, Transactions1.source_id == 1)) \
        .filter(func.date(Transactions1.begin_date) >= start_time) \
        .filter(func.date(Transactions1.begin_date) <= end_time) \
        .group_by(Transactions1.store_id).order_by(
        desc('count')).limit(5).all()
    return theft

def get_top5_employee_performance_res(db, store_id, region_id, area_id, start_time, end_time):
    if start_time == None or start_time == "":
        start_time, end_time = current_date_time()

    # create aliases for the tables
    transactions_alias = aliased(Transactions)
    transaction_items_alias = aliased(Transaction_items)
    stores_alias = aliased(Stores)
    operators_alias = aliased(Operators)

    # build the query using the ORM query API
    query = (db.query(stores_alias.name, operators_alias.operator_id,
                           func.count(func.distinct(transactions_alias.transaction_id)).label("count"),
                           func.sum(
                               transaction_items_alias.regular_sales_unit_price * transaction_items_alias.quantity).label(
                               "total"))
             .select_from(transactions_alias)
             .join(transaction_items_alias, transactions_alias.transaction_id == transaction_items_alias.transaction_id)
             .join(stores_alias, transactions_alias.store_id == stores_alias.id)
             .join(operators_alias, transactions_alias.operator_id == operators_alias.id)
             .filter(transaction_items_alias.missed == 1)
             .filter(transactions_alias.hidden == 0)
             .filter(transactions_alias.missed_scan == 1)
             .filter(
                    or_(transactions_alias.store_id == store_id, true() if store_id is None else false()),
                    or_(stores_alias.region_id == region_id, true() if region_id is None else false()),
                    or_(stores_alias.area_id == area_id, true() if area_id is None else false())
                    )
             .filter(func.date(transactions_alias.begin_date) >= start_time)
             .filter(func.date(transactions_alias.begin_date) <= end_time)
             .group_by(transactions_alias.store_id)
             .order_by(desc("count"))
             )
    # print(query.limit(5).all())
    return query.limit(5).all()

def add_comment_of_transection_id(db, details):
    details["created_at"] = datetime.now()
    details["updated_at"] = datetime.now()
    comment = Comments(**details)
    db.add(comment)
    db.commit()

def all_region(db, store_id, area_id):
    results = db.query(distinct(Stores.region_id)).filter(
                    or_(Stores.id == store_id, true() if store_id is None else false()),
                    or_(Stores.area_id == area_id, true() if area_id is None else false())
                    ).all()
    values = [result[0] for result in results]
    return {"all_region": sorted(values)}

def all_area(db):
    results = db.query(distinct(Stores.area_id)).all()
    values = [result[0] for result in results]
    return {"all_area": sorted(values)}

def report_top10_sco_main(db, store_id, region_id, area_id, start_time, end_time):
    if start_time == None or start_time == "":
        start_time, end_time = current_date_time()

    result_count = db.query(Stores.name,
                      func.count(distinct(Transactions.transaction_id)).label('count')
                      ).select_from(Transactions) \
        .join(Stores, Transactions.store_id == Stores.id) \
        .filter(or_(Stores.region_id == region_id, true() if region_id is None else false()),
                or_(Stores.area_id == area_id, true() if area_id is None else false()))\
        .filter(Transactions.store_id == Stores.id) \
        .filter(Transactions.hidden == 0, Transactions.missed_scan == 1) \
        .filter(func.date(Transactions.begin_date) >= start_time) \
        .filter(func.date(Transactions.begin_date) <= end_time) \
        .group_by(Transactions.store_id).order_by(
        desc('count')).limit(10)

    result_price = db.query(Stores.name,
                                   func.count(distinct(Transactions.transaction_id)).label('count'),
                                   func.ifnull(
                                       func.sum(Transaction_items.regular_sales_unit_price * Transaction_items.quantity),0)
                            .label('Total')).select_from(Transactions) \
        .join(Stores, Transactions.store_id == Stores.id)\
        .join(Transaction_items,
              and_(Transaction_items.transaction_id == Transactions.transaction_id, Transaction_items.missed == 1,
                   Transactions.hidden == 0, Transactions.missed_scan == 1)) \
        .filter(or_(Stores.region_id == region_id, true() if region_id is None else false()),
                or_(Stores.area_id == area_id, true() if area_id is None else false()))\
        .filter(func.date(Transactions.begin_date) >= start_time) \
        .filter(func.date(Transactions.begin_date) <= end_time) \
        .group_by(Transactions.store_id).order_by(
        desc('count')).limit(10)

    in_region_theft_count = result_count.all()
    in_region_theft_price = result_price.all()
    in_region_theft = [(*count, price[-1]) for count, price in zip(in_region_theft_count, in_region_theft_price)]
    keys_in_region_theft = ["name", "count", "Total"]
    result = [dict(zip(keys_in_region_theft, tup)) for tup in in_region_theft]
    return result

def list_of_sco_main_theft(db, store_id, region_id, area_id, start_time, end_time):
    if start_time == None or start_time == "":
        start_time, end_time = current_date_time()

    main_count = db.query(Stores.name.label("store_name"), \
        func.count(distinct(Transactions.transaction_id)).label("main_count"))\
        .join(Stores, Transactions.store_id == Stores.id).filter(
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.region_id == region_id, true() if region_id is None else false()),
        or_(Stores.area_id == area_id, true() if area_id is None else false())
        ).filter(Transactions.hidden == 0, Transactions.missed_scan == 1) \
        .filter(func.date(Transactions.begin_date) >= start_time) \
        .filter(func.date(Transactions.begin_date) <= end_time).group_by(Transactions.store_id).all()

    main_price = db.query(
        Stores.name.label("store_name"),\
        func.count(distinct(Transactions.transaction_id)).label("main_count"),
        func.ifnull(func.sum(Transaction_items.regular_sales_unit_price * Transaction_items.quantity), 0).label(
            'main_total')).join(Stores, Transactions.store_id == Stores.id).filter(
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.region_id == region_id, true() if region_id is None else false()),
        or_(Stores.area_id == area_id, true() if area_id is None else false())
        ).join(Transaction_items,
               and_(Transaction_items.transaction_id == Transactions.transaction_id, Transaction_items.missed == 1)) \
        .filter(Transactions.hidden == 0, Transactions.missed_scan == 1) \
        .filter(func.date(Transactions.begin_date) >= start_time) \
        .filter(func.date(Transactions.begin_date) <= end_time).group_by(Transactions.store_id).all()

    main_count_list = [dict(zip(["store_name", "main_count"], tup)) for tup in main_count]
    main_price_list = [dict(zip(["store_name", "main_count", "main_total"], tup)) for tup in main_price]
    if len(main_count_list) > 0:
        merged = merge_dicts(main_count_list, main_price_list)
        result = all_store_count_list_of_sco_main(db=db, data=merged, store_id=store_id, region_id=region_id, area_id=area_id)
    else:
        result = all_store_count_list_of_sco_main(db=db, data=main_price_list, store_id=store_id, region_id=region_id, area_id=area_id)
        return {"merge": result}
    return {"merge": result}

def get_comment_transaction_id(details, db):
    transaction_id = details["transaction_id"]
    result = db.query(Comments.body).filter(Comments.transaction_id == transaction_id).all()
    return result

def get_store_region_area_(db, store_id, region_id, area_id):
    result = db.query(Stores.id, Stores.name, Stores.region_id, Stores.area_id).filter(
        or_(Stores.id == store_id, true() if store_id is None else false()),
        or_(Stores.region_id == region_id, true() if region_id is None else false()),
        or_(Stores.area_id == area_id, true() if area_id is None else false())
        ).all()
    return result

