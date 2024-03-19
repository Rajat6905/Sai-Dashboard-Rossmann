import datetime as dt
from datetime import datetime
from dateutil.relativedelta import relativedelta
from model.model import Stores
from pprint import pprint
import itertools
from sqlalchemy import func, and_, distinct, desc, or_, true, false, asc, extract, text

def current_date_time():
    todayDate = dt.date.today()
    # if todayDate.day > 30:
    #     todayDate += dt.timedelta(7)
    #     print(todayDate)

    # Get the start date of the current month
    # start_time = todayDate.replace(day=1)
    start_time = todayDate.replace(year=2023,month=1,day=1)

    # Get the number of days in the current month
    # _, last_day = calendar.monthrange(now.year, now.month)

    # Get the end date of the current month
    end_time = datetime.now().strftime("%Y-%m-%d")
    return start_time, end_time

def get_last_3_months_from_current_date():
    todayDate = dt.date.today() # NOQA
    three_months = dt.date.today() + relativedelta(months=-2) # NOQA
    start_time = three_months.replace(day=1)
    end_time = datetime.now().strftime("%Y-%m-%d")
    return start_time, end_time

def add_values_stats(main, sco):
    if len(main) > 0 and len(sco) > 0:
        total_data = []
        counts = {}
        for year, month, count, total in main + sco:
            year_month = (year, month)
            if year_month in counts:
                counts[year_month]['count'] += count
                counts[year_month]['total'] += total
            else:
                counts[year_month] = {'count': count, 'total': total}

        for i, (year, month, count, total) in enumerate(main):
            year_month = (year, month)
            if year_month in counts:
                main[i] = (year, month, counts[year_month]['count'], counts[year_month]['total'])

        for year_month_val, counts_val in counts.items():
            total_data.append({"Year": year_month_val[0],
                        "Month": year_month_val[1],
                        "count": counts_val.get("count", 0),
                        "total": counts_val.get("total", 0)
                    })

        return total_data

    elif len(main) == 0:
        return sco

    elif len(sco) == 0:
        return main

def sort_by_year_month(item):
    return (item['Year'], item['Month'])

def sort_by_year_month_week(item):
    return (item["Year"], item['Month'], item["Week"])

def sort_by_year_month_day(item):
    return (item["Year"], item['Month'], item["Day"])

def merge_dicts(a, b):
    store_names = set(map(lambda x: x["store_name"], a + b))
    result = []
    for store_name in store_names:
        a_store = next((x for x in a if x["store_name"] == store_name), {})
        b_store = next((x for x in b if x["store_name"] == store_name), {})
        result.append({
            "store_name": store_name,
            "main_count": a_store.get("main_count", 0),
            "main_total": b_store.get("main_total", 0)
        })
    return result

def merge_list_of_aisle_theft(aisle_list, intervention_list):
    result = []
    for aisle_item in aisle_list:
        for intervention_item in intervention_list:
            if aisle_item["name"] == intervention_item["name"]:
                result.append({**aisle_item, **intervention_item})
    for aisle_item in aisle_list:
        flag = False # Assume not in the other list
        for result_item in result:
            if aisle_item["name"] == result_item["name"]:
                flag = True # Found in the list
                break
        if flag == False:
            result.append({
                "id": aisle_item["id"],
                "name": aisle_item['name'],
                "region_id": aisle_item["region_id"],
                "area_id": aisle_item["area_id"],
                "intervention_no_of_theft": 0,
                "intervention_total": 0,
                "aisle_no_of_theft": aisle_item['aisle_no_of_theft'],
                "aisle_total": aisle_item['aisle_total']
            })
    return result

def set_permissions(role):
    permissions = {
        "admin": False,
        'stores': False,
        'region': False,
        'area': False,
    }
    permissions[role] = True
    return permissions

def all_store_count_aisle(type=None, data=None, db=None, store_id=None, region_id=None, area_id=None):
    if type == "admin":
        all = db.query(func.count(distinct(Stores.name)).label('store_count'), Stores.region_id).filter(Stores.store_running == 1) \
            .filter(or_(Stores.id == store_id, true() if store_id is None else false()),
                    or_(Stores.region_id == region_id, true() if region_id is None else false()),
                    or_(Stores.area_id == area_id, true() if area_id is None else false())
                    ).group_by(Stores.region_id).all()
        print(all)
        r_data = []
        for region_data in all:
            flag = 0
            result = {}
            for i in data:
                if i._asdict()["value"] == region_data[1]:
                    result["value"] = region_data[1]
                    result["aisle_total"] = int(i._asdict()["aisle_total"])
                    result["count"] = i._asdict()["count"]
                    result["store_count"] = region_data[0]
                    flag = 1
            if flag == 0:
                result["value"] = region_data[1]
                result["aisle_total"] = 0.0
                result["count"] = 0
                result["store_count"] = region_data[0]
            r_data.append(result)
        sorted_data = sorted(r_data, key=lambda d: -d['count'])
        return sorted_data

    elif type == "region":
        all = db.query(func.count(distinct(Stores.name)).label('store_count'), Stores.area_id).filter(Stores.store_running == 1) \
            .filter(or_(Stores.id == store_id, true() if store_id is None else false()),
                    or_(Stores.region_id == region_id, true() if region_id is None else false()),
                    or_(Stores.area_id == area_id, true() if area_id is None else false())
                    ).group_by(Stores.area_id).all()
        r_data = []
        for region_data in all:
            flag = 0
            result = {}
            for i in data:
                if i._asdict()["value"] == region_data[1]:
                    result["value"] = region_data[1]
                    result["aisle_total"] = i._asdict()["aisle_total"]
                    result["count"] = i._asdict()["count"]
                    result["store_count"] = region_data[0]
                    flag = 1
            if flag == 0:
                result["value"] = region_data[1]
                result["aisle_total"] = 0.0
                result["count"] = 0
                result["store_count"] = region_data[0]
            r_data.append(result)
        sorted_data = sorted(r_data, key=lambda d: -d['count'])
        return sorted_data

    elif type == "area":
        all = db.query(func.count(distinct(Stores.name)).label('store_count'),Stores.name, Stores.area_id).filter(Stores.store_running == 1)\
            .filter(or_(Stores.id == store_id, true() if store_id is None else false()),
                    or_(Stores.region_id == region_id, true() if region_id is None else false()),
                    or_(Stores.area_id == area_id, true() if area_id is None else false())
                    ).group_by(Stores.area_id, Stores.id).all()
        print(all)
        r_data = []
        for region_data in all:
            flag = 0
            result = {}
            for i in data:
                if i._asdict()["value"] == region_data[1]:
                    result["value"] = region_data[1]
                    result["aisle_total"] = i._asdict()["aisle_total"]
                    result["count"] = i._asdict()["count"]
                    result["store_count"] = region_data[0]
                    flag = 1
            if flag == 0:
                result["value"]=region_data[1]
                result["aisle_total"] = 0.0
                result["count"] = 0
                result["store_count"] = region_data[0]
            r_data.append(result)
        sorted_data = sorted(r_data, key=lambda d: -d['count'])
        return sorted_data

def all_store_count_sco_main(type=None, data=None, db=None, store_id=None, region_id=None, area_id=None):

    if type == "admin":
        all = db.query(func.count(distinct(Stores.name)).label('store_count'), Stores.region_id).filter(Stores.store_running == 1) \
            .filter(or_(Stores.id == store_id, true() if store_id is None else false()),
                    or_(Stores.region_id == region_id, true() if region_id is None else false()),
                    or_(Stores.area_id == area_id, true() if area_id is None else false())
                    ).group_by(Stores.region_id).all()
        r_data = []
        for region_data in all:
            flag = 0
            result = {}
            for i in data:
                if i["value"] == region_data[1]:
                    result["value"] = region_data[1]
                    result["Total"] = i["Total"]
                    result["count"] = i["count"]
                    result["store_count"] = region_data[0]
                    flag = 1
            if flag == 0:
                result["value"] = region_data[1]
                result["Total"] = 0.0
                result["count"] = 0
                result["store_count"] = region_data[0]
            r_data.append(result)
        sorted_data = sorted(r_data, key=lambda d: -d['count'])
        return sorted_data

    elif type == "region":
        all = db.query(func.count(distinct(Stores.name)).label('store_count'), Stores.area_id).filter(
            Stores.store_running == 1) \
            .filter(or_(Stores.id == store_id, true() if store_id is None else false()),
                    or_(Stores.region_id == region_id, true() if region_id is None else false()),
                    or_(Stores.area_id == area_id, true() if area_id is None else false())
                    ).group_by(Stores.area_id).all()
        r_data = []
        for region_data in all:
            flag = 0
            result = {}
            for i in data:
                if i["value"] == region_data[1]:
                    result["value"] = region_data[1]
                    result["Total"] = i["Total"]
                    result["count"] = i["count"]
                    result["store_count"] = region_data[0]
                    flag = 1
            if flag == 0:
                result["value"] = region_data[1]
                result["Total"] = 0.0
                result["count"] = 0
                result["store_count"] = region_data[0]
            r_data.append(result)
        sorted_data = sorted(r_data, key=lambda d: -d['count'])
        return sorted_data

    elif type == "area":
        all = db.query(func.count(distinct(Stores.name)).label('store_count'),Stores.name, Stores.area_id).filter(Stores.store_running == 1)\
            .filter(or_(Stores.id == store_id, true() if store_id is None else false()),
                    or_(Stores.region_id == region_id, true() if region_id is None else false()),
                    or_(Stores.area_id == area_id, true() if area_id is None else false())
                    ).group_by(Stores.area_id, Stores.id).all()
        print(all)
        r_data = []
        for region_data in all:
            flag = 0
            result = {}
            for i in data:
                if i["area_id"] == region_data[2]:
                    result["value"] = region_data[1]
                    result["Total"] = i["Total"]
                    result["count"] = i["count"]
                    result["store_count"] = region_data[0]
                    flag = 1
            if flag == 0:
                result["value"]=region_data[1]
                result["Total"] = 0.0
                result["count"] = 0
                result["store_count"] = region_data[0]
            r_data.append(result)
        sorted_data = sorted(r_data, key=lambda d: -d['count'])
        return sorted_data

def all_store_count_list_of_aisle_theft(db, data, store_id=None, region_id=None, area_id=None):
    # print(store_id)
    # print(region_id)
    # print(area_id)
    all = db.query(Stores.name, Stores.id,Stores.region_id,Stores.area_id).filter(
        Stores.store_running == 1) \
        .filter(or_(Stores.id == store_id, true() if store_id is None else false()),
                or_(Stores.region_id == region_id, true() if region_id is None else false()),
                or_(Stores.area_id == area_id, true() if area_id is None else false())
                ).all()
    # all = db.query(Stores.name, Stores.id).f.all()
    if len(data)>0:
        result = data.copy()
        names = [d['name'] for d in data]
        for stores_name in all:
            # print(stores_name)
            if stores_name[0] not in names:
                result.append(
                    {
                        "id": stores_name[1],
                        "name": stores_name[0],
                        "region_id":stores_name[2],
                        "area_id":stores_name[3],
                        "aisle_no_of_theft":0,
                        "aisle_total":0,
                        "intervention_no_of_theft":0,
                        "intervention_total":0,
                        "no_of_alert":0,
                    }
                )
    else:
        result = []
        for stores_name in all:
            result.append(
                {
                    "id": stores_name[1],
                    "name": stores_name[0],
                    "region_id": stores_name[2],
                    "area_id": stores_name[3],
                    "aisle_no_of_theft": 0,
                    "aisle_total": 0,
                    "intervention_no_of_theft": 0,
                    "intervention_total": 0,
                    "no_of_alert": 0,
                }
            )
    return result



    # if len(data)<=0:
    #     for stores_name in all:
    #         d = {}
    #         d["id"] = stores_name[1]
    #         d["name"] = stores_name[0]
    #         d["aisle_no_of_theft"] = 0
    #         d["aisle_total"] = 0.0
    #         d["intervention_no_of_theft"] = 0
    #         d["intervention_total"] = 0
    #         d["no_of_alert"] = 0
    #         result.append(d)
    # names = [d['name'] for d in data]
    # for stores_name in all:
    #     d = {}
    #     # for val in data:
    #     # print(val["name"], stores_name[0])
    #     # if val["name"] != stores_name[0]:
    #     if stores_name[0] not in names:
    #         d["id"] = stores_name[1]
    #         d["name"] = stores_name[0]
    #         d["aisle_no_of_theft"] = 0
    #         d["aisle_total"] = 0.0
    #         d["intervention_no_of_theft"] = 0
    #         d["intervention_total"] = 0
    #         d["no_of_alert"] = 0
    #     result.append(d)
    #
    # return result

def all_store_count_list_of_sco_main(db, data, store_id=None, region_id=None, area_id=None):
    all = db.query(Stores.name, Stores.id).filter(
        Stores.store_running == 1) \
        .filter(or_(Stores.id == store_id, true() if store_id is None else false()),
                or_(Stores.region_id == region_id, true() if region_id is None else false()),
                or_(Stores.area_id == area_id, true() if area_id is None else false())
                ).all()
    result = data.copy()
    if len(data)<=0:
        for i in all:
            d = {}
            d["main_count"] = 0
            d["main_total"] = 0
            d["store_name"] = i[0]
            result.append(d)
        return result
    names = [d['store_name'] for d in data]
    for i in all:
        d = {}
        # for val in data:
        if i[0] not in names:
            d["main_count"] = 0
            d["main_total"] = 0
            d["store_name"] = i[0]
            result.append(d)
            # break
            # else:
            #     continue
    return result

