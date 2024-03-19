from sqlalchemy import func, and_, distinct, desc, or_, true, false, asc, extract, text,select
from model.model import Transactions, Stores, Transaction_items, Outage,Sources, AppUsers
from sqlalchemy.orm import Session, aliased
from datetime import datetime, timedelta
import requests
import json
from utils import current_date_time

BASE_URL = "https://saivalentine.com/"

# def get_app_report(db,store_id=None,region_id=None,area_id=None,start_date=None,end_date=None,company_name=None):
#
#
#     all = db.query(Stores.name, Stores.id,Stores.store_actual_id, Stores.region_id, Stores.area_id).filter(
#         Stores.store_running == 1) \
#         .filter(or_(Stores.id == store_id, true() if store_id is None else false()),
#                 or_(Stores.region_id == region_id, true() if region_id is None else false()),
#                 or_(Stores.area_id == area_id, true() if area_id is None else false())
#                 ).all()
#
#     url_ext="dashboard_report"
#     headers = {
#         'accept': 'application/json',
#     }
#
#     params = {
#         'store_id': store_id,
#         'start_date': start_date,
#         'end_date': end_date,
#         'company_name': company_name,
#     }
#
#     response1 = requests.post(BASE_URL+url_ext, params=params, headers=headers)
#     if response1.status_code==200:
#         response=response1.json()
#         api_data_by_id = {entry["store_id"]: entry for entry in response["data"]}
#         # Merge SQL Alchemy response with API response, adding region_id and area_id
#         merged_data = []
#         for sql_entry in all:
#             store_name,store_id,store_actual_id, region_id, area_id = sql_entry
#             api_entry = api_data_by_id.get(store_id)
#             if api_entry:
#                 # Add region_id and area_id from the SQL Alchemy response to the API entry
#                 api_entry["region_id"] = region_id
#                 api_entry["area_id"] = area_id
#                 api_entry['store_actual_id']=store_actual_id
#                 merged_data.append(api_entry)
#
#             else:
#                 api_entry={**sql_entry,
#                         "no_of_alert_generated": 0,
#                         "no_of_alert_received": 0,
#                         "no_of_alert_seen": 0,
#                         "no_of_alert_commented": 0}
#                 merged_data.append(api_entry)
#
#         return merged_data
#     else:
#         return [{**item,
#             "no_of_alert_generated": 0,
#             "no_of_alert_received": 0,
#             "no_of_alert_seen": 0,
#             "no_of_alert_commented": 0} for item in all]
def get_app_report(db,store_id=None,region_id=None,area_id=None,start_date=None,end_date=None,company_name=None):
    if start_date == None or end_date == "":
        start_date, end_date = current_date_time()
    all = db.query(Stores.name, Stores.id,Stores.store_actual_id, Stores.region_id, Stores.area_id).filter(
        Stores.store_running == 1) \
        .filter(or_(Stores.id == store_id, true() if store_id is None else false()),
                or_(Stores.region_id == region_id, true() if region_id is None else false()),
                or_(Stores.area_id == area_id, true() if area_id is None else false())
                ).all()

    url_ext="dashboard_report"
    headers = {
        'accept': 'application/json',
    }

    params = {
        'store_id': store_id,
        'start_date': start_date,
        'end_date': end_date,
        'company_name': company_name,
    }

    response1 = requests.post(BASE_URL+url_ext, params=params, headers=headers)
    if response1.status_code==200:
        response=response1.json()
        api_data_by_id = {entry["store_id"]: entry for entry in response["data"]}
        # Merge SQL Alchemy response with API response, adding region_id and area_id
        merged_data = []
        for sql_entry in all:
            store_name,store_id,store_actual_id, region_id, area_id = sql_entry
            api_entry = api_data_by_id.get(store_id)
            if api_entry:
                # Add region_id and area_id from the SQL Alchemy response to the API entry
                api_entry["region_id"] = region_id
                api_entry["area_id"] = area_id
                api_entry['store_actual_id']=store_actual_id
                merged_data.append(api_entry)

            else:
                api_entry={**sql_entry,
                        "no_of_alert_generated": 0,
                        "no_of_alert_received": 0,
                        "no_of_alert_seen": 0,
                        "no_of_alert_commented": 0}
                merged_data.append(api_entry)

        return merged_data
    else:
        return [{**item,
            "no_of_alert_generated": 0,
            "no_of_alert_received": 0,
            "no_of_alert_seen": 0,
            "no_of_alert_commented": 0} for item in all]



def get_notification_details(db, store_id=None, region_id=None, area_id=None, start_time=None, end_time=None):

    # try:
    if start_time == None or start_time == "":
        start_time, end_time = current_date_time()

    url_ext = "get_notification_detail_new?"
    if store_id:
        url_ext = url_ext+"&store_id={}".format(store_id)
    if start_time:
        url_ext = url_ext+"&start_data={}".format(start_time)
    if end_time:
        url_ext = url_ext+"&end_date={}".format(end_time)
    # print(BASE_URL+url_ext)
    # "?store_id=1&start_time=2022-12-01&end_time=2022-12-31"
    payload = {}
    headers = {
        'accept': 'application/json'
    }
    response = requests.post(BASE_URL+url_ext, headers=headers, data=payload, verify=False)
    if response.status_code == 200:
        response = response.json()
    else:
        response = {}
        response["data"] = []

    # pprint(response)
    result = db.query(AppUsers, Stores.name).join(Stores, Stores.id == AppUsers.store_id).filter(
        or_(AppUsers.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.region_id == region_id, true() if region_id is None else false()),
        or_(Stores.area_id == area_id, true() if area_id is None else false())
    ).all()
    # print(result[0][0].__dict__)
    result_dict = [dict(row[0].__dict__, store_name=row[1]) for row in result]
    # print(result_dict)
    output = []
    for i in result_dict:
        d = dict()
        d["store_id"] = i["store_id"]
        d["store_name"] = i["store_name"]
        del i["store_id"]
        del i["store_name"]
        for key, val in zip(list(i.keys()),list(i.values())):
            if val in response["data"]:
                val = dict(tabel_val = val,
                green_status = True)
            else:
                val = dict(tabel_val=val,
                           green_status=False)
            d[key] = val
        output.append(d)

    return output

def get_user_data(user_id):
    """
    :param user: 
    :return: json user_status is 1 means active 0 means inactive
    """
    url_ext = "get_user_data"
    payload = json.dumps({
        "user_id": user_id
    })
    headers = {
        'Content-Type': 'application/json'
    }
    response = requests.request("POST", BASE_URL+url_ext, headers=headers, data=payload, verify=False)
    return response.json()

def get_active_user_report(db,store_id=None, company_name=None,region_id=None,area_id=None,):
        all = db.query(Stores.name, Stores.id,Stores.store_actual_id, Stores.region_id, Stores.area_id).filter(
            Stores.store_running == 1) \
            .filter(or_(Stores.id == store_id, true() if store_id is None else false()),
                    or_(Stores.region_id == region_id, true() if region_id is None else false()),
                    or_(Stores.area_id == area_id, true() if area_id is None else false())
                    ).all()
        # print(all)
        url_ext = "user_dashboard_report"
        headers = {
            'accept': 'application/json',
        }
        params = {
            'store_id': store_id,
            'company_name': company_name,
        }
        response = requests.post(BASE_URL + url_ext, params=params, headers=headers)
        # if response.status_code == 200:
        #     response = response.json()['data']
        #     # print(response)
        #     filtered_main_dict = [item for item in response if
        #                           any(int(filter_item[1]) == int(item['store_id']) for filter_item in all)]
        #     filtered_main_dict={"data":filtered_main_dict}
        #     return filtered_main_dict
        # else:
        #     return False

        combined_data = []
        if response.status_code == 200:
            response = response.json()['data']
            for response_item in response:
                store_id = response_item['store_id']
                matching_all_item = next((filter_item for filter_item in all if int(filter_item[1]) == store_id), None)
                if matching_all_item:
                    combined_item = {
                        'store_id': store_id,
                        'name': response_item['name'],
                        'users': response_item['users'],
                        'store_actual_id': matching_all_item[2]
                    }
                    combined_data.append(combined_item)
            return combined_data
        else:
            return False


def update_user_password(details):
    url_ext = "update_user_password_new"
    payload = json.dumps({
        "user": details["user"],
        "user_password": details["user_password"]
    })
    headers = {
        'Content-Type': 'application/json'
    }
    response = requests.request("POST", BASE_URL + url_ext, headers=headers, data=payload, verify=False)
    return response.json()

def update_mobie_number(details):
    url_ext = "update_user_mobile_number_new"
    payload = json.dumps({
        "user": details["user"],
        "mobile_number": details["mobile_number"],
        "country_code": details["country_code"]
    })
    headers = {
        'Content-Type': 'application/json'
    }
    response = requests.request("POST", BASE_URL + url_ext, headers=headers, data=payload, verify=False)
    return response.json()

def update_user_status(details):
    url_ext = "update_user_status_new"
    payload = json.dumps({
        "user": details["user"],
        "user_status": details["user_status"]
    })
    headers = {
        'Content-Type': 'application/json'
    }
    response = requests.request("POST", BASE_URL + url_ext, headers=headers, data=payload, verify=False)
    return response.json()




def get_count_aisle_for_concealments(db: Session, store_id, region_id, area_id, start_time, end_time,company_name):
    if start_time is None or start_time == "":
        start_time, end_time = current_date_time()
    StoresAlias = aliased(Stores)
    TransactionsAlias = aliased(Transactions)

    # Create an alias for the master store list subquery
    master_stores = db.query(
        StoresAlias.id,
        StoresAlias.name.label('name'),
        StoresAlias.store_actual_id,
        StoresAlias.region_id,
        StoresAlias.area_id,
        StoresAlias.level_of_focus,
    ).filter(StoresAlias.store_running == 1,
        or_(StoresAlias.id == store_id, store_id is None),
        or_(StoresAlias.region_id == region_id, region_id is None),
        or_(StoresAlias.area_id == area_id, area_id is None)
    ).subquery()



    # # Create aliases for the subqueries
    subquery_aisle = db.query(
        master_stores.c.id,
        func.count(func.distinct(TransactionsAlias.transaction_id)).label('aisle_count')
    ).join(TransactionsAlias, TransactionsAlias.store_id == master_stores.c.id) \
        .filter(
        TransactionsAlias.source_id == 3,
        TransactionsAlias.hidden == 0,
        TransactionsAlias.triggers == 1,
        func.date(TransactionsAlias.begin_date) >= start_time,
        func.date(TransactionsAlias.begin_date) <= end_time
    ).group_by(
        master_stores.c.id
    ).subquery()

    subquery_intervention = db.query(
        master_stores.c.id,
        func.count(func.distinct(TransactionsAlias.transaction_id)).label('intervention_count')
    ).join(TransactionsAlias, TransactionsAlias.store_id == master_stores.c.id) \
        .filter(
        TransactionsAlias.source_id == 3,
        TransactionsAlias.hidden == 0,
        TransactionsAlias.triggers == 1,
        TransactionsAlias.intervention == 1,
        func.date(TransactionsAlias.begin_date) >= start_time,
        func.date(TransactionsAlias.begin_date) <= end_time
    ).group_by(
        master_stores.c.id
    ).subquery()

    # Combined query with LEFT JOINs
    combined_query = db.query(
        master_stores.c.id,
        master_stores.c.name,
        master_stores.c.store_actual_id,
        master_stores.c.region_id,
        master_stores.c.area_id,
        master_stores.c.level_of_focus,
        func.coalesce(subquery_aisle.c.aisle_count, 0).label('aisle_count'),
        func.coalesce(subquery_intervention.c.intervention_count, 0).label('intervention_count')
    ).outerjoin(subquery_aisle, master_stores.c.id == subquery_aisle.c.id) \
        .outerjoin(subquery_intervention, master_stores.c.id == subquery_intervention.c.id)

    results = combined_query.all()

    # Call the API to fetch data
    url_ext = "dashboard_report"
    headers = {
        'accept': 'application/json',
    }
    params = {
        'store_id': store_id,
        'start_date': start_time,
        'end_date': end_time,
        'company_name': company_name,
    }
    # response = requests.post(BASE_URL + url_ext, params=params, headers=headers)
    # if response.status_code == 200:
    #     api_response = response.json()
    #     # Extract the relevant fields from the API response
    #     api_data = api_response['data']
    #
    #     # Calculate the sum of no_of_alert_generated and (no_of_alert_generated - no_of_alert_received)
    #     # only for stores where store_id matches subquery_aisle store_id
    #     sum_alert_generated = 0
    #     sum_alert_difference = 0
    #
    #     for item in api_data:
    #         if item.get('store_id') is not None and item['store_id'] == subquery_aisle.c.id:
    #             sum_alert_generated += item.get('no_of_alert_generated', 0)
    #             sum_alert_difference += item.get('no_of_alert_generated', 0) - item.get('no_of_alert_received', 0)
    #
    #     # Add the sums to the results
    #     results = combined_query.first()
    #     # results.sum_alert_generated = sum_alert_generated
    #     # results.sum_alert_difference = sum_alert_difference
    #
    #     result_data = dict(results)
    #
    #     # Add the sums to the dictionary
    #     result_data['sum_alert_generated'] = sum_alert_generated
    #     result_data['sum_alert_difference'] = sum_alert_difference
    #
    #     # Return the dictionary as the result
    #     return result_data
    # else:
    #     # Handle API error
    #     results = None
    #
    # return results
    response1 = requests.post(BASE_URL + url_ext, params=params, headers=headers)
    api_response = response1.json()

    # Extract the relevant fields from the API response
    api_data = api_response['data']
    alert_data_map = {item['store_id']: item for item in api_data}

    # Convert the results to the desired format and merge with API data
    combined_result = []
    for result in results:
        store_id = result.id
        alert_data = alert_data_map.get(store_id, {})
        # print("alert_data:--",alert_data)
        no_of_alert_generated = alert_data.get('no_of_alert_generated', 0)
        no_of_alert_commented = alert_data.get('no_of_alert_commented', 0)
        no_of_alert_seen = alert_data.get('no_of_alert_seen',0)


        combined_result.append({
            "id": store_id,
            "name": result.name,
            "store_actual_id": result.store_actual_id,
            "region_id": result.region_id,
            "area_id": result.area_id,
            "level_of_focus": result.level_of_focus,
            "aisle_count": result.aisle_count,
            "intervention_count": result.intervention_count if result.intervention_count is not None else 0,
            "no_of_alert_generated": no_of_alert_generated,
            "no_of_alert_commented": no_of_alert_commented,
            "no_of_alert_seen":no_of_alert_seen
        })

    return combined_result