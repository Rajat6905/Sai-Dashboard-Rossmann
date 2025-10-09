import base64
import pandas as pd
from io import BytesIO
from fastapi import APIRouter, Depends, HTTPException, status, File, UploadFile, FastAPI, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials


router = APIRouter()
security = HTTPBearer()

from sqlalchemy.orm import Session
from database import get_db, get_db2
from utils import set_permissions
from crud.sco_main import (get_count_sco_main,
                           get_drop_down_info_stores,
                           get_top5_store_area,
                           get_top5_store_region,
                           get_main_count_data,
                           get_count_sco_main_by_month,
                           get_sco_main_active_status,
                           performance_comparison,
                           get_transaction_data,
                           get_drop_down_info_region,
                           get_drop_down_info_area,
                           get_top5_main_theft_data,
                           get_top5_theft_sco_res,
                           get_top5_theft_main_res,
                           get_top5_employee_performance_res,
                           add_comment_of_transection_id,
                           all_region, all_area,
                           report_top10_sco_main,
                           list_of_sco_main_theft,
                           get_comment_transaction_id,
                           get_store_region_area_,
    # get_sco_main_stats_for_month
                           )
from crud.role_base_sco_main import (store_region_area_wise_data_sco_main,
                                     store_region_area_wise_data_aisle)

from crud.aisle import (get_count_aisle,
                        get_top5_aisle_store_region,
                        get_top5_aisle_store_area,
                        get_count_aisle_stats,
                        get_top3_theft_current_month_data,
                        get_top3_intervention_current_month_data,
                        get_aisle_count_data,
                        get_ailse_transaction_data_by_id,
                        report_top10_aisle,
                        report_top10_intervention,
                        list_of_aisle_theft,
                        active_stores
                        )


from crud.search import search_by_id_res, search_by_transaction, delete_item
from crud.mobile import (get_notification_details,
                         get_count_aisle_for_concealments,
                         get_user_data,
                         update_user_password,
                         update_mobie_number,
                         update_user_status,
                         get_app_report,
                         get_active_user_report)

from crud.store import (get_issue_category,
                        store_issue_data,
                        store_map_data,
                        add_issue_comment)

from crud.status import fetch_camera_status_for_report, fetch_system_status_for_report, fetch_store_status_for_report
from crud.csv_file_into_db import (issue_description_data_update)
from crud.login import AuthHandler, user_validate
from crud.register_user import (create_user,
                                get_data_acc_to_stores, all_users)

from crud.aisle_images import get_aisle_images

from crud.machine_issue import add_data, get_data, get_all_data

from utils import set_permissions

auth_handler = AuthHandler()

from crud.login import AuthHandler, user_validate
from schemas import AuthDetails,PresignedUrlRequest
from crud.register_user import (create_user,
                                get_data_acc_to_stores, all_users)

from crud.for_xml_table_operations import upload_transaction, update_transaction_and_items

import boto3
# import config
from starlette.responses import StreamingResponse
from s3_utils import create_presigned_url
from schemas import UploadTransactionSkipRequestModel, UploadTransactionDetailsRequestModel, UploadTransactionToDbRequestModel, UploadTransactionStatusDetailsRequestModel
from crud.transactions import *
from crud.store import fetch_searched_store

auth_handler = AuthHandler()


@router.get("/health_check", status_code=status.HTTP_200_OK)
def health_check():
    return {"message": "ok"}


@router.get("/get_count_sco_main", status_code=status.HTTP_200_OK)  # Authorization Done
def get_count_sco_main_(db: Session = Depends(get_db), store_id: int = None, region: int = None, area: int = None,
                        start_time: str = None, end_time: str = None, token_data=Depends(auth_handler.auth_wrapper)):
    result = get_data_acc_to_stores(token_data, db)
    null_val = [None, ""]
    null_res = {"main_count": 0, "main_total": 0, "sco_count": 0, "sco_total": 0}
    if result['roles'] == "stores":
        if store_id not in null_val:
            return get_count_sco_main(db, store_id, region, area, start_time, end_time)
        return null_res
    elif result["roles"] == "region":
        if region not in null_val:
            return get_count_sco_main(db, store_id, region, area, start_time, end_time)
        return null_res
    elif result["roles"] == "area":
        if area not in null_val:
            return get_count_sco_main(db, store_id, region, area, start_time, end_time)
        return null_res
    else:
        return get_count_sco_main(db, store_id, region, area, start_time, end_time)


@router.get("/get_count_aisle", status_code=status.HTTP_200_OK)  # Authorization Done
def get_count_aisle_(db: Session = Depends(get_db), store_id: int = None, region: int = None, area: int = None,
                     start_time: str = None, end_time: str = None, token_data=Depends(auth_handler.auth_wrapper)):
    result = get_data_acc_to_stores(token_data, db)
    null_val = [None, ""]
    null_res = {"aisle_count": 0, "aisle_total": 0, "intervention_count": 0, "intervention_total": 0}
    if result['roles'] == "stores":
        if store_id not in null_val:
            return get_count_aisle(db, store_id, region, area, start_time, end_time)
        return null_res
    elif result["roles"] == "region":
        if region not in null_val:
            return get_count_aisle(db, store_id, region, area, start_time, end_time)
        return null_res
    elif result["roles"] == "area":
        if area not in null_val:
            return get_count_aisle(db, store_id, region, area, start_time, end_time)
        return null_res
    else:
        return get_count_aisle(db, store_id, region, area, start_time, end_time)


@router.get("/get_dropdown_info_stores", status_code=status.HTTP_200_OK)
def get_one_post(db: Session = Depends(get_db), region_id: int = None, area_id: int = None,
                 token_data=Depends(auth_handler.auth_wrapper)):
    result = get_data_acc_to_stores(token_data, db)
    if result["roles"] == "stores":
        return {"store_data": [{"id": int(d["id"]), "name": d["name"]} for d in eval(result["stores_name"])]}

    elif result["roles"] == "region":
        if region_id and region_id in eval(result["region_id"]):
            return get_drop_down_info_stores(db=db, region_id=region_id, area_id=area_id)
        else:
            return {"message": "No region found"}
    elif result["roles"] == "area":
        if area_id and area_id in eval(result["area_id"]):
            return get_drop_down_info_stores(db=db, region_id=region_id, area_id=area_id)
        else:
            return {"message": "No area found"}
    return get_drop_down_info_stores(db=db, region_id=region_id, area_id=area_id)


@router.get("/get_dropdown_info_region", status_code=status.HTTP_200_OK)
def get_one_post(db: Session = Depends(get_db), store_id: int = None, area_id: int = None,
                 token_data=Depends(auth_handler.auth_wrapper)):
    result = get_data_acc_to_stores(token_data, db)
    if result["roles"] == "region":
        return {"region": eval(result["region_id"])}
    elif result["roles"] == "stores":
        if store_id and store_id in eval(result["store_id"]):
            return get_drop_down_info_region(db=db, store_id=store_id, area_id=area_id)
        else:
            return {"message": "No Store found"}
    elif result["roles"] == "area":
        if area_id and area_id in eval(result["area_id"]):
            return get_drop_down_info_region(db=db, store_id=store_id, area_id=area_id)
        else:
            return {"message": "No area found"}
    return get_drop_down_info_region(db=db, store_id=store_id, area_id=area_id)


@router.get("/get_dropdown_info_area", status_code=status.HTTP_200_OK)
def get_area(db: Session = Depends(get_db), store_id: int = None, region_id: int = None,
             token_data=Depends(auth_handler.auth_wrapper)):
    result = get_data_acc_to_stores(token_data, db)
    if result["roles"] == "area":
        return {"area": eval(result["area_id"])}
    elif result["roles"] == "stores":
        if store_id and store_id in eval(result["store_id"]):
            return get_drop_down_info_area(db=db, store_id=store_id, region_id=region_id)
        else:
            return {"message": "No Store found"}
    elif result["roles"] == "region":
        if region_id and region_id in eval(result["region_id"]):
            return get_drop_down_info_area(db=db, store_id=store_id, region_id=region_id)
        else:
            return {"message": "No region found"}
    return get_drop_down_info_area(db=db, store_id=store_id, region_id=region_id)


@router.get("/get_dropdown_info_area", status_code=status.HTTP_200_OK)  # Authorization Done
def get_area(db: Session = Depends(get_db), store_name: str = None, region: str = None,
             token_data=Depends(auth_handler.auth_wrapper)):
    result = get_data_acc_to_stores(token_data, db)
    if result["roles"] == "area":
        return {"area": [int(i) for i in eval(result["area_id"])]}
    else:
        return get_drop_down_info_area(db=db, store_name=store_name, region=region)


@router.get("/get_top5_store_by_area", status_code=status.HTTP_200_OK)  # Authorization Done
def get_one_post(db: Session = Depends(get_db), area: int = None, start_time: str = None, end_time: str = None,
                 token_data=Depends(auth_handler.auth_wrapper)):
    result = get_data_acc_to_stores(token_data, db)
    null_val = [None, ""]
    null_res = {"top5_store_in_area_theft": [], "top5_store_in_area_non_theft": []}
    if result["roles"] == "admin":
        return get_top5_store_area(db=db, area_id=area, start_time=start_time, end_time=end_time)
    elif area in null_val:
        return null_res
    return get_top5_store_area(db=db, area_id=area, start_time=start_time, end_time=end_time)


@router.get("/get_top5_store_by_region", status_code=status.HTTP_200_OK)  # Authorization Done
def get_one_post(db: Session = Depends(get_db), region: int = None, start_time: str = None, end_time: str = None,
                 token_data=Depends(auth_handler.auth_wrapper)):
    null_val = [None, ""]
    null_res = {"top5_store_in_region_theft": [], "top5_store_in_region_non_theft": []}
    result = get_data_acc_to_stores(token_data, db)
    if result["roles"] == "admin":
        return get_top5_store_region(db=db, region_id=region, start_time=start_time, end_time=end_time)
    elif region in null_val:
        return null_res
    return get_top5_store_region(db=db, region_id=region, start_time=start_time, end_time=end_time)


@router.get("/get_sco_main_data", status_code=status.HTTP_200_OK)  # Authorization Done
def get_count_sco_main_(db: Session = Depends(get_db),
                        store_id: int = None, region_id: int = None, area_id: int = None, type: int = None,
                        start_time: str = None, end_time: str = None, page: int = 1, seen_status: int = 0,
                        token_data=Depends(auth_handler.auth_wrapper)):
    """
    :param db:
    :param store_id:
    :param region_id:
    :param area_id:
    :param type:
    :param start_time:
    :param end_time:
    :param page:
    :param seen_status: (0, 1) 0--> which is not seen yet and 1 --> which seens
    :return:
    """
    null_val = [None, ""]
    null_res = [[], {"total_count": 0}]
    result = get_data_acc_to_stores(token_data, db)
    if result["roles"] == "admin":
        return get_main_count_data(db, store_id, region_id, area_id, type, start_time, end_time, page, seen_status)
    elif result["roles"] == "stores":
        if store_id not in null_val:
            return get_main_count_data(db, store_id, region_id, area_id, type, start_time, end_time, page, seen_status)
        return null_res
    elif result["roles"] == "region":
        if region_id not in null_val:
            return get_main_count_data(db, store_id, region_id, area_id, type, start_time, end_time, page, seen_status)
        return null_res
    elif result["roles"] == "area":
        if area_id not in null_val:
            return get_main_count_data(db, store_id, region_id, area_id, type, start_time, end_time, page, seen_status)
        return null_res


@router.get("/get_transaction_data", status_code=status.HTTP_200_OK)
def get_transaction_data_by_ID(db: Session = Depends(get_db), transaction_id: str = None):
    return get_transaction_data(db, transaction_id)


@router.get("/get_sco_main_stats", status_code=status.HTTP_200_OK)  # Authorization Done
def get_count_sco_main_by_month_(db: Session = Depends(get_db),
                                 store_id: int = None,
                                 region: int = None, area: int = None,
                                 start_time: str = None, end_time: str = None,
                                 token_data=Depends(auth_handler.auth_wrapper)):
    null_val = [None, ""]
    null_res = {"Month": {"main": [], "sco": []},
                "Week": {"main": [], "sco": []},
                "Days": {"main": [], "sco": []}
                }
    result = get_data_acc_to_stores(token_data, db)
    if result["roles"] == "admin":
        return get_count_sco_main_by_month(db, store_id, region, area, start_time, end_time)
    elif result["roles"] == "stores":
        if store_id not in null_val:
            return get_count_sco_main_by_month(db, store_id, region, area, start_time, end_time)
        return null_res
    elif result["roles"] == "region":
        if region not in null_val:
            return get_count_sco_main_by_month(db, store_id, region, area, start_time, end_time)
        return null_res
    elif result["roles"] == "area":
        if area not in null_val:
            return get_count_sco_main_by_month(db, store_id, region, area, start_time, end_time)
        return null_res


@router.get("/get_top5_theft_sco", status_code=status.HTTP_200_OK)
def get_top5_theft_sco(db: Session = Depends(get_db), start_time: str = None, end_time: str = None):
    return get_top5_theft_sco_res(db, start_time, end_time)


@router.get("/get_top5_theft_main", status_code=status.HTTP_200_OK)
def get_top5_theft_main(db: Session = Depends(get_db), start_time: str = None, end_time: str = None):
    return get_top5_theft_main_res(db, start_time, end_time)


@router.get("/get_top5_employee_performance", status_code=status.HTTP_200_OK)
def get_top5_employee_performance(db: Session = Depends(get_db), store_id: int = None, region_id: int = None,
                                  area_id: int = None, start_time: str = None, end_time: str = None):
    return get_top5_employee_performance_res(db, store_id, region_id, area_id, start_time, end_time)


@router.post("/add_comment_of_transection_id", status_code=status.HTTP_200_OK)
def add_comment_of_transection_id_(details: dict, db: Session = Depends(get_db)):
    add_comment_of_transection_id(db, details)
    return details


# Aisle
@router.get("/get_top5_aisle_store_by_region", status_code=status.HTTP_200_OK)  # Authorization Done
def get_top5_aisle_store_region_data(db: Session = Depends(get_db), region: int = None, start_time: str = None,
                                     end_time: str = None,
                                     token_data=Depends(auth_handler.auth_wrapper)):
    null_val = [None, ""]
    null_res = {"top5_aisle_store_in_region_theft": [], "top5_aisle_store_in_region_non_theft": []}
    result = get_data_acc_to_stores(token_data, db)
    if result["roles"] == "admin":
        return get_top5_aisle_store_region(db, region, start_time, end_time)
    elif region in null_val:
        return null_res
    return get_top5_aisle_store_region(db, region, start_time, end_time)


@router.get("/get_top5_aisle_store_by_area", status_code=status.HTTP_200_OK)  # Authorization Done
def get_top5_aisle_store_area_data(db: Session = Depends(get_db), area: int = None, start_time: str = None,
                                   end_time: str = None,
                                   token_data=Depends(auth_handler.auth_wrapper)):
    null_val = [None, ""]
    null_res = {"top5_aisle_store_in_area_theft": [], "top5_aisle_store_in_area_non_theft": []}
    result = get_data_acc_to_stores(token_data, db)
    if result["roles"] == "admin":
        return get_top5_aisle_store_area(db, area, start_time, end_time)
    elif area in null_val:
        return null_res
    return get_top5_aisle_store_area(db, area, start_time, end_time)


@router.get("/get_sco_main_active_status", status_code=status.HTTP_200_OK)
def get_sco_main_active_status_data(db: Session = Depends(get_db), store_id: int = None):
    return get_sco_main_active_status(db, store_id)


@router.get("/get_sco_main_performance_comparison", status_code=status.HTTP_200_OK)
def get_sco_main_performance_comparison_data(db: Session = Depends(get_db), store_id: int = None,
                                             start_time: str = None, end_time: str = None):
    return performance_comparison(db, store_id, start_time, end_time)


# @router.get("/get_sco_main_stats_for_month", status_code=status.HTTP_200_OK)
# def get_test(db:Session=Depends(get_db), store_id:int=None, start_date:str=None, end_date:str=None):
#     return get_sco_main_stats_for_month(db, store_id, start_date, end_date)

@router.get("/get_count_aisle_stats", status_code=status.HTTP_200_OK)  # Authorization Done
def get_count_aisle_stats_data(db: Session = Depends(get_db),
                               store_id: int = None, region: int = None, area: int = None,
                               start_time: str = None, end_time: str = None,
                               token_data=Depends(auth_handler.auth_wrapper)):
    null_val = [None, ""]
    null_res = {"aisle_month": [], "aisle_intervention_month": [],
                "aisle_week": [], "aisle_intervention_week": [],
                "aisle_days": [], "aisle_intervention_daye": []}
    result = get_data_acc_to_stores(token_data, db)
    if result["roles"] == "admin":
        return get_count_aisle_stats(db, store_id, region, area, start_time, end_time)
    elif result["roles"] == "stores":
        if store_id not in null_val:
            return get_count_aisle_stats(db, store_id, region, area, start_time, end_time)
        return null_res
    elif result["roles"] == "region":
        if region not in null_val:
            return get_count_aisle_stats(db, store_id, region, area, start_time, end_time)
        return null_res
    elif result["roles"] == "area":
        if area not in null_val:
            return get_count_aisle_stats(db, store_id, region, area, start_time, end_time)
        return null_res


@router.get("/get_top5_sco_main_theft", status_code=status.HTTP_200_OK)
def get_top5_main_theft_fun(db: Session = Depends(get_db), store_id: str = None, region_id: str = None,
                            area_id: str = None, start_time: str = None, end_time: str = None):
    return get_top5_main_theft_data(db, store_id, region_id, area_id, start_time, end_time)


@router.get("/get_top3_aisle_theft_current_month", status_code=status.HTTP_200_OK)
def get_top3_theft_current_month(db: Session = Depends(get_db)):
    return get_top3_theft_current_month_data(db)


@router.get("/get_top3_aisle_intervention_current_month", status_code=status.HTTP_200_OK)
def get_top3_intervention_current_month(db: Session = Depends(get_db)):
    return get_top3_intervention_current_month_data(db)


@router.get("/get_aisle_intervention_data", status_code=status.HTTP_200_OK)  # Authorization Done
def get_count_aisle_intervention_(db: Session = Depends(get_db), store_id: int = None, region_id: int = None,
                                  area_id: int = None, is_intervention: int = 0,
                                  start_time: str = None, end_time: str = None,
                                  page: int = 1, seen_status: int = 0,
                                  token_data=Depends(auth_handler.auth_wrapper)):
    """
    :param db:
    :param store_id:
    :param region_id:
    :param area_id:
    :param is_intervention:
    :param start_time:
    :param end_time:
    :param page:
    :param seen_status: (0, 1) 0--> which is not seen yet and 1 --> which seens
    :return:
    """
    null_val = [None, ""]
    null_res = [[], {"total_count": 0}]
    result = get_data_acc_to_stores(token_data, db)
    if result["roles"] == "admin":
        return get_aisle_count_data(db, store_id, region_id, area_id, is_intervention, start_time, end_time, page,
                                    seen_status)
    elif result["roles"] == "stores":
        if store_id not in null_val:
            return get_aisle_count_data(db, store_id, region_id, area_id, is_intervention, start_time, end_time, page,
                                        seen_status)
        return null_res
    elif result["roles"] == "region":
        if region_id not in null_val:
            return get_aisle_count_data(db, store_id, region_id, area_id, is_intervention, start_time, end_time, page,
                                        seen_status)
        return null_res
    elif result["roles"] == "area":
        if area_id not in null_val:
            return get_aisle_count_data(db, store_id, region_id, area_id, is_intervention, start_time, end_time, page,
                                        seen_status)
        return null_res
    # return get_aisle_count_data(db, store_id, region_id, area_id, is_intervention, start_time, end_time, page, seen_status)


# Search API
@router.get("/search_by_id", status_code=status.HTTP_200_OK)
async def search_by_id(search_query: str = None, db: Session = Depends(get_db), page: int = None, store_id: int = None,
                       region_id: int = None, area_id: int = None):
    return search_by_id_res(db, search_query, page, store_id, region_id, area_id)


# App Notifications Details
@router.get("/get_notification_details")  # Authorization Done
def get_notification_details_(db: Session = Depends(get_db), store_id: int = None,
                              region_id: int = None, area_id: int = None,
                              start_time: str = None, end_time: str = None,
                              token_data=Depends(auth_handler.auth_wrapper)):
    null_val = [None, ""]
    null_res = [{}]
    result = get_data_acc_to_stores(token_data, db)
    if result["roles"] == "admin":
        return get_notification_details(db, store_id, region_id, area_id, start_time, end_time)
    elif result["roles"] == "stores":
        if store_id not in null_val:
            return get_notification_details(db, store_id, region_id, area_id, start_time, end_time)
        return null_res
    elif result["roles"] == "region":
        if region_id not in null_val:
            return get_notification_details(db, store_id, region_id, area_id, start_time, end_time)
        return null_res
    elif result["roles"] == "area":
        if area_id not in null_val:
            return get_notification_details(db, store_id, region_id, area_id, start_time, end_time)
        return null_res


@router.post("/get_user_data", status_code=status.HTTP_200_OK)
def get_user_data_(details: dict):
    return get_user_data(details["user_id"])


@router.post("/update_user_password")
def update_user_password_(details: dict):
    return update_user_password(details)


@router.post("/update_mobile_no")
def update_mobie_number_(details: dict):
    return update_mobie_number(details)


@router.post("/update_user_status")
def update_user_status_(details: dict):
    return update_user_status(details)


# Store Issue
@router.get("/get_issue_category")
def issue_category_(db: Session = Depends(get_db)):
    return get_issue_category(db)


@router.get("/get_store_issue_data")  # Authorization Done
def store_issue_data_(db: Session = Depends(get_db), store_id: int = None,
                      region_id: int = None, area_id: int = None,
                      category_id: int = None, page: int = 1,
                      token_data=Depends(auth_handler.auth_wrapper)):
    null_val = [None, ""]
    null_res = [[{}], {"total_count": 0}]
    result = get_data_acc_to_stores(token_data, db)
    if result["roles"] == "admin":
        return store_issue_data(db, store_id, region_id, area_id, category_id, page)
    elif result["roles"] == "stores":
        if store_id not in null_val:
            return store_issue_data(db, store_id, region_id, area_id, category_id, page)
        return null_res
    elif result["roles"] == "region":
        if region_id not in null_val:
            return store_issue_data(db, store_id, region_id, area_id, category_id, page)
        return null_res
    elif result["roles"] == "area":
        if area_id not in null_val:
            return store_issue_data(db, store_id, region_id, area_id, category_id, page)
        return null_res


@router.get("/get_store_map_data")
def store_map_data_(db: Session = Depends(get_db), store_id: int = None, region_id: int = None, area_id: int = None):
    return store_map_data(db, store_id, region_id, area_id)


@router.get("/get_all_region")  # Authorization Done
def all_region_(db: Session = Depends(get_db), store_id: int = None, area_id: int = None,
                token_data=Depends(auth_handler.auth_wrapper)):
    result = get_data_acc_to_stores(token_data, db)
    if result["roles"] == "admin":
        return all_region(db, store_id, area_id)
    elif result["roles"] == "region":
        return {"all_region": [int(i) for i in eval(result["region_id"])]}
    else:
        # return all_region(db, store_id, area_id)
        return {"all_region": []}


@router.get("/get_all_area")  # Authorization Done
def all_area_(db: Session = Depends(get_db), token_data=Depends(auth_handler.auth_wrapper)):
    result = get_data_acc_to_stores(token_data, db)
    if result["roles"] == "admin":
        return all_area(db)
    elif result["roles"] == "area":
        return {"all_area": [int(i) for i in eval(result["area_id"])]}
    else:
        return {"all_area": []}


@router.post("/add_comment_of_store_issue", status_code=status.HTTP_200_OK)
def add_issue_comment_(details: dict, db: Session = Depends(get_db)):
    return add_issue_comment(db, details)


@router.post("/get_ailse_transaction_data", status_code=status.HTTP_200_OK)
def get_ailse_transaction_data_by_id_(details: dict, db: Session = Depends(get_db)):
    return get_ailse_transaction_data_by_id(details, db)


@router.get("/report_top10_sco_main", status_code=status.HTTP_200_OK)
def report_top10_sco_main_(db: Session = Depends(get_db), store_id: int = None, region_id: int = None,
                           area_id: int = None, start_time: str = None, end_time: str = None):
    return report_top10_sco_main(db, store_id, region_id, area_id, start_time, end_time)


@router.get("/report_top10_aisle", status_code=status.HTTP_200_OK)
def report_top10_aisle_(db: Session = Depends(get_db), store_id: int = None, region_id: int = None, area_id: int = None,
                        start_time: str = None, end_time: str = None):
    return report_top10_aisle(db, store_id, region_id, area_id, start_time, end_time)


@router.get("/report_top10_intervention", status_code=status.HTTP_200_OK)
def report_top10_intervention_(db: Session = Depends(get_db), store_id: int = None, region_id: int = None,
                               area_id: int = None, start_time: str = None, end_time: str = None):
    return report_top10_intervention(db, store_id, region_id, area_id, start_time, end_time)


@router.get("/report_list_of_aisle_theft", status_code=status.HTTP_200_OK)
def list_of_aisle_theft_(db: Session = Depends(get_db), store_id: int = None, region_id: int = None,
                         area_id: int = None, start_time: str = None, end_time: str = None, company_name: str = None):
    return list_of_aisle_theft(db, store_id, region_id, area_id, start_time, end_time, company_name)


@router.get("/report_list_of_sco_main_theft", status_code=status.HTTP_200_OK)
def list_of_sco_main_theft_(db: Session = Depends(get_db), store_id: int = None, region_id: int = None,
                            area_id: int = None, start_time: str = None, end_time: str = None):
    return list_of_sco_main_theft(db, store_id, region_id, area_id, start_time, end_time)


@router.post("/login")
def login_(details: dict, db: Session = Depends(get_db)):
    is_valid = user_validate(details, db)
    if is_valid:
        hash_pwd = is_valid.password
        if auth_handler.verify_password(details["password"], hash_pwd):
            token = {"access_token": auth_handler.access_encode_token(details["email"], is_valid.roles),
                     "refresh_token": auth_handler.refresh_encode_token(details["email"], is_valid.roles)
                     }
            roles = set_permissions(is_valid.roles)
            return {"tokens": token, "roles": roles, "email": details["email"]}
        else:
            return HTTPException(status_code=401, detail="Unauthorized")
    else:
        return HTTPException(status_code=404, detail="User not found")


@router.get("/active_stores", status_code=status.HTTP_200_OK)
def active_stores_(db: Session = Depends(get_db), store_id: int = None, region_id: int = None, area_id: int = None):
    return active_stores(db, store_id=store_id, region_id=region_id, area_id=area_id)


@router.post("/create_user", status_code=status.HTTP_200_OK)
def create_user_(details: dict, db: Session = Depends(get_db), token_data=Depends(auth_handler.auth_wrapper)):
    result = get_data_acc_to_stores(token_data, db)
    if result["roles"] == "admin":
        # print(details)
        details["password"] = auth_handler.get_password_hash(details["password"])
        # return details
        return create_user(details, db)
    raise HTTPException(status_code=401, detail="Unauthorized")


@router.post("/get_comment_transaction_id", status_code=status.HTTP_200_OK)
def get_comment_(details: dict, db: Session = Depends(get_db)):
    return get_comment_transaction_id(details, db)


@router.get("/all_users", status_code=status.HTTP_200_OK)  # Authorization Done
def get_all_users(db: Session = Depends(get_db), token_data=Depends(auth_handler.auth_wrapper)):
    result = get_data_acc_to_stores(token_data, db)
    if result["roles"] == "admin":
        return all_users(db)
    raise HTTPException(status_code=401, detail="Unauthorized")


@router.post("/upload_issue_description")
async def create_upload_file(file: UploadFile = File(...), db: Session = Depends(get_db)):
    # Read the uploaded file using BytesIO
    contents = await file.read()
    issue_description_data_update(contents, db)
    return {"filename": file.filename}


@router.get("/all_users", status_code=status.HTTP_200_OK)  # Authorization Done
def get_all_users(db: Session = Depends(get_db), token_data=Depends(auth_handler.auth_wrapper)):
    result = get_data_acc_to_stores(token_data, db)
    if result["roles"] == "admin":
        return all_users(db)
    raise HTTPException(status_code=401, detail="Unauthorized")


@router.get("/store_region_area_wise_data_sco_main", status_code=status.HTTP_200_OK)
def store_region_area_wise_data_sco_main_(db: Session = Depends(get_db),
                                          token_data=Depends(auth_handler.auth_wrapper),
                                          type: str = None,
                                          store_id: int = None,
                                          region_id: int = None,
                                          area_id: int = None,
                                          start_time: str = None, end_time: str = None):
    return store_region_area_wise_data_sco_main(db, type, store_id, region_id, area_id, start_time, end_time)


@router.get("/store_region_area_wise_data_aisle", status_code=status.HTTP_200_OK)
def store_region_area_wise_data_aisle_(db: Session = Depends(get_db),
                                       token_data=Depends(auth_handler.auth_wrapper),
                                       type: str = None,
                                       store_id: int = None,
                                       region_id: int = None,
                                       area_id: int = None,
                                       start_time: str = None, end_time: str = None):
    return store_region_area_wise_data_aisle(db, type, store_id, region_id, area_id, start_time, end_time)


@router.get("/get_aisle_images", status_code=status.HTTP_200_OK)
def get_aisle_images_data(db: Session = Depends(get_db), store_id: str = None):
    return get_aisle_images(db, store_id)


@router.get("/get_app_report", status_code=status.HTTP_200_OK)
def get_app_report_data(db: Session = Depends(get_db), store_id: str = None, region_id: int = None, area_id: int = None,
                        start_time: str = None, end_time: str = None, company_name: str = None):
    return get_app_report(db, store_id, region_id, area_id, start_time, end_time, company_name)


@router.get("/get_active_user_report", status_code=status.HTTP_200_OK)
def get_active_user_report_data(db: Session = Depends(get_db), store_id: str = None, company_name: str = None,
                                region_id: int = None, area_id: int = None, ):
    return get_active_user_report(db, store_id, company_name, region_id, area_id)


@router.get("/list_of_concealments", status_code=status.HTTP_200_OK)  # Authorization Done
def get_list_of_concealments(db: Session = Depends(get_db), store_id: int = None, region_id: int = None,
                             area_id: int = None, start_time: str = None, end_time: str = None, company_name=None,
                             token_data=Depends(auth_handler.auth_wrapper)):
    # print(store_id, region_id)
    result = get_data_acc_to_stores(token_data, db)
    null_val = [None, ""]
    null_res = {"aisle_count": 0, "aisle_total": 0, "intervention_count": 0, "intervention_total": 0}
    if result['roles'] == "stores":
        if store_id not in null_val:
            return get_count_aisle_for_concealments(db, store_id, region_id, area_id, start_time, end_time,
                                                    company_name)
        return null_res
    elif result["roles"] == "region":
        if region_id not in null_val:
            return get_count_aisle_for_concealments(db, store_id, region_id, area_id, start_time, end_time,
                                                    company_name)
        return null_res
    elif result["roles"] == "area":
        if area_id not in null_val:
            return get_count_aisle_for_concealments(db, store_id, region_id, area_id, start_time, end_time,
                                                    company_name)
        return null_res
    else:
        return get_count_aisle_for_concealments(db, store_id, region_id, area_id, start_time, end_time, company_name)


@router.get("/saml_login", status_code=status.HTTP_200_OK)
def saml_login_(db: Session = Depends(get_db), url: str = None):
    if url == None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
    try:
        details = {"email": base64.b64decode(url).decode()}
        is_valid = user_validate(details, db)
        if is_valid:
            token = {"access_token": auth_handler.access_encode_token(details["email"], is_valid.roles),
                     "refresh_token": auth_handler.refresh_encode_token(details["email"], is_valid.roles)
                     }
            roles = set_permissions(is_valid.roles)
            return {"tokens": token, "roles": roles, "email": details["email"]}
        return HTTPException(status_code=404, detail="User not found")
    except:
        return HTTPException(status_code=404, detail="User not found")


@router.post("/add_machine_issue")
def add_machine_issue(details: dict, db: Session = Depends(get_db)):
    store_id = details['store_id']
    camera_issue = details.get('camera_issue', None)
    not_responding = details.get('not_responding', None)
    network_issue = details.get('network_issue', None)

    if add_data(db, store_id, camera_issue, not_responding, network_issue):
        return {"message": "success"}
    else:
        return HTTPException(status_code=404, detail="Someting went wrong")


@router.get("/get_machine_issue")
def get_machine_issue_data(db: Session = Depends(get_db), store_id: str = None):
    data = get_data(db, store_id)
    if data:
        return data
    else:
        return HTTPException(status_code=404, detail="No data found !")


@router.get("/get_machine_issue_all")
def get_machine_issue_data(db: Session = Depends(get_db), store_id: int = None, page: int = 1):
    data = get_all_data(db, page, store_id)
    if data:
        return data
    else:
        return HTTPException(status_code=404, detail="No data found !")




@router.get("/search-transaction", status_code=status.HTTP_200_OK)
def search_transaction_(db:Session=Depends(get_db),db1 :Session=Depends(get_db2), transaction_id:str=None):
    return search_by_transaction(db, db1, transaction_id)


@router.get("/upload-transaction", status_code=status.HTTP_200_OK)
def upload_transaction_(db2:Session=Depends(get_db2), db:Session=Depends(get_db), transaction_id:str=None, counter_type:int=2):
    val = upload_transaction(db2, db, transaction_id=transaction_id, counter_type=counter_type)
    return val


@router.post("/update_transactions_and_items", status_code=status.HTTP_200_OK)
def update_transaction_and_items_(details:dict, transaction_id:str, db: Session = Depends(get_db)):
    return update_transaction_and_items(details, transaction_id, db)


@router.get("/delete_item", status_code=status.HTTP_200_OK)
def delete_item_(db:Session=Depends(get_db), db_id:int=None):
    return delete_item(db, db_id)


@router.get("/camera-status-report")
def get_camera_status_report(store_id: int = None, region_id: int = None, area_id: int = None, page: int = 1,
                             per_page: int = 10, token_data=Depends(auth_handler.auth_wrapper),
                             db: Session = Depends(get_db)):
    result, count = fetch_camera_status_for_report(db, store_id, region_id, area_id, page, per_page)
    return {"data": result, "count": count}


@router.get("/system-status-report")
def get_system_status_report(store_id: int = None, region_id: int = None, area_id: int = None, page: int = 1,
                             per_page: int = 10, token_data=Depends(auth_handler.auth_wrapper),
                             db: Session = Depends(get_db)):
    result, count = fetch_system_status_for_report(db, store_id, region_id, area_id, page, per_page)
    return {"data": result, "count": count}


@router.get("/store-status-report", status_code=status.HTTP_200_OK)
def get_store_status_report(db: Session = Depends(get_db), token_data=Depends(auth_handler.auth_wrapper)):
    data_camera, data_store = fetch_store_status_for_report(db)

    df_camera = pd.DataFrame(data_camera, columns=["Store ID", "Store Name", "Camera Number", "Status",
                                                   "Checked On", "Last Seen"])
    df_store = pd.DataFrame(data_store, columns=["Store ID", "Store Name", "Status", "Checked On", "Last Seen"])

    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        df_camera.to_excel(writer, sheet_name="Cameras", index=False)
        df_store.to_excel(writer, sheet_name="Stores", index=False)

    buffer.seek(0)

    return StreamingResponse(buffer, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                             headers={
                                 "Content-Disposition": "attachment; filename=StoreCameraStatusReport.xlsx"
                             })


@router.post("/presigned-url")
def get_presigned_url(body: PresignedUrlRequest, token_data=Depends(auth_handler.auth_wrapper)):
    body = body.dict()
    object_name = body.get("object_name")
    expires_in = body.get("expires_in")

    res = create_presigned_url(object_name,expires_in)

    if not res:
        raise HTTPException(status_code=500, detail="Error generating presigned URL")

    return {"presigned_url": res}

@router.get("/search-store", status_code=status.HTTP_200_OK)
def get_search_store(
    db: Session = Depends(get_db), 
    token_data=Depends(auth_handler.auth_wrapper)
):
    result = fetch_searched_store(db)
    return result

@router.post("/upload-transaction-list", status_code=status.HTTP_200_OK)
def upload_transaction_details(
    body: UploadTransactionDetailsRequestModel,
    db: Session = Depends(get_db2),
    token_data=Depends(auth_handler.auth_wrapper)
):
    body = body.dict()
    stores_id_list = body.get("store_ids", [])
    start_time = body.get("start_time", None)
    end_time = body.get("end_time", None)
    nudge_type = body.get("nudge_type", "all")
    transaction_id, item_details, missed_items_details, store_name, store_id = fetch_upload_transaction_details(
        stores_id_list=stores_id_list, start_time=start_time, end_time=end_time, db_xml_dev=db,nudge_type=nudge_type
    )

    transaction_date = missed_items_details[0].beginDate if missed_items_details else None
    sequence_number = item_details[0].sequence_no if item_details else None
    till_number = item_details[0].counterno if item_details else None
    nudge_count = len(missed_items_details) if missed_items_details else 0
    
    if not missed_items_details:
        return {}
    
    if nudge_type == "not_attended":
        video_url = f"Videos/{transaction_date[:10]}/{str(store_id)}/unzip/not_attended/{transaction_id}.mp4"
    else:
        video_url = f"Videos/{transaction_date[:10]}/{str(store_id)}/unzip/alerts/{transaction_id}.mp4"

    items = []
    for record in item_details:
        items.append({
            "item_name": record.Name,
            "item_quantity": int(record.Quantity) if record.Quantity else 0,
            "item_date": record.BeginDateTime.replace("T", " "),
            "missed_items": False
        })
    
    for record in missed_items_details:
        items.append({
            "item_name": "Item",
            "item_quantity": 1,
            "item_date": record.beginDate.replace("T", " "),
            "missed_items": True
        })

    items = sorted(items, key=lambda x: x["item_date"])
    transaction_date = items[0]["item_date"] if items else None

    return {
        "transaction_id": transaction_id,
        "store_name": store_name,
        "store_id": store_id,
        "sequence_number": sequence_number,
        "till_number": till_number,
        "nudge_count": nudge_count,
        "transaction_date": transaction_date,
        "video_url": video_url,
        "items": items
    }


@router.put("/upload-transaction-to-db", status_code=status.HTTP_200_OK)
def upload_transaction_to_db(
    body: UploadTransactionToDbRequestModel,
    db: Session = Depends(get_db),
    db_xml_dev_db: Session = Depends(get_db2),
    token_data=Depends(auth_handler.auth_wrapper)
):
    body = body.dict()
    transaction_id = body.get("transaction_id", None)
    store_id = body.get("store_id", None)
    description = body.get("description", None)
    clubcard = body.get("clubcard", None)
    nudge_type = body.get("nudge_type", "all")

    try:
        insert_transaction_to_dashboard_db(transaction_id, description, clubcard, store_id, db, db_xml_dev_db,nudge_type=nudge_type)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail=f"Error uploading transaction to dashboard: {str(e)}"
        )

    update_transaction_entry_status(transaction_id, db_xml_dev_db)

    return {
        "message": "Transaction uploaded to dashboard successfully"
    }


@router.post("/upload-transaction-skip", status_code=status.HTTP_200_OK)
def upload_transaction_skip(
    body: UploadTransactionSkipRequestModel,
    db_xml_dev_db: Session = Depends(get_db2),
    token_data=Depends(auth_handler.auth_wrapper)
):
    body = body.dict()
    transaction_id = body.get("transaction_id", None)

    try:
        upload_transaction_skip_op(transaction_id, db_xml_dev_db)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error uploading transaction skip: {str(e)}")

    return {"message": "Transaction skipped successfully"}


@router.post("/upload-transaction-status-details", status_code=status.HTTP_200_OK)
def upload_transaction_status_details(
    body: UploadTransactionStatusDetailsRequestModel,
    db: Session = Depends(get_db2),
    token_data=Depends(auth_handler.auth_wrapper)
):
    body = body.dict()
    store_ids = body.get("store_ids", [])
    start_time = body.get("start_time", None)
    end_time = body.get("end_time", None)
    nudge_type = body.get("nudge_type", "all")

    entry_status_result, skip_result = fetch_transaction_status_details(store_ids, start_time, end_time, db, nudge_type=nudge_type)

    entry_status_dict = {record.id: record for record in entry_status_result}
    skip_dict = {record.id: record for record in skip_result}

    store_id_result = set(list(entry_status_dict.keys()) + list(skip_dict.keys()))

    res = []
    total_entry_status_count = 0
    total_skip_count = 0
    for store_id in store_id_result:
        store_name = None
        if store_id in entry_status_dict:
            store_name = entry_status_dict.get(store_id).name
        elif store_id in skip_dict:
            store_name = skip_dict.get(store_id).name

        entry_status_count = 0
        if store_id in entry_status_dict:
            entry_status_count = entry_status_dict.get(store_id).entry_status_count
        
        skip_count = 0
        if store_id in skip_dict:
            skip_count = skip_dict.get(store_id).skip_count

        total_entry_status_count += entry_status_count
        total_skip_count += skip_count

        if store_name:
            res.append({
                "store_id": store_id,
                "store_name": store_name,
                "entry_status_count": entry_status_count,
                "skip_count": skip_count
            })

    print("total_entry_status_count: ", total_entry_status_count)
    print("total_skip_count: ", total_skip_count)

    return {"data": res}