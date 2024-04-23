from sqlalchemy import func, and_, distinct, desc, or_, true, false, asc, extract, text
from model.model import Transaction_SCO, Transaction_Main,Transaction_Details_SCO, Transaction_Details_Main,Stores#, Transactions, Operators, Transaction_items
from model.model_dummy_database import Transactions, Operators, Transaction_items
from sqlalchemy.orm import Session, aliased
from datetime import datetime, timedelta
import itertools
# import pandas as pd

from dateutil import parser

from utils import (current_date_time,
                   get_last_3_months_from_current_date,
                   add_values_stats,
                   sort_by_year_month,
                   sort_by_year_month_week,
                   sort_by_year_month_day,
                   merge_dicts,
                   all_store_count_list_of_sco_main)
from operator import itemgetter
import requests
import json

def update_transactions_table(db2, db, sco, transaction_item_sco, transaction_id, counter_type):
    store_id = transaction_item_sco.store_id
    o_id = transaction_item_sco.operator_id
    if o_id == "":
        operator_id = 150
    else:
        operator_id = db.query(Operators).filter(Operators.operator_id == o_id).first()
        if operator_id:
            operator_id = operator_id.id
        else:
            o_val = Operators(operator_id=o_id,
                              store_id=store_id)
            db.add(o_val)
            db.commit()
            operator_id = o_val.id
    data = dict(transaction_id=sco.transactionId,
                sequence_no=transaction_item_sco.sequence_no,
                store_id=sco.storeId,
                counter_no=sco.counterNo,
                source_id=counter_type,
                operator_id=operator_id,
                description="",
                begin_date=sco.beginDate,
                end_date=sco.endDate,
                staffcard=sco.staffcard,
                missed_scan=1,
                video_link= str(transaction_id)+".mp4",
                extended_total_amount=sco.extendedTotalAmount,
                total_number_of_items=sco.totalNumberOfItems,
                cardno=sco.cardno)
    transaction = Transactions(**data)
    db.add(transaction)
    if counter_type == 2:

        transaction_items = db2.query(Transaction_Details_SCO).filter(
            Transaction_Details_SCO.TransactionID == transaction_id).all()

    elif counter_type==1:
        transaction_items = db2.query(Transaction_Details_Main).filter(
            Transaction_Details_Main.TransactionID == transaction_id).all()



    # print(transaction_items)
    transaction_items_data = [{"name":val.Name,
                               "transaction_id":val.TransactionID,
                               "transaction_type": val.transaction_type,
                               "pos_item_id":val.POSItemID,
                               "item_id":val.ItemID,
                               "regular_sales_unit_price":val.RegularSalesUnitPrice,
                               "actual_sales_unit_price":val.ActualSalesUnitPrice,
                               "extended_amount":val.ExtendedAmount,
                               "quantity":val.Quantity,
                               "checked_quantity":val.Quantity,
                               "missed": 0,
                               "overscan": 0,
                               "trigger_id": 0,
                               "scan_data":val.ScanData,
                               "begin_date_time": val.BeginDateTime,
                               "end_date_time": val.EndDateTime}
                              for val in transaction_items]
    # print(transaction_items_data)
    transactions_items_data_insert = [Transaction_items(**row) for row in transaction_items_data]
    db.add_all(transactions_items_data_insert)
    db.commit()
    return data, transaction_items_data


def move_data_in_s3(date,store_actual,count,video_name):
    url = "https://k9ga2mekwk.execute-api.eu-west-2.amazonaws.com/s1/copy_file"

    headers = {
        "Content-Type": "application/json"
    }

    # Parse the string into a datetime object
    # date = datetime.fromisoformat(date)
    # Extract the date

    parsed_date = parser.parse(date)
    begin_date = parsed_date.date()


    data = {
        "date": str(begin_date),
        "store_id": store_actual,
        "count": count,
        "bucket_name": "rossman2",
        "video_name": video_name
    }
    print(data)
    response = requests.post(url, headers=headers, data=json.dumps(data))

    if response.status_code == 200:
        pass
        # print("Request successful")
        # print(response.json())  # If you want to see the response JSON
    else:
        pass
        # print("Request failed with status code:", response.status_code)

def upload_transaction(db2: Session, db:Session, transaction_id, counter_type):
    is_exists = db.query(Transactions).filter(Transactions.transaction_id == transaction_id,Transactions.source_id==counter_type).first()
    if is_exists:
        # print("exists")
        beginDate = is_exists.begin_date
        count = is_exists.total_number_of_items
        video_name = transaction_id + ".mp4"

        store_actual_id = db.query(Stores.store_actual_id).filter(Stores.id == is_exists.store_id).first()
        move_data_in_s3(beginDate, store_actual_id[0], count, video_name)
        return {"message": "Transaction already exists"}
    if counter_type == 2:
        # print("sco")
        sco = db2.query(Transaction_SCO).filter(Transaction_SCO.transactionId == transaction_id).first()
        if sco:
            beginDate = sco.beginDate
            count = sco.totalNumberOfItems
            video_name = transaction_id + ".mp4"

            store_actual_id = db2.query(Stores.store_actual_id).filter(Stores.id == sco.storeId).first()
            move_data_in_s3(beginDate, store_actual_id[0], count, video_name)

        transaction_item_sco = db2.query(Transaction_Details_SCO).filter(
            Transaction_Details_SCO.TransactionID == transaction_id).first()
        data = update_transactions_table(db2, db, sco, transaction_item_sco, transaction_id, counter_type)
        return data
    elif counter_type == 1:
        # print("main")
        main = db2.query(Transaction_Main).filter(Transaction_Main.transactionId == transaction_id).first()
        if main:
            beginDate = main.beginDate
            count = main.totalNumberOfItems
            video_name = transaction_id + ".mp4"
            store_actual_id = db2.query(Stores.store_actual_id).filter(Stores.id == main.storeId).first()
            move_data_in_s3(beginDate, store_actual_id[0], count, video_name)

        transaction_item_sco = db2.query(Transaction_Details_Main).filter(
            Transaction_Details_Main.TransactionID == transaction_id).first()

        data = update_transactions_table(db2, db, main, transaction_item_sco, transaction_id, counter_type)
        return data

def update_transaction_and_items(details, transaction_id, db):
    transaction = details["transaction_details"]
    all_transaction_items = details["transaction_items"]
    db.query(Transactions).filter(Transactions.transaction_id == transaction_id).update({"description":transaction["description"],
                                                                                         "checked_items":transaction["checked_items"],
                                                                                         "bag_quantity": transaction["bag_quantity"],
                                                                                         "bag_price": transaction["bag_price"],
                                                                                         "first_item_at": transaction["first_item_at"],
                                                                                         "hidden": transaction["hidden"],
                                                                                         "highlighted": transaction["highlighted"]
                                                                                         })
    db.commit()
    for transaction_items in all_transaction_items:
        if transaction_items.get("db_id", None):
            db.query(Transaction_items).filter(Transaction_items.id == transaction_items["db_id"]).update({"name": transaction_items["name"],
                                                                                     "quantity": transaction_items["quantity"],
                                                                                     "begin_date_time": transaction_items["begin_date_time"],
                                                                                     "end_date_time": transaction_items["begin_date_time"],
                                                                                     "transaction_type": transaction_items["transaction_type"],
                                                                                     "regular_sales_unit_price": transaction_items["regular_sales_unit_price"],
                                                                                     "missed": transaction_items["missed"],
                                                                                     "overscan": 0,
                                                                                     "extended_amount": eval(transaction_items["regular_sales_unit_price"]) * eval(transaction_items["quantity"]),
                                                                                     })
            db.commit()
        else:
            val = {"name": transaction_items["name"],
                   "transaction_id": transaction_id,
                   "quantity": transaction_items["quantity"],
                   "begin_date_time": transaction_items["begin_date_time"],
                   "end_date_time": transaction_items["begin_date_time"],
                   "transaction_type": transaction_items["transaction_type"],
                   "regular_sales_unit_price": transaction_items["regular_sales_unit_price"],
                   "missed": transaction_items['missed'],
                   "overscan": 0,
                   "extended_amount": eval(transaction_items["regular_sales_unit_price"]) * eval(transaction_items["quantity"]),
                   }
            transac_items = Transaction_items(**val)
            db.add(transac_items)
            db.commit()

    return {"message": "Done"}