from sqlalchemy import desc, distinct
from sqlalchemy.sql import func

from model.model import (
    Transaction_Details_SCO, Transaction_SCO_Alert, Stores_XML, Transactions, Transaction_items, Operators
)
from crud.for_xml_table_operations import move_data_in_s3

def fetch_upload_transaction_details(stores_id_list, start_time, end_time, db_xml_dev, nudge_type):
    query = db_xml_dev.query(
        Transaction_SCO_Alert.transactionId,
        Stores_XML.name.label("store_name"),
        Stores_XML.store_num.label("store_id")
    ).join(
        Stores_XML, Transaction_SCO_Alert.storeId == Stores_XML.id
    ).filter(
        Transaction_SCO_Alert.Entrystatus == 0,
        Transaction_SCO_Alert.skip == False
    )

    if nudge_type == "not_attended":
        query = query.filter(
            Transaction_SCO_Alert.type == "Not Attended"
        )
    else:
        query = query.filter(
            Transaction_SCO_Alert.type != "Not Attended"
        )

    if start_time and end_time:
        query = query.filter(
            Transaction_SCO_Alert.beginDate >= start_time,
            Transaction_SCO_Alert.beginDate <= end_time
        )

    if stores_id_list:
        query = query.filter(
            Transaction_SCO_Alert.storeId.in_(stores_id_list)
        )

    transaction_to_show = query.order_by(
        (Transaction_SCO_Alert.beginDate)
    ).limit(1).all()

    transaction_id = transaction_to_show[0].transactionId if transaction_to_show else None
    store_name = transaction_to_show[0].store_name if transaction_to_show else None
    store_id = transaction_to_show[0].store_id if transaction_to_show else None

    missed_items_details = db_xml_dev.query(
        Transaction_SCO_Alert
    ).filter(
        Transaction_SCO_Alert.transactionId == transaction_id
    ).all()

    item_details = db_xml_dev.query(
        Transaction_Details_SCO
    ).filter(
        Transaction_Details_SCO.TransactionID == transaction_id
    ).all()
    
    return transaction_id, item_details, missed_items_details, store_name, store_id


def update_transaction_entry_status(transaction_id, db_xml_dev):
    db_xml_dev.query(
        Transaction_SCO_Alert
    ).filter(
        Transaction_SCO_Alert.transactionId == transaction_id
    ).update({"Entrystatus": 1})

    result = db_xml_dev.commit()
    
    return result

    
def insert_transaction_to_dashboard_db(transaction_id, input_description, input_clubcard, actual_store_id, db, 
                                       db_xml_dev,nudge_type):
    transaction_result = db.query(Transactions).filter(Transactions.transaction_id == transaction_id).first()
    if transaction_result:
        return
    
    item_details = db_xml_dev.query(
        Transaction_Details_SCO
    ).filter(
        Transaction_Details_SCO.TransactionID == transaction_id
    ).all()

    missed_items_details = db_xml_dev.query(
        Transaction_SCO_Alert
    ).filter(
        Transaction_SCO_Alert.transactionId == transaction_id
    ).all()

    o_id = item_details[0].operator_id
    if o_id == "":
        operator_id = 150
    else:
        operator_id = db.query(Operators).filter(Operators.operator_id == o_id).first()
        if operator_id:
            operator_id = operator_id.id
        else:
            o_val = Operators(operator_id=o_id,store_id=item_details[0].store_id)
            db.add(o_val)
            db.commit()
            operator_id = o_val.id

    date = missed_items_details[0].beginDate[:10]

    transaction_obj = Transactions(
        transaction_id=item_details[0].TransactionID if item_details else None,
        sequence_no=item_details[0].sequence_no if item_details else None,
        store_id=item_details[0].store_id if item_details else None,
        operator_id=operator_id,
        counter_no=item_details[0].counterno if item_details else None,
        source_id=2,
        extended_total_amount="",
        total_number_of_items=len(item_details),
        checked_items=len(item_details),
        missed_scan=1,
        over_scan=0,
        triggers=0,
        description=input_description if input_description else missed_items_details[0].type,
        # clubcard=input_clubcard,
        bag_quantity=0,
        bag_price=0,
        first_item_at=5,
        hidden=1,
        highlighted=0,
        video_link=f"{transaction_id}.mp4",
        begin_date=min([item.BeginDateTime for item in item_details]).replace("T", " "),
        end_date=max([item.BeginDateTime for item in item_details]).replace("T", " "),
        final_status=0,
        staffcard=""
    )

    transaction_item_obj_list = []
    for item in item_details:
        transaction_item_obj_list.append(
            Transaction_items(
                name=item.Name,
                transaction_id=item.TransactionID,
                transaction_type="Sale",
                pos_item_id="",
                item_id=item.ItemID,
                regular_sales_unit_price=item.RegularSalesUnitPrice,
                actual_sales_unit_price=item.ActualSalesUnitPrice,
                extended_amount=item.ExtendedAmount,
                quantity=item.Quantity,
                checked_quantity=item.Quantity,
                missed=0,
                overscan=0,
                trigger_id=0,
                scan_data=item.ScanData,
                begin_date_time=item.BeginDateTime.replace("T", " "),
                end_date_time=item.BeginDateTime.replace("T", " ")
            )
        )

    for missed_item in missed_items_details:
        transaction_item_obj_list.append(
            Transaction_items(
                name="Item",
                transaction_id=missed_item.transactionId,
                transaction_type="Sale",
                pos_item_id="0000",
                item_id="0000",
                regular_sales_unit_price=5,
                actual_sales_unit_price=5,
                extended_amount=5,
                quantity=1,
                checked_quantity=1,
                missed=1,
                overscan=0,
                trigger_id=0,
                scan_data="0000",
                begin_date_time=missed_item.beginDate.replace("T", " "),
                end_date_time=missed_item.beginDate.replace("T", " ")
            )
        )


    if nudge_type == "not_attended":
        transfer_status = move_data_in_s3(date, actual_store_id, "not_attended", f"{transaction_id}.mp4")
    else:
        transfer_status = move_data_in_s3(date, actual_store_id, "alerts", f"{transaction_id}.mp4")
    print("Video transfer status: ", transfer_status)

    db.add(transaction_obj)
    db.add_all(transaction_item_obj_list)
    db.commit()

    return True


def upload_transaction_skip_op(transaction_id, db_xml_dev):
    db_xml_dev.query(
        Transaction_SCO_Alert
    ).filter(   
        Transaction_SCO_Alert.transactionId == transaction_id
    ).update({"skip": True})
    
    db_xml_dev.commit()


def fetch_transaction_status_details(store_ids, start_time, end_time, db_xml_dev, nudge_type):
    query = db_xml_dev.query(
        Stores_XML.id,
        Stores_XML.name,
        func.count(distinct(Transaction_SCO_Alert.transactionId)).label("entry_status_count"),
    ).join(
        Stores_XML, Transaction_SCO_Alert.storeId == Stores_XML.id
    ).filter(
            Transaction_SCO_Alert.Entrystatus == 1
    )

    if nudge_type == "not_attended":
        query = query.filter(
            Transaction_SCO_Alert.type == "Not Attended"
        )
    else:
        query = query.filter(
            Transaction_SCO_Alert.type != "Not Attended"
        )

    if store_ids:
        query = query.filter(
            Transaction_SCO_Alert.storeId.in_(store_ids)
        )
    
    if start_time and end_time:
        # start_time = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        # end_time = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        query = query.filter(
            func.str_to_date(Transaction_SCO_Alert.beginDate, "%Y-%m-%d %H:%i:%s") >= start_time,
            func.str_to_date(Transaction_SCO_Alert.beginDate, "%Y-%m-%d %H:%i:%s") <= end_time
        )
        
    entry_status_result = query.group_by(
        Stores_XML.id
    ).all()

    query = db_xml_dev.query(
        Stores_XML.id,
        Stores_XML.name,
        func.count(distinct(Transaction_SCO_Alert.transactionId)).label("skip_count"),
    ).join(
        Stores_XML, Transaction_SCO_Alert.storeId == Stores_XML.id
    ).filter(
            Transaction_SCO_Alert.skip == True
    )

    if nudge_type == "not_attended":
        query = query.filter(
            Transaction_SCO_Alert.type == "Not Attended"
        )
    else:
        query = query.filter(
            Transaction_SCO_Alert.type != "Not Attended"
        )

    if store_ids:
        query = query.filter(
            Transaction_SCO_Alert.storeId.in_(store_ids)
        )
        
    if start_time and end_time:
        query = query.filter(
            Transaction_SCO_Alert.beginDate >= start_time,
            Transaction_SCO_Alert.beginDate <= end_time
        )
        
    skip_result = query.group_by(
        Stores_XML.id
    ).all()

    return entry_status_result, skip_result
