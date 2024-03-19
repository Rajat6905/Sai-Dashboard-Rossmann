from sqlalchemy import func, and_, distinct, desc, or_, true, false, asc, extract, text
from model.model import Transactions, Stores, Transaction_items, Outage,Sources
from model.model_dummy_database import Transactions as Transactions_dummy
from sqlalchemy.orm import Session, aliased
from datetime import datetime, timedelta
from dateutil import parser

def search_by_id_res(db, search_query, page, store_id, region_id ,area_id):
    per_page = 10
    result = db.query(
                Stores.name.label("store"),
                Transactions.transaction_id,
                Transactions.sequence_no,
                Transactions.counter_no,
                Transactions.operator_id,
                Transactions.description,
                Transactions.missed_scan,
                Transactions.video_link,
                Transactions.begin_date,
                ) \
        .join(Stores, Transactions.store_id == Stores.id)\
        .filter(
                or_(Transactions.store_id == store_id, true() if store_id is None else false()),
                or_(Stores.region_id == region_id, true() if region_id is None else false()),
                or_(Stores.area_id == area_id, true() if area_id is None else false())
            )

    count = {"total_count": result.count()}
    if page != None and (search_query == None or search_query == " " or search_query == ""):
        result = result.order_by(desc(Transactions.begin_date)).limit(10).offset((page - 1) * per_page).all()
        return result, count

    # result = result.filter(Transactions.transaction_id.ilike(f"%{search_query}%"))
    result = result.filter(Transactions.sequence_no.ilike(f"%{search_query}%")).order_by(desc(Transactions.begin_date))
    count = {"total_count": result.count()}
    return result.all(), count

def search_by_transaction(db, db1 , transaction_id):
    # val = db.query(Transactions_dummy).filter(Transactions_dummy.transaction_id == transaction_id).first()
    # if val:
    #     return val
    # return {"message": "No Transaction Found!!"}

    # Fetch the transaction based on transaction_id
    transaction = db.query(Transactions_dummy).filter(Transactions_dummy.transaction_id == transaction_id).first()

    if transaction:
        # Extract relevant attributes from the transaction
        store_id = transaction.store_id
        counter_no = transaction.counter_no
        begin_date = transaction.begin_date
        source_id= transaction.source_id
        if source_id == 2:
            table_name= "transactions_sco"
        elif source_id ==1:
            table_name = "transactions"

        # Fetch the previous_transaction_id
        query_previous = text(f'''
                SELECT t3.transactionId,s.store_actual_id, t3.totalNumberOfItems, t3.beginDate
                FROM {table_name} t3
                INNER JOIN stores s ON t3.storeId = s.id
                WHERE t3.beginDate < :begin_date
                AND t3.storeId = :store_id
                AND t3.counterNo = :counter_no
                ORDER BY t3.beginDate DESC
                LIMIT 1
            ''')
        previous_transaction_id = db1.execute(query_previous, {'begin_date': begin_date, 'store_id': store_id,
                                                              'counter_no': counter_no}).fetchone()
        if previous_transaction_id:

            parsed_date = parser.parse(previous_transaction_id.beginDate)
            parsed_date = parsed_date.date()

            previous_video_url= f"Videos/{parsed_date}/{previous_transaction_id.store_actual_id}/unzip/{previous_transaction_id.totalNumberOfItems}/{previous_transaction_id.transactionId}.mp4"
        else:
            previous_video_url= None


        # Fetch the next_transaction_id
        query_next = text(f'''
                SELECT t2.transactionId,s.store_actual_id, t2.totalNumberOfItems, t2.beginDate
                FROM {table_name} t2
                INNER JOIN stores s ON t2.storeId = s.id
                WHERE t2.beginDate > :begin_date
                AND t2.storeId = :store_id
                AND t2.counterNo = :counter_no
                ORDER BY t2.beginDate ASC
                LIMIT 1
            ''')
        next_transaction_id = db1.execute(query_next, {'table_name':table_name,'begin_date': begin_date, 'store_id': store_id,
                                                      'counter_no': counter_no}).fetchone()

        if next_transaction_id:

            parsed_date = parser.parse(next_transaction_id.beginDate)
            parsed_date = parsed_date.date()

            next_video_url= f"Videos/{parsed_date}/{next_transaction_id.store_actual_id}/unzip/{next_transaction_id.totalNumberOfItems}/{next_transaction_id.transactionId}.mp4"
        else:
            next_video_url= None


        # Convert the transaction object to a dictionary
        transaction_dict = {key: getattr(transaction, key) for key in transaction.__dict__.keys() if
                            not key.startswith('_')}

        # # Add the new attributes to the dictionary
        transaction_dict[
            "previous_video"] = previous_video_url
        transaction_dict["next_video"] = next_video_url

        return transaction_dict

    return {"message": "No Transaction Found!!"}

def delete_item(db, db_id):
    record_to_delete = db.query(Transaction_items).filter(Transaction_items.id == db_id).first()

    if record_to_delete:
        # Delete the record if it exists
        db.delete(record_to_delete)
        db.commit()  # Commit the deletion
        return {"message": "Transaction Deleted!"}
    else:
        return {"message": "No Transaction Found!!"}