from sqlalchemy import func, and_, distinct, desc, or_, true, false, asc, extract, text
from model.model import Transactions, Stores, Transaction_items, Outage,Sources, Operators, Comments, IssueCategory, IssueDescription
from sqlalchemy.orm import Session, aliased
from datetime import datetime, timedelta
import itertools
import pandas as pd
from utils import current_date_time, get_last_3_months_from_current_date, add_values_stats
from operator import itemgetter

def get_issue_category(db):
    categorys = db.query(IssueCategory).all()
    return categorys

def store_issue_data(db, store_id, region_id, area_id, category_id, page):
    per_page = 10
    results = db.query(Stores.name, Stores.region_id, Stores.area_id,
                       IssueDescription.problem, IssueDescription.comment,
                       IssueCategory.category_name)\
    .join(IssueDescription, Stores.id == IssueDescription.store_id)\
    .join(IssueCategory, IssueDescription.category == IssueCategory.id) \
    .filter(
        or_(IssueDescription.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.region_id == region_id, true() if region_id is None else false()),
        or_(Stores.area_id == area_id, true() if area_id is None else false()),
        or_(IssueDescription.category == category_id, true() if category_id is None else false()),
    ).filter(IssueDescription.problem != "",IssueDescription.status==1)
    count = {"total_count": results.count()}
    return results.limit(10).offset((page - 1) * per_page).all(), count

def store_map_data(db, store_id, region_id, area_id):
    per_page = 10
    result = db.query(Stores.name, Stores.region_id, Stores.area_id,
             IssueDescription.problem, IssueDescription.comment,
             IssueCategory.category_name, Stores.latitude, Stores.longitude, IssueCategory.category_color) \
        .join(IssueDescription, Stores.id == IssueDescription.store_id) \
        .join(IssueCategory, IssueDescription.category == IssueCategory.id) \
        .filter(
        or_(IssueDescription.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.region_id == region_id, true() if region_id is None else false()),
        or_(Stores.area_id == area_id, true() if area_id is None else false()),
    ) \
        .all()
    return result

def add_issue_comment(db, details):
    try:
        store_id = details["store_id"]
        comment = details["comment"]
        db.query(IssueDescription).filter_by(store_id=store_id).update({IssueDescription.comment: comment})
        db.commit()
        return {
            "message": "Comment added successfully",
            "status_code":200
        }
    except:
        return {
            "message": "Internal server error",
            "status_code":500
        }
    
def fetch_searched_store(db):
    query = db.query(
        Stores.id, Stores.name
    ).filter(
        Stores.store_running == 1
    )

    result = query.order_by(Stores.name).all()

    return result