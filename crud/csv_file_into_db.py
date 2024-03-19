import time
from io import BytesIO, StringIO
import pandas as pd
from sqlalchemy import func, and_, distinct, desc, or_, true, false, asc, extract, text
from model.model import IssueDescription
from sqlalchemy.orm import Session, aliased
from datetime import datetime, timedelta
import requests
import json
from utils import current_date_time
from pprint import pprint


def issue_description_data_update(contents, db):
    data = StringIO(contents.decode())
    # data = BytesIO(contents)
    print(data)
    # Read the contents of the CSV file using Pandas
    df = pd.read_csv(data)
    df = df.where(pd.notnull(df), "")
    db.query(IssueDescription).delete()
    db.commit()
    print(df)

    # time.sleep(10)
    for index, row in df.iterrows():
        row_data = row.to_dict()
        if row_data.get("store_id") not in [None, ""]:
            new_row_data = {
                "id": row_data.get("id"),
                "store_id": row_data.get("store_id"),
                "category": '' if row_data.get("category") == "" else row_data.get("category"),
                "problem": '' if row_data.get("problem").strip() == "" else row_data.get("problem"),
                "comment": '' if row_data.get("comment").strip() == "" else row_data.get("comment"),
                # Add mappings for other columns...
            }
            print("----->", new_row_data)
            new_row = IssueDescription(**new_row_data)
            db.add(new_row)
            # new_row = IssueDescription(**row.to_dict())
            # print(new_row)
            # db.add(new_row)
        else:
            break
        # new_row = IssueDescription(**row.to_dict())
        # print(new_row)
        # db.add(new_row)
    db.commit()
