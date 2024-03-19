from model.model import Machine_issue,Machine_issue_details,Stores
from sqlalchemy.orm import Session
from datetime import datetime
from sqlalchemy import func


def add_data(db: Session, store_id, camera_issue=None, not_responding=None, network_issue=None):
    existing_row = db.query(Machine_issue).filter_by(store_id=store_id).first()

    if existing_row:
        # Update the existing row if it exists
        if camera_issue is not None:
            existing_row.camera_issue = camera_issue
            if camera_issue == 0:
                # Update the end_time for the corresponding issue
                end_time = datetime.now()

                # Find the corresponding issue in Machine_issue_details and update the end_time
                db.query(Machine_issue_details) \
                    .filter_by(machine_issue_id=existing_row.id, issue_type='camera_issue', end_time=None) \
                    .update({'end_time': end_time})

            elif camera_issue == 1:
                # Create a new row in Machine_issue_details for camera_issue
                start_time = datetime.now()
                detail_row = Machine_issue_details(
                    start_time=start_time,
                    machine_issue_id=existing_row.id,
                    issue_type='camera_issue'
                )
                db.add(detail_row)

        if not_responding is not None:
            existing_row.not_responding = not_responding
            if not_responding == 0:
                # Update the end_time for the corresponding issue
                end_time = datetime.now()

                # Find the corresponding issue in Machine_issue_details and update the end_time
                db.query(Machine_issue_details) \
                    .filter_by(machine_issue_id=existing_row.id, issue_type='not_responding', end_time=None) \
                    .update({'end_time': end_time})

            elif not_responding == 1:
                # Create a new row in Machine_issue_details for not_responding
                start_time = datetime.now()
                detail_row = Machine_issue_details(
                    start_time=start_time,
                    machine_issue_id=existing_row.id,
                    issue_type='not_responding'
                )
                db.add(detail_row)

        if network_issue is not None:
            existing_row.network_issue = network_issue
            if network_issue == 0:
                # Update the end_time for the corresponding issue
                end_time = datetime.now()

                # Find the corresponding issue in Machine_issue_details and update the end_time
                db.query(Machine_issue_details) \
                    .filter_by(machine_issue_id=existing_row.id, issue_type='network_issue', end_time=None) \
                    .update({'end_time': end_time})

            elif network_issue == 1:
                # Create a new row in Machine_issue_details for network_issue
                start_time = datetime.now()
                detail_row = Machine_issue_details(
                    start_time=start_time,
                    machine_issue_id=existing_row.id,
                    issue_type='network_issue'
                )
                db.add(detail_row)

        db.commit()  # Commit the changes to the database

        return existing_row
    else:
        # Create a new row in Machine_issue if it doesn't exist
        new_row = Machine_issue(
            store_id=store_id,
            camera_issue=camera_issue,
            not_responding=not_responding,
            network_issue=network_issue
        )
        db.add(new_row)
        db.commit()
        print("<><><>",new_row.id)

        if camera_issue == 1:
            # Create a new row in Machine_issue_details for camera_issue
            start_time = datetime.now()
            detail_row = Machine_issue_details(
                start_time=start_time,
                machine_issue_id=new_row.id,
                issue_type='camera_issue'
            )
            db.add(detail_row)

        if not_responding == 1:
            # Create a new row in Machine_issue_details for not_responding
            start_time = datetime.now()
            detail_row = Machine_issue_details(
                start_time=start_time,
                machine_issue_id=new_row.id,
                issue_type='not_responding'
            )
            db.add(detail_row)


        if network_issue == 1:
            # Create a new row in Machine_issue_details for network_issue
            start_time = datetime.now()
            detail_row = Machine_issue_details(
                start_time=start_time,
                machine_issue_id=new_row.id,
                issue_type='network_issue'
            )
            db.add(detail_row)

        db.commit()  # Commit the changes to the database

        return True



def get_data(db: Session,store_id=None):
    # Check if a row with the given store_id exists
    existing_row = db.query(Machine_issue).filter_by(store_id=store_id).first()
    return existing_row



def get_all_data(db: Session, page=1, store_id=None):
    per_page = 10
    # Calculate the offset to skip the correct number of records based on the page number
    offset = (page - 1) * per_page

    # Create a subquery to get the counts of issues for each store
    subquery = db.query(
        Machine_issue.store_id,
        func.coalesce(func.sum(Machine_issue.network_issue), 0).label('network_issue'),
        func.coalesce(func.sum(Machine_issue.not_responding), 0).label('not_responding'),
        func.coalesce(func.sum(Machine_issue.camera_issue), 0).label('camera_issue')
    ).group_by(Machine_issue.store_id).subquery()

    query = db.query(Stores, subquery.c.network_issue, subquery.c.not_responding, subquery.c.camera_issue) \
        .outerjoin(subquery, Stores.id == subquery.c.store_id)

    if store_id is not None:
        # If store_id is provided, filter the results to include data for that store only
        query = query.filter(Stores.id == store_id)

    query = query.filter(Stores.store_running == 1, Stores.store_active == 1)

    total_count = query.count()
    if offset > 0:
        query = query.offset(offset)

    query = query.limit(per_page)

    all_data = query.all()

    final_data = [
        {
            "store_id": store.id,
            "network_issue": network_issue if network_issue is not None else 0,
            "not_responding": not_responding if not_responding is not None else 0,
            "camera_issue": camera_issue if camera_issue is not None else 0,
            "id": store.id,
            "name": store.name
        }
        for store, network_issue, not_responding, camera_issue in all_data
    ]

    return {
        "total_count": total_count,
        "data": final_data
    }

