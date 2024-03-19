from model.model import Transactions, Stores, Transaction_items, Outage,Sources, Operators, Comments, Users, Roles

def find_by_email(email, db):
    is_exists = db.query(Users).filter(Users.email == email).first()
    return is_exists

def create_user(data, db):
    if find_by_email(data["email"], db):
        return {"message": "Email already exists"}
    user_obj = Users(
        name=data["name"],
        email=data["email"],
        password=data["password"],
        roles = data["role"]
    )
    db.add(user_obj)
    db.commit()
    # result = [{"id": int(id), "name": name} for id, name in zip(data["store_id"], data["store_name"])]
    user_roles_obj = Roles(
        stores_name = str(data["store_name"]),
        # stores_name = data["store_name"],
        region_id = str(data["region_id"]),
        area_id = str(data["area_id"]),
        store_id=str(data["store_id"]),
        user_id=user_obj.id)
    db.add(user_roles_obj)
    db.commit()
    return {"message": "user created"}

def get_data_acc_to_stores(data, db):
    result = db.query(Users.email,
                      Users.roles,
                      Roles.user_id,
                      Roles.region_id,
                      Roles.area_id,
                      Roles.store_id,
                      Roles.stores_name)\
        .join(Roles, Users.id == Roles.user_id).filter(Users.email==data["sub"]).first()

    print("token data: ", result)
    return result

def all_users(db):
    print(Roles.stores_name)
    result = db.query(Users.name, Users.email, Users.roles, Users.id).all()

    return result

def delete_user(db):
    print(Roles.stores_name)
