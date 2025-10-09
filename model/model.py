from database import Base,Base2, engine, engine2
from sqlalchemy import Table
class Transactions(Base):
    __table__ = Table('transactions', Base.metadata,
                    autoload=True, autoload_with=engine)

class Stores(Base):
    __table__ = Table('stores', Base.metadata,
                    autoload=True, autoload_with=engine)

class Transaction_items(Base):
    __table__ = Table('transaction_items', Base.metadata,
                    autoload=True, autoload_with=engine)


class Outage(Base):
    __table__ = Table('outage', Base.metadata,
                    autoload=True, autoload_with=engine)


class Sources(Base):
    __table__ = Table('sources', Base.metadata,
                    autoload=True, autoload_with=engine)

class Operators(Base):
    __table__ = Table('operators', Base.metadata,
                      autoload=True, autoload_with=engine)

class Comments(Base):
    __table__ = Table('comments', Base.metadata,
                    autoload=True, autoload_with=engine)

class AppUsers(Base):
    __table__ = Table('app_user', Base.metadata,
                    autoload=True, autoload_with=engine)

class IssueCategory(Base):
    __table__ = Table("issue_category", Base.metadata,
                      autoload=True, autoload_with=engine)

class IssueDescription(Base):
    __table__ = Table("issue_description", Base.metadata,
                      autoload=True, autoload_with=engine)

class Users(Base):
    __table__ = Table("users", Base.metadata,
                      autoload=True, autoload_with=engine)

class Roles(Base):
    __table__ = Table("user_roles", Base.metadata,
                      autoload=True, autoload_with=engine)
class Aisle_images(Base):
    __table__ = Table("aisle_images", Base.metadata,
                      autoload=True, autoload_with=engine)
class Machine_issue(Base):
    __table__ = Table("machine_issue", Base.metadata,
                      autoload=True, autoload_with=engine)

class Machine_issue_details(Base):
    __table__ = Table("machine_issue_details", Base.metadata,
                      autoload=True, autoload_with=engine)
    
class Status(Base):
    __table__ = Table("status", Base.metadata,
                      autoload=True, autoload_with=engine)
    
class LatestStatus(Base):
    __table__ = Table("latest_status", Base.metadata,
                      autoload=True, autoload_with=engine)



############# For xmldata Table ##########################
class Transaction_SCO(Base2):
    __table__ = Table("transactions_sco", Base2.metadata,
                      autoload=True, autoload_with=engine2)

class Transaction_Main(Base2):
    __table__ = Table("transactions", Base2.metadata,
                      autoload=True, autoload_with=engine2)

class Transaction_Details_SCO(Base2):
    __table__ = Table("transaction_details_sco", Base2.metadata,
                      autoload=True, autoload_with=engine2)

class Transaction_Details_Main(Base2):
    __table__ = Table("transaction_details", Base2.metadata,
                      autoload=True, autoload_with=engine2)

class Transaction_SCO_Alert(Base2):
    __table__ = Table("transactions_sco_alert", Base2.metadata,
                      autoload=True, autoload_with=engine2)
    

class Stores_XML(Base2):
    __table__ = Table("stores", Base2.metadata,
                      autoload=True, autoload_with=engine2)