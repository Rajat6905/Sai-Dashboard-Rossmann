from database import Base,engine
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
