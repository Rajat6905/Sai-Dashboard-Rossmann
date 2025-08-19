from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

SQLALCHEMY_DATABASE_URL = 'mysql+mysqlconnector://rossmann_dashboard_db_user:SnowFall9@centraldb.mysql.database.azure.com:3306/sai_rossmann'
SQLALCHEMY_DATABASE_URL2 = 'mysql+mysqlconnector://rossmann_xmldata_dashboard:DogWalk44@centraldb.mysql.database.azure.com:3306/xmldata_rossmann'

engine = create_engine(SQLALCHEMY_DATABASE_URL)
print(engine)
print(engine.connect())

engine2 = create_engine(SQLALCHEMY_DATABASE_URL2)
print(engine2)
print(engine2.connect())


SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
SessionLocal2 = sessionmaker(autocommit=False, autoflush=False, bind=engine2)


Base = declarative_base()
Base2 = declarative_base(engine2)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_db2():
    db = SessionLocal2()
    try:
        yield db
    finally:
        db.close()

