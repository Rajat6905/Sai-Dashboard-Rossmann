import jwt
from fastapi import HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from passlib.context import CryptContext
from datetime import datetime, timedelta

from sqlalchemy import func, and_, distinct, desc, or_, true, false, asc, extract, text
from model.model import Transactions, Stores, Transaction_items, Outage,Sources, Operators, Comments, Users
from sqlalchemy.orm import Session, aliased

class AuthHandler():
    security = HTTPBearer()
    pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
    secret = "SaiGroup@987"

    def get_password_hash(self, password):
        return self.pwd_context.hash(password)

    def verify_password(self, plain_password, hashed_password):
        return self.pwd_context.verify(plain_password, hashed_password)

    def access_encode_token(self, user_id, role):

        payload = {
            "exp": datetime.utcnow()+timedelta(days=2, minutes=5),
            "iat": datetime.utcnow(),
            "sub": user_id,
            "role": role
        }
        return jwt.encode(
            payload,
            self.secret,
            algorithm="HS256"
        )
    def refresh_encode_token(self, user_id, role):
        payload = {

            "exp": datetime.utcnow()+timedelta(days=30),
            "iat": datetime.utcnow(),
            "sub": user_id,
            "role": role
        }
        return jwt.encode(
            payload,
            self.secret,
            algorithm="HS256"
        )

    def decode_token(self, token):
        try:
            payload = jwt.decode(token, self.secret, algorithms=["HS256"])
            return payload
        except jwt.ExpiredSignatureError:
            raise HTTPException(status_code=401, detail="signature has expired")
        except jwt.InvalidTokenError as e:
            raise HTTPException(status_code=401, detail="Invalid Token")

    def auth_wrapper(self, auth: HTTPAuthorizationCredentials = Security(security)):
        # print(auth.credentials)
        return self.decode_token(auth.credentials)

def user_validate(details, db):
    result = db.query(Users).filter(Users.email == details["email"]).first()
    return result