import datetime
import os

from flask_jwt_extended import get_jwt_identity
from flask import request, has_request_context
from src.infrastructure.repositories.mongodb.mongodb_repository import MongodbRepository
from src.infrastructure.repositories.mongodb.user_repository import UserRepository

user_repository = UserRepository()
COLLECTION_NAME = "logs"

class LogRepository(MongodbRepository):
    def __init__(self):
        super().__init__(
            os.getenv("MONGO_DATABASE_NAME"),
            COLLECTION_NAME,
        )

        

    def create_log(self, origen, type_log, status_code, details="", data=None, user_id=None):
        created_at = datetime.datetime.now()
        ip_address = request.remote_addr if has_request_context() else "No disponible"

        if user_id is None and has_request_context():
            try:
                identity = get_jwt_identity()
                user = user_repository.get(email=identity)
                user_id = user.get("_id") if user else None
            except Exception:
                user_id = None

        log_entry = {
            "ip_address": ip_address,
            "origen": origen,
            "details": details,
            "type_log": type_log,
            "created_at": created_at,
            "status_code": status_code,
            "user": user_id,
        }

        return self.create(**log_entry)