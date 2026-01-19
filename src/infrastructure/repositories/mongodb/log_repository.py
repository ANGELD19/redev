import datetime
import os
from flask_jwt_extended import get_jwt_identity
from flask import request
from src.infrastructure.repositories.mongodb.mongodb_repository import MongodbRepository
from src.infrastructure.repositories.mongodb.users_repository import UsersRepository

users_repository = UsersRepository()
COLLECTION_NAME = "logs"


class LogRepository(MongodbRepository):
    def __init__(self):
        user = os.getenv("MONGO_DATABASE_USERNAME")
        password = os.getenv("MONGO_DATABASE_PASSWORD")
        cluster = os.getenv("MONGO_DATABASE_CLUSTER")
        string_connection = f"mongodb+srv://{user}:{password}@{cluster}/{os.getenv("MONGO_DATABASE_NAME")}?retryWrites=true&w=majority"
        super().__init__(
            string_connection,
            os.getenv("MONGO_DATABASE_NAME"),
            COLLECTION_NAME,
        )

    def create_log(self, origen, type_log, details="", user=None):
        if user is None:
            try:
                user = users_repository.get(email=get_jwt_identity()).get("_id", "")
            except RuntimeError:
                user = ""

        try:
            ip_address = request.remote_addr
        except RuntimeError:
            ip_address = "0.0.0.0"

        created_at = datetime.datetime.now()

        return self.create(
            ip_address=ip_address,
            origen=origen,
            details=details,
            type_log=type_log,
            created_at=created_at,
            user=user,
        )