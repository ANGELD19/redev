import os
from src.infrastructure.repositories.mongodb.mongodb_repository import MongodbRepository


COLLECTION_NAME = "processStatus"


class ProcessStatusRepository(MongodbRepository):
    def __init__(self):
        user = os.getenv("MONGO_DATABASE_USERNAME")
        password = os.getenv("MONGO_DATABASE_PASSWORD")
        cluster = os.getenv("MONGO_DATABASE_CLUSTER")
        string_connection = f"mongodb+srv://{user}:{password}@{cluster}/test?authSource=admin&readPreference=primary&ssl=true"
        super().__init__(
            string_connection,
            os.getenv("MONGO_DATABASE_NAME"),
            COLLECTION_NAME,
        )