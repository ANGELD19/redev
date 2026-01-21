import os
from src.infrastructure.repositories.mongodb.mongodb_repository import MongodbRepository

COLLECTION_NAME = "users"

class UserRepository(MongodbRepository):
    def __init__(self):
        super().__init__(
            os.getenv("MONGO_DATABASE_NAME"),
            COLLECTION_NAME,
        )
