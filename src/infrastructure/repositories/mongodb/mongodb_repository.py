from bson import ObjectId
from .mongodb_client import get_mongo_client  

class MongodbRepository:
    def __init__(self, name_db, collection_name):
        self.client = get_mongo_client()
        self.db = self.client[name_db]
        self.collection = self.db[collection_name]

    def get(self, details=[], **kwargs):
        pipeline = [{"$match": kwargs}]
        pipeline = self.add_pipeline(pipeline, details)
        result = list(self.collection.aggregate(pipeline))
        return result[0] if result else None

    def get_all(self, page: int, page_size: int, details=[], **kwargs):
        skip = (page - 1) * page_size

        query = {}
        for key, value in kwargs.items():
            if value:
                query[key] = {"$regex": value, "$options": "i"} if isinstance(value, str) else value

        total_documents = self.collection.count_documents(query)
        if total_documents == 0:
            return [], 0
        if total_documents <= skip:
            raise ValueError("Page not found")

        pipeline = [
            {"$match": query},
            {"$skip": skip},
            {"$limit": page_size},
        ]
        pipeline = self.add_pipeline(pipeline, details)
        data = list(self.collection.aggregate(pipeline))
        total_pages = (total_documents + page_size - 1) // page_size
        return data, total_pages

    def create(self, **kwargs):
        return self.collection.insert_one(kwargs)

    def update(self, id: str, **kwargs):
        return self.collection.update_one({"_id": id}, {"$set": kwargs})

    def delete(self, _id: ObjectId):
        return self.collection.delete_one({"_id": _id})

    def add_pipeline(self, pipeline, details):
        return pipeline + details