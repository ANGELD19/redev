from pymongo import MongoClient, UpdateOne
from bson import ObjectId

class MongodbRepository:
    def __init__(self, string_connection, name_db, collection_name):
        self.string_connection = string_connection
        self.name_db = name_db
        self.collection_name = collection_name
        self.client = None
        self.db = None
        self.collection = None

    def __enter__(self):
        self.client = MongoClient(self.string_connection)
        self.db = self.client[self.name_db]
        self.collection = self.db[self.collection_name]
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.client:
            self.client.close()

    def get(self, details=[], **kwargs):
        with self as repo:
            pipeline = [
                {"$match": kwargs},
            ]
            pipeline = repo.add_pipeline(pipeline, details)

            result = list(repo.collection.aggregate(pipeline))
            return result[0] if result else None

    def get_all(self, page: int, page_size: int, details=[], **kwargs):
        with self as repo:
            skip = (page - 1) * page_size

            query = {}
            for key, value in kwargs.items():
                if isinstance(value, dict):
                    query[key] = value
                elif isinstance(value, str):
                    if value.strip():
                        query[key] = {"$regex": value, "$options": "i"}
                elif value is not None:
                    query[key] = value
            total_documents = repo.collection.count_documents(query)
            if total_documents == 0:
                return [], 0
            if total_documents <= skip:
                raise ValueError("Page not found")
            
            pipeline = [
                {"$match": query},
                {"$sort": {"date_created": -1}},
                {"$skip": skip},
                {"$limit": page_size},
            ]
            pipeline = repo.add_pipeline(pipeline, details)
            data = list(repo.collection.aggregate(pipeline))
            total_pages = (total_documents + page_size - 1) // page_size
            return data, total_pages

    def create(self, **kwargs):
        with self as repo:
            return repo.collection.insert_one(kwargs)

    def update(self, id: str, **kwargs):
        with self as repo:
            return repo.collection.update_one({"_id": id}, {"$set": kwargs})

    def add_pipeline(self, pipeline, details):
        return pipeline + details

    def delete(self, id: str):
        with self as repo:
            return repo.collection.delete_one({"_id": id})
        
    def find_many(self, filter: dict):
        with self as repo:
            return list(repo.collection.find(filter))
        
    def update_fields(self, id: str, set_fields: dict = None, push_fields: dict = None):
        update_obj = {}
        if set_fields:
            update_obj["$set"] = set_fields
        if push_fields:
            update_obj["$push"] = push_fields

        with self as repo:
            return repo.collection.update_one({"_id": id}, update_obj)

    def bulk_update(self, updates: list[dict]):
        """
        Realiza una actualización masiva de documentos por ID.
        
        Args:
            updates: Lista de diccionarios con estructura:
                {
                    "id": ObjectId o str,
                    "data": dict con campos a actualizar
                }
        """
        operations = []
        for update in updates:
            doc_id = update["id"]
            if isinstance(doc_id, str):
                doc_id = ObjectId(doc_id)
            operations.append(UpdateOne({"_id": doc_id}, {"$set": update["data"]}))

        if not operations:
            return None

        with self as repo:
            return repo.collection.bulk_write(operations)
        
    def get_many(self, filter: dict):
        with self as repo:
            return list(repo.collection.find(filter))