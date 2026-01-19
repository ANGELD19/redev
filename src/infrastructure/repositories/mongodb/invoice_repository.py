import os
from bson import ObjectId
from src.infrastructure.repositories.mongodb.mongodb_repository import MongodbRepository
from src.domain.constant import LOOKUP, SORT, UNSET, MATCH


COLLECTION_NAME = "invoices"


class InvoiceRepository(MongodbRepository):
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
        
    def get_invoices(self, page, page_size, filters):
        return self.get_all(page, page_size, self.add_details(), **filters)
    
    def get_invoice(self, invoice_id):
        return self.get(self.add_details(), _id=ObjectId(invoice_id))
    
    def add_details(self):
        return [
            {
                SORT: {
                    "date_created": -1
                }
            },
            {
                LOOKUP: {
                    "from": "company",
                    "localField": "company",
                    "foreignField": "_id",
                    "as": "company",
                }
            },
            {
                LOOKUP: {
                    "from": "invoiceStatus",
                    "localField": "status",
                    "foreignField": "_id",
                    "as": "status",
                }
            },
            {
                LOOKUP: {
                    "from": "invoiceStatus",
                    "localField": "status_history.status",
                    "foreignField": "_id",
                    "as": "status_history.status",
                }
            },
        ]