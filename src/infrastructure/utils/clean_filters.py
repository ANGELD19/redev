import datetime
from bson import ObjectId

def clean_filters(filters):
    filters.pop("page", None)
    filters.pop("limit", None)
    if "_id" in filters:
        filters["_id"] = ObjectId(filters["_id"])
    if "company" in filters:
        filters["company"] = ObjectId(filters["company"])
    if "status" in filters:
        filters["status"] = ObjectId(filters["status"])
                
    return filters

def dateformat(date_str):
    try:
        date_str = date_str.strftime('%m-%d-%Y')
    except:
        date_str = datetime.datetime.strptime(date_str, '%m-%d-%Y T%H:%M')
        date_str = date_str.strftime('%m-%d-%Y')
    return date_str
