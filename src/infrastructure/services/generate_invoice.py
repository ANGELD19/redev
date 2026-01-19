import datetime
import calendar
import traceback

from flask import current_app
from dateutil.relativedelta import relativedelta
from bson import ObjectId
from src.infrastructure.repositories.mongodb.log_repository import LogRepository
from src.infrastructure.repositories.mongodb.invoice_status_repository import InvoiceStatusRepository
from src.infrastructure.repositories.mongodb.invoice_repository import InvoiceRepository
from src.infrastructure.repositories.mongodb.process_repository import ProcessRepository
from src.infrastructure.repositories.mongodb.process_status_repository import ProcessStatusRepository
from src.infrastructure.repositories.mongodb.country_repository import CountryRepository
from src.infrastructure.repositories.mongodb.company_repository import CompanyRepository
from src.infrastructure.repositories.mongodb.ship_repository import ShipRepository
from src.infrastructure.repositories.mongodb.user_repository import PositionRepository
from src.infrastructure.repositories.mongodb.users_repository import UsersRepository
from src.infrastructure.services.send_messages import SendMessages

from src.infrastructure.utils.handler_error import (
    handle_general_error
)

company_repository = CompanyRepository()
users_repository = UsersRepository()
position_repository = PositionRepository()
country_repository = CountryRepository()
ship_repository = ShipRepository()
process_repository = ProcessRepository()
process_status_repository = ProcessStatusRepository()
invoice_status_repository = InvoiceStatusRepository()
invoice_repository = InvoiceRepository()
log_repository = LogRepository()
send_message = SendMessages()

class GenerateInvoice:
    def generate(self, company, user):
        try:
            # Only get process that are in Onboard status and haven't been billed
            onboard_status = process_status_repository.get(status="Onboard")
            returning_crew_status = process_status_repository.get(status="Returning Crew")
            
            current_date = datetime.datetime.now()
            embarkation_date = current_date - relativedelta(months=2)
            embarkation_date = embarkation_date.replace(
                day=calendar.monthrange(embarkation_date.year, embarkation_date.month)[-1],
                hour=23,
                minute=59,
                second=59,
            )
            created_status = invoice_status_repository.get(status="Created")

            processes = []
            if company.get("company") == "Norwegian Cruise Lines":
                oceania_company = company_repository.get(company="OCEANIA CRUISE LINES")
                regent_company = company_repository.get(company="REGENT CRUISE LINES")

                filters = {
                    "$or": [
                        {"status": onboard_status.get("_id")},
                        {"status": returning_crew_status.get("_id")}
                    ],
                    "billed": False,
                    "$and": [
                        {
                        "answers": {
                            "$elemMatch": {
                                "field": "company",
                                "answer": {
                                    "$in": [
                                        company.get("_id"),
                                        oceania_company.get("_id"),
                                        regent_company.get("_id"),
                                    ]   
                                }
                            }
                        }
                        },
                        {
                        "answers": {
                            "$elemMatch": {
                                "field": "embarkation_date",
                                "answer": {
                                    "$lte": embarkation_date
                                }
                            }
                        }
                        }
                    ]
                }

                processes, _ = process_repository.get_all(1, 1000000, **filters)

            elif (
                company.get("company") == "OCEANIA CRUISE LINES"
                or company.get("company") == "REGENT CRUISE LINES"
            ):
                pass
            elif company.get("company") == "Princess Cruises":
                embarkation_date = current_date - relativedelta(months=1)
                start_embarkation_date = embarkation_date.replace(
                    day=1,
                    hour=0,
                    minute=0,
                    second=0,
                )
                end_embarkation_date = embarkation_date.replace(
                    day=calendar.monthrange(embarkation_date.year, embarkation_date.month)[
                        -1
                    ],
                    hour=23,
                    minute=59,
                    second=59,
                )

                filters = {
                    "$or": [
                        {"status": onboard_status.get("_id")},
                        {"status": returning_crew_status.get("_id")}
                    ],
                    "billed": False,
                    "$and": [
                        {
                        "answers": {
                            "$elemMatch": {
                                "field": "company",
                                "answer": company.get("_id")    
                            }
                        }
                        },
                        {
                        "answers": {
                            "$elemMatch": {
                                "field": "embarkation_date",
                                "answer": {
                                    "$gte": start_embarkation_date,
                                    "$lte": end_embarkation_date,
                                }
                            }
                        }
                        }
                    ]
                }

                processes, _ = process_repository.get_all(1, 100000, **filters)

            else:
                filters = {
                    "$or": [
                        {"status": onboard_status.get("_id")},
                        {"status": returning_crew_status.get("_id")}
                    ],
                    "billed": False,
                    "$and": [
                        {
                        "answers": {
                            "$elemMatch": {
                                "field": "company",
                                "answer": company.get("_id")    
                            }
                        }
                        },
                        {
                        "answers": {
                            "$elemMatch": {
                                "field": "embarkation_date",
                                "answer": {
                                    "$lte": embarkation_date,
                                }
                            }
                        }
                        }
                    ]
                }

                processes, _ = process_repository.get_all(1, 10000, **filters)

            if len(processes) > 0:
                if company.get("company") == "Princess Cruises":
                    new_processes = []
                    for process in processes:
                        if process.get("status") == onboard_status.get("_id"):
                            new_processes.append(process)

                    processes = processes + new_processes

                invoice = {
                    "invoice_id": f"{company.get('billing_next_number')}".zfill(4),
                    "company": company.get("_id"),
                    "date_created": datetime.datetime.now(),
                    "items": [],
                    "status": created_status.get("_id"),
                    "status_history": [
                        {"status": created_status.get("_id"), "date": datetime.datetime.now()}
                    ],
                    "total": 0.0,
                }

                # Update company's billing info
                company["billing_next_number"] = str(
                    int(company.get("billing_next_number", 0)) + 1
                )
                company_data = {k: v for k, v in company.items() if k != "_id"}

                company_repository.update(company["_id"], **company_data)

                invoice_items = []
                invoice_total = 0
                for process in processes:
                    process_answers = process.get("answers", [])
                    position = [item for item in process_answers if item.get("field") == "position"][0].get("answer")
                    position = position_repository.get(_id=ObjectId(position)) if position != "" else None
                    candidate = users_repository.get(_id=process.get("candidate"))
                    ship = [item for item in process_answers if item.get("field") == "ship"][0].get("answer")
                    ship = ship_repository.get(_id=ObjectId(ship)) if ship != "" else None

                    item = {
                        "process": process.get("_id"),
                        "total": 0.0,
                    }

                    process_total = 0
                    if company.get("company") == "Princess Cruises":
                        if process.get("status") == returning_crew_status.get("_id"):
                            process_total = 95
                        elif process.get("status") == onboard_status.get("_id"):
                            if not any(
                                d["process"] == process.get("_id") for d in invoice_items
                            ):
                                process_total = position.get("price") if position else 0
                            else:
                                process_total = 125

                    else:
                        process_total = position.get("price", 0) if position else 0

                    item["total"] = float(process_total)

                    invoice_items.append(item)
                    invoice_total += float(process_total)

                invoice["items"] = invoice_items
                invoice["total"] = invoice_total

                invoice = invoice_repository.create(**invoice)
                invoice = invoice_repository.get(_id=invoice.inserted_id)

                send_message.send_email(
                    to=["jflorez@ssg.eu.com"],
                    body="invoice_generated_admin",
                    subject=f"Invoice {company.get('billing_prefix')} {invoice.get('invoice_id')} created",
                    invoice=invoice,
                    company=company,
                    app=current_app,
                )

                return invoice

            else:
                send_message.send_email(
                    to=["jflorez@ssg.eu.com"],
                    body="no_processes_to_bill",
                    company=company,
                    app=current_app
                )

                return {}
            
        except Exception as e:
            log_repository.create_log("Generate invoice", f"Error: {str(e)}")
            print("ERROR CONSOLA: ", traceback.format_exc())
            raise e