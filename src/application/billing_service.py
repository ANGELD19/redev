import traceback
import json
import secrets
import os

from flask import current_app
from datetime import datetime, timezone
from bson import ObjectId, json_util
from flask import request
from marshmallow import ValidationError

from src.domain.constant import LOOKUP, SORT, UNSET, MATCH

from src.infrastructure.repositories.mongodb.log_repository import LogRepository
from src.infrastructure.repositories.mongodb.invoice_status_repository import InvoiceStatusRepository
from src.infrastructure.repositories.mongodb.invoice_repository import InvoiceRepository
from src.infrastructure.repositories.mongodb.process_repository import ProcessRepository
from src.infrastructure.repositories.mongodb.process_status_repository import ProcessStatusRepository
from src.infrastructure.repositories.mongodb.country_repository import CountryRepository
from src.infrastructure.repositories.mongodb.company_repository import CompanyRepository
from src.infrastructure.repositories.mongodb.ship_repository import ShipRepository
from src.infrastructure.repositories.mongodb.position_repository import PositionRepository
from src.infrastructure.repositories.mongodb.users_repository import UsersRepository
from src.infrastructure.services.invoiceGenerator import create_invoice
from flask_jwt_extended import get_jwt_identity

from src.infrastructure.utils.clean_filters import clean_filters
from src.infrastructure.utils.handler_error import (
    handle_general_error,
    handle_client_error
)
from src.infrastructure.services.s3 import S3
from src.infrastructure.services.generate_invoice import GenerateInvoice
from src.infrastructure.services.send_messages import SendMessages


generate_invoice = GenerateInvoice()
company_repository = CompanyRepository()
users_repository = UsersRepository()
position_repository = PositionRepository()
country_repository = CountryRepository()
ship_repository = ShipRepository()
process_repository = ProcessRepository()
process_status_repository = ProcessStatusRepository()
invoice_status_repository = InvoiceStatusRepository()
invoice_repository = InvoiceRepository()
send_message = SendMessages()
log_repository = LogRepository()
s3 = S3()

class Billing:
    def invoice_status(self):
        origen = "All invoice status"
        try:
            page = int(request.args.get("page", 1))
            page_size = int(request.args.get("limit", 20))
            filters = clean_filters(request.args.to_dict())
            
            all_data, total_pages = invoice_status_repository.get_all(
                page, page_size, **filters
            )

            log_repository.create_log(origen, "Exitoso")

            data = {
                "data": all_data,
                "total_pages": total_pages,
                "current_page": page,
                "message": {
                    "text": "All invoice status successfully loaded.",
                    "type": "success"
                }
            }
            return json.loads(json_util.dumps(data)), 200

        except ValueError as e:
            return handle_client_error(e, origen, 404)
        except ValidationError as e:
            return handle_client_error(e, origen, 400)
        except Exception as e:
            return handle_general_error(e, origen)
        
    def get_all(self):
        origen = "All invoices"
        try:
            page = int(request.args.get("page", 1))
            page_size = int(request.args.get("limit", 20))
            
            raw_filters = request.args.to_dict()

            start_date_str = raw_filters.pop("start_date", None)
            end_date_str = raw_filters.pop("end_date", None)

            filters = clean_filters(raw_filters)

            if start_date_str and end_date_str:
                start_date = datetime.strptime(start_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                end_date = datetime.strptime(end_date_str, "%Y-%m-%d").replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
                filters["date_created"] = {"$gte": start_date, "$lte": end_date}

            invoices, total_pages = invoice_repository.get_invoices(
                page, page_size, filters
            )

            log_repository.create_log(origen, "Exitoso")

            data = {
                "data": {"invoices": invoices},
                "total_pages": total_pages,
                "current_page": page,
                "message": {
                    "text": "All invoices successfully loaded",
                    "type": "success",
                },
            }
            log_repository.create_log(origen, "Exitoso")
            
            return json.loads(json_util.dumps(data)), 200

        except ValueError as e:
            return handle_client_error(e, origen, 404)
        except ValidationError as e:
            return handle_client_error(e, origen, 400)
        except Exception as e:
            return handle_general_error(e, origen)


    def view(self, invoice_id):
        origen = "View invoice"
        try:
            invoice = invoice_repository.get_invoice(invoice_id)
            if not invoice:
                raise ValueError("We did not find the invoice you were looking for")

            invoice_status = invoice.get("status", {})
            if invoice_status and isinstance(invoice_status, list) and invoice_status[0].get("status") == "Created":
                under_review_status = invoice_status_repository.get(status="Under review")
                invoice_repository.update_fields(
                    id=invoice["_id"],
                    set_fields={"status": under_review_status["_id"]},
                    push_fields={"status_history": {"status": under_review_status["_id"], "date": datetime.now()}}
                )

                invoice = invoice_repository.get_invoice(invoice_id)

            invoice_items = list(invoice.get("items", []))

            process_ids = [ObjectId(item["process"]) for item in invoice_items if item.get("process")]
            processes = {
                p["_id"]: p for p in process_repository.find_many({"_id": {"$in": process_ids}})
            }

            candidate_ids, recruiter_ids, company_ids = set(), set(), set()
            status_ids, ship_ids, position_ids = set(), set(), set()

            for process in processes.values():
                answers = {a["field"]: a.get("answer") for a in process.get("answers", [])}
                if process.get("candidate"):
                    candidate_ids.add(process["candidate"])
                recruiter_ids.update(process.get("recruiters", []))
                status_ids.add(process.get("status"))

                if (cid := answers.get("company")): company_ids.add(cid)
                if (sid := answers.get("ship")): ship_ids.add(sid)
                if (pid := answers.get("position")): position_ids.add(pid)

            # Consultas en batch
            candidates = {
                c["_id"]: c for c in users_repository.find_many({"_id": {"$in": list(candidate_ids)}})
            }
            recruiters = {
                r["_id"]: r for r in users_repository.find_many({"_id": {"$in": list(recruiter_ids)}})
            }
            companies = {
                c["_id"]: c for c in company_repository.find_many({"_id": {"$in": list(company_ids)}})
            }
            statuses = {
                s["_id"]: s for s in process_status_repository.find_many({"_id": {"$in": list(status_ids)}})
            }
            ships = {
                s["_id"]: s for s in ship_repository.find_many({"_id": {"$in": list(ship_ids)}})
            }
            positions = {
                p["_id"]: p for p in position_repository.find_many({"_id": {"$in": list(position_ids)}})
            }
            countries = {}

            # Enriquecer los items
            for item in invoice_items:
                process = processes.get(ObjectId(item.get("process")))
                if not process:
                    item["process"] = {"embarkation_date": "", "status": ""}
                    continue

                answers = {a["field"]: a.get("answer") for a in process.get("answers", [])}

                # Embarkation date
                embarkation_date = answers.get("embarkation_date")
                process["embarkation_date"] = (
                    datetime.strftime(embarkation_date, "%m-%d-%Y")
                    if isinstance(embarkation_date, datetime)
                    else ""
                )

                raw_status = process.get("status")

                if isinstance(raw_status, str):
                    resolved_status = raw_status

                else:
                    if isinstance(raw_status, dict):
                        status_id = raw_status.get("_id")
                    elif isinstance(raw_status, ObjectId):
                        status_id = raw_status
                    else:
                        status_id = None

                    resolved_status = statuses.get(status_id, {}).get("status", "Unknown") if status_id else "Unknown"

                process["status"] = resolved_status

                # Company
                company_id = answers.get("company")
                process["company"] = companies.get(company_id, {}).get("company", "") if company_id else ""

                original_recruiters = process.get("recruiters", [])
                process["recruiters"] = []
                for r in original_recruiters:
                    rid = r["_id"] if isinstance(r, dict) else r
                    recruiter_data = recruiters.get(rid)
                    if recruiter_data:
                        process["recruiters"].append(recruiter_data)

                # Candidate
                candidate = candidates.get(process.get("candidate"))
                item["candidate"] = candidate or {}
                if candidate:
                    item["candidate"]["fullname"] = f"{candidate.get('lastname', '')}, {candidate.get('name', '')} {candidate.get('middle_name', '')}".strip()

                    passport = candidate.get("passport", {})
                    country_data = passport.get("country")
                    country_id = country_data.get("_id") if isinstance(country_data, dict) else country_data
                    
                    if country_id and country_id not in countries:
                        country = country_repository.get(_id=country_id)
                        countries[country_id] = country if country else ""
                    if country_id:
                        passport["country"] = countries.get(country_id, "")
                        item["candidate"]["passport"] = passport

                # Ship & position
                item["ship"] = ships.get(answers.get("ship")) if answers.get("ship") else None
                item["position"] = positions.get(answers.get("position")) if answers.get("position") else None

                # Attach process
                item["process"] = process

            # Sort items
            invoice_items = sorted(invoice_items, key=lambda d: d.get("candidate", {}).get("fullname", ""))
            invoice["items"] = invoice_items

            invoice["pdf_url"] = s3.generate_presigned_url(
                f"invoices/invoice_{invoice.get('company')[0].get('billing_prefix', invoice.get('company')[0].get('short'))}_{invoice.get('invoice_id')}.pdf",
                1800
            )

            log_repository.create_log(origen, "Exitoso")

            data = {
                "data": {"invoice": invoice},
                "message": {
                    "text": "Invoices are being generated. You will be notified once the process has been completed",
                    "type": "success",
                },
            }

            return json.loads(json_util.dumps(data)), 200
        except ValueError as e:
            return handle_client_error(e, origen, 404)
        except ValidationError as e:
            return handle_client_error(e, origen, 400)
        except Exception as e:
            return handle_general_error(e, origen)


    def create_company_invoice(self, company_id):
        origen = "Create company invoice"
        try:
            user = users_repository.get(email=get_jwt_identity())
            company = company_repository.get(_id=ObjectId(company_id))
            
            # Generate Invoice
            invoice = generate_invoice.generate(company=company, user=user)

            # Create invoice's PDF
            _ = create_invoice(invoice=invoice)
            
            data = {
                "data": {
                    "invoice" : invoice
                },
                "message": {
                    "text": "Invoice successfully generated.",
                    "type": "success",
                }
            }
            log_repository.create_log(origen, "Exitoso")

            return json.loads(json_util.dumps(data)), 200
        
        except ValueError as e:
            return handle_client_error(e, origen, 404)
        except ValidationError as e:
            return handle_client_error(e, origen, 400)
        except Exception as e:
            return handle_general_error(e, origen)
        
    def generate_pdf(self, invoice_id):
        origen = "Generate invoice"
        try:
            user = users_repository.get(email=get_jwt_identity())
            invoice = invoice_repository.get(_id=ObjectId(invoice_id))
            if not invoice:
                raise ValueError("We did not find the invoice you were looking for")
            
            key = create_invoice(invoice=invoice)
            
            pdf_url = s3.generate_presigned_url(key, 1800)

            log_repository.create_log(origen, "Exitoso")

            data = {
                "data": {"pdf_url": pdf_url},
                "message": {
                    "text": "The invoice PDF will be generated and sent to your email once completed.",
                    "type": "success",
                },
            }

            return json.loads(json_util.dumps(data)), 200

        except ValueError as e:
            return handle_client_error(e, origen, 404)
        except ValidationError as e:
            return handle_client_error(e, origen, 400)
        except Exception as e:
            return handle_general_error(e, origen)
    
    def send_invoice(self, invoice_id):
        origen = "Send invoice"
        try:
            invoice = invoice_repository.get(_id=ObjectId(invoice_id))
            if not invoice:
                raise ValueError("Invoice not found")

            company = company_repository.get(_id=invoice.get("company"))
            sent_to_company_status = invoice_status_repository.get(status="Submitted to company")

            invoice_data = {
                **invoice,
                "status": sent_to_company_status["_id"],
                "status_history": invoice.get("status_history", []) + [
                    {"status": sent_to_company_status["_id"], "date": datetime.now()}
                ]
            }
            invoice_data.pop("_id", None)
            invoice_repository.update(id=invoice["_id"], **invoice_data)

            invoice = invoice_repository.get_invoice(invoice_id)

            company_emails = company.get("billing_emails", []) + ["jflorez@ssg.eu.com"]

            filename = f"invoice_{invoice['company'][0].get('billing_prefix', invoice['company'][0].get('short'))}_{invoice['invoice_id']}.pdf"
            s3_key = f"invoices/{filename}"

            send_message.send_email(
                to=company_emails,
                cc=["mbonnett@xpetech.com", "jflorez@ssg.eu.com"] if os.getenv("ENV") == "production" else [os.getenv("TESTING_EMAIL")],
                body="invoice_generated",
                subject=f"Invoice {company.get('billing_prefix')} {invoice['invoice_id']}",
                invoice=invoice,
                company=company,
                filename=filename,
                attach_s3_key=s3_key,
            )

            invoice_items = invoice.get("items", [])
            process_ids = [ObjectId(item.get("process")) for item in invoice_items if item.get("process")]

            if process_ids:
                processes = process_repository.find_many({
                    "_id": {"$in": process_ids}
                })
                process_map = {p["_id"]: p for p in processes}

                updates = []
                for pid in process_ids:
                    process = process_map.get(pid)
                    if process:
                        process["billed"] = True
                        updates.append({
                            "id": pid,
                            "data": {k: v for k, v in process.items() if k != "_id"}
                        })

                if updates:
                    process_repository.bulk_update(updates)

            log_repository.create_log(origen, "Exitoso")

            return json.loads(json_util.dumps({
                "data": {},
                "message": {
                    "text": "The invoice was sent correctly",
                    "type": "success",
                },
            })), 200

        except ValueError as e:
            return handle_client_error(e, origen, 404)
        except ValidationError as e:
            return handle_client_error(e, origen, 400)
        except Exception as e:
            return handle_general_error(e, origen)
        
    def edit(self, invoice_id):
        origen = "Edit invoice"
        try:
            invoice = invoice_repository.get(_id=ObjectId(invoice_id))
            if not invoice:
                raise ValueError("Invoice not found")

            current_invoice_items = invoice.get("items", [])
            new_items_data = request.json.get("items", [])
            new_process_ids = {str(item.get("process")) for item in new_items_data}

            # Cache & update non-used processes
            updated_processes = []
            for item in current_invoice_items:
                process_id = item.get("process")
                if str(process_id) not in new_process_ids:
                    updated_processes.append(process_id)

            if updated_processes:
                for pid in updated_processes:
                    process_repository.update(id=ObjectId(pid), billed=False)

            # Prefetch all required processes in bulk
            all_process_ids = [ObjectId(item.get("process")) for item in new_items_data]
            processes = process_repository.find_many({"_id": {"$in": all_process_ids}})
            process_cache = {proc["_id"]: proc for proc in processes}

            # Caches
            position_ids = set()
            ship_ids = set()
            user_ids = set()

            invoice_items = []
            invoice_total = 0.0

            for item in new_items_data:
                process_id = ObjectId(item.get("process"))
                process = process_cache.get(process_id)
                if not process:
                    continue

                answers = {a["field"]: a.get("answer") for a in process.get("answers", [])}
                if answers.get("position"): position_ids.add(answers["position"])
                if answers.get("ship"): ship_ids.add(answers["ship"])
                if process.get("candidate"): user_ids.add(process["candidate"])

                invoice_items.append({
                    "process": process_id,
                    "total": float(item.get("total", 0.0)),
                })
                invoice_total += float(item.get("total", 0.0))
                
            # Edit invoice's information
            invoice["items"] = invoice_items
            invoice["total"] = float(invoice_total)
            
            invoice_data = dict(invoice)
            invoice_data.pop("_id", None)

            invoice_repository.update(
                id=invoice.get("_id"),
                **invoice_data
            )
            
            key = create_invoice(invoice=invoice)
            
            invoice = invoice_repository.get_invoice(invoice_id)
            
            # Prefetch candidates (users)
            users = users_repository.find_many({"_id": {"$in": list(user_ids)}})
            user_cache = {user["_id"]: user for user in users}
            
            # Prefetch final processes, ships, and positions in bulk
            final_process_ids = [item.get("process") for item in invoice.get("items")]
            final_processes = process_repository.find_many({"_id": {"$in": final_process_ids}})
            process_cache = {proc["_id"]: proc for proc in final_processes}

            ship_cache = {}
            if ship_ids:
                ships = ship_repository.find_many({"_id": {"$in": list(ship_ids)}})
                ship_cache = {ship["_id"]: ship for ship in ships}

            position_cache = {}
            if position_ids:
                positions = position_repository.find_many({"_id": {"$in": list(position_ids)}})
                position_cache = {pos["_id"]: pos for pos in positions}

            for item in invoice.get("items"):
                process = process_cache.get(item.get("process"))
                item["process"] = process

                answers = {a["field"]: a.get("answer") for a in process.get("answers", [])}

                candidate = user_cache.get(process.get("candidate"))
                if candidate:
                    candidate["fullname"] = f"{candidate.get('lastname')}, {candidate.get('name')} {candidate.get('middle_name', '')}"

                ship_id = answers.get("ship")
                item["ship"] = ship_cache.get(ship_id) if ship_id else None

                position_id = answers.get("position")
                item["position"] = position_cache.get(position_id) if position_id else None

            invoice["pdf_url"] = s3.generate_presigned_url(key, 1800)
            
            data = {
                "data": {"invoice": invoice},
                "message": {
                    "text": "Invoice successfully generated.",
                    "type": "success",
                },
            }
            log_repository.create_log(origen, "Exitoso")
            return json.loads(json_util.dumps(data)), 200
        except ValueError as e:
            return handle_client_error(e, origen, 404)
        except ValidationError as e:
            return handle_client_error(e, origen, 400)
        except Exception as e:
            return handle_general_error(e, origen)
        
    def add_process(self, invoice_id):
        origen = "Add procces to invoice"
        try:
            # Generate Invoice
            invoice = invoice_repository.get(_id=ObjectId(invoice_id))
            
            if not invoice:
                raise ValueError("We did not find the invoice you were looking for")
            
            # Get process from request
            process = process_repository.get(_id=ObjectId(request.json.get("process_id")))
            if not process:
                data = {
                    "data": {},
                    "message": {
                        "text": "We did not find the process you were looking for",
                        "type": "success",
                    },
                }
                return json.loads(json_util.dumps(data)), 404
            
            # Get other variables
            process_answers = process.get("answers", [])
            position = [item for item in process_answers if item.get("field") == "position"][0].get("answer")
            position = position_repository.get(_id=ObjectId(position)) if position != "" else None
            ship = [item for item in process_answers if item.get("field") == "ship"][0].get("answer")
            ship = ship_repository.get(_id=ObjectId(ship)) if ship != "" else None
            # Get candidate information
            
            candidate = users_repository.get(_id=process.get("candidate"))
            
            # Add item to invoice
            invoice_items = list(invoice.get("items"))
            invoice_items.append(
                {
                    "process": process.get("_id"),
                    "total": (
                        int(request.json.get("total"))
                        if request.json.get("total")
                        else position.get("price", 0)
                    ),
                }
            )

            invoice["items"] = invoice_items

            # Recalculate invoice's total
            invoice_total = 0.0
            for item in invoice.get("items"):
                invoice_total += float(item.get("total", 0))

            # Edit invoice's information
            invoice["total"] = float(invoice_total)
            
            invoice_data = dict(invoice)
            invoice_data.pop("_id", None)

            invoice_repository.update(
                id=invoice.get("_id"),
                **invoice_data
            )
            
            invoice = invoice_repository.get_invoice(invoice_id)
            
            data = {
                "data": {
                    "invoice" : invoice
                },
                "message": {
                    "text": "Invoice successfully generated.",
                    "type": "success",
                },
            }
            log_repository.create_log(origen, "Exitoso")

            return json.loads(json_util.dumps(data)), 200
        
        except ValueError as e:
            return handle_client_error(e, origen, 404)
        except ValidationError as e:
            return handle_client_error(e, origen, 400)
        except Exception as e:
            return handle_general_error(e, origen)
        
    def not_billed(self):
        origen = "Mark process as not billed"
        try:
            company = company_repository.get(_id=ObjectId(request.json.get("company")))
            embarkation_date = datetime.strptime(request.json.get("date"), "%Y-%m-%d")
            
            filters = {
                "billed": True,
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
                                    "$gte": embarkation_date
                                }
                            }
                        }
                    }
                ]
            }

            processes, _ = process_repository.get_all(
                page=1,
                page_size=100000,
                **filters
            )

            for process in processes:
                process["billed"] = False
                process_data = dict(process)
                process_data.pop("_id", None)

                process_repository.update(
                    id=process.get("_id"),
                    **process_data
                )

            data = {
                "data": {},
                "message": {
                    "text": "Process successfully updated.",
                    "type": "success",
                },
            }
            log_repository.create_log(origen, "Exitoso")

            return json.loads(json_util.dumps(data)), 200

        except ValueError as e:
            return handle_client_error(e, origen, 404)
        except ValidationError as e:
            return handle_client_error(e, origen, 400)
        except Exception as e:
            return handle_general_error(e, origen)
        
    def billed(self):
        origen = "Mark process as billed"
        try:
            company = company_repository.get(_id=ObjectId(request.json.get("company")))
            embarkation_date = datetime.strptime(request.json.get("date"), "%Y-%m-%d")

            filters = {
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
                                    "$gte": embarkation_date
                                }
                            }
                        }
                    }
                ]
            }

            processes, _ = process_repository.get_all(
                page=1,
                page_size=100000,
                **filters
            )

            for process in processes:
                process["billed"] = True
                process_data = dict(process)
                process_data.pop("_id", None)

                process_repository.update(
                    id=process.get("_id"),
                    **process_data
                )

            data = {
                "data": {},
                "message": {
                    "text": "Process successfully updated.",
                    "type": "success",
                },
            }
            
            log_repository.create_log(origen, "Exitoso")

            return json.loads(json_util.dumps(data)), 200

        except ValueError as e:
            return handle_client_error(e, origen, 404)
        except ValidationError as e:
            return handle_client_error(e, origen, 400)
        except Exception as e:
            return handle_general_error(e, origen)
        
    def delete(self, invoice_id):
        origen = "Delete invoice"
        try:
            # Generate Invoice
            invoice = invoice_repository.get(_id=ObjectId(invoice_id))
            
            if not invoice:
                raise ValueError("We did not find the invoice you were looking for")
            
            company = company_repository.get(_id=invoice.get("company"))
            company["billing_next_number"] = str(
                int(company.get("billing_next_number", 0)) - 1
            )
            company_data = dict(company)
            company_data.pop("_id", None)

            company_repository.update(
                id=company.get("_id"),
                **company_data
            )
            
            # Mark all invoices processes as not billed
            invoice_processes = [
                process_repository.get(_id=item.get("process"))
                for item in invoice.get("items")
            ]
            
            for process in invoice_processes:
                process["billed"] = False
                
                process_data = dict(process)
                process_data.pop("_id", None)

                process_repository.update(
                    id=process.get("_id"),
                    **process_data
                )
            
            # Get invoice
            invoice_repository.delete(id=invoice.get("_id"))
            
            data = {
                "data": {},
                "message": {
                    "text": "Invoice successfully deleted.",
                    "type": "success",
                },
            }
            log_repository.create_log(origen, "Exitoso")

            return json.loads(json_util.dumps(data)), 200
        
        except ValueError as e:
            return handle_client_error(e, origen, 404)
        except ValidationError as e:
            return handle_client_error(e, origen, 400)
        except Exception as e:
            return handle_general_error(e, origen)
        
    def mark_as_paid(self, invoice_id):
        origen = "Mark invoice as paid"
        try:
            invoice = invoice_repository.get(_id=ObjectId(invoice_id))
            if not invoice:
                raise ValueError("We did not find the invoice you were looking for")
            
            paid_status = invoice_status_repository.get(status="Paid")
            invoice["status"] = paid_status.get("_id")
            invoice["status_history"].append(
                {
                    "status": paid_status.get("_id"),
                    "date": datetime.now(),
                }
            )
            invoice_data = dict(invoice)
            invoice_data.pop("_id", None)

            invoice_repository.update(
                id=invoice.get("_id"),
                **invoice_data
            )
            
            invoice = invoice_repository.get_invoice(invoice_id)
            invoice_items = list(invoice.get("items"))

            for item in invoice_items:
                try:
                    process = process_repository.get(_id=ObjectId(item.get("process")))
                    if process:
                        item["process"] = process

                        answers = {a["field"]: a.get("answer") for a in process.get("answers", [])}

                        # Embarkation date
                        embarkation_date = answers.get("embarkation_date")
                        item["process"]["embarkation_date"] = (
                            datetime.strftime(embarkation_date, "%m-%d-%Y")
                            if isinstance(embarkation_date, datetime)
                            else ""
                        )

                        # Status
                        status_obj = process_status_repository.get(_id=process.get("status"))
                        item["process"]["status"] = status_obj.get("status") if status_obj else ""

                        # Company
                        company_id = answers.get("company")
                        company_obj = company_repository.get(_id=company_id) if company_id else None
                        item["process"]["company"] = company_obj.get("company") if company_obj else ""

                        # Recruiters
                        item["process"]["recruiters"] = [
                            users_repository.get(_id=recruiter_id)
                            for recruiter_id in process.get("recruiters", [])
                        ]

                        # Candidate
                        candidate = users_repository.get(_id=process.get("candidate"))
                        item["candidate"] = candidate
                        item["candidate"]["fullname"] = f"{candidate.get('lastname')}, {candidate.get('name')} {candidate.get('middle_name', '')}"

                        country_id = candidate.get("passport", {}).get("country")
                        country = country_repository.get(_id=country_id) if country_id else ""
                        item["candidate"]["passport"]["country"] = country if country else ""

                        # Ship y position
                        ship_id = answers.get("ship")
                        item["ship"] = ship_repository.get(_id=ship_id) if ship_id else None

                        position_id = answers.get("position")
                        item["position"] = position_repository.get(_id=position_id) if position_id else None

                    else:
                        item["process"] = {"embarkation_date": "", "status": ""}

                except Exception:
                    continue
            
            # Sort items
            invoice_items = sorted(
                invoice_items, key=lambda d: d["candidate"]["fullname"]
            )
            invoice["items"] = invoice_items
            
            invoice["pdf_url"] = s3.generate_presigned_url(f"invoices/invoice_{invoice.get('company')[0].get('billing_prefix', invoice.get('company')[0].get('short'))}_{invoice.get('invoice_id')}.pdf", 1800)

            log_repository.create_log(origen, "Exitoso")

            data = {
                "data": {"invoice": invoice},
                "message": {
                    "text": "Invoice has been marked as paid by company",
                    "type": "success",
                },
            }
            return json.loads(json_util.dumps(data)), 200

        except ValueError as e:
            return handle_client_error(e, origen, 404)
        except ValidationError as e:
            return handle_client_error(e, origen, 400)
        except Exception as e:
            return handle_general_error(e, origen)