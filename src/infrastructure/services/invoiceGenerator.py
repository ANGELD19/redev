import calendar
import math
import datetime
import os
import boto3
import html

from threading import Thread
from dateutil.relativedelta import relativedelta
from fpdf import FPDF, HTMLMixin

from flask import current_app
from bson import ObjectId

from src.infrastructure.utils.clean_filters import dateformat

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



WIDTH = 279
HEIGHT = 216


class MyFPDF(FPDF, HTMLMixin):
    pass


def create_title(pdf, invoice, company):
    # Set invoice date
    current_date = datetime.datetime.now()
    invoice_date = current_date - relativedelta(months=2) if company.get("company") != "Princess Cruises" else current_date
    invoice_date = invoice_date.replace(day=calendar.monthrange(invoice_date.year, invoice_date.month)[-1],hour=23,minute=59,second=59,) if company.get("company") != "Princess Cruises" else current_date
    invoice_date = invoice_date.strftime("%B %d, %Y")

    pdf.image(
        name=f'https://{os.getenv("S3_BUCKET_PREFIX")}-public-{os.getenv("S3_BUCKET_ENV")}.s3.amazonaws.com/images/logo.png',
        x=15,
        y=5,
        w=45,
    )

    # Set x and y
    pdf.set_xy(x=-60, y=10)

    # Then put a red
    pdf.set_font("Arial", "B", 12)
    pdf.set_fill_color(r=117, g=190, b=235)
    pdf.multi_cell(
        w=60,
        h=10,
        txt=f"{invoice.get('invoice_id')}\n{invoice_date}",
        border=0,
        align="J",
        fill=True,
    )

    # Set x and y
    pdf.set_xy(x=15, y=50)
    pdf.write(h=5, txt="Bill to:")
    pdf.set_font("Arial", "", 12)
    pdf.set_xy(x=15, y=55)
    pdf.write(h=5, txt=f"{company.get('company')}")
    pdf.set_xy(x=15, y=60)
    pdf.write(h=5, txt=f"{company.get('billing_address', '')}")
    pdf.set_xy(x=15, y=65)
    pdf.write(h=5, txt=f"Attn: {company.get('billing_attn')}")

    # Set x and y
    pdf.set_font("Arial", "B", 9)
    pdf.set_xy(x=-150, y=50)
    pdf.multi_cell(
        w=140,
        h=5,
        txt=f"BANK ACCOUNT DETAILS:\nBANK OF AMERICA - MIAMI DOWNTOWN - 1 SE 3RD AVENUE - MIAMI, FLORIDA 33132\n+1 305 350 6350\nABA: 026009593\nSWIFT: BOFAUS3N\nACC #229032548243\nROUTING #063100277",
        border=0,
        align="R",
        fill=False,
    )


def create_items_list(pdf, items, invoice, is_last_page):
    pdf.set_xy(x=-150, y=90)

    pdf.write_html("""
        <table border="0" align="center" width="100%">
            <thead>
                <tr bgcolor="#75BEEB" style="font-weight: bold;">
                    <th width="50%" align="center" style="border-top: 1px solid #000; border-bottom: 1px solid #000;">JOB</th>
                    <th width="50%" align="center" style="border-top: 1px solid #000; border-bottom: 1px solid #000;">PAYMENT METHOD</th>
                </tr>
            </thead>
            <tbody>
                <tr bgcolor="#FFFFFF">
                    <td width="50%" align="center">Recruitment Fees</td>
                    <td width="50%" align="center">WIRE TRANSFER</td>
                </tr>
            </tbody>
        </table>
    """)

    pdf.set_font("Arial", "", 9)

    html = """
        <table border="0" align="center" width="100%" style="font-family: 'Times New Roman'; font-size: 9pt;">
            <thead>
                <tr bgcolor="#75BEEB" style="font-weight: bold;">
                    <th width="10%" align="center">ID</th>
                    <th width="25%" align="center">Candidate</th>
                    <th width="30%" align="center">Position</th>
                    <th width="15%" align="center">Ship</th>
                    <th width="10%" align="center">Date Joined</th>
                    <th width="10%" align="center">Total (USD)</th>
                </tr>
            </thead>
            <tbody>
    """

    for item in items:
        html += f"""
            <tr bgcolor="#FFFFFF">
                <td width="10%" align="center">{item.get("process_code", "-")}</td>
                <td width="25%" align="center">{item["candidate"].get("fullname", "-")}</td>
                <td width="30%" align="center">{item.get("position", "TBD")}</td>
                <td width="15%" align="center">{item.get("ship", "TBD")}</td>
                <td width="10%" align="center">{item.get("embarkation_date", "TBD")}</td>
                <td width="10%" align="center">{float(item.get("total", 0))}</td>
            </tr>
        """

    html += "</tbody>"

    if is_last_page:
        html += f"""         
                </tbody>
                <tfoot>
                    <tr bgcolor="#FEFF99">
                        <td colspan="5"></td>
                        <td align="right">${invoice.get('total')} USD</td>
                    </tr>
                </tfoot>
            </table>
        """
    else:
        html += "</tbody></table>"


    pdf.write_html(html)


def create_footer(pdf):
    # Set x and y
    pdf.set_xy(x=70, y=-25)
    pdf.set_font("Arial", "B", 10)
    pdf.multi_cell(
        w=150,
        h=5,
        txt=f"11850 Biscayne Blvd. #347 - Miami, Florida 33181 | Phone +1 786 804 1064\nwww.thesevenseasgroup.eu\n Welcome Onboard! Let the journey to success begin with us...",
        border=0,
        align="C",
        fill=False,
    )


def create_invoice(invoice):
        if not invoice:
            return

        # Obtener empresa
        company = company_repository.get(_id=invoice.get("company"))
        items = list(invoice.get("items"))

        # Obtener procesos
        process_ids = [ObjectId(item["process"]) for item in items if item.get("process")]
        processes = process_repository.get_many({"_id": {"$in": process_ids}})
        process_map = {str(p["_id"]): p for p in processes}

        # Obtener candidatos
        candidate_ids = [ObjectId(p["candidate"]) for p in processes if p.get("candidate")]
        candidates = users_repository.get_many({"_id": {"$in": candidate_ids}})
        candidate_map = {str(c["_id"]): c for c in candidates}

        # Obtener ships y positions desde answers
        position_ids = set()
        ship_ids = set()
        for process in processes:
            answers_list = process.get("answers", [])
            answers = {a.get("field"): a.get("answer") for a in answers_list}

            if answers.get("position"):
                position_ids.add(ObjectId(answers["position"]))

            if answers.get("ship"):
                ship_ids.add(ObjectId(answers["ship"]))

        positions = position_repository.get_many({"_id": {"$in": list(position_ids)}})
        ships = ship_repository.get_many({"_id": {"$in": list(ship_ids)}})
        position_map = {str(p["_id"]): p.get("name", "TBD") for p in positions}
        ship_map = {str(s["_id"]): s.get("name", "TBD") for s in ships}

        # Enlazar datos a los items
        for item in items:
            process_id = str(item.get("process"))
            process = process_map.get(process_id)
            if not process:
                continue

            candidate = candidate_map.get(str(process.get("candidate")))
            if not candidate:
                continue

            # Formatear nombre completo
            candidate["fullname"] = f"{candidate.get('lastname')}, {candidate.get('name')} {candidate.get('middle_name', '')}"

            # Procesar answers como dict
            answers_list = process.get("answers", [])
            answers = {a.get("field"): a.get("answer") for a in answers_list}

            embarkation_date = dateformat(answers.get("embarkation_date")) if answers.get("embarkation_date") else "TBD"
            position = position_map.get(str(answers.get("position"))) if answers.get("position") else "TBD"
            ship = ship_map.get(str(answers.get("ship"))) if answers.get("ship") else "TBD"
            process_code = str(answers.get("process_id", "")).replace(".00", "").replace(".0", "") or "-"

            item.update({
                "candidate": candidate,
                "process_code": process_code,
                "embarkation_date": embarkation_date,
                "position": position,
                "ship": ship
            })

        # Ordenar por nombre completo
        items = sorted(items, key=lambda d: d["candidate"]["fullname"])
        invoice["items"] = items

        # Crear PDF
        pdf = MyFPDF("L", "mm", "Letter")
        pdf.set_auto_page_break(0)

        number_of_pages = math.ceil(len(items) / 10)
        for page in range(number_of_pages):
            pdf.add_page()
            is_last_page = page == number_of_pages - 1

            # Título
            create_title(pdf=pdf, invoice=invoice, company=company)

            # Paginación
            page_items = items[page * 10: (page + 1) * 10]

            # Tabla
            create_items_list(
                pdf=pdf,
                items=page_items,
                invoice=invoice,
                is_last_page=is_last_page,
            )

            # Footer
            create_footer(pdf=pdf)

        # Nombre de archivo
        filename = f"invoice_{company.get('billing_prefix', company.get('short'))}_{invoice.get('invoice_id')}.pdf"

        # Subir a S3
        s3_resource = boto3.resource("s3")
        s3_resource.Bucket(
            f"{os.getenv('S3_BUCKET_PREFIX')}-private-{os.getenv('S3_BUCKET_ENV')}"
        ).put_object(
            Key=f"invoices/{filename}",
            Body=pdf.output(dest="S"),
            ContentType="application/pdf",
        )
        return f"invoices/{filename}"