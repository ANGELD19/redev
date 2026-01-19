import os
import datetime

from flask import Flask
from flask_cors import CORS
from flask_jwt_extended import jwt_required, JWTManager

from src.application.billing_service import Billing

from src.middleware.hasRole import has_role

app = Flask(__name__)
app.config["CHARSET"] = "UTF-8"
configurations = os.environ

app.config["JWT_SECRET_KEY"] = configurations.get("JWT_SECRET_KEY")
app.config["JWT_ACCESS_TOKEN_EXPIRES"] = datetime.timedelta(hours=6)

jwt = JWTManager(app)

billing = Billing()

CORS(app, resources={r"/*": {"origins": "*", "send_wildcard": "True"}})

@app.route("/")
def index():
    return "Testing, Flask!"

@app.route("/billing/invoice-status", methods=["GET"])
@jwt_required()
def get_invoice_status():
    return billing.invoice_status()

@app.route("/billing", methods=["GET"])
@jwt_required()
@has_role(["Superadmin"])
def get_invoices():
    return billing.get_all()

@app.route("/billing/view-invoice/<invoice_id>", methods=["GET"])
@jwt_required()
@has_role(["Superadmin"])
def view_invoice(invoice_id):
    return billing.view(invoice_id)

@app.route("/billing/create-company-invoice/<company_id>", methods=["POST"])
@jwt_required()
@has_role(["Superadmin"])
def create_company_invoice(company_id):
    return billing.create_company_invoice(company_id)

@app.route("/billing/generate-invoice-pdf/<invoice_id>", methods=["POST"])
@jwt_required()
@has_role(["Superadmin"])
def generate_invoice_pdf(invoice_id):
    return billing.generate_pdf(invoice_id)

@app.route("/billing/send-invoice/<invoice_id>", methods=["POST"])
@jwt_required()
@has_role(["Visa Processing", "Superadmin"])
def send_invoice_to_company(invoice_id):
    return billing.send_invoice(invoice_id)

@app.route("/billing/edit-invoice/<invoice_id>", methods=["PUT"])
@jwt_required()
@has_role(["Superadmin"])
def remove_process_from_invoice(invoice_id):
    return billing.edit(invoice_id)

@app.route("/billing/add-process-to-invoice/<invoice_id>", methods=["PUT"])
@jwt_required()
@has_role(["Superadmin"])
def add_process_to_invoice(invoice_id):
    return billing.add_process(invoice_id)

@app.route("/billing/mark-processes-as-not-billed", methods=["POST"])
@jwt_required()
@has_role(["Superadmin"])
def mark_processes_as_not_billed():
    return billing.not_billed()

@app.route("/billing/mark-processes-as-billed", methods=["POST"])
@jwt_required()
@has_role(["Superadmin"])
def mark_processes_as_billed():
    return billing.billed()

@app.route("/billing/delete-invoice/<invoice_id>", methods=["DELETE"])
@jwt_required()
@has_role(["Superadmin"])
def delete_invoice(invoice_id):
    return billing.delete(invoice_id)

@app.route("/billing/mark-invoice-as-paid/<invoice_id>", methods=["PUT"])
@jwt_required()
@has_role(["Superadmin"])
def mark_invoice_as_paid(invoice_id):
    return billing.mark_as_paid(invoice_id)

if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=8080)