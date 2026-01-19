import traceback

from marshmallow import ValidationError
from flask import jsonify
from botocore.exceptions import ClientError
from src.infrastructure.repositories.mongodb.log_repository import LogRepository

log_repository = LogRepository()


def handle_client_error(e, origen, status_code=401):
    if isinstance(e, ClientError) and e.operation_name:
        if e.response["Error"]["Code"] == "AccessDeniedException":
            return handle_general_error(e, origen)

    if isinstance(e, str):
        message = e
    elif isinstance(e, (ValueError, ValidationError)):
        message = str(e)
    else:
        message = e.response["Error"]["Message"]

    log_repository.create_log(origen, "Advertencia", message)
    return jsonify({"message": message}), status_code


def handle_general_error(e, origen):
    print(traceback.format_exc())
    log_repository.create_log(origen, "Error no controlado", traceback.format_exc())
    return (
        jsonify(
            {
                "message": f"Ha ocurrido un error inesperado al {origen}. Por favor, contacta al administrador"
            }
        ),
        500,
    )
