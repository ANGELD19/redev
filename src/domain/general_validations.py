from marshmallow import ValidationError


def validate_hexadecimal(value):
    if not all(c in "0123456789abcdefABCDEF" for c in value):
        raise ValidationError("Invalid hexadecimal value")
