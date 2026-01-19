import functools

from flask_jwt_extended import get_jwt


def has_role(roles):
    def real_decorator(f):
        def wraps(*args, **kwargs):
            # Get all attributes in the JWT token
            claims = get_jwt()

            # Block user's access by default
            can_access = False

            # Check if user has the roles to access the view
            for role in claims.get("roles", []):
                if role in roles:
                    can_access = True
                    break

            if can_access:
                return f(*args, **kwargs)
            else:
                return {
                    "data": {},
                    "message": "You don't have permissions to access this page",
                }, 403

        return functools.update_wrapper(wraps, f)

    return real_decorator