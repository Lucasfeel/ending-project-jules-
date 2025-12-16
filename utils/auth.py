import os
from functools import wraps

import jwt
from flask import jsonify, request, g
from jwt import ExpiredSignatureError, InvalidIssuerError, InvalidTokenError

JWT_SECRET = os.getenv('JWT_SECRET')
JWT_ISSUER = os.getenv('JWT_ISSUER', 'ending-signal')


def _error_response(status_code: int, code: str, message: str):
    return (
        jsonify({'success': False, 'error': {'code': code, 'message': message}}),
        status_code,
    )


def _decode_token(token: str):
    if not JWT_SECRET:
        raise InvalidTokenError('JWT secret is not configured')

    return jwt.decode(
        token,
        JWT_SECRET,
        algorithms=['HS256'],
        issuer=JWT_ISSUER,
    )


def login_required(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        auth_header = request.headers.get('Authorization', '')
        if not auth_header.startswith('Bearer '):
            return _error_response(401, 'AUTH_REQUIRED', 'Authentication required')

        token = auth_header.split(' ', 1)[1].strip()
        if not token:
            return _error_response(401, 'AUTH_REQUIRED', 'Authentication required')

        try:
            payload = _decode_token(token)
        except ExpiredSignatureError:
            return _error_response(401, 'TOKEN_EXPIRED', 'Token has expired')
        except InvalidIssuerError:
            return _error_response(401, 'INVALID_TOKEN', 'Invalid token issuer')
        except InvalidTokenError:
            return _error_response(401, 'INVALID_TOKEN', 'Invalid token')

        g.current_user = {
            'id': payload.get('uid'),
            'email': payload.get('email'),
            'role': payload.get('role'),
        }
        return func(*args, **kwargs)

    return wrapper


def admin_required(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        current_user = getattr(g, 'current_user', None)
        if not current_user or current_user.get('role') != 'admin':
            return _error_response(403, 'FORBIDDEN', 'Admin privileges required')
        return func(*args, **kwargs)

    return wrapper
