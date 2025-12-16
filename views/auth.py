from flask import Blueprint, jsonify, request, g
import psycopg2

from services.auth_service import (
    authenticate_user,
    create_access_token,
    is_valid_email,
    register_user,
)
from utils.auth import admin_required, login_required

auth_bp = Blueprint('auth', __name__)


@auth_bp.route('/api/auth/register', methods=['POST'])
def register():
    data = request.get_json() or {}
    email = data.get('email')
    password = data.get('password')

    if not email or not password:
        return jsonify({'success': False, 'message': '이메일과 비밀번호가 필요합니다.'}), 400
    if not is_valid_email(email):
        return jsonify({'success': False, 'message': '올바른 이메일 형식이 아닙니다.'}), 400

    try:
        user, error = register_user(email, password)
        if error:
            return jsonify({'success': False, 'message': error}), 400
        return jsonify({'success': True, 'user_id': user['id']}), 201
    except psycopg2.Error:
        return jsonify({'success': False, 'message': '데이터베이스 오류가 발생했습니다.'}), 500
    except Exception:
        return jsonify({'success': False, 'message': '서버 오류가 발생했습니다.'}), 500


@auth_bp.route('/api/auth/login', methods=['POST'])
def login():
    data = request.get_json() or {}
    email = data.get('email')
    password = data.get('password')

    if not email or not password:
        return jsonify({'success': False, 'message': '이메일과 비밀번호가 필요합니다.'}), 400

    try:
        user = authenticate_user(email, password)
        if not user:
            return jsonify({'success': False, 'message': '이메일 또는 비밀번호가 올바르지 않습니다.'}), 401

        token, expires_in = create_access_token(user)
        return (
            jsonify(
                {
                    'access_token': token,
                    'token_type': 'bearer',
                    'expires_in': expires_in,
                    'user': user,
                }
            ),
            200,
        )
    except psycopg2.Error:
        return jsonify({'success': False, 'message': '데이터베이스 오류가 발생했습니다.'}), 500
    except Exception:
        return jsonify({'success': False, 'message': '서버 오류가 발생했습니다.'}), 500


@auth_bp.route('/api/auth/logout', methods=['POST'])
def logout():
    return jsonify({'success': True}), 200


@auth_bp.route('/api/auth/me', methods=['GET'])
@login_required
def me():
    return jsonify({'success': True, 'user': g.current_user}), 200


@auth_bp.route('/api/auth/admin/ping', methods=['GET'])
@login_required
@admin_required
def admin_ping():
    return jsonify({'success': True, 'message': 'admin ok'}), 200
