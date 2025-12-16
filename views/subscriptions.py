# views/subscriptions.py

import psycopg2
from flask import Blueprint, jsonify, request, g

from database import get_db, get_cursor
from utils.auth import login_required

subscriptions_bp = Blueprint('subscriptions', __name__)


def _error_response(code: str, message: str, status: int):
    return jsonify({'success': False, 'error': {'code': code, 'message': message}}), status


def _get_payload_content(data):
    content_id = data.get('contentId') or data.get('content_id')
    source = data.get('source')
    return content_id, source


@subscriptions_bp.route('/api/me/subscriptions', methods=['GET'])
@login_required
def list_subscriptions():
    user_id = g.current_user.get('id')
    try:
        conn = get_db()
        cursor = get_cursor(conn)

        cursor.execute(
            """
            SELECT s.content_id, s.source, c.content_type, c.title, c.status, c.meta
            FROM subscriptions s
            JOIN contents c ON s.content_id = c.content_id AND s.source = c.source
            WHERE s.user_id = %s
            ORDER BY c.title ASC
            """,
            (user_id,),
        )
        rows = cursor.fetchall()
        cursor.close()

        subscriptions = []
        for row in rows:
            subscriptions.append(
                {
                    'contentId': row['content_id'],
                    'source': row['source'],
                    'contentType': row['content_type'],
                    'title': row['title'],
                    'status': row['status'],
                    'meta': row['meta'],
                }
            )

        return jsonify({'success': True, 'subscriptions': subscriptions}), 200
    except psycopg2.Error:
        return _error_response('DB_ERROR', '데이터베이스 오류가 발생했습니다.', 500)
    except Exception:
        return _error_response('SERVER_ERROR', '서버 오류가 발생했습니다.', 500)


@subscriptions_bp.route('/api/me/subscriptions', methods=['POST'])
@login_required
def subscribe():
    data = request.get_json() or {}
    content_id, source = _get_payload_content(data)

    if not content_id or not source:
        return _error_response('INVALID_REQUEST', 'contentId와 source가 필요합니다.', 400)

    user_id = g.current_user.get('id')

    try:
        conn = get_db()
        cursor = get_cursor(conn)

        cursor.execute(
            'SELECT 1 FROM contents WHERE content_id = %s AND source = %s',
            (str(content_id), source),
        )
        if cursor.fetchone() is None:
            cursor.close()
            return _error_response('CONTENT_NOT_FOUND', '존재하지 않는 콘텐츠입니다.', 404)

        cursor.execute(
            """
            INSERT INTO subscriptions (user_id, content_id, source)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id, content_id, source) DO NOTHING
            """,
            (user_id, str(content_id), source),
        )
        conn.commit()
        cursor.close()
        return jsonify({'success': True}), 200
    except psycopg2.Error:
        conn.rollback()
        return _error_response('DB_ERROR', '데이터베이스 오류가 발생했습니다.', 500)
    except Exception:
        conn.rollback()
        return _error_response('SERVER_ERROR', '서버 오류가 발생했습니다.', 500)


@subscriptions_bp.route('/api/me/subscriptions', methods=['DELETE'])
@login_required
def unsubscribe():
    data = request.get_json() or {}
    content_id, source = _get_payload_content(data)

    if not content_id or not source:
        return _error_response('INVALID_REQUEST', 'contentId와 source가 필요합니다.', 400)

    user_id = g.current_user.get('id')

    try:
        conn = get_db()
        cursor = get_cursor(conn)

        cursor.execute(
            'DELETE FROM subscriptions WHERE user_id = %s AND content_id = %s AND source = %s',
            (user_id, str(content_id), source),
        )
        conn.commit()
        cursor.close()
        return jsonify({'success': True}), 200
    except psycopg2.Error:
        conn.rollback()
        return _error_response('DB_ERROR', '데이터베이스 오류가 발생했습니다.', 500)
    except Exception:
        conn.rollback()
        return _error_response('SERVER_ERROR', '서버 오류가 발생했습니다.', 500)

