# services/notification_service.py

import os
import smtplib
from email.mime.text import MIMEText
from datetime import datetime
import config

def send_email(recipient_email, subject, body, smtp_server=None):
    sender_email = os.getenv('EMAIL_ADDRESS')
    sender_password = os.getenv('EMAIL_PASSWORD')
    if not sender_email or not sender_password:
        print("오류: 이메일 발송을 위한 환경 변수가 설정되지 않았습니다.")
        return False
    msg = MIMEText(body, _charset='utf-8')
    msg['Subject'] = subject
    msg['From'] = sender_email
    msg['To'] = recipient_email
    try:
        if smtp_server is None:
            with smtplib.SMTP(config.SMTP_SERVER, config.SMTP_PORT) as server:
                server.starttls()
                server.login(sender_email, sender_password)
                server.sendmail(sender_email, recipient_email, msg.as_string())
        else:
            smtp_server.sendmail(sender_email, recipient_email, msg.as_string())
        return True
    except Exception as e:
        print(f"오류: {recipient_email}에게 이메일 발송 실패 - {e}")
        return False

def send_completion_notifications(cursor, newly_completed_ids, all_content_today, source):
    if not newly_completed_ids:
        print("\n새롭게 완결된 콘텐츠가 없습니다.")
        return [], 0

    print(f"\n🔥 새로운 완결 콘텐츠 {len(newly_completed_ids)}개 발견! 구독자 정보를 취합합니다.")

    # 1. 완결된 모든 콘텐츠에 대한 구독자 정보를 한 번에 가져오기
    # IN 연산자의 파라미터를 안전하게 전달하기 위해 튜플로 변환
    placeholders = ','.join(['%s'] * len(newly_completed_ids))
    query = f"SELECT email, content_id FROM subscriptions WHERE content_id IN ({placeholders}) AND source = %s"
    params = tuple(newly_completed_ids) + (source,)
    cursor.execute(query, params)

    subscriptions = cursor.fetchall()

    if not subscriptions:
        print(" -> 완결된 콘텐츠에 대한 구독자가 없습니다.")
        # 보고서에 기록하기 위해 콘텐츠 목록은 반환
        completed_details = [f"- '{all_content_today.get(cid, {}).get('titleName', f'ID {cid}')}' : 구독자 없음" for cid in newly_completed_ids]
        return completed_details, 0

    # 2. 사용자별로 완결된 콘텐츠 목록 그룹화
    user_notifications = {}
    for sub in subscriptions:
        email = sub['email']
        content_id = sub['content_id']
        title = all_content_today.get(content_id, {}).get('titleName', f'ID {content_id}')

        if email not in user_notifications:
            user_notifications[email] = []
        user_notifications[email].append(title)

    print(f" -> 총 {len(subscriptions)}개의 구독 건을 {len(user_notifications)}명의 사용자에게 통합하여 발송합니다.")

    # 3. 이메일 발송
    completed_details = []
    total_notified_users = 0
    sender_email = os.getenv('EMAIL_ADDRESS')
    sender_password = os.getenv('EMAIL_PASSWORD')

    if not sender_email or not sender_password:
        print("오류: 이메일 발송을 위한 환경 변수가 설정되지 않았습니다.")
        return [], 0

    try:
        with smtplib.SMTP(config.SMTP_SERVER, config.SMTP_PORT) as smtp_server:
            smtp_server.starttls()
            smtp_server.login(sender_email, sender_password)

            for email, titles in user_notifications.items():
                title_count = len(titles)
                first_title = titles[0]

                subject = f"콘텐츠 완결 알림: '{first_title}' 등 {title_count}건이 완결되었습니다!"

                body_lines = [
                    "안녕하세요! Ending Signal입니다.",
                    "\n회원님께서 구독하신 콘텐츠가 완결되어 알려드립니다.\n",
                    "--- 완결 목록 ---"
                ]
                body_lines.extend([f"- {title}" for title in titles])
                body_lines.append("\n지금 바로 정주행을 시작해보세요!\n\n감사합니다.")

                body = "\n".join(body_lines)

                send_email(email, subject, body, smtp_server)

            total_notified_users = len(user_notifications)

            # 보고용 상세 내역 생성
            for cid in newly_completed_ids:
                title = all_content_today.get(cid, {}).get('titleName', f'ID {cid}')
                subscriber_count = sum(1 for email, titles in user_notifications.items() if title in titles)
                if subscriber_count > 0:
                    completed_details.append(f"- '{title}' : {subscriber_count}명에게 알림 발송")
                else:
                    completed_details.append(f"- '{title}' : 구독자 없음")

            print(f"\n✅ 총 {total_notified_users}명에게 통합 알림 발송 완료.")

    except Exception as e:
        print(f"❌ 이메일 서버 연결 또는 발송 중 심각한 오류 발생: {e}")
        # 오류 발생 시 보고를 위해 빈 리스트 대신 None 등을 반환하거나 예외를 다시 발생시킬 수 있음
        completed_details.append(f"오류: {e}")

    return completed_details, total_notified_users

