# test_email.py
import sqlite3
import os
import sys
import re

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from services.notification_service import send_email

DATABASE = 'webtoons.db'

def is_valid_email(email):
    """서버 단에서 이메일 형식의 유효성을 검증합니다."""
    return re.match(r"^[^\s@]+@[^\s@]+\.[^\s@]+$", email)

def run_test():
    """
    이메일 발송 기능 자체를 직접 테스트합니다.
    """
    TEST_CONTENT_ID = '747269' # '전지적 독자 시점'
    TEST_SOURCE = 'naver_webtoon'
    TEST_RECIPIENT_EMAIL = "jules.testing.bot@gmail.com"

    if not is_valid_email(TEST_RECIPIENT_EMAIL):
        print("❌ [설정 필요] test_email.py 파일의 TEST_RECIPIENT_EMAIL 변수를 실제 이메일 주소로 변경해주세요.")
        return

    print("=== 이메일 발송 기능 테스트 시작 ===")

    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    cursor.execute("SELECT title FROM contents WHERE content_id = ? AND source = ?", (TEST_CONTENT_ID, TEST_SOURCE))
    result = cursor.fetchone()
    conn.close()

    if not result:
        print(f"❌ 오류: DB에서 콘텐츠 ID {TEST_CONTENT_ID} ({TEST_SOURCE})를 찾을 수 없습니다.")
        print("   'python3 crawlers/naver_webtoon_crawler.py'를 실행하여 DB에 데이터가 올바르게 수집되었는지 확인해주세요.")
        return

    content_title = result[0]
    print(f"테스트 대상 콘텐츠: '{content_title}' (ID: {TEST_CONTENT_ID})")
    print(f"테스트 메일 수신 주소: {TEST_RECIPIENT_EMAIL}")

    subject = f"[테스트] 콘텐츠 완결 알림: '{content_title}'가 완결되었습니다!"
    body = f"""안녕하세요! Ending Signal입니다.

이것은 이메일 발송 기능 테스트를 위해 전송된 메일입니다.
회원님께서 구독하신 콘텐츠 '{content_title}'가 완결될 경우, 이와 같은 형식으로 메일이 발송됩니다.

감사합니다.
"""

    print("\n이메일 발송을 시도합니다...")
    send_email(TEST_RECIPIENT_EMAIL, subject, body)

    print("=== 테스트 종료 ===")


if __name__ == '__main__':
    if not os.getenv('EMAIL_ADDRESS') or not os.getenv('EMAIL_PASSWORD'):
        print("="*60)
        print("❌ [오류] 이메일 발송을 위한 환경 변수가 설정되지 않았습니다.")
        print("   테스트를 진행하려면 터미널에 아래 명령어를 먼저 입력해주세요.")
        print("\n   export EMAIL_ADDRESS='당신의G메일주소'")
        print("   export EMAIL_PASSWORD='당신의16자리앱비밀번호'\n")
        print("="*60)
    else:
        run_test()
