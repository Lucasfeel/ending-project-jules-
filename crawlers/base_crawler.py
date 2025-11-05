# crawlers/base_crawler.py

from abc import ABC, abstractmethod

class ContentCrawler(ABC):
    """
    모든 콘텐츠 크롤러를 위한 추상 기본 클래스입니다.
    각 크롤러는 이 클래스를 상속받아 특정 콘텐츠 소스에 대한
    데이터 수집, 동기화, 점검 로직을 구현해야 합니다.
    """

    def __init__(self, source_name):
        self.source_name = source_name

    @abstractmethod
    async def fetch_all_data(self):
        """
        소스에서 모든 콘텐츠 데이터를 비동기적으로 가져옵니다.
        """
        pass

    @abstractmethod
    def synchronize_database(self, conn, all_content_today, ongoing_today, hiatus_today, finished_today):
        """
        데이터베이스를 최신 상태로 동기화합니다.
        """
        pass

    @abstractmethod
    async def run_daily_check(self, conn):
        """
        일일 데이터 점검 및 완결 알림 프로세스를 실행합니다.
        """
        pass
