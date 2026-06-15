"""데이터 소스 공통 인터페이스."""

from __future__ import annotations

from abc import ABC, abstractmethod

from lib.models import PipelineRun


class DataSource(ABC):
    """모든 소스(Glue/Airflow/Airbyte/Sample)가 구현하는 계약.

    화면은 이 인터페이스에만 의존한다. fetch_runs() 가 정규화된
    PipelineRun 목록만 돌려주면, 내부 연결 방식은 화면과 무관하다.
    """

    name: str = "base"

    @abstractmethod
    def fetch_runs(self) -> list[PipelineRun]:
        """이 소스의 최근 실행 이력을 PipelineRun 목록으로 반환한다."""
        raise NotImplementedError
