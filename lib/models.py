"""정규화된 파이프라인 실행 모델.

Glue · Airflow · Airbyte 는 상태 표현이 제각각이다. 이 모듈은 그 차이를
하나의 `PipelineRun` 으로 흡수해 화면이 단일 모델만 다루도록 한다.
(stdlib 만 의존 — pandas/streamlit 등 UI 의존성을 넣지 않는다.)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class Source(str, Enum):
    """데이터 소스 종류."""

    GLUE = "Glue"
    AIRFLOW = "Airflow"
    AIRBYTE = "Airbyte"


class RunStatus(str, Enum):
    """정규화된 실행 상태. 시스템별 원시 상태를 이 값으로 매핑한다."""

    SUCCESS = "Success"
    FAILED = "Failed"
    RUNNING = "Running"
    CANCELLED = "Cancelled"
    PENDING = "Pending"
    UNKNOWN = "Unknown"


# 시스템별 원시 상태 문자열 → 정규화 상태.
# 실제 소스 연결 시 각 API/DB가 돌려주는 상태값을 여기서 한 곳에 모아 매핑한다.
_STATUS_MAP: dict[Source, dict[str, RunStatus]] = {
    Source.GLUE: {
        "SUCCEEDED": RunStatus.SUCCESS,
        "FAILED": RunStatus.FAILED,
        "ERROR": RunStatus.FAILED,
        "TIMEOUT": RunStatus.FAILED,
        "RUNNING": RunStatus.RUNNING,
        "STARTING": RunStatus.RUNNING,
        "STOPPING": RunStatus.RUNNING,
        "STOPPED": RunStatus.CANCELLED,
        "WAITING": RunStatus.PENDING,
    },
    Source.AIRFLOW: {
        "success": RunStatus.SUCCESS,
        "failed": RunStatus.FAILED,
        "upstream_failed": RunStatus.FAILED,
        "running": RunStatus.RUNNING,
        "up_for_retry": RunStatus.RUNNING,
        "queued": RunStatus.PENDING,
        "scheduled": RunStatus.PENDING,
    },
    Source.AIRBYTE: {
        "succeeded": RunStatus.SUCCESS,
        "failed": RunStatus.FAILED,
        "incomplete": RunStatus.FAILED,
        "running": RunStatus.RUNNING,
        "pending": RunStatus.PENDING,
        "cancelled": RunStatus.CANCELLED,
    },
}


def normalize_status(source: Source, raw_status: str | None) -> RunStatus:
    """시스템별 원시 상태 문자열을 정규화된 RunStatus 로 변환한다.

    매핑에 없는 값은 UNKNOWN 으로 떨어뜨린다 (조용히 성공으로 처리하지 않는다).
    """
    if not raw_status:
        return RunStatus.UNKNOWN
    return _STATUS_MAP.get(source, {}).get(raw_status, RunStatus.UNKNOWN)


@dataclass
class PipelineRun:
    """소스를 막론하고 화면이 다루는 단일 실행 단위."""

    source: Source
    pipeline_name: str  # Job명 / DAG id / Connection명
    run_id: str
    status: RunStatus
    started_at: datetime | None = None
    ended_at: datetime | None = None
    last_run_at: datetime | None = None
    message: str | None = None  # 실패 사유 등
    extra: dict[str, Any] = field(default_factory=dict)  # 시스템별 부가정보

    @property
    def duration_seconds(self) -> float | None:
        """완료된 실행의 소요시간(초). 미완료면 None."""
        if self.started_at and self.ended_at:
            return (self.ended_at - self.started_at).total_seconds()
        return None
