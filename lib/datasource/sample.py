"""샘플 데이터 소스 — 외부 연결 없이 화면을 동작시킨다.

세 소스(Glue/Airflow/Airbyte)의 형태·규모를 흉내 낸 합성 실행 이력을 생성한다.
실행은 최근 _WINDOW_DAYS 일 구간에 '일 평균 cadence' 로 흩뿌려, 상단의 기간
선택(Start date range)으로 필터하면 숫자가 자연스럽게 줄고 늘도록 한다.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone

from lib.datasource.base import DataSource
from lib.models import PipelineRun, RunStatus, Source

_WINDOW_DAYS = 7  # 생성 구간 = 선택 가능한 최대 기간

# 소스별 (파이프라인 개수, 파이프라인당 일 평균 실행 수)
_SCALE: dict[Source, tuple[int, float]] = {
    Source.GLUE: (65, 3.0),
    Source.AIRFLOW: (127, 6.0),
    Source.AIRBYTE: (84, 4.0),
}

_DOMAINS = [
    "orders",
    "payments",
    "customer",
    "marketing",
    "inventory",
    "events",
    "finance",
    "ml",
]
_AIRBYTE_SRC = ["mysql_prod", "postgres_orders", "salesforce", "stripe", "ga4"]
_AIRBYTE_DST = ["s3_raw", "snowflake", "bigquery", "redshift"]

_FAIL_MESSAGES = [
    "OutOfMemoryError on executor",
    "Connection timeout to source",
    "Schema mismatch detected",
    "Upstream dependency failed",
    "Credentials expired",
]


def _pipeline_name(source: Source, i: int) -> str:
    """소스별 형태를 흉내 낸 고유 파이프라인 이름 (index 로 유일성 보장)."""
    if source is Source.GLUE:
        return f"glue-{_DOMAINS[i % len(_DOMAINS)]}-{i:02d}"
    if source is Source.AIRFLOW:
        return f"dag_{_DOMAINS[i % len(_DOMAINS)]}_{i:02d}"
    src = _AIRBYTE_SRC[i % len(_AIRBYTE_SRC)]
    dst = _AIRBYTE_DST[i % len(_AIRBYTE_DST)]
    return f"{src} → {dst} #{i:02d}"


class SampleDataSource(DataSource):
    """결정적(seed 고정) 합성 데이터를 생성하는 소스."""

    name = "sample"

    def __init__(self, seed: int = 42, now: datetime | None = None) -> None:
        self._rng = random.Random(seed)
        self._now = now or datetime.now(timezone.utc)  # Glue 등 실데이터와 tz 정합
        self._window = timedelta(days=_WINDOW_DAYS)

    def fetch_runs(self) -> list[PipelineRun]:
        runs: list[PipelineRun] = []
        for source, (n_pipelines, cadence) in _SCALE.items():
            per_pipeline = max(1, round(cadence * _WINDOW_DAYS))
            for i in range(n_pipelines):
                name = _pipeline_name(source, i)
                runs += self._gen_pipeline_runs(source, name, per_pipeline)
        return runs

    # --- 내부 생성기 -------------------------------------------------------

    def _gen_pipeline_runs(
        self, source: Source, name: str, n: int
    ) -> list[PipelineRun]:
        runs: list[PipelineRun] = []
        for j in range(n):
            # 0(최근)~1(과거)로 구간에 흩뿌림 + jitter
            frac = (j + self._rng.random()) / n
            started = self._now - self._window * frac
            status = self._pick_status(source, is_latest=(j == 0))
            if status == RunStatus.RUNNING:
                ended, duration_s = None, 0
            else:
                duration_s = self._rng.randint(30, 1800)
                ended = started + timedelta(seconds=duration_s)
            runs.append(
                PipelineRun(
                    source=source,
                    pipeline_name=name,
                    run_id=f"{source.value.lower()}-{name}-{j}",
                    status=status,
                    started_at=started,
                    ended_at=ended,
                    last_run_at=started,
                    message=self._message(status),
                    extra=self._extra(source, status, duration_s),
                )
            )
        return runs

    def _pick_status(self, source: Source, is_latest: bool) -> RunStatus:
        roll = self._rng.random()
        if is_latest and roll < 0.13:  # 진행중은 가장 최근 실행에만 (전체 ~36건)
            return RunStatus.RUNNING
        if roll < 0.95:
            return RunStatus.SUCCESS
        if roll < 0.985:
            return RunStatus.FAILED
        # 나머지: 소스별 중간 상태 (Glue=취소, Airflow=queued, Airbyte=scheduled)
        return RunStatus.CANCELLED if source is Source.GLUE else RunStatus.PENDING

    def _message(self, status: RunStatus) -> str | None:
        return self._rng.choice(_FAIL_MESSAGES) if status == RunStatus.FAILED else None

    def _extra(self, source: Source, status: RunStatus, duration_s: int) -> dict:
        if source is Source.GLUE:
            dpu = self._rng.choice([2, 5, 10])
            return {
                "dpu": dpu,
                "worker_type": "G.1X",
                "dpu_hours": dpu * duration_s / 3600,  # 할당 DPU × 실행시간[h]
            }
        if source is Source.AIRFLOW:
            return {"tasks": self._rng.randint(3, 20)}
        records = 0 if status == RunStatus.FAILED else self._rng.randint(1_000, 500_000)
        return {"records_synced": records, "streams": self._rng.randint(1, 8)}
