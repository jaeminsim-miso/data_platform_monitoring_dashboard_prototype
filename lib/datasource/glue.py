"""AWS Glue Job Run 소스 — boto3 Glue API(get_job_runs) 직접 조회.

CloudWatch가 아니라 Glue API 가 잡 실행 상태의 정본이다. 읽기 전용 권한만 사용하고
(get_job_runs/list_jobs), 인증은 AWS SSO 프로파일(기본 'miso')로 한다. 호출량은
호출부(app.py 의 st.cache_data)에서 억제한다.

필요 IAM: glue:ListJobs, glue:GetJobRuns (+ 선택적으로 glue:GetJobRun)

DPU hours 는 '할당 DPU 기준'으로 계산한다 (오토스케일/Flex 미사용 전제):
    run별 dpu_hours = 할당 DPU × ExecutionTime[h]
    할당 DPU = NumberOfWorkers × WorkerType DPU  (없으면 MaxCapacity)
매핑 로직(_allocated_dpu / _to_pipeline_run)은 boto3 없이도 검증 가능한 순수 함수다.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any

from lib.datasource.base import DataSource
from lib.models import PipelineRun, Source, normalize_status

# WorkerType → 워커당 DPU (할당 DPU 계산용)
_WORKER_DPU = {
    "G.025X": 0.25,
    "G.1X": 1.0,
    "G.2X": 2.0,
    "G.4X": 4.0,
    "G.8X": 8.0,
    "Standard": 1.0,
}

_MAX_RESULTS = 200  # 페이지당 Run 수


def _allocated_dpu(run: dict[str, Any]) -> float:
    """run에 할당된 DPU. 워커 기반(Glue 2.0+)이 없으면 MaxCapacity 사용."""
    workers = run.get("NumberOfWorkers")
    worker_type = run.get("WorkerType")
    if workers and worker_type:
        return float(workers) * _WORKER_DPU.get(worker_type, 1.0)
    cap = run.get("MaxCapacity") or run.get("AllocatedCapacity") or 0.0
    return float(cap)


def _to_pipeline_run(job_name: str, run: dict[str, Any]) -> PipelineRun:
    """Glue JobRun(dict) → 정규화된 PipelineRun. (순수 함수)"""
    dpu = _allocated_dpu(run)
    exec_seconds = run.get("ExecutionTime") or 0
    return PipelineRun(
        source=Source.GLUE,
        pipeline_name=job_name,
        run_id=run.get("Id", ""),
        status=normalize_status(Source.GLUE, run.get("JobRunState")),
        started_at=run.get("StartedOn"),
        ended_at=run.get("CompletedOn"),
        last_run_at=run.get("StartedOn"),
        message=run.get("ErrorMessage"),
        extra={
            "dpu": dpu,
            "execution_seconds": exec_seconds,
            "dpu_hours": dpu * exec_seconds / 3600,
            "worker_type": run.get("WorkerType"),
        },
    )


class GlueDataSource(DataSource):
    """boto3 Glue API 로 Job Run 을 읽어 PipelineRun 목록으로 반환한다."""

    name = "glue"

    def __init__(self, config: dict | None = None) -> None:
        cfg = config or {}
        self._profile = cfg.get("profile", "miso")
        self._region = cfg.get("region")  # None 이면 프로파일 기본 리전
        self._lookback_days = int(cfg.get("lookback_days", 7))
        self._max_runs_per_job = int(cfg.get("max_runs_per_job", 1000))
        self._max_workers = int(cfg.get("max_workers", 16))  # 잡별 병렬 조회 수
        # None=전체 잡, list=지정 잡만 (초기 검증 시 1~2개로 제한 가능)
        self._job_names = cfg.get("job_names")

    def fetch_runs(self) -> list[PipelineRun]:
        import boto3  # 선택 의존성 — 실제 조회 시점에만 필요

        session = boto3.Session(profile_name=self._profile, region_name=self._region)
        # botocore low-level client 는 스레드 안전 → 스레드 간 공유 OK
        client = session.client("glue")
        cutoff = datetime.now(timezone.utc) - timedelta(days=self._lookback_days)
        job_names = self._job_names or self._list_job_names(client)

        # 잡별 get_job_runs 를 병렬로 — 네트워크 왕복이 지배적이라 가장 큰 속도 개선
        runs: list[PipelineRun] = []
        with ThreadPoolExecutor(max_workers=self._max_workers) as pool:
            for job_runs in pool.map(
                lambda name: self._job_runs(client, name, cutoff), job_names
            ):
                runs.extend(job_runs)
        return runs

    def _list_job_names(self, client) -> list[str]:
        names: list[str] = []
        token: str | None = None
        while True:
            kwargs: dict[str, Any] = {"MaxResults": _MAX_RESULTS}
            if token:
                kwargs["NextToken"] = token
            resp = client.list_jobs(**kwargs)
            names.extend(resp.get("JobNames", []))
            token = resp.get("NextToken")
            if not token:
                return names

    def _job_runs(self, client, job_name: str, cutoff: datetime) -> list[PipelineRun]:
        out: list[PipelineRun] = []
        token: str | None = None
        while True:
            kwargs: dict[str, Any] = {"JobName": job_name, "MaxResults": _MAX_RESULTS}
            if token:
                kwargs["NextToken"] = token
            resp = client.get_job_runs(**kwargs)
            for run in resp.get("JobRuns", []):
                started = run.get("StartedOn")
                # JobRun 은 최신순 → cutoff 이전을 만나면 이 잡은 더 볼 필요 없음
                if started and started < cutoff:
                    return out
                out.append(_to_pipeline_run(job_name, run))
                if len(out) >= self._max_runs_per_job:
                    return out
            token = resp.get("NextToken")
            if not token:
                return out
