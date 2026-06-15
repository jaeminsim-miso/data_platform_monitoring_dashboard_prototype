"""AWS Glue Job Run 소스 (스텁 — 접근 권한 확보 후 구현).

예상 구현 흐름:
    import boto3
    client = boto3.client("glue", region_name=cfg["region"])
    for job in client.list_jobs()["JobNames"]:
        for r in client.get_job_runs(JobName=job, MaxResults=N)["JobRuns"]:
            PipelineRun(
                source=Source.GLUE,
                pipeline_name=job,
                run_id=r["Id"],
                status=normalize_status(Source.GLUE, r["JobRunState"]),
                started_at=r.get("StartedOn"),
                ended_at=r.get("CompletedOn"),
                message=r.get("ErrorMessage"),
                extra={"dpu": r.get("MaxCapacity"), "worker_type": r.get("WorkerType")},
            )

접속정보: .streamlit/secrets.toml 의 [glue] 섹션 (secrets.toml.example 참고).
운영 보호: 읽기 전용 IAM 권한, 호출은 st.cache_data(ttl) 로 억제.
"""

from __future__ import annotations

from lib.datasource.base import DataSource
from lib.models import PipelineRun


class GlueDataSource(DataSource):
    name = "glue"

    def __init__(self, config: dict | None = None) -> None:
        self._config = config or {}

    def fetch_runs(self) -> list[PipelineRun]:
        raise NotImplementedError(
            "GlueDataSource 는 접근 권한 확보 후 구현 예정입니다. "
            "현재 프로토타입은 SampleDataSource 로 동작합니다."
        )
