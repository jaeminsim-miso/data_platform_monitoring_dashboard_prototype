"""Airflow on EKS DAG Run 소스 (스텁 — 접근 권한 확보 후 구현).

예상 구현 흐름 (Airflow REST API v1):
    import requests
    auth = (cfg["username"], cfg["password"])
    dags = requests.get(f"{base}/api/v1/dags", auth=auth).json()["dags"]
    for d in dags:
        runs = requests.get(
            f"{base}/api/v1/dags/{d['dag_id']}/dagRuns",
            params={"order_by": "-execution_date", "limit": N}, auth=auth,
        ).json()["dag_runs"]
        for r in runs:
            PipelineRun(
                source=Source.AIRFLOW,
                pipeline_name=d["dag_id"],
                run_id=r["dag_run_id"],
                status=normalize_status(Source.AIRFLOW, r["state"]),
                started_at=r.get("start_date"),
                ended_at=r.get("end_date"),
            )

접속정보: .streamlit/secrets.toml 의 [airflow] 섹션 (base_url/username/password).
EKS 내부 엔드포인트면 포트포워딩 또는 사내망 접근이 필요할 수 있다.
운영 보호: 읽기 전용 권한, st.cache_data(ttl) 로 호출 억제.
"""

from __future__ import annotations

from lib.datasource.base import DataSource
from lib.models import PipelineRun


class AirflowDataSource(DataSource):
    name = "airflow"

    def __init__(self, config: dict | None = None) -> None:
        self._config = config or {}

    def fetch_runs(self) -> list[PipelineRun]:
        raise NotImplementedError(
            "AirflowDataSource 는 접근 권한 확보 후 구현 예정입니다. "
            "현재 프로토타입은 SampleDataSource 로 동작합니다."
        )
