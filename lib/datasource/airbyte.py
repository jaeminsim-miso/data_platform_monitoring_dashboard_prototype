"""Airbyte on EKS Connection/Sync 소스 (스텁 — 접근 권한 확보 후 구현).

예상 구현 흐름 (Airbyte API):
    import requests
    conns = requests.post(f"{base}/connections/list",
                          json={"workspaceId": ws}, auth=auth).json()["connections"]
    for c in conns:
        jobs = requests.post(f"{base}/jobs/list",
                             json={"configId": c["connectionId"], "configTypes": ["sync"]},
                             auth=auth).json()["jobs"]
        for j in jobs:
            attempt = j["attempts"][-1] if j.get("attempts") else {}
            PipelineRun(
                source=Source.AIRBYTE,
                pipeline_name=c["name"],
                run_id=str(j["job"]["id"]),
                status=normalize_status(Source.AIRBYTE, j["job"]["status"]),
                started_at=...,  # epoch → datetime 변환
                ended_at=...,
                extra={"records_synced": attempt.get("recordsSynced"),
                       "streams": len(c.get("syncCatalog", {}).get("streams", []))},
            )

접속정보: .streamlit/secrets.toml 의 [airbyte] 섹션 (base_url + 인증).
운영 보호: 읽기 전용, st.cache_data(ttl) 로 호출 억제.
"""

from __future__ import annotations

from lib.datasource.base import DataSource
from lib.models import PipelineRun


class AirbyteDataSource(DataSource):
    name = "airbyte"

    def __init__(self, config: dict | None = None) -> None:
        self._config = config or {}

    def fetch_runs(self) -> list[PipelineRun]:
        raise NotImplementedError(
            "AirbyteDataSource 는 접근 권한 확보 후 구현 예정입니다. "
            "현재 프로토타입은 SampleDataSource 로 동작합니다."
        )
