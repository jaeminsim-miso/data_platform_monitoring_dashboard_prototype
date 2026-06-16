"""Airflow on EKS DAG Run 소스 — Stable REST API(/api/v1) + basic_auth.

UI 인증은 Google OAuth지만, API는 basic_auth(전용 Viewer 로컬 계정)로 접근한다.
webserver는 사설망이라 로컬은 포트포워딩:
    kubectl -n airflow port-forward svc/airflow-webserver 8080:8080
읽기 전용. 호출량은 호출부(st.cache_data)에서 억제.

지표:
  - 완료(success/failed): start_date 가 기간 내인 DAG Run
  - Running/Queued: 현재 시점 전체(기간 무관)
  - Active DAGs: GET /dags?paused=false&only_active=true 의 total_entries
상태 매핑(success/failed/running/queued)은 models._STATUS_MAP[AIRFLOW].
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from lib.datasource.base import DataSource
from lib.models import PipelineRun, Source, normalize_status

_PAGE = 100  # API page_limit 최대값


def _parse(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


class AirflowDataSource(DataSource):
    """Airflow REST API 로 DAG Run / Active DAG 수를 읽는다."""

    name = "airflow"

    def __init__(self, config: dict | None = None) -> None:
        cfg = config or {}
        self._base = cfg.get("base_url", "http://localhost:8080").rstrip("/")
        self._user = cfg.get("username", "")
        self._password = cfg.get("password", "")
        self._lookback_days = int(cfg.get("lookback_days", 7))
        self._timeout = int(cfg.get("timeout", 10))

    def fetch(self) -> tuple[list[PipelineRun], int]:
        """(DAG Run 목록, Active DAGs 수). 세션 1개로 호출."""
        import requests

        sess = requests.Session()
        sess.auth = (self._user, self._password)
        cutoff = datetime.now(timezone.utc) - timedelta(days=self._lookback_days)

        runs: list[PipelineRun] = []
        # 완료: 기간 내 start_date (success/failed)
        runs += self._list_runs(
            sess,
            {"states": ["success", "failed"], "start_date_gte": cutoff.isoformat()},
        )
        # 현재 진행/대기: 기간 무관 (running/queued)
        runs += self._list_runs(sess, {"states": ["running", "queued"]})
        return runs, self._active_dags(sess)

    def fetch_runs(self) -> list[PipelineRun]:
        return self.fetch()[0]

    def _list_runs(self, sess, body: dict[str, Any]) -> list[PipelineRun]:
        out: list[PipelineRun] = []
        offset = 0
        while True:
            payload = {**body, "page_limit": _PAGE, "page_offset": offset}
            resp = sess.post(
                f"{self._base}/api/v1/dags/~/dagRuns/list",
                json=payload,
                timeout=self._timeout,
            )
            resp.raise_for_status()
            data = resp.json()
            dag_runs = data.get("dag_runs", [])
            out.extend(self._to_run(dr) for dr in dag_runs)
            offset += len(dag_runs)
            if not dag_runs or offset >= data.get("total_entries", 0):
                return out

    def _active_dags(self, sess) -> int:
        resp = sess.get(
            f"{self._base}/api/v1/dags",
            params={"paused": "false", "only_active": "true", "limit": 1},
            timeout=self._timeout,
        )
        resp.raise_for_status()
        return int(resp.json().get("total_entries", 0))

    def _to_run(self, dr: dict[str, Any]) -> PipelineRun:
        started = _parse(dr.get("start_date"))
        return PipelineRun(
            source=Source.AIRFLOW,
            pipeline_name=dr.get("dag_id", ""),
            run_id=dr.get("dag_run_id", ""),
            status=normalize_status(Source.AIRFLOW, dr.get("state")),
            started_at=started,
            ended_at=_parse(dr.get("end_date")),
            last_run_at=started,
            message=None,
            extra={"run_type": dr.get("run_type")},
        )
