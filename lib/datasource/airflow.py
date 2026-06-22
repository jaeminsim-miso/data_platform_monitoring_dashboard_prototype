"""Airflow on EKS DAG Run 소스 — Stable REST API(/api/v1) + basic_auth.

UI 인증은 Google OAuth지만, API는 basic_auth(전용 Viewer 로컬 계정)로 접근한다.
webserver는 사설망이라 로컬은 포트포워딩:
    kubectl -n airflow port-forward svc/airflow-webserver 8080:8080
읽기 전용. 호출량은 호출부(st.cache_data)에서 억제.

지표:
  - 완료(success/failed): start_date 가 기간 내인 DAG Run
  - Running/Queued: 현재 시점 전체(기간 무관)
  - Active DAGs: 표시 대상 DAG 중 paused 아닌 것의 수
상태 매핑(success/failed/running/queued)은 models._STATUS_MAP[AIRFLOW].

표시 대상은 태그(기본 layer:lint/lext/stdz)로 제한한다 — Glue 의 DataLayer 필터와
대칭. 태그는 DAG 에만 붙고 DAG Run 엔 없으므로 2단계로 푼다: GET /dags 의 `tags`
파라미터(OR 매칭)로 태그→dag_id 를 먼저 해석한 뒤, 그 dag_ids 로 Run 을 조회한다.
허용값은 config(tags)로 바꿀 수 있고, 빈 값이면 필터 없이 전체 DAG 를 본다.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from lib.datasource.base import DataSource
from lib.models import PipelineRun, Source, normalize_status

_PAGE = 100  # API page_limit 최대값

# 표시 대상 DAG 를 거르는 태그(OR 매칭). layer:lint/lext/stdz(Glue DataLayer 와 대칭)
# + dbt(dbt 기반 변환 DAG). 빈 값으로 두면 필터 해제(전체 DAG).
_DEFAULT_TAGS = ["layer:lint", "layer:lext", "layer:stdz", "dbt"]


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
        # 태그 필터(OR). 미설정이면 기본값, 빈 리스트/None 이면 필터 해제(전체 DAG).
        tags = cfg.get("tags", _DEFAULT_TAGS)
        self._tags = list(tags) if tags else None

    def fetch(self) -> tuple[list[PipelineRun], int]:
        """(DAG Run 목록, Active DAGs 수). 세션 1개로 호출.

        tags 가 설정돼 있으면 그 태그(OR)를 가진 DAG 로만 스코핑한다. Run 객체엔
        태그가 없어, GET /dags 로 태그→dag_id 를 해석한 뒤 dag_ids 로 Run 을 거른다.
        Active DAGs 수는 같은 결과에서 paused 아닌 것만 센다. tags 가 비면 전체 DAG.
        """
        import requests

        sess = requests.Session()
        sess.auth = (self._user, self._password)
        cutoff = datetime.now(timezone.utc) - timedelta(days=self._lookback_days)

        scope: dict[str, Any] = {}
        if self._tags:
            tagged = self._tagged_dags(sess)
            dag_ids = [d["dag_id"] for d in tagged]
            active = sum(1 for d in tagged if not d["is_paused"])
            if not dag_ids:  # 태그에 해당하는 DAG 가 없으면 조회할 Run 도 없음
                return [], 0
            scope["dag_ids"] = dag_ids
        else:
            active = self._active_dags(sess)

        runs: list[PipelineRun] = []
        # 완료: 기간 내 start_date (success/failed)
        runs += self._list_runs(
            sess,
            {**scope, "states": ["success", "failed"], "start_date_gte": cutoff.isoformat()},
        )
        # 현재 진행/대기: 기간 무관 (running/queued)
        runs += self._list_runs(sess, {**scope, "states": ["running", "queued"]})
        return runs, active

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

    def _tagged_dags(self, sess) -> list[dict[str, Any]]:
        """tags(OR 매칭) 에 해당하는 DAG 의 {dag_id, is_paused} 목록.

        Airflow GET /dags 의 `tags` 파라미터는 OR(주어진 태그 중 하나라도 보유)다.
        layer:lint|lext|stdz 처럼 같은 네임스페이스 값들의 합집합엔 정확히 맞는다.
        only_active=true 로 삭제/미파싱 DAG 는 제외한다. (requests 가 list 를
        tags=a&tags=b 로 직렬화하고, 콜론 등 특수문자는 쿼리 인코딩 처리한다.)
        """
        out: list[dict[str, Any]] = []
        offset = 0
        while True:
            resp = sess.get(
                f"{self._base}/api/v1/dags",
                params={
                    "tags": self._tags,
                    "only_active": "true",
                    "limit": _PAGE,
                    "offset": offset,
                },
                timeout=self._timeout,
            )
            resp.raise_for_status()
            data = resp.json()
            dags = data.get("dags", [])
            out.extend(
                {"dag_id": d.get("dag_id", ""), "is_paused": bool(d.get("is_paused"))}
                for d in dags
            )
            offset += len(dags)
            if not dags or offset >= data.get("total_entries", 0):
                return out

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
