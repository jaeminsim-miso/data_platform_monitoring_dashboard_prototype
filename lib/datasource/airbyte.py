"""Airbyte Connection/Stream Sync 소스 — 인클러스터 Postgres 메타DB 직접 조회.

Airbyte OSS(self-hosted)의 메타DB(db-airbyte)에서 sync 작업/커넥션을 읽어 카운트한다.
RDS가 아니라 인클러스터 Postgres(airbyte-db-svc:5432)라, 로컬은 포트포워딩으로 접근:
    kubectl -n airbyte port-forward svc/airbyte-db-svc 5432:5432
읽기 전용 SELECT 만 사용. 호출량은 호출부(app.py 의 st.cache_data)에서 억제.

지표:
  - Total/Running/Scheduled/Successful/Failed: jobs(config_type='sync') 상태 집계.
    완료(succeeded/failed/…)는 기간 내(created_at), running/pending(=Scheduled)은 현재 전체.
  - Enabled Connections: connection.status='active' 개수 (현재 상태, 기간 무관).
상태 매핑(succeeded/failed/incomplete/running/pending/cancelled)은 models._STATUS_MAP[AIRBYTE].
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from lib.datasource.base import DataSource
from lib.models import PipelineRun, Source, normalize_status

# 종료 상태(updated_at 을 종료시각으로 사용). running/pending 은 진행중이라 제외.
_TERMINAL = {"succeeded", "failed", "incomplete", "cancelled"}


def _utc(dt: datetime | None) -> datetime | None:
    """naive datetime 이면 UTC 로 간주 (다른 소스와 tz 정합)."""
    if dt is not None and dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


class AirbyteDataSource(DataSource):
    """Airbyte 메타DB(Postgres)에서 sync 실행/커넥션을 읽는다."""

    name = "airbyte"

    def __init__(self, config: dict | None = None) -> None:
        cfg = config or {}
        self._host = cfg.get("host", "localhost")
        self._port = int(cfg.get("port", 5432))
        self._dbname = cfg.get("dbname", "db-airbyte")
        self._user = cfg.get("user", "airbyte")
        self._password = cfg.get("password", "")
        self._lookback_days = int(cfg.get("lookback_days", 7))

    def fetch(self) -> tuple[list[PipelineRun], int]:
        """(sync 실행 목록, Enabled Connections 수). 연결 1회로 두 쿼리."""
        import psycopg2  # 선택 의존성 — 실제 조회 시점에만 필요

        conn = psycopg2.connect(
            host=self._host,
            port=self._port,
            dbname=self._dbname,
            user=self._user,
            password=self._password,
            connect_timeout=8,
        )
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT j.id, j.scope, j.status::text, j.created_at, j.updated_at, c.name
                    FROM jobs j
                    LEFT JOIN connection c ON j.scope = c.id::text
                    WHERE j.config_type = 'sync'
                      AND (j.created_at >= now() - make_interval(days => %s)
                           OR j.status IN ('running', 'pending'))
                    """,
                    (self._lookback_days,),
                )
                runs = [self._row_to_run(r) for r in cur.fetchall()]
                cur.execute("SELECT count(*) FROM connection WHERE status = 'active'")
                enabled = int(cur.fetchone()[0])
        finally:
            conn.close()
        return runs, enabled

    def fetch_runs(self) -> list[PipelineRun]:
        return self.fetch()[0]

    def _row_to_run(self, row: tuple[Any, ...]) -> PipelineRun:
        job_id, scope, status, created_at, updated_at, name = row
        ended = _utc(updated_at) if status in _TERMINAL else None
        return PipelineRun(
            source=Source.AIRBYTE,
            pipeline_name=name or scope or "unknown",
            run_id=str(job_id),
            status=normalize_status(Source.AIRBYTE, status),
            started_at=_utc(created_at),
            ended_at=ended,
            last_run_at=_utc(created_at),
            message=None,
            extra={"connection_id": scope},
        )
