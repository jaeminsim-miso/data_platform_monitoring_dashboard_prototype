"""Data Platform 통합 모니터링 — 단일 화면 요약 (프로토타입).

상단 기간 선택 → 전체 합산(Pipeline Utilization) → 소스별 Runs Summary.

데이터: Glue(boto3) · Airflow(REST API) · Airbyte(메타DB) 모두 실연결.
각 소스는 연결 실패 시 해당 소스만 샘플로 폴백하고 경고를 띄운다(화면이 깨지지 않게).
"""

from __future__ import annotations

from datetime import timedelta

import streamlit as st

from lib.components import render_dashboard_styles, render_source_summary
from lib.datasource.airbyte import AirbyteDataSource
from lib.datasource.airflow import AirflowDataSource
from lib.datasource.glue import GlueDataSource
from lib.datasource.sample import SampleDataSource
from lib.models import PipelineRun, RunStatus, Source
from lib.summary import overall_summary, summarize

st.set_page_config(page_title="Data Platform 모니터링", page_icon="📊", layout="wide")
render_dashboard_styles()

# 기간 선택지 (일). 샘플 생성 구간(7일) 및 실연결 lookback 과 맞춘다.
_RANGES = {"1 Day": 1, "3 Days": 3, "7 Days": 7}


def _secrets(section: str) -> dict:
    try:
        return dict(st.secrets.get(section, {}))
    except Exception:
        return {}


def _glue_config() -> dict:
    cfg = _secrets("glue")
    cfg.setdefault("profile", "miso")
    return cfg


def _airbyte_config() -> dict:
    cfg = _secrets("airbyte")
    cfg.setdefault("host", "localhost")
    cfg.setdefault("port", 5432)
    cfg.setdefault("dbname", "db-airbyte")
    cfg.setdefault("user", "airbyte")
    return cfg


def _airflow_config() -> dict:
    cfg = _secrets("airflow")
    cfg.setdefault("base_url", "http://localhost:8080")
    return cfg


@st.cache_data(ttl=60, show_spinner="실데이터를 불러오는 중…")
def load_runs(
    lookback_days: int,
) -> tuple[
    list[PipelineRun],
    dict[Source, int],
    dict[Source, str],
    dict[Source, tuple[str, str]],
]:
    """선택 기간만큼 실행 이력을 불러온다.

    반환: (runs, tails, errors, notes)
      - tails:  소스별 tail 실측값(Enabled Connections / Active DAGs)
      - errors: 소스별 연결 실패 메시지(캐시 밖에서 경고 렌더)
      - notes:  소스별 카드 안내 (라벨, 툴팁) — 예: 제외된 Glue 잡
    소스별로 실패 시 그 소스만 샘플로 폴백한다.
    """
    sample = SampleDataSource().fetch_runs()
    runs: list[PipelineRun] = []
    tails: dict[Source, int] = {}
    errors: dict[Source, str] = {}
    notes: dict[Source, tuple[str, str]] = {}

    def fallback(src: Source, exc: Exception) -> None:
        errors[src] = f"{type(exc).__name__}: {exc}"
        runs.extend(r for r in sample if r.source == src)

    # AWS Glue (boto3 실연결)
    try:
        cfg = _glue_config()
        cfg["lookback_days"] = lookback_days
        glue = GlueDataSource(cfg)
        runs.extend(glue.fetch_runs())
        if glue.excluded_jobs:
            notes[Source.GLUE] = (
                f"제외된 잡 {len(glue.excluded_jobs)}개",
                "지표에서 제외: " + ", ".join(glue.excluded_jobs),
            )
    except Exception as exc:
        fallback(Source.GLUE, exc)

    # Airbyte (메타DB 실연결)
    try:
        cfg = _airbyte_config()
        cfg["lookback_days"] = lookback_days
        ab_runs, enabled = AirbyteDataSource(cfg).fetch()
        runs.extend(ab_runs)
        tails[Source.AIRBYTE] = enabled
    except Exception as exc:
        fallback(Source.AIRBYTE, exc)

    # Airflow (REST API + basic_auth 실연결)
    try:
        cfg = _airflow_config()
        cfg["lookback_days"] = lookback_days
        af_runs, active = AirflowDataSource(cfg).fetch()
        runs.extend(af_runs)
        tails[Source.AIRFLOW] = active
    except Exception as exc:
        fallback(Source.AIRFLOW, exc)
    return runs, tails, errors, notes


st.title("Data Platform Pipeline Monitoring Dashboard")
st.caption("AWS Glue · Airflow · Airbyte 통합 현황 — 프로토타입")

picker, _ = st.columns([1, 4])
with picker:
    range_label = st.selectbox("Start date range", list(_RANGES), index=0)
lookback = _RANGES[range_label]

runs, tails, errors, notes = load_runs(lookback)
for src, msg in errors.items():
    st.warning(f"{src.value} 실데이터 연결 실패 — 샘플로 대체했습니다. ({msg})")


# 기간 필터: running/queued(=현재 진행/대기)는 기간 무관 포함, 완료는 시작시각 기준
anchor = max((r.started_at for r in runs if r.started_at), default=None)
cutoff = anchor - timedelta(days=lookback) if anchor else None


def _in_window(r: PipelineRun) -> bool:
    if r.status in (RunStatus.RUNNING, RunStatus.PENDING):
        return True
    return cutoff is None or (r.started_at is not None and r.started_at >= cutoff)


windowed = [r for r in runs if _in_window(r)]

render_source_summary(overall_summary(windowed))
for source in Source:
    src_runs = [r for r in windowed if r.source == source]
    summary = summarize(source, src_runs, tail_count=tails.get(source))
    if source in notes:
        summary.note, summary.note_detail = notes[source]
    render_source_summary(summary)
