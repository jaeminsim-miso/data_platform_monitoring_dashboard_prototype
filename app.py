"""Data Platform 통합 모니터링 — 단일 화면 요약 (프로토타입).

상단 기간 선택 → 전체 합산(Pipeline Utilization) → 소스별 Runs Summary 를
필터 없이 한 화면에 보여준다. 현재는 SampleDataSource(합성 데이터)로 동작한다.
실제 연결 시 load_runs() 안에서 소스 구현만 교체/병합하면 화면 코드는 그대로다.
"""

from __future__ import annotations

from datetime import timedelta

import streamlit as st

from lib.components import render_dashboard_styles, render_source_summary
from lib.datasource.sample import SampleDataSource
from lib.models import PipelineRun, Source
from lib.summary import overall_summary, summarize

st.set_page_config(page_title="Data Platform 모니터링", page_icon="📊", layout="wide")
render_dashboard_styles()

# 기간 선택지 (일). 샘플 데이터 생성 구간(7일) 이내로 둔다.
_RANGES = {"1 Day": 1, "3 Days": 3, "7 Days": 7}


@st.cache_data(ttl=60)
def load_runs() -> list[PipelineRun]:
    """실행 이력을 불러온다 (생성 구간 전체).

    실제 연결 시: GlueDataSource/AirflowDataSource/AirbyteDataSource 를 각각
    fetch_runs() 한 뒤 합쳐서 반환하면 된다. (화면/요약 코드는 그대로)
    """
    return SampleDataSource().fetch_runs()


runs = load_runs()

st.title("Data Platform")
st.caption("AWS Glue · Airflow · Airbyte 통합 현황 — 프로토타입 (샘플 데이터)")

picker, _ = st.columns([1, 4])
with picker:
    range_label = st.selectbox("Start date range", list(_RANGES), index=0)

# 선택 기간으로 필터 (데이터의 최신 시각 기준 — 실행 시각 드리프트에 견고)
anchor = max((r.started_at for r in runs if r.started_at), default=None)
if anchor is not None:
    cutoff = anchor - timedelta(days=_RANGES[range_label])
    windowed = [r for r in runs if r.started_at and r.started_at >= cutoff]
else:
    windowed = runs

render_source_summary(overall_summary(windowed))
for source in Source:
    src_runs = [r for r in windowed if r.source == source]
    render_source_summary(summarize(source, src_runs))
