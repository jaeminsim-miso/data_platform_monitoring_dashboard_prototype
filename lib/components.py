"""화면 공통 UI 컴포넌트 — 시안에 맞춘 커스텀 요약 카드.

st.metric 은 값 색상·세로 구분선·hover 툴팁(토스트형)을 지원하지 않는다. 시안과
요청(지표 hover 시 설명 표시)을 재현하려고 카드 전체를 가벼운 HTML/CSS 로 렌더한다.
카드 테두리도 직접 그려(overflow:visible) 툴팁이 카드 밖으로 잘리지 않게 한다.
"""

from __future__ import annotations

import html

import streamlit as st

from lib.models import Source
from lib.summary import SourceSummary

# 지표 kind → 값 색상 (다크 테마 기준)
_COLOR = {
    "total": "#C7CDD6",
    "running": "#4FA8FF",
    "info": "#4FA8FF",
    "neutral": "#7E8590",
    "success": "#36C275",
    "failed": "#FF6B6B",
    "rate": "#36C275",
}

# 소스 → (브랜드명, 색상 점)
_BRAND = {
    Source.GLUE: ("AWS Glue", "#8C4FFF"),
    Source.AIRFLOW: ("Airflow", "#11B5E4"),
    Source.AIRBYTE: ("Airbyte", "#615EFF"),
}

_STYLES = """
<style>
.dp-card { border:1px solid rgba(255,255,255,.12); border-radius:.6rem; padding:1rem 1.25rem; margin-bottom:1rem; }
.dp-head { display:flex; justify-content:space-between; align-items:flex-start; margin-bottom:.5rem; }
.dp-title { font-size:1.25rem; font-weight:700; }
.dp-sub { color:#9aa0a6; font-size:.85rem; margin-top:.1rem; }
.dp-brand { display:flex; align-items:center; gap:.4rem; color:#c7cdd6; font-weight:600; }
.dp-dot { width:13px; height:13px; border-radius:3px; display:inline-block; }
.dp-infowrap { position:relative; display:inline-block; }
.dp-info { color:#6b7280; font-size:.8rem; margin-left:.4rem; cursor:help; }
.dp-row { display:flex; width:100%; }
.dp-cell { position:relative; flex:1; padding:.2rem 1.1rem; border-left:1px solid rgba(255,255,255,.08); cursor:default; }
.dp-cell:first-child { border-left:none; padding-left:0; }
.dp-lbl { color:#9aa0a6; font-size:.82rem; margin-bottom:.35rem; }
.dp-val { font-size:2rem; font-weight:400; line-height:1.1; }
.dp-tip { visibility:hidden; opacity:0; position:absolute; left:0; top:100%; margin-top:.4rem; z-index:1000;
  width:max-content; max-width:240px; background:#0e1117; color:#e6e9ee;
  border:1px solid rgba(255,255,255,.18); border-radius:.5rem; padding:.5rem .7rem;
  font-size:.78rem; font-weight:400; line-height:1.4; box-shadow:0 8px 24px rgba(0,0,0,.5);
  transition:opacity .12s ease; pointer-events:none; white-space:normal; }
.dp-cell:hover > .dp-tip, .dp-infowrap:hover > .dp-tip { visibility:visible; opacity:1; }
</style>
"""


def render_dashboard_styles() -> None:
    """카드 스타일을 한 번 주입한다 (페이지 상단에서 1회 호출)."""
    st.markdown(_STYLES, unsafe_allow_html=True)


def _tip(text: str) -> str:
    """설명 텍스트가 있으면 툴팁 span 을, 없으면 빈 문자열을 반환."""
    return f'<span class="dp-tip">{html.escape(text)}</span>' if text else ""


def render_source_summary(summary: SourceSummary) -> None:
    """요약 카드 한 개를 렌더한다 (지표 hover 시 설명 툴팁).

    source=None(Pipeline Utilization)이면 브랜드 라벨 없이 info 아이콘을 보인다.
    """
    brand = ""
    if summary.source in _BRAND:
        name, color = _BRAND[summary.source]
        brand = (
            f'<span class="dp-brand"><span class="dp-dot" '
            f'style="background:{color}"></span>{name}</span>'
        )
    info = ""
    if summary.help:
        info = (
            f'<span class="dp-infowrap"><span class="dp-info">&#9432;</span>'
            f"{_tip(summary.help)}</span>"
        )
    sub = f'<div class="dp-sub">{summary.subtitle}</div>' if summary.subtitle else ""
    head = (
        f'<div class="dp-head"><div>'
        f'<div class="dp-title">{summary.title}{info}</div>{sub}'
        f"</div>{brand}</div>"
    )
    cells = "".join(
        f'<div class="dp-cell"><div class="dp-lbl">{label}</div>'
        f'<div class="dp-val" style="color:{_COLOR[kind]}">{value}</div>'
        f"{_tip(help_text)}</div>"
        for label, value, kind, help_text in summary.metrics
    )
    st.markdown(
        f'<div class="dp-card">{head}<div class="dp-row">{cells}</div></div>',
        unsafe_allow_html=True,
    )
