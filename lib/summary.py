"""소스별 / 전체 실행 요약(Runs Summary) 계산.

화면에 보여줄 카드 한 줄(Total runs / Running / … / 소스 고유 지표)을
PipelineRun 목록에서 집계한다. streamlit·pandas 의존 없는 순수 로직이라
연결 없이도 단위 테스트가 가능하다.

각 지표는 (라벨, 표시문자열, kind) 3-튜플이다. kind 는 화면에서 값 색상을
정하는 의미 키(total/running/info/neutral/success/failed/rate)다.

소스별 컬럼 구성:
  - 공통:  Total runs · Running · Successful runs · Failed runs · Run success rate
  - 중간 상태(소스별):  Glue=Canceled · Airflow=Queued · Airbyte=Scheduled
  - 고유 지표(소스별):  Glue=DPU hours · Airflow=Active DAGs · Airbyte=Enabled Connections
"""

from __future__ import annotations

from dataclasses import dataclass

from lib.models import PipelineRun, RunStatus, Source

# 소스별: (제목, 부제, 중간상태 라벨, 중간상태, 중간상태 kind, 고유지표 라벨)
_SPEC: dict[Source, tuple[str, str, str, RunStatus, str, str]] = {
    Source.GLUE: (
        "AWS Glue Job",
        "Job runs summary",
        "Canceled",
        RunStatus.CANCELLED,
        "neutral",
        "DPU hours",
    ),
    Source.AIRFLOW: (
        "Airflow DAG",
        "DAG runs summary",
        "Queued",
        RunStatus.PENDING,
        "info",
        "Active DAGs",
    ),
    Source.AIRBYTE: (
        "Airbyte Connection / Stream Sync",
        "Sync runs summary",
        "Scheduled",
        RunStatus.PENDING,
        "info",
        "Enabled Connections",
    ),
}


# 지표 설명 (hover 툴팁용). 같은 라벨도 소스마다 의미가 달라 소스별로 둔다.
# None 키는 전체 합산(Pipeline Utilization) 카드용.
_METRIC_HELP: dict[Source | None, dict[str, str]] = {
    None: {
        "Total runs": "Glue·Airflow·Airbyte 전체 실행 건수 합계",
        "Running": "현재 실행 중인 전체 작업 수 (세 소스 합계)",
        "Successful runs": "성공 완료된 전체 실행 건수 합계",
        "Failed runs": "실패한 전체 실행 건수 합계",
        "Run success rate": "전체 성공률 = 성공 / (성공 + 실패)",
    },
    Source.GLUE: {
        "Total runs": "전체 Job 실행 건수",
        "Running": "현재 실행 중인 Job 수",
        "Canceled": "사용자 또는 시스템에 의해 취소된 Job 수",
        "Successful runs": "성공 완료 건수",
        "Failed runs": "실패 건수",
        "Run success rate": "성공률",
        "DPU hours": "사용된 총 DPU 시간",
    },
    Source.AIRFLOW: {
        "Total runs": "DAG Run 전체 실행 건수",
        "Running": "현재 실행 중인 DAG Run 수",
        "Queued": "Scheduler 대기 중인 DAG Run 수",
        "Successful runs": "성공 완료된 DAG Run 수",
        "Failed runs": "실패한 DAG Run 수",
        "Run success rate": "DAG 실행 성공률",
        "Active DAGs": "활성화(ON) 상태의 DAG 개수",
    },
    Source.AIRBYTE: {
        "Total runs": "전체 Sync 실행 건수",
        "Running": "현재 수행 중인 Sync 수",
        "Scheduled": "예약된 Sync 수",
        "Successful runs": "성공 완료된 Sync 수",
        "Failed runs": "실패한 Sync 수",
        "Run success rate": "Sync 성공률",
        "Enabled Connections": "활성화된 Connection 수",
    },
}


def _with_help(
    source: Source | None, base: list[tuple[str, str, str]]
) -> list[tuple[str, str, str, str]]:
    """(라벨, 값, kind) 목록에 소스별 설명을 붙여 4-튜플로 만든다."""
    helps = _METRIC_HELP.get(source, {})
    return [(label, value, kind, helps.get(label, "")) for label, value, kind in base]


@dataclass
class SourceSummary:
    """카드 한 개 분량의 요약. source=None 이면 전체 합산(Pipeline Utilization)."""

    source: Source | None
    title: str
    subtitle: str | None
    metrics: list[tuple[str, str, str, str]]  # (라벨, 표시문자열, kind, 설명)
    help: str | None = None


def _fmt_rate(success: int, failed: int) -> str:
    """성공률 = 성공 / (성공 + 실패). 완료된 실행만 분모로 본다."""
    denom = success + failed
    if not denom:
        return "—"
    rate = success / denom * 100
    return f"{rate:.0f}%" if rate == round(rate) else f"{rate:.1f}%"


def _counts(runs: list[PipelineRun]) -> tuple[int, int, int]:
    running = sum(r.status == RunStatus.RUNNING for r in runs)
    success = sum(r.status == RunStatus.SUCCESS for r in runs)
    failed = sum(r.status == RunStatus.FAILED for r in runs)
    return running, success, failed


def overall_summary(runs: list[PipelineRun]) -> SourceSummary:
    """모든 소스를 합산한 전체 가동 현황(Pipeline Utilization)."""
    running, success, failed = _counts(runs)
    base = [
        ("Total runs", f"{len(runs):,}", "total"),
        ("Running", f"{running:,}", "running"),
        ("Successful runs", f"{success:,}", "success"),
        ("Failed runs", f"{failed:,}", "failed"),
        ("Run success rate", _fmt_rate(success, failed), "rate"),
    ]
    return SourceSummary(
        source=None,
        title="Pipeline Utilization",
        subtitle=None,
        metrics=_with_help(None, base),
        help="모든 소스(Glue·Airflow·Airbyte)의 실행을 합산한 전체 가동 현황",
    )


def summarize(source: Source, runs: list[PipelineRun]) -> SourceSummary:
    title, subtitle, extra_label, extra_status, extra_kind, tail_label = _SPEC[source]
    running, success, failed = _counts(runs)
    extra = sum(r.status == extra_status for r in runs)

    if source is Source.GLUE:
        # DPU hours = Σ(DPU × 소요시간[h])
        dpu_hours = sum(
            (r.extra.get("dpu", 0) or 0) * ((r.duration_seconds or 0) / 3600)
            for r in runs
        )
        tail_value = f"{dpu_hours:,.0f}"
    else:
        # Active DAGs / Enabled Connections = 서로 다른 파이프라인 수
        tail_value = f"{len({r.pipeline_name for r in runs}):,}"

    base = [
        ("Total runs", f"{len(runs):,}", "total"),
        ("Running", f"{running:,}", "running"),
        (extra_label, f"{extra:,}", extra_kind),
        ("Successful runs", f"{success:,}", "success"),
        ("Failed runs", f"{failed:,}", "failed"),
        ("Run success rate", _fmt_rate(success, failed), "rate"),
        (tail_label, tail_value, "info"),
    ]
    return SourceSummary(
        source=source, title=title, subtitle=subtitle, metrics=_with_help(source, base)
    )
