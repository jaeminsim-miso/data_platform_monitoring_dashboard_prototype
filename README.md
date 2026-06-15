# Data Platform 통합 모니터링 대시보드 — 프로토타입

사내 모니터링 대시보드(**하트비트**)에 **Data Platform 통합 모니터링**을 구축하기 전,
**Streamlit**으로 화면 기획(정보구조·레이아웃)을 빠르게 검증하기 위한 프로토타입입니다.

> **이 레포의 목적은 "동작하는 운영 대시보드"가 아니라 "화면 기획 검증"입니다.**
> 그래서 환경은 얇게 가져가고, 실제 데이터 소스 연결은 *인터페이스만 먼저* 설계한 뒤
> 접근 권한이 확보되는 시점에 붙입니다. 배포는 하지 않고 로컬에서만 구동합니다.

---

## 무엇을 검증하나

서로 다른 오케스트레이터에 흩어져 있는 **파이프라인·배치 잡 상태를 한 화면에서 통합**해
보여줄 수 있는지를 검증합니다. 대상 소스는 세 가지입니다.

| 소스 | 단위 | 가져올 정보 (예시) |
|------|------|---------------------|
| **AWS Glue** | Job | Job Run 상태, 시작/종료 시각, 소요시간, DPU |
| **Airflow on EKS** | DAG | DAG Run 상태, 실행 시각, Task 수 |
| **Airbyte on EKS** | Connection / Stream | Sync 상태, 동기화 레코드 수, 스트림 |

### 핵심 설계 — 정규화된 실행 모델 `PipelineRun`

세 시스템은 상태 표현이 제각각입니다 (Glue `SUCCEEDED/FAILED`, Airflow `success/queued`,
Airbyte `succeeded/cancelled` …). 이걸 한 화면에서 일관되게 보여주는 것이 이번 기획의 핵심이고,
그 답은 **공통 실행 모델로 정규화**하는 것입니다.

| 필드 | 설명 |
|------|------|
| `source` | `Glue` \| `Airflow` \| `Airbyte` |
| `pipeline_name` | Job명 / DAG id / Connection명 |
| `run_id` | 실행 식별자 |
| `status` | **정규화된 상태** (`Success`/`Failed`/`Running`/`Cancelled` …) — 시스템별 원시 상태를 매핑 |
| `started_at`, `ended_at`, `duration` | 실행 구간·소요시간 |
| `last_run_at` | 최근 실행 시각 |
| `message` | 실패 사유 |
| `extra` | 시스템별 부가정보 (Airbyte: records/stream, Glue: DPU, Airflow: task 수) |

화면은 이 모델에만 의존합니다. → 추후 하트비트로 이관할 때 **화면 코드는 그대로 두고
데이터 소스 구현(`lib/datasource/*`)만 교체**하면 됩니다.

---

## 범위

**이번 프로토타입에서 한다**
- 단일 화면: *파이프라인·배치 잡 상태 통합 뷰*
- 정규화 모델(`PipelineRun`) + 데이터 소스 인터페이스 설계
- 임시 샘플 데이터로 화면·레이아웃 검증
- 로컬 실행

**이번엔 (의도적으로) 안 한다**
- 실제 소스 연결 — 접근 권한 확보 후. 지금은 인터페이스/스텁만 준비
- 배포 — 로컬 전용
- 인증·권한, 알림, 다중 화면 — 기획 검증이 끝난 뒤 하트비트 본 구현에서

---

## 프로젝트 구조

```
.
├── .python-version                 # Python 3.12.13 고정 (pyenv)
├── .streamlit/
│   ├── config.toml                 # 테마 + 텔레메트리 off (커밋)
│   └── secrets.toml.example        # 연결정보 템플릿 (실제 secrets.toml은 gitignore)
├── app.py                          # 단일 화면 — 소스별 Runs Summary 를 한 번에 (진입점)
├── lib/
│   ├── models.py                   # PipelineRun 정규화 모델 + 상태 매핑
│   ├── summary.py                  # 소스별 Runs Summary 집계 (순수 로직)
│   ├── datasource/
│   │   ├── base.py                 # DataSource 인터페이스: fetch_runs() -> list[PipelineRun]
│   │   ├── sample.py               # 샘플 소스 (지금 동작 — 3소스 형태·규모 모사)
│   │   ├── glue.py                 # 스텁 (TODO: boto3)
│   │   ├── airflow.py              # 스텁 (TODO: Airflow REST API)
│   │   └── airbyte.py              # 스텁 (TODO: Airbyte API)
│   └── components.py               # 공통 UI (색상 값·구분선·테두리 카드 + hover 툴팁 HTML 렌더)
├── requirements.txt                # 런타임: streamlit (그 외 라이브러리는 주석 처리)
├── requirements-dev.txt            # 개발용: ruff
└── README.md
```

> `.gitignore`: 소스 패키지가 `lib/` 에 있어, 표준 Python `.gitignore` 의 `lib/`(빌드 산출물용) 무시 규칙을
> `!lib/` 로 풀어 두었다. **이 줄을 지우면 `lib/` 가 git 에서 통째로 누락**되니 주의.

### 화면 구성

다크 테마, 필터 없이 **한 화면에 위→아래로 한 번에** 보여준다. 값은 의미별로 색을 입힌다
(진행중=파랑, 성공/성공률=초록, 실패=빨강).

1. **Start date range** 셀렉터 — `1 Day / 3 Days / 7 Days`. 선택 기간(최신 실행 시각 기준)으로 모든 카드를 필터.
2. **Pipeline Utilization** — 세 소스 합산 카드: Total · Running · Successful · Failed · Run success rate.
3. **소스별 Runs Summary** 카드 (우측에 브랜드 라벨):

| 소스 | 제목 / 부제 | 지표 (좌→우) |
|------|-------------|--------------|
| **Glue** | AWS Glue Job / Job runs summary | Total · Running · **Canceled** · Successful · Failed · Success rate · **DPU hours** |
| **Airflow** | Airflow DAG / DAG runs summary | Total · Running · **Queued** · Successful · Failed · Success rate · **Active DAGs** |
| **Airbyte** | Airbyte Connection / Stream Sync / Sync runs summary | Total · Running · **Scheduled** · Successful · Failed · Success rate · **Enabled Connections** |

> 공통 지표(Total/Running/Successful/Failed/Success rate)에 더해, **중간 상태**(Canceled/Queued/Scheduled)와
> **소스 고유 지표**(DPU hours/Active DAGs/Enabled Connections)가 소스별로 다르다.
> 성공률 = 성공 / (성공 + 실패).
>
> 각 지표에 **마우스를 올리면 설명 툴팁**이 뜬다(설명은 소스별로 다르며 `summary.py` 의 `_METRIC_HELP` 에 있다).
>
> *색상 값·세로 구분선·테두리 카드·hover 툴팁은 `st.metric` 으로 표현이 안 돼 `components.py` 에서 가벼운 HTML/CSS 로 렌더한다.*

---

## 실행 방법

> **진행 상태 (현재 구현됨):** 단일 화면에 ① 기간 선택(1/3/7일) ② 전체 합산(Pipeline Utilization)
> ③ 소스별 Runs Summary 카드(색상 값 · hover 설명 툴팁 · 브랜드 라벨)까지 **샘플 데이터로 동작**합니다. 다크 테마.
> 다음 단계는 실제 소스 연결(아래 "데이터 소스 연결" 참고).
> (전제: macOS, [pyenv](https://github.com/pyenv/pyenv) 설치)

```bash
# 1. Python 버전 고정 (pyenv)
pyenv local 3.12.13

# 2. 가상환경 생성·활성화
python -m venv .venv
source .venv/bin/activate

# 3. 의존성 설치
pip install -r requirements.txt
pip install -r requirements-dev.txt   # 개발용(ruff). 필요 시

# 4. 실행
streamlit run app.py                  # http://localhost:8501
```

샘플 데이터로 동작하므로 외부 연결 없이 바로 화면을 확인할 수 있습니다.
(샘플은 최근 7일 구간에 소스별 현실적 규모로 생성되어, 기간 선택에 따라 수치가 자연스럽게 변합니다.)

---

## 개발 · 검증

배포 대상이 아니라 테스트 스위트 대신 **lint + 헤드리스 스모크 실행**을 검증 계층으로 둔다.

```bash
# 코드 스타일 · 정적 검사 (개발 의존성 필요: pip install -r requirements-dev.txt)
ruff check .
ruff format .

# 앱이 예외 없이 끝까지 렌더되는지 — 브라우저 없이 검증
python -c "from streamlit.testing.v1 import AppTest; at = AppTest.from_file('app.py').run(); assert not at.exception, at.exception; print('OK')"
```

- 집계(`lib/summary.py`)·정규화(`lib/models.py`)는 streamlit 의존이 없는 순수 함수라, 연결 없이도 값 검증이 쉽다.
- 지표 설명(hover 툴팁) 문구는 소스별로 `lib/summary.py` 의 `_METRIC_HELP` 에서 관리한다.

---

## 데이터 소스 연결 (후속 작업)

접근 권한이 확보되면 `lib/datasource/{glue,airflow,airbyte}.py` 스텁을 실제 구현으로 채웁니다.

- **접속정보**는 `.streamlit/secrets.toml` 에 둡니다 (gitignore됨 — `secrets.toml.example` 참고). 코드에 하드코딩 금지.
- **운영 소스 보호 원칙**: 읽기 전용 계정 + `st.cache_data(ttl)` 캐싱으로 호출 횟수를 억제합니다.
- ⚠️ **실연결 코드는 운영 시스템에 닿는 핵심 경로**이므로 머지 전 사람 검토가 필요합니다.

---

## 향후 (하트비트 이관)

프로토타입에서 화면 기획이 검증되면, 검증된 레이아웃과 `PipelineRun` 모델을 기준으로
하트비트 환경에 본 구현을 진행합니다. 데이터 소스 인터페이스를 분리해 둔 덕분에
화면 설계는 재사용하고 연동부만 하트비트 스택에 맞춰 다시 작성하면 됩니다.
