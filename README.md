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
│   │   ├── sample.py               # 샘플 소스 (실연결 실패 시 폴백용)
│   │   ├── glue.py                 # ✅ boto3 Glue API 실연결
│   │   ├── airflow.py              # ✅ REST API(/api/v1) + basic_auth 실연결
│   │   └── airbyte.py              # ✅ 메타DB(Postgres) 실연결
│   └── components.py               # 공통 UI (색상 값·구분선·테두리 카드 + hover 툴팁 HTML 렌더)
├── requirements.txt                # 런타임: streamlit, boto3, psycopg2-binary, requests (그 외 주석)
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

> **진행 상태:** 단일 화면(기간 선택 · 전체 합산 · 소스별 카드 · 색상 값 · hover 툴팁 · 다크 테마) 완성.
> **3개 소스 모두 실연결** — Glue(boto3) · Airbyte(메타DB) · Airflow(REST API). 소스별로 실패 시 샘플 자동 폴백.
> (전제: macOS + [pyenv](https://github.com/pyenv/pyenv); Glue는 `aws sso login --profile miso`, Airbyte/Airflow는 각 `kubectl … port-forward`)

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

## 데이터 소스 연결

| 소스 | 상태 | 방식 |
|------|------|------|
| **AWS Glue** | ✅ 실연결 | boto3 Glue API (`list_jobs`/`get_job_runs`) · AWS SSO 프로파일 |
| **Airbyte** | ✅ 실연결 | 인클러스터 메타DB(Postgres) 직접 조회 · 포트포워딩 |
| **Airflow** | ✅ 실연결 | REST API(`/api/v1`) + basic_auth · 포트포워딩 |

### AWS Glue (구현됨 — `lib/datasource/glue.py`)

- **인증**: AWS SSO 프로파일(기본 `miso`). 실행 전 `aws sso login --profile miso` 로 세션 갱신.
- **읽기 전용**: `glue:ListJobs`, `glue:GetJobRuns` 만 사용.
- **비용**: 잡 관리 API(ListJobs/GetJobRuns)는 **요청당 과금 없음** (과금은 잡 실행 DPU-시간·Data Catalog 요청에만 해당).
- **성능**: 잡별 조회를 **병렬화**(ThreadPool 16) + **선택 기간만** 조회 + `st.cache_data(ttl=60)`. 7일 전체 기준 약 50s → 5s.
- **DPU hours**: 할당 DPU × `ExecutionTime`[h] 합산 (오토스케일/Flex 미사용 전제). 매핑은 `_to_pipeline_run`/`_allocated_dpu` 순수 함수 → AWS 없이 검증 가능.
- **폴백**: 연결 실패(SSO 만료·권한·네트워크) 시 **샘플 Glue 로 자동 대체 + 경고 배너** → 화면이 깨지지 않음.
- **설정** (`.streamlit/secrets.toml` `[glue]`, 없어도 기본값 동작): `profile` / `region` / `lookback_days` / `job_names`(해당 잡만) / `exclude_jobs`(지표에서 제외할 잡 — 정확한 이름 또는 `test-*` 같은 glob).

> ⚠️ 운영 계정에 닿는 핵심 경로. 읽기 전용·캐싱으로 부하를 억제하지만, 처음엔 `job_names` 로 좁혀 검증 후 전체로 넓히길 권장.

### Airbyte (구현됨 — `lib/datasource/airbyte.py`)

- **방식**: Airbyte OSS의 **인클러스터 Postgres 메타DB**(`db-airbyte`) 직접 조회. (RDS가 아니라 포트포워딩으로 로컬 접근 가능)
  ```bash
  kubectl -n airbyte port-forward svc/airbyte-db-svc 5432:5432   # 켜둔 채로 실행
  ```
- **읽기 전용 SELECT**: `jobs`(config_type='sync') 상태 집계 + `connection`(status='active') 카운트. 연결 1회로 두 쿼리.
- **지표 의미**: 완료(succeeded/failed/…)는 기간 내(`created_at`), **Running/Scheduled(=pending)는 현재 시점 전체**, Enabled Connections는 활성 커넥션 수.
- **설정** (`.streamlit/secrets.toml` `[airbyte]`): `host`/`port`/`dbname`/`user`/`password`. 기본값(localhost:5432, db-airbyte, airbyte)으로 동작하며, DB의 loopback 인증 설정에 따라 password 없이도 붙을 수 있음.
- **폴백**: 연결 실패 시 샘플 Airbyte 로 자동 대체 + 경고 배너.

### Airflow (구현됨 — `lib/datasource/airflow.py`)

- **방식**: Airflow Stable REST API(`/api/v1`). webserver 가 사설망이라 포트포워딩으로 로컬 접근:
  ```bash
  kubectl -n airflow port-forward svc/airflow-webserver 8080:8080   # 켜둔 채로 실행
  ```
- **인증**: UI는 Google OAuth지만 API는 **basic_auth + 전용 Viewer 로컬 계정**(`dashboard-monitor`). 메타DB는 사설 RDS라 직접 조회 불가였고, GitOps로 `auth_backends`에 basic_auth 추가해 해결.
- **읽기 전용**: 완료(success/failed)는 `dagRuns/list` 의 `start_date_gte`, running/queued는 현재 시점, Active DAGs는 `GET /dags?paused=false&only_active=true`.
- **설정** (`.streamlit/secrets.toml` `[airflow]`): `base_url`(`http://localhost:8080`)/`username`/`password`.
- **폴백**: 연결 실패 시 샘플 Airflow 로 자동 대체 + 경고 배너.

> 🔒 운영 메모: 이 Airflow는 `AUTH_USER_REGISTRATION_ROLE=Admin`(OAuth 로그인 시 자동 Admin 부여)이라 별건으로 점검 권장.

---

## 향후 (하트비트 이관)

프로토타입에서 화면 기획이 검증되면, 검증된 레이아웃과 `PipelineRun` 모델을 기준으로
하트비트 환경에 본 구현을 진행합니다. 데이터 소스 인터페이스를 분리해 둔 덕분에
화면 설계는 재사용하고 연동부만 하트비트 스택에 맞춰 다시 작성하면 됩니다.
