# World News GDELT Pipeline

Apache Airflow를 사용한 GDELT(Global Database of Events, Language, and Tone) 데이터 파이프라인입니다.

## 개요

이 프로젝트는 GDELT 데이터를 자동으로 수집, 처리, 저장하고 분석하는 데이터 파이프라인을 제공합니다.

### 주요 기능

- **자동 데이터 수집**: GDELT 웹사이트에서 최신 데이터 자동 다운로드
- **데이터 처리**: Export, Mentions, GKG 데이터 정제 및 변환
- **PostgreSQL 저장**: 정제된 데이터를 PostgreSQL 데이터베이스에 저장
- **Google Cloud Storage 연동**: 처리된 데이터를 GCS에 백업
- **모니터링**: 데이터 품질 및 파이프라인 실행 모니터링
- **Slack 알림**: 일일 리포트 및 오류 알림

## 아키텍처

```
GDELT API → Airflow → Data Processing → PostgreSQL
                                    ↓
                              Google Cloud Storage
                                    ↓
                                 Slack 알림
```

## 구성 요소

### DAGs
- `gdelt_to_gcs.py`: 메인 데이터 파이프라인
- `gdelt_daily_report.py`: 일일 리포트 생성

### Scripts
- `schemas.py`: 데이터 스키마 정의
- `load_to_postgres.py`: PostgreSQL 로딩 스크립트
- `monitoring.py`: 데이터 품질 및 실행 메트릭 수집
- `daily_report.py`: Slack 리포트 생성
- `converters.py`: 데이터 변환 유틸리티
- `utils.py`: 공통 유틸리티 함수

### Process Scripts
- `export.py`: Export 데이터 처리
- `mentions.py`: Mentions 데이터 처리
- `gkg.py`: GKG 데이터 처리

## 배포

### Kubernetes 배포

GKE(Google Kubernetes Engine)에서 실행하도록 설계되었습니다.

#### 요구사항
- Google Cloud Project
- GKE 클러스터
- Cloud SQL PostgreSQL 인스턴스
- Google Cloud Storage 버킷
- Artifact Registry

#### 환경 변수
```bash
GOOGLE_CLOUD_PROJECT=your-project-id
POSTGRES_HOST=your-postgres-host
POSTGRES_DB=your-database
POSTGRES_USER=your-username
POSTGRES_PASSWORD=your-password
GCS_BUCKET=your-gcs-bucket
SLACK_WEBHOOK_URL=your-slack-webhook
```

## 로컬 개발

### Docker Compose
```bash
docker-compose up -d
```

### 수동 설치
```bash
pip install -r requirements.txt
```

## 모니터링

파이프라인은 다음 메트릭을 수집합니다:

- **실행 메트릭**: DAG 실행 시간, 성공/실패 상태
- **데이터 품질 메트릭**: 레코드 수, 데이터 유효성 검사 결과
- **PostgreSQL 저장**: 모든 메트릭이 데이터베이스에 저장됨

## Slack 통합

일일 리포트와 오류 알림을 Slack으로 전송합니다:

- 📊 일일 데이터 처리 현황
- ⚠️ 에러 및 경고 알림
- 📈 데이터 품질 리포트

## 라이선스

MIT License
