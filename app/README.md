# FastAPI 프로젝트: GDELT Postgres 데이터 조회 API 및 웹사이트

이 프로젝트는 GDELT Postgres 테이블의 데이터를 조회하는 API와 웹사이트를 제공합니다.

## 주요 기능
- FastAPI 기반 REST API: Postgres 테이블의 일부 컬럼을 조회
- 웹사이트: API 결과를 테이블 형태로 표시
- 다중 테이블 지원: raw_export, raw_gkg, raw_mentions

## 로컬 개발

### 시작하기
1. 의존성 설치: `pip install -r requirements.txt`
2. 환경변수 설정: `.env` 파일에 DATABASE_URL 설정
3. 서버 실행: `uvicorn main:app --reload`
4. 웹사이트 접속: `http://localhost:8000`

### Cloud SQL 연결 (로컬)
1. Cloud SQL Auth Proxy 실행:
   ```bash
   ./cloud-sql-proxy silicon-garage-464510-p4:asia-northeast3:gdelt-db --port 5432
   ```
2. `.env`에서 DATABASE_URL을 localhost로 설정

## GCP 배포 (서버리스)

### 방법 1: 수동 배포 (최초 배포)
```bash
./deploy.sh
```

### 방법 2: 코드 수정 후 재배포
코드를 수정한 후 변경사항을 적용하려면:
```bash
./redeploy.sh
```

이 스크립트는 다음 작업을 자동으로 수행합니다:
1. 타임스탬프가 포함된 새로운 Docker 이미지 빌드
2. Container Registry에 이미지 푸시
3. Cloud Run 서비스 업데이트
4. 새 배포 URL 확인

### 방법 3: VS Code에서 재배포
VS Code의 Task Runner를 사용할 수도 있습니다:
- `Ctrl+Shift+P` → "Tasks: Run Task" → "Deploy to Cloud Run"

### 방법 4: Git 연동 자동 배포 (GitHub Actions)
1. GitHub 리포지토리 생성
2. 다음 시크릿 설정:
   - `GCP_SA_KEY`: 서비스 계정 JSON 키
   - `DB_PASSWORD`: 데이터베이스 비밀번호
3. main 브랜치에 푸시하면 자동 배포

### 방법 5: Cloud Build Trigger
1. Cloud Console에서 Cloud Build Trigger 생성
2. GitHub/GitLab 리포지토리 연결
3. `cloudbuild.yaml` 사용

## 개발 워크플로우

### 일반적인 개발 과정
1. **로컬 개발**: 코드를 수정하고 `uvicorn main:app --reload`로 로컬 테스트
2. **재배포**: 변경사항을 프로덕션에 적용하려면 `./redeploy.sh` 실행
3. **확인**: 배포된 URL에서 변경사항 확인

### 빠른 테스트 예시
```bash
# 1. 코드 수정 (예: templates/index.html의 제목 변경)
# 2. 재배포
./redeploy.sh
# 3. 브라우저에서 확인
```

## 아키텍처
- **Frontend**: Jinja2 템플릿 + CSS
- **Backend**: FastAPI (Python)
- **Database**: GCP Cloud SQL (PostgreSQL)
- **Deployment**: GCP Cloud Run (서버리스)
- **CI/CD**: GitHub Actions 또는 Cloud Build

## API 엔드포인트
- `GET /`: 웹 인터페이스
- `GET /?table={table_name}`: 특정 테이블 조회
- `GET /api/rows?table={table_name}`: JSON API

## 환경 변수
- `DATABASE_URL`: Postgres 연결 문자열

## 폴더 구조
- `main.py`: FastAPI 진입점
- `db.py`: 데이터베이스 연결 및 쿼리
- `templates/`: 웹사이트 템플릿
- `static/`: 정적 파일(CSS 등)
- `Dockerfile`: 컨테이너 이미지 빌드
- `deploy.sh`: 최초 배포 스크립트
- `redeploy.sh`: 코드 수정 후 재배포 스크립트
- `cloudbuild.yaml`: Cloud Build 설정
- `.github/workflows/`: GitHub Actions

---
이 프로젝트는 GDELT Postgres 테이블을 대상으로 하며, 서버리스 환경에서 확장 가능한 API 및 웹사이트를 제공합니다.
