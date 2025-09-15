#!/bin/bash

# GDELT Web App을 GCP Cloud Run에 배포하는 스크립트

PROJECT_ID="silicon-garage-464510-p4"
REGION="asia-northeast3"
SERVICE_NAME="gdelt-web-app"
CLOUD_SQL_INSTANCE="silicon-garage-464510-p4:asia-northeast3:gdelt-db"

echo "🚀 GDELT Web App을 Cloud Run에 배포합니다..."

# 1. 프로젝트 설정 확인
echo "📋 프로젝트 설정 확인 중..."
gcloud config set project $PROJECT_ID

# 2. 필요한 API 활성화
echo "🔧 필요한 API들을 활성화 중..."
gcloud services enable cloudbuild.googleapis.com
gcloud services enable run.googleapis.com
gcloud services enable containerregistry.googleapis.com

# 3. Docker 이미지 빌드 및 푸시
echo "🐳 Docker 이미지 빌드 중..."
IMAGE_NAME="gcr.io/$PROJECT_ID/$SERVICE_NAME"
docker build -t $IMAGE_NAME .
docker push $IMAGE_NAME

# 4. Cloud Run에 배포
echo "☁️  Cloud Run에 배포 중..."
gcloud run deploy $SERVICE_NAME \
  --image $IMAGE_NAME \
  --region $REGION \
  --platform managed \
  --allow-unauthenticated \
  --add-cloudsql-instances $CLOUD_SQL_INSTANCE \
  --set-env-vars "DATABASE_URL=postgresql://postgres:\$DB_PASSWORD@/cloudsql/$CLOUD_SQL_INSTANCE/db" \
  --memory 512Mi \
  --cpu 1 \
  --max-instances 10

echo "✅ 배포 완료!"
echo "🌐 서비스 URL을 확인하세요:"
gcloud run services describe $SERVICE_NAME --region $REGION --format 'value(status.url)'
