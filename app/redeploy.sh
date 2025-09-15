#!/bin/bash

# 빠른 재배포 스크립트
PROJECT_ID="silicon-garage-464510-p4"
SERVICE_NAME="gdelt-web-app"
REGION="asia-northeast1"

echo "🔄 코드 변경사항을 재배포합니다..."

# Docker 이미지 빌드 (새 태그 사용)
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
IMAGE_NAME="gcr.io/$PROJECT_ID/$SERVICE_NAME:$TIMESTAMP"

echo "🐳 Docker 이미지 빌드 중..."
docker build -t $IMAGE_NAME .

echo "📤 이미지 업로드 중..."
docker push $IMAGE_NAME

echo "🚀 Cloud Run 업데이트 중..."
gcloud run deploy $SERVICE_NAME \
  --image $IMAGE_NAME \
  --region $REGION \
  --platform managed \
  --set-env-vars DATABASE_URL="postgresql://postgres:0503Tae**&@localhost/db?host=/cloudsql/silicon-garage-464510-p4:asia-northeast3:gdelt-db" \
  --add-cloudsql-instances silicon-garage-464510-p4:asia-northeast3:gdelt-db

echo "✅ 재배포 완료!"
echo "🌐 서비스 URL들:"
echo "  📍 기본 URL: $(gcloud run services describe $SERVICE_NAME --region $REGION --format 'value(status.url)')"

# 커스텀 도메인이 있다면 표시
CUSTOM_DOMAINS=$(gcloud run domain-mappings list --region $REGION --filter="metadata.labels.run.googleapis.com/service=$SERVICE_NAME" --format="value(spec.routeName)" 2>/dev/null)
if [ ! -z "$CUSTOM_DOMAINS" ]; then
    echo "  🌍 커스텀 도메인:"
    echo "$CUSTOM_DOMAINS" | while read domain; do
        echo "     https://$domain"
    done
fi
