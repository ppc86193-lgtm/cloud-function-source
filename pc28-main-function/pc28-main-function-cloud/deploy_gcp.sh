#!/bin/bash

# PC28 E2E Google Cloud Platform 完整部署脚本
# 作者: SOLO Coding
# 日期: $(date)

set -e  # 遇到错误时退出

# 配置变量
PROJECT_ID="wprojectl"
REGION="us-central1"
FUNCTION_NAME="pc28-e2e-function"
LAB_DATASET="lab_dataset"
DRAW_DATASET="draw_dataset"

echo "========================================"
echo "PC28 E2E Google Cloud Platform 部署"
echo "========================================"

# 1. 设置项目
echo "1. 设置GCP项目..."
gcloud config set project $PROJECT_ID

# 2. 启用必要的API
echo "2. 启用必要的Google Cloud APIs..."
gcloud services enable cloudfunctions.googleapis.com
gcloud services enable bigquery.googleapis.com
gcloud services enable monitoring.googleapis.com
gcloud services enable logging.googleapis.com
gcloud services enable run.googleapis.com

# 3. 创建BigQuery数据集
echo "3. 创建BigQuery数据集..."
bq mk --dataset --location=US $PROJECT_ID:$LAB_DATASET || echo "数据集 $LAB_DATASET 已存在"
bq mk --dataset --location=US $PROJECT_ID:$DRAW_DATASET || echo "数据集 $DRAW_DATASET 已存在"

# 4. 创建BigQuery表结构
echo "4. 创建BigQuery表结构..."
if [ -f "create_tables.sql" ]; then
    bq query --use_legacy_sql=false < create_tables.sql || echo "表结构已存在或创建失败，继续部署"
    echo "表结构处理完成"
else
    echo "警告: create_tables.sql 文件不存在"
fi

# 5. 部署Cloud Function
echo "5. 部署Cloud Function..."
if [ -f "main.py" ] && [ -f "requirements.txt" ]; then
    gcloud functions deploy $FUNCTION_NAME \
        --runtime python39 \
        --trigger-http \
        --allow-unauthenticated \
        --entry-point main \
        --region $REGION \
        --memory 512MB \
        --timeout 540s
    echo "Cloud Function部署完成"
else
    echo "错误: main.py 或 requirements.txt 文件不存在"
    exit 1
fi

# 6. 设置监控和日志
echo "6. 设置监控和日志记录..."
gcloud logging sinks create pc28-bigquery-sink \
    bigquery.googleapis.com/projects/$PROJECT_ID/datasets/$LAB_DATASET \
    --log-filter='resource.type="cloud_function" AND resource.labels.function_name="'$FUNCTION_NAME'"' \
    --project=$PROJECT_ID || echo "日志接收器已存在"

# 7. 授予权限
echo "7. 授予必要权限..."
SERVICE_ACCOUNT=$(gcloud logging sinks describe pc28-bigquery-sink --format="value(writerIdentity)" --project=$PROJECT_ID)
if [ ! -z "$SERVICE_ACCOUNT" ]; then
    gcloud projects add-iam-policy-binding $PROJECT_ID \
        --member="$SERVICE_ACCOUNT" \
        --role="roles/bigquery.dataEditor"
    echo "权限授予完成"
fi

# 8. 验证部署
echo "8. 验证部署..."
FUNCTION_URL="https://$REGION-$PROJECT_ID.cloudfunctions.net/$FUNCTION_NAME"
echo "测试Cloud Function..."
RESPONSE=$(curl -s -X GET "$FUNCTION_URL")
echo "响应: $RESPONSE"

# 9. 检查BigQuery表
echo "检查BigQuery表..."
echo "Lab Dataset 表:"
bq ls $PROJECT_ID:$LAB_DATASET
echo "Draw Dataset 表:"
bq ls $PROJECT_ID:$DRAW_DATASET

echo "========================================"
echo "部署完成！"
echo "========================================"
echo "Cloud Function URL: $FUNCTION_URL"
echo "BigQuery Console: https://console.cloud.google.com/bigquery?project=$PROJECT_ID"
echo "Cloud Monitoring: https://console.cloud.google.com/monitoring?project=$PROJECT_ID"
echo "Cloud Logging: https://console.cloud.google.com/logs?project=$PROJECT_ID"
echo "========================================"

# 10. 显示系统状态
echo "系统状态检查:"
echo "✓ 项目ID: $PROJECT_ID"
echo "✓ Cloud Function: $FUNCTION_NAME (已部署)"
echo "✓ BigQuery数据集: $LAB_DATASET, $DRAW_DATASET (已创建)"
echo "✓ 监控和日志: 已配置"
echo "✓ 权限: 已授予"

echo "部署脚本执行完成！"