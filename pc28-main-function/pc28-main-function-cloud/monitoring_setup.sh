#!/bin/bash

# 设置项目ID
PROJECT_ID="wprojectl"

echo "设置Google Cloud Monitoring和日志记录..."

# 启用必要的API
echo "启用Cloud Monitoring API..."
gcloud services enable monitoring.googleapis.com --project=$PROJECT_ID

echo "启用Cloud Logging API..."
gcloud services enable logging.googleapis.com --project=$PROJECT_ID

echo "启用Cloud Functions API..."
gcloud services enable cloudfunctions.googleapis.com --project=$PROJECT_ID

echo "启用BigQuery API..."
gcloud services enable bigquery.googleapis.com --project=$PROJECT_ID

# 创建日志记录策略
echo "创建日志记录策略..."
gcloud logging sinks create pc28-bigquery-sink \
    bigquery.googleapis.com/projects/$PROJECT_ID/datasets/lab_dataset \
    --log-filter='resource.type="cloud_function" AND resource.labels.function_name="pc28-e2e-function"' \
    --project=$PROJECT_ID

# 创建监控告警策略
echo "创建监控告警策略..."
cat > alert_policy.json << EOF
{
  "displayName": "PC28 Function Error Rate",
  "conditions": [
    {
      "displayName": "Function error rate too high",
      "conditionThreshold": {
        "filter": "resource.type=\"cloud_function\" AND resource.label.function_name=\"pc28-e2e-function\"",
        "comparison": "COMPARISON_GREATER_THAN",
        "thresholdValue": 0.1,
        "duration": "300s",
        "aggregations": [
          {
            "alignmentPeriod": "60s",
            "perSeriesAligner": "ALIGN_RATE",
            "crossSeriesReducer": "REDUCE_MEAN"
          }
        ]
      }
    }
  ],
  "enabled": true
}
EOF

gcloud alpha monitoring policies create --policy-from-file=alert_policy.json --project=$PROJECT_ID

echo "监控和日志记录设置完成！"
echo "可以在以下位置查看:"
echo "- Cloud Console: https://console.cloud.google.com/monitoring?project=$PROJECT_ID"
echo "- 日志: https://console.cloud.google.com/logs?project=$PROJECT_ID"
echo "- BigQuery: https://console.cloud.google.com/bigquery?project=$PROJECT_ID"