#!/bin/bash

# PC28 E2E 系统状态检查脚本
# 验证所有Google Cloud服务的运行状态

PROJECT_ID="wprojectl"
REGION="us-central1"
FUNCTION_NAME="pc28-e2e-function"

echo "========================================"
echo "PC28 E2E 系统状态检查"
echo "========================================"

# 1. 检查项目配置
echo "1. 检查GCP项目配置..."
CURRENT_PROJECT=$(gcloud config get-value project 2>/dev/null)
if [ "$CURRENT_PROJECT" = "$PROJECT_ID" ]; then
    echo "✓ 项目配置正确: $PROJECT_ID"
else
    echo "✗ 项目配置错误: 当前 $CURRENT_PROJECT, 期望 $PROJECT_ID"
fi

# 2. 检查API启用状态
echo "\n2. 检查API启用状态..."
APIS=("cloudfunctions.googleapis.com" "bigquery.googleapis.com" "monitoring.googleapis.com" "logging.googleapis.com")
for api in "${APIS[@]}"; do
    if gcloud services list --enabled --filter="name:$api" --format="value(name)" | grep -q "$api"; then
        echo "✓ $api 已启用"
    else
        echo "✗ $api 未启用"
    fi
done

# 3. 检查Cloud Function状态
echo "\n3. 检查Cloud Function状态..."
FUNCTION_STATUS=$(gcloud functions list --regions=$REGION --filter="name:$FUNCTION_NAME" --format="value(state)" 2>/dev/null)
if [ "$FUNCTION_STATUS" = "ACTIVE" ]; then
    echo "✓ Cloud Function状态: ACTIVE"
    FUNCTION_URL="https://$REGION-$PROJECT_ID.cloudfunctions.net/$FUNCTION_NAME"
    echo "  URL: $FUNCTION_URL"
    
    # 测试函数响应
    echo "  测试函数响应..."
    RESPONSE=$(curl -s -w "HTTP_CODE:%{http_code}" -X GET "$FUNCTION_URL")
    HTTP_CODE=$(echo "$RESPONSE" | grep -o "HTTP_CODE:[0-9]*" | cut -d: -f2)
    BODY=$(echo "$RESPONSE" | sed 's/HTTP_CODE:[0-9]*$//')
    
    if [ "$HTTP_CODE" = "200" ]; then
        echo "  ✓ HTTP响应: $HTTP_CODE"
        echo "  ✓ 响应内容: $BODY"
    else
        echo "  ✗ HTTP响应错误: $HTTP_CODE"
        echo "  ✗ 响应内容: $BODY"
    fi
else
    echo "✗ Cloud Function状态: $FUNCTION_STATUS"
fi

# 4. 检查BigQuery数据集
echo "\n4. 检查BigQuery数据集..."
DATASETS=("lab_dataset" "draw_dataset")
for dataset in "${DATASETS[@]}"; do
    if bq ls --project_id=$PROJECT_ID | grep -q "$dataset"; then
        echo "✓ 数据集 $dataset 存在"
        
        # 检查表
        echo "  表列表:"
        bq ls $PROJECT_ID:$dataset | tail -n +3 | while read line; do
            if [ ! -z "$line" ]; then
                table_name=$(echo $line | awk '{print $1}')
                echo "    - $table_name"
            fi
        done
    else
        echo "✗ 数据集 $dataset 不存在"
    fi
done

# 5. 检查监控和日志配置
echo "\n5. 检查监控和日志配置..."
if gcloud logging sinks describe pc28-bigquery-sink --project=$PROJECT_ID >/dev/null 2>&1; then
    echo "✓ 日志接收器 pc28-bigquery-sink 已配置"
else
    echo "✗ 日志接收器 pc28-bigquery-sink 未配置"
fi

# 6. 检查权限配置
echo "\n6. 检查权限配置..."
IAM_POLICY=$(gcloud projects get-iam-policy $PROJECT_ID --format=json)
if echo "$IAM_POLICY" | grep -q "gcp-sa-logging.iam.gserviceaccount.com"; then
    echo "✓ 日志服务账户权限已配置"
else
    echo "✗ 日志服务账户权限未配置"
fi

# 7. 系统整体状态评估
echo "\n========================================"
echo "系统整体状态评估"
echo "========================================"

# 检查关键组件
ERROR_COUNT=0

# Cloud Function检查
if [ "$FUNCTION_STATUS" != "ACTIVE" ]; then
    ((ERROR_COUNT++))
    echo "✗ Cloud Function未正常运行"
fi

# BigQuery检查
for dataset in "${DATASETS[@]}"; do
    if ! bq ls --project_id=$PROJECT_ID | grep -q "$dataset"; then
        ((ERROR_COUNT++))
        echo "✗ BigQuery数据集 $dataset 缺失"
    fi
done

if [ $ERROR_COUNT -eq 0 ]; then
    echo "🎉 系统状态良好！所有组件正常运行"
    echo "\n📊 访问链接:"
    echo "   • Cloud Function: https://$REGION-$PROJECT_ID.cloudfunctions.net/$FUNCTION_NAME"
    echo "   • BigQuery Console: https://console.cloud.google.com/bigquery?project=$PROJECT_ID"
    echo "   • Cloud Monitoring: https://console.cloud.google.com/monitoring?project=$PROJECT_ID"
    echo "   • Cloud Logging: https://console.cloud.google.com/logs?project=$PROJECT_ID"
else
    echo "⚠️  发现 $ERROR_COUNT 个问题，请检查上述错误信息"
fi

echo "\n检查完成！"