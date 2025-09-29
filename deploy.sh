#!/bin/bash

# PC28 运维管理系统 Google Cloud 部署脚本

set -e  # 遇到错误立即退出

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 配置变量
PROJECT_ID="pc28-ops-system"
SERVICE_NAME="pc28-ops-system"
REGION="us-central1"
DEPLOY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

log_info "开始部署 PC28 运维管理系统到 Google Cloud..."
log_info "项目ID: $PROJECT_ID"
log_info "服务名: $SERVICE_NAME"
log_info "区域: $REGION"
log_info "部署目录: $DEPLOY_DIR"

# 检查必要的工具
check_dependencies() {
    log_info "检查依赖工具..."
    
    if ! command -v gcloud &> /dev/null; then
        log_error "gcloud CLI 未安装，请先安装 Google Cloud SDK"
        exit 1
    fi
    
    if ! command -v python3 &> /dev/null; then
        log_error "Python 3 未安装"
        exit 1
    fi
    
    log_success "依赖检查完成"
}

# 验证 Google Cloud 认证
check_auth() {
    log_info "检查 Google Cloud 认证..."
    
    if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q .; then
        log_warning "未检测到活跃的认证账户"
        log_info "请运行: gcloud auth login"
        exit 1
    fi
    
    log_success "认证检查完成"
}

# 设置项目
setup_project() {
    log_info "设置 Google Cloud 项目..."
    
    # 检查项目是否存在
    if ! gcloud projects describe $PROJECT_ID &> /dev/null; then
        log_warning "项目 $PROJECT_ID 不存在，正在创建..."
        gcloud projects create $PROJECT_ID --name="PC28 Ops Management System"
        log_success "项目创建完成"
    fi
    
    # 设置当前项目
    gcloud config set project $PROJECT_ID
    log_success "项目设置完成"
}

# 启用必要的 API
enable_apis() {
    log_info "启用必要的 Google Cloud API..."
    
    apis=(
        "appengine.googleapis.com"
        "cloudbuild.googleapis.com"
        "cloudresourcemanager.googleapis.com"
        "bigquery.googleapis.com"
        "monitoring.googleapis.com"
        "logging.googleapis.com"
        "storage.googleapis.com"
        "secretmanager.googleapis.com"
    )
    
    for api in "${apis[@]}"; do
        log_info "启用 API: $api"
        gcloud services enable $api
    done
    
    log_success "API 启用完成"
}

# 创建 App Engine 应用
setup_appengine() {
    log_info "设置 App Engine 应用..."
    
    # 检查 App Engine 应用是否已存在
    if ! gcloud app describe &> /dev/null; then
        log_info "创建 App Engine 应用..."
        gcloud app create --region=$REGION
        log_success "App Engine 应用创建完成"
    else
        log_info "App Engine 应用已存在"
    fi
}

# 准备部署文件
prepare_deployment() {
    log_info "准备部署文件..."
    
    cd $DEPLOY_DIR
    
    # 检查必要文件
    required_files=("app.yaml" "main.py" "requirements.txt" "ops_system_main.py")
    
    for file in "${required_files[@]}"; do
        if [ ! -f "$file" ]; then
            log_error "缺少必要文件: $file"
            exit 1
        fi
    done
    
    # 创建配置目录
    mkdir -p config
    mkdir -p logs
    mkdir -p static
    
    # 检查配置文件是否存在，如果不存在则创建默认配置
    config_files=(
        "config/monitoring_config.json"
        "config/data_quality_config.json"
        "config/concurrency_config.json"
        "config/component_config.json"
        "config/alert_config.json"
    )
    
    for config_file in "${config_files[@]}"; do
        if [ ! -f "$config_file" ]; then
            log_warning "配置文件 $config_file 不存在，将在运行时创建默认配置"
        fi
    done
    
    log_success "部署文件准备完成"
}

# 运行本地测试
run_local_tests() {
    log_info "运行本地测试..."
    
    cd $DEPLOY_DIR
    
    # 安装依赖（如果需要）
    if [ ! -d "venv" ]; then
        log_info "创建虚拟环境..."
        python3 -m venv venv
        source venv/bin/activate
        pip install -r requirements.txt
    else
        source venv/bin/activate
    fi
    
    # 运行基本的导入测试
    log_info "测试模块导入..."
    python3 -c "import ops_system_main; print('✅ ops_system_main 导入成功')"
    python3 -c "import main; print('✅ main 导入成功')"
    
    # 运行系统初始化测试
    log_info "测试系统初始化..."
    python3 -c "
import sys
import os
sys.path.insert(0, os.getcwd())
from ops_system_main import OpsSystemManager
try:
    manager = OpsSystemManager()
    print('✅ 运维系统管理器初始化成功')
except Exception as e:
    print(f'❌ 运维系统管理器初始化失败: {e}')
    sys.exit(1)
"
    
    deactivate
    log_success "本地测试完成"
}

# 部署到 App Engine
deploy_to_appengine() {
    log_info "部署到 App Engine..."
    
    cd $DEPLOY_DIR
    
    # 部署应用
    log_info "开始部署应用..."
    gcloud app deploy app.yaml --quiet --promote
    
    # 获取应用 URL
    APP_URL=$(gcloud app describe --format="value(defaultHostname)")
    
    log_success "部署完成！"
    log_success "应用 URL: https://$APP_URL"
    log_success "监控仪表板: https://$APP_URL/monitoring"
    log_success "健康检查: https://$APP_URL/health"
}

# 部署后验证
post_deployment_verification() {
    log_info "部署后验证..."
    
    APP_URL=$(gcloud app describe --format="value(defaultHostname)")
    
    # 等待应用启动
    log_info "等待应用启动..."
    sleep 30
    
    # 健康检查
    log_info "执行健康检查..."
    if curl -f -s "https://$APP_URL/health" > /dev/null; then
        log_success "健康检查通过"
    else
        log_warning "健康检查失败，请检查应用日志"
        log_info "查看日志: gcloud app logs tail -s $SERVICE_NAME"
    fi
    
    # API 测试
    log_info "测试 API 端点..."
    if curl -f -s "https://$APP_URL/api/status" > /dev/null; then
        log_success "API 端点测试通过"
    else
        log_warning "API 端点测试失败"
    fi
    
    log_success "部署验证完成"
}

# 显示部署信息
show_deployment_info() {
    log_info "部署信息:"
    
    APP_URL=$(gcloud app describe --format="value(defaultHostname)")
    
    echo ""
    echo "🚀 PC28 运维管理系统部署成功！"
    echo ""
    echo "📊 监控仪表板: https://$APP_URL/monitoring"
    echo "🔍 健康检查: https://$APP_URL/health"
    echo "📡 API 状态: https://$APP_URL/api/status"
    echo ""
    echo "📋 管理命令:"
    echo "  查看日志: gcloud app logs tail -s $SERVICE_NAME"
    echo "  查看版本: gcloud app versions list"
    echo "  停止服务: gcloud app versions stop [VERSION]"
    echo "  删除版本: gcloud app versions delete [VERSION]"
    echo ""
    echo "🔧 API 端点:"
    echo "  GET  /api/status - 获取系统状态"
    echo "  POST /api/system/start - 启动系统"
    echo "  POST /api/system/stop - 停止系统"
    echo "  POST /api/data-quality/check - 数据质量检查"
    echo "  POST /api/components/check-updates - 组件更新检查"
    echo "  POST /api/concurrency/tune - 并发参数调优"
    echo "  POST /api/test/e2e - 端到端测试"
    echo ""
}

# 清理函数
cleanup() {
    log_info "清理临时文件..."
    # 这里可以添加清理逻辑
    log_success "清理完成"
}

# 错误处理
error_handler() {
    log_error "部署过程中发生错误！"
    log_info "请检查上面的错误信息并重试"
    cleanup
    exit 1
}

# 设置错误处理
trap error_handler ERR

# 主部署流程
main() {
    log_info "=== PC28 运维管理系统 Google Cloud 部署 ==="
    
    check_dependencies
    check_auth
    setup_project
    enable_apis
    setup_appengine
    prepare_deployment
    run_local_tests
    deploy_to_appengine
    post_deployment_verification
    show_deployment_info
    
    log_success "🎉 部署完成！"
}

# 如果直接运行脚本
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi