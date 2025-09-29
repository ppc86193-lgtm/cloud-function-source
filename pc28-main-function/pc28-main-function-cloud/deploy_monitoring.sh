#!/bin/bash
# PC28监控系统部署脚本
# 自动化部署和配置监控组件

set -e

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

# 检查依赖
check_dependencies() {
    log_info "检查系统依赖..."
    
    # 检查Python3
    if ! command -v python3 &> /dev/null; then
        log_error "未找到Python3，请先安装Python3"
        exit 1
    fi
    log_success "Python3: $(python3 --version)"
    
    # 检查pip3
    if ! command -v pip3 &> /dev/null; then
        log_error "未找到pip3，请先安装pip3"
        exit 1
    fi
    log_success "pip3: $(pip3 --version)"
    
    # 检查必要的Python包
    local required_packages=("psutil" "requests" "asyncio")
    for package in "${required_packages[@]}"; do
        if ! python3 -c "import $package" &> /dev/null; then
            log_warning "缺少Python包: $package，正在安装..."
            pip3 install $package
        fi
    done
    
    log_success "依赖检查完成"
}

# 创建目录结构
setup_directories() {
    log_info "创建监控目录结构..."
    
    local directories=(
        "logs/monitoring"
        "data/monitoring"
        "config/monitoring"
        "backups/monitoring"
        "tmp/monitoring"
    )
    
    for dir in "${directories[@]}"; do
        mkdir -p "$dir"
        log_success "创建目录: $dir"
    done
}

# 配置文件检查和复制
setup_config() {
    log_info "配置监控系统..."
    
    # 检查主配置文件
    if [[ ! -f "config/ops_config.json" ]]; then
        log_error "未找到主配置文件: config/ops_config.json"
        exit 1
    fi
    
    # 复制配置文件到监控目录
    cp config/ops_config.json config/monitoring/
    log_success "复制配置文件到监控目录"
    
    # 检查环境变量文件
    if [[ -f ".env" ]]; then
        log_success "找到环境变量文件: .env"
    else
        log_warning "未找到.env文件，请确保配置了必要的环境变量"
    fi
    
    # 验证关键配置
    log_info "验证监控配置..."
    python3 -c "
import json
import sys

try:
    with open('config/ops_config.json', 'r') as f:
        config = json.load(f)
    
    # 检查必要的配置节点
    required_sections = ['monitoring', 'alerts', 'logging']
    for section in required_sections:
        if section not in config:
            print(f'错误: 配置文件缺少 {section} 节点')
            sys.exit(1)
    
    print('配置文件验证通过')
except Exception as e:
    print(f'配置文件验证失败: {e}')
    sys.exit(1)
"
    
    if [[ $? -eq 0 ]]; then
        log_success "配置文件验证通过"
    else
        log_error "配置文件验证失败"
        exit 1
    fi
}

# 设置文件权限
set_permissions() {
    log_info "设置文件权限..."
    
    # 监控脚本可执行权限
    local scripts=(
        "python/system_monitor.py"
        "python/monitoring_dashboard.py"
        "python/api_monitor.py"
        "python/data_quality_monitor.py"
        "test_monitoring_system.py"
    )
    
    for script in "${scripts[@]}"; do
        if [[ -f "$script" ]]; then
            chmod +x "$script"
            log_success "设置可执行权限: $script"
        else
            log_warning "脚本不存在: $script"
        fi
    done
    
    # 设置日志目录权限
    chmod 755 logs/monitoring
    chmod 755 data/monitoring
    
    log_success "权限设置完成"
}

# 创建systemd服务（Linux系统）
setup_systemd_service() {
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        log_info "配置systemd服务..."
        
        local service_file="/etc/systemd/system/pc28-monitoring.service"
        local current_dir=$(pwd)
        local user=$(whoami)
        
        # 创建服务文件
        sudo tee "$service_file" > /dev/null <<EOF
[Unit]
Description=PC28 Monitoring System
After=network.target
Wants=network.target

[Service]
Type=simple
User=$user
Group=$user
WorkingDirectory=$current_dir
ExecStart=/usr/bin/python3 $current_dir/python/system_monitor.py
ExecReload=/bin/kill -HUP \$MAINPID
Restart=always
RestartSec=10
KillMode=mixed
TimeoutStopSec=30

# 环境变量
Environment=PYTHONPATH=$current_dir
EnvironmentFile=-$current_dir/.env

# 日志配置
StandardOutput=append:$current_dir/logs/monitoring/system_monitor.log
StandardError=append:$current_dir/logs/monitoring/system_monitor_error.log

[Install]
WantedBy=multi-user.target
EOF

        # 重新加载systemd配置
        sudo systemctl daemon-reload
        
        # 启用服务
        sudo systemctl enable pc28-monitoring
        
        log_success "systemd服务配置完成"
        log_info "使用以下命令管理服务:"
        log_info "  启动: sudo systemctl start pc28-monitoring"
        log_info "  停止: sudo systemctl stop pc28-monitoring"
        log_info "  状态: sudo systemctl status pc28-monitoring"
        log_info "  日志: sudo journalctl -u pc28-monitoring -f"
    else
        log_info "非Linux系统，跳过systemd服务配置"
    fi
}

# 创建启动脚本
create_startup_script() {
    log_info "创建启动脚本..."
    
    cat > start_monitoring.sh <<'EOF'
#!/bin/bash
# PC28监控系统启动脚本

set -e

# 检查是否已经运行
if [[ -f "monitoring.pid" ]]; then
    PID=$(cat monitoring.pid)
    if ps -p $PID > /dev/null 2>&1; then
        echo "监控系统已在运行中 (PID: $PID)"
        exit 1
    else
        echo "清理过期的PID文件"
        rm -f monitoring.pid
    fi
fi

# 加载环境变量
if [[ -f ".env" ]]; then
    source .env
fi

# 启动监控系统
echo "启动PC28监控系统..."
nohup python3 python/system_monitor.py > logs/monitoring/system_monitor.log 2>&1 &
MONITOR_PID=$!
echo $MONITOR_PID > monitoring.pid

echo "监控系统已启动"
echo "进程ID: $MONITOR_PID"
echo "日志文件: logs/monitoring/system_monitor.log"
echo "停止监控: ./stop_monitoring.sh"
EOF

    chmod +x start_monitoring.sh
    log_success "创建启动脚本: start_monitoring.sh"
    
    # 创建停止脚本
    cat > stop_monitoring.sh <<'EOF'
#!/bin/bash
# PC28监控系统停止脚本

if [[ -f "monitoring.pid" ]]; then
    PID=$(cat monitoring.pid)
    if ps -p $PID > /dev/null 2>&1; then
        echo "停止监控系统 (PID: $PID)..."
        kill $PID
        
        # 等待进程结束
        for i in {1..10}; do
            if ! ps -p $PID > /dev/null 2>&1; then
                break
            fi
            sleep 1
        done
        
        # 强制结束
        if ps -p $PID > /dev/null 2>&1; then
            echo "强制结束进程..."
            kill -9 $PID
        fi
        
        rm -f monitoring.pid
        echo "监控系统已停止"
    else
        echo "监控系统未运行"
        rm -f monitoring.pid
    fi
else
    echo "未找到PID文件，监控系统可能未运行"
fi
EOF

    chmod +x stop_monitoring.sh
    log_success "创建停止脚本: stop_monitoring.sh"
}

# 运行测试
run_tests() {
    log_info "运行监控系统测试..."
    
    if [[ -f "test_monitoring_system.py" ]]; then
        python3 test_monitoring_system.py
        if [[ $? -eq 0 ]]; then
            log_success "监控系统测试通过"
        else
            log_warning "监控系统测试失败，请检查配置"
        fi
    else
        log_warning "未找到测试脚本: test_monitoring_system.py"
    fi
}

# 显示部署总结
show_summary() {
    log_success "PC28监控系统部署完成！"
    echo
    echo "=== 部署总结 ==="
    echo "监控脚本目录: python/"
    echo "配置文件: config/ops_config.json"
    echo "日志目录: logs/monitoring/"
    echo "数据目录: data/monitoring/"
    echo
    echo "=== 启动方式 ==="
    echo "手动启动: ./start_monitoring.sh"
    echo "手动停止: ./stop_monitoring.sh"
    
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        echo "服务启动: sudo systemctl start pc28-monitoring"
        echo "服务停止: sudo systemctl stop pc28-monitoring"
    fi
    
    echo
    echo "=== 监控访问 ==="
    echo "监控仪表板: http://localhost:8080 (如果已启动)"
    echo "实时日志: tail -f logs/monitoring/system_monitor.log"
    echo
    echo "=== 配置提醒 ==="
    echo "1. 请确保配置了.env文件中的必要环境变量"
    echo "2. 请根据实际需求调整config/ops_config.json中的监控阈值"
    echo "3. 请配置告警通道（邮件、Slack、Telegram等）"
    echo "4. 建议定期检查监控日志和系统状态"
    echo
}

# 主函数
main() {
    echo "=== PC28监控系统部署脚本 ==="
    echo "开始部署监控系统..."
    echo
    
    check_dependencies
    setup_directories
    setup_config
    set_permissions
    setup_systemd_service
    create_startup_script
    run_tests
    show_summary
    
    log_success "部署完成！"
}

# 脚本入口
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi