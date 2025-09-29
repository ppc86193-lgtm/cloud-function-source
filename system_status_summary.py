#!/usr/bin/env python3
"""
系统状态总结和运行状态检查
检查所有运行中的自动化系统并生成状态报告
"""

import json
import os
import sqlite3
import psutil
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SystemStatusChecker:
    def __init__(self):
        self.project_root = "/Users/a606/cloud_function_source"
        self.local_data_dir = "/Users/a606/cloud_function_source/local_data"
        
    def check_running_processes(self) -> Dict[str, Any]:
        """检查运行中的Python进程"""
        running_processes = {
            "total_python_processes": 0,
            "system_processes": [],
            "resource_usage": {}
        }
        
        try:
            system_scripts = [
                "automated_compliance_checker.py",
                "enhanced_data_flow_system.py", 
                "monitoring_system.py",
                "cloud_to_local_sync_system.py",
                "data_consistency_checker.py"
            ]
            
            for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'cpu_percent', 'memory_percent']):
                try:
                    if proc.info['name'] == 'python3' and proc.info['cmdline']:
                        cmdline = ' '.join(proc.info['cmdline'])
                        
                        for script in system_scripts:
                            if script in cmdline:
                                running_processes["system_processes"].append({
                                    "script": script,
                                    "pid": proc.info['pid'],
                                    "cpu_percent": proc.info['cpu_percent'],
                                    "memory_percent": proc.info['memory_percent'],
                                    "status": "running"
                                })
                                break
                        
                        if 'python3' in cmdline:
                            running_processes["total_python_processes"] += 1
                            
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
            
            # 系统资源使用情况
            running_processes["resource_usage"] = {
                "cpu_percent": psutil.cpu_percent(interval=1),
                "memory_percent": psutil.virtual_memory().percent,
                "disk_usage_percent": psutil.disk_usage('/').percent,
                "disk_free_gb": psutil.disk_usage('/').free / (1024**3)
            }
            
        except Exception as e:
            logger.error(f"检查运行进程失败: {e}")
        
        return running_processes
    
    def check_database_status(self) -> Dict[str, Any]:
        """检查数据库状态"""
        db_status = {
            "databases": [],
            "total_size_mb": 0.0,
            "health_status": "unknown"
        }
        
        try:
            db_files = [
                "monitoring.db",
                "consistency_checks.db", 
                "sync_metadata.db",
                "export_verification.db",
                "ai_analysis.db",
                "pc28_complete_mirror.db"
            ]
            
            for db_file in db_files:
                db_path = os.path.join(self.local_data_dir, db_file)
                
                if os.path.exists(db_path):
                    file_size = os.path.getsize(db_path) / (1024 * 1024)  # MB
                    
                    try:
                        # 尝试连接数据库检查健康状态
                        conn = sqlite3.connect(db_path)
                        cursor = conn.cursor()
                        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                        tables = cursor.fetchall()
                        conn.close()
                        
                        db_status["databases"].append({
                            "name": db_file,
                            "size_mb": file_size,
                            "table_count": len(tables),
                            "status": "healthy"
                        })
                        
                    except Exception as e:
                        db_status["databases"].append({
                            "name": db_file,
                            "size_mb": file_size,
                            "status": "error",
                            "error": str(e)
                        })
                    
                    db_status["total_size_mb"] += file_size
                else:
                    db_status["databases"].append({
                        "name": db_file,
                        "status": "missing"
                    })
            
            # 判断整体健康状态
            healthy_dbs = sum(1 for db in db_status["databases"] if db.get("status") == "healthy")
            total_dbs = len([db for db in db_status["databases"] if db.get("status") != "missing"])
            
            if total_dbs == 0:
                db_status["health_status"] = "no_databases"
            elif healthy_dbs == total_dbs:
                db_status["health_status"] = "excellent"
            elif healthy_dbs >= total_dbs * 0.8:
                db_status["health_status"] = "good"
            else:
                db_status["health_status"] = "needs_attention"
                
        except Exception as e:
            logger.error(f"检查数据库状态失败: {e}")
        
        return db_status
    
    def check_data_freshness(self) -> Dict[str, Any]:
        """检查数据新鲜度"""
        freshness_status = {
            "data_files": [],
            "oldest_file_hours": 0,
            "newest_file_hours": 0,
            "freshness_score": 0
        }
        
        try:
            current_time = datetime.now()
            
            if os.path.exists(self.local_data_dir):
                for file_name in os.listdir(self.local_data_dir):
                    if file_name.endswith('.json') and not file_name.startswith('.'):
                        file_path = os.path.join(self.local_data_dir, file_name)
                        
                        try:
                            file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                            hours_old = (current_time - file_mtime).total_seconds() / 3600
                            
                            # 尝试读取文件获取记录数
                            with open(file_path, 'r', encoding='utf-8') as f:
                                data = json.load(f)
                            
                            record_count = len(data.get("data", []))
                            
                            freshness_status["data_files"].append({
                                "file_name": file_name,
                                "hours_old": hours_old,
                                "record_count": record_count,
                                "last_modified": file_mtime.isoformat(),
                                "freshness": "fresh" if hours_old < 24 else 
                                           "stale" if hours_old < 72 else "very_stale"
                            })
                            
                        except Exception as e:
                            logger.warning(f"处理文件 {file_name} 失败: {e}")
                
                if freshness_status["data_files"]:
                    hours_list = [f["hours_old"] for f in freshness_status["data_files"]]
                    freshness_status["oldest_file_hours"] = max(hours_list)
                    freshness_status["newest_file_hours"] = min(hours_list)
                    
                    # 计算新鲜度分数 (0-100)
                    fresh_files = sum(1 for f in freshness_status["data_files"] if f["freshness"] == "fresh")
                    total_files = len(freshness_status["data_files"])
                    freshness_status["freshness_score"] = (fresh_files / total_files) * 100 if total_files > 0 else 0
                    
        except Exception as e:
            logger.error(f"检查数据新鲜度失败: {e}")
        
        return freshness_status
    
    def check_monitoring_alerts(self) -> Dict[str, Any]:
        """检查监控告警"""
        alert_status = {
            "active_alerts": [],
            "resolved_alerts": [],
            "alert_summary": {}
        }
        
        try:
            monitoring_db_path = os.path.join(self.local_data_dir, "monitoring.db")
            
            if os.path.exists(monitoring_db_path):
                conn = sqlite3.connect(monitoring_db_path)
                cursor = conn.cursor()
                
                # 获取活跃告警
                cursor.execute('''
                    SELECT alert_type, severity, message, alert_time
                    FROM alerts 
                    WHERE resolved = 0 
                    ORDER BY alert_time DESC
                ''')
                
                active_alerts = cursor.fetchall()
                alert_status["active_alerts"] = [
                    {
                        "type": alert[0],
                        "severity": alert[1], 
                        "message": alert[2],
                        "time": alert[3]
                    }
                    for alert in active_alerts
                ]
                
                # 获取最近24小时已解决的告警
                yesterday = (datetime.now() - timedelta(days=1)).isoformat()
                cursor.execute('''
                    SELECT alert_type, severity, message, alert_time, resolved_time
                    FROM alerts 
                    WHERE resolved = 1 AND alert_time > ?
                    ORDER BY resolved_time DESC
                    LIMIT 10
                ''', (yesterday,))
                
                resolved_alerts = cursor.fetchall()
                alert_status["resolved_alerts"] = [
                    {
                        "type": alert[0],
                        "severity": alert[1],
                        "message": alert[2],
                        "alert_time": alert[3],
                        "resolved_time": alert[4]
                    }
                    for alert in resolved_alerts
                ]
                
                # 告警统计
                cursor.execute('''
                    SELECT 
                        COUNT(*) as total_alerts,
                        SUM(CASE WHEN resolved = 0 THEN 1 ELSE 0 END) as active_count,
                        SUM(CASE WHEN severity = 'critical' AND resolved = 0 THEN 1 ELSE 0 END) as critical_count
                    FROM alerts
                ''')
                
                summary = cursor.fetchone()
                if summary:
                    alert_status["alert_summary"] = {
                        "total_alerts": summary[0],
                        "active_alerts": summary[1],
                        "critical_alerts": summary[2]
                    }
                
                conn.close()
                
        except Exception as e:
            logger.error(f"检查监控告警失败: {e}")
        
        return alert_status
    
    def generate_status_summary(self) -> Dict[str, Any]:
        """生成系统状态总结"""
        logger.info("开始生成系统状态总结")
        
        # 收集各项状态
        process_status = self.check_running_processes()
        db_status = self.check_database_status()
        freshness_status = self.check_data_freshness()
        alert_status = self.check_monitoring_alerts()
        
        # 计算整体健康分数
        health_score = 0
        max_score = 100
        
        # 进程状态评分 (25分)
        if len(process_status["system_processes"]) >= 2:
            health_score += 25
        elif len(process_status["system_processes"]) >= 1:
            health_score += 15
        
        # 数据库状态评分 (25分)
        if db_status["health_status"] == "excellent":
            health_score += 25
        elif db_status["health_status"] == "good":
            health_score += 20
        elif db_status["health_status"] == "needs_attention":
            health_score += 10
        
        # 数据新鲜度评分 (25分)
        health_score += min(25, freshness_status["freshness_score"] * 0.25)
        
        # 告警状态评分 (25分)
        critical_alerts = alert_status["alert_summary"].get("critical_alerts", 0)
        active_alerts = alert_status["alert_summary"].get("active_alerts", 0)
        
        if critical_alerts == 0 and active_alerts == 0:
            health_score += 25
        elif critical_alerts == 0 and active_alerts <= 2:
            health_score += 20
        elif critical_alerts <= 1:
            health_score += 10
        
        # 确定整体状态
        if health_score >= 90:
            overall_status = "excellent"
        elif health_score >= 75:
            overall_status = "good"
        elif health_score >= 60:
            overall_status = "acceptable"
        else:
            overall_status = "needs_attention"
        
        # 构建状态总结
        status_summary = {
            "summary_metadata": {
                "generation_time": datetime.now().isoformat(),
                "health_score": health_score,
                "overall_status": overall_status
            },
            "system_processes": process_status,
            "database_status": db_status,
            "data_freshness": freshness_status,
            "monitoring_alerts": alert_status,
            "recommendations": self.generate_recommendations(
                process_status, db_status, freshness_status, alert_status
            )
        }
        
        return status_summary
    
    def generate_recommendations(self, process_status: Dict, db_status: Dict, 
                               freshness_status: Dict, alert_status: Dict) -> List[str]:
        """生成改进建议"""
        recommendations = []
        
        # 进程相关建议
        if len(process_status["system_processes"]) == 0:
            recommendations.append("🚨 没有检测到运行中的系统进程，建议启动监控和同步系统")
        elif len(process_status["system_processes"]) < 2:
            recommendations.append("⚠️ 系统进程数量较少，建议检查所有自动化系统是否正常运行")
        
        # 资源使用建议
        if process_status["resource_usage"]["cpu_percent"] > 80:
            recommendations.append("🔥 CPU使用率过高，建议优化系统性能")
        
        if process_status["resource_usage"]["memory_percent"] > 85:
            recommendations.append("💾 内存使用率过高，建议释放内存或增加系统资源")
        
        if process_status["resource_usage"]["disk_free_gb"] < 5:
            recommendations.append("💿 磁盘空间不足，建议清理临时文件或扩展存储")
        
        # 数据库相关建议
        if db_status["health_status"] == "needs_attention":
            recommendations.append("🗄️ 数据库状态需要关注，建议检查数据库完整性")
        elif db_status["health_status"] == "no_databases":
            recommendations.append("❌ 未发现数据库文件，建议重新初始化系统")
        
        # 数据新鲜度建议
        if freshness_status["freshness_score"] < 50:
            recommendations.append("📅 数据新鲜度较低，建议检查数据同步机制")
        
        if freshness_status["oldest_file_hours"] > 72:
            recommendations.append("⏰ 发现过期数据文件，建议更新数据或清理旧文件")
        
        # 告警相关建议
        critical_alerts = alert_status["alert_summary"].get("critical_alerts", 0)
        if critical_alerts > 0:
            recommendations.append(f"🚨 发现{critical_alerts}个严重告警，需要立即处理")
        
        active_alerts = alert_status["alert_summary"].get("active_alerts", 0)
        if active_alerts > 5:
            recommendations.append(f"⚠️ 活跃告警数量较多({active_alerts}个)，建议批量处理")
        
        # 通用建议
        if not recommendations:
            recommendations.append("✅ 系统运行状态良好，建议继续保持定期监控")
        
        return recommendations

def main():
    """主函数"""
    checker = SystemStatusChecker()
    
    print("=" * 80)
    print("PC28系统状态检查器")
    print("=" * 80)
    
    # 生成状态总结
    status_summary = checker.generate_status_summary()
    
    # 显示总体状态
    metadata = status_summary["summary_metadata"]
    print(f"检查时间: {metadata['generation_time']}")
    print(f"健康分数: {metadata['health_score']}/100")
    print(f"整体状态: {metadata['overall_status']}")
    
    # 显示运行进程
    processes = status_summary["system_processes"]
    print(f"\n🔄 运行中的系统进程: {len(processes['system_processes'])}")
    for proc in processes["system_processes"]:
        print(f"  ✓ {proc['script']} (PID: {proc['pid']}, CPU: {proc['cpu_percent']:.1f}%)")
    
    # 显示资源使用
    resources = processes["resource_usage"]
    print(f"\n💻 系统资源使用:")
    print(f"  CPU: {resources['cpu_percent']:.1f}%")
    print(f"  内存: {resources['memory_percent']:.1f}%")
    print(f"  磁盘: {resources['disk_usage_percent']:.1f}% (剩余: {resources['disk_free_gb']:.1f}GB)")
    
    # 显示数据库状态
    db_status = status_summary["database_status"]
    print(f"\n🗄️ 数据库状态: {db_status['health_status']} (总大小: {db_status['total_size_mb']:.1f}MB)")
    healthy_dbs = sum(1 for db in db_status["databases"] if db.get("status") == "healthy")
    print(f"  健康数据库: {healthy_dbs}/{len(db_status['databases'])}")
    
    # 显示数据新鲜度
    freshness = status_summary["data_freshness"]
    print(f"\n📅 数据新鲜度: {freshness['freshness_score']:.1f}%")
    if freshness["data_files"]:
        fresh_files = sum(1 for f in freshness["data_files"] if f["freshness"] == "fresh")
        print(f"  新鲜文件: {fresh_files}/{len(freshness['data_files'])}")
    
    # 显示告警状态
    alerts = status_summary["monitoring_alerts"]
    alert_summary = alerts["alert_summary"]
    if alert_summary:
        print(f"\n🚨 监控告警:")
        print(f"  活跃告警: {alert_summary.get('active_alerts', 0)}")
        print(f"  严重告警: {alert_summary.get('critical_alerts', 0)}")
    
    # 显示建议
    recommendations = status_summary["recommendations"]
    if recommendations:
        print(f"\n💡 改进建议:")
        for rec in recommendations[:5]:  # 只显示前5个建议
            print(f"  {rec}")
    
    # 保存详细报告
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = f"/Users/a606/cloud_function_source/reports/system_status_{timestamp}.json"
        
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(status_summary, f, ensure_ascii=False, indent=2)
        
        print(f"\n📄 详细报告已保存: {report_path}")
        
    except Exception as e:
        logger.error(f"保存报告失败: {e}")
    
    print("=" * 80)
    print("状态检查完成")
    print("=" * 80)
    
    return status_summary

if __name__ == "__main__":
    main()