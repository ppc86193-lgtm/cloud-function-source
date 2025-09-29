#!/usr/bin/env python3
"""
PC28统一修复工作流系统
整合所有修复功能，实现一键修复和自动化监控
"""

import json
import subprocess
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class UnifiedRepairWorkflow:
    """统一修复工作流系统"""
    
    def __init__(self):
        self.project_id = "wprojectl"
        self.dataset_id = "pc28_lab"
        self.repair_history = []
        self.critical_tables = [
            'p_cloud_today_v',
            'p_map_today_v', 
            'p_map_today_canon_v',
            'p_size_today_v',
            'p_size_today_canon_v',
            'signal_pool_union_v3',
            'lab_push_candidates_v2',
            'ensemble_pool_today_v2'
        ]
        
    def run_bq_query(self, query: str) -> Dict[str, Any]:
        """执行BigQuery查询"""
        try:
            cmd = ['bq', 'query', '--use_legacy_sql=false', '--format=json', query]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            return {"success": True, "data": json.loads(result.stdout) if result.stdout.strip() else []}
        except subprocess.CalledProcessError as e:
            logger.error(f"BigQuery查询失败: {e.stderr}")
            return {"success": False, "error": e.stderr}
        except json.JSONDecodeError as e:
            logger.error(f"JSON解析失败: {e}")
            return {"success": False, "error": str(e)}
    
    def check_system_health(self) -> Dict[str, Any]:
        """检查系统健康状态"""
        logger.info("🔍 检查系统健康状态...")
        health_report = {
            "timestamp": datetime.now().isoformat(),
            "overall_status": "healthy",
            "issues": [],
            "table_status": {},
            "data_freshness": {},
            "recommendations": []
        }
        
        # 检查关键表状态
        for table in self.critical_tables:
            status = self._check_table_health(table)
            health_report["table_status"][table] = status
            
            if not status["healthy"]:
                health_report["issues"].append({
                    "table": table,
                    "issue": status["issue"],
                    "severity": status["severity"]
                })
                
        # 检查数据新鲜度
        freshness_check = self._check_data_freshness()
        health_report["data_freshness"] = freshness_check
        
        # 生成修复建议
        health_report["recommendations"] = self._generate_recommendations(health_report)
        
        # 确定整体状态
        if health_report["issues"]:
            critical_issues = [i for i in health_report["issues"] if i["severity"] == "critical"]
            health_report["overall_status"] = "critical" if critical_issues else "warning"
            
        return health_report
    
    def _check_table_health(self, table_name: str) -> Dict[str, Any]:
        """检查单个表的健康状态"""
        # 检查表是否存在
        query = f"SELECT COUNT(*) as count FROM `{self.project_id}.{self.dataset_id}.{table_name}`"
        result = self.run_bq_query(query)
        
        if not result["success"]:
            return {
                "healthy": False,
                "issue": "table_not_accessible",
                "severity": "critical",
                "details": result["error"]
            }
        
        row_count = int(result["data"][0]["count"]) if result["data"] else 0
        
        if row_count == 0:
            return {
                "healthy": False,
                "issue": "no_data",
                "severity": "critical" if "today" in table_name else "warning",
                "row_count": row_count
            }
        
        return {
            "healthy": True,
            "row_count": row_count
        }
    
    def _check_data_freshness(self) -> Dict[str, Any]:
        """检查数据新鲜度"""
        freshness_report = {}
        
        # 检查主要数据表的最新数据时间
        tables_to_check = [
            'p_cloud_today_v',
            'p_map_today_v',
            'p_size_today_v',
            'signal_pool_union_v3'
        ]
        
        for table in tables_to_check:
            query = f"""
            SELECT 
                MAX(DATE(ts_utc, 'Asia/Shanghai')) as latest_date,
                COUNT(*) as total_rows,
                CURRENT_DATE('Asia/Shanghai') as today
            FROM `{self.project_id}.{self.dataset_id}.{table}`
            """
            
            result = self.run_bq_query(query)
            if result["success"] and result["data"]:
                data = result["data"][0]
                latest_date = data.get("latest_date")
                today = data.get("today")
                
                if latest_date and today:
                    days_behind = (datetime.strptime(today, "%Y-%m-%d") - 
                                 datetime.strptime(latest_date, "%Y-%m-%d")).days
                    
                    freshness_report[table] = {
                        "latest_date": latest_date,
                        "days_behind": days_behind,
                        "total_rows": int(data.get("total_rows", 0)),
                        "status": "fresh" if days_behind <= 1 else "stale"
                    }
        
        return freshness_report
    
    def _generate_recommendations(self, health_report: Dict[str, Any]) -> List[str]:
        """生成修复建议"""
        recommendations = []
        
        # 基于问题生成建议
        for issue in health_report["issues"]:
            table = issue["table"]
            issue_type = issue["issue"]
            
            if issue_type == "no_data":
                if "today" in table:
                    recommendations.append(f"运行数据采集修复: {table}")
                else:
                    recommendations.append(f"检查数据源: {table}")
            elif issue_type == "table_not_accessible":
                recommendations.append(f"修复表结构: {table}")
        
        # 基于数据新鲜度生成建议
        for table, freshness in health_report["data_freshness"].items():
            if freshness["status"] == "stale":
                recommendations.append(f"更新过期数据: {table} (落后{freshness['days_behind']}天)")
        
        return recommendations
    
    def auto_repair(self) -> Dict[str, Any]:
        """自动修复系统问题"""
        logger.info("🔧 开始自动修复...")
        
        # 先检查系统状态
        health_report = self.check_system_health()
        
        repair_report = {
            "timestamp": datetime.now().isoformat(),
            "health_check": health_report,
            "repairs_attempted": [],
            "repairs_successful": [],
            "repairs_failed": [],
            "final_status": {}
        }
        
        # 执行修复操作
        if health_report["overall_status"] != "healthy":
            logger.info(f"发现 {len(health_report['issues'])} 个问题，开始修复...")
            
            # 1. 修复数据采集问题
            if self._needs_data_collection_repair(health_report):
                repair_result = self._repair_data_collection()
                repair_report["repairs_attempted"].append("data_collection")
                if repair_result["success"]:
                    repair_report["repairs_successful"].append("data_collection")
                else:
                    repair_report["repairs_failed"].append({
                        "type": "data_collection",
                        "error": repair_result["error"]
                    })
            
            # 2. 修复视图依赖问题
            if self._needs_view_repair(health_report):
                repair_result = self._repair_views()
                repair_report["repairs_attempted"].append("view_repair")
                if repair_result["success"]:
                    repair_report["repairs_successful"].append("view_repair")
                else:
                    repair_report["repairs_failed"].append({
                        "type": "view_repair", 
                        "error": repair_result["error"]
                    })
            
            # 3. 修复参数配置问题
            if self._needs_params_repair(health_report):
                repair_result = self._repair_runtime_params()
                repair_report["repairs_attempted"].append("params_repair")
                if repair_result["success"]:
                    repair_report["repairs_successful"].append("params_repair")
                else:
                    repair_report["repairs_failed"].append({
                        "type": "params_repair",
                        "error": repair_result["error"]
                    })
        
        # 再次检查系统状态
        final_health = self.check_system_health()
        repair_report["final_status"] = final_health
        
        # 保存修复报告
        self._save_repair_report(repair_report)
        
        return repair_report
    
    def _needs_data_collection_repair(self, health_report: Dict[str, Any]) -> bool:
        """判断是否需要数据采集修复"""
        for table, freshness in health_report["data_freshness"].items():
            if freshness["status"] == "stale" and freshness["days_behind"] > 1:
                return True
        return False
    
    def _needs_view_repair(self, health_report: Dict[str, Any]) -> bool:
        """判断是否需要视图修复"""
        for issue in health_report["issues"]:
            if issue["issue"] == "table_not_accessible":
                return True
        return False
    
    def _needs_params_repair(self, health_report: Dict[str, Any]) -> bool:
        """判断是否需要参数修复"""
        # 检查lab_push_candidates_v2是否有数据
        candidates_status = health_report["table_status"].get("lab_push_candidates_v2", {})
        return not candidates_status.get("healthy", False)
    
    def _repair_data_collection(self) -> Dict[str, Any]:
        """修复数据采集问题"""
        try:
            logger.info("修复数据采集...")
            
            # 运行数据采集修复脚本
            result = subprocess.run(
                ['python', 'data_collection_repair.py'],
                capture_output=True,
                text=True,
                check=True
            )
            
            return {"success": True, "output": result.stdout}
            
        except subprocess.CalledProcessError as e:
            logger.error(f"数据采集修复失败: {e.stderr}")
            return {"success": False, "error": e.stderr}
    
    def _repair_views(self) -> Dict[str, Any]:
        """修复视图依赖问题"""
        try:
            logger.info("修复视图依赖...")
            
            # 运行综合修复系统
            result = subprocess.run(
                ['python', 'comprehensive_repair_system.py'],
                capture_output=True,
                text=True,
                check=True
            )
            
            return {"success": True, "output": result.stdout}
            
        except subprocess.CalledProcessError as e:
            logger.error(f"视图修复失败: {e.stderr}")
            return {"success": False, "error": e.stderr}
    
    def _repair_runtime_params(self) -> Dict[str, Any]:
        """修复运行时参数"""
        try:
            logger.info("修复运行时参数...")
            
            # 确保pc28市场参数存在
            query = """
            INSERT INTO `wprojectl.pc28_lab.runtime_params` 
            (market, p_min_base, ev_min, max_kelly, target_acc, target_cov)
            SELECT 'pc28', 0.55, 1.0E-6, 0.05, 0.8, 0.5
            WHERE NOT EXISTS (
                SELECT 1 FROM `wprojectl.pc28_lab.runtime_params` 
                WHERE market = 'pc28'
            )
            """
            
            result = self.run_bq_query(query)
            return result
            
        except Exception as e:
            logger.error(f"参数修复失败: {e}")
            return {"success": False, "error": str(e)}
    
    def _save_repair_report(self, report: Dict[str, Any]):
        """保存修复报告"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"unified_repair_report_{timestamp}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        logger.info(f"修复报告已保存: {filename}")
    
    def run_tests(self) -> Dict[str, Any]:
        """运行完整测试套件"""
        logger.info("🧪 运行测试套件...")
        
        try:
            result = subprocess.run(
                ['python', 'pc28_data_flow_test.py'],
                capture_output=True,
                text=True,
                check=True
            )
            
            return {"success": True, "output": result.stdout}
            
        except subprocess.CalledProcessError as e:
            logger.error(f"测试运行失败: {e.stderr}")
            return {"success": False, "error": e.stderr}
    
    def start_monitoring(self, interval_minutes: int = 30):
        """启动监控任务"""
        logger.info(f"🔍 启动监控任务，检查间隔: {interval_minutes}分钟")
        
        while True:
            try:
                # 检查系统健康状态
                health_report = self.check_system_health()
                
                if health_report["overall_status"] != "healthy":
                    logger.warning(f"发现系统问题: {len(health_report['issues'])} 个问题")
                    
                    # 自动修复
                    repair_result = self.auto_repair()
                    
                    if repair_result["repairs_successful"]:
                        logger.info(f"自动修复成功: {repair_result['repairs_successful']}")
                    
                    if repair_result["repairs_failed"]:
                        logger.error(f"自动修复失败: {repair_result['repairs_failed']}")
                else:
                    logger.info("系统状态正常")
                
                # 等待下次检查
                time.sleep(interval_minutes * 60)
                
            except KeyboardInterrupt:
                logger.info("监控任务已停止")
                break
            except Exception as e:
                logger.error(f"监控任务异常: {e}")
                time.sleep(60)  # 异常时等待1分钟后重试

def main():
    """主函数"""
    import sys
    
    workflow = UnifiedRepairWorkflow()
    
    if len(sys.argv) < 2:
        print("用法: python unified_repair_workflow.py <command>")
        print("命令:")
        print("  health    - 检查系统健康状态")
        print("  repair    - 自动修复系统问题")
        print("  test      - 运行测试套件")
        print("  monitor   - 启动监控任务")
        print("  full      - 完整修复流程(健康检查+修复+测试)")
        return
    
    command = sys.argv[1].lower()
    
    if command == "health":
        print("🔍 检查系统健康状态...")
        health_report = workflow.check_system_health()
        print(f"整体状态: {health_report['overall_status']}")
        print(f"发现问题: {len(health_report['issues'])} 个")
        
        if health_report['issues']:
            print("\n问题详情:")
            for issue in health_report['issues']:
                print(f"  - {issue['table']}: {issue['issue']} ({issue['severity']})")
        
        if health_report['recommendations']:
            print("\n修复建议:")
            for rec in health_report['recommendations']:
                print(f"  - {rec}")
    
    elif command == "repair":
        print("🔧 开始自动修复...")
        repair_report = workflow.auto_repair()
        print(f"修复尝试: {len(repair_report['repairs_attempted'])} 个")
        print(f"修复成功: {len(repair_report['repairs_successful'])} 个")
        print(f"修复失败: {len(repair_report['repairs_failed'])} 个")
        
        final_status = repair_report['final_status']['overall_status']
        print(f"最终状态: {final_status}")
    
    elif command == "test":
        print("🧪 运行测试套件...")
        test_result = workflow.run_tests()
        if test_result["success"]:
            print("测试完成")
            print(test_result["output"])
        else:
            print(f"测试失败: {test_result['error']}")
    
    elif command == "monitor":
        interval = int(sys.argv[2]) if len(sys.argv) > 2 else 30
        workflow.start_monitoring(interval)
    
    elif command == "full":
        print("🚀 开始完整修复流程...")
        
        # 1. 健康检查
        print("\n1️⃣ 健康检查...")
        health_report = workflow.check_system_health()
        print(f"发现问题: {len(health_report['issues'])} 个")
        
        # 2. 自动修复
        if health_report['overall_status'] != 'healthy':
            print("\n2️⃣ 自动修复...")
            repair_report = workflow.auto_repair()
            print(f"修复成功: {len(repair_report['repairs_successful'])} 个")
        else:
            print("\n✅ 系统健康，无需修复")
        
        # 3. 运行测试
        print("\n3️⃣ 运行测试...")
        test_result = workflow.run_tests()
        if test_result["success"]:
            print("✅ 测试通过")
        else:
            print(f"❌ 测试失败: {test_result['error']}")
        
        print("\n🎉 完整修复流程完成!")
    
    else:
        print(f"未知命令: {command}")

if __name__ == "__main__":
    main()