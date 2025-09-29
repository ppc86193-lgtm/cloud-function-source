"""
生成pytest日志合规性验证报告
根据PROJECT_RULES.md合约条款，验证所有日志均通过pytest自动化生成
"""

import os
import json
import sqlite3
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from contract_compliance_logger import ContractComplianceLogger
import logging

logger = logging.getLogger(__name__)

class PytestComplianceReportGenerator:
    """pytest合规性报告生成器"""
    
    def __init__(self):
        self.compliance_logger = ContractComplianceLogger()
        self.report_data = {}
        
    def generate_comprehensive_report(self) -> Dict[str, Any]:
        """生成全面的pytest合规性报告"""
        try:
            logger.info("🔍 开始生成pytest合规性验证报告...")
            
            # 收集基础信息
            self._collect_basic_info()
            
            # 分析pytest日志
            self._analyze_pytest_logs()
            
            # 分析违规记录
            self._analyze_violations()
            
            # 分析审计日志
            self._analyze_audit_logs()
            
            # 生成合规性评估
            self._generate_compliance_assessment()
            
            # 生成建议和行动计划
            self._generate_recommendations()
            
            # 保存报告
            self._save_report()
            
            logger.info("✅ pytest合规性验证报告生成完成")
            
            return self.report_data
            
        except Exception as e:
            logger.error(f"生成pytest合规性报告失败: {e}")
            raise
    
    def _collect_basic_info(self):
        """收集基础信息"""
        self.report_data.update({
            'report_metadata': {
                'generated_at': datetime.now(timezone.utc).isoformat(),
                'report_version': '1.0',
                'contract_version': self.compliance_logger.contract_version,
                'report_type': 'pytest_compliance_verification',
                'generator': 'PytestComplianceReportGenerator'
            },
            'project_info': {
                'working_directory': os.getcwd(),
                'database_path': self.compliance_logger.db_path,
                'database_exists': os.path.exists(self.compliance_logger.db_path)
            }
        })
    
    def _analyze_pytest_logs(self):
        """分析pytest日志"""
        try:
            if not os.path.exists(self.compliance_logger.db_path):
                self.report_data['pytest_logs_analysis'] = {
                    'total_pytest_logs': 0,
                    'error': 'Database not found'
                }
                return
            
            with sqlite3.connect(self.compliance_logger.db_path) as conn:
                cursor = conn.cursor()
                
                # 统计pytest日志总数
                cursor.execute("SELECT COUNT(*) FROM pytest_logs")
                total_pytest_logs = cursor.fetchone()[0]
                
                # 按测试结果统计
                cursor.execute("""
                    SELECT test_result, COUNT(*) 
                    FROM pytest_logs 
                    GROUP BY test_result
                """)
                result_stats = dict(cursor.fetchall())
                
                # 按日期统计
                cursor.execute("""
                    SELECT DATE(timestamp) as date, COUNT(*) 
                    FROM pytest_logs 
                    GROUP BY DATE(timestamp) 
                    ORDER BY date DESC 
                    LIMIT 7
                """)
                daily_stats = dict(cursor.fetchall())
                
                # 获取最近的pytest日志
                cursor.execute("""
                    SELECT test_name, test_result, timestamp, pytest_version
                    FROM pytest_logs 
                    ORDER BY timestamp DESC 
                    LIMIT 10
                """)
                recent_logs = [
                    {
                        'test_name': row[0],
                        'test_result': row[1],
                        'timestamp': row[2],
                        'pytest_version': row[3]
                    }
                    for row in cursor.fetchall()
                ]
                
                # 统计pytest版本分布
                cursor.execute("""
                    SELECT pytest_version, COUNT(*) 
                    FROM pytest_logs 
                    GROUP BY pytest_version
                """)
                version_stats = dict(cursor.fetchall())
                
                self.report_data['pytest_logs_analysis'] = {
                    'total_pytest_logs': total_pytest_logs,
                    'result_statistics': result_stats,
                    'daily_statistics': daily_stats,
                    'recent_logs': recent_logs,
                    'pytest_version_distribution': version_stats,
                    'compliance_status': 'COMPLIANT' if total_pytest_logs > 0 else 'NO_LOGS'
                }
                
        except Exception as e:
            logger.error(f"分析pytest日志失败: {e}")
            self.report_data['pytest_logs_analysis'] = {
                'error': str(e)
            }
    
    def _analyze_violations(self):
        """分析违规记录"""
        try:
            if not os.path.exists(self.compliance_logger.db_path):
                self.report_data['violations_analysis'] = {
                    'total_violations': 0,
                    'error': 'Database not found'
                }
                return
            
            with sqlite3.connect(self.compliance_logger.db_path) as conn:
                cursor = conn.cursor()
                
                # 统计违规总数
                cursor.execute("SELECT COUNT(*) FROM contract_violations")
                total_violations = cursor.fetchone()[0]
                
                # 按违规类型统计
                cursor.execute("""
                    SELECT violation_type, COUNT(*) 
                    FROM contract_violations 
                    GROUP BY violation_type
                """)
                type_stats = dict(cursor.fetchall())
                
                # 按严重程度统计
                cursor.execute("""
                    SELECT severity, COUNT(*) 
                    FROM contract_violations 
                    GROUP BY severity
                """)
                severity_stats = dict(cursor.fetchall())
                
                # 统计pytest验证状态
                cursor.execute("""
                    SELECT pytest_validated, COUNT(*) 
                    FROM contract_violations 
                    GROUP BY pytest_validated
                """)
                pytest_validation_stats = dict(cursor.fetchall())
                
                # 获取非pytest相关的违规
                cursor.execute("""
                    SELECT violation_id, title, violation_type, severity, detected_at
                    FROM contract_violations 
                    WHERE violation_type IN ('manual_log_creation', 'non_pytest_log', 'pytest_validation_failure')
                    ORDER BY detected_at DESC 
                    LIMIT 20
                """)
                pytest_related_violations = [
                    {
                        'violation_id': row[0],
                        'title': row[1],
                        'violation_type': row[2],
                        'severity': row[3],
                        'detected_at': row[4]
                    }
                    for row in cursor.fetchall()
                ]
                
                # 计算赔偿总额
                cursor.execute("SELECT SUM(compensation_amount) FROM contract_violations")
                total_compensation = cursor.fetchone()[0] or 0
                
                self.report_data['violations_analysis'] = {
                    'total_violations': total_violations,
                    'violation_type_statistics': type_stats,
                    'severity_statistics': severity_stats,
                    'pytest_validation_statistics': pytest_validation_stats,
                    'pytest_related_violations': pytest_related_violations,
                    'total_compensation_amount': total_compensation,
                    'compliance_impact': self._assess_violation_impact(total_violations, type_stats)
                }
                
        except Exception as e:
            logger.error(f"分析违规记录失败: {e}")
            self.report_data['violations_analysis'] = {
                'error': str(e)
            }
    
    def _analyze_audit_logs(self):
        """分析审计日志"""
        try:
            if not os.path.exists(self.compliance_logger.db_path):
                self.report_data['audit_logs_analysis'] = {
                    'total_audit_logs': 0,
                    'error': 'Database not found'
                }
                return
            
            with sqlite3.connect(self.compliance_logger.db_path) as conn:
                cursor = conn.cursor()
                
                # 统计审计日志总数
                cursor.execute("SELECT COUNT(*) FROM audit_logs")
                total_audit_logs = cursor.fetchone()[0]
                
                # 按pytest上下文统计
                cursor.execute("""
                    SELECT pytest_context, COUNT(*) 
                    FROM audit_logs 
                    GROUP BY pytest_context
                """)
                pytest_context_stats = dict(cursor.fetchall())
                
                # 按操作类型统计
                cursor.execute("""
                    SELECT operation_type, COUNT(*) 
                    FROM audit_logs 
                    GROUP BY operation_type
                """)
                operation_type_stats = dict(cursor.fetchall())
                
                # 获取非pytest上下文的审计日志
                cursor.execute("""
                    SELECT operation_type, operation_details, operator, timestamp
                    FROM audit_logs 
                    WHERE pytest_context = 0 OR pytest_context IS NULL
                    ORDER BY timestamp DESC 
                    LIMIT 10
                """)
                non_pytest_audit_logs = [
                    {
                        'operation_type': row[0],
                        'operation_details': row[1],
                        'operator': row[2],
                        'timestamp': row[3]
                    }
                    for row in cursor.fetchall()
                ]
                
                self.report_data['audit_logs_analysis'] = {
                    'total_audit_logs': total_audit_logs,
                    'pytest_context_statistics': pytest_context_stats,
                    'operation_type_statistics': operation_type_stats,
                    'non_pytest_audit_logs': non_pytest_audit_logs,
                    'pytest_compliance_rate': self._calculate_pytest_compliance_rate(pytest_context_stats)
                }
                
        except Exception as e:
            logger.error(f"分析审计日志失败: {e}")
            self.report_data['audit_logs_analysis'] = {
                'error': str(e)
            }
    
    def _assess_violation_impact(self, total_violations: int, type_stats: Dict[str, int]) -> str:
        """评估违规影响"""
        if total_violations == 0:
            return "COMPLIANT"
        
        pytest_related_violations = (
            type_stats.get('manual_log_creation', 0) +
            type_stats.get('non_pytest_log', 0) +
            type_stats.get('pytest_validation_failure', 0)
        )
        
        if pytest_related_violations > 10:
            return "CRITICAL_NON_COMPLIANCE"
        elif pytest_related_violations > 5:
            return "HIGH_NON_COMPLIANCE"
        elif pytest_related_violations > 0:
            return "MODERATE_NON_COMPLIANCE"
        else:
            return "MINOR_NON_COMPLIANCE"
    
    def _calculate_pytest_compliance_rate(self, pytest_context_stats: Dict[str, int]) -> float:
        """计算pytest合规率"""
        total_logs = sum(pytest_context_stats.values())
        if total_logs == 0:
            return 0.0
        
        pytest_logs = pytest_context_stats.get(1, 0) + pytest_context_stats.get(True, 0)
        return (pytest_logs / total_logs) * 100
    
    def _generate_compliance_assessment(self):
        """生成合规性评估"""
        try:
            pytest_logs = self.report_data.get('pytest_logs_analysis', {})
            violations = self.report_data.get('violations_analysis', {})
            audit_logs = self.report_data.get('audit_logs_analysis', {})
            
            # 计算总体合规性得分
            score = 100
            
            # pytest日志存在性 (30分)
            if pytest_logs.get('total_pytest_logs', 0) == 0:
                score -= 30
            elif pytest_logs.get('total_pytest_logs', 0) < 10:
                score -= 15
            
            # 违规情况 (40分)
            total_violations = violations.get('total_violations', 0)
            if total_violations > 20:
                score -= 40
            elif total_violations > 10:
                score -= 30
            elif total_violations > 5:
                score -= 20
            elif total_violations > 0:
                score -= 10
            
            # pytest合规率 (30分)
            compliance_rate = audit_logs.get('pytest_compliance_rate', 0)
            if compliance_rate < 50:
                score -= 30
            elif compliance_rate < 70:
                score -= 20
            elif compliance_rate < 90:
                score -= 10
            
            # 确定合规等级
            if score >= 90:
                compliance_grade = "A - 优秀"
                compliance_status = "FULLY_COMPLIANT"
            elif score >= 80:
                compliance_grade = "B - 良好"
                compliance_status = "MOSTLY_COMPLIANT"
            elif score >= 70:
                compliance_grade = "C - 一般"
                compliance_status = "PARTIALLY_COMPLIANT"
            elif score >= 60:
                compliance_grade = "D - 较差"
                compliance_status = "POORLY_COMPLIANT"
            else:
                compliance_grade = "F - 不合规"
                compliance_status = "NON_COMPLIANT"
            
            self.report_data['compliance_assessment'] = {
                'overall_score': score,
                'compliance_grade': compliance_grade,
                'compliance_status': compliance_status,
                'assessment_criteria': {
                    'pytest_logs_existence': 30,
                    'violation_impact': 40,
                    'pytest_compliance_rate': 30
                },
                'key_findings': self._generate_key_findings()
            }
            
        except Exception as e:
            logger.error(f"生成合规性评估失败: {e}")
            self.report_data['compliance_assessment'] = {
                'error': str(e)
            }
    
    def _generate_key_findings(self) -> List[str]:
        """生成关键发现"""
        findings = []
        
        pytest_logs = self.report_data.get('pytest_logs_analysis', {})
        violations = self.report_data.get('violations_analysis', {})
        audit_logs = self.report_data.get('audit_logs_analysis', {})
        
        # pytest日志相关发现
        total_pytest_logs = pytest_logs.get('total_pytest_logs', 0)
        if total_pytest_logs > 0:
            findings.append(f"✅ 系统已记录 {total_pytest_logs} 条pytest自动化日志")
        else:
            findings.append("❌ 系统中未发现pytest自动化日志")
        
        # 违规相关发现
        total_violations = violations.get('total_violations', 0)
        if total_violations > 0:
            findings.append(f"⚠️ 发现 {total_violations} 个合约违规记录")
            
            type_stats = violations.get('violation_type_statistics', {})
            if 'manual_log_creation' in type_stats:
                findings.append(f"❌ 发现 {type_stats['manual_log_creation']} 个手动日志创建违规")
            if 'non_pytest_log' in type_stats:
                findings.append(f"❌ 发现 {type_stats['non_pytest_log']} 个非pytest日志违规")
        else:
            findings.append("✅ 未发现合约违规记录")
        
        # 合规率相关发现
        compliance_rate = audit_logs.get('pytest_compliance_rate', 0)
        if compliance_rate >= 90:
            findings.append(f"✅ pytest合规率达到 {compliance_rate:.1f}%，表现优秀")
        elif compliance_rate >= 70:
            findings.append(f"⚠️ pytest合规率为 {compliance_rate:.1f}%，需要改进")
        else:
            findings.append(f"❌ pytest合规率仅为 {compliance_rate:.1f}%，严重不合规")
        
        return findings
    
    def _generate_recommendations(self):
        """生成建议和行动计划"""
        try:
            recommendations = []
            action_plan = []
            
            pytest_logs = self.report_data.get('pytest_logs_analysis', {})
            violations = self.report_data.get('violations_analysis', {})
            audit_logs = self.report_data.get('audit_logs_analysis', {})
            
            # 基于pytest日志情况的建议
            if pytest_logs.get('total_pytest_logs', 0) == 0:
                recommendations.append("立即实施pytest自动化测试，确保所有日志通过pytest生成")
                action_plan.append("1. 配置pytest环境和合规性插件")
                action_plan.append("2. 编写pytest测试用例覆盖所有功能模块")
                action_plan.append("3. 集成pytest到CI/CD流程")
            
            # 基于违规情况的建议
            total_violations = violations.get('total_violations', 0)
            if total_violations > 0:
                recommendations.append("修复所有已识别的合约违规，特别是pytest相关违规")
                action_plan.append("4. 审查并修复所有手动日志创建违规")
                action_plan.append("5. 确保所有日志记录通过pytest自动化系统")
                action_plan.append("6. 实施自动化违规检测和预防机制")
            
            # 基于合规率的建议
            compliance_rate = audit_logs.get('pytest_compliance_rate', 0)
            if compliance_rate < 90:
                recommendations.append("提高pytest合规率，确保所有操作在pytest上下文中执行")
                action_plan.append("7. 强制所有测试和日志操作通过pytest执行")
                action_plan.append("8. 实施pytest上下文验证机制")
                action_plan.append("9. 定期监控和报告pytest合规性状态")
            
            # 通用建议
            recommendations.extend([
                "建立定期的pytest合规性审计机制",
                "实施实时监控和告警系统",
                "提供团队pytest合规性培训",
                "建立pytest最佳实践文档"
            ])
            
            action_plan.extend([
                "10. 建立每日pytest合规性检查流程",
                "11. 配置实时违规告警系统",
                "12. 制定pytest合规性培训计划",
                "13. 编写pytest合规性操作手册"
            ])
            
            self.report_data['recommendations'] = {
                'immediate_actions': recommendations[:3],
                'long_term_improvements': recommendations[3:],
                'detailed_action_plan': action_plan,
                'priority_level': self._determine_priority_level(total_violations, compliance_rate)
            }
            
        except Exception as e:
            logger.error(f"生成建议失败: {e}")
            self.report_data['recommendations'] = {
                'error': str(e)
            }
    
    def _determine_priority_level(self, total_violations: int, compliance_rate: float) -> str:
        """确定优先级别"""
        if total_violations > 10 or compliance_rate < 50:
            return "CRITICAL - 立即行动"
        elif total_violations > 5 or compliance_rate < 70:
            return "HIGH - 本周内完成"
        elif total_violations > 0 or compliance_rate < 90:
            return "MEDIUM - 本月内完成"
        else:
            return "LOW - 持续改进"
    
    def _save_report(self):
        """保存报告"""
        try:
            # 生成报告文件名
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_filename = f"pytest_compliance_report_{timestamp}.json"
            
            # 保存JSON格式报告
            with open(report_filename, 'w', encoding='utf-8') as f:
                json.dump(self.report_data, f, indent=2, ensure_ascii=False)
            
            # 生成简化的文本报告
            text_report_filename = f"pytest_compliance_summary_{timestamp}.txt"
            self._generate_text_summary(text_report_filename)
            
            # 记录报告生成
            self.compliance_logger._log_audit_operation(
                operation_type="PYTEST_COMPLIANCE_REPORT_GENERATED",
                operation_details=f"pytest合规性报告已生成: {report_filename}",
                operator="AUTOMATED_REPORT_GENERATOR",
                pytest_context=False
            )
            
            self.report_data['report_files'] = {
                'json_report': report_filename,
                'text_summary': text_report_filename
            }
            
            logger.info(f"📄 报告已保存: {report_filename}")
            
        except Exception as e:
            logger.error(f"保存报告失败: {e}")
    
    def _generate_text_summary(self, filename: str):
        """生成文本摘要报告"""
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write("=" * 60 + "\\n")
                f.write("pytest合规性验证报告摘要\\n")
                f.write("=" * 60 + "\\n\\n")
                
                # 基本信息
                metadata = self.report_data.get('report_metadata', {})
                f.write(f"报告生成时间: {metadata.get('generated_at', 'N/A')}\\n")
                f.write(f"合约版本: {metadata.get('contract_version', 'N/A')}\\n\\n")
                
                # 合规性评估
                assessment = self.report_data.get('compliance_assessment', {})
                f.write("合规性评估:\\n")
                f.write(f"  总体得分: {assessment.get('overall_score', 'N/A')}/100\\n")
                f.write(f"  合规等级: {assessment.get('compliance_grade', 'N/A')}\\n")
                f.write(f"  合规状态: {assessment.get('compliance_status', 'N/A')}\\n\\n")
                
                # 关键发现
                key_findings = assessment.get('key_findings', [])
                if key_findings:
                    f.write("关键发现:\\n")
                    for finding in key_findings:
                        f.write(f"  {finding}\\n")
                    f.write("\\n")
                
                # 建议
                recommendations = self.report_data.get('recommendations', {})
                immediate_actions = recommendations.get('immediate_actions', [])
                if immediate_actions:
                    f.write("立即行动建议:\\n")
                    for i, action in enumerate(immediate_actions, 1):
                        f.write(f"  {i}. {action}\\n")
                    f.write("\\n")
                
                # 优先级
                priority = recommendations.get('priority_level', 'N/A')
                f.write(f"优先级: {priority}\\n")
                
        except Exception as e:
            logger.error(f"生成文本摘要失败: {e}")


def main():
    """主函数 - 生成pytest合规性报告"""
    try:
        print("📊 开始生成pytest合规性验证报告...")
        
        generator = PytestComplianceReportGenerator()
        report = generator.generate_comprehensive_report()
        
        # 输出摘要
        assessment = report.get('compliance_assessment', {})
        print(f"\\n📋 报告摘要:")
        print(f"合规得分: {assessment.get('overall_score', 'N/A')}/100")
        print(f"合规等级: {assessment.get('compliance_grade', 'N/A')}")
        print(f"合规状态: {assessment.get('compliance_status', 'N/A')}")
        
        # 输出关键发现
        key_findings = assessment.get('key_findings', [])
        if key_findings:
            print(f"\\n🔍 关键发现:")
            for finding in key_findings[:5]:  # 只显示前5个
                print(f"  {finding}")
        
        # 输出报告文件
        report_files = report.get('report_files', {})
        if report_files:
            print(f"\\n📄 报告文件:")
            print(f"  详细报告: {report_files.get('json_report', 'N/A')}")
            print(f"  摘要报告: {report_files.get('text_summary', 'N/A')}")
        
        print("\\n✅ pytest合规性验证报告生成完成")
        
    except Exception as e:
        print(f"❌ 生成报告失败: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)