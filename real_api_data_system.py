#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
真实API数据获取和写入系统
使用真实的API接口获取数据，结合去重机制安全写入数据库
"""

import hashlib
import json
import requests
import sqlite3
import threading
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from urllib.parse import urlencode
import logging

# 导入去重系统
from data_deduplication_system import DataDeduplicationSystem

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class APIConfig:
    """API配置信息"""
    base_url: str = "https://rijb.api.storeapi.net/api/119/261"
    appid: str = ""  # 用户需要替换为真实的appid
    secret_key: str = ""  # 用户需要替换为真实的密钥
    timeout: int = 30
    retry_times: int = 3
    retry_delay: float = 1.0

@dataclass
class APIResponse:
    """API响应数据结构 - 已优化，移除未使用字段"""
    codeid: Optional[int] = None
    # curtime: 已移除 - 未使用的时间戳字段
    l_alias: Optional[str] = None
    l_exp: Optional[List] = None
    l_issue: Optional[int] = None
    l_logo: Optional[str] = None
    l_name: Optional[str] = None
    l_tdiff: Optional[int] = None
    l_time: Optional[List] = None
    message: Optional[str] = None
    retdata: Optional[List] = None
    x1_: Optional[int] = None

@dataclass
class LotteryRecord:
    """彩票记录数据结构"""
    draw_id: str
    issue: str
    numbers: List[int]
    sum_value: int
    big_small: str
    odd_even: str
    dragon_tiger: str
    timestamp: datetime
    server_time: Optional[str] = None
    next_draw_id: Optional[str] = None
    next_draw_time: Optional[str] = None
    countdown_seconds: Optional[int] = None
    source: str = "real_api"
    raw_data: Optional[Dict] = None

@dataclass
class WriteResult:
    """写入结果"""
    success: bool
    record_id: str
    message: str
    is_duplicate: bool = False
    processing_time: float = 0.0

class RealAPIDataSystem:
    """真实API数据系统"""
    
    def __init__(self, api_config: APIConfig, db_path: str = "lottery_data.db"):
        self.api_config = api_config
        self.db_path = db_path
        self.session = requests.Session()
        self.lock = threading.RLock()
        
        # 初始化去重系统
        self.dedup_system = DataDeduplicationSystem("deduplication.db")
        
        # 统计信息
        self.stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'total_records': 0,
            'written_records': 0,
            'duplicate_records': 0,
            'error_records': 0
        }
        
        self._init_database()
        self._validate_config()
    
    def _validate_config(self):
        """验证API配置"""
        if not self.api_config.appid:
            logger.warning("警告: appid未设置，请替换为真实的appid")
        if not self.api_config.secret_key:
            logger.warning("警告: secret_key未设置，请替换为真实的密钥")
        if len(self.api_config.secret_key) != 32 and self.api_config.secret_key:
            logger.warning("警告: secret_key长度不是32位，请检查密钥格式")
    
    def _init_database(self):
        """初始化数据库"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS lottery_records (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        draw_id TEXT UNIQUE NOT NULL,
                        issue TEXT NOT NULL,
                        numbers TEXT NOT NULL,
                        sum_value INTEGER NOT NULL,
                        big_small TEXT NOT NULL,
                        odd_even TEXT NOT NULL,
                        dragon_tiger TEXT NOT NULL,
                        timestamp DATETIME NOT NULL,
                        server_time TEXT,
                        next_draw_id TEXT,
                        next_draw_time TEXT,
                        countdown_seconds INTEGER,
                        source TEXT DEFAULT 'real_api',
                        raw_data TEXT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_draw_id ON lottery_records(draw_id)
                """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_timestamp ON lottery_records(timestamp)
                """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_issue ON lottery_records(issue)
                """)
                
                conn.commit()
                logger.info("数据库初始化完成")
        except Exception as e:
            logger.error(f"数据库初始化失败: {e}")
            raise
    
    def _generate_sign(self, params: Dict[str, Any]) -> str:
        """生成API签名"""
        try:
            # 按照API文档要求的顺序生成签名
            sign_params = []
            
            # 添加参数（按字母顺序）
            for key in sorted(params.keys()):
                if params[key] is not None and params[key] != '':
                    sign_params.append(f"{key}{params[key]}")
            
            # 添加密钥
            sign_string = ''.join(sign_params) + self.api_config.secret_key
            
            # MD5加密
            return hashlib.md5(sign_string.encode('utf-8')).hexdigest()
        except Exception as e:
            logger.error(f"生成签名失败: {e}")
            return ""
    
    def _make_api_request(self, params: Dict[str, Any]) -> Optional[Dict]:
        """发起API请求"""
        self.stats['total_requests'] += 1
        
        for attempt in range(self.api_config.retry_times):
            try:
                # 添加基础参数
                request_params = {
                    'appid': self.api_config.appid,
                    'format': 'json',
                    'time': str(int(time.time()))
                }
                request_params.update(params)
                
                # 生成签名
                request_params['sign'] = self._generate_sign(request_params)
                
                # 发起请求
                response = self.session.get(
                    self.api_config.base_url,
                    params=request_params,
                    timeout=self.api_config.timeout
                )
                
                response.raise_for_status()
                data = response.json()
                
                # 检查API响应状态
                if data.get('codeid') == 10000:
                    self.stats['successful_requests'] += 1
                    return data
                else:
                    logger.warning(f"API返回错误: {data.get('message', '未知错误')}")
                    if attempt == self.api_config.retry_times - 1:
                        self.stats['failed_requests'] += 1
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"API请求失败 (尝试 {attempt + 1}/{self.api_config.retry_times}): {e}")
                if attempt < self.api_config.retry_times - 1:
                    time.sleep(self.api_config.retry_delay * (attempt + 1))
                else:
                    self.stats['failed_requests'] += 1
            
            except json.JSONDecodeError as e:
                logger.error(f"JSON解析失败: {e}")
                if attempt == self.api_config.retry_times - 1:
                    self.stats['failed_requests'] += 1
        
        return None
    
    def get_current_lottery_data(self) -> Optional[List[LotteryRecord]]:
        """获取当前开奖数据"""
        try:
            logger.info("获取当前开奖数据...")
            
            # 获取最新数据
            params = {'limit': '10'}  # 获取最近10期
            response_data = self._make_api_request(params)
            
            if not response_data:
                return None
            
            return self._parse_lottery_data(response_data)
            
        except Exception as e:
            logger.error(f"获取当前开奖数据失败: {e}")
            return None
    
    def get_history_lottery_data(self, date: str, limit: int = 100) -> Optional[List[LotteryRecord]]:
        """获取历史开奖数据"""
        try:
            logger.info(f"获取历史开奖数据: {date}")
            
            params = {
                'date': date,
                'limit': str(limit)
            }
            response_data = self._make_api_request(params)
            
            if not response_data:
                return None
            
            return self._parse_lottery_data(response_data)
            
        except Exception as e:
            logger.error(f"获取历史开奖数据失败: {e}")
            return None
    
    def _parse_lottery_data(self, response_data: Dict) -> List[LotteryRecord]:
        """解析开奖数据"""
        records = []
        
        try:
            retdata = response_data.get('retdata', [])
            if not isinstance(retdata, list):
                logger.warning("API返回数据格式异常")
                return records
            
            for item in retdata:
                try:
                    # 解析开奖号码
                    numbers = self._parse_numbers(item)
                    if not numbers:
                        continue
                    
                    # 计算衍生字段
                    sum_value = sum(numbers)
                    big_small = 'big' if sum_value >= 14 else 'small'
                    odd_even = 'odd' if sum_value % 2 == 1 else 'even'
                    dragon_tiger = 'dragon' if numbers[0] > numbers[-1] else 'tiger' if numbers[0] < numbers[-1] else 'tie'
                    
                    # 生成draw_id
                    draw_id = self._generate_draw_id(item)
                    
                    # 创建记录
                    record = LotteryRecord(
                        draw_id=draw_id,
                        issue=str(item.get('issue', '')),
                        numbers=numbers,
                        sum_value=sum_value,
                        big_small=big_small,
                        odd_even=odd_even,
                        dragon_tiger=dragon_tiger,
                        timestamp=self._parse_timestamp(item),
                        # server_time=response_data.get('curtime'),  # 已移除 - 优化API响应
                        next_draw_id=item.get('next_issue'),
                        next_draw_time=item.get('next_time'),
                        countdown_seconds=item.get('countdown'),
                        raw_data=item
                    )
                    
                    records.append(record)
                    
                except Exception as e:
                    logger.error(f"解析单条记录失败: {e}, 数据: {item}")
                    continue
            
            logger.info(f"成功解析 {len(records)} 条记录")
            return records
            
        except Exception as e:
            logger.error(f"解析开奖数据失败: {e}")
            return records
    
    def _parse_numbers(self, item: Dict) -> Optional[List[int]]:
        """解析开奖号码"""
        try:
            # 尝试多种可能的字段名
            number_fields = ['numbers', 'result', 'data', 'num', 'lottery_numbers']
            
            for field in number_fields:
                if field in item:
                    numbers_data = item[field]
                    
                    if isinstance(numbers_data, list):
                        return [int(x) for x in numbers_data if str(x).isdigit()]
                    elif isinstance(numbers_data, str):
                        # 尝试解析字符串格式的号码
                        if ',' in numbers_data:
                            return [int(x.strip()) for x in numbers_data.split(',') if x.strip().isdigit()]
                        elif '+' in numbers_data:
                            return [int(x.strip()) for x in numbers_data.split('+') if x.strip().isdigit()]
                        else:
                            # 单个数字或其他格式
                            if numbers_data.isdigit():
                                return [int(numbers_data)]
            
            # 如果没有找到标准字段，尝试从其他字段推断
            for key, value in item.items():
                if isinstance(value, list) and len(value) >= 3:
                    try:
                        return [int(x) for x in value[:3] if str(x).isdigit()]
                    except:
                        continue
            
            logger.warning(f"无法解析开奖号码: {item}")
            return None
            
        except Exception as e:
            logger.error(f"解析号码失败: {e}")
            return None
    
    def _generate_draw_id(self, item: Dict) -> str:
        """生成开奖ID"""
        try:
            # 尝试从多个字段生成ID
            issue = item.get('issue', '')
            date_str = item.get('date', datetime.now().strftime('%Y%m%d'))
            time_str = item.get('time', str(int(time.time())))
            
            if issue:
                return f"PC28_{date_str}_{issue}"
            else:
                return f"PC28_{date_str}_{time_str}"
                
        except Exception as e:
            logger.error(f"生成draw_id失败: {e}")
            return f"PC28_{int(time.time())}"
    
    def _parse_timestamp(self, item: Dict) -> datetime:
        """解析时间戳"""
        try:
            # 尝试多种时间字段
            time_fields = ['timestamp', 'time', 'draw_time', 'open_time', 'created_at']
            
            for field in time_fields:
                if field in item:
                    time_value = item[field]
                    
                    if isinstance(time_value, (int, float)):
                        # Unix时间戳
                        if time_value > 1e10:  # 毫秒时间戳
                            time_value = time_value / 1000
                        return datetime.fromtimestamp(time_value)
                    
                    elif isinstance(time_value, str):
                        # 字符串时间格式
                        try:
                            return datetime.fromisoformat(time_value.replace('Z', '+00:00'))
                        except:
                            try:
                                return datetime.strptime(time_value, '%Y-%m-%d %H:%M:%S')
                            except:
                                continue
            
            # 默认使用当前时间
            return datetime.now()
            
        except Exception as e:
            logger.error(f"解析时间戳失败: {e}")
            return datetime.now()
    
    def write_record(self, record: LotteryRecord) -> WriteResult:
        """写入单条记录"""
        start_time = time.time()
        
        try:
            self.stats['total_records'] += 1
            
            # 转换为字典进行去重检查
            record_dict = {
                'draw_id': record.draw_id,
                'issue': record.issue,
                'numbers': record.numbers,
                'sum_value': record.sum_value,
                'big_small': record.big_small,
                'odd_even': record.odd_even,
                'dragon_tiger': record.dragon_tiger,
                'timestamp': record.timestamp
            }
            
            # 去重检查
            should_write = self.dedup_system.process_record(record_dict, record.draw_id)
            
            if not should_write:
                self.stats['duplicate_records'] += 1
                return WriteResult(
                    success=False,
                    record_id=record.draw_id,
                    message="记录重复，已跳过",
                    is_duplicate=True,
                    processing_time=time.time() - start_time
                )
            
            # 写入数据库
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO lottery_records 
                    (draw_id, issue, numbers, sum_value, big_small, odd_even, dragon_tiger,
                     timestamp, server_time, next_draw_id, next_draw_time, countdown_seconds,
                     source, raw_data, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    record.draw_id,
                    record.issue,
                    json.dumps(record.numbers),
                    record.sum_value,
                    record.big_small,
                    record.odd_even,
                    record.dragon_tiger,
                    record.timestamp,
                    record.server_time,
                    record.next_draw_id,
                    record.next_draw_time,
                    record.countdown_seconds,
                    record.source,
                    json.dumps(record.raw_data) if record.raw_data else None,
                    datetime.now()
                ))
                conn.commit()
            
            self.stats['written_records'] += 1
            
            return WriteResult(
                success=True,
                record_id=record.draw_id,
                message="记录写入成功",
                processing_time=time.time() - start_time
            )
            
        except Exception as e:
            self.stats['error_records'] += 1
            logger.error(f"写入记录失败: {e}")
            
            return WriteResult(
                success=False,
                record_id=record.draw_id,
                message=f"写入失败: {str(e)}",
                processing_time=time.time() - start_time
            )
    
    def batch_write_records(self, records: List[LotteryRecord]) -> List[WriteResult]:
        """批量写入记录"""
        results = []
        
        for record in records:
            result = self.write_record(record)
            results.append(result)
        
        return results
    
    def sync_current_data(self) -> Dict[str, Any]:
        """同步当前数据"""
        try:
            logger.info("开始同步当前数据...")
            
            # 获取数据
            records = self.get_current_lottery_data()
            if not records:
                return {'success': False, 'message': '获取数据失败'}
            
            # 批量写入
            results = self.batch_write_records(records)
            
            # 统计结果
            success_count = sum(1 for r in results if r.success)
            duplicate_count = sum(1 for r in results if r.is_duplicate)
            
            return {
                'success': True,
                'total_records': len(records),
                'written_records': success_count,
                'duplicate_records': duplicate_count,
                'failed_records': len(results) - success_count,
                'processing_time': sum(r.processing_time for r in results)
            }
            
        except Exception as e:
            logger.error(f"同步当前数据失败: {e}")
            return {'success': False, 'message': str(e)}
    
    def sync_history_data(self, start_date: str, end_date: str) -> Dict[str, Any]:
        """同步历史数据"""
        try:
            logger.info(f"开始同步历史数据: {start_date} 到 {end_date}")
            
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            
            total_records = 0
            total_written = 0
            total_duplicates = 0
            total_failed = 0
            
            current_date = start_dt
            while current_date <= end_dt:
                date_str = current_date.strftime('%Y-%m-%d')
                
                # 获取当天数据
                records = self.get_history_lottery_data(date_str)
                if records:
                    # 批量写入
                    results = self.batch_write_records(records)
                    
                    # 统计
                    total_records += len(records)
                    total_written += sum(1 for r in results if r.success)
                    total_duplicates += sum(1 for r in results if r.is_duplicate)
                    total_failed += len(results) - sum(1 for r in results if r.success)
                    
                    logger.info(f"日期 {date_str}: 获取 {len(records)} 条，写入 {sum(1 for r in results if r.success)} 条")
                
                current_date += timedelta(days=1)
                time.sleep(0.5)  # 避免请求过于频繁
            
            return {
                'success': True,
                'date_range': f"{start_date} 到 {end_date}",
                'total_records': total_records,
                'written_records': total_written,
                'duplicate_records': total_duplicates,
                'failed_records': total_failed
            }
            
        except Exception as e:
            logger.error(f"同步历史数据失败: {e}")
            return {'success': False, 'message': str(e)}
    
    def get_system_stats(self) -> Dict[str, Any]:
        """获取系统统计信息"""
        dedup_stats = self.dedup_system.get_statistics()
        
        return {
            'api_stats': self.stats.copy(),
            'deduplication_stats': asdict(dedup_stats),
            'database_stats': self._get_database_stats()
        }
    
    def _get_database_stats(self) -> Dict[str, Any]:
        """获取数据库统计信息"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("SELECT COUNT(*) FROM lottery_records")
                total_records = cursor.fetchone()[0]
                
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM lottery_records 
                    WHERE created_at > datetime('now', '-1 day')
                """)
                recent_records = cursor.fetchone()[0]
                
                cursor = conn.execute("""
                    SELECT MIN(timestamp), MAX(timestamp) FROM lottery_records
                """)
                time_range = cursor.fetchone()
                
                return {
                    'total_records': total_records,
                    'recent_24h_records': recent_records,
                    'earliest_record': time_range[0],
                    'latest_record': time_range[1]
                }
        except Exception as e:
            logger.error(f"获取数据库统计失败: {e}")
            return {}

def main():
    """测试真实API数据系统"""
    print("=== 真实API数据系统测试 ===")
    
    # 从环境变量或配置文件获取API配置
    import os
    
    appid = os.getenv('PC28_API_APPID', 'your_real_appid_here')
    secret_key = os.getenv('PC28_API_SECRET_KEY', 'your_32_character_secret_key_here')
    
    # 配置API
    api_config = APIConfig(
        appid=appid,
        secret_key=secret_key
    )
    
    # 初始化系统
    data_system = RealAPIDataSystem(api_config)
    
    print("\n=== API配置说明 ===")
    print("请通过以下方式之一配置真实的API凭据:")
    print("1. 设置环境变量:")
    print("   export PC28_API_APPID='your_real_appid'")
    print("   export PC28_API_SECRET_KEY='your_32_character_secret_key'")
    print("2. 或在.env文件中添加:")
    print("   PC28_API_APPID=your_real_appid")
    print("   PC28_API_SECRET_KEY=your_32_character_secret_key")
    print("3. 或直接修改本文件中的APIConfig参数")
    print(f"\n当前配置状态:")
    print(f"  appid: {api_config.appid}")
    print(f"  secret_key: {api_config.secret_key[:8] if len(api_config.secret_key) > 8 else '***'}...")
    
    if api_config.appid == "your_real_appid_here" or api_config.secret_key == "your_32_character_secret_key_here":
        print("\n⚠️  警告: 检测到默认配置，请设置真实的API配置后再进行数据同步")
        print("\n如需获取API配置，请联系:")
        print("  - 彩票数据提供商")
        print("  - 系统管理员")
        print("  - 查看项目文档中的API配置说明")
        return
    
    print("\n1. 测试获取当前数据:")
    sync_result = data_system.sync_current_data()
    print(json.dumps(sync_result, indent=2, ensure_ascii=False))
    
    print("\n2. 测试获取历史数据 (最近3天):")
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
    
    history_result = data_system.sync_history_data(start_date, end_date)
    print(json.dumps(history_result, indent=2, ensure_ascii=False))
    
    print("\n3. 系统统计信息:")
    stats = data_system.get_system_stats()
    print(json.dumps(stats, indent=2, ensure_ascii=False, default=str))
    
    print("\n=== 真实API数据系统测试完成 ===")

if __name__ == "__main__":
    main()