#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28实时数据推送和通知系统
负责实时开奖数据的推送、通知和分发
"""

import json
import time
import asyncio
import logging
import threading
from typing import Dict, List, Any, Optional, Callable
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, asdict
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor
from api_field_optimization import OptimizedPC28DataProcessor, OptimizedLotteryData

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class NotificationEvent:
    """通知事件数据结构"""
    event_type: str  # 'new_draw', 'data_update', 'system_alert'
    timestamp: str
    draw_data: Optional[OptimizedLotteryData] = None
    message: str = ""
    priority: str = "normal"  # 'low', 'normal', 'high', 'critical'
    metadata: Dict[str, Any] = None

@dataclass
class SubscriberInfo:
    """订阅者信息"""
    subscriber_id: str
    callback: Callable[[NotificationEvent], None]
    event_types: List[str]
    active: bool = True
    last_notification: Optional[str] = None

class RealtimeNotificationSystem:
    """实时通知系统"""
    
    def __init__(self, polling_interval: int = 30):
        self.api_client = PC28UpstreamAPI()
        self.data_processor = OptimizedPC28DataProcessor()
        self.polling_interval = polling_interval
        
        # 订阅者管理
        self.subscribers: Dict[str, SubscriberInfo] = {}
        self.notification_queue = Queue(maxsize=1000)
        
        # 数据缓存
        self.last_draw_data: Optional[OptimizedLotteryData] = None
        self.data_cache: Dict[str, Any] = {}
        
        # 系统状态
        self.is_running = False
        self.polling_thread: Optional[threading.Thread] = None
        self.notification_thread: Optional[threading.Thread] = None
        self.executor = ThreadPoolExecutor(max_workers=5)
        
        # 统计信息
        self.stats = {
            'total_notifications': 0,
            'successful_deliveries': 0,
            'failed_deliveries': 0,
            'active_subscribers': 0,
            'last_poll_time': None,
            'system_start_time': None
        }
        
        # 中国时区
        self.china_tz = timezone(timedelta(hours=8))
    
    def subscribe(self, subscriber_id: str, callback: Callable[[NotificationEvent], None], 
                 event_types: List[str] = None) -> bool:
        """订阅通知"""
        if event_types is None:
            event_types = ['new_draw', 'data_update', 'system_alert']
        
        try:
            self.subscribers[subscriber_id] = SubscriberInfo(
                subscriber_id=subscriber_id,
                callback=callback,
                event_types=event_types,
                active=True
            )
            
            self.stats['active_subscribers'] = len([s for s in self.subscribers.values() if s.active])
            logger.info(f"订阅者 {subscriber_id} 已订阅事件: {event_types}")
            return True
            
        except Exception as e:
            logger.error(f"订阅失败 {subscriber_id}: {e}")
            return False
    
    def unsubscribe(self, subscriber_id: str) -> bool:
        """取消订阅"""
        try:
            if subscriber_id in self.subscribers:
                self.subscribers[subscriber_id].active = False
                del self.subscribers[subscriber_id]
                
                self.stats['active_subscribers'] = len([s for s in self.subscribers.values() if s.active])
                logger.info(f"订阅者 {subscriber_id} 已取消订阅")
                return True
            return False
            
        except Exception as e:
            logger.error(f"取消订阅失败 {subscriber_id}: {e}")
            return False
    
    def start_realtime_monitoring(self) -> bool:
        """启动实时监控"""
        if self.is_running:
            logger.warning("实时监控已在运行中")
            return False
        
        try:
            self.is_running = True
            self.stats['system_start_time'] = datetime.now(self.china_tz).isoformat()
            
            # 启动数据轮询线程
            self.polling_thread = threading.Thread(target=self._polling_worker, daemon=True)
            self.polling_thread.start()
            
            # 启动通知处理线程
            self.notification_thread = threading.Thread(target=self._notification_worker, daemon=True)
            self.notification_thread.start()
            
            logger.info(f"实时监控系统已启动，轮询间隔: {self.polling_interval}秒")
            
            # 发送系统启动通知
            self._queue_notification(NotificationEvent(
                event_type='system_alert',
                timestamp=datetime.now(self.china_tz).isoformat(),
                message='实时监控系统已启动',
                priority='normal'
            ))
            
            return True
            
        except Exception as e:
            logger.error(f"启动实时监控失败: {e}")
            self.is_running = False
            return False
    
    def stop_realtime_monitoring(self) -> bool:
        """停止实时监控"""
        try:
            self.is_running = False
            
            # 发送系统停止通知
            self._queue_notification(NotificationEvent(
                event_type='system_alert',
                timestamp=datetime.now(self.china_tz).isoformat(),
                message='实时监控系统已停止',
                priority='normal'
            ))
            
            # 等待线程结束
            if self.polling_thread and self.polling_thread.is_alive():
                self.polling_thread.join(timeout=5)
            
            if self.notification_thread and self.notification_thread.is_alive():
                self.notification_thread.join(timeout=5)
            
            logger.info("实时监控系统已停止")
            return True
            
        except Exception as e:
            logger.error(f"停止实时监控失败: {e}")
            return False
    
    def _polling_worker(self):
        """数据轮询工作线程"""
        logger.info("数据轮询线程已启动")
        
        while self.is_running:
            try:
                # 获取实时数据
                raw_data = self.api_client.get_realtime_lottery()
                self.stats['last_poll_time'] = datetime.now(self.china_tz).isoformat()
                
                if raw_data and raw_data.get('codeid') == 10000:
                    # 处理数据
                    processed_data = self.data_processor.process_realtime_data(raw_data)
                    
                    if processed_data:
                        latest_draw = processed_data[0]  # 最新一期
                        
                        # 检查是否有新开奖
                        if self._is_new_draw(latest_draw):
                            logger.info(f"检测到新开奖: 期号 {latest_draw.draw_id}")
                            
                            # 更新缓存
                            self.last_draw_data = latest_draw
                            self._update_data_cache(latest_draw)
                            
                            # 发送新开奖通知
                            self._queue_notification(NotificationEvent(
                                event_type='new_draw',
                                timestamp=datetime.now(self.china_tz).isoformat(),
                                draw_data=latest_draw,
                                message=f"新开奖: 期号 {latest_draw.draw_id}, 号码 {latest_draw.numbers}, 和值 {latest_draw.result_sum}",
                                priority='high'
                            ))
                        
                        # 发送数据更新通知
                        self._queue_notification(NotificationEvent(
                            event_type='data_update',
                            timestamp=datetime.now(self.china_tz).isoformat(),
                            draw_data=latest_draw,
                            message=f"数据已更新: 期号 {latest_draw.draw_id}",
                            priority='normal'
                        ))
                
                else:
                    logger.warning(f"获取实时数据失败: {raw_data}")
                    
                    # 发送系统警告
                    self._queue_notification(NotificationEvent(
                        event_type='system_alert',
                        timestamp=datetime.now(self.china_tz).isoformat(),
                        message='获取实时数据失败',
                        priority='high'
                    ))
                
            except Exception as e:
                logger.error(f"数据轮询异常: {e}")
                
                # 发送系统错误通知
                self._queue_notification(NotificationEvent(
                    event_type='system_alert',
                    timestamp=datetime.now(self.china_tz).isoformat(),
                    message=f'数据轮询异常: {str(e)}',
                    priority='critical'
                ))
            
            # 等待下次轮询
            time.sleep(self.polling_interval)
        
        logger.info("数据轮询线程已停止")
    
    def _notification_worker(self):
        """通知处理工作线程"""
        logger.info("通知处理线程已启动")
        
        while self.is_running:
            try:
                # 从队列获取通知事件
                event = self.notification_queue.get(timeout=1)
                
                # 分发通知给所有订阅者
                self._distribute_notification(event)
                
                self.notification_queue.task_done()
                
            except Empty:
                continue
            except Exception as e:
                logger.error(f"通知处理异常: {e}")
        
        logger.info("通知处理线程已停止")
    
    def _is_new_draw(self, draw_data: OptimizedLotteryData) -> bool:
        """检查是否为新开奖"""
        if not self.last_draw_data:
            return True
        
        return draw_data.draw_id != self.last_draw_data.draw_id
    
    def _update_data_cache(self, draw_data: OptimizedLotteryData):
        """更新数据缓存"""
        self.data_cache.update({
            'latest_draw_id': draw_data.draw_id,
            'latest_numbers': draw_data.numbers,
            'latest_sum': draw_data.result_sum,
            'latest_timestamp': draw_data.timestamp,
            'cache_update_time': datetime.now(self.china_tz).isoformat()
        })
    
    def _queue_notification(self, event: NotificationEvent):
        """将通知事件加入队列"""
        try:
            if not self.notification_queue.full():
                self.notification_queue.put(event, block=False)
                self.stats['total_notifications'] += 1
            else:
                logger.warning("通知队列已满，丢弃通知")
        except Exception as e:
            logger.error(f"通知入队失败: {e}")
    
    def _distribute_notification(self, event: NotificationEvent):
        """分发通知给订阅者"""
        active_subscribers = [s for s in self.subscribers.values() if s.active and event.event_type in s.event_types]
        
        if not active_subscribers:
            return
        
        # 并发发送通知
        futures = []
        for subscriber in active_subscribers:
            future = self.executor.submit(self._send_notification_to_subscriber, subscriber, event)
            futures.append(future)
        
        # 等待所有通知发送完成
        for future in futures:
            try:
                future.result(timeout=5)  # 5秒超时
                self.stats['successful_deliveries'] += 1
            except Exception as e:
                logger.error(f"通知发送失败: {e}")
                self.stats['failed_deliveries'] += 1
    
    def _send_notification_to_subscriber(self, subscriber: SubscriberInfo, event: NotificationEvent):
        """向单个订阅者发送通知"""
        try:
            subscriber.callback(event)
            subscriber.last_notification = event.timestamp
            logger.debug(f"通知已发送给订阅者 {subscriber.subscriber_id}: {event.event_type}")
        except Exception as e:
            logger.error(f"向订阅者 {subscriber.subscriber_id} 发送通知失败: {e}")
            raise
    
    def get_system_status(self) -> Dict[str, Any]:
        """获取系统状态"""
        return {
            'is_running': self.is_running,
            'polling_interval': self.polling_interval,
            'active_subscribers': len([s for s in self.subscribers.values() if s.active]),
            'total_subscribers': len(self.subscribers),
            'queue_size': self.notification_queue.qsize(),
            'last_draw_data': asdict(self.last_draw_data) if self.last_draw_data else None,
            'data_cache': self.data_cache,
            'statistics': self.stats
        }
    
    def get_subscriber_list(self) -> List[Dict[str, Any]]:
        """获取订阅者列表"""
        return [
            {
                'subscriber_id': sub.subscriber_id,
                'event_types': sub.event_types,
                'active': sub.active,
                'last_notification': sub.last_notification
            }
            for sub in self.subscribers.values()
        ]
    
    def send_custom_notification(self, message: str, priority: str = 'normal', 
                               event_type: str = 'system_alert') -> bool:
        """发送自定义通知"""
        try:
            self._queue_notification(NotificationEvent(
                event_type=event_type,
                timestamp=datetime.now(self.china_tz).isoformat(),
                message=message,
                priority=priority
            ))
            return True
        except Exception as e:
            logger.error(f"发送自定义通知失败: {e}")
            return False

# 示例订阅者回调函数
def console_notification_handler(event: NotificationEvent):
    """控制台通知处理器"""
    priority_symbols = {
        'low': '🔵',
        'normal': '🟢', 
        'high': '🟡',
        'critical': '🔴'
    }
    
    symbol = priority_symbols.get(event.priority, '⚪')
    print(f"{symbol} [{event.event_type.upper()}] {event.timestamp}: {event.message}")
    
    if event.draw_data:
        print(f"   期号: {event.draw_data.draw_id}, 号码: {event.draw_data.numbers}, 和值: {event.draw_data.result_sum}")

def file_notification_handler(event: NotificationEvent):
    """文件通知处理器"""
    log_file = f"notifications_{datetime.now().strftime('%Y%m%d')}.log"
    
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(f"{event.timestamp} [{event.event_type}] {event.priority}: {event.message}\n")
        if event.draw_data:
            f.write(f"  Draw Data: {asdict(event.draw_data)}\n")

def main():
    """测试实时通知系统"""
    notification_system = RealtimeNotificationSystem(polling_interval=10)
    
    try:
        # 订阅通知
        notification_system.subscribe('console_logger', console_notification_handler)
        notification_system.subscribe('file_logger', file_notification_handler, ['new_draw', 'system_alert'])
        
        print("=== PC28实时通知系统测试 ===")
        print(f"订阅者数量: {len(notification_system.get_subscriber_list())}")
        
        # 启动监控
        if notification_system.start_realtime_monitoring():
            print("✅ 实时监控已启动")
            
            # 运行30秒
            time.sleep(30)
            
            # 发送自定义通知
            notification_system.send_custom_notification("测试自定义通知", "high")
            
            # 显示系统状态
            status = notification_system.get_system_status()
            print("\n=== 系统状态 ===")
            print(json.dumps(status, indent=2, ensure_ascii=False))
            
        else:
            print("❌ 启动实时监控失败")
    
    except KeyboardInterrupt:
        print("\n用户中断")
    
    finally:
        # 停止监控
        notification_system.stop_realtime_monitoring()
        print("实时通知系统已停止")

if __name__ == "__main__":
    main()