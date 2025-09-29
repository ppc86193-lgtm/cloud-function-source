#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PC28 数据库可视化查看器
基于 Streamlit 的数据库可视化工具，支持 BigQuery 数据查看和分析
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
import subprocess
import sys
import os
import threading
import fcntl
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import logging

# 设置页面配置
st.set_page_config(
    page_title="PC28 数据库查看器",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 添加 python 目录到路径
sys.path.append(os.path.join(os.path.dirname(__file__), 'python'))

class DatabaseViewer:
    """数据库可视化查看器"""
    
    def __init__(self):
        self.project_id = "wprojectl"
        self.datasets = {
            "pc28_lab": "实验室数据集",
            "pc28": "生产数据集", 
            "draw_dataset": "开奖数据集",
            "lab_dataset": "实验数据集"
        }
        
        # 添加BigQuery操作锁机制
        self._query_lock = threading.RLock()  # 可重入锁，支持同一线程多次获取
        self._dataset_lock = threading.Lock()  # 数据集操作锁
        self._table_lock = threading.Lock()  # 表操作锁
        self._lock_timeout = 30  # 锁超时时间（秒）

    def run_bigquery(self, sql: str, timeout: int = 60) -> pd.DataFrame:
        """执行 BigQuery 查询"""
        # 使用查询锁防止并发查询冲突
        if not self._query_lock.acquire(timeout=self._lock_timeout):
            st.error("获取查询锁超时，请稍后重试")
            return pd.DataFrame()
        
        try:
            cmd = f'bq query --use_legacy_sql=false --format=json --project_id={self.project_id} "{sql}"'
            result = subprocess.run(
                cmd, 
                shell=True, 
                capture_output=True, 
                text=True, 
                timeout=timeout
            )
            
            if result.returncode != 0:
                st.error(f"查询失败: {result.stderr}")
                return pd.DataFrame()
                
            if result.stdout.strip():
                data = json.loads(result.stdout)
                return pd.DataFrame(data)
            else:
                return pd.DataFrame()
                
        except subprocess.TimeoutExpired:
            st.error("查询超时")
            return pd.DataFrame()
        except Exception as e:
            st.error(f"查询错误: {str(e)}")
            return pd.DataFrame()
        finally:
            self._query_lock.release()
    
    def get_datasets(self) -> List[str]:
        """获取数据集列表"""
        # 使用数据集锁防止并发操作
        if not self._dataset_lock.acquire(timeout=self._lock_timeout):
            st.error("获取数据集锁超时，请稍后重试")
            return []
        
        try:
            cmd = f"bq ls --project_id={self.project_id}"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            
            if result.returncode == 0:
                lines = result.stdout.strip().split('\n')
                datasets = []
                for line in lines[2:]:  # 跳过标题行
                    if line.strip():
                        dataset_id = line.split()[0]
                        if dataset_id != "datasetId":  # 跳过标题
                            datasets.append(dataset_id)
                return datasets
            else:
                st.error(f"获取数据集失败: {result.stderr}")
                return []
        except Exception as e:
            st.error(f"获取数据集错误: {str(e)}")
            return []
        finally:
            self._dataset_lock.release()
        return list(self.datasets.keys())
    
    def get_tables(self, dataset: str) -> List[Dict[str, Any]]:
        """获取数据集中的表列表"""
        # 使用表锁防止并发操作
        if not self._table_lock.acquire(timeout=self._lock_timeout):
            st.error("获取表锁超时，请稍后重试")
            return []
        
        try:
            cmd = f"bq ls --project_id={self.project_id} {dataset}"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            
            if result.returncode == 0:
                lines = result.stdout.strip().split('\n')
                tables = []
                for line in lines[2:]:  # 跳过标题行
                    if line.strip():
                        parts = line.strip().split()
                        if len(parts) >= 2:
                            table_name = parts[0]
                            table_type = parts[1] if len(parts) > 1 else "TABLE"
                            tables.append({
                                "name": table_name,
                                "type": table_type
                            })
                return tables
            return []
        except Exception as e:
            st.error(f"获取表列表错误: {str(e)}")
            return []
        finally:
            self._table_lock.release()
    
    def get_table_schema(self, dataset: str, table: str) -> pd.DataFrame:
        """获取表结构"""
        sql = f"""
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default
        FROM `{self.project_id}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '{table}'
        ORDER BY ordinal_position
        """
        return self.run_bigquery(sql)
    
    def get_table_preview(self, dataset: str, table: str, limit: int = 100) -> pd.DataFrame:
        """获取表数据预览"""
        sql = f"SELECT * FROM `{self.project_id}.{dataset}.{table}` LIMIT {limit}"
        return self.run_bigquery(sql)
    
    def get_table_stats(self, dataset: str, table: str) -> Dict[str, Any]:
        """获取表统计信息"""
        sql = f"""
        SELECT 
            COUNT(*) as row_count,
            COUNT(DISTINCT *) as unique_rows
        FROM `{self.project_id}.{dataset}.{table}`
        """
        df = self.run_bigquery(sql)
        if not df.empty:
            return df.iloc[0].to_dict()
        return {}

def main():
    """主函数"""
    st.title("📊 PC28 数据库可视化查看器")
    st.markdown("---")
    
    # 初始化查看器
    viewer = DatabaseViewer()
    
    # 侧边栏
    st.sidebar.title("🔍 数据库导航")
    
    # 获取数据集列表
    datasets = viewer.get_datasets()
    
    if not datasets:
        st.error("无法获取数据集列表，请检查 BigQuery 连接")
        return
    
    # 选择数据集
    selected_dataset = st.sidebar.selectbox(
        "选择数据集",
        datasets,
        format_func=lambda x: f"{x} ({viewer.datasets.get(x, '未知')})"
    )
    
    if selected_dataset:
        # 获取表列表
        tables = viewer.get_tables(selected_dataset)
        
        if tables:
            table_names = [t["name"] for t in tables]
            selected_table = st.sidebar.selectbox("选择表", table_names)
            
            if selected_table:
                # 主界面标签页
                tab1, tab2, tab3, tab4 = st.tabs(["📋 表结构", "📊 数据预览", "📈 统计信息", "🔍 自定义查询"])
                
                with tab1:
                    st.subheader(f"表结构: {selected_dataset}.{selected_table}")
                    
                    # 获取表结构
                    schema_df = viewer.get_table_schema(selected_dataset, selected_table)
                    
                    if not schema_df.empty:
                        st.dataframe(
                            schema_df,
                            use_container_width=True,
                            hide_index=True
                        )
                        
                        # 显示字段统计
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("总字段数", len(schema_df))
                        with col2:
                            nullable_count = schema_df[schema_df['is_nullable'] == 'YES'].shape[0]
                            st.metric("可空字段", nullable_count)
                        with col3:
                            non_nullable_count = schema_df[schema_df['is_nullable'] == 'NO'].shape[0]
                            st.metric("非空字段", non_nullable_count)
                    else:
                        st.warning("无法获取表结构")
                
                with tab2:
                    st.subheader(f"数据预览: {selected_dataset}.{selected_table}")
                    
                    # 设置预览行数
                    preview_limit = st.slider("预览行数", 10, 1000, 100)
                    
                    if st.button("刷新数据", key="refresh_preview"):
                        # 获取数据预览
                        preview_df = viewer.get_table_preview(selected_dataset, selected_table, preview_limit)
                        
                        if not preview_df.empty:
                            st.dataframe(
                                preview_df,
                                use_container_width=True,
                                hide_index=True
                            )
                            
                            # 下载数据
                            csv = preview_df.to_csv(index=False)
                            st.download_button(
                                label="下载 CSV",
                                data=csv,
                                file_name=f"{selected_dataset}_{selected_table}_preview.csv",
                                mime="text/csv"
                            )
                        else:
                            st.warning("表中没有数据或查询失败")
                
                with tab3:
                    st.subheader(f"统计信息: {selected_dataset}.{selected_table}")
                    
                    if st.button("获取统计信息", key="get_stats"):
                        # 获取表统计
                        stats = viewer.get_table_stats(selected_dataset, selected_table)
                        
                        if stats:
                            col1, col2 = st.columns(2)
                            with col1:
                                st.metric("总行数", f"{stats.get('row_count', 0):,}")
                            with col2:
                                st.metric("唯一行数", f"{stats.get('unique_rows', 0):,}")
                            
                            # 数据质量指标
                            if stats.get('row_count', 0) > 0:
                                duplicate_rate = (stats.get('row_count', 0) - stats.get('unique_rows', 0)) / stats.get('row_count', 1) * 100
                                st.metric("重复率", f"{duplicate_rate:.2f}%")
                        else:
                            st.warning("无法获取统计信息")
                
                with tab4:
                    st.subheader("🔍 自定义 SQL 查询")
                    
                    # 预设查询模板
                    query_templates = {
                        "基本查询": f"SELECT * FROM `{viewer.project_id}.{selected_dataset}.{selected_table}` LIMIT 100",
                        "行数统计": f"SELECT COUNT(*) as total_rows FROM `{viewer.project_id}.{selected_dataset}.{selected_table}`",
                        "最近数据": f"SELECT * FROM `{viewer.project_id}.{selected_dataset}.{selected_table}` ORDER BY timestamp DESC LIMIT 50" if selected_table in ['score_ledger', 'draws_14w_dedup_v'] else f"SELECT * FROM `{viewer.project_id}.{selected_dataset}.{selected_table}` LIMIT 50"
                    }
                    
                    # 选择查询模板
                    template_choice = st.selectbox("选择查询模板", list(query_templates.keys()))
                    
                    # SQL 输入框
                    sql_query = st.text_area(
                        "SQL 查询",
                        value=query_templates[template_choice],
                        height=150,
                        help="输入您的 BigQuery SQL 查询"
                    )
                    
                    col1, col2 = st.columns([1, 4])
                    with col1:
                        if st.button("执行查询", type="primary"):
                            if sql_query.strip():
                                with st.spinner("执行查询中..."):
                                    result_df = viewer.run_bigquery(sql_query)
                                    
                                    if not result_df.empty:
                                        st.success(f"查询成功！返回 {len(result_df)} 行数据")
                                        st.dataframe(
                                            result_df,
                                            use_container_width=True,
                                            hide_index=True
                                        )
                                        
                                        # 下载结果
                                        csv = result_df.to_csv(index=False)
                                        st.download_button(
                                            label="下载查询结果",
                                            data=csv,
                                            file_name=f"query_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                            mime="text/csv"
                                        )
                                    else:
                                        st.warning("查询没有返回数据")
                            else:
                                st.error("请输入 SQL 查询")
        else:
            st.warning(f"数据集 {selected_dataset} 中没有表")
    
    # 页脚信息
    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center; color: #666;'>
            <p>PC28 数据库可视化查看器 | 基于 Streamlit 构建</p>
            <p>支持 BigQuery 数据查看、表结构分析和自定义查询</p>
        </div>
        """,
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()