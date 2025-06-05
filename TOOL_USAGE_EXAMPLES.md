# TDengine MCP 工具使用指南

本文档介绍TDengine MCP服务器的全套工具，以及如何组合使用这些工具完成复杂的时序数据查询和分析。

## 工具总览

### 基础工具 (6个原始工具)
1. `test_table_exists` - 检查stable是否存在
2. `get_all_dbs` - 获取所有数据库
3. `get_all_stables` - 获取所有超级表
4. `switch_db` - 切换数据库
5. `get_field_infos` - 获取stable的字段信息
6. `query_taos_db_data` - 执行只读SQL查询

### 扩展工具 (10个新增工具)
7. `get_all_tables` - 获取所有子表
8. `get_tag_infos` - 获取标签信息
9. `get_table_stats` - 获取表统计信息
10. `get_latest_data` - 获取最新数据
11. `get_data_by_time_range` - 按时间范围查询数据
12. `aggregate_query` - 聚合查询工具
13. `get_tag_values` - 获取标签值分布
14. `get_db_info` - 获取数据库详细信息
15. `check_data_integrity` - 检查数据完整性
16. `analyze_performance` - 性能分析工具

### 复合工具 (2个高级组合工具)
17. `comprehensive_stable_analysis` - 综合stable分析
18. `time_series_dashboard_data` - 时序仪表板数据生成

## 工具组合使用案例

### 案例1: 数据探索流程

```python
# 1. 首先查看所有数据库
get_all_dbs()

# 2. 切换到目标数据库
switch_db(db_name="sensor_data")

# 3. 查看所有超级表
get_all_stables(db_name="sensor_data")

# 4. 检查特定超级表是否存在
test_table_exists(stable_name="temperature_sensors")

# 5. 获取超级表的字段信息
get_field_infos(db_name="sensor_data", stable_name="temperature_sensors")

# 6. 获取标签信息
get_tag_infos(db_name="sensor_data", stable_name="temperature_sensors")
```

### 案例2: 数据质量检查

```python
# 1. 获取表统计信息
get_table_stats(db_name="sensor_data", stable_name="temperature_sensors")

# 2. 检查数据完整性
check_data_integrity(
    db_name="sensor_data", 
    stable_name="temperature_sensors",
    check_nulls=True,
    check_duplicates=True
)

# 3. 性能分析
analyze_performance(db_name="sensor_data", stable_name="temperature_sensors")

# 4. 获取标签值分布
get_tag_values(
    db_name="sensor_data",
    stable_name="temperature_sensors", 
    tag_name="location"
)
```

### 案例3: 时序数据分析

```python
# 1. 获取最新数据
get_latest_data(
    db_name="sensor_data",
    stable_name="temperature_sensors",
    limit=10
)

# 2. 按时间范围查询
get_data_by_time_range(
    db_name="sensor_data",
    stable_name="temperature_sensors",
    start_time="2024-01-01 00:00:00",
    end_time="2024-01-02 00:00:00",
    limit=1000
)

# 3. 聚合查询 - 每小时平均温度
aggregate_query(
    db_name="sensor_data",
    stable_name="temperature_sensors",
    agg_function="avg",
    column_name="temperature",
    interval="1h",
    group_by_tags=["location"],
    start_time="2024-01-01 00:00:00",
    end_time="2024-01-02 00:00:00"
)
```

### 案例4: 使用复合工具进行综合分析

```python
# 1. 综合分析一个超级表
comprehensive_stable_analysis(
    db_name="sensor_data",
    stable_name="temperature_sensors",
    include_sample_data=True,
    days_back=7
)

# 2. 生成仪表板数据
time_series_dashboard_data(
    db_name="sensor_data",
    stable_name="temperature_sensors",
    metric_column="temperature",
    time_range_hours=24,
    interval_minutes=60,
    group_by_tag="location"
)
```

## 高级组合模式

### 模式1: 多层次数据探索

```python
# 1. 数据库级别探索
dbs = get_all_dbs()
for db in dbs['data']:
    db_name = db[0]
    
    # 2. 数据库详细信息
    db_info = get_db_info(db_name=db_name)
    
    # 3. 该数据库的所有超级表
    stables = get_all_stables(db_name=db_name)
    
    for stable_info in stables['data']:
        stable_name = stable_info[0]
        
        # 4. 每个超级表的综合分析
        analysis = comprehensive_stable_analysis(
            db_name=db_name,
            stable_name=stable_name
        )
```

### 模式2: 性能监控和异常检测

```python
# 1. 获取所有超级表
stables = get_all_stables()

for stable_info in stables['data']:
    stable_name = stable_info[0]
    
    # 2. 性能分析
    perf = analyze_performance(stable_name=stable_name)
    
    # 3. 数据完整性检查
    integrity = check_data_integrity(stable_name=stable_name)
    
    # 4. 如果发现问题，获取详细信息
    if integrity['null_counts']:
        # 获取最新数据查看问题
        latest = get_latest_data(stable_name=stable_name, limit=20)
        
        # 获取字段信息进行分析
        fields = get_field_infos(stable_name=stable_name)
```

### 模式3: 实时监控仪表板

```python
# 1. 获取实时数据
latest_data = get_latest_data(
    stable_name="sensor_readings",
    limit=1
)

# 2. 获取过去24小时的趋势
dashboard_data = time_series_dashboard_data(
    stable_name="sensor_readings",
    metric_column="value",
    time_range_hours=24,
    interval_minutes=10,
    group_by_tag="sensor_type"
)

# 3. 获取异常值（最大最小值）
extreme_values = aggregate_query(
    stable_name="sensor_readings",
    agg_function="max",
    column_name="value",
    start_time="2024-01-01 00:00:00",
    end_time="2024-01-01 23:59:59"
)
```

## 最佳实践

### 1. 工具链式调用
- 先用基础工具探索数据结构
- 再用分析工具深入了解数据质量
- 最后用复合工具生成报告

### 2. 错误处理
- 复合工具内置了错误处理机制
- 单独使用工具时要考虑异常情况
- 建议在调用前先检查表是否存在

### 3. 性能优化
- 大数据查询时使用limit参数
- 时间范围查询时指定合适的时间窗口
- 聚合查询时选择合适的时间间隔

### 4. 数据安全
- 所有工具都是只读的，符合安全要求
- 不允许执行修改、删除等危险操作
- SQL注入防护已内置到工具中

## 工具输出格式

### TaosSqlResponse 格式
```json
{
    "status": "succ",
    "head": ["column1", "column2"],
    "column_meta": [["column1", "type", length], ["column2", "type", length]],
    "data": [["value1", "value2"], ["value3", "value4"]],
    "rows": 2
}
```

### 复合工具返回格式
```json
{
    "stable_name": "temperature_sensors",
    "database_name": "sensor_data", 
    "analysis_status": "completed",
    "schema": {...},
    "tags": {...},
    "performance": {...},
    "statistics": {...},
    "data_integrity": {...},
    "sample_data": {...}
}
```

这套工具集提供了从基础数据探索到高级分析的完整解决方案，支持灵活组合使用以满足各种复杂的时序数据查询需求。 