import logging
import os
from contextlib import asynccontextmanager
from typing import (
    Any,
    AsyncIterator,
    Dict,
    Generator,
    List,
    Optional,
    Tuple,
    TypedDict,
)

from dotenv import load_dotenv
from mcp.server.fastmcp import Context, FastMCP
from mcp.server.fastmcp.prompts.base import Message, UserMessage
from pydantic import Field
from taosrest import RestClient

from template import get_prompt_template
from args import parse_arguments, get_taos_config, TaosConfig

logger = logging.getLogger(__name__)


NOT_ALLOWED_TAOS_SQL: Tuple = (
    "ALTER",
    "CREATE",
    "DELETE",
    "DROP",
    "INSERT",
    "UPDATE",
    "TRIM",
    "FLUSH",
    "BALANCE",
    "REDISTRIBUTE",
    "GRANT",
    "REVOKE",
    "RESET",
    "KILL",
    "COMPACT",
)


class TaosSqlResponse(TypedDict):
    """The response from the TDengine database.

    TaosV2: The response format below is from TDengine V2.
    TaosV3: The `status` and `head` fields will be removed from the response, and a `code` field will be added
    """

    status: str
    head: List[str]
    # Column（string）、Column type（string）、Column length（int）
    column_meta: List[List[str | int]]
    data: List[List[Any]]
    rows: int


class TAOSClient:
    def __init__(
        self,
        *,
        url: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
        timeout: int = 30,
    ) -> None:
        self.url = url
        self.username = username
        self.password = password
        self.database = database
        self.timeout = timeout

        self.client: RestClient = next(self.init_db())

    def init_db(self) -> Generator[RestClient, None, None]:
        _url = self.url
        try:
            client = RestClient(
                url=_url,
                user=self.username,  # type: ignore
                password=self.password,  # type: ignore
                database=self.database,  # type: ignore
                timeout=self.timeout,  # type: ignore
            )
            logger.info("Have initialized the taos client.")
            yield client
        except Exception as e:
            logger.error(
                f"Failed to connect to taos db => url: {_url}, username: {self.username}, database: {self.database}"
            )
            raise e

    def execute_sql(self, sql_stmt: str) -> TaosSqlResponse:
        """Execute SQL query and return the result."""

        logger.debug(f"Received TaosSQL statement: {sql_stmt}")
        validate_sql_stmt(sql_stmt)

        try:
            result = self.client.sql(sql_stmt)

            return TaosSqlResponse(
                status=result.get("status", ""),
                # https://docs.taosdata.com/2.6/reference/rest-api/#http-%E8%BF%94%E5%9B%9E%E6%A0%BC%E5%BC%8F
                # head可能会在后续版本移除, 当前版本2还能使用, 官方推荐使用column_meta
                head=result.get("head", []),
                column_meta=result.get("column_meta", []),
                data=result.get("data", []),
                rows=result.get("rows", -1),
            )
        except Exception as e:
            logger.error(f"Failed to execute SQL statement: {sql_stmt}")
            raise e


class TaosContext:
    def __init__(self, client: TAOSClient):
        self.client = client


@asynccontextmanager
async def server_lifespan(server: FastMCP) -> AsyncIterator[TaosContext]:
    """Manage application lifecycle for TDengine client."""

    config = server.config

    try:
        yield TaosContext(TAOSClient(**config))
    finally:
        pass


def validate_sql_stmt(sql_stmt: str):
    """Check if the SQL statement is allowed."""

    logger.debug(f"Received TaosSQL statement: {sql_stmt}")
    sql_stmt = sql_stmt.strip()
    if sql_stmt.upper().startswith(NOT_ALLOWED_TAOS_SQL):
        logger.warning(f"Only isReadOnly statements are allowed. Received: {sql_stmt}")
        raise ValueError(
            "Security restrictions: Only read-only statements such as queries are allowed to be executed. All other operations are prohibited."
        )


def register_tools(mcp: FastMCP):
    """Register tools for the FastMCP application."""

    # ==================== 基础工具：数据探索 ====================
    
    @mcp.tool(name="get_data_latest_date")
    def get_data_latest_date(
        ctx: Context,
        db_name: Optional[str] = Field(None, description="数据库名称，为空则使用当前配置的数据库"),
    ) -> Dict[str, str]:
        """获取数据库中所有数据的最新更新日期。
        
        这个工具会遍历所有超级表，找到最新的时间戳，并将其定义为"今天"。
        当用户问"今天"、"昨天"或"最近"时，应首先使用此工具来确定日期上下文。
        
        Args:
            db_name: 数据库名称
            
        Returns:
            Dict: 包含最新日期的字典，格式如 {"latest_date": "YYYY-MM-DD HH:MM:SS.mmm"}
        """
        taos = ctx.request_context.lifespan_context.client
        if db_name is None or db_name == "":
            db_name = taos.database

        try:
            stables_result = taos.execute_sql(f"SHOW {db_name}.STABLES;")
            stables = [stable[0] for stable in stables_result.get("data", [])]
        except Exception as e:
            logger.error(f"Failed to get stables list: {e}")
            from datetime import datetime
            return {"latest_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}

        latest_timestamp = None

        for stable_name in stables:
            try:
                # Check if table has data by getting max timestamp
                query = f"SELECT MAX(ts) as max_ts FROM {db_name}.{stable_name};"
                result = taos.execute_sql(query)
                
                if result.get("data") and result["data"][0][0] is not None:
                    current_stable_latest = result["data"][0][0]
                    
                    # Convert to string for comparison if needed
                    if isinstance(current_stable_latest, str):
                        current_ts_str = current_stable_latest
                    else:
                        current_ts_str = str(current_stable_latest)
                    
                    if latest_timestamp is None or current_ts_str > latest_timestamp:
                        latest_timestamp = current_ts_str

            except Exception as e:
                logger.warning(f"Could not get latest timestamp for stable {stable_name}: {e}")
                continue

        if latest_timestamp:
            return {"latest_date": latest_timestamp}
        else:
            # Fallback to current system time if no data found
            from datetime import datetime
            return {"latest_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}

    @mcp.tool(name="get_all_dbs")
    def get_all_dbs(ctx: Context) -> TaosSqlResponse:
        """获取所有数据库列表。
        
        Returns:
            TaosSqlResponse: 系统中所有可用的数据库列表。
        """
        taos = ctx.request_context.lifespan_context.client
        result = taos.execute_sql("SHOW DATABASES;")
        return result

    @mcp.tool(name="get_all_stables")
    def get_all_stables(
        ctx: Context,
        db_name: Optional[str] = Field(
            None,
            description="数据库名称，为空则使用当前配置的数据库"
        ),
    ) -> TaosSqlResponse:
        """获取指定数据库中的所有超级表列表。
        
        Args:
            db_name: 数据库名称，为空则使用当前配置的数据库
            
        Returns:
            TaosSqlResponse: 数据库中所有超级表的列表。
        """
        taos = ctx.request_context.lifespan_context.client
        if db_name is None or db_name == "":
            db_name = taos.database

        result = taos.execute_sql(f"SHOW {db_name}.STABLES;")
        return result

    @mcp.tool(name="switch_db")
    def switch_db(
        ctx: Context,
        db_name: str = Field(description="要切换到的数据库名称"),
    ) -> TaosSqlResponse:
        """切换到指定的数据库。
        
        Args:
            db_name: 要切换到的数据库名称
            
        Returns:
            TaosSqlResponse: 切换操作的结果。
        """
        taos = ctx.request_context.lifespan_context.client
        result = taos.execute_sql(f"USE {db_name};")
        return result

    @mcp.tool(name="get_stable_schema")
    def get_stable_schema(
        ctx: Context,
        stable_name: str = Field(description="超级表名称"),
        db_name: Optional[str] = Field(
            None,
            description="数据库名称，为空则使用当前配置的数据库"
        ),
    ) -> TaosSqlResponse:
        """获取超级表的结构信息，包括字段定义和数据类型。
        
        Args:
            stable_name: 超级表名称
            db_name: 数据库名称，为空则使用当前配置的数据库
            
        Returns:
            TaosSqlResponse: 超级表的字段结构信息。
        """
        taos = ctx.request_context.lifespan_context.client
        if db_name is None or db_name == "":
            db_name = taos.database

        result = taos.execute_sql(f"DESCRIBE {db_name}.{stable_name};")
        return result

    @mcp.tool(name="get_tag_info")
    def get_tag_info(
        ctx: Context,
        stable_name: str = Field(description="超级表名称"),
        db_name: Optional[str] = Field(
            None,
            description="数据库名称，为空则使用当前配置的数据库"
        ),
    ) -> TaosSqlResponse:
        """获取超级表的标签信息。
        
        Args:
            stable_name: 超级表名称
            db_name: 数据库名称，为空则使用当前配置的数据库
            
        Returns:
            TaosSqlResponse: 超级表的标签定义信息。
        """
        taos = ctx.request_context.lifespan_context.client
        if db_name is None or db_name == "":
            db_name = taos.database

        result = taos.execute_sql(f"SHOW TAGS FROM {db_name}.{stable_name};")
        return result

    @mcp.tool(name="test_stable_exists")
    def test_stable_exists(
        ctx: Context,
        stable_name: str = Field(description="超级表名称"),
        db_name: Optional[str] = Field(
            None,
            description="数据库名称，为空则使用当前配置的数据库"
        ),
    ) -> Dict[str, bool]:
        """检查指定的超级表是否存在。
        
        Args:
            stable_name: 超级表名称
            db_name: 数据库名称，为空则使用当前配置的数据库
            
        Returns:
            Dict: 包含超级表名称和是否存在的布尔值。
        """
        taos = ctx.request_context.lifespan_context.client
        if db_name is None or db_name == "":
            db_name = taos.database
            
        query = f"SHOW {db_name}.STABLES LIKE '{stable_name}'"
        result = taos.execute_sql(query)
        return {stable_name: len(result.get("data", [])) > 0}

    # ==================== 扩展工具：实际应用场景 ====================

    @mcp.tool(name="get_device_trajectory")
    def get_device_trajectory(
        ctx: Context,
        device_id: str = Field(description="设备ID或编号"),
        start_time: str = Field(description="开始时间，格式：YYYY-MM-DD HH:MM:SS"),
        end_time: str = Field(description="结束时间，格式：YYYY-MM-DD HH:MM:SS"),
        stable_name: Optional[str] = Field(None, description="指定超级表名称，为空时自动匹配"),
        db_name: Optional[str] = Field(None, description="数据库名称"),
        limit: int = Field(1000, description="返回记录数限制"),
    ) -> TaosSqlResponse:
        """获取指定设备在时间范围内的轨迹数据。
        
        这个工具用于查询设备的移动轨迹，包括GPS坐标、高度等位置信息。
        
        Args:
            device_id: 设备唯一标识符
            start_time: 轨迹查询开始时间
            end_time: 轨迹查询结束时间
            stable_name: 超级表名称，为空时根据数据特征自动匹配
            db_name: 数据库名称
            limit: 最大返回记录数
            
        Returns:
            TaosSqlResponse: 设备轨迹数据，包括时间戳、经纬度、高度等信息。
        """
        taos = ctx.request_context.lifespan_context.client
        if db_name is None or db_name == "":
            db_name = taos.database

        # 如果没有指定表名，尝试自动查找包含位置信息的表
        if stable_name is None:
            # 查询所有超级表，寻找包含GPS/位置字段的表
            stables_result = taos.execute_sql(f"SHOW {db_name}.STABLES;")
            for stable_info in stables_result.get("data", []):
                table_name = stable_info[0]
                try:
                    # 检查表结构是否包含位置相关字段
                    desc_result = taos.execute_sql(f"DESCRIBE {db_name}.{table_name};")
                    fields = [field[0].lower() for field in desc_result.get("data", [])]
                    if any(pos_field in fields for pos_field in ['lat', 'lon', 'latitude', 'longitude', 'x', 'y']):
                        stable_name = table_name
                        break
                except:
                    continue
            
            if stable_name is None:
                raise ValueError("未找到包含位置信息的超级表")

        # 构建查询SQL，尝试多种可能的设备ID字段名
        possible_id_fields = ['dev_id', 'device_id', 'id', 'sn', 'serial_no']
        
        # 获取表结构来确定实际的字段名
        desc_result = taos.execute_sql(f"DESCRIBE {db_name}.{stable_name};")
        actual_fields = [field[0] for field in desc_result.get("data", [])]
        
        # 找到匹配的设备ID字段
        id_field = None
        for field in possible_id_fields:
            if field in actual_fields:
                id_field = field
                break
        
        if id_field is None:
            # 如果没找到标准字段，使用第一个非时间戳字段
            for field in actual_fields:
                if field.lower() not in ['ts', 'timestamp']:
                    id_field = field
                    break
        
        if id_field is None:
            raise ValueError("无法确定设备ID字段")

        sql = f"""
        SELECT * FROM {db_name}.{stable_name} 
        WHERE {id_field} = '{device_id}' 
        AND ts >= '{start_time}' 
        AND ts <= '{end_time}' 
        ORDER BY ts 
        LIMIT {limit};
        """
        
        result = taos.execute_sql(sql)
        return result

    @mcp.tool(name="get_field_statistics")
    def get_field_statistics(
        ctx: Context,
        stable_name: str = Field(description="超级表名称"),
        field_name: str = Field(description="要统计的字段名称"),
        stat_type: str = Field(description="统计类型：distinct(去重), count(计数), value_counts(值分布统计)"),
        start_time: Optional[str] = Field(None, description="开始时间过滤"),
        end_time: Optional[str] = Field(None, description="结束时间过滤"),
        db_name: Optional[str] = Field(None, description="数据库名称"),
        limit: int = Field(100, description="返回记录数限制"),
    ) -> TaosSqlResponse:
        """对指定字段进行统计分析。
        
        Args:
            stable_name: 超级表名称
            field_name: 要分析的字段名称
            stat_type: 统计类型 - distinct/count/value_counts
            start_time: 时间过滤开始
            end_time: 时间过滤结束
            db_name: 数据库名称
            limit: 结果限制数量
            
        Returns:
            TaosSqlResponse: 字段统计结果。
        """
        taos = ctx.request_context.lifespan_context.client
        if db_name is None or db_name == "":
            db_name = taos.database

        # 构建时间条件
        time_condition = ""
        if start_time and end_time:
            time_condition = f"WHERE ts >= '{start_time}' AND ts <= '{end_time}'"
        elif start_time:
            time_condition = f"WHERE ts >= '{start_time}'"
        elif end_time:
            time_condition = f"WHERE ts <= '{end_time}'"

        # 根据统计类型构建SQL
        if stat_type == "distinct":
            sql = f"SELECT DISTINCT {field_name} FROM {db_name}.{stable_name} {time_condition} LIMIT {limit};"
        elif stat_type == "count":
            sql = f"SELECT COUNT({field_name}) as count FROM {db_name}.{stable_name} {time_condition};"
        elif stat_type == "value_counts":
            sql = f"SELECT {field_name}, COUNT(*) as count FROM {db_name}.{stable_name} {time_condition} GROUP BY {field_name} ORDER BY count DESC LIMIT {limit};"
        else:
            raise ValueError("stat_type必须是distinct、count或value_counts之一")

        result = taos.execute_sql(sql)
        return result

    @mcp.tool(name="get_aggregated_data")
    def get_aggregated_data(
        ctx: Context,
        stable_name: str = Field(description="超级表名称"),
        agg_function: str = Field(description="聚合函数：max, min, avg, sum, count"),
        field_name: str = Field(description="要聚合的字段名称"),
        group_by_field: Optional[str] = Field(None, description="分组字段"),
        start_time: Optional[str] = Field(None, description="开始时间过滤"),
        end_time: Optional[str] = Field(None, description="结束时间过滤"),
        db_name: Optional[str] = Field(None, description="数据库名称"),
        limit: int = Field(100, description="返回记录数限制"),
    ) -> TaosSqlResponse:
        """对数据进行聚合计算，如求最大值、最小值、平均值等。
        
        Args:
            stable_name: 超级表名称
            agg_function: 聚合函数类型
            field_name: 要聚合的字段
            group_by_field: 按哪个字段分组
            start_time: 时间过滤开始
            end_time: 时间过滤结束
            db_name: 数据库名称
            limit: 结果限制数量
            
        Returns:
            TaosSqlResponse: 聚合计算结果。
        """
        taos = ctx.request_context.lifespan_context.client
        if db_name is None or db_name == "":
            db_name = taos.database

        # 构建时间条件
        time_condition = ""
        if start_time and end_time:
            time_condition = f"WHERE ts >= '{start_time}' AND ts <= '{end_time}'"
        elif start_time:
            time_condition = f"WHERE ts >= '{start_time}'"
        elif end_time:
            time_condition = f"WHERE ts <= '{end_time}'"

        # 构建聚合SQL
        agg_expr = f"{agg_function.upper()}({field_name}) as {agg_function}_{field_name}"
        
        if group_by_field:
            if time_condition:
                sql = f"SELECT {group_by_field}, {agg_expr} FROM {db_name}.{stable_name} {time_condition} GROUP BY {group_by_field} ORDER BY {agg_function}_{field_name} DESC LIMIT {limit};"
            else:
                sql = f"SELECT {group_by_field}, {agg_expr} FROM {db_name}.{stable_name} GROUP BY {group_by_field} ORDER BY {agg_function}_{field_name} DESC LIMIT {limit};"
        else:
            if time_condition:
                sql = f"SELECT {agg_expr} FROM {db_name}.{stable_name} {time_condition};"
            else:
                sql = f"SELECT {agg_expr} FROM {db_name}.{stable_name};"

        result = taos.execute_sql(sql)
        return result

    @mcp.tool(name="filter_data_by_condition")
    def filter_data_by_condition(
        ctx: Context,
        stable_name: str = Field(description="超级表名称"),
        filter_conditions: str = Field(description="过滤条件，如：field1='value1' AND field2>100"),
        start_time: Optional[str] = Field(None, description="开始时间过滤"),
        end_time: Optional[str] = Field(None, description="结束时间过滤"),
        db_name: Optional[str] = Field(None, description="数据库名称"),
        limit: int = Field(100, description="返回记录数限制"),
    ) -> TaosSqlResponse:
        """根据指定条件筛选数据。
        
        Args:
            stable_name: 超级表名称
            filter_conditions: 筛选条件表达式
            start_time: 时间过滤开始
            end_time: 时间过滤结束
            db_name: 数据库名称
            limit: 结果限制数量
            
        Returns:
            TaosSqlResponse: 符合条件的数据记录。
        """
        taos = ctx.request_context.lifespan_context.client
        if db_name is None or db_name == "":
            db_name = taos.database

        # 构建WHERE条件
        conditions = []
        if start_time:
            conditions.append(f"ts >= '{start_time}'")
        if end_time:
            conditions.append(f"ts <= '{end_time}'")
        conditions.append(filter_conditions)

        where_clause = " AND ".join(conditions)
        
        sql = f"SELECT * FROM {db_name}.{stable_name} WHERE {where_clause} ORDER BY ts DESC LIMIT {limit};"
        
        result = taos.execute_sql(sql)
        return result

    @mcp.tool(name="analyze_time_series_trend")
    def analyze_time_series_trend(
        ctx: Context,
        stable_name: str = Field(description="超级表名称"),
        metric_field: str = Field(description="要分析趋势的指标字段"),
        time_interval: str = Field(description="时间聚合间隔，如：1h, 10m, 1d"),
        start_time: str = Field(description="分析开始时间"),
        end_time: str = Field(description="分析结束时间"),
        db_name: Optional[str] = Field(None, description="数据库名称"),
        group_by_tag: Optional[str] = Field(None, description="按标签分组"),
    ) -> TaosSqlResponse:
        """分析时序数据的趋势变化。
        
        Args:
            stable_name: 超级表名称
            metric_field: 分析的指标字段
            time_interval: 时间聚合间隔
            start_time: 开始时间
            end_time: 结束时间
            db_name: 数据库名称
            group_by_tag: 分组标签
            
        Returns:
            TaosSqlResponse: 时序趋势分析结果。
        """
        taos = ctx.request_context.lifespan_context.client
        if db_name is None or db_name == "":
            db_name = taos.database

        # 构建SELECT字段
        select_fields = ["_wstart", f"AVG({metric_field}) as avg_value"]
        
        if group_by_tag:
            select_fields.append(group_by_tag)

        # 构建SQL - TDengine中INTERVAL和PARTITION BY的正确语法
        sql_parts = [
            f"SELECT {', '.join(select_fields)}",
            f"FROM {db_name}.{stable_name}",
            f"WHERE ts >= '{start_time}' AND ts <= '{end_time}'"
        ]
        
        if group_by_tag:
            sql_parts.append(f"PARTITION BY {group_by_tag}")
            
        sql_parts.append(f"INTERVAL({time_interval})")
        sql_parts.append("ORDER BY _wstart")

        sql = " ".join(sql_parts) + ";"
        
        result = taos.execute_sql(sql)
        return result

    @mcp.tool(name="calculate_geo_distance")
    def calculate_geo_distance(
        ctx: Context,
        stable_name: str = Field(description="超级表名称"),
        lat1_field: str = Field(description="第一个纬度字段名"),
        lon1_field: str = Field(description="第一个经度字段名"),
        lat2_field: str = Field(description="第二个纬度字段名"),
        lon2_field: str = Field(description="第二个经度字段名"),
        distance_threshold: Optional[float] = Field(None, description="距离阈值（度），超过此值的记录将被标记"),
        start_time: Optional[str] = Field(None, description="开始时间过滤"),
        end_time: Optional[str] = Field(None, description="结束时间过滤"),
        db_name: Optional[str] = Field(None, description="数据库名称"),
        limit: int = Field(100, description="返回记录数限制"),
    ) -> TaosSqlResponse:
        """计算两点间的地理距离。
        
        注意：此工具计算简单的坐标差值，如需精确的地理距离，请在结果中进行后处理。
        
        Args:
            stable_name: 超级表名称
            lat1_field: 第一点纬度字段
            lon1_field: 第一点经度字段
            lat2_field: 第二点纬度字段
            lon2_field: 第二点经度字段
            distance_threshold: 距离阈值（度）
            start_time: 时间过滤开始
            end_time: 时间过滤结束
            db_name: 数据库名称
            limit: 结果限制数量
            
        Returns:
            TaosSqlResponse: 包含距离计算结果的数据。
        """
        taos = ctx.request_context.lifespan_context.client
        if db_name is None or db_name == "":
            db_name = taos.database

        # 构建WHERE条件
        conditions = []
        if start_time:
            conditions.append(f"ts >= '{start_time}'")
        if end_time:
            conditions.append(f"ts <= '{end_time}'")

        # 使用简单的坐标差值计算（避免复杂数学函数）
        lat_diff = f"ABS({lat2_field} - {lat1_field})"
        lon_diff = f"ABS({lon2_field} - {lon1_field})"
        simple_distance = f"({lat_diff} + {lon_diff})"
        
        # 添加距离阈值过滤
        if distance_threshold is not None:
            conditions.append(f"({simple_distance}) > {distance_threshold}")

        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)

        sql = f"""
        SELECT *, 
               {lat_diff} as lat_diff,
               {lon_diff} as lon_diff,
               {simple_distance} as simple_distance
        FROM {db_name}.{stable_name} 
        {where_clause}
        ORDER BY ts DESC 
        LIMIT {limit};
        """
        
        result = taos.execute_sql(sql)
        return result

    @mcp.tool(name="get_latest_records")
    def get_latest_records(
        ctx: Context,
        stable_name: str = Field(description="超级表名称"),
        device_filter: Optional[str] = Field(None, description="设备过滤条件，如：dev_id='ABC123'"),
        db_name: Optional[str] = Field(None, description="数据库名称"),
        limit: int = Field(10, description="返回最新记录数量"),
    ) -> TaosSqlResponse:
        """获取最新的数据记录。
        
        Args:
            stable_name: 超级表名称
            device_filter: 设备过滤条件
            db_name: 数据库名称
            limit: 返回记录数量
            
        Returns:
            TaosSqlResponse: 最新的数据记录。
        """
        taos = ctx.request_context.lifespan_context.client
        if db_name is None or db_name == "":
            db_name = taos.database

        where_clause = ""
        if device_filter:
            where_clause = f"WHERE {device_filter}"

        sql = f"SELECT * FROM {db_name}.{stable_name} {where_clause} ORDER BY ts DESC LIMIT {limit};"
        
        result = taos.execute_sql(sql)
        return result

    @mcp.tool(name="get_data_by_time_range")
    def get_data_by_time_range(
        ctx: Context,
        stable_name: str = Field(description="超级表名称"),
        start_time: str = Field(description="开始时间，格式：YYYY-MM-DD HH:MM:SS"),
        end_time: str = Field(description="结束时间，格式：YYYY-MM-DD HH:MM:SS"),
        device_filter: Optional[str] = Field(None, description="设备过滤条件"),
        db_name: Optional[str] = Field(None, description="数据库名称"),
        limit: int = Field(1000, description="返回记录数限制"),
    ) -> TaosSqlResponse:
        """按时间范围查询数据。
        
        Args:
            stable_name: 超级表名称
            start_time: 查询开始时间
            end_time: 查询结束时间
            device_filter: 设备过滤条件
            db_name: 数据库名称
            limit: 结果限制数量
            
        Returns:
            TaosSqlResponse: 指定时间范围内的数据。
        """
        taos = ctx.request_context.lifespan_context.client
        if db_name is None or db_name == "":
            db_name = taos.database

        conditions = [f"ts >= '{start_time}'", f"ts <= '{end_time}'"]
        if device_filter:
            conditions.append(device_filter)

        where_clause = "WHERE " + " AND ".join(conditions)

        sql = f"SELECT * FROM {db_name}.{stable_name} {where_clause} ORDER BY ts LIMIT {limit};"
        
        result = taos.execute_sql(sql)
        return result

    @mcp.tool(name="detect_anomalies")
    def detect_anomalies(
        ctx: Context,
        stable_name: str = Field(description="超级表名称"),
        anomaly_conditions: str = Field(description="异常检测条件，如：field1 IS NULL OR field2 > 1000"),
        start_time: Optional[str] = Field(None, description="开始时间过滤"),
        end_time: Optional[str] = Field(None, description="结束时间过滤"),
        db_name: Optional[str] = Field(None, description="数据库名称"),
        limit: int = Field(100, description="返回异常记录数限制"),
    ) -> TaosSqlResponse:
        """检测数据中的异常值或违规情况。
        
        Args:
            stable_name: 超级表名称
            anomaly_conditions: 异常检测条件表达式
            start_time: 时间过滤开始
            end_time: 时间过滤结束
            db_name: 数据库名称
            limit: 异常记录限制数量
            
        Returns:
            TaosSqlResponse: 检测到的异常数据记录。
        """
        taos = ctx.request_context.lifespan_context.client
        if db_name is None or db_name == "":
            db_name = taos.database

        conditions = [anomaly_conditions]
        if start_time:
            conditions.append(f"ts >= '{start_time}'")
        if end_time:
            conditions.append(f"ts <= '{end_time}'")

        where_clause = "WHERE " + " AND ".join(conditions)

        sql = f"SELECT * FROM {db_name}.{stable_name} {where_clause} ORDER BY ts DESC LIMIT {limit};"
        
        result = taos.execute_sql(sql)
        return result

    @mcp.tool(name="cross_table_lookup")
    def cross_table_lookup(
        ctx: Context,
        source_stable: str = Field(description="源超级表名称"),
        target_stable: str = Field(description="目标超级表名称"),
        lookup_field: str = Field(description="关联查找的字段名"),
        lookup_value: str = Field(description="要查找的值"),
        time_tolerance_minutes: int = Field(5, description="时间容忍度（分钟）"),
        db_name: Optional[str] = Field(None, description="数据库名称"),
        limit: int = Field(50, description="返回记录数限制"),
    ) -> Dict[str, TaosSqlResponse]:
        """跨表关联查询验证，用于多源数据核实。
        
        Args:
            source_stable: 源数据表名
            target_stable: 目标验证表名
            lookup_field: 关联字段名
            lookup_value: 查找值
            time_tolerance_minutes: 时间容忍度
            db_name: 数据库名称
            limit: 结果限制数量
            
        Returns:
            Dict[str, TaosSqlResponse]: 包含源表和目标表查询结果的字典。
        """
        taos = ctx.request_context.lifespan_context.client
        if db_name is None or db_name == "":
            db_name = taos.database

        results = {}

        # 首先从源表查询
        source_sql = f"SELECT * FROM {db_name}.{source_stable} WHERE {lookup_field} = '{lookup_value}' ORDER BY ts DESC LIMIT {limit};"
        source_result = taos.execute_sql(source_sql)
        results["source_table"] = source_result

        # 如果源表有数据，基于时间范围在目标表中查找
        if source_result.get("data"):
            # 获取源表数据的时间范围
            first_record = source_result["data"][0]
            # 假设第一列是时间戳
            base_time = first_record[0]
            
            # 计算时间范围 - 使用字符串操作而不是INTERVAL函数
            try:
                from datetime import datetime, timedelta
                if isinstance(base_time, str):
                    base_dt = datetime.fromisoformat(base_time.replace('T', ' ').replace('Z', ''))
                else:
                    base_dt = base_time
                
                start_time = base_dt - timedelta(minutes=time_tolerance_minutes)
                end_time = base_dt + timedelta(minutes=time_tolerance_minutes)
                
                start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
                end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
                
                # 构建目标表查询
                target_sql = f"""
                SELECT * FROM {db_name}.{target_stable} 
                WHERE ts >= '{start_time_str}' AND ts <= '{end_time_str}'
                ORDER BY ts 
                LIMIT {limit};
                """
                target_result = taos.execute_sql(target_sql)
                results["target_table"] = target_result
                
            except Exception as e:
                logger.warning(f"Failed to calculate time range: {e}")
                # 简化查询，只在目标表中查找相近时间的数据
                simplified_sql = f"SELECT * FROM {db_name}.{target_stable} ORDER BY ts DESC LIMIT {limit};"
                try:
                    target_result = taos.execute_sql(simplified_sql)
                    results["target_table"] = target_result
                except:
                    results["target_table"] = {"status": "error", "data": [], "rows": 0}
        else:
            results["target_table"] = {"status": "no_data", "data": [], "rows": 0}

        return results

    @mcp.tool(name="query_taos_db_data")
    def query_taos_db_data(
        ctx: Context,
        sql_stmt: str = Field(description="要执行的SQL语句（仅限只读查询）"),
    ) -> TaosSqlResponse:
        """执行自定义SQL查询（最后选择）。
        
        **重要提示**: 这是一个通用工具，只有在其他专用工具无法满足需求时才使用。
        所有SQL语句必须是只读的（SELECT查询），不允许修改数据。
        
        Args:
            sql_stmt: 要执行的只读SQL语句
            
        Returns:
            TaosSqlResponse: SQL查询结果。
        """
        taos = ctx.request_context.lifespan_context.client
        return taos.execute_sql(sql_stmt)


def register_resources(mcp: FastMCP):
    taos = TAOSClient(**mcp.config)

    @mcp.resource("taos://database", mime_type="text/plain")
    def get_current_taos_database() -> List:
        """Get current taos database."""
        result = taos.execute_sql("SHOW DATABASES;")
        return result.get("data", [])

    @mcp.resource("taos://schemas", mime_type="application/json")
    def get_current_db_all_taos_schema() -> Dict[str, Any]:
        """Provide all schema in the current database."""
        schema = {}
        stables = taos.execute_sql("SHOW STABLES;")

        for stable in stables["data"]:
            if stable:
                stable_name = stable[0]
                column = taos.execute_sql(f"DESCRIBE {stable_name};")
                logger.debug(f"{stable_name} - Field meta: {column}")

                data = column.get("data", [])
                column_meta = column.get("column_meta", [])

                # Retrieve the meta definition information of the field
                table_column_meta = [meta[0] for meta in column_meta]
                table_schema = []

                # Combine the field information and the actual field data
                for d in data:
                    table_schema.append(dict(zip(table_column_meta, d)))

                schema[stable_name] = table_schema

        return schema


def register_prompts(mcp: FastMCP):
    @mcp.prompt()
    def taos_query() -> str:
        """Query a Taos(涛思) database."""
        return get_prompt_template("prompt")

    @mcp.prompt()
    def describe_query_prompt(
        query: str = Field(description="The SQL query string"),
    ) -> List[Message]:
        """
        Generate a prompt to ask an LLM to explain what a given SQL query does.

        Args:
            query: The SQL query string.

        Returns:
            A list containing a prompt message to explain the query.
        """
        logger.debug(f"Entering describe_query_prompt() with query: {query}")
        prompt_text = (
            f"Explain the following SQL query:\n\n{query}\n\n"
            "Describe what data it retrieves and suggest any potential improvements."
        )
        logger.debug(f"Generated describe_query_prompt text: {prompt_text}")
        result = [UserMessage(prompt_text)]
        logger.debug("Exiting describe_query_prompt()")
        return result  # type: ignore


def main():
    # Initialize the Taos client
    load_dotenv()
    args = parse_arguments()

    # Set up logging. You can adjust the log level as needed. But the environment variable LOG_LEVEL has higher priority.
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", args.log_level).upper(),
        format="%(asctime)s - %(module)s.%(funcName)s:%(lineno)d - | %(levelname)s | - %(message)s",
    )

    _transport = os.environ.get("TRANSPORT", args.transport)
    _port = int(os.environ.get("PORT", args.port))
    _host = os.environ.get("HOST", args.host)

    mcp_app = FastMCP(
        name="[TDengine-MCP-Server]",
        description="TDengine-MCP-Server",
        lifespan=server_lifespan,
        dependencies=["dotenv", "taospy"],
        port=_port,
        host=_host,
    )
    mcp_app.config = get_taos_config(args)

    for register_func in (register_prompts, register_tools, register_resources):
        register_func(mcp_app)

    logger.info(f"[TDengine-MCP-Server] server started with transport: {_transport}")
    if _transport == "sse":
        logger.info(f"Listening on {_host}:{_port}")
    mcp_app.run(transport=_transport)


if __name__ == "__main__":
    main()
