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

from .template import get_prompt_template
from .args import parse_arguments, get_taos_config, TaosConfig

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

    @mcp.tool(name="test_table_exists")
    def test_table_exists(
        ctx: Context,
        stable_name: str = Field(description="The name of the stable"),
    ) -> Dict[str, bool]:
        """**Important**: Check if the `stable` exists in the current `Taos database(涛思数据库)` configuration.

        Args:
            stable_name (str): The name of the stable.

        Returns:
            Dict: The `stable_name` exists or not in the current Taos configuration. If the `stable_name` does not exist, an empty dictionary is returned.

            The key of the dictionary is the `stable_name` name, and the value is a boolean indicating whether the `stable_name` exists.
        """

        taos = ctx.request_context.lifespan_context.client
        query = f"SHOW STABLES LIKE '{stable_name}'"
        result = taos.execute_sql(query)
        return {stable_name: bool(result)}

    @mcp.tool(name="get_all_dbs")
    def get_all_dbs(ctx: Context) -> TaosSqlResponse:
        """Get all databases.

        Returns:
            TaosSqlResponse: All databases in the current Taos configuration.
        """

        taos = ctx.request_context.lifespan_context.client
        result = taos.execute_sql("SHOW DATABASES;")

        return result

    @mcp.tool(name="get_all_stables")
    def get_all_stables(
        ctx: Context,
        db_name: Optional[str] = Field(
            None,
            description="The name of the database. Default is None which means the configured database.",
        ),
    ) -> TaosSqlResponse:
        """Get all stables.

        Args:
            db_name (Optional[str]): The name of the database. Defaults to None. When the value is None, it means the configured database is used.

        Returns:
            TaosSqlResponse: All stables in the current Taos database.
        """

        taos = ctx.request_context.lifespan_context.client
        if db_name is None or db_name == "":
            db_name = taos.database

        result = taos.execute_sql(f"SHOW {db_name}.STABLES;")

        return result

    @mcp.tool(name="switch_db")
    def switch_db(
        ctx: Context,
        db_name: str = Field(description="The name of the database to switch to"),
    ) -> TaosSqlResponse:
        """Switch to the specified database.

        Args:
            db_name (str): The name of the database to switch to.

        Returns:
            TaosSqlResponse: The result of the `USE` command.
        """

        taos = ctx.request_context.lifespan_context.client
        result = taos.execute_sql(f"USE {db_name};")

        return result

    @mcp.tool(name="get_field_infos")
    def get_field_infos(
        ctx: Context,
        db_name: Optional[str] = Field(
            None,
            description="The name of the database. Default is None which means the configured database.",
        ),
        stable_name: str = Field(description="The name of the stable"),
    ) -> TaosSqlResponse:
        """Get the field information of the specified stable.

        Args:
            db_name (Optional[str]): The name of the database. Defaults to None. When the value is None, it means the configured database is used.
            stable_name (str): The name of the stable.

        Returns:
            TaosSqlResponse: The field information of the specified stable.
        """

        taos = ctx.request_context.lifespan_context.client
        if db_name is None or db_name == "":
            db_name = taos.database

        result = taos.execute_sql(f"DESCRIBE {db_name}.{stable_name};")

        return result

    @mcp.tool(name="query_taos_db_data")
    def query_taos_db_data(
        ctx: Context,
        sql_stmt: str = Field(
            description="The sql statement you want to retrieve data from taos db"
        ),
    ) -> TaosSqlResponse:
        """**Important**: Run a read-only SQL query on `Taos database(涛思数据库)`.

        Args:
            sql_stmt (str): The sql statement you want to retrieve data from taos db.

        Returns:
            List: All data from the specified table.

        """

        taos = ctx.request_context.lifespan_context.client
        return taos.execute_sql(sql_stmt)  # type: ignore

    @mcp.tool(name="get_all_tables")
    def get_all_tables(
        ctx: Context,
        db_name: Optional[str] = Field(
            None,
            description="The name of the database. Default is None which means the configured database.",
        ),
        stable_name: Optional[str] = Field(
            None,
            description="The name of the stable to filter tables. If specified, only tables under this stable will be returned.",
        ),
    ) -> TaosSqlResponse:
        """Get all tables (子表) in the database or under a specific stable.

        Args:
            db_name (Optional[str]): The name of the database. Defaults to None.
            stable_name (Optional[str]): The name of the stable to filter tables. Defaults to None.

        Returns:
            TaosSqlResponse: All tables in the specified database or under the stable.
        """

        taos = ctx.request_context.lifespan_context.client
        if db_name is None or db_name == "":
            db_name = taos.database

        if stable_name:
            result = taos.execute_sql(f"SHOW {db_name}.TABLES LIKE '{stable_name}_%';")
        else:
            result = taos.execute_sql(f"SHOW {db_name}.TABLES;")

        return result

    @mcp.tool(name="get_tag_infos")
    def get_tag_infos(
        ctx: Context,
        db_name: Optional[str] = Field(
            None,
            description="The name of the database. Default is None which means the configured database.",
        ),
        stable_name: str = Field(description="The name of the stable"),
    ) -> TaosSqlResponse:
        """Get tag information of the specified stable.

        Args:
            db_name (Optional[str]): The name of the database. Defaults to None.
            stable_name (str): The name of the stable.

        Returns:
            TaosSqlResponse: Tag information of the specified stable.
        """

        taos = ctx.request_context.lifespan_context.client
        if db_name is None or db_name == "":
            db_name = taos.database

        result = taos.execute_sql(f"SHOW TAGS FROM {db_name}.{stable_name};")
        return result

    @mcp.tool(name="get_table_stats")
    def get_table_stats(
        ctx: Context,
        db_name: Optional[str] = Field(
            None,
            description="The name of the database. Default is None which means the configured database.",
        ),
        stable_name: Optional[str] = Field(
            None,
            description="The name of the stable. If specified, get stats for this stable.",
        ),
        table_name: Optional[str] = Field(
            None,
            description="The name of the table. If specified, get stats for this table.",
        ),
    ) -> TaosSqlResponse:
        """Get statistics information for tables or stables including row count, size, etc.

        Args:
            db_name (Optional[str]): The name of the database. Defaults to None.
            stable_name (Optional[str]): The name of the stable. Defaults to None.
            table_name (Optional[str]): The name of the table. Defaults to None.

        Returns:
            TaosSqlResponse: Statistics information for the specified table/stable.
        """

        taos = ctx.request_context.lifespan_context.client
        if db_name is None or db_name == "":
            db_name = taos.database

        if table_name:
            sql = f"SELECT COUNT(*) as row_count FROM {db_name}.{table_name};"
        elif stable_name:
            sql = f"SELECT COUNT(*) as row_count FROM {db_name}.{stable_name};"
        else:
            # Get stats for all stables
            sql = f"SELECT stable_name, COUNT(*) as row_count FROM information_schema.ins_stables WHERE db_name='{db_name}' GROUP BY stable_name;"

        result = taos.execute_sql(sql)
        return result

    @mcp.tool(name="get_latest_data")
    def get_latest_data(
        ctx: Context,
        db_name: Optional[str] = Field(
            None,
            description="The name of the database. Default is None which means the configured database.",
        ),
        stable_name: Optional[str] = Field(
            None,
            description="The name of the stable.",
        ),
        table_name: Optional[str] = Field(
            None,
            description="The name of the table.",
        ),
        limit: int = Field(
            10,
            description="The number of latest records to retrieve. Default is 10.",
        ),
    ) -> TaosSqlResponse:
        """Get the latest data from a table or stable ordered by timestamp.

        Args:
            db_name (Optional[str]): The name of the database. Defaults to None.
            stable_name (Optional[str]): The name of the stable. Defaults to None.
            table_name (Optional[str]): The name of the table. Defaults to None.
            limit (int): The number of latest records to retrieve. Default is 10.

        Returns:
            TaosSqlResponse: The latest data records.
        """

        taos = ctx.request_context.lifespan_context.client
        if db_name is None or db_name == "":
            db_name = taos.database

        if table_name:
            target = f"{db_name}.{table_name}"
        elif stable_name:
            target = f"{db_name}.{stable_name}"
        else:
            raise ValueError("Either stable_name or table_name must be specified")

        sql = f"SELECT * FROM {target} ORDER BY ts DESC LIMIT {limit};"
        result = taos.execute_sql(sql)
        return result

    @mcp.tool(name="get_data_by_time_range")
    def get_data_by_time_range(
        ctx: Context,
        db_name: Optional[str] = Field(
            None,
            description="The name of the database. Default is None which means the configured database.",
        ),
        stable_name: Optional[str] = Field(
            None,
            description="The name of the stable.",
        ),
        table_name: Optional[str] = Field(
            None,
            description="The name of the table.",
        ),
        start_time: str = Field(
            description="Start time in format 'YYYY-MM-DD HH:MM:SS' or 'YYYY-MM-DD'",
        ),
        end_time: str = Field(
            description="End time in format 'YYYY-MM-DD HH:MM:SS' or 'YYYY-MM-DD'",
        ),
        limit: Optional[int] = Field(
            None,
            description="Maximum number of records to retrieve. Default is None (no limit).",
        ),
    ) -> TaosSqlResponse:
        """Get data within a specific time range.

        Args:
            db_name (Optional[str]): The name of the database. Defaults to None.
            stable_name (Optional[str]): The name of the stable. Defaults to None.
            table_name (Optional[str]): The name of the table. Defaults to None.
            start_time (str): Start time in format 'YYYY-MM-DD HH:MM:SS' or 'YYYY-MM-DD'.
            end_time (str): End time in format 'YYYY-MM-DD HH:MM:SS' or 'YYYY-MM-DD'.
            limit (Optional[int]): Maximum number of records to retrieve. Default is None.

        Returns:
            TaosSqlResponse: Data within the specified time range.
        """

        taos = ctx.request_context.lifespan_context.client
        if db_name is None or db_name == "":
            db_name = taos.database

        if table_name:
            target = f"{db_name}.{table_name}"
        elif stable_name:
            target = f"{db_name}.{stable_name}"
        else:
            raise ValueError("Either stable_name or table_name must be specified")

        sql = f"SELECT * FROM {target} WHERE ts >= '{start_time}' AND ts <= '{end_time}' ORDER BY ts"
        if limit:
            sql += f" LIMIT {limit}"
        sql += ";"

        result = taos.execute_sql(sql)
        return result

    @mcp.tool(name="aggregate_query")
    def aggregate_query(
        ctx: Context,
        db_name: Optional[str] = Field(
            None,
            description="The name of the database. Default is None which means the configured database.",
        ),
        stable_name: Optional[str] = Field(
            None,
            description="The name of the stable.",
        ),
        table_name: Optional[str] = Field(
            None,
            description="The name of the table.",
        ),
        agg_function: str = Field(
            description="Aggregation function: avg, sum, count, max, min, first, last, etc.",
        ),
        column_name: str = Field(
            description="The column name to apply aggregation function.",
        ),
        interval: Optional[str] = Field(
            None,
            description="Time interval for aggregation (e.g., '1h', '10m', '1d'). Default is None.",
        ),
        group_by_tags: Optional[List[str]] = Field(
            None,
            description="List of tag names to group by. Default is None.",
        ),
        start_time: Optional[str] = Field(
            None,
            description="Start time filter in format 'YYYY-MM-DD HH:MM:SS' or 'YYYY-MM-DD'",
        ),
        end_time: Optional[str] = Field(
            None,
            description="End time filter in format 'YYYY-MM-DD HH:MM:SS' or 'YYYY-MM-DD'",
        ),
    ) -> TaosSqlResponse:
        """Perform aggregation queries on time series data.

        Args:
            db_name (Optional[str]): The name of the database. Defaults to None.
            stable_name (Optional[str]): The name of the stable. Defaults to None.
            table_name (Optional[str]): The name of the table. Defaults to None.
            agg_function (str): Aggregation function to apply.
            column_name (str): The column name to apply aggregation function.
            interval (Optional[str]): Time interval for aggregation. Defaults to None.
            group_by_tags (Optional[List[str]]): List of tag names to group by. Defaults to None.
            start_time (Optional[str]): Start time filter. Defaults to None.
            end_time (Optional[str]): End time filter. Defaults to None.

        Returns:
            TaosSqlResponse: Aggregated query results.
        """

        taos = ctx.request_context.lifespan_context.client
        if db_name is None or db_name == "":
            db_name = taos.database

        if table_name:
            target = f"{db_name}.{table_name}"
        elif stable_name:
            target = f"{db_name}.{stable_name}"
        else:
            raise ValueError("Either stable_name or table_name must be specified")

        # Build SELECT clause
        select_clause = f"{agg_function.upper()}({column_name})"
        if interval:
            select_clause = f"_wstart, {select_clause}"

        # Build FROM clause
        from_clause = target

        # Build WHERE clause
        where_conditions = []
        if start_time:
            where_conditions.append(f"ts >= '{start_time}'")
        if end_time:
            where_conditions.append(f"ts <= '{end_time}'")

        where_clause = ""
        if where_conditions:
            where_clause = " WHERE " + " AND ".join(where_conditions)

        # Build GROUP BY clause
        group_by_clause = ""
        if interval:
            group_by_clause = f" INTERVAL({interval})"
        if group_by_tags:
            partition_by = ", ".join(group_by_tags)
            group_by_clause = f" PARTITION BY {partition_by}" + group_by_clause

        sql = f"SELECT {select_clause} FROM {from_clause}{where_clause}{group_by_clause};"
        result = taos.execute_sql(sql)
        return result

    @mcp.tool(name="get_tag_values")
    def get_tag_values(
        ctx: Context,
        db_name: Optional[str] = Field(
            None,
            description="The name of the database. Default is None which means the configured database.",
        ),
        stable_name: str = Field(description="The name of the stable"),
        tag_name: str = Field(description="The name of the tag"),
        limit: Optional[int] = Field(
            100,
            description="Maximum number of unique tag values to retrieve. Default is 100.",
        ),
    ) -> TaosSqlResponse:
        """Get distinct values for a specific tag in a stable.

        Args:
            db_name (Optional[str]): The name of the database. Defaults to None.
            stable_name (str): The name of the stable.
            tag_name (str): The name of the tag.
            limit (Optional[int]): Maximum number of unique tag values. Default is 100.

        Returns:
            TaosSqlResponse: Distinct tag values.
        """

        taos = ctx.request_context.lifespan_context.client
        if db_name is None or db_name == "":
            db_name = taos.database

        sql = f"SELECT DISTINCT {tag_name} FROM {db_name}.{stable_name}"
        if limit:
            sql += f" LIMIT {limit}"
        sql += ";"

        result = taos.execute_sql(sql)
        return result

    @mcp.tool(name="get_db_info")
    def get_db_info(
        ctx: Context,
        db_name: Optional[str] = Field(
            None,
            description="The name of the database. Default is None which means the configured database.",
        ),
    ) -> TaosSqlResponse:
        """Get detailed information about a database including configuration and statistics.

        Args:
            db_name (Optional[str]): The name of the database. Defaults to None.

        Returns:
            TaosSqlResponse: Database information and statistics.
        """

        taos = ctx.request_context.lifespan_context.client
        if db_name is None or db_name == "":
            db_name = taos.database

        sql = f"SELECT * FROM information_schema.ins_databases WHERE name='{db_name}';"
        result = taos.execute_sql(sql)
        return result

    @mcp.tool(name="check_data_integrity")
    def check_data_integrity(
        ctx: Context,
        db_name: Optional[str] = Field(
            None,
            description="The name of the database. Default is None which means the configured database.",
        ),
        stable_name: str = Field(description="The name of the stable"),
        check_nulls: bool = Field(
            True,
            description="Whether to check for NULL values. Default is True.",
        ),
        check_duplicates: bool = Field(
            False,
            description="Whether to check for duplicate timestamps. Default is False.",
        ),
    ) -> Dict[str, Any]:
        """Check data integrity for a stable including NULL values and duplicate timestamps.

        Args:
            db_name (Optional[str]): The name of the database. Defaults to None.
            stable_name (str): The name of the stable.
            check_nulls (bool): Whether to check for NULL values. Default is True.
            check_duplicates (bool): Whether to check for duplicate timestamps. Default is False.

        Returns:
            Dict[str, Any]: Data integrity check results.
        """

        taos = ctx.request_context.lifespan_context.client
        if db_name is None or db_name == "":
            db_name = taos.database

        results = {}

        # Get total row count
        total_count_sql = f"SELECT COUNT(*) as total_rows FROM {db_name}.{stable_name};"
        total_result = taos.execute_sql(total_count_sql)
        results["total_rows"] = total_result["data"][0][0] if total_result["data"] else 0

        if check_nulls:
            # Get column information to check for NULLs
            describe_sql = f"DESCRIBE {db_name}.{stable_name};"
            describe_result = taos.execute_sql(describe_sql)
            
            null_counts = {}
            for row in describe_result["data"]:
                col_name = row[0]
                if col_name != "ts":  # Skip timestamp column
                    null_sql = f"SELECT COUNT(*) as null_count FROM {db_name}.{stable_name} WHERE {col_name} IS NULL;"
                    try:
                        null_result = taos.execute_sql(null_sql)
                        null_counts[col_name] = null_result["data"][0][0] if null_result["data"] else 0
                    except Exception:
                        null_counts[col_name] = "Unable to check"
            
            results["null_counts"] = null_counts

        if check_duplicates:
            # Check for duplicate timestamps (should not exist in TDengine)
            dup_sql = f"SELECT ts, COUNT(*) as dup_count FROM {db_name}.{stable_name} GROUP BY ts HAVING COUNT(*) > 1 LIMIT 10;"
            try:
                dup_result = taos.execute_sql(dup_sql)
                results["duplicate_timestamps"] = dup_result["data"]
            except Exception as e:
                results["duplicate_timestamps"] = f"Error checking duplicates: {str(e)}"

        return results

    @mcp.tool(name="analyze_performance")
    def analyze_performance(
        ctx: Context,
        db_name: Optional[str] = Field(
            None,
            description="The name of the database. Default is None which means the configured database.",
        ),
        stable_name: Optional[str] = Field(
            None,
            description="The name of the stable to analyze. If None, analyze all stables.",
        ),
    ) -> Dict[str, Any]:
        """Analyze database/stable performance including data distribution and query hints.

        Args:
            db_name (Optional[str]): The name of the database. Defaults to None.
            stable_name (Optional[str]): The name of the stable. If None, analyze all stables.

        Returns:
            Dict[str, Any]: Performance analysis results.
        """

        taos = ctx.request_context.lifespan_context.client
        if db_name is None or db_name == "":
            db_name = taos.database

        results = {}

        if stable_name:
            # Analyze specific stable
            # Get time range
            time_range_sql = f"SELECT MIN(ts) as min_time, MAX(ts) as max_time FROM {db_name}.{stable_name};"
            time_result = taos.execute_sql(time_range_sql)
            results["time_range"] = time_result["data"][0] if time_result["data"] else [None, None]

            # Get record count
            count_sql = f"SELECT COUNT(*) as total_records FROM {db_name}.{stable_name};"
            count_result = taos.execute_sql(count_sql)
            results["total_records"] = count_result["data"][0][0] if count_result["data"] else 0

            # Get table count for this stable
            tables_sql = f"SELECT COUNT(*) as table_count FROM information_schema.ins_tables WHERE stable_name='{stable_name}' AND db_name='{db_name}';"
            tables_result = taos.execute_sql(tables_sql)
            results["table_count"] = tables_result["data"][0][0] if tables_result["data"] else 0

        else:
            # Analyze all stables in database
            stables_sql = f"SELECT stable_name, COUNT(*) as table_count FROM information_schema.ins_tables WHERE db_name='{db_name}' GROUP BY stable_name;"
            stables_result = taos.execute_sql(stables_sql)
            results["stables_summary"] = stables_result["data"] if stables_result["data"] else []

        return results

    @mcp.tool(name="comprehensive_stable_analysis")
    def comprehensive_stable_analysis(
        ctx: Context,
        db_name: Optional[str] = Field(
            None,
            description="The name of the database. Default is None which means the configured database.",
        ),
        stable_name: str = Field(description="The name of the stable to analyze"),
        include_sample_data: bool = Field(
            True,
            description="Whether to include sample data in analysis. Default is True.",
        ),
        days_back: Optional[int] = Field(
            7,
            description="Number of days back to analyze recent data. Default is 7 days.",
        ),
    ) -> Dict[str, Any]:
        """Perform comprehensive analysis of a stable using multiple tools working together.
        
        This tool combines multiple MCP tools to provide:
        - Basic stable information and schema
        - Data integrity checks
        - Performance analysis
        - Recent data sampling
        - Tag value distribution
        - Time-based statistics

        Args:
            db_name (Optional[str]): The name of the database. Defaults to None.
            stable_name (str): The name of the stable to analyze.
            include_sample_data (bool): Whether to include sample data. Default is True.
            days_back (Optional[int]): Number of days back for recent analysis. Default is 7.

        Returns:
            Dict[str, Any]: Comprehensive analysis results combining multiple tool outputs.
        """

        taos = ctx.request_context.lifespan_context.client
        if db_name is None or db_name == "":
            db_name = taos.database

        analysis_results = {
            "stable_name": stable_name,
            "database_name": db_name,
            "analysis_timestamp": "NOW()",
        }

        try:
            # 1. Get stable schema information
            schema_info = get_field_infos(ctx, db_name, stable_name)
            analysis_results["schema"] = {
                "columns": schema_info["data"],
                "column_count": len(schema_info["data"]) if schema_info["data"] else 0
            }

            # 2. Get tag information
            try:
                tag_info = get_tag_infos(ctx, db_name, stable_name)
                analysis_results["tags"] = {
                    "tag_columns": tag_info["data"],
                    "tag_count": len(tag_info["data"]) if tag_info["data"] else 0
                }
            except Exception as e:
                analysis_results["tags"] = {"error": f"Could not retrieve tag info: {str(e)}"}

            # 3. Get performance analysis
            performance = analyze_performance(ctx, db_name, stable_name)
            analysis_results["performance"] = performance

            # 4. Get table statistics
            stats = get_table_stats(ctx, db_name, stable_name, None)
            analysis_results["statistics"] = stats

            # 5. Data integrity check
            integrity = check_data_integrity(ctx, db_name, stable_name, True, False)
            analysis_results["data_integrity"] = integrity

            # 6. Get recent data if requested
            if include_sample_data:
                try:
                    latest_data = get_latest_data(ctx, db_name, stable_name, None, 5)
                    analysis_results["sample_data"] = {
                        "latest_records": latest_data["data"],
                        "sample_count": len(latest_data["data"]) if latest_data["data"] else 0
                    }
                except Exception as e:
                    analysis_results["sample_data"] = {"error": f"Could not retrieve sample data: {str(e)}"}

            # 7. Time range analysis for recent days
            if days_back and days_back > 0:
                try:
                    from datetime import datetime, timedelta
                    end_time = datetime.now()
                    start_time = end_time - timedelta(days=days_back)
                    
                    start_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
                    end_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
                    
                    recent_count_sql = f"SELECT COUNT(*) as recent_count FROM {db_name}.{stable_name} WHERE ts >= '{start_str}' AND ts <= '{end_str}';"
                    recent_result = taos.execute_sql(recent_count_sql)
                    
                    analysis_results["recent_activity"] = {
                        "days_analyzed": days_back,
                        "time_range": f"{start_str} to {end_str}",
                        "recent_record_count": recent_result["data"][0][0] if recent_result["data"] else 0
                    }
                except Exception as e:
                    analysis_results["recent_activity"] = {"error": f"Could not analyze recent activity: {str(e)}"}

            # 8. Tag value distribution analysis (for first tag column if exists)
            if "tags" in analysis_results and isinstance(analysis_results["tags"], dict) and analysis_results["tags"].get("tag_columns"):
                try:
                    first_tag = analysis_results["tags"]["tag_columns"][0][0]  # Get first tag column name
                    tag_values = get_tag_values(ctx, db_name, stable_name, first_tag, 20)
                    analysis_results["tag_distribution"] = {
                        "tag_name": first_tag,
                        "unique_values": tag_values["data"],
                        "unique_count": len(tag_values["data"]) if tag_values["data"] else 0
                    }
                except Exception as e:
                    analysis_results["tag_distribution"] = {"error": f"Could not analyze tag distribution: {str(e)}"}

            analysis_results["analysis_status"] = "completed"

        except Exception as e:
            analysis_results["analysis_status"] = "failed"
            analysis_results["error"] = str(e)

        return analysis_results

    @mcp.tool(name="time_series_dashboard_data")
    def time_series_dashboard_data(
        ctx: Context,
        db_name: Optional[str] = Field(
            None,
            description="The name of the database. Default is None which means the configured database.",
        ),
        stable_name: str = Field(description="The name of the stable"),
        metric_column: str = Field(description="The column name for the main metric to analyze"),
        time_range_hours: int = Field(
            24,
            description="Number of hours back to analyze. Default is 24 hours.",
        ),
        interval_minutes: int = Field(
            60,
            description="Aggregation interval in minutes. Default is 60 minutes.",
        ),
        group_by_tag: Optional[str] = Field(
            None,
            description="Tag column to group results by. Default is None.",
        ),
    ) -> Dict[str, Any]:
        """Generate dashboard-ready time series data using multiple aggregation tools.
        
        This composite tool provides data suitable for time series dashboards by combining:
        - Time-based aggregations with intervals
        - Statistical summaries
        - Tag-based grouping
        - Recent trends analysis

        Args:
            db_name (Optional[str]): The name of the database. Defaults to None.
            stable_name (str): The name of the stable.
            metric_column (str): The column name for the main metric.
            time_range_hours (int): Hours back to analyze. Default is 24.
            interval_minutes (int): Aggregation interval in minutes. Default is 60.
            group_by_tag (Optional[str]): Tag to group by. Default is None.

        Returns:
            Dict[str, Any]: Dashboard-ready time series data.
        """

        taos = ctx.request_context.lifespan_context.client
        if db_name is None or db_name == "":
            db_name = taos.database

        from datetime import datetime, timedelta

        # Calculate time range
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=time_range_hours)
        start_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
        end_str = end_time.strftime('%Y-%m-%d %H:%M:%S')

        dashboard_data = {
            "stable_name": stable_name,
            "metric_column": metric_column,
            "time_range": f"{start_str} to {end_str}",
            "interval": f"{interval_minutes}m",
            "group_by_tag": group_by_tag
        }

        try:
            # 1. Get time series data with aggregation
            group_by_tags = [group_by_tag] if group_by_tag else None
            
            # Average values over time
            avg_data = aggregate_query(
                ctx, db_name, stable_name, None, "avg", metric_column, 
                f"{interval_minutes}m", group_by_tags, start_str, end_str
            )
            dashboard_data["avg_time_series"] = avg_data

            # Max values over time
            max_data = aggregate_query(
                ctx, db_name, stable_name, None, "max", metric_column,
                f"{interval_minutes}m", group_by_tags, start_str, end_str
            )
            dashboard_data["max_time_series"] = max_data

            # Min values over time
            min_data = aggregate_query(
                ctx, db_name, stable_name, None, "min", metric_column,
                f"{interval_minutes}m", group_by_tags, start_str, end_str
            )
            dashboard_data["min_time_series"] = min_data

            # 2. Get overall statistics for the time period
            overall_stats = {}
            
            # Overall average
            overall_avg = aggregate_query(
                ctx, db_name, stable_name, None, "avg", metric_column,
                None, group_by_tags, start_str, end_str
            )
            overall_stats["average"] = overall_avg
            
            # Overall count
            overall_count = aggregate_query(
                ctx, db_name, stable_name, None, "count", metric_column,
                None, group_by_tags, start_str, end_str
            )
            overall_stats["count"] = overall_count

            dashboard_data["overall_statistics"] = overall_stats

            # 3. Get latest values for real-time display
            latest_data = get_latest_data(ctx, db_name, stable_name, None, 1)
            dashboard_data["latest_value"] = latest_data

            # 4. If grouping by tag, get tag distribution
            if group_by_tag:
                tag_values = get_tag_values(ctx, db_name, stable_name, group_by_tag, 50)
                dashboard_data["tag_distribution"] = tag_values

            dashboard_data["status"] = "success"

        except Exception as e:
            dashboard_data["status"] = "error"
            dashboard_data["error"] = str(e)

        return dashboard_data


def register_resources(mcp: FastMCP):
    taos = TAOSClient(**mcp.config)

    @mcp.resource("taos://database", mime_type="text/plain")
    def get_current_taos_database() -> List:
        """Get current mysql database."""

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

    mcp_app = FastMCP(
        name="[TDengine-MCP-Server]",
        description="TDengine-MCP-Server",
        lifespan=server_lifespan,
        dependencies=["dotenv", "taospy"],
    )
    mcp_app.config = get_taos_config(args)

    for register_func in (register_prompts, register_tools, register_resources):
        register_func(mcp_app)

    _transort = os.environ.get("TRANSPORT", args.transport)
    logger.info(f"[TDengine-MCP-Server] server started with transport: {_transort}")
    mcp_app.run(transport=_transort)
