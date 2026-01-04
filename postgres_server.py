
# Import statements and MCP initialization
import os
import json
import logging
from typing import Any, Dict, List, Optional
import asyncpg
from mcp.server.fastmcp import FastMCP, Context
from pydantic import BaseModel, Field

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize FastMCP server
mcp = FastMCP("PostgreSQL Server")

# Database configuration from environment variables
DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "postgresql://postgres:password@127.0.0.1:5432/postgres"
)

# Replace localhost with 127.0.0.1 to avoid DNS issues
if 'localhost' in DATABASE_URL:
    DATABASE_URL = DATABASE_URL.replace('localhost', '127.0.0.1')
    logger.info(f"Replaced localhost with 127.0.0.1 in DATABASE_URL")

logger.info(f"Using DATABASE_URL: {DATABASE_URL.replace(DATABASE_URL.split('@')[0].split('//')[1], '***:***')}")

# Connection pool for better performance
connection_pool = None

async def get_pool():
    """Get or create the connection pool."""
    global connection_pool
    if connection_pool is None:
        try:
            logger.info("Attempting to create database connection pool...")
            logger.info(f"Connecting to host: {DATABASE_URL.split('@')[1].split('/')[0].split(':')[0]}")
            connection_pool = await asyncpg.create_pool(
                DATABASE_URL,
                min_size=1,
                max_size=10,
                command_timeout=30
            )
            logger.info("✅ Database connection pool created successfully")
        except Exception as e:
            logger.error(f"❌ Failed to create database connection pool: {str(e)}")
            logger.error(f"Connection URL format: postgresql://user:***@host:port/database")
            raise Exception(f"Database connection failed: {str(e)}")
    return connection_pool

async def execute_query(query: str, *args) -> List[Dict[str, Any]]:
    """Execute a query and return results as a list of dictionaries."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        try:
            rows = await conn.fetch(query, *args)
            return [dict(row) for row in rows]
        except Exception as e:
            raise Exception(f"Database error: {str(e)}")

async def execute_non_query(query: str, *args) -> str:
    """Execute a non-query (INSERT, UPDATE, DELETE) and return affected rows count."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        try:
            result = await conn.execute(query, *args)
            return result
        except Exception as e:
            raise Exception(f"Database error: {str(e)}")

# Pydantic models for structured output
class TableInfo(BaseModel):
    """Information about a database table."""
    table_name: str = Field(description="Name of the table")
    table_type: str = Field(description="Type of the table (TABLE, VIEW, etc.)")
    table_schema: str = Field(description="Schema containing the table")

class ColumnInfo(BaseModel):
    """Information about a table column."""
    column_name: str = Field(description="Name of the column")
    data_type: str = Field(description="Data type of the column")
    is_nullable: str = Field(description="Whether the column can be NULL")
    column_default: Optional[str] = Field(description="Default value of the column")

class QueryResult(BaseModel):
    """Result of a database query."""
    rows: List[Dict[str, Any]] = Field(description="Query result rows")
    row_count: int = Field(description="Number of rows returned")

# PostgreSQL Tools

@mcp.tool()
async def PostgreSQL_get_lock_statistics() -> List[Dict[str, Any]]:
    """Get detailed statistics of database locks, including wait times."""
    query = """
        SELECT 
            locktype,
            mode,
            count(*) as count,
            max(EXTRACT(epoch FROM (now() - query_start))) as max_wait_seconds,
            avg(EXTRACT(epoch FROM (now() - query_start))) as avg_wait_seconds,
            min(EXTRACT(epoch FROM (now() - query_start))) as min_wait_seconds
        FROM pg_locks l
        JOIN pg_stat_activity a ON l.pid = a.pid
        WHERE NOT l.granted
        GROUP BY locktype, mode
        ORDER BY count DESC, locktype, mode
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_prepared_transactions() -> List[Dict[str, Any]]:
    """Get the list and stats of prepared transactions, if any."""
    query = """
        SELECT 
            gid as transaction_id,
            prepared,
            owner,
            database
        FROM pg_prepared_xacts
        ORDER BY prepared
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_detailed_foreign_tables() -> List[Dict[str, Any]]:
    """Get advanced details about foreign tables and their servers."""
    query = """
        SELECT 
            n.nspname as schema_name,
            c.relname as table_name,
            s.srvname as server_name,
            w.fdwname as wrapper_name,
            u.rolname as table_owner,
            ft.ftoptions as foreign_table_options,
            s.srvoptions as server_options
        FROM pg_foreign_table ft
        JOIN pg_class c ON ft.ftrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        JOIN pg_foreign_server s ON ft.ftserver = s.oid
        JOIN pg_foreign_data_wrapper w ON s.srvfdw = w.oid
        JOIN pg_roles u ON c.relowner = u.oid
        ORDER BY n.nspname, c.relname
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_event_triggers_detailed() -> List[Dict[str, Any]]:
    """Get comprehensive details about event triggers."""
    query = """
        SELECT 
            et.evtname as trigger_name,
            et.evtevent as event,
            r.rolname as owner,
            p.proname as function_name,
            n.nspname as function_schema,
            et.evtenabled as is_enabled,
            et.evttags as filter_tags,
            obj_description(et.oid, 'pg_event_trigger') as description
        FROM pg_event_trigger et
        JOIN pg_roles r ON et.evtowner = r.oid
        JOIN pg_proc p ON et.evtfoid = p.oid
        JOIN pg_namespace n ON p.pronamespace = n.oid
        ORDER BY et.evtname
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_publication_details() -> List[Dict[str, Any]]:
    """Retrieve details of all logical replication publications."""
    query = """
        SELECT 
            p.pubname as publication_name,
            r.rolname as owner,
            p.puballtables as publishes_all_tables,
            p.pubinsert as publishes_insert,
            p.pubupdate as publishes_update,
            p.pubdelete as publishes_delete,
            p.pubtruncate as publishes_truncate,
            (
                SELECT count(*) FROM pg_publication_rel pr 
                WHERE pr.prpubid = p.oid
            ) as table_count
        FROM pg_publication p
        JOIN pg_roles r ON p.pubowner = r.oid
        ORDER BY p.pubname
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_full_text_search_configs() -> List[Dict[str, Any]]:
    """List all full-text search configurations available."""
    query = """
        SELECT 
            n.nspname as schema_name,
            c.cfgname as config_name,
            r.rolname as owner,
            p.prsname as parser_name,
            obj_description(c.oid, 'pg_ts_config') as description
        FROM pg_ts_config c
        JOIN pg_namespace n ON c.cfgnamespace = n.oid
        JOIN pg_roles r ON c.cfgowner = r.oid
        JOIN pg_ts_parser p ON c.cfgparser = p.oid
        ORDER BY n.nspname, c.cfgname
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_autovacuum_stats_per_table() -> List[Dict[str, Any]]:
    """Get autovacuum operation stats per table."""
    query = """
        SELECT 
            schemaname,
            tablename,
            last_vacuum,
            last_autovacuum,
            last_analyze,
            last_autoanalyze,
            vacuum_count,
            autovacuum_count,
            analyze_count,
            autoanalyze_count,
            n_dead_tup as dead_tuples,
            n_live_tup as live_tuples,
            CASE 
                WHEN n_live_tup + n_dead_tup > 0 THEN
                    ROUND((n_dead_tup::float / (n_live_tup + n_dead_tup)) * 100, 2)
                ELSE 0
            END as dead_tuple_ratio
        FROM pg_stat_user_tables
        ORDER BY 
            CASE WHEN last_autovacuum IS NULL THEN '1970-01-01'::timestamp ELSE last_autovacuum END ASC,
            dead_tuple_ratio DESC
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_foreign_keys_referencing_table(table_name: str, schema_name: str = "public") -> List[Dict[str, Any]]:
    """List tables referencing the specified table via foreign keys."""
    query = """
        SELECT DISTINCT
            tc.table_schema as referencing_schema,
            tc.table_name as referencing_table,
            tc.constraint_name,
            kcu.column_name as referencing_column,
            ccu.column_name as referenced_column,
            rc.update_rule,
            rc.delete_rule
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
        JOIN information_schema.referential_constraints AS rc
            ON tc.constraint_name = rc.constraint_name
            AND tc.table_schema = rc.constraint_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
            AND ccu.table_schema = $1
            AND ccu.table_name = $2
        ORDER BY tc.table_schema, tc.table_name, tc.constraint_name
    """
    
    rows = await execute_query(query, schema_name, table_name)
    return rows

@mcp.tool()
async def PostgreSQL_get_table_rules() -> List[Dict[str, Any]]:
    """Get all rules defined on tables."""
    query = """
        SELECT 
            n.nspname as schema_name,
            c.relname as table_name,
            r.rulename as rule_name,
            CASE r.ev_type
                WHEN '1' THEN 'SELECT'
                WHEN '2' THEN 'UPDATE'
                WHEN '3' THEN 'INSERT'
                WHEN '4' THEN 'DELETE'
                ELSE 'UNKNOWN'
            END as event_type,
            r.is_instead as is_instead,
            pg_get_ruledef(r.oid) as rule_definition
        FROM pg_rewrite r
        JOIN pg_class c ON r.ev_class = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE r.rulename != '_RETURN'
        AND n.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        ORDER BY n.nspname, c.relname, r.rulename
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_partition_details() -> List[Dict[str, Any]]:
    """Get partitioning details of tables and partition strategies."""
    query = """
        SELECT 
            n.nspname as schema_name,
            c.relname as table_name,
            CASE c.relkind
                WHEN 'p' THEN 'PARTITION'
                WHEN 'r' THEN 'TABLE'
                ELSE 'OTHER'
            END as table_type,
            CASE 
                WHEN c.relkind = 'p' THEN pg_get_partkeydef(c.oid)
                ELSE NULL
            END as partition_info,
            c.relispartition as is_partition,
            (
                SELECT n2.nspname || '.' || c2.relname
                FROM pg_inherits i
                JOIN pg_class c2 ON i.inhparent = c2.oid
                JOIN pg_namespace n2 ON c2.relnamespace = n2.oid
                WHERE i.inhrelid = c.oid
            ) as parent_table,
            (
                SELECT count(*)
                FROM pg_inherits i
                WHERE i.inhparent = c.oid
            ) as child_partitions,
            pg_size_pretty(pg_total_relation_size(c.oid)) as total_size
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE (c.relkind = 'p' OR c.relispartition = TRUE)
        AND n.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        ORDER BY n.nspname, c.relname
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_replication_slot_infos() -> List[Dict[str, Any]]:
    """Get detailed replication slot information."""
    query = """
        SELECT 
            slot_name,
            plugin,
            slot_type,
            database,
            temporary,
            active,
            active_pid,
            xmin,
            catalog_xmin,
            restart_lsn,
            confirmed_flush_lsn,
            wal_status,
            (SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)).
            as replication_lag_size,
            CASE 
                WHEN active THEN 'ACTIVE'
                ELSE 'INACTIVE'
            END as status
        FROM pg_replication_slots
        ORDER BY slot_name
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_backup_details() -> Dict[str, Any]:
    """Get last known backup status and WAL archiving info."""
    query = """
        SELECT 
            archived_count,
            failed_count,
            last_archived_wal,
            last_archived_time,
            stats_reset,
            archived_count - failed_count as successful_count
        FROM pg_stat_archiver
    """
    
    rows = await execute_query(query)
    return rows[0] if rows else {}

@mcp.tool()
async def PostgreSQL_get_query_plan_cache_stats() -> List[Dict[str, Any]]:
    """Get statistics about cached query plans and their effectiveness."""
    query = """
        SELECT 
            schemaname,
            tablename,
            seq_scan,
            seq_tup_read,
            idx_scan,
            idx_tup_fetch,
            n_tup_ins,
            n_tup_upd,
            n_tup_del,
            n_tup_hot_upd,
            CASE 
                WHEN seq_scan + idx_scan > 0 THEN
                    ROUND((idx_scan::float / (seq_scan + idx_scan)) * 100, 2)
                ELSE 0
            END as index_usage_ratio,
            CASE 
                WHEN n_tup_upd > 0 THEN
                    ROUND((n_tup_hot_upd::float / n_tup_upd) * 100, 2)
                ELSE 0
            END as hot_update_ratio
        FROM pg_stat_user_tables
        WHERE seq_scan + idx_scan > 0
        ORDER BY seq_scan + idx_scan DESC
        LIMIT 50
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_constraint_violations() -> List[Dict[str, Any]]:
    """Check for potential constraint violations and data integrity issues."""
    query = """
        WITH constraint_info AS (
            SELECT 
                tc.table_schema,
                tc.table_name,
                tc.constraint_name,
                tc.constraint_type,
                cc.check_clause
            FROM information_schema.table_constraints tc
            LEFT JOIN information_schema.check_constraints cc
                ON tc.constraint_name = cc.constraint_name
            WHERE tc.constraint_type IN ('CHECK', 'FOREIGN KEY', 'UNIQUE')
            AND tc.table_schema NOT IN ('information_schema', 'pg_catalog')
        )
        SELECT 
            table_schema,
            table_name,
            constraint_name,
            constraint_type,
            check_clause,
            CASE constraint_type
                WHEN 'CHECK' THEN 'Verify check constraint logic'
                WHEN 'FOREIGN KEY' THEN 'Check referential integrity'
                WHEN 'UNIQUE' THEN 'Check for duplicate values'
                ELSE 'Unknown constraint type'
            END as suggested_action
        FROM constraint_info
        ORDER BY table_schema, table_name, constraint_type
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_slow_query_patterns() -> List[Dict[str, Any]]:
    """Analyze patterns in slow queries from pg_stat_statements."""
    query = """
        SELECT 
            LEFT(query, 100) as query_pattern,
            calls,
            total_exec_time,
            mean_exec_time,
            stddev_exec_time,
            rows,
            100.0 * shared_blks_hit / nullif(shared_blks_hit + shared_blks_read, 0) AS hit_percent,
            shared_blks_read,
            shared_blks_written,
            temp_blks_read,
            temp_blks_written,
            CASE 
                WHEN mean_exec_time > 1000 THEN 'VERY SLOW'
                WHEN mean_exec_time > 500 THEN 'SLOW'
                WHEN mean_exec_time > 100 THEN 'MODERATE'
                ELSE 'FAST'
            END as performance_category
        FROM pg_stat_statements
        WHERE mean_exec_time > 10  -- Only queries slower than 10ms
        ORDER BY mean_exec_time DESC
        LIMIT 25
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_database_growth_trend() -> List[Dict[str, Any]]:
    """Analyze database growth patterns over time using pg_stat_database."""
    query = """
        SELECT 
            datname as database_name,
            pg_size_pretty(pg_database_size(datname)) as current_size,
            pg_database_size(datname) as size_bytes,
            xact_commit,
            xact_rollback,
            CASE 
                WHEN xact_commit + xact_rollback > 0 THEN
                    ROUND((xact_rollback::float / (xact_commit + xact_rollback)) * 100, 2)
                ELSE 0
            END as rollback_ratio,
            blks_read,
            blks_hit,
            CASE 
                WHEN blks_read + blks_hit > 0 THEN
                    ROUND((blks_hit::float / (blks_read + blks_hit)) * 100, 2)
                ELSE 0
            END as cache_hit_ratio,
            tup_returned,
            tup_fetched,
            tup_inserted,
            tup_updated,
            tup_deleted,
            stats_reset
        FROM pg_stat_database
        WHERE datname NOT IN ('template0', 'template1', 'postgres')
        ORDER BY pg_database_size(datname) DESC
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_vacuum_progress() -> List[Dict[str, Any]]:
    """Monitor currently running VACUUM operations and their progress."""
    query = """
        SELECT 
            p.pid,
            p.datname as database_name,
            p.relid::regclass as table_name,
            p.phase,
            p.heap_tuples_scanned,
            p.heap_tuples_vacuumed,
            p.index_vacuum_count,
            p.max_dead_tuples,
            p.num_dead_tuples,
            CASE 
                WHEN p.heap_tuples_scanned > 0 AND p.phase != 'initializing' THEN
                    ROUND((p.heap_tuples_vacuumed::float / p.heap_tuples_scanned) * 100, 2)
                ELSE 0
            END as progress_percent,
            a.query_start,
            NOW() - a.query_start as duration
        FROM pg_stat_progress_vacuum p
        JOIN pg_stat_activity a ON p.pid = a.pid
        ORDER BY a.query_start
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_materialized_view_stats() -> List[Dict[str, Any]]:
    """Get statistics and refresh information for materialized views."""
    query = """
        SELECT 
            n.nspname as schema_name,
            c.relname as view_name,
            c.ispopulated as is_populated,
            pg_size_pretty(pg_total_relation_size(c.oid)) as size,
            pg_stat_get_numscans(c.oid) as seq_scans,
            pg_stat_get_tuples_returned(c.oid) as tuples_read,
            pg_stat_get_tuples_fetched(c.oid) as tuples_fetched,
            obj_description(c.oid, 'pg_class') as description,
            CASE 
                WHEN c.ispopulated THEN 'POPULATED'
                ELSE 'NOT POPULATED'
            END as status,
            (
                SELECT COUNT(*)
                FROM pg_depend d
                WHERE d.objid = c.oid
                AND d.deptype = 'n'
            ) as dependency_count
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE c.relkind = 'm'
        AND n.nspname NOT IN ('information_schema', 'pg_catalog')
        ORDER BY n.nspname, c.relname
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_publication_subscription_details() -> Dict[str, List[Dict[str, Any]]]:
    """Get logical replication publication and subscription details."""
    # Get publications
    pub_query = """
        SELECT 
            pubname as name,
            pubowner::regrole as owner,
            puballtables as all_tables,
            pubinsert as replicate_insert,
            pubupdate as replicate_update,
            pubdelete as replicate_delete,
            pubtruncate as replicate_truncate
        FROM pg_publication
        ORDER BY pubname
    """
    
    # Get subscriptions  
    sub_query = """
        SELECT 
            subname as name,
            subowner::regrole as owner,
            subenabled as enabled,
            subconninfo as connection_info,
            subslotname as slot_name,
            subsynccommit as sync_commit,
            subpublications as publications
        FROM pg_subscription
        ORDER BY subname
    """
    
    publications = await execute_query(pub_query)
    subscriptions = await execute_query(sub_query)
    
    return {
        "publications": publications,
        "subscriptions": subscriptions
    }

@mcp.tool()
async def PostgreSQL_get_sequence_usage_stats() -> List[Dict[str, Any]]:
    """Analyze sequence usage patterns and potential exhaustion risks."""
    query = """
        SELECT 
            n.nspname as schema_name,
            c.relname as sequence_name,
            s.last_value,
            s.start_value,
            s.increment_by,
            s.max_value,
            s.min_value,
            s.cache_size,
            s.is_cycled,
            CASE 
                WHEN s.increment_by > 0 THEN
                    ROUND(((s.last_value - s.start_value)::float / (s.max_value - s.start_value)) * 100, 2)
                ELSE
                    ROUND(((s.start_value - s.last_value)::float / (s.start_value - s.min_value)) * 100, 2)
            END as usage_percent,
            CASE 
                WHEN s.increment_by > 0 AND s.last_value > (s.max_value * 0.8) THEN 'HIGH RISK'
                WHEN s.increment_by > 0 AND s.last_value > (s.max_value * 0.6) THEN 'MEDIUM RISK'
                WHEN s.increment_by < 0 AND s.last_value < (s.min_value * 0.8) THEN 'HIGH RISK'
                WHEN s.increment_by < 0 AND s.last_value < (s.min_value * 0.6) THEN 'MEDIUM RISK'
                ELSE 'LOW RISK'
            END as exhaustion_risk,
            pg_sequence_parameters(c.oid) as parameters
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        JOIN pg_sequences s ON s.schemaname = n.nspname AND s.sequencename = c.relname
        WHERE c.relkind = 'S'
        AND n.nspname NOT IN ('information_schema', 'pg_catalog')
        ORDER BY usage_percent DESC
    """
    
    rows = await execute_query(query)
    return rows

# Tools
@mcp.tool()
async def PostgreSQL_list_tables(schema_name: str = "public") -> List[TableInfo]:
    """List all tables in a schema.
    
    Args:
        schema_name: Database schema name (default: public)
    """
    query = """
        SELECT table_name, table_type, table_schema
        FROM information_schema.tables
        WHERE table_schema = $1
        ORDER BY table_name
    """
    
    rows = await execute_query(query, schema_name)
    return [TableInfo(**row) for row in rows]

@mcp.tool()
async def PostgreSQL_describe_table(table_name: str, schema_name: str = "public") -> List[ColumnInfo]:
    """Get detailed information about a table's columns.
    
    Args:
        table_name: Name of the table to describe
        schema_name: Database schema name (default: public)
    """
    query = """
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2
        ORDER BY ordinal_position
    """
    
    rows = await execute_query(query, schema_name, table_name)
    if not rows:
        raise ValueError(f"Table '{table_name}' not found in schema '{schema_name}'")
    
    return [ColumnInfo(**row) for row in rows]

@mcp.tool()
async def PostgreSQL_execute_select_query(query: str, ctx: Context) -> QueryResult:
    """Execute a SELECT query and return results.
    
    Args:
        query: SQL SELECT query to execute
    """
    # Basic validation to ensure it's a SELECT query
    if not query.strip().upper().startswith("SELECT"):
        raise ValueError("Only SELECT queries are allowed")
    
    await ctx.info(f"Executing query: {query[:100]}...")
    
    rows = await execute_query(query)
    result = QueryResult(rows=rows, row_count=len(rows))
    
    await ctx.info(f"Query returned {result.row_count} rows")
    return result

@mcp.tool()
async def PostgreSQL_execute_update_query(query: str, ctx: Context) -> str:
    """Execute an UPDATE, INSERT, or DELETE query.
    
    Args:
        query: SQL query to execute (INSERT, UPDATE, DELETE)
    """
    # Basic validation
    query_upper = query.strip().upper()
    if not any(query_upper.startswith(cmd) for cmd in ["INSERT", "UPDATE", "DELETE"]):
        raise ValueError("Only INSERT, UPDATE, and DELETE queries are allowed")
    
    await ctx.warning(f"Executing potentially destructive query: {query[:100]}...")
    
    result = await execute_non_query(query)
    
    await ctx.info(f"Query executed successfully: {result}")
    return f"Query executed successfully: {result}"

@mcp.tool()
async def PostgreSQL_get_table_size(table_name: str, schema_name: str = "public") -> Dict[str, Any]:
    """Get the size information for a table.
    
    Args:
        table_name: Name of the table
        schema_name: Database schema name (default: public)
    """
    query = """
        SELECT 
            schemaname,
            tablename,
            attname,
            n_distinct,
            most_common_vals
        FROM pg_stats 
        WHERE schemaname = $1 AND tablename = $2
        LIMIT 10
    """
    
    size_query = """
        SELECT 
            pg_size_pretty(pg_total_relation_size($1)) as total_size,
            pg_size_pretty(pg_relation_size($1)) as table_size,
            (SELECT COUNT(*) FROM {}.{}) as row_count
    """.format(schema_name, table_name)
    
    full_table_name = f"{schema_name}.{table_name}"
    
    stats = await execute_query(query, schema_name, table_name)
    size_info = await execute_query(size_query, full_table_name)
    
    return {
        "table": f"{schema_name}.{table_name}",
        "size_info": size_info[0] if size_info else {},
        "column_stats": stats
    }

@mcp.tool()
async def PostgreSQL_get_table_count(table_name: str, schema_name: str = "public") -> Dict[str, Any]:
    """Get the row count of a specific table.
    
    Args:
        table_name: Name of the table
        schema_name: Database schema name (default: public)
    """
    query = f"SELECT COUNT(*) as row_count FROM {schema_name}.{table_name}"
    result = await execute_query(query)
    return {
        "table": f"{schema_name}.{table_name}",
        "row_count": result[0]["row_count"] if result else 0
    }

@mcp.tool()
async def PostgreSQL_list_indexes(table_name: str, schema_name: str = "public") -> List[Dict[str, Any]]:
    """List all indexes on a specific table.
    
    Args:
        table_name: Name of the table
        schema_name: Database schema name (default: public)
    """
    query = """
        SELECT 
            i.indexname as index_name,
            i.indexdef as index_definition,
            am.amname as index_type,
            CASE WHEN i.indisunique THEN 'YES' ELSE 'NO' END as is_unique,
            CASE WHEN i.indisprimary THEN 'YES' ELSE 'NO' END as is_primary
        FROM pg_indexes i
        JOIN pg_class c ON c.relname = i.tablename
        JOIN pg_index idx ON idx.indexrelid = (SELECT oid FROM pg_class WHERE relname = i.indexname)
        JOIN pg_am am ON am.oid = (SELECT relam FROM pg_class WHERE relname = i.indexname)
        WHERE i.schemaname = $1 AND i.tablename = $2
        ORDER BY i.indexname
    """
    
    rows = await execute_query(query, schema_name, table_name)
    return rows

@mcp.tool()
async def PostgreSQL_list_schemas() -> List[Dict[str, Any]]:
    """List all schemas in the database."""
    query = """
        SELECT 
            schema_name,
            schema_owner,
            CASE 
                WHEN schema_name IN ('information_schema', 'pg_catalog', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1') 
                THEN 'SYSTEM'
                ELSE 'USER'
            END as schema_type
        FROM information_schema.schemata
        ORDER BY 
            CASE WHEN schema_name = 'public' THEN 0 ELSE 1 END,
            schema_name
    """
    
    rows = await execute_query(query)
    return rows

# Additional PostgreSQL Administrative Tools

@mcp.tool()
async def PostgreSQL_list_sequences(schema_name: str = "public") -> List[Dict[str, Any]]:
    """List all sequences in a schema.
    
    Args:
        schema_name: Database schema name (default: public)
    """
    query = """
        SELECT 
            sequence_name,
            data_type,
            start_value,
            minimum_value,
            maximum_value,
            increment
        FROM information_schema.sequences
        WHERE sequence_schema = $1
        ORDER BY sequence_name
    """
    
    rows = await execute_query(query, schema_name)
    return rows

@mcp.tool()
async def PostgreSQL_get_sequence_value(sequence_name: str, schema_name: str = "public") -> Dict[str, Any]:
    """Get the next value of a sequence without incrementing it.
    
    Args:
        sequence_name: Name of the sequence
        schema_name: Database schema name (default: public)
    """
    query = f"SELECT currval('{schema_name}.{sequence_name}') as current_value, nextval('{schema_name}.{sequence_name}') as next_value"
    
    try:
        result = await execute_query(query)
        return {
            "sequence": f"{schema_name}.{sequence_name}",
            "current_value": result[0]["current_value"],
            "next_value": result[0]["next_value"]
        }
    except Exception as e:
        # If sequence hasn't been used yet, currval will fail, so just get nextval
        query = f"SELECT nextval('{schema_name}.{sequence_name}') as next_value"
        result = await execute_query(query)
        return {
            "sequence": f"{schema_name}.{sequence_name}",
            "current_value": None,
            "next_value": result[0]["next_value"]
        }

@mcp.tool()
async def PostgreSQL_list_users_and_roles() -> List[Dict[str, Any]]:
    """List all database users and roles with their attributes."""
    query = """
        SELECT 
            rolname as role_name,
            rolsuper as is_superuser,
            rolinherit as can_inherit,
            rolcreaterole as can_create_roles,
            rolcreatedb as can_create_databases,
            rolcanlogin as can_login,
            rolreplication as can_replicate,
            rolconnlimit as connection_limit,
            rolvaliduntil as valid_until
        FROM pg_roles
        ORDER BY rolname
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_active_connections() -> List[Dict[str, Any]]:
    """Get information about currently active database connections."""
    query = """
        SELECT 
            pid,
            usename as username,
            datname as database,
            client_addr as client_address,
            client_port,
            backend_start,
            query_start,
            state,
            query
        FROM pg_stat_activity
        WHERE state != 'idle'
        ORDER BY backend_start DESC
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_database_size() -> List[Dict[str, Any]]:
    """Get size information for all databases in the cluster."""
    query = """
        SELECT 
            datname as database_name,
            pg_size_pretty(pg_database_size(datname)) as size,
            pg_database_size(datname) as size_bytes
        FROM pg_database
        WHERE datistemplate = false
        ORDER BY pg_database_size(datname) DESC
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_tablespace_info() -> List[Dict[str, Any]]:
    """Get information about all tablespaces."""
    query = """
        SELECT 
            spcname as tablespace_name,
            pg_catalog.pg_get_userbyid(spcowner) as owner,
            pg_catalog.pg_tablespace_location(oid) as location,
            spcacl as access_privileges
        FROM pg_tablespace
        ORDER BY spcname
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_list_views(schema_name: str = "public") -> List[Dict[str, Any]]:
    """List all views in a schema.
    
    Args:
        schema_name: Database schema name (default: public)
    """
    query = """
        SELECT 
            table_name as view_name,
            view_definition
        FROM information_schema.views
        WHERE table_schema = $1
        ORDER BY table_name
    """
    
    rows = await execute_query(query, schema_name)
    return rows

@mcp.tool()
async def PostgreSQL_list_functions(schema_name: str = "public") -> List[Dict[str, Any]]:
    """List all functions and their definitions in a schema.
    
    Args:
        schema_name: Database schema name (default: public)
    """
    query = """
        SELECT 
            routine_name as function_name,
            routine_type as function_type,
            data_type as return_type,
            routine_definition as function_definition,
            is_deterministic
        FROM information_schema.routines
        WHERE routine_schema = $1
        ORDER BY routine_name
    """
    
    rows = await execute_query(query, schema_name)
    return rows

@mcp.tool()
async def PostgreSQL_get_table_constraints(table_name: str, schema_name: str = "public") -> List[Dict[str, Any]]:
    """Get all constraints (primary key, foreign keys, etc.) for a table.
    
    Args:
        table_name: Name of the table
        schema_name: Database schema name (default: public)
    """
    query = """
        SELECT 
            tc.constraint_name,
            tc.constraint_type,
            kcu.column_name,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name,
            rc.update_rule,
            rc.delete_rule
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        LEFT JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
        LEFT JOIN information_schema.referential_constraints AS rc
            ON tc.constraint_name = rc.constraint_name
            AND tc.table_schema = rc.constraint_schema
        WHERE tc.table_name = $1 AND tc.table_schema = $2
        ORDER BY tc.constraint_type, tc.constraint_name
    """
    
    rows = await execute_query(query, table_name, schema_name)
    return rows

@mcp.tool()
async def PostgreSQL_list_databases() -> List[Dict[str, Any]]:
    """List all databases in the PostgreSQL cluster."""
    query = """
        SELECT 
            datname as database_name,
            pg_catalog.pg_get_userbyid(datdba) as owner,
            pg_encoding_to_char(encoding) as encoding,
            datcollate as collation,
            datctype as character_type,
            datacl as access_privileges,
            datistemplate as is_template,
            datallowconn as allow_connections
        FROM pg_database
        ORDER BY datname
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_server_version() -> Dict[str, Any]:
    """Get PostgreSQL server version and configuration information."""
    version_query = "SELECT version() as full_version"
    settings_query = """
        SELECT 
            name,
            setting,
            unit,
            context,
            short_desc
        FROM pg_settings 
        WHERE name IN ('server_version', 'server_version_num', 'data_directory', 'config_file', 'max_connections')
        ORDER BY name
    """
    
    version_info = await execute_query(version_query)
    settings_info = await execute_query(settings_query)
    
    return {
        "version_string": version_info[0]["full_version"] if version_info else "Unknown",
        "server_settings": settings_info
    }

@mcp.tool()
async def PostgreSQL_check_table_bloat(table_name: str, schema_name: str = "public") -> Dict[str, Any]:
    """Check for table bloat statistics to identify tables that may need maintenance.
    
    Args:
        table_name: Name of the table
        schema_name: Database schema name (default: public)
    """
    query = """
        SELECT 
            schemaname,
            tablename,
            n_tup_ins as inserts,
            n_tup_upd as updates,
            n_tup_del as deletes,
            n_live_tup as live_tuples,
            n_dead_tup as dead_tuples,
            ROUND((n_dead_tup::float / GREATEST(n_live_tup + n_dead_tup, 1)) * 100, 2) as dead_tuple_percent,
            last_vacuum,
            last_autovacuum,
            last_analyze,
            last_autoanalyze
        FROM pg_stat_user_tables
        WHERE schemaname = $1 AND tablename = $2
    """
    
    rows = await execute_query(query, schema_name, table_name)
    if not rows:
        return {"error": f"Table '{schema_name}.{table_name}' not found or has no statistics"}
    
    return rows[0]

@mcp.tool()
async def PostgreSQL_list_triggers(table_name: str, schema_name: str = "public") -> List[Dict[str, Any]]:
    """List all triggers on a specific table.
    
    Args:
        table_name: Name of the table
        schema_name: Database schema name (default: public)
    """
    query = """
        SELECT 
            trigger_name,
            event_manipulation as trigger_event,
            action_timing,
            action_statement,
            action_orientation
        FROM information_schema.triggers
        WHERE event_object_schema = $1 AND event_object_table = $2
        ORDER BY trigger_name
    """
    
    rows = await execute_query(query, schema_name, table_name)
    return rows

@mcp.tool()
async def PostgreSQL_get_table_permissions(table_name: str, schema_name: str = "public") -> List[Dict[str, Any]]:
    """Get permissions/grants on a specific table.
    
    Args:
        table_name: Name of the table
        schema_name: Database schema name (default: public)
    """
    query = """
        SELECT 
            grantee,
            privilege_type,
            is_grantable,
            grantor
        FROM information_schema.role_table_grants
        WHERE table_schema = $1 AND table_name = $2
        ORDER BY grantee, privilege_type
    """
    
    rows = await execute_query(query, schema_name, table_name)
    return rows

@mcp.tool()
async def PostgreSQL_vacuum_analyze_table(table_name: str, ctx: Context, schema_name: str = "public") -> str:
    """Run VACUUM ANALYZE on a specific table to reclaim space and update statistics.
    
    Args:
        table_name: Name of the table
        schema_name: Database schema name (default: public)
    """
    full_table_name = f"{schema_name}.{table_name}"
    
    await ctx.info(f"Running VACUUM ANALYZE on {full_table_name}...")
    
    # First, get table stats before vacuum
    stats_query = """
        SELECT 
            n_live_tup as live_tuples,
            n_dead_tup as dead_tuples,
            last_vacuum,
            last_analyze
        FROM pg_stat_user_tables
        WHERE schemaname = $1 AND tablename = $2
    """
    
    before_stats = await execute_query(stats_query, schema_name, table_name)
    
    # Run VACUUM ANALYZE
    vacuum_query = f"VACUUM ANALYZE {schema_name}.{table_name}"
    await execute_non_query(vacuum_query)
    
    # Get stats after vacuum
    after_stats = await execute_query(stats_query, schema_name, table_name)
    
    await ctx.info(f"VACUUM ANALYZE completed for {full_table_name}")
    
    return {
        "table": full_table_name,
        "operation": "VACUUM ANALYZE",
        "status": "completed",
        "before_stats": before_stats[0] if before_stats else None,
        "after_stats": after_stats[0] if after_stats else None
    }

@mcp.tool()
async def PostgreSQL_check_long_running_queries(min_duration_seconds: int = 60) -> List[Dict[str, Any]]:
    """Check for queries that have been running longer than the specified duration.
    
    Args:
        min_duration_seconds: Minimum duration in seconds to consider a query long-running
    """
    query = """
        SELECT 
            pid,
            usename as username,
            datname as database,
            client_addr as client_address,
            query_start,
            state,
            EXTRACT(EPOCH FROM (now() - query_start)) as duration_seconds,
            query
        FROM pg_stat_activity
        WHERE state != 'idle'
          AND query_start IS NOT NULL
          AND EXTRACT(EPOCH FROM (now() - query_start)) > $1
        ORDER BY duration_seconds DESC
    """
    
    rows = await execute_query(query, min_duration_seconds)
    return rows

@mcp.tool()
async def PostgreSQL_grant_privileges(grantee: str, privilege: str, table_name: str, ctx: Context, schema_name: str = "public") -> str:
    """Grant specific privileges on a table to a user or role.
    
    Args:
        grantee: User or role to grant privileges to
        privilege: Type of privilege (SELECT, INSERT, UPDATE, DELETE, ALL)
        table_name: Name of the table
        schema_name: Database schema name (default: public)
    """
    valid_privileges = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'ALL', 'REFERENCES', 'TRIGGER']
    if privilege.upper() not in valid_privileges:
        raise ValueError(f"Invalid privilege '{privilege}'. Must be one of: {', '.join(valid_privileges)}")
    
    full_table_name = f"{schema_name}.{table_name}"
    grant_query = f"GRANT {privilege.upper()} ON {full_table_name} TO {grantee}"
    
    await ctx.warning(f"Granting {privilege.upper()} privilege on {full_table_name} to {grantee}")
    
    result = await execute_non_query(grant_query)
    
    await ctx.info(f"Successfully granted {privilege.upper()} on {full_table_name} to {grantee}")
    return f"Successfully granted {privilege.upper()} on {full_table_name} to {grantee}: {result}"

@mcp.tool()
async def PostgreSQL_revoke_privileges(grantee: str, privilege: str, table_name: str, ctx: Context, schema_name: str = "public") -> str:
    """Revoke specific privileges on a table from a user or role.
    
    Args:
        grantee: User or role to revoke privileges from
        privilege: Type of privilege (SELECT, INSERT, UPDATE, DELETE, ALL)
        table_name: Name of the table
        schema_name: Database schema name (default: public)
    """
    valid_privileges = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'ALL', 'REFERENCES', 'TRIGGER']
    if privilege.upper() not in valid_privileges:
        raise ValueError(f"Invalid privilege '{privilege}'. Must be one of: {', '.join(valid_privileges)}")
    
    full_table_name = f"{schema_name}.{table_name}"
    revoke_query = f"REVOKE {privilege.upper()} ON {full_table_name} FROM {grantee}"
    
    await ctx.warning(f"Revoking {privilege.upper()} privilege on {full_table_name} from {grantee}")
    
    result = await execute_non_query(revoke_query)
    
    await ctx.info(f"Successfully revoked {privilege.upper()} on {full_table_name} from {grantee}")
    return f"Successfully revoked {privilege.upper()} on {full_table_name} from {grantee}: {result}"

@mcp.tool()
async def PostgreSQL_create_index(index_name: str, table_name: str, columns: str, ctx: Context, schema_name: str = "public", unique: bool = False, method: str = "btree") -> str:
    """Create an index on a table.
    
    Args:
        index_name: Name for the new index
        table_name: Name of the table
        columns: Comma-separated list of column names
        schema_name: Database schema name (default: public)
        unique: Whether to create a unique index
        method: Index method (btree, hash, gist, spgist, gin, brin)
    """
    valid_methods = ['btree', 'hash', 'gist', 'spgist', 'gin', 'brin']
    if method.lower() not in valid_methods:
        raise ValueError(f"Invalid index method '{method}'. Must be one of: {', '.join(valid_methods)}")
    
    full_table_name = f"{schema_name}.{table_name}"
    unique_clause = "UNIQUE " if unique else ""
    
    create_index_query = f"""
        CREATE {unique_clause}INDEX {index_name}
        ON {full_table_name}
        USING {method.lower()} ({columns})
    """
    
    await ctx.info(f"Creating {'unique ' if unique else ''}index {index_name} on {full_table_name}({columns}) using {method}")
    
    result = await execute_non_query(create_index_query)
    
    await ctx.info(f"Successfully created index {index_name}")
    return f"Successfully created {'unique ' if unique else ''}index {index_name} on {full_table_name}: {result}"

@mcp.tool()
async def PostgreSQL_drop_index(index_name: str, ctx: Context, schema_name: str = "public", cascade: bool = False) -> str:
    """Drop an index.
    
    Args:
        index_name: Name of the index to drop
        schema_name: Database schema name (default: public)
        cascade: Whether to cascade the drop operation
    """
    full_index_name = f"{schema_name}.{index_name}"
    cascade_clause = " CASCADE" if cascade else ""
    
    drop_index_query = f"DROP INDEX {full_index_name}{cascade_clause}"
    
    await ctx.warning(f"Dropping index {full_index_name}{'with CASCADE' if cascade else ''}")
    
    result = await execute_non_query(drop_index_query)
    
    await ctx.info(f"Successfully dropped index {full_index_name}")
    return f"Successfully dropped index {full_index_name}: {result}"

@mcp.tool()
async def PostgreSQL_explain_query(query: str, analyze: bool = False) -> List[Dict[str, Any]]:
    """Run EXPLAIN on a query to show the execution plan.
    
    Args:
        query: SQL query to explain
        analyze: Whether to run EXPLAIN ANALYZE (executes the query)
    """
    # Basic validation to ensure it's a SELECT query for EXPLAIN ANALYZE
    if analyze and not query.strip().upper().startswith("SELECT"):
        raise ValueError("EXPLAIN ANALYZE can only be used with SELECT queries for safety")
    
    explain_type = "EXPLAIN ANALYZE" if analyze else "EXPLAIN"
    explain_query = f"{explain_type} (FORMAT JSON) {query}"
    
    rows = await execute_query(explain_query)
    return rows

@mcp.tool()
async def PostgreSQL_reset_sequence(sequence_name: str, ctx: Context, schema_name: str = "public", restart_value: Optional[int] = None) -> str:
    """Reset a sequence to a specific value or restart from 1.
    
    Args:
        sequence_name: Name of the sequence
        schema_name: Database schema name (default: public)
        restart_value: Value to restart the sequence from (default: 1)
    """
    full_sequence_name = f"{schema_name}.{sequence_name}"
    restart_val = restart_value if restart_value is not None else 1
    
    reset_query = f"ALTER SEQUENCE {full_sequence_name} RESTART WITH {restart_val}"
    
    await ctx.warning(f"Resetting sequence {full_sequence_name} to {restart_val}")
    
    result = await execute_non_query(reset_query)
    
    await ctx.info(f"Successfully reset sequence {full_sequence_name} to {restart_val}")
    return f"Successfully reset sequence {full_sequence_name} to {restart_val}: {result}"

@mcp.tool()
async def PostgreSQL_get_replication_status() -> List[Dict[str, Any]]:
    """Get information about PostgreSQL replication status."""
    query = """
        SELECT 
            pid,
            usesysid,
            usename as username,
            application_name,
            client_addr as client_address,
            client_hostname,
            client_port,
            backend_start,
            backend_xmin,
            state,
            sent_lsn,
            write_lsn,
            flush_lsn,
            replay_lsn,
            write_lag,
            flush_lag,
            replay_lag,
            sync_priority,
            sync_state
        FROM pg_stat_replication
        ORDER BY application_name
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_locks_info() -> List[Dict[str, Any]]:
    """Get information about current database locks."""
    query = """
        SELECT 
            pl.pid,
            pa.usename as username,
            pa.datname as database,
            pl.locktype,
            pl.relation::regclass as relation,
            pl.mode,
            pl.granted,
            pa.query_start,
            pa.query
        FROM pg_locks pl
        LEFT JOIN pg_stat_activity pa ON pl.pid = pa.pid
        WHERE pl.relation IS NOT NULL
        ORDER BY pl.granted, pa.query_start
    """
    
    rows = await execute_query(query)
    return rows

# Additional PostgreSQL Administrative Tools

@mcp.tool()
async def PostgreSQL_get_table_statistics() -> List[Dict[str, Any]]:
    """Get comprehensive statistics for all user tables."""
    query = """
        SELECT 
            schemaname,
            tablename,
            n_live_tup as live_tuples,
            n_dead_tup as dead_tuples,
            n_tup_ins as total_inserts,
            n_tup_upd as total_updates,
            n_tup_del as total_deletes,
            n_tup_hot_upd as hot_updates,
            seq_scan as sequential_scans,
            seq_tup_read as seq_tuples_read,
            idx_scan as index_scans,
            idx_tup_fetch as idx_tuples_fetched,
            last_vacuum,
            last_autovacuum,
            last_analyze,
            last_autoanalyze
        FROM pg_stat_user_tables
        ORDER BY schemaname, tablename
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_index_usage() -> List[Dict[str, Any]]:
    """Get index usage statistics to identify unused indexes."""
    query = """
        SELECT 
            schemaname,
            tablename,
            indexrelname as index_name,
            idx_scan as index_scans,
            idx_tup_read as tuples_read,
            idx_tup_fetch as tuples_fetched,
            pg_size_pretty(pg_relation_size(indexrelid)) as index_size
        FROM pg_stat_user_indexes
        ORDER BY idx_scan ASC, schemaname, tablename
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_kill_connection(pid: int, ctx: Context) -> str:
    """Terminate a specific database connection by PID.
    
    Args:
        pid: Process ID of the connection to terminate
    """
    # First check if the connection exists
    check_query = "SELECT pid, usename, datname, query FROM pg_stat_activity WHERE pid = $1"
    existing = await execute_query(check_query, pid)
    
    if not existing:
        return f"Connection with PID {pid} not found"
    
    connection_info = existing[0]
    await ctx.warning(f"Terminating connection PID {pid} (user: {connection_info['usename']}, database: {connection_info['datname']})")
    
    kill_query = "SELECT pg_terminate_backend($1) as terminated"
    result = await execute_query(kill_query, pid)
    
    if result[0]['terminated']:
        await ctx.info(f"Successfully terminated connection PID {pid}")
        return f"Successfully terminated connection PID {pid}"
    else:
        return f"Failed to terminate connection PID {pid}"

@mcp.tool()
async def PostgreSQL_get_slow_queries(min_duration_ms: int = 1000) -> List[Dict[str, Any]]:
    """Get slow queries from pg_stat_statements (if available).
    
    Args:
        min_duration_ms: Minimum duration in milliseconds to consider slow
    """
    # Check if pg_stat_statements extension exists
    check_ext_query = "SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'"
    
    try:
        ext_check = await execute_query(check_ext_query)
        if not ext_check:
            return [{"error": "pg_stat_statements extension is not installed"}]
        
        query = """
            SELECT 
                query,
                calls,
                total_exec_time,
                mean_exec_time,
                max_exec_time,
                min_exec_time,
                rows as total_rows,
                shared_blks_hit,
                shared_blks_read,
                shared_blks_written
            FROM pg_stat_statements
            WHERE mean_exec_time > $1
            ORDER BY mean_exec_time DESC
            LIMIT 50
        """
        
        rows = await execute_query(query, min_duration_ms)
        return rows
    except Exception as e:
        return [{"error": f"Error retrieving slow queries: {str(e)}"}]

@mcp.tool()
async def PostgreSQL_get_cache_hit_ratio() -> List[Dict[str, Any]]:
    """Get cache hit ratios for tables and indexes."""
    query = """
        SELECT 
            'Tables' as type,
            ROUND(
                sum(heap_blks_hit) * 100.0 / GREATEST(sum(heap_blks_hit + heap_blks_read), 1), 2
            ) as cache_hit_ratio
        FROM pg_statio_user_tables
        UNION ALL
        SELECT 
            'Indexes' as type,
            ROUND(
                sum(idx_blks_hit) * 100.0 / GREATEST(sum(idx_blks_hit + idx_blks_read), 1), 2
            ) as cache_hit_ratio
        FROM pg_statio_user_indexes
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_checkpoint_stats() -> Dict[str, Any]:
    """Get checkpoint and background writer statistics."""
    query = """
        SELECT 
            checkpoints_timed,
            checkpoints_req,
            checkpoint_write_time,
            checkpoint_sync_time,
            buffers_checkpoint,
            buffers_clean,
            maxwritten_clean,
            buffers_backend,
            buffers_backend_fsync,
            buffers_alloc,
            stats_reset
        FROM pg_stat_bgwriter
    """
    
    rows = await execute_query(query)
    return rows[0] if rows else {}

@mcp.tool()
async def PostgreSQL_get_wal_stats() -> Dict[str, Any]:
    """Get Write-Ahead Log (WAL) statistics and status."""
    query = """
        SELECT 
            pg_current_wal_lsn() as current_wal_lsn,
            pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0') as total_wal_bytes,
            pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')) as total_wal_size,
            (SELECT setting FROM pg_settings WHERE name = 'wal_level') as wal_level,
            (SELECT setting FROM pg_settings WHERE name = 'archive_mode') as archive_mode,
            (SELECT setting FROM pg_settings WHERE name = 'archive_command') as archive_command
    """
    
    rows = await execute_query(query)
    return rows[0] if rows else {}

@mcp.tool()
async def PostgreSQL_create_user(username: str, password: str, ctx: Context, can_login: bool = True, is_superuser: bool = False, can_create_db: bool = False) -> str:
    """Create a new database user/role.
    
    Args:
        username: Username for the new user
        password: Password for the new user
        can_login: Whether the user can log in
        is_superuser: Whether the user is a superuser
        can_create_db: Whether the user can create databases
    """
    options = []
    if can_login:
        options.append("LOGIN")
    else:
        options.append("NOLOGIN")
    
    if is_superuser:
        options.append("SUPERUSER")
    else:
        options.append("NOSUPERUSER")
    
    if can_create_db:
        options.append("CREATEDB")
    else:
        options.append("NOCREATEDB")
    
    options_str = " ".join(options)
    create_user_query = f"CREATE USER {username} WITH {options_str} PASSWORD %s"
    
    await ctx.warning(f"Creating user '{username}' with options: {options_str}")
    
    result = await execute_non_query(create_user_query, password)
    
    await ctx.info(f"Successfully created user '{username}'")
    return f"Successfully created user '{username}': {result}"

@mcp.tool()
async def PostgreSQL_drop_user(username: str, ctx: Context) -> str:
    """Drop a database user/role.
    
    Args:
        username: Username to drop
    """
    drop_user_query = f"DROP USER {username}"
    
    await ctx.warning(f"Dropping user '{username}'")
    
    result = await execute_non_query(drop_user_query)
    
    await ctx.info(f"Successfully dropped user '{username}'")
    return f"Successfully dropped user '{username}': {result}"

@mcp.tool()
async def PostgreSQL_get_bloated_tables(bloat_threshold: float = 20.0) -> List[Dict[str, Any]]:
    """Find tables with significant bloat that may need maintenance.
    
    Args:
        bloat_threshold: Minimum bloat percentage to consider significant
    """
    query = """
        SELECT 
            schemaname,
            tablename,
            n_live_tup as live_tuples,
            n_dead_tup as dead_tuples,
            ROUND((n_dead_tup::float / GREATEST(n_live_tup + n_dead_tup, 1)) * 100, 2) as bloat_percentage,
            pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size,
            last_vacuum,
            last_autovacuum
        FROM pg_stat_user_tables
        WHERE (n_dead_tup::float / GREATEST(n_live_tup + n_dead_tup, 1)) * 100 > $1
        ORDER BY bloat_percentage DESC
    """
    
    rows = await execute_query(query, bloat_threshold)
    return rows

@mcp.tool()
async def PostgreSQL_get_unused_indexes(scan_threshold: int = 0) -> List[Dict[str, Any]]:
    """Find indexes that are rarely or never used.
    
    Args:
        scan_threshold: Maximum number of scans to consider unused
    """
    query = """
        SELECT 
            schemaname,
            tablename,
            indexrelname as index_name,
            idx_scan as scan_count,
            pg_size_pretty(pg_relation_size(indexrelid)) as index_size,
            pg_relation_size(indexrelid) as size_bytes
        FROM pg_stat_user_indexes
        WHERE idx_scan <= $1
        ORDER BY pg_relation_size(indexrelid) DESC
    """
    
    rows = await execute_query(query, scan_threshold)
    return rows

@mcp.tool()
async def PostgreSQL_analyze_database(ctx: Context) -> str:
    """Run ANALYZE on the entire database to update statistics."""
    await ctx.info("Running ANALYZE on entire database...")
    
    analyze_query = "ANALYZE"
    result = await execute_non_query(analyze_query)
    
    await ctx.info("Database analysis completed")
    return f"Successfully analyzed entire database: {result}"

@mcp.tool()
async def PostgreSQL_get_extension_list() -> List[Dict[str, Any]]:
    """List all installed PostgreSQL extensions."""
    query = """
        SELECT 
            extname as extension_name,
            extversion as version,
            n.nspname as schema,
            extrelocatable as is_relocatable,
            extconfig as configuration_tables
        FROM pg_extension e
        LEFT JOIN pg_namespace n ON n.oid = e.extnamespace
        ORDER BY extname
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_create_schema(schema_name: str, ctx: Context, owner: Optional[str] = None) -> str:
    """Create a new database schema.
    
    Args:
        schema_name: Name of the new schema
        owner: Owner of the schema (optional)
    """
    if owner:
        create_schema_query = f"CREATE SCHEMA {schema_name} AUTHORIZATION {owner}"
    else:
        create_schema_query = f"CREATE SCHEMA {schema_name}"
    
    await ctx.info(f"Creating schema '{schema_name}'" + (f" with owner '{owner}'" if owner else ""))
    
    result = await execute_non_query(create_schema_query)
    
    await ctx.info(f"Successfully created schema '{schema_name}'")
    return f"Successfully created schema '{schema_name}': {result}"

@mcp.tool()
async def PostgreSQL_drop_schema(schema_name: str, ctx: Context, cascade: bool = False) -> str:
    """Drop a database schema.
    
    Args:
        schema_name: Name of the schema to drop
        cascade: Whether to cascade the drop operation
    """
    cascade_clause = " CASCADE" if cascade else " RESTRICT"
    drop_schema_query = f"DROP SCHEMA {schema_name}{cascade_clause}"
    
    await ctx.warning(f"Dropping schema '{schema_name}'" + (" with CASCADE" if cascade else ""))
    
    result = await execute_non_query(drop_schema_query)
    
    await ctx.info(f"Successfully dropped schema '{schema_name}'")
    return f"Successfully dropped schema '{schema_name}': {result}"

@mcp.tool()
async def PostgreSQL_get_foreign_keys(table_name: str, schema_name: str = "public") -> List[Dict[str, Any]]:
    """Get all foreign key relationships for a table (both referencing and referenced).
    
    Args:
        table_name: Name of the table
        schema_name: Database schema name (default: public)
    """
    query = """
        SELECT 
            tc.constraint_name,
            tc.table_schema as source_schema,
            tc.table_name as source_table,
            kcu.column_name as source_column,
            ccu.table_schema as target_schema,
            ccu.table_name as target_table,
            ccu.column_name as target_column,
            rc.update_rule,
            rc.delete_rule,
            'OUTGOING' as relationship_type
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
        JOIN information_schema.referential_constraints AS rc
            ON tc.constraint_name = rc.constraint_name
            AND tc.table_schema = rc.constraint_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_schema = $1 
            AND tc.table_name = $2
        
        UNION ALL
        
        SELECT 
            tc.constraint_name,
            ccu.table_schema as source_schema,
            ccu.table_name as source_table,
            ccu.column_name as source_column,
            tc.table_schema as target_schema,
            tc.table_name as target_table,
            kcu.column_name as target_column,
            rc.update_rule,
            rc.delete_rule,
            'INCOMING' as relationship_type
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
        JOIN information_schema.referential_constraints AS rc
            ON tc.constraint_name = rc.constraint_name
            AND tc.table_schema = rc.constraint_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
            AND ccu.table_schema = $1 
            AND ccu.table_name = $2
        
        ORDER BY relationship_type, constraint_name
    """
    
    rows = await execute_query(query, schema_name, table_name)
    return rows

@mcp.tool()
async def PostgreSQL_get_materialized_views(schema_name: str = "public") -> List[Dict[str, Any]]:
    """List all materialized views in a schema.
    
    Args:
        schema_name: Database schema name (default: public)
    """
    query = """
        SELECT 
            schemaname,
            matviewname as view_name,
            matviewowner as owner,
            tablespace,
            hasindexes as has_indexes,
            ispopulated as is_populated,
            definition
        FROM pg_matviews
        WHERE schemaname = $1
        ORDER BY matviewname
    """
    
    rows = await execute_query(query, schema_name)
    return rows

@mcp.tool()
async def PostgreSQL_refresh_materialized_view(view_name: str, ctx: Context, schema_name: str = "public", concurrently: bool = False) -> str:
    """Refresh a materialized view.
    
    Args:
        view_name: Name of the materialized view
        schema_name: Database schema name (default: public)
        concurrently: Whether to refresh concurrently (requires unique index)
    """
    full_view_name = f"{schema_name}.{view_name}"
    concurrent_clause = " CONCURRENTLY" if concurrently else ""
    
    refresh_query = f"REFRESH MATERIALIZED VIEW{concurrent_clause} {full_view_name}"
    
    await ctx.info(f"Refreshing materialized view {full_view_name}" + (" concurrently" if concurrently else ""))
    
    result = await execute_non_query(refresh_query)
    
    await ctx.info(f"Successfully refreshed materialized view {full_view_name}")
    return f"Successfully refreshed materialized view {full_view_name}: {result}"

@mcp.tool()
async def PostgreSQL_get_connection_limits() -> Dict[str, Any]:
    """Get information about connection limits and current usage."""
    query = """
        SELECT 
            (SELECT setting::int FROM pg_settings WHERE name = 'max_connections') as max_connections,
            (SELECT count(*) FROM pg_stat_activity) as current_connections,
            (SELECT count(*) FROM pg_stat_activity WHERE state = 'active') as active_connections,
            (SELECT count(*) FROM pg_stat_activity WHERE state = 'idle') as idle_connections,
            (SELECT count(*) FROM pg_stat_activity WHERE state = 'idle in transaction') as idle_in_transaction
    """
    
    rows = await execute_query(query)
    result = rows[0] if rows else {}
    
    if result:
        result['connection_usage_percent'] = round(
            (result['current_connections'] / result['max_connections']) * 100, 2
        )
    
    return result

# Additional 20 Advanced PostgreSQL Tools

@mcp.tool()
async def PostgreSQL_analyze_index_bloat(threshold: float = 30.0) -> List[Dict[str, Any]]:
    """
    Identify indexes with significant bloat above a threshold percentage.
    
    Args:
      threshold: Minimum bloat percentage to report.
    """
    query = """
    WITH index_bloat AS (
      SELECT
        schemaname,
        tablename,
        indexrelname AS index_name,
        pg_relation_size(indexrelid) AS real_size,
        pg_relation_size(indrelid) AS table_size,
        (pg_relation_size(indexrelid) - pg_relation_size(indrelid)) * 100.0 / pg_relation_size(indexrelid) AS bloat_percent
      FROM pg_stat_user_indexes
      JOIN pg_index ON indexrelid = pg_stat_user_indexes.indexrelid
    )
    SELECT
      schemaname,
      tablename,
      index_name,
      pg_size_pretty(real_size) AS index_size,
      ROUND(bloat_percent, 2) AS bloat_percentage
    FROM index_bloat
    WHERE bloat_percent > $1
    ORDER BY bloat_percentage DESC
    """
    rows = await execute_query(query, threshold)
    return rows


@mcp.tool()
async def PostgreSQL_detect_conflicting_queries(min_locks: int = 2) -> List[Dict[str, Any]]:
    """
    Detect queries that hold locks conflicting with multiple other queries.
    
    Args:
      min_locks: Minimum number of conflicting locks to consider.
    """
    query = """
    SELECT
      blocking.pid AS blocking_pid,
      blocking.query AS blocking_query,
      COUNT(DISTINCT blocked.pid) AS num_conflicts
    FROM pg_locks blocking
    JOIN pg_stat_activity blocking_activity ON blocking.pid = blocking_activity.pid
    JOIN pg_locks blocked ON blocking.locktype = blocked.locktype
      AND blocking.database IS NOT DISTINCT FROM blocked.database
      AND blocking.relation IS NOT DISTINCT FROM blocked.relation
      AND blocking.page IS NOT DISTINCT FROM blocked.page
      AND blocking.tuple IS NOT DISTINCT FROM blocked.tuple
      AND blocking.virtualxid IS NOT DISTINCT FROM blocked.virtualxid
      AND blocking.transactionid IS NOT DISTINCT FROM blocked.transactionid
      AND blocking.classid IS NOT DISTINCT FROM blocked.classid
      AND blocking.objid IS NOT DISTINCT FROM blocked.objid
      AND blocking.objsubid IS NOT DISTINCT FROM blocked.objsubid
      AND blocking.pid != blocked.pid
    JOIN pg_stat_activity blocked_activity ON blocked.pid = blocked_activity.pid
    WHERE NOT blocking.granted AND blocking.granted
    GROUP BY blocking.pid, blocking.query
    HAVING COUNT(DISTINCT blocked.pid) >= $1
    ORDER BY num_conflicts DESC
    """
    rows = await execute_query(query, min_locks)
    return rows


@mcp.tool()
async def PostgreSQL_table_io_patterns(limit: int = 50) -> List[Dict[str, Any]]:
    """
    Analyze table I/O patterns for sequential vs index scans.
    
    Args:
      limit: Number of top tables to return.
    """
    query = """
    SELECT
      schemaname,
      relname AS table_name,
      seq_scan,
      idx_scan,
      seq_tup_read,
      idx_tup_fetch,
      ROUND(COALESCE(NULLIF(seq_scan + idx_scan, 0),1)::numeric*100.0,2) AS total_scans,
      ROUND(CASE WHEN seq_scan + idx_scan > 0 THEN idx_scan * 100.0 / (seq_scan + idx_scan) ELSE 0 END, 2) AS index_scan_ratio
    FROM pg_stat_user_tables
    ORDER BY total_scans DESC
    LIMIT $1
    """
    rows = await execute_query(query, limit)
    return rows


@mcp.tool()
async def PostgreSQL_longest_idle_transactions(min_idle_seconds: int = 300) -> List[Dict[str, Any]]:
    """
    Find transactions idle in transaction state longer than a threshold.
    
    Args:
      min_idle_seconds: Minimum idle time in seconds.
    """
    query = """
    SELECT pid, usename AS username, datname AS database, state, query_start,
           now() - query_start AS idle_duration, query
    FROM pg_stat_activity
    WHERE state = 'idle in transaction'
      AND EXTRACT(EPOCH FROM (now() - query_start)) > $1
    ORDER BY idle_duration DESC
    """
    rows = await execute_query(query, min_idle_seconds)
    return rows


@mcp.tool()
async def PostgreSQL_index_redundancy_detection() -> List[Dict[str, Any]]:
    """
    Identify potentially redundant or overlapping indexes for tables.
    """
    query = """
    WITH index_columns AS (
      SELECT
        c.oid AS index_oid,
        n.nspname AS schema_name,
        t.relname AS table_name,
        i.relname AS index_name,
        idx.indisunique AS is_unique,
        array_to_string(array_agg(a.attname ORDER BY k), ',') AS columns
      FROM pg_index idx
      JOIN pg_class i ON idx.indexrelid = i.oid
      JOIN pg_class t ON idx.indrelid = t.oid
      JOIN pg_namespace n ON t.relnamespace = n.oid
      JOIN unnest(idx.indkey) WITH ORDINALITY AS k(attnum, k) ON true
      JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
      WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
      GROUP BY c.oid, n.nspname, t.relname, i.relname, idx.indisunique
    ), redundancy AS (
      SELECT
        ic1.schema_name,
        ic1.table_name,
        ic1.index_name AS index1,
        ic1.columns AS index1_columns,
        ic2.index_name AS index2,
        ic2.columns AS index2_columns
      FROM index_columns ic1
      JOIN index_columns ic2 ON ic1.schema_name = ic2.schema_name
        AND ic1.table_name = ic2.table_name
        AND ic1.index_name <> ic2.index_name
        AND ic1.columns LIKE ic2.columns || '%'
    )
    SELECT * FROM redundancy
    ORDER BY schema_name, table_name;
    """
    rows = await execute_query(query)
    return rows


@mcp.tool()
async def PostgreSQL_high_io_tables(min_calls: int = 1000) -> List[Dict[str, Any]]:
    """
    Detect tables with high I/O operations based on statistics.
    
    Args:
      min_calls: Minimum calls to consider.
    """
    query = """
    SELECT
      schemaname,
      relname AS table_name,
      seq_scan,
      idx_scan,
      n_tup_ins,
      n_tup_upd,
      n_tup_del,
      pg_size_pretty(pg_total_relation_size(relid)) AS total_size
    FROM pg_stat_user_tables
    WHERE seq_scan + idx_scan > $1
    ORDER BY seq_scan + idx_scan DESC
    """
    rows = await execute_query(query, min_calls)
    return rows


@mcp.tool()
async def PostgreSQL_foreign_key_conflicts(limit: int = 50) -> List[Dict[str, Any]]:
    """
    Analyze foreign key constraints that could cause lock conflicts.
    
    Args:
      limit: Number of rows to return.
    """
    query = """
    SELECT
      conname AS constraint_name,
      conrelid::regclass AS table_name,
      confrelid::regclass AS referenced_table,
      confupdtype AS update_action,
      confdeltype AS delete_action,
      condeferrable AS is_deferrable
    FROM pg_constraint
    WHERE contype = 'f'
    ORDER BY conrelid
    LIMIT $1
    """
    rows = await execute_query(query, limit)
    return rows


@mcp.tool()
async def PostgreSQL_active_temp_file_users(min_temp_bytes: int = 100000000) -> List[Dict[str, Any]]:
    """
    List active databases with significant temporary file usage indicating memory pressure.
    
    Args:
      min_temp_bytes: Minimum temp file bytes to report.
    """
    query = """
    SELECT
      datname AS database,
      temp_files,
      pg_size_pretty(temp_bytes) AS temp_size,
      temp_bytes
    FROM pg_stat_database
    WHERE temp_bytes > $1
    ORDER BY temp_bytes DESC
    """
    rows = await execute_query(query, min_temp_bytes)
    return rows


@mcp.tool()
async def PostgreSQL_logical_replication_slot_lag() -> List[Dict[str, Any]]:
    """
    List logical replication slots with current lag in bytes and status.
    """
    query = """
    SELECT
      slot_name,
      database,
      active,
      confirmed_flush_lsn,
      pg_current_wal_lsn() AS current_wal_lsn,
      pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) AS lag_bytes,
      CASE
        WHEN active THEN 'ACTIVE'
        ELSE 'INACTIVE'
      END AS status
    FROM pg_replication_slots
    WHERE slot_type = 'logical'
    ORDER BY lag_bytes DESC
    """
    rows = await execute_query(query)
    return rows


@mcp.tool()
async def PostgreSQL_list_roles_with_superuser(rights_only: bool = False) -> List[Dict[str, Any]]:
    """
    List database roles, optionally showing only superusers.
    
    Args:
      rights_only: If true, only show superusers.
    """
    if rights_only:
        query = """
        SELECT rolname, rolcanlogin, rolsuper, rolcreaterole, rolcreatedb
        FROM pg_roles
        WHERE rolsuper
        ORDER BY rolname
        """
        rows = await execute_query(query)
    else:
        query = """
        SELECT rolname, rolcanlogin, rolsuper, rolcreaterole, rolcreatedb
        FROM pg_roles
        ORDER BY rolname
        """
        rows = await execute_query(query)
    return rows


@mcp.tool()
async def PostgreSQL_predicate_lock_analysis() -> List[Dict[str, Any]]:
    """
    Analyze predicate locks that may lead to serialization failures or contention.
    """
    query = """
    SELECT
      s.pid,
      s.usename,
      l.locktype,
      l.mode,
      l.granted,
      s.query,
      s.query_start
    FROM pg_locks l
    JOIN pg_stat_activity s ON l.pid = s.pid
    WHERE l.locktype = 'predicate'
    ORDER BY s.query_start DESC
    """
    rows = await execute_query(query)
    return rows


@mcp.tool()
async def PostgreSQL_get_high_wait_events() -> List[Dict[str, Any]]:
    """
    Get wait events with the highest average wait times.
    """
    query = """
    SELECT
       wait_event_type,
       wait_event,
       AVG(extract(epoch from now() - state_change)) AS avg_wait_sec,
       COUNT(*) AS session_count
    FROM pg_stat_activity
    WHERE wait_event IS NOT NULL
    GROUP BY wait_event_type, wait_event
    ORDER BY avg_wait_sec DESC
    LIMIT 20
    """
    rows = await execute_query(query)
    return rows


@mcp.tool()
async def PostgreSQL_get_vacuum_inefficiency_tables(min_dead_ratio: float = 10.0) -> List[Dict[str, Any]]:
    """
    Detect tables with inefficient vacuuming based on dead tuple ratio.
    
    Args:
      min_dead_ratio: Minimum dead tuple percentage to report.
    """
    query = """
    SELECT
      schemaname,
      tablename,
      n_dead_tup,
      n_live_tup,
      ROUND(100.0 * n_dead_tup / GREATEST(n_live_tup + n_dead_tup, 1), 2) AS dead_ratio,
      last_vacuum,
      last_autovacuum
    FROM pg_stat_user_tables
    WHERE n_dead_tup > 0 AND ROUND(100.0 * n_dead_tup / GREATEST(n_live_tup + n_dead_tup, 1), 2) > $1
    ORDER BY dead_ratio DESC
    """
    rows = await execute_query(query, min_dead_ratio)
    return rows


@mcp.tool()
async def PostgreSQL_verify_table_column_compression(table_name: str, schema_name: str = "public") -> List[Dict[str, Any]]:
    """
    Verify compression methods applied to table columns (if supported).
    
    Args:
      table_name: The table to check.
      schema_name: The schema name.
    """
    query = """
    SELECT
      attname AS column_name,
      attcompression AS compression_method
    FROM pg_attribute
    WHERE attrelid = $1::regclass
      AND attnum > 0
      AND NOT attisdropped
    """
    full_table_name = f"{schema_name}.{table_name}"
    rows = await execute_query(query, full_table_name)
    return rows


@mcp.tool()
async def PostgreSQL_detect_index_lock_waits() -> List[Dict[str, Any]]:
    """
    Detect locking waits specifically on indexes.
    """
    query = """
    SELECT
      blocked_locks.pid AS blocked_pid,
      blocked_activity.usename AS blocked_user,
      blocking_locks.pid AS blocking_pid,
      blocking_activity.usename AS blocking_user,
      blocked_locks.locktype,
      blocked_locks.mode AS blocked_mode,
      blocking_locks.mode AS blocking_mode,
      blocked_activity.query AS blocked_query,
      blocking_activity.query AS blocking_query,
      EXTRACT(epoch FROM (now() - blocked_activity.query_start)) AS wait_duration_seconds
    FROM pg_locks blocked_locks
    JOIN pg_stat_activity blocked_activity ON blocked_locks.pid = blocked_activity.pid
    JOIN pg_locks blocking_locks 
      ON blocking_locks.locktype = blocked_locks.locktype
      AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
      AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
      AND blocking_locks.pid != blocked_locks.pid
    JOIN pg_stat_activity blocking_activity ON blocking_locks.pid = blocking_activity.pid
    WHERE NOT blocked_locks.granted
      AND blocked_locks.locktype = 'index'
    ORDER BY wait_duration_seconds DESC
    """
    rows = await execute_query(query)
    return rows


@mcp.tool()
async def PostgreSQL_analyze_table_freeze_stats() -> List[Dict[str, Any]]:
    """
    Analyze XID freeze stats to detect tables nearing transaction wraparound risks.
    """
    query = """
    SELECT
      schemaname,
      tablename,
      age(relfrozenxid) AS xid_age,
      n_live_tup,
      n_dead_tup,
      pg_size_pretty(pg_relation_size((schemaname || '.' || tablename)::regclass)) AS table_size,
      CASE
        WHEN age(relfrozenxid) > 1500000000 THEN 'CRITICAL'
        WHEN age(relfrozenxid) > 1000000000 THEN 'HIGH'
        WHEN age(relfrozenxid) > 500000000 THEN 'MEDIUM'
        ELSE 'LOW'
      END AS risk_level
    FROM pg_stat_user_tables
    ORDER BY age(relfrozenxid) DESC
    LIMIT 30
    """
    rows = await execute_query(query)
    return rows


@mcp.tool()
async def PostgreSQL_check_blocking_queries() -> List[Dict[str, Any]]:
    """
    Get currently blocking and blocked queries with detailed info.
    """
    query = """
    SELECT
      blocked_locks.pid AS blocked_pid,
      blocked_activity.usename AS blocked_user,
      blocking_locks.pid AS blocking_pid,
      blocking_activity.usename AS blocking_user,
      blocked_activity.query AS blocked_query,
      blocking_activity.query AS blocking_query,
      blocked_locks.locktype,
      blocked_locks.mode AS blocked_mode,
      blocking_locks.mode AS blocking_mode,
      EXTRACT(epoch FROM (now() - blocked_activity.query_start)) AS blocked_duration_seconds
    FROM pg_catalog.pg_locks blocked_locks
    JOIN pg_catalog.pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
    JOIN pg_catalog.pg_locks blocking_locks
      ON blocking_locks.locktype = blocked_locks.locktype
      AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
      AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
      AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
      AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
      AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
      AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
      AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
      AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
      AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
      AND blocking_locks.pid != blocked_locks.pid
    JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
    WHERE NOT blocked_locks.granted
      AND blocking_locks.granted
    ORDER BY blocked_duration_seconds DESC
    """
    rows = await execute_query(query)
    return rows

# Additional 20 Basic PostgreSQL Tools

@mcp.tool()
async def PostgreSQL_get_database_config() -> List[Dict[str, Any]]:
    """Get important database configuration parameters."""
    query = """
        SELECT 
            name,
            setting,
            unit,
            category,
            short_desc as description,
            context,
            vartype as type,
            source,
            min_val,
            max_val,
            boot_val as boot_value
        FROM pg_settings
        WHERE name IN (
            'shared_buffers', 'work_mem', 'maintenance_work_mem', 
            'effective_cache_size', 'checkpoint_segments', 'checkpoint_completion_target',
            'wal_buffers', 'default_statistics_target', 'random_page_cost',
            'seq_page_cost', 'max_connections', 'listen_addresses',
            'port', 'log_destination', 'logging_collector', 'log_min_messages'
        )
        ORDER BY category, name
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_all_constraints() -> List[Dict[str, Any]]:
    """Get all table constraints (CHECK, UNIQUE, PRIMARY KEY, FOREIGN KEY)."""
    query = """
        SELECT 
            tc.table_schema,
            tc.table_name,
            tc.constraint_name,
            tc.constraint_type,
            CASE 
                WHEN tc.constraint_type = 'CHECK' THEN cc.check_clause
                WHEN tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE') THEN 
                    COALESCE(
                        (SELECT string_agg(kcu.column_name, ', ' ORDER BY kcu.ordinal_position)
                         FROM information_schema.key_column_usage kcu
                         WHERE kcu.constraint_name = tc.constraint_name
                         AND kcu.table_schema = tc.table_schema
                         AND kcu.table_name = tc.table_name),
                        'N/A'
                    )
                WHEN tc.constraint_type = 'FOREIGN KEY' THEN 
                    (SELECT kcu.column_name || ' -> ' || ccu.table_name || '(' || ccu.column_name || ')'
                     FROM information_schema.key_column_usage kcu
                     JOIN information_schema.constraint_column_usage ccu ON kcu.constraint_name = ccu.constraint_name
                     WHERE kcu.constraint_name = tc.constraint_name
                     AND kcu.table_schema = tc.table_schema
                     LIMIT 1)
                ELSE 'N/A'
            END AS constraint_definition
        FROM information_schema.table_constraints tc
        LEFT JOIN information_schema.check_constraints cc
            ON tc.constraint_name = cc.constraint_name
            AND tc.table_schema = cc.constraint_schema
        WHERE tc.table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY tc.table_schema, tc.table_name, tc.constraint_type, tc.constraint_name
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_sequences() -> List[Dict[str, Any]]:
    """Get all sequences in the database with their current values."""
    query = """
        SELECT 
            schemaname,
            sequencename,
            sequenceowner as owner,
            data_type,
            start_value,
            min_value,
            max_value,
            increment_by,
            cycle,
            cache_size,
            last_value
        FROM pg_sequences
        ORDER BY schemaname, sequencename
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_triggers() -> List[Dict[str, Any]]:
    """Get all triggers in the database."""
    query = """
        SELECT 
            n.nspname as schema_name,
            c.relname as table_name,
            t.tgname as trigger_name,
            pg_get_triggerdef(t.oid) as trigger_definition,
            CASE t.tgtype & 66
                WHEN 2 THEN 'BEFORE'
                WHEN 64 THEN 'INSTEAD OF'
                ELSE 'AFTER'
            END as timing,
            CASE t.tgtype & 28
                WHEN 4 THEN 'INSERT'
                WHEN 8 THEN 'DELETE'
                WHEN 16 THEN 'UPDATE'
                WHEN 12 THEN 'INSERT, DELETE'
                WHEN 20 THEN 'INSERT, UPDATE'
                WHEN 24 THEN 'DELETE, UPDATE'
                WHEN 28 THEN 'INSERT, DELETE, UPDATE'
                ELSE 'UNKNOWN'
            END as events,
            t.tgenabled as is_enabled
        FROM pg_trigger t
        JOIN pg_class c ON t.tgrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE NOT t.tgisinternal
        ORDER BY n.nspname, c.relname, t.tgname
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_functions() -> List[Dict[str, Any]]:
    """Get all user-defined functions in the database."""
    query = """
        SELECT 
            n.nspname as schema_name,
            p.proname as function_name,
            pg_get_function_identity_arguments(p.oid) as arguments,
            t.typname as return_type,
            l.lanname as language,
            CASE p.provolatile
                WHEN 'i' THEN 'IMMUTABLE'
                WHEN 's' THEN 'STABLE'
                WHEN 'v' THEN 'VOLATILE'
            END as volatility,
            p.prosecdef as security_definer,
            obj_description(p.oid, 'pg_proc') as description
        FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        JOIN pg_type t ON p.prorettype = t.oid
        JOIN pg_language l ON p.prolang = l.oid
        WHERE n.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        AND p.prokind = 'f'
        ORDER BY n.nspname, p.proname
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_table_inheritance() -> List[Dict[str, Any]]:
    """Get table inheritance relationships."""
    query = """
        SELECT 
            n1.nspname as parent_schema,
            c1.relname as parent_table,
            n2.nspname as child_schema,
            c2.relname as child_table,
            i.inhseqno as inheritance_sequence
        FROM pg_inherits i
        JOIN pg_class c1 ON i.inhparent = c1.oid
        JOIN pg_namespace n1 ON c1.relnamespace = n1.oid
        JOIN pg_class c2 ON i.inhrelid = c2.oid
        JOIN pg_namespace n2 ON c2.relnamespace = n2.oid
        ORDER BY n1.nspname, c1.relname, i.inhseqno
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_temp_files() -> List[Dict[str, Any]]:
    """Get information about temporary files usage by queries."""
    query = """
        SELECT 
            datname as database_name,
            temp_files,
            temp_bytes,
            pg_size_pretty(temp_bytes) as temp_size,
            stats_reset
        FROM pg_stat_database
        WHERE temp_files > 0
        ORDER BY temp_bytes DESC
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_autovacuum_settings() -> List[Dict[str, Any]]:
    """Get autovacuum settings for all tables."""
    query = """
        SELECT 
            schemaname,
            tablename,
            attname as setting_name,
            attvalue as setting_value
        FROM (
            SELECT 
                n.nspname as schemaname,
                c.relname as tablename,
                unnest(ARRAY[
                    'autovacuum_enabled',
                    'autovacuum_vacuum_threshold',
                    'autovacuum_analyze_threshold',
                    'autovacuum_vacuum_scale_factor',
                    'autovacuum_analyze_scale_factor',
                    'autovacuum_vacuum_cost_delay',
                    'autovacuum_vacuum_cost_limit'
                ]) as attname,
                unnest(ARRAY[
                    COALESCE(c.reloptions[array_position(c.reloptions, 'autovacuum_enabled=true')], 
                             c.reloptions[array_position(c.reloptions, 'autovacuum_enabled=false')], 'default'),
                    COALESCE(substring(c.reloptions[array_position(c.reloptions, 
                        (SELECT opt FROM unnest(c.reloptions) opt WHERE opt LIKE 'autovacuum_vacuum_threshold=%'))]
                        FROM 'autovacuum_vacuum_threshold=(.*)'), 'default'),
                    COALESCE(substring(c.reloptions[array_position(c.reloptions,
                        (SELECT opt FROM unnest(c.reloptions) opt WHERE opt LIKE 'autovacuum_analyze_threshold=%'))]
                        FROM 'autovacuum_analyze_threshold=(.*)'), 'default'),
                    COALESCE(substring(c.reloptions[array_position(c.reloptions,
                        (SELECT opt FROM unnest(c.reloptions) opt WHERE opt LIKE 'autovacuum_vacuum_scale_factor=%'))]
                        FROM 'autovacuum_vacuum_scale_factor=(.*)'), 'default'),
                    COALESCE(substring(c.reloptions[array_position(c.reloptions,
                        (SELECT opt FROM unnest(c.reloptions) opt WHERE opt LIKE 'autovacuum_analyze_scale_factor=%'))]
                        FROM 'autovacuum_analyze_scale_factor=(.*)'), 'default'),
                    COALESCE(substring(c.reloptions[array_position(c.reloptions,
                        (SELECT opt FROM unnest(c.reloptions) opt WHERE opt LIKE 'autovacuum_vacuum_cost_delay=%'))]
                        FROM 'autovacuum_vacuum_cost_delay=(.*)'), 'default'),
                    COALESCE(substring(c.reloptions[array_position(c.reloptions,
                        (SELECT opt FROM unnest(c.reloptions) opt WHERE opt LIKE 'autovacuum_vacuum_cost_limit=%'))]
                        FROM 'autovacuum_vacuum_cost_limit=(.*)'), 'default')
                ]) as attvalue
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE c.relkind IN ('r', 't')
            AND n.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        ) t
        WHERE attvalue != 'default'
        ORDER BY schemaname, tablename, setting_name
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_partitioned_tables() -> List[Dict[str, Any]]:
    """Get information about partitioned tables and their partitions."""
    query = """
        SELECT 
            n.nspname as schema_name,
            c.relname as table_name,
            CASE c.relkind
                WHEN 'p' THEN 'PARTITIONED TABLE'
                WHEN 'r' THEN 'PARTITION'
                ELSE 'OTHER'
            END as table_type,
            pg_get_partkeydef(c.oid) as partition_key,
            pg_get_expr(c.relpartbound, c.oid) as partition_bound,
            (SELECT count(*) FROM pg_inherits WHERE inhparent = c.oid) as num_partitions,
            pg_size_pretty(pg_total_relation_size(c.oid)) as total_size
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE c.relkind IN ('p', 'r')
        AND (c.relispartition OR EXISTS (SELECT 1 FROM pg_inherits WHERE inhparent = c.oid))
        AND n.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        ORDER BY n.nspname, c.relname
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_wait_events() -> List[Dict[str, Any]]:
    """Get current wait events from active sessions."""
    query = """
        SELECT 
            wait_event_type,
            wait_event,
            count(*) as session_count,
            array_agg(DISTINCT state) as states,
            round(avg(EXTRACT(epoch FROM (now() - state_change))), 2) as avg_wait_seconds
        FROM pg_stat_activity
        WHERE wait_event IS NOT NULL
        AND state != 'idle'
        GROUP BY wait_event_type, wait_event
        ORDER BY session_count DESC, wait_event_type, wait_event
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_replication_slots() -> List[Dict[str, Any]]:
    """Get information about replication slots."""
    query = """
        SELECT 
            slot_name,
            plugin,
            slot_type,
            datoid,
            database,
            temporary,
            active,
            active_pid,
            xmin,
            catalog_xmin,
            restart_lsn,
            confirmed_flush_lsn,
            wal_status,
            safe_wal_size
        FROM pg_replication_slots
        ORDER BY slot_name
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_event_triggers() -> List[Dict[str, Any]]:
    """Get all event triggers in the database."""
    query = """
        SELECT 
            evtname as trigger_name,
            evtevent as event,
            evtowner::regrole as owner,
            evtfoid::regproc as function_name,
            evtenabled as enabled,
            evttags as tags
        FROM pg_event_trigger
        ORDER BY evtname
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_publication_tables() -> List[Dict[str, Any]]:
    """Get tables included in logical replication publications."""
    query = """
        SELECT 
            p.pubname as publication_name,
            n.nspname as schema_name,
            c.relname as table_name,
            p.pubinsert as replicate_insert,
            p.pubupdate as replicate_update,
            p.pubdelete as replicate_delete,
            p.pubtruncate as replicate_truncate
        FROM pg_publication p
        LEFT JOIN pg_publication_rel pr ON pr.prpubid = p.oid
        LEFT JOIN pg_class c ON pr.prrelid = c.oid
        LEFT JOIN pg_namespace n ON c.relnamespace = n.oid
        ORDER BY p.pubname, n.nspname, c.relname
    """
    
    rows = await execute_query(query)
    return rows

# Advanced PostgreSQL Analysis Tools - 20 New Tools

@mcp.tool()
async def PostgreSQL_analyze_query_complexity(min_cost: float = 1000.0) -> List[Dict[str, Any]]:
    """Analyze query complexity by examining execution plans and costs.
    
    Args:
        min_cost: Minimum estimated cost to consider a query complex
    """
    query = """
        WITH query_stats AS (
            SELECT 
                LEFT(query, 100) as query_pattern,
                calls,
                total_exec_time,
                mean_exec_time,
                stddev_exec_time,
                rows,
                shared_blks_hit + shared_blks_read as total_blks_accessed,
                shared_blks_read,
                shared_blks_written,
                temp_blks_read + temp_blks_written as temp_usage,
                CASE 
                    WHEN mean_exec_time > 5000 THEN 'VERY_COMPLEX'
                    WHEN mean_exec_time > 1000 THEN 'COMPLEX' 
                    WHEN mean_exec_time > 500 THEN 'MODERATE'
                    ELSE 'SIMPLE'
                END as complexity_level,
                ROUND((shared_blks_read::float / NULLIF(shared_blks_hit + shared_blks_read, 0)) * 100, 2) as cache_miss_ratio
            FROM pg_stat_statements
            WHERE calls > 5
        )
        SELECT *,
            ROUND(total_exec_time / calls, 2) as avg_exec_time_ms,
            ROUND(total_blks_accessed::float / calls, 2) as avg_blocks_per_call
        FROM query_stats
        WHERE mean_exec_time >= $1
        ORDER BY mean_exec_time DESC, calls DESC
        LIMIT 25
    """
    
    try:
        rows = await execute_query(query, min_cost)
        return rows
    except Exception as e:
        return [{"error": f"pg_stat_statements not available or error: {str(e)}"}]

@mcp.tool()
async def PostgreSQL_detect_foreign_key_lock_contention() -> List[Dict[str, Any]]:
    """Detect potential foreign key lock contention and blocking scenarios."""
    query = """
        WITH lock_conflicts AS (
            SELECT 
                blocking.pid as blocking_pid,
                blocking.usename as blocking_user,
                blocking.query as blocking_query,
                blocked.pid as blocked_pid,
                blocked.usename as blocked_user,
                blocked.query as blocked_query,
                blocked_locks.locktype,
                blocked_locks.mode as blocked_mode,
                blocking_locks.mode as blocking_mode,
                blocked_locks.relation::regclass as relation_name,
                EXTRACT(epoch FROM (now() - blocked.query_start)) as blocked_duration_seconds
            FROM pg_locks blocked_locks
            JOIN pg_stat_activity blocked ON blocked_locks.pid = blocked.pid
            JOIN pg_locks blocking_locks ON (
                blocked_locks.relation = blocking_locks.relation
                AND blocked_locks.pid != blocking_locks.pid
            )
            JOIN pg_stat_activity blocking ON blocking_locks.pid = blocking.pid
            WHERE NOT blocked_locks.granted
            AND blocking_locks.granted
            AND blocked_locks.locktype IN ('relation', 'tuple')
        ),
        fk_relations AS (
            SELECT DISTINCT
                conrelid::regclass as table_name,
                confrelid::regclass as referenced_table
            FROM pg_constraint
            WHERE contype = 'f'
        )
        SELECT 
            lc.*,
            CASE WHEN fk.table_name IS NOT NULL THEN 'FOREIGN_KEY_RELATED' ELSE 'OTHER' END as contention_type,
            fk.referenced_table
        FROM lock_conflicts lc
        LEFT JOIN fk_relations fk ON lc.relation_name = fk.table_name OR lc.relation_name = fk.referenced_table
        ORDER BY blocked_duration_seconds DESC, contention_type
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_diagnose_logical_replication_lag() -> List[Dict[str, Any]]:
    """Diagnose logical replication lag and identify bottlenecks."""
    query = """
        WITH replication_metrics AS (
            SELECT 
                s.slot_name,
                s.database,
                s.active,
                s.active_pid,
                s.restart_lsn,
                s.confirmed_flush_lsn,
                s.wal_status,
                CASE WHEN s.active THEN 
                    pg_wal_lsn_diff(pg_current_wal_lsn(), s.confirmed_flush_lsn)
                ELSE NULL END as replication_lag_bytes,
                CASE WHEN s.active THEN 
                    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), s.confirmed_flush_lsn))
                ELSE 'INACTIVE' END as replication_lag_size,
                r.state as replication_state,
                r.sent_lsn,
                r.write_lsn,
                r.flush_lsn,
                r.replay_lsn,
                r.write_lag,
                r.flush_lag,
                r.replay_lag,
                r.sync_state
            FROM pg_replication_slots s
            LEFT JOIN pg_stat_replication r ON s.active_pid = r.pid
            WHERE s.slot_type = 'logical'
        )
        SELECT 
            *,
            CASE 
                WHEN replication_lag_bytes > 100*1024*1024 THEN 'CRITICAL_LAG'  -- >100MB
                WHEN replication_lag_bytes > 10*1024*1024 THEN 'HIGH_LAG'      -- >10MB 
                WHEN replication_lag_bytes > 1024*1024 THEN 'MODERATE_LAG'     -- >1MB
                WHEN replication_lag_bytes IS NULL THEN 'INACTIVE'
                ELSE 'LOW_LAG'
            END as lag_severity,
            CASE 
                WHEN NOT active THEN 'Replication slot inactive'
                WHEN wal_status = 'lost' THEN 'WAL files lost - replication may be broken'
                WHEN replication_lag_bytes > 100*1024*1024 THEN 'High lag detected - check network and subscriber performance'
                ELSE 'Normal operation'
            END as diagnostic_recommendation
        FROM replication_metrics
        ORDER BY replication_lag_bytes DESC NULLS LAST
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_analyze_autovacuum_efficiency() -> List[Dict[str, Any]]:
    """Analyze autovacuum efficiency and provide tuning recommendations."""
    query = """
        WITH table_stats AS (
            SELECT 
                schemaname,
                tablename,
                n_live_tup,
                n_dead_tup,
                n_tup_ins,
                n_tup_upd,
                n_tup_del,
                autovacuum_count,
                last_autovacuum,
                EXTRACT(epoch FROM (now() - last_autovacuum))/3600 as hours_since_last_autovacuum,
                CASE WHEN n_live_tup + n_dead_tup > 0 THEN
                    ROUND((n_dead_tup::float / (n_live_tup + n_dead_tup)) * 100, 2)
                ELSE 0 END as dead_tuple_ratio,
                pg_total_relation_size(schemaname||'.'||tablename) as table_size_bytes
            FROM pg_stat_user_tables
            WHERE n_live_tup + n_dead_tup > 1000  -- Only tables with substantial data
        ),
        vacuum_settings AS (
            SELECT 
                current_setting('autovacuum_vacuum_threshold')::int as vacuum_threshold,
                current_setting('autovacuum_vacuum_scale_factor')::float as vacuum_scale_factor
        )
        SELECT 
            ts.*,
            pg_size_pretty(table_size_bytes) as table_size,
            CASE 
                WHEN dead_tuple_ratio > 20 THEN 'NEEDS_IMMEDIATE_VACUUM'
                WHEN dead_tuple_ratio > 10 THEN 'NEEDS_VACUUM_SOON' 
                WHEN dead_tuple_ratio > 5 THEN 'MONITOR_CLOSELY'
                ELSE 'HEALTHY'
            END as vacuum_status,
            vs.vacuum_threshold + (vs.vacuum_scale_factor * n_live_tup) as calculated_vacuum_trigger,
            CASE 
                WHEN hours_since_last_autovacuum > 24 AND dead_tuple_ratio > 5 THEN 
                    'Consider reducing autovacuum_vacuum_scale_factor or vacuum_threshold'
                WHEN autovacuum_count = 0 THEN 
                    'Autovacuum may be disabled or not triggered yet'
                WHEN dead_tuple_ratio > 15 THEN 
                    'Frequent updates detected - consider HOT updates optimization'
                ELSE 'Autovacuum efficiency appears normal'
            END as recommendation
        FROM table_stats ts
        CROSS JOIN vacuum_settings vs
        ORDER BY dead_tuple_ratio DESC, table_size_bytes DESC
        LIMIT 30
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_detect_transaction_wraparound_risk() -> Dict[str, Any]:
    """Detect transaction ID wraparound risks and provide early warning."""
    query = """
        WITH xid_info AS (
            SELECT 
                datname,
                age(datfrozenxid) as xid_age,
                datfrozenxid,
                2147483648 as max_xid_age,  -- 2^31, wraparound point
                CASE 
                    WHEN age(datfrozenxid) > 1800000000 THEN 'CRITICAL'  -- >1.8B
                    WHEN age(datfrozenxid) > 1500000000 THEN 'HIGH'      -- >1.5B
                    WHEN age(datfrozenxid) > 1000000000 THEN 'MEDIUM'    -- >1B
                    ELSE 'LOW'
                END as risk_level
            FROM pg_database 
            WHERE datistemplate = false
        ),
        table_xid_info AS (
            SELECT 
                n.nspname as schema_name,
                c.relname as table_name,
                age(c.relfrozenxid) as table_xid_age,
                c.relfrozenxid,
                CASE 
                    WHEN age(c.relfrozenxid) > 1800000000 THEN 'CRITICAL'
                    WHEN age(c.relfrozenxid) > 1500000000 THEN 'HIGH' 
                    WHEN age(c.relfrozenxid) > 1000000000 THEN 'MEDIUM'
                    ELSE 'LOW'
                END as table_risk_level
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE c.relkind IN ('r', 't')  -- regular tables and TOAST tables
            AND n.nspname NOT IN ('information_schema', 'pg_catalog')
            ORDER BY age(c.relfrozenxid) DESC
            LIMIT 10
        )
        SELECT 
            'database_summary' as analysis_type,
            json_agg(
                json_build_object(
                    'database_name', datname,
                    'xid_age', xid_age,
                    'risk_level', risk_level,
                    'remaining_transactions', max_xid_age - xid_age,
                    'percent_to_wraparound', ROUND((xid_age::float / max_xid_age) * 100, 2)
                )
            ) as database_info
        FROM xid_info
        
        UNION ALL
        
        SELECT 
            'high_risk_tables' as analysis_type,
            json_agg(
                json_build_object(
                    'schema_name', schema_name,
                    'table_name', table_name,
                    'table_xid_age', table_xid_age,
                    'table_risk_level', table_risk_level
                )
            ) as table_info
        FROM table_xid_info
        WHERE table_risk_level IN ('HIGH', 'CRITICAL')
    """
    
    rows = await execute_query(query)
    result = {}
    for row in rows:
        result[row['analysis_type']] = row.get('database_info') or row.get('table_info')
    
    # Add overall risk assessment
    risk_query = """
        SELECT 
            MAX(age(datfrozenxid)) as max_db_age,
            AVG(age(datfrozenxid)) as avg_db_age,
            (SELECT MAX(age(relfrozenxid)) FROM pg_class WHERE relkind IN ('r', 't')) as max_table_age
    """
    
    risk_stats = await execute_query(risk_query)
    if risk_stats:
        stats = risk_stats[0]
        result['overall_assessment'] = {
            'max_database_age': stats['max_db_age'],
            'average_database_age': stats['avg_db_age'], 
            'max_table_age': stats['max_table_age'],
            'critical_threshold': 1800000000,
            'recommendation': 'Run VACUUM FREEZE on high-risk tables/databases' if stats['max_db_age'] > 1500000000 else 'Monitor regularly'
        }
    
    return result

@mcp.tool()
async def PostgreSQL_analyze_advanced_buffer_usage() -> List[Dict[str, Any]]:
    """Analyze advanced buffer cache utilization patterns and efficiency."""
    query = """
        WITH buffer_stats AS (
            SELECT 
                schemaname,
                tablename,
                heap_blks_read,
                heap_blks_hit,
                idx_blks_read,
                idx_blks_hit,
                toast_blks_read,
                toast_blks_hit,
                tidx_blks_read,
                tidx_blks_hit,
                -- Calculate hit ratios
                CASE WHEN heap_blks_read + heap_blks_hit > 0 THEN
                    ROUND((heap_blks_hit::float / (heap_blks_read + heap_blks_hit)) * 100, 2)
                ELSE 0 END as heap_hit_ratio,
                CASE WHEN idx_blks_read + idx_blks_hit > 0 THEN
                    ROUND((idx_blks_hit::float / (idx_blks_read + idx_blks_hit)) * 100, 2)
                ELSE 0 END as index_hit_ratio,
                -- Total block access patterns
                heap_blks_read + heap_blks_hit + idx_blks_read + idx_blks_hit as total_block_access,
                heap_blks_read + idx_blks_read + COALESCE(toast_blks_read, 0) + COALESCE(tidx_blks_read, 0) as total_reads_from_disk
            FROM pg_statio_user_tables
            WHERE heap_blks_read + heap_blks_hit + idx_blks_read + idx_blks_hit > 0
        ),
        table_sizes AS (
            SELECT 
                schemaname,
                tablename,
                pg_total_relation_size(schemaname||'.'||tablename) as total_size_bytes,
                pg_relation_size(schemaname||'.'||tablename) as table_size_bytes
            FROM pg_tables
            WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
        )
        SELECT 
            bs.schemaname,
            bs.tablename,
            pg_size_pretty(ts.total_size_bytes) as total_size,
            bs.heap_hit_ratio,
            bs.index_hit_ratio,
            bs.total_block_access,
            bs.total_reads_from_disk,
            ROUND((bs.total_reads_from_disk::float / NULLIF(bs.total_block_access, 0)) * 100, 2) as overall_miss_ratio,
            CASE 
                WHEN bs.heap_hit_ratio < 90 THEN 'POOR_HEAP_CACHING'
                WHEN bs.index_hit_ratio < 95 THEN 'POOR_INDEX_CACHING'
                WHEN bs.total_reads_from_disk > 10000 THEN 'HIGH_DISK_IO'
                ELSE 'OPTIMAL'
            END as performance_status,
            CASE 
                WHEN bs.heap_hit_ratio < 90 THEN 'Consider increasing shared_buffers or analyze table access patterns'
                WHEN bs.index_hit_ratio < 95 THEN 'Index may be too large for buffer cache or infrequently used'
                WHEN bs.total_reads_from_disk > 10000 THEN 'High disk I/O detected - consider buffer tuning'
                ELSE 'Buffer utilization appears optimal'
            END as optimization_suggestion
        FROM buffer_stats bs
        JOIN table_sizes ts ON bs.schemaname = ts.schemaname AND bs.tablename = ts.tablename
        ORDER BY bs.total_reads_from_disk DESC, bs.heap_hit_ratio ASC
        LIMIT 25
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_predict_sequence_exhaustion() -> List[Dict[str, Any]]:
    """Predict sequence exhaustion based on usage patterns and growth trends."""
    query = """
        WITH sequence_analysis AS (
            SELECT 
                n.nspname as schema_name,
                c.relname as sequence_name,
                s.last_value,
                s.start_value,
                s.increment_by,
                s.max_value,
                s.min_value,
                s.cache_size,
                s.is_cycled,
                -- Calculate usage percentage
                CASE 
                    WHEN s.increment_by > 0 THEN
                        ROUND(((s.last_value - s.start_value)::float / NULLIF((s.max_value - s.start_value), 0)) * 100, 2)
                    ELSE
                        ROUND(((s.start_value - s.last_value)::float / NULLIF((s.start_value - s.min_value), 0)) * 100, 2)
                END as usage_percentage,
                -- Calculate remaining values
                CASE 
                    WHEN s.increment_by > 0 THEN s.max_value - s.last_value
                    ELSE s.last_value - s.min_value 
                END as remaining_values,
                -- Estimate time to exhaustion based on table statistics
                (
                    SELECT COALESCE(n_tup_ins + n_tup_upd, 0) 
                    FROM pg_stat_user_tables st 
                    WHERE st.schemaname = n.nspname 
                    AND EXISTS (
                        SELECT 1 FROM pg_depend d 
                        JOIN pg_attrdef ad ON d.objid = ad.oid 
                        WHERE d.refobjid = c.oid 
                        AND ad.adrelid = st.relid
                    )
                    LIMIT 1
                ) as related_table_modifications
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            JOIN pg_sequences s ON s.schemaname = n.nspname AND s.sequencename = c.relname
            WHERE c.relkind = 'S'
            AND n.nspname NOT IN ('information_schema', 'pg_catalog')
        ),
        risk_assessment AS (
            SELECT 
                *,
                CASE 
                    WHEN usage_percentage > 95 THEN 'CRITICAL'
                    WHEN usage_percentage > 85 THEN 'HIGH'
                    WHEN usage_percentage > 70 THEN 'MEDIUM'
                    WHEN usage_percentage > 50 THEN 'LOW'
                    ELSE 'MINIMAL'
                END as exhaustion_risk,
                CASE 
                    WHEN related_table_modifications > 0 AND remaining_values > 0 THEN
                        ROUND(remaining_values::float / (related_table_modifications::float / 7.0), 0) -- Estimate days based on weekly average
                    ELSE NULL
                END as estimated_days_to_exhaustion
            FROM sequence_analysis
        )
        SELECT 
            schema_name,
            sequence_name,
            last_value,
            max_value,
            usage_percentage,
            remaining_values,
            exhaustion_risk,
            estimated_days_to_exhaustion,
            is_cycled,
            CASE 
                WHEN exhaustion_risk = 'CRITICAL' THEN 
                    CASE WHEN is_cycled THEN 'Sequence will wrap around - verify application handles this' 
                         ELSE 'URGENT: Increase max_value or restart sequence' END
                WHEN exhaustion_risk = 'HIGH' THEN 'Consider increasing max_value soon'
                WHEN estimated_days_to_exhaustion IS NOT NULL AND estimated_days_to_exhaustion < 30 THEN
                    'Monitor closely - may exhaust within a month'
                ELSE 'Monitor periodically'
            END as recommendation
        FROM risk_assessment
        WHERE usage_percentage > 10  -- Only show sequences with substantial usage
        ORDER BY usage_percentage DESC, exhaustion_risk DESC
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_identify_index_redundancy() -> List[Dict[str, Any]]:
    """Identify potentially redundant or overlapping indexes."""
    query = """
        WITH index_info AS (
            SELECT 
                n.nspname as schema_name,
                t.relname as table_name,
                i.relname as index_name,
                idx.indisunique as is_unique,
                idx.indisprimary as is_primary,
                pg_get_indexdef(i.oid) as index_definition,
                array_to_string(ARRAY(
                    SELECT pg_get_indexdef(i.oid, k + 1, true)
                    FROM generate_subscripts(idx.indkey, 1) as k
                    ORDER BY k
                ), ', ') as index_columns,
                array_length(idx.indkey, 1) as column_count,
                pg_relation_size(i.oid) as index_size_bytes,
                COALESCE(s.idx_scan, 0) as index_scans,
                COALESCE(s.idx_tup_read, 0) as tuples_read,
                COALESCE(s.idx_tup_fetch, 0) as tuples_fetched
            FROM pg_index idx
            JOIN pg_class i ON i.oid = idx.indexrelid
            JOIN pg_class t ON t.oid = idx.indrelid
            JOIN pg_namespace n ON t.relnamespace = n.oid
            LEFT JOIN pg_stat_user_indexes s ON s.indexrelid = i.oid
            WHERE n.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            AND i.relkind = 'i'
        ),
        redundancy_analysis AS (
            SELECT 
                i1.schema_name,
                i1.table_name,
                i1.index_name as index1_name,
                i1.index_columns as index1_columns,
                i1.is_unique as index1_unique,
                i1.is_primary as index1_primary,
                i1.index_scans as index1_scans,
                pg_size_pretty(i1.index_size_bytes) as index1_size,
                i2.index_name as index2_name,
                i2.index_columns as index2_columns,
                i2.is_unique as index2_unique,
                i2.is_primary as index2_primary,
                i2.index_scans as index2_scans,
                pg_size_pretty(i2.index_size_bytes) as index2_size,
                CASE 
                    WHEN i1.index_columns = i2.index_columns THEN 'IDENTICAL'
                    WHEN i1.index_columns LIKE i2.index_columns || '%' THEN 'INDEX1_EXTENDS_INDEX2'
                    WHEN i2.index_columns LIKE i1.index_columns || '%' THEN 'INDEX2_EXTENDS_INDEX1'
                    WHEN position(i2.index_columns in i1.index_columns) = 1 THEN 'INDEX1_COVERS_INDEX2'
                    WHEN position(i1.index_columns in i2.index_columns) = 1 THEN 'INDEX2_COVERS_INDEX1'
                    ELSE 'NO_REDUNDANCY'
                END as redundancy_type
            FROM index_info i1
            JOIN index_info i2 ON (
                i1.schema_name = i2.schema_name 
                AND i1.table_name = i2.table_name
                AND i1.index_name < i2.index_name  -- Avoid duplicate pairs
            )
        )
        SELECT 
            schema_name,
            table_name,
            index1_name,
            index1_columns,
            index1_scans,
            index1_size,
            index2_name,
            index2_columns,
            index2_scans,
            index2_size,
            redundancy_type,
            CASE 
                WHEN redundancy_type = 'IDENTICAL' THEN 
                    CASE WHEN index1_scans < index2_scans THEN 'Consider dropping ' || index1_name
                         ELSE 'Consider dropping ' || index2_name END
                WHEN redundancy_type LIKE '%EXTENDS%' THEN 
                    'The extending index may make the shorter one redundant'
                WHEN redundancy_type LIKE '%COVERS%' THEN 
                    'The covering index may make the covered one redundant'
                ELSE 'No specific recommendation'
            END as recommendation
        FROM redundancy_analysis
        WHERE redundancy_type != 'NO_REDUNDANCY'
        AND NOT (index1_primary OR index2_primary)  -- Don't suggest dropping primary keys
        AND NOT (index1_unique AND index2_unique AND redundancy_type = 'IDENTICAL')  -- Keep unique constraints
        ORDER BY schema_name, table_name, redundancy_type
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_assess_trigger_performance_impact() -> List[Dict[str, Any]]:
    """Assess the performance impact of triggers on table operations."""
    query = """
        WITH trigger_info AS (
            SELECT 
                n.nspname as schema_name,
                c.relname as table_name,
                t.tgname as trigger_name,
                CASE t.tgtype & 66
                    WHEN 2 THEN 'BEFORE'
                    WHEN 64 THEN 'INSTEAD OF'
                    ELSE 'AFTER'
                END as timing,
                CASE t.tgtype & 28
                    WHEN 4 THEN 'INSERT'
                    WHEN 8 THEN 'DELETE' 
                    WHEN 16 THEN 'UPDATE'
                    WHEN 12 THEN 'INSERT, DELETE'
                    WHEN 20 THEN 'INSERT, UPDATE'
                    WHEN 24 THEN 'DELETE, UPDATE'
                    WHEN 28 THEN 'INSERT, DELETE, UPDATE'
                    ELSE 'UNKNOWN'
                END as events,
                p.proname as function_name,
                l.lanname as language,
                CASE WHEN t.tgtype & 1 = 1 THEN 'ROW' ELSE 'STATEMENT' END as orientation,
                t.tgenabled as is_enabled,
                LENGTH(p.prosrc) as function_code_length
            FROM pg_trigger t
            JOIN pg_class c ON t.tgrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            JOIN pg_proc p ON t.tgfoid = p.oid
            JOIN pg_language l ON p.prolang = l.oid
            WHERE NOT t.tgisinternal
            AND n.nspname NOT IN ('information_schema', 'pg_catalog')
        ),
        table_activity AS (
            SELECT 
                schemaname,
                tablename,
                n_tup_ins as inserts,
                n_tup_upd as updates,
                n_tup_del as deletes,
                n_tup_ins + n_tup_upd + n_tup_del as total_modifications,
                seq_scan + COALESCE(idx_scan, 0) as total_scans
            FROM pg_stat_user_tables
        )
        SELECT 
            ti.schema_name,
            ti.table_name,
            ti.trigger_name,
            ti.timing,
            ti.events,
            ti.function_name,
            ti.language,
            ti.orientation,
            ti.is_enabled,
            COALESCE(ta.total_modifications, 0) as table_modifications,
            COALESCE(ta.inserts, 0) as table_inserts,
            COALESCE(ta.updates, 0) as table_updates,
            COALESCE(ta.deletes, 0) as table_deletes,
            ti.function_code_length,
            -- Impact assessment
            CASE 
                WHEN ti.timing = 'BEFORE' AND ta.total_modifications > 10000 THEN 'HIGH_IMPACT'
                WHEN ti.orientation = 'ROW' AND ta.total_modifications > 50000 THEN 'HIGH_IMPACT'
                WHEN ti.function_code_length > 5000 AND ta.total_modifications > 1000 THEN 'MODERATE_IMPACT'
                WHEN ta.total_modifications > 1000 THEN 'LOW_IMPACT'
                ELSE 'MINIMAL_IMPACT'
            END as performance_impact,
            CASE 
                WHEN ti.timing = 'BEFORE' AND ta.total_modifications > 10000 THEN 
                    'BEFORE triggers on high-activity tables can significantly slow operations'
                WHEN ti.orientation = 'ROW' AND ta.total_modifications > 50000 THEN
                    'Row-level triggers on very active tables may cause performance issues'
                WHEN ti.language != 'plpgsql' AND ti.language != 'c' THEN
                    'Trigger uses ' || ti.language || ' which may have performance implications'
                WHEN ti.function_code_length > 5000 THEN
                    'Large trigger function may impact performance - consider optimization'
                ELSE 'Performance impact appears acceptable'
            END as recommendation
        FROM trigger_info ti
        LEFT JOIN table_activity ta ON ti.schema_name = ta.schemaname AND ti.table_name = ta.tablename
        ORDER BY 
            CASE WHEN performance_impact = 'HIGH_IMPACT' THEN 1
                 WHEN performance_impact = 'MODERATE_IMPACT' THEN 2
                 ELSE 3 END,
            COALESCE(ta.total_modifications, 0) DESC
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_analyze_connection_pool_efficiency() -> Dict[str, Any]:
    """Analyze connection pool efficiency and usage patterns."""
    query = """
        WITH connection_stats AS (
            SELECT 
                datname as database_name,
                usename as username,
                client_addr,
                state,
                COUNT(*) as connection_count,
                AVG(EXTRACT(epoch FROM (now() - backend_start))) as avg_connection_age_seconds,
                MAX(EXTRACT(epoch FROM (now() - backend_start))) as max_connection_age_seconds,
                MIN(EXTRACT(epoch FROM (now() - backend_start))) as min_connection_age_seconds,
                AVG(CASE WHEN state_change IS NOT NULL THEN 
                    EXTRACT(epoch FROM (now() - state_change)) ELSE NULL END) as avg_state_duration_seconds
            FROM pg_stat_activity
            WHERE pid != pg_backend_pid()  -- Exclude current connection
            GROUP BY datname, usename, client_addr, state
        ),
        idle_analysis AS (
            SELECT 
                COUNT(*) FILTER (WHERE state = 'idle') as idle_connections,
                COUNT(*) FILTER (WHERE state = 'active') as active_connections,
                COUNT(*) FILTER (WHERE state = 'idle in transaction') as idle_in_transaction,
                COUNT(*) FILTER (WHERE state = 'idle in transaction (aborted)') as idle_in_transaction_aborted,
                COUNT(*) as total_connections,
                AVG(EXTRACT(epoch FROM (now() - backend_start))) FILTER (WHERE state = 'idle') as avg_idle_connection_age,
                COUNT(*) FILTER (WHERE state = 'idle in transaction' 
                    AND EXTRACT(epoch FROM (now() - state_change)) > 300) as long_idle_transactions
            FROM pg_stat_activity
            WHERE pid != pg_backend_pid()
        ),
        settings_info AS (
            SELECT 
                (SELECT setting::int FROM pg_settings WHERE name = 'max_connections') as max_connections,
                (SELECT setting FROM pg_settings WHERE name = 'shared_preload_libraries') as shared_preload_libraries
        )
        SELECT 
            'connection_distribution' as analysis_type,
            json_agg(
                json_build_object(
                    'database_name', database_name,
                    'username', username,
                    'client_addr', client_addr,
                    'state', state,
                    'connection_count', connection_count,
                    'avg_connection_age_hours', ROUND(avg_connection_age_seconds / 3600.0, 2),
                    'max_connection_age_hours', ROUND(max_connection_age_seconds / 3600.0, 2)
                )
            ) as connection_data
        FROM connection_stats
        WHERE connection_count > 1  -- Focus on multiple connections
        
        UNION ALL
        
        SELECT 
            'efficiency_metrics' as analysis_type,
            json_build_object(
                'total_connections', total_connections,
                'max_connections', max_connections,
                'connection_utilization_percent', ROUND((total_connections::float / max_connections) * 100, 2),
                'active_connections', active_connections,
                'idle_connections', idle_connections,
                'idle_in_transaction', idle_in_transaction,
                'idle_in_transaction_aborted', idle_in_transaction_aborted,
                'long_idle_transactions', long_idle_transactions,
                'avg_idle_connection_age_hours', ROUND(COALESCE(avg_idle_connection_age, 0) / 3600.0, 2),
                'efficiency_score', CASE 
                    WHEN total_connections = 0 THEN 100
                    ELSE ROUND((active_connections::float / total_connections) * 100, 2)
                END,
                'has_connection_pooler', CASE 
                    WHEN shared_preload_libraries LIKE '%pg_bouncer%' OR shared_preload_libraries LIKE '%pgpool%' THEN true
                    ELSE false
                END
            ) as efficiency_data
        FROM idle_analysis
        CROSS JOIN settings_info
    """
    
    rows = await execute_query(query)
    result = {}
    for row in rows:
        result[row['analysis_type']] = row.get('connection_data') or row.get('efficiency_data')
    
    # Add recommendations
    if 'efficiency_metrics' in result:
        metrics = result['efficiency_metrics']
        recommendations = []
        
        if metrics.get('long_idle_transactions', 0) > 0:
            recommendations.append("Terminate long-running idle transactions")
        
        if metrics.get('efficiency_score', 0) < 50:
            recommendations.append("High number of idle connections - consider connection pooling")
            
        if metrics.get('connection_utilization_percent', 0) > 80:
            recommendations.append("Connection utilization high - monitor for connection exhaustion")
            
        if not metrics.get('has_connection_pooler', False) and metrics.get('total_connections', 0) > 50:
            recommendations.append("Consider implementing connection pooling (PgBouncer/PgPool)")
            
        result['recommendations'] = recommendations
    
    return result

@mcp.tool()
async def PostgreSQL_detect_table_bloat_regression() -> List[Dict[str, Any]]:
    """Detect table bloat regression patterns over time and predict maintenance needs."""
    query = """
        WITH current_bloat AS (
            SELECT 
                schemaname,
                tablename,
                n_live_tup,
                n_dead_tup,
                n_tup_ins,
                n_tup_upd,
                n_tup_del,
                n_tup_hot_upd,
                vacuum_count,
                autovacuum_count,
                last_vacuum,
                last_autovacuum,
                CASE WHEN n_live_tup + n_dead_tup > 0 THEN
                    ROUND((n_dead_tup::float / (n_live_tup + n_dead_tup)) * 100, 2)
                ELSE 0 END as current_bloat_ratio,
                pg_total_relation_size(schemaname||'.'||tablename) as table_size_bytes
            FROM pg_stat_user_tables
            WHERE n_live_tup + n_dead_tup > 100  -- Tables with substantial data
        ),
        bloat_analysis AS (
            SELECT 
                *,
                -- Calculate bloat trend indicators
                CASE WHEN n_tup_upd > 0 THEN
                    ROUND((n_tup_hot_upd::float / n_tup_upd) * 100, 2)
                ELSE 0 END as hot_update_ratio,
                -- Vacuum frequency analysis
                EXTRACT(epoch FROM (now() - COALESCE(last_autovacuum, last_vacuum)))/(24*3600) as days_since_last_vacuum,
                CASE WHEN vacuum_count + autovacuum_count > 0 THEN
                    (n_tup_upd + n_tup_del)::float / (vacuum_count + autovacuum_count)
                ELSE 0 END as avg_modifications_per_vacuum,
                pg_size_pretty(table_size_bytes) as table_size
            FROM current_bloat
        ),
        regression_assessment AS (
            SELECT 
                *,
                CASE 
                    WHEN current_bloat_ratio > 25 THEN 'SEVERE_BLOAT'
                    WHEN current_bloat_ratio > 15 THEN 'HIGH_BLOAT'
                    WHEN current_bloat_ratio > 10 THEN 'MODERATE_BLOAT'
                    WHEN current_bloat_ratio > 5 THEN 'MINOR_BLOAT'
                    ELSE 'HEALTHY'
                END as bloat_status,
                CASE 
                    WHEN days_since_last_vacuum > 7 AND current_bloat_ratio > 15 THEN 'VACUUM_OVERDUE'
                    WHEN days_since_last_vacuum > 3 AND current_bloat_ratio > 10 THEN 'VACUUM_NEEDED'
                    WHEN avg_modifications_per_vacuum > 10000 THEN 'VACUUM_TOO_INFREQUENT'
                    ELSE 'VACUUM_OK'
                END as vacuum_status,
                CASE 
                    WHEN hot_update_ratio < 50 AND n_tup_upd > 1000 THEN 'POOR_HOT_UPDATES'
                    WHEN hot_update_ratio < 70 AND n_tup_upd > 5000 THEN 'SUBOPTIMAL_HOT_UPDATES'
                    ELSE 'HOT_UPDATES_OK'
                END as update_efficiency
            FROM bloat_analysis
        )
        SELECT 
            schemaname,
            tablename,
            table_size,
            current_bloat_ratio,
            bloat_status,
            n_live_tup as live_tuples,
            n_dead_tup as dead_tuples,
            hot_update_ratio,
            update_efficiency,
            ROUND(days_since_last_vacuum, 1) as days_since_last_vacuum,
            vacuum_status,
            ROUND(avg_modifications_per_vacuum, 0) as avg_modifications_per_vacuum,
            -- Predictive maintenance recommendations
            CASE 
                WHEN bloat_status = 'SEVERE_BLOAT' THEN 'IMMEDIATE: Run VACUUM FULL during maintenance window'
                WHEN bloat_status = 'HIGH_BLOAT' AND vacuum_status = 'VACUUM_OVERDUE' THEN 
                    'URGENT: Run VACUUM immediately, consider more frequent autovacuum'
                WHEN update_efficiency = 'POOR_HOT_UPDATES' THEN 
                    'OPTIMIZE: Review table schema for HOT update optimization (reduce indexed columns on frequently updated fields)'
                WHEN vacuum_status = 'VACUUM_TOO_INFREQUENT' THEN
                    'TUNE: Decrease autovacuum_vacuum_scale_factor or autovacuum_vacuum_threshold'
                WHEN current_bloat_ratio > 10 AND days_since_last_vacuum > 1 THEN
                    'SCHEDULE: Plan vacuum during low-activity period'
                ELSE 'MONITOR: Current state is acceptable'
            END as maintenance_recommendation,
            -- Bloat growth prediction
            CASE 
                WHEN avg_modifications_per_vacuum > 0 AND days_since_last_vacuum > 0 THEN
                    ROUND((n_tup_upd + n_tup_del) / (days_since_last_vacuum * avg_modifications_per_vacuum), 2)
                ELSE NULL
            END as estimated_bloat_growth_rate_per_day
        FROM regression_assessment
        WHERE current_bloat_ratio > 5  -- Focus on tables with meaningful bloat
        OR bloat_status != 'HEALTHY'
        OR vacuum_status != 'VACUUM_OK'
        ORDER BY 
            CASE WHEN bloat_status = 'SEVERE_BLOAT' THEN 1
                 WHEN bloat_status = 'HIGH_BLOAT' THEN 2
                 WHEN bloat_status = 'MODERATE_BLOAT' THEN 3
                 ELSE 4 END,
            current_bloat_ratio DESC
        LIMIT 25
    """
    
    rows = await execute_query(query)
    return rows

# ===== NEW ADVANCED POSTGRESQL TOOLS =====

@mcp.tool()
async def PostgreSQL_vacuum_freeze_age_analysis() -> List[Dict[str, Any]]:
    """Identify tables and databases approaching XID wraparound vacuum freeze threshold."""
    query = """
        WITH freeze_analysis AS (
            SELECT 
                current_database() as database_name,
                schemaname,
                tablename,
                age(relfrozenxid) as table_age,
                age(datfrozenxid) as database_age,
                -- Calculate percentage toward autovacuum_freeze_max_age (default 200M)
                ROUND((age(relfrozenxid)::float / 200000000.0) * 100, 2) as table_freeze_percent,
                ROUND((age(datfrozenxid)::float / 200000000.0) * 100, 2) as database_freeze_percent,
                last_vacuum,
                last_autovacuum,
                n_tup_ins + n_tup_upd + n_tup_del as total_modifications
            FROM pg_stat_user_tables psu
            JOIN pg_class pc ON psu.relid = pc.oid
            CROSS JOIN pg_database pd
            WHERE pd.datname = current_database()
        ),
        risk_assessment AS (
            SELECT 
                *,
                CASE 
                    WHEN table_freeze_percent > 90 THEN 'CRITICAL'
                    WHEN table_freeze_percent > 75 THEN 'HIGH'
                    WHEN table_freeze_percent > 50 THEN 'MODERATE'
                    WHEN table_freeze_percent > 25 THEN 'LOW'
                    ELSE 'MINIMAL'
                END as table_risk_level,
                CASE 
                    WHEN database_freeze_percent > 90 THEN 'CRITICAL'
                    WHEN database_freeze_percent > 75 THEN 'HIGH'
                    WHEN database_freeze_percent > 50 THEN 'MODERATE'
                    WHEN database_freeze_percent > 25 THEN 'LOW'
                    ELSE 'MINIMAL'
                END as database_risk_level,
                200000000 - table_age as transactions_until_freeze,
                EXTRACT(epoch FROM (now() - COALESCE(last_autovacuum, last_vacuum)))/(24*3600) as days_since_last_vacuum
            FROM freeze_analysis
        )
        SELECT 
            database_name,
            schemaname,
            tablename,
            table_age,
            table_freeze_percent,
            table_risk_level,
            database_age,
            database_freeze_percent,
            database_risk_level,
            transactions_until_freeze,
            ROUND(days_since_last_vacuum, 1) as days_since_last_vacuum,
            total_modifications,
            CASE 
                WHEN table_risk_level = 'CRITICAL' THEN 'IMMEDIATE: Force vacuum freeze required'
                WHEN table_risk_level = 'HIGH' THEN 'URGENT: Schedule vacuum freeze soon'
                WHEN database_risk_level = 'HIGH' THEN 'URGENT: Database-wide freeze risk'
                WHEN table_risk_level = 'MODERATE' THEN 'MONITOR: Plan maintenance window'
                ELSE 'NORMAL: Continue monitoring'
            END as recommendation
        FROM risk_assessment
        WHERE table_risk_level != 'MINIMAL' OR database_risk_level != 'MINIMAL'
        ORDER BY table_freeze_percent DESC, database_freeze_percent DESC
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_replication_slot_activity_analysis() -> List[Dict[str, Any]]:
    """Detailed analysis of logical and physical replication slots with lag statistics."""
    query = """
        WITH slot_analysis AS (
            SELECT 
                slot_name,
                plugin,
                slot_type,
                database,
                active,
                active_pid,
                restart_lsn,
                confirmed_flush_lsn,
                wal_status,
                safe_wal_size,
                two_phase,
                -- Calculate WAL lag
                CASE WHEN confirmed_flush_lsn IS NOT NULL THEN
                    pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)
                ELSE
                    pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)
                END as wal_lag_bytes,
                CASE WHEN active_pid IS NOT NULL THEN
                    EXTRACT(epoch FROM (now() - backend_start))
                ELSE NULL END as connection_duration_seconds
            FROM pg_replication_slots prs
            LEFT JOIN pg_stat_activity psa ON prs.active_pid = psa.pid
        ),
        lag_analysis AS (
            SELECT 
                *,
                pg_size_pretty(wal_lag_bytes) as wal_lag_size,
                CASE 
                    WHEN wal_lag_bytes > 1073741824 THEN 'CRITICAL'  -- > 1GB
                    WHEN wal_lag_bytes > 268435456 THEN 'HIGH'        -- > 256MB
                    WHEN wal_lag_bytes > 67108864 THEN 'MODERATE'     -- > 64MB
                    WHEN wal_lag_bytes > 16777216 THEN 'LOW'          -- > 16MB
                    ELSE 'NORMAL'
                END as lag_severity,
                CASE WHEN safe_wal_size IS NOT NULL THEN
                    pg_size_pretty(safe_wal_size)
                ELSE 'N/A' END as safe_wal_size_formatted,
                ROUND(connection_duration_seconds / 3600.0, 2) as connection_hours
            FROM slot_analysis
        )
        SELECT 
            slot_name,
            plugin,
            slot_type,
            database,
            active,
            active_pid,
            wal_lag_size,
            lag_severity,
            wal_status,
            safe_wal_size_formatted,
            two_phase,
            connection_hours,
            CASE 
                WHEN lag_severity = 'CRITICAL' THEN 'IMMEDIATE: Replication lag critical - check consumer'
                WHEN lag_severity = 'HIGH' THEN 'URGENT: High replication lag detected'
                WHEN NOT active THEN 'WARNING: Replication slot inactive'
                WHEN wal_status = 'lost' THEN 'ERROR: WAL files lost for this slot'
                WHEN wal_status = 'unreserved' THEN 'CAUTION: WAL retention not guaranteed'
                ELSE 'OK: Replication slot healthy'
            END as status_recommendation,
            -- Estimate time to fill remaining safe WAL space
            CASE WHEN active AND safe_wal_size IS NOT NULL AND wal_lag_bytes > 0 THEN
                ROUND(((safe_wal_size - wal_lag_bytes)::float / (wal_lag_bytes / GREATEST(connection_duration_seconds, 3600))) / 3600.0, 2)
            ELSE NULL END as estimated_hours_until_wal_full
        FROM lag_analysis
        ORDER BY 
            CASE WHEN lag_severity = 'CRITICAL' THEN 1
                 WHEN lag_severity = 'HIGH' THEN 2
                 WHEN NOT active THEN 3
                 ELSE 4 END,
            wal_lag_bytes DESC NULLS LAST
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_long_running_prepared_transactions() -> List[Dict[str, Any]]:
    """List prepared transactions sorted by duration with detailed analysis."""
    query = """
        WITH prepared_tx_analysis AS (
            SELECT 
                gid as transaction_id,
                prepared as prepare_timestamp,
                owner,
                database,
                EXTRACT(epoch FROM (now() - prepared)) as duration_seconds,
                EXTRACT(epoch FROM (now() - prepared)) / 3600.0 as duration_hours,
                EXTRACT(epoch FROM (now() - prepared)) / (24*3600.0) as duration_days
            FROM pg_prepared_xacts
        ),
        impact_analysis AS (
            SELECT 
                *,
                CASE 
                    WHEN duration_hours > 24 THEN 'CRITICAL'
                    WHEN duration_hours > 12 THEN 'HIGH'
                    WHEN duration_hours > 4 THEN 'MODERATE'
                    WHEN duration_hours > 1 THEN 'LOW'
                    ELSE 'NORMAL'
                END as risk_level,
                CASE 
                    WHEN duration_hours > 24 THEN 'SEVERE: May be blocking vacuum and causing bloat'
                    WHEN duration_hours > 12 THEN 'HIGH: Potential vacuum blocking and lock contention'
                    WHEN duration_hours > 4 THEN 'MODERATE: Monitor for increasing duration'
                    WHEN duration_hours > 1 THEN 'LOW: Normal duration range'
                    ELSE 'NORMAL: Recently prepared'
                END as impact_description
            FROM prepared_tx_analysis
        )
        SELECT 
            transaction_id,
            owner,
            database,
            prepare_timestamp,
            ROUND(duration_hours, 2) as duration_hours,
            ROUND(duration_days, 2) as duration_days,
            risk_level,
            impact_description,
            CASE 
                WHEN risk_level = 'CRITICAL' THEN 'IMMEDIATE: Investigate and resolve/rollback prepared transaction'
                WHEN risk_level = 'HIGH' THEN 'URGENT: Contact application team to resolve transaction'
                WHEN risk_level = 'MODERATE' THEN 'MONITOR: Set alert for further duration increase'
                ELSE 'OK: Continue monitoring'
            END as recommendation,
            -- Show potential blocking info
            CASE WHEN duration_hours > 1 THEN
                'May be preventing vacuum from cleaning old row versions'
            ELSE 'No immediate vacuum impact expected' END as vacuum_impact
        FROM impact_analysis
        ORDER BY duration_seconds DESC
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_vacuum_progress_monitoring() -> List[Dict[str, Any]]:
    """Monitor active vacuum operations and their performance impact."""
    query = """
        WITH vacuum_progress AS (
            SELECT 
                p.pid,
                p.datname as database_name,
                p.relid::regclass as table_name,
                p.phase,
                p.heap_blks_total,
                p.heap_blks_scanned,
                p.heap_blks_vacuumed,
                p.index_vacuum_count,
                p.max_dead_tuples,
                p.num_dead_tuples,
                -- Calculate progress percentages
                CASE WHEN p.heap_blks_total > 0 THEN
                    ROUND((p.heap_blks_scanned::float / p.heap_blks_total) * 100, 2)
                ELSE 0 END as scan_progress_percent,
                CASE WHEN p.heap_blks_scanned > 0 THEN
                    ROUND((p.heap_blks_vacuumed::float / p.heap_blks_scanned) * 100, 2)
                ELSE 0 END as vacuum_progress_percent,
                -- Get session info
                a.backend_start,
                a.xact_start,
                a.query_start,
                a.state,
                EXTRACT(epoch FROM (now() - a.query_start)) as vacuum_duration_seconds,
                -- Calculate rates
                CASE WHEN EXTRACT(epoch FROM (now() - a.query_start)) > 0 THEN
                    ROUND(p.heap_blks_scanned / EXTRACT(epoch FROM (now() - a.query_start)), 2)
                ELSE 0 END as blocks_per_second
            FROM pg_stat_progress_vacuum p
            JOIN pg_stat_activity a ON p.pid = a.pid
        ),
        performance_analysis AS (
            SELECT 
                *,
                ROUND(vacuum_duration_seconds / 60.0, 2) as vacuum_duration_minutes,
                CASE WHEN scan_progress_percent > 0 AND blocks_per_second > 0 THEN
                    ROUND(((heap_blks_total - heap_blks_scanned) / blocks_per_second) / 60.0, 2)
                ELSE NULL END as estimated_minutes_remaining,
                CASE 
                    WHEN blocks_per_second < 100 THEN 'SLOW'
                    WHEN blocks_per_second < 500 THEN 'MODERATE'
                    WHEN blocks_per_second < 1000 THEN 'GOOD'
                    ELSE 'FAST'
                END as vacuum_speed_rating,
                pg_size_pretty(heap_blks_total * 8192) as table_size,
                pg_size_pretty(heap_blks_scanned * 8192) as scanned_size
            FROM vacuum_progress
        )
        SELECT 
            pid,
            database_name,
            table_name,
            phase,
            table_size,
            scanned_size,
            scan_progress_percent,
            vacuum_progress_percent,
            vacuum_duration_minutes,
            estimated_minutes_remaining,
            blocks_per_second,
            vacuum_speed_rating,
            index_vacuum_count,
            num_dead_tuples,
            max_dead_tuples,
            CASE WHEN max_dead_tuples > 0 THEN
                ROUND((num_dead_tuples::float / max_dead_tuples) * 100, 2)
            ELSE 0 END as dead_tuple_buffer_usage_percent,
            CASE 
                WHEN vacuum_speed_rating = 'SLOW' AND vacuum_duration_minutes > 30 THEN 
                    'CONCERN: Slow vacuum may impact performance - consider increasing maintenance_work_mem'
                WHEN estimated_minutes_remaining > 60 THEN
                    'INFO: Long-running vacuum expected - monitor system load'
                WHEN phase = 'vacuuming heap' AND scan_progress_percent < 10 THEN
                    'EARLY: Vacuum just started'
                ELSE 'OK: Vacuum progressing normally'
            END as performance_assessment
        FROM performance_analysis
        ORDER BY vacuum_duration_minutes DESC
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_buffer_cache_relation_analysis() -> List[Dict[str, Any]]:
    """Analyze buffer cache distribution per relation with detailed breakdown."""
    query = """
        WITH buffer_cache_stats AS (
            SELECT 
                c.oid as relid,
                n.nspname as schema_name,
                c.relname as table_name,
                c.relkind,
                COUNT(*) as cached_pages,
                COUNT(*) * 8192 as cached_bytes,
                pg_total_relation_size(c.oid) as total_relation_size,
                pg_relation_size(c.oid) as table_size,
                -- Calculate cache hit ratio for this relation
                COUNT(*) FILTER (WHERE b.isdirty) as dirty_pages,
                COUNT(*) FILTER (WHERE b.usagecount > 3) as frequently_accessed_pages,
                AVG(b.usagecount) as avg_usage_count
            FROM pg_buffercache b
            JOIN pg_class c ON b.relfilenode = pg_relation_filenode(c.oid)
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE b.relfilenode IS NOT NULL
            GROUP BY c.oid, n.nspname, c.relname, c.relkind
        ),
        cache_efficiency AS (
            SELECT 
                *,
                pg_size_pretty(cached_bytes) as cached_size,
                pg_size_pretty(total_relation_size) as total_size,
                CASE WHEN total_relation_size > 0 THEN
                    ROUND((cached_bytes::float / total_relation_size) * 100, 2)
                ELSE 0 END as cache_coverage_percent,
                ROUND((dirty_pages::float / cached_pages) * 100, 2) as dirty_page_percent,
                ROUND((frequently_accessed_pages::float / cached_pages) * 100, 2) as hot_page_percent,
                ROUND(avg_usage_count, 2) as avg_usage_count_rounded,
                CASE 
                    WHEN relkind = 'r' THEN 'table'
                    WHEN relkind = 'i' THEN 'index'
                    WHEN relkind = 't' THEN 'toast'
                    WHEN relkind = 'm' THEN 'materialized_view'
                    ELSE relkind::text
                END as relation_type
            FROM buffer_cache_stats
        ),
        performance_assessment AS (
            SELECT 
                *,
                CASE 
                    WHEN cache_coverage_percent = 100 AND total_relation_size > 8192000 THEN 'FULLY_CACHED'
                    WHEN cache_coverage_percent > 80 THEN 'WELL_CACHED'
                    WHEN cache_coverage_percent > 50 THEN 'MODERATELY_CACHED'
                    WHEN cache_coverage_percent > 20 THEN 'POORLY_CACHED'
                    ELSE 'RARELY_CACHED'
                END as cache_status,
                CASE 
                    WHEN avg_usage_count_rounded > 4 THEN 'HIGH_ACTIVITY'
                    WHEN avg_usage_count_rounded > 2 THEN 'MODERATE_ACTIVITY'
                    WHEN avg_usage_count_rounded > 1 THEN 'LOW_ACTIVITY'
                    ELSE 'MINIMAL_ACTIVITY'
                END as access_pattern
            FROM cache_efficiency
        )
        SELECT 
            schema_name,
            table_name,
            relation_type,
            cached_pages,
            cached_size,
            total_size,
            cache_coverage_percent,
            cache_status,
            dirty_page_percent,
            hot_page_percent,
            avg_usage_count_rounded,
            access_pattern,
            CASE 
                WHEN cache_status = 'POORLY_CACHED' AND access_pattern = 'HIGH_ACTIVITY' THEN
                    'OPTIMIZE: Consider increasing shared_buffers for this high-activity relation'
                WHEN dirty_page_percent > 50 THEN
                    'MONITOR: High dirty page ratio - check checkpoint frequency'
                WHEN cache_coverage_percent = 100 AND total_relation_size > 100000000 THEN
                    'GOOD: Large relation fully cached - excellent performance expected'
                WHEN cache_status = 'RARELY_CACHED' AND relation_type = 'index' THEN
                    'INVESTIGATE: Index not being cached - check if index is being used'
                ELSE 'OK: Normal cache behavior'
            END as optimization_recommendation
        FROM performance_assessment
        WHERE cached_pages > 10  -- Focus on relations with meaningful cache presence
        ORDER BY cached_bytes DESC
        LIMIT 30
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_column_statistics() -> List[Dict[str, Any]]:
    """Get statistics information for table columns."""
    query = """
        SELECT 
            schemaname,
            tablename,
            attname as column_name,
            n_distinct,
            correlation,
            most_common_vals[1:5] as top_values,
            most_common_freqs[1:5] as top_frequencies,
            histogram_bounds[1:5] as sample_values
        FROM pg_stats
        WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
        AND n_distinct IS NOT NULL
        ORDER BY schemaname, tablename, attname
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_toast_tables() -> List[Dict[str, Any]]:
    """Get information about TOAST tables and their usage."""
    query = """
        SELECT 
            n.nspname as schema_name,
            ct.relname as table_name,
            toast_n.nspname as toast_schema,
            toast_c.relname as toast_table_name,
            pg_size_pretty(pg_total_relation_size(toast_c.oid)) as toast_size,
            pg_size_pretty(pg_total_relation_size(ct.oid)) as main_table_size,
            round(
                (pg_total_relation_size(toast_c.oid)::float / 
                 GREATEST(pg_total_relation_size(ct.oid), 1)) * 100, 2
            ) as toast_percentage
        FROM pg_class ct
        JOIN pg_namespace n ON ct.relnamespace = n.oid
        JOIN pg_class toast_c ON ct.reltoastrelid = toast_c.oid
        JOIN pg_namespace toast_n ON toast_c.relnamespace = toast_n.oid
        WHERE ct.relkind = 'r'
        AND n.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        ORDER BY pg_total_relation_size(toast_c.oid) DESC
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_foreign_tables() -> List[Dict[str, Any]]:
    """Get information about foreign tables and foreign data wrappers."""
    query = """
        SELECT 
            n.nspname as schema_name,
            c.relname as table_name,
            s.srvname as server_name,
            w.fdwname as fdw_name,
            ft.ftoptions as table_options,
            s.srvoptions as server_options
        FROM pg_foreign_table ft
        JOIN pg_class c ON ft.ftrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        JOIN pg_foreign_server s ON ft.ftserver = s.oid
        JOIN pg_foreign_data_wrapper w ON s.srvfdw = w.oid
        ORDER BY n.nspname, c.relname
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_checkpoint_info() -> Dict[str, Any]:
    """Get checkpoint timing and performance information."""
    query = """
        SELECT 
            checkpoints_timed,
            checkpoints_req as checkpoints_requested,
            checkpoint_write_time,
            checkpoint_sync_time,
            buffers_checkpoint,
            buffers_clean,
            maxwritten_clean,
            buffers_backend,
            buffers_backend_fsync,
            buffers_alloc,
            stats_reset,
            round(
                (checkpoint_write_time + checkpoint_sync_time)::float / 
                GREATEST(checkpoints_timed + checkpoints_req, 1), 2
            ) as avg_checkpoint_time_ms
        FROM pg_stat_bgwriter
    """
    
    rows = await execute_query(query)
    return rows[0] if rows else {}

@mcp.tool()
async def PostgreSQL_get_query_plans(limit: int = 10) -> List[Dict[str, Any]]:
    """Get query execution plans from pg_stat_statements (if available).
    
    Args:
        limit: Maximum number of plans to return
    """
    query = """
        SELECT 
            query,
            calls,
            total_exec_time,
            mean_exec_time,
            rows,
            100.0 * shared_blks_hit / GREATEST(shared_blks_hit + shared_blks_read, 1) as hit_percent,
            temp_blks_read,
            temp_blks_written
        FROM pg_stat_statements
        ORDER BY total_exec_time DESC
        LIMIT $1
    """
    
    try:
        rows = await execute_query(query, limit)
        return rows
    except Exception:
        return [{"error": "pg_stat_statements extension not available or enabled"}]

@mcp.tool()
async def PostgreSQL_get_subscription_info() -> List[Dict[str, Any]]:
    """Get logical replication subscription information."""
    query = """
        SELECT 
            s.subname as subscription_name,
            s.subowner::regrole as owner,
            s.subenabled as enabled,
            s.subconninfo as connection_info,
            s.subslotname as slot_name,
            s.subsynccommit as sync_commit,
            array_to_string(s.subpublications, ', ') as publications
        FROM pg_subscription s
        ORDER BY s.subname
    """
    
    rows = await execute_query(query)
    return rows

# 20 Additional PostgreSQL Tools

@mcp.tool()
async def PostgreSQL_list_extensions() -> List[Dict[str, Any]]:
    """List all available PostgreSQL extensions (installed and available)."""
    query = """
        SELECT 
            name,
            default_version,
            installed_version,
            comment,
            CASE WHEN installed_version IS NOT NULL THEN 'INSTALLED' ELSE 'AVAILABLE' END as status
        FROM pg_available_extensions
        ORDER BY name
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_tablespace_usage() -> List[Dict[str, Any]]:
    """Get tablespace usage statistics and disk space information."""
    query = """
        SELECT 
            spcname as tablespace_name,
            pg_catalog.pg_get_userbyid(spcowner) as owner,
            pg_catalog.pg_tablespace_location(oid) as location,
            (
                SELECT count(*) FROM pg_class 
                WHERE reltablespace = ts.oid
            ) as objects_count,
            (
                SELECT pg_size_pretty(sum(pg_total_relation_size(c.oid)))
                FROM pg_class c WHERE c.reltablespace = ts.oid
            ) as used_space
        FROM pg_tablespace ts
        ORDER BY spcname
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_database_size_by_tablespace() -> List[Dict[str, Any]]:
    """Get database size breakdown by tablespace."""
    query = """
        SELECT 
            ts.spcname as tablespace_name,
            d.datname as database_name,
            pg_size_pretty(
                COALESCE(
                    (SELECT sum(pg_total_relation_size(c.oid))
                     FROM pg_class c
                     JOIN pg_namespace n ON c.relnamespace = n.oid
                     WHERE c.reltablespace = ts.oid
                     AND n.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                    ), 0
                )
            ) as size,
            COUNT(DISTINCT c.oid) as object_count
        FROM pg_tablespace ts
        CROSS JOIN pg_database d
        LEFT JOIN pg_class c ON c.reltablespace = ts.oid
        WHERE d.datistemplate = false
        GROUP BY ts.spcname, d.datname
        HAVING COUNT(DISTINCT c.oid) > 0
        ORDER BY ts.spcname, d.datname
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_list_roles_with_login() -> List[Dict[str, Any]]:
    """List all database roles with their login capabilities and attributes."""
    query = """
        SELECT 
            r.rolname as role_name,
            r.rolcanlogin as can_login,
            r.rolsuper as is_superuser,
            r.rolinherit as inherits_privileges,
            r.rolcreaterole as can_create_roles,
            r.rolcreatedb as can_create_databases,
            r.rolreplication as can_replicate,
            r.rolconnlimit as connection_limit,
            r.rolvaliduntil as password_expires,
            ARRAY(
                SELECT b.rolname 
                FROM pg_catalog.pg_auth_members m 
                JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid) 
                WHERE m.member = r.oid
            ) as member_of
        FROM pg_roles r
        ORDER BY r.rolcanlogin DESC, r.rolname
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_lock_waits() -> List[Dict[str, Any]]:
    """Get detailed information about current lock waits and blocking sessions."""
    query = """
        SELECT 
            blocked_locks.pid AS blocked_pid,
            blocked_activity.usename AS blocked_user,
            blocking_locks.pid AS blocking_pid,
            blocking_activity.usename AS blocking_user,
            blocked_activity.query AS blocked_statement,
            blocking_activity.query AS blocking_statement,
            blocked_locks.locktype,
            blocked_locks.mode AS blocked_mode,
            blocking_locks.mode AS blocking_mode,
            blocked_activity.query_start AS blocked_query_start,
            EXTRACT(epoch FROM (now() - blocked_activity.query_start)) AS wait_duration_seconds
        FROM pg_catalog.pg_locks blocked_locks
        JOIN pg_catalog.pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
        JOIN pg_catalog.pg_locks blocking_locks 
            ON blocking_locks.locktype = blocked_locks.locktype
            AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
            AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
            AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
            AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
            AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
            AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
            AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
            AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
            AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
            AND blocking_locks.pid != blocked_locks.pid
        JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
        WHERE NOT blocked_locks.granted
        ORDER BY wait_duration_seconds DESC
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_prepared_transactions() -> List[Dict[str, Any]]:
    """Get information about prepared transactions (two-phase commit)."""
    query = """
        SELECT 
            gid as transaction_id,
            prepared,
            owner,
            database
        FROM pg_prepared_xacts
        ORDER BY prepared
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_list_foreign_tables_detailed() -> List[Dict[str, Any]]:
    """Get detailed information about foreign tables and their servers."""
    query = """
        SELECT 
            n.nspname as schema_name,
            c.relname as table_name,
            srv.srvname as server_name,
            fdw.fdwname as wrapper_name,
            u.rolname as table_owner,
            ft.ftoptions as foreign_table_options,
            srv.srvoptions as server_options,
            fdw.fdwoptions as wrapper_options
        FROM pg_foreign_table ft
        JOIN pg_class c ON ft.ftrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        JOIN pg_foreign_server srv ON ft.ftserver = srv.oid
        JOIN pg_foreign_data_wrapper fdw ON srv.srvfdw = fdw.oid
        JOIN pg_roles u ON c.relowner = u.oid
        ORDER BY n.nspname, c.relname
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_list_event_triggers_detailed() -> List[Dict[str, Any]]:
    """Get detailed information about event triggers including their definitions."""
    query = """
        SELECT 
            et.evtname as trigger_name,
            et.evtevent as event,
            r.rolname as owner,
            p.proname as function_name,
            n.nspname as function_schema,
            et.evtenabled as is_enabled,
            et.evttags as filter_tags,
            obj_description(et.oid, 'pg_event_trigger') as description
        FROM pg_event_trigger et
        JOIN pg_roles r ON et.evtowner = r.oid
        JOIN pg_proc p ON et.evtfoid = p.oid
        JOIN pg_namespace n ON p.pronamespace = n.oid
        ORDER BY et.evtname
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_publications() -> List[Dict[str, Any]]:
    """Get information about logical replication publications."""
    query = """
        SELECT 
            p.pubname as publication_name,
            r.rolname as owner,
            p.puballtables as publishes_all_tables,
            p.pubinsert as publishes_insert,
            p.pubupdate as publishes_update,
            p.pubdelete as publishes_delete,
            p.pubtruncate as publishes_truncate,
            (
                SELECT count(*) FROM pg_publication_rel pr 
                WHERE pr.prpubid = p.oid
            ) as table_count
        FROM pg_publication p
        JOIN pg_roles r ON p.pubowner = r.oid
        ORDER BY p.pubname
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_text_search_configs() -> List[Dict[str, Any]]:
    """Get full-text search configurations available in the database."""
    query = """
        SELECT 
            n.nspname as schema_name,
            c.cfgname as config_name,
            r.rolname as owner,
            p.prsname as parser_name,
            obj_description(c.oid, 'pg_ts_config') as description
        FROM pg_ts_config c
        JOIN pg_namespace n ON c.cfgnamespace = n.oid
        JOIN pg_roles r ON c.cfgowner = r.oid
        JOIN pg_ts_parser p ON c.cfgparser = p.oid
        ORDER BY n.nspname, c.cfgname
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_autovacuum_activity() -> List[Dict[str, Any]]:
    """Get statistics on autovacuum operations and activity."""
    query = """
        SELECT 
            schemaname,
            tablename,
            last_vacuum,
            last_autovacuum,
            last_analyze,
            last_autoanalyze,
            vacuum_count,
            autovacuum_count,
            analyze_count,
            autoanalyze_count,
            n_dead_tup as dead_tuples,
            n_live_tup as live_tuples,
            CASE 
                WHEN n_live_tup + n_dead_tup > 0 THEN
                    ROUND((n_dead_tup::float / (n_live_tup + n_dead_tup)) * 100, 2)
                ELSE 0
            END as dead_tuple_ratio
        FROM pg_stat_user_tables
        ORDER BY 
            CASE WHEN last_autovacuum IS NULL THEN '1970-01-01'::timestamp ELSE last_autovacuum END ASC,
            dead_tuple_ratio DESC
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_list_foreign_key_references(table_name: str, schema_name: str = "public") -> List[Dict[str, Any]]:
    """List all tables that reference the specified table via foreign keys."""
    query = """
        SELECT DISTINCT
            tc.table_schema as referencing_schema,
            tc.table_name as referencing_table,
            tc.constraint_name,
            kcu.column_name as referencing_column,
            ccu.column_name as referenced_column,
            rc.update_rule,
            rc.delete_rule
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
        JOIN information_schema.referential_constraints AS rc
            ON tc.constraint_name = rc.constraint_name
            AND tc.table_schema = rc.constraint_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
            AND ccu.table_schema = $1
            AND ccu.table_name = $2
        ORDER BY tc.table_schema, tc.table_name, tc.constraint_name
    """
    
    rows = await execute_query(query, schema_name, table_name)
    return rows

@mcp.tool()
async def PostgreSQL_list_table_rules() -> List[Dict[str, Any]]:
    """List all rules defined on tables in the database."""
    query = """
        SELECT 
            n.nspname as schema_name,
            c.relname as table_name,
            r.rulename as rule_name,
            CASE r.ev_type
                WHEN '1' THEN 'SELECT'
                WHEN '2' THEN 'UPDATE'
                WHEN '3' THEN 'INSERT'
                WHEN '4' THEN 'DELETE'
                ELSE 'UNKNOWN'
            END as event_type,
            r.is_instead as is_instead,
            pg_get_ruledef(r.oid) as rule_definition
        FROM pg_rewrite r
        JOIN pg_class c ON r.ev_class = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE r.rulename != '_RETURN'
        AND n.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        ORDER BY n.nspname, c.relname, r.rulename
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_partition_info_detailed() -> List[Dict[str, Any]]:
    """Get detailed information about partitioned tables and their partition strategies."""
    query = """
        SELECT 
            n.nspname as schema_name,
            c.relname as table_name,
            CASE c.relkind
                WHEN 'p' THEN 'PARTITIONED_TABLE'
                WHEN 'r' THEN 'REGULAR_TABLE'
                ELSE 'OTHER'
            END as table_type,
            CASE 
                WHEN c.relkind = 'p' THEN pg_get_partkeydef(c.oid)
                WHEN c.relispartition THEN pg_get_expr(c.relpartbound, c.oid)
                ELSE NULL
            END as partition_info,
            c.relispartition as is_partition,
            (
                SELECT n2.nspname || '.' || c2.relname
                FROM pg_inherits i
                JOIN pg_class c2 ON i.inhparent = c2.oid
                JOIN pg_namespace n2 ON c2.relnamespace = n2.oid
                WHERE i.inhrelid = c.oid
            ) as parent_table,
            (
                SELECT count(*)
                FROM pg_inherits i
                WHERE i.inhparent = c.oid
            ) as child_partitions,
            pg_size_pretty(pg_total_relation_size(c.oid)) as total_size
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE (c.relkind = 'p' OR c.relispartition = true)
        AND n.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        ORDER BY n.nspname, c.relname
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_replication_slot_details() -> List[Dict[str, Any]]:
    """Get detailed replication slot information including lag and usage."""
    query = """
        SELECT 
            slot_name,
            plugin,
            slot_type,
            datoid,
            database,
            temporary,
            active,
            active_pid,
            xmin,
            catalog_xmin,
            restart_lsn,
            confirmed_flush_lsn,
            wal_status,
            safe_wal_size,
            pg_size_pretty(
                COALESCE(
                    pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 
                    0
                )
            ) as replication_lag_size,
            CASE 
                WHEN active THEN 'ACTIVE'
                WHEN temporary THEN 'TEMPORARY'
                ELSE 'INACTIVE'
            END as status
        FROM pg_replication_slots
        ORDER BY slot_name
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_backup_status() -> Dict[str, Any]:
    """Get the last known backup status and WAL archiving information."""
    query = """
        SELECT 
            (SELECT setting FROM pg_settings WHERE name = 'archive_mode') as archive_mode,
            (SELECT setting FROM pg_settings WHERE name = 'archive_command') as archive_command,
            (SELECT setting FROM pg_settings WHERE name = 'archive_timeout') as archive_timeout,
            pg_current_wal_lsn() as current_wal_lsn,
            pg_walfile_name(pg_current_wal_lsn()) as current_wal_file,
            CASE 
                WHEN (SELECT setting FROM pg_settings WHERE name = 'archive_mode') = 'on' 
                THEN 'ENABLED' 
                ELSE 'DISABLED' 
            END as archiving_status,
            pg_is_in_recovery() as is_in_recovery,
            CASE WHEN pg_is_in_recovery() THEN pg_last_wal_receive_lsn() ELSE NULL END as last_received_lsn,
            CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn() ELSE NULL END as last_replayed_lsn
    """
    
    rows = await execute_query(query)
    return rows[0] if rows else {}

@mcp.tool()
async def PostgreSQL_get_slow_query_statements(min_calls: int = 10) -> List[Dict[str, Any]]:
    """Get slow queries from pg_stat_statements with additional performance metrics."""
    check_ext_query = "SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'"
    
    try:
        ext_check = await execute_query(check_ext_query)
        if not ext_check:
            return [{"error": "pg_stat_statements extension is not installed"}]
        
        query = """
            SELECT 
                query,
                calls,
                total_exec_time,
                mean_exec_time,
                max_exec_time,
                min_exec_time,
                stddev_exec_time,
                rows as total_rows_returned,
                shared_blks_hit,
                shared_blks_read,
                shared_blks_dirtied,
                shared_blks_written,
                local_blks_hit,
                local_blks_read,
                local_blks_dirtied,
                local_blks_written,
                temp_blks_read,
                temp_blks_written,
                blk_read_time,
                blk_write_time,
                ROUND(
                    (100.0 * shared_blks_hit / GREATEST(shared_blks_hit + shared_blks_read, 1)), 2
                ) as cache_hit_ratio
            FROM pg_stat_statements
            WHERE calls >= $1
            ORDER BY total_exec_time DESC
            LIMIT 50
        """
        
        rows = await execute_query(query, min_calls)
        return rows
    except Exception as e:
        return [{"error": f"Error retrieving slow queries: {str(e)}"}]

@mcp.tool()
async def PostgreSQL_get_active_transactions() -> List[Dict[str, Any]]:
    """Get information about currently active transactions."""
    query = """
        SELECT 
            sa.pid,
            sa.usename as username,
            sa.datname as database_name,
            sa.client_addr,
            sa.client_port,
            sa.application_name,
            sa.backend_start,
            sa.xact_start as transaction_start,
            sa.query_start,
            sa.state_change,
            sa.state,
            EXTRACT(epoch FROM (now() - sa.xact_start)) as transaction_duration_seconds,
            EXTRACT(epoch FROM (now() - sa.query_start)) as query_duration_seconds,
            sa.wait_event_type,
            sa.wait_event,
            sa.query
        FROM pg_stat_activity sa
        WHERE sa.xact_start IS NOT NULL
        AND sa.state != 'idle'
        ORDER BY sa.xact_start ASC
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_column_privileges(table_name: str, schema_name: str = "public") -> List[Dict[str, Any]]:
    """Get column-level privileges on a specific table."""
    query = """
        SELECT 
            grantee,
            column_name,
            privilege_type,
            is_grantable,
            grantor
        FROM information_schema.column_privileges
        WHERE table_schema = $1 AND table_name = $2
        ORDER BY column_name, grantee, privilege_type
    """
    
    rows = await execute_query(query, schema_name, table_name)
    return rows

@mcp.tool()
async def PostgreSQL_get_wal_archiving_settings() -> Dict[str, Any]:
    """Get comprehensive WAL (Write Ahead Log) archiving configuration and status."""
    query = """
        SELECT 
            (SELECT setting FROM pg_settings WHERE name = 'wal_level') as wal_level,
            (SELECT setting FROM pg_settings WHERE name = 'archive_mode') as archive_mode,
            (SELECT setting FROM pg_settings WHERE name = 'archive_command') as archive_command,
            (SELECT setting FROM pg_settings WHERE name = 'archive_timeout') as archive_timeout,
            (SELECT setting FROM pg_settings WHERE name = 'wal_keep_size') as wal_keep_size,
            (SELECT setting FROM pg_settings WHERE name = 'max_wal_size') as max_wal_size,
            (SELECT setting FROM pg_settings WHERE name = 'min_wal_size') as min_wal_size,
            (SELECT setting FROM pg_settings WHERE name = 'wal_compression') as wal_compression,
            (SELECT setting FROM pg_settings WHERE name = 'wal_buffers') as wal_buffers,
            pg_current_wal_lsn() as current_wal_lsn,
            pg_walfile_name(pg_current_wal_lsn()) as current_wal_filename,
            (
                SELECT count(*) FROM pg_ls_waldir() 
                WHERE name ~ '^[0-9A-F]{24}$'
            ) as wal_files_count
    """
    
    rows = await execute_query(query)
    return rows[0] if rows else {}

@mcp.tool()
async def PostgreSQL_get_index_usage_stats() -> List[Dict[str, Any]]:
    """Get comprehensive index usage statistics to identify unused or underutilized indexes."""
    query = """
        SELECT 
            schemaname,
            tablename,
            indexname,
            idx_tup_read,
            idx_tup_fetch,
            idx_scan,
            idx_tup_read / GREATEST(idx_scan, 1) as tuples_per_scan,
            CASE 
                WHEN idx_scan = 0 THEN 'Never used'
                WHEN idx_scan < 10 THEN 'Rarely used'
                WHEN idx_scan < 100 THEN 'Occasionally used'
                ELSE 'Frequently used'
            END as usage_category,
            pg_size_pretty(pg_relation_size(indexrelid)) as index_size,
            pg_relation_size(indexrelid) as index_size_bytes
        FROM pg_stat_user_indexes
        ORDER BY idx_scan ASC, pg_relation_size(indexrelid) DESC
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_table_bloat_estimation() -> List[Dict[str, Any]]:
    """Estimate table bloat to identify tables that may need maintenance."""
    query = """
        SELECT 
            schemaname,
            tablename,
            n_tup_ins as inserts,
            n_tup_upd as updates,
            n_tup_del as deletes,
            n_live_tup as live_tuples,
            n_dead_tup as dead_tuples,
            ROUND(
                100.0 * n_dead_tup / GREATEST(n_live_tup + n_dead_tup, 1), 2
            ) as dead_tuple_ratio,
            last_vacuum,
            last_autovacuum,
            last_analyze,
            last_autoanalyze,
            vacuum_count,
            autovacuum_count,
            analyze_count,
            autoanalyze_count,
            pg_size_pretty(pg_total_relation_size(relid)) as total_size
        FROM pg_stat_user_tables
        WHERE n_live_tup + n_dead_tup > 0
        ORDER BY dead_tuple_ratio DESC, n_dead_tup DESC
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_lock_monitoring() -> List[Dict[str, Any]]:
    """Monitor current locks and potential blocking situations."""
    query = """
        SELECT 
            pl.locktype,
            pl.database,
            pl.relation::regclass as relation_name,
            pl.page,
            pl.tuple,
            pl.virtualxid,
            pl.transactionid,
            pl.classid,
            pl.objid,
            pl.objsubid,
            pl.virtualtransaction,
            pl.pid,
            pl.mode,
            pl.granted,
            sa.usename,
            sa.query,
            sa.query_start,
            EXTRACT(epoch FROM (now() - sa.query_start)) as query_duration_seconds
        FROM pg_locks pl
        LEFT JOIN pg_stat_activity sa ON pl.pid = sa.pid
        WHERE pl.granted = false
        OR pl.mode IN ('AccessExclusiveLock', 'ExclusiveLock', 'ShareUpdateExclusiveLock')
        ORDER BY pl.granted ASC, query_duration_seconds DESC
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_connection_pool_stats() -> Dict[str, Any]:
    """Get detailed connection and activity statistics."""
    query = """
        SELECT 
            COUNT(*) as total_connections,
            COUNT(*) FILTER (WHERE state = 'active') as active_connections,
            COUNT(*) FILTER (WHERE state = 'idle') as idle_connections,
            COUNT(*) FILTER (WHERE state = 'idle in transaction') as idle_in_transaction,
            COUNT(*) FILTER (WHERE state = 'idle in transaction (aborted)') as idle_in_transaction_aborted,
            COUNT(*) FILTER (WHERE wait_event IS NOT NULL) as waiting_connections,
            MAX(EXTRACT(epoch FROM (now() - backend_start))) as longest_connection_seconds,
            MAX(EXTRACT(epoch FROM (now() - query_start))) as longest_query_seconds,
            MAX(EXTRACT(epoch FROM (now() - xact_start))) as longest_transaction_seconds,
            (SELECT setting::int FROM pg_settings WHERE name = 'max_connections') as max_connections
        FROM pg_stat_activity
        WHERE pid != pg_backend_pid()
    """
    
    rows = await execute_query(query)
    return rows[0] if rows else {}

@mcp.tool()
async def PostgreSQL_get_checkpoint_stats() -> Dict[str, Any]:
    """Get checkpoint activity and WAL statistics for performance monitoring."""
    query = """
        SELECT 
            checkpoints_timed,
            checkpoints_req as checkpoints_requested,
            checkpoint_write_time,
            checkpoint_sync_time,
            buffers_checkpoint,
            buffers_clean,
            maxwritten_clean,
            buffers_backend,
            buffers_backend_fsync,
            buffers_alloc,
            stats_reset
        FROM pg_stat_bgwriter
    """
    
    rows = await execute_query(query)
    return rows[0] if rows else {}

@mcp.tool()
async def PostgreSQL_get_cache_hit_ratios() -> Dict[str, Any]:
    """Calculate cache hit ratios for buffer and index performance analysis."""
    query = """
        SELECT 
            'Buffer Cache Hit Ratio' as metric,
            ROUND(
                100.0 * 
                (SELECT sum(blks_hit) FROM pg_stat_database) / 
                GREATEST(
                    (SELECT sum(blks_hit) FROM pg_stat_database) + 
                    (SELECT sum(blks_read) FROM pg_stat_database), 
                    1
                ), 2
            ) as percentage
        
        UNION ALL
        
        SELECT 
            'Index Hit Ratio' as metric,
            ROUND(
                100.0 * 
                (SELECT sum(idx_blks_hit) FROM pg_statio_user_indexes) / 
                GREATEST(
                    (SELECT sum(idx_blks_hit) FROM pg_statio_user_indexes) + 
                    (SELECT sum(idx_blks_read) FROM pg_statio_user_indexes), 
                    1
                ), 2
            ) as percentage
        
        UNION ALL
        
        SELECT 
            'Table Hit Ratio' as metric,
            ROUND(
                100.0 * 
                (SELECT sum(heap_blks_hit) FROM pg_statio_user_tables) / 
                GREATEST(
                    (SELECT sum(heap_blks_hit) FROM pg_statio_user_tables) + 
                    (SELECT sum(heap_blks_read) FROM pg_statio_user_tables), 
                    1
                ), 2
            ) as percentage
    """
    
    rows = await execute_query(query)
    return {row['metric']: row['percentage'] for row in rows}

@mcp.tool()
async def PostgreSQL_get_vacuum_analyze_recommendations() -> List[Dict[str, Any]]:
    """Get recommendations for tables that may need vacuum or analyze operations."""
    query = """
        SELECT 
            schemaname,
            tablename,
            n_live_tup as live_tuples,
            n_dead_tup as dead_tuples,
            ROUND(100.0 * n_dead_tup / GREATEST(n_live_tup + n_dead_tup, 1), 2) as dead_ratio,
            last_vacuum,
            last_autovacuum,
            last_analyze,
            last_autoanalyze,
            CASE 
                WHEN last_vacuum IS NULL AND last_autovacuum IS NULL THEN 'Never vacuumed'
                WHEN GREATEST(last_vacuum, last_autovacuum) < now() - interval '7 days' THEN 'Vacuum overdue'
                WHEN n_dead_tup > n_live_tup * 0.2 THEN 'High dead tuple ratio'
                ELSE 'OK'
            END as vacuum_recommendation,
            CASE 
                WHEN last_analyze IS NULL AND last_autoanalyze IS NULL THEN 'Never analyzed'
                WHEN GREATEST(last_analyze, last_autoanalyze) < now() - interval '7 days' THEN 'Analyze overdue'
                ELSE 'OK'
            END as analyze_recommendation,
            pg_size_pretty(pg_total_relation_size(relid)) as table_size
        FROM pg_stat_user_tables
        WHERE n_live_tup + n_dead_tup > 100  -- Only show tables with significant data
        ORDER BY dead_ratio DESC, n_dead_tup DESC
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_blocking_locks() -> List[Dict[str, Any]]:
    """Identify blocking and blocked queries with detailed lock information."""
    query = """
        SELECT 
            blocked_locks.pid AS blocked_pid,
            blocked_activity.usename AS blocked_user,
            blocking_locks.pid AS blocking_pid,
            blocking_activity.usename AS blocking_user,
            blocked_activity.query AS blocked_statement,
            blocking_activity.query AS blocking_statement,
            blocked_locks.mode AS blocked_mode,
            blocking_locks.mode AS blocking_mode,
            blocked_locks.locktype,
            blocked_locks.relation::regclass AS relation_name,
            EXTRACT(epoch FROM (now() - blocked_activity.query_start)) AS blocked_duration_seconds,
            EXTRACT(epoch FROM (now() - blocking_activity.query_start)) AS blocking_duration_seconds
        FROM pg_catalog.pg_locks blocked_locks
        JOIN pg_catalog.pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
        JOIN pg_catalog.pg_locks blocking_locks 
            ON blocking_locks.locktype = blocked_locks.locktype
            AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
            AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
            AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
            AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
            AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
            AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
            AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
            AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
            AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
            AND blocking_locks.pid != blocked_locks.pid
        JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
        WHERE NOT blocked_locks.granted
        AND blocking_locks.granted
        ORDER BY blocked_duration_seconds DESC
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_top_heavy_queries() -> List[Dict[str, Any]]:
    """Get top queries by total time, calls, and mean time from pg_stat_statements."""
    query = """
        SELECT 
            query,
            calls,
            ROUND(total_exec_time::numeric, 2) as total_time_ms,
            ROUND(mean_exec_time::numeric, 2) as mean_time_ms,
            ROUND(100.0 * total_exec_time / sum(total_exec_time) OVER(), 2) AS percentage_of_total,
            rows as total_rows,
            ROUND(rows::numeric / calls, 2) as mean_rows_per_call,
            shared_blks_hit + shared_blks_read as total_blocks,
            ROUND(100.0 * shared_blks_hit / GREATEST(shared_blks_hit + shared_blks_read, 1), 2) as hit_percent
        FROM pg_stat_statements
        WHERE calls > 5  -- Only queries called more than 5 times
        ORDER BY total_exec_time DESC
        LIMIT 20
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_table_size_summary() -> List[Dict[str, Any]]:
    """Get comprehensive table size information including indexes and toast."""
    query = """
        SELECT 
            schemaname,
            tablename,
            pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size,
            pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as table_size,
            pg_size_pretty(pg_indexes_size(schemaname||'.'||tablename)) as indexes_size,
            pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename) - pg_relation_size(schemaname||'.'||tablename)) as external_size,
            pg_total_relation_size(schemaname||'.'||tablename) as total_bytes,
            ROUND(100.0 * pg_relation_size(schemaname||'.'||tablename) / GREATEST(pg_total_relation_size(schemaname||'.'||tablename), 1), 1) as table_percentage,
            ROUND(100.0 * pg_indexes_size(schemaname||'.'||tablename) / GREATEST(pg_total_relation_size(schemaname||'.'||tablename), 1), 1) as index_percentage
        FROM pg_tables
        WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
        ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_index_usage_stats() -> List[Dict[str, Any]]:
    """Get detailed index usage statistics and effectiveness metrics."""
    query = """
        SELECT 
            t.schemaname,
            t.tablename,
            i.indexrelname as index_name,
            i.idx_tup_read as index_reads,
            i.idx_tup_fetch as index_fetches,
            pg_size_pretty(pg_relation_size(i.indexrelid)) as index_size,
            pg_relation_size(i.indexrelid) as index_bytes,
            CASE 
                WHEN i.idx_tup_read = 0 THEN 'UNUSED'
                WHEN i.idx_tup_read < 100 THEN 'LOW USAGE'
                WHEN i.idx_tup_read < 1000 THEN 'MODERATE USAGE'
                ELSE 'HIGH USAGE'
            END as usage_category,
            ROUND(100.0 * i.idx_tup_fetch / GREATEST(i.idx_tup_read, 1), 2) as selectivity_ratio
        FROM pg_stat_user_indexes i
        JOIN pg_stat_user_tables t ON i.relid = t.relid
        WHERE i.schemaname NOT IN ('information_schema', 'pg_catalog')
        ORDER BY index_bytes DESC, i.idx_tup_read DESC
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_prepared_transactions() -> List[Dict[str, Any]]:
    """Get information about currently prepared transactions (two-phase commit)."""
    query = """
        SELECT 
            gid as transaction_id,
            prepared as prepared_time,
            owner,
            database,
            EXTRACT(epoch FROM (now() - prepared)) as seconds_since_prepared
        FROM pg_prepared_xacts
        ORDER BY prepared ASC
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_replication_stats() -> List[Dict[str, Any]]:
    """Get replication slot and standby server statistics."""
    query = """
        SELECT 
            slot_name,
            plugin,
            slot_type,
            datoid,
            database,
            temporary,
            active,
            active_pid,
            xmin,
            catalog_xmin,
            restart_lsn,
            confirmed_flush_lsn,
            wal_status,
            safe_wal_size,
            two_phase
        FROM pg_replication_slots
        
        UNION ALL
        
        SELECT 
            'standby_' || client_addr::text as slot_name,
            'physical' as plugin,
            'physical' as slot_type,
            NULL as datoid,
            NULL as database,
            FALSE as temporary,
            state = 'streaming' as active,
            pid as active_pid,
            NULL as xmin,
            NULL as catalog_xmin,
            sent_lsn as restart_lsn,
            flush_lsn as confirmed_flush_lsn,
            NULL as wal_status,
            NULL as safe_wal_size,
            NULL as two_phase
        FROM pg_stat_replication
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_estimated_row_counts() -> List[Dict[str, Any]]:
    """Get estimated row counts for all tables using statistics (faster than COUNT(*))."""
    query = """
        SELECT 
            schemaname,
            tablename,
            n_live_tup as estimated_rows,
            n_dead_tup as dead_rows,
            ROUND(100.0 * n_dead_tup / GREATEST(n_live_tup + n_dead_tup, 1), 2) as dead_percentage,
            last_vacuum,
            last_autovacuum,
            last_analyze,
            last_autoanalyze,
            seq_scan as sequential_scans,
            seq_tup_read as sequential_reads,
            idx_scan as index_scans,
            idx_tup_fetch as index_reads
        FROM pg_stat_user_tables
        ORDER BY n_live_tup DESC
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_long_running_transactions() -> List[Dict[str, Any]]:
    """Identify long-running transactions that may be holding locks or bloating tables."""
    query = """
        SELECT 
            pid,
            usename,
            application_name,
            client_addr,
            backend_start,
            xact_start as transaction_start,
            query_start,
            state_change,
            state,
            query,
            backend_xid,
            backend_xmin,
            EXTRACT(epoch FROM (now() - xact_start)) as transaction_duration_seconds,
            EXTRACT(epoch FROM (now() - query_start)) as query_duration_seconds,
            wait_event_type,
            wait_event
        FROM pg_stat_activity
        WHERE xact_start IS NOT NULL
        AND state != 'idle'
        AND EXTRACT(epoch FROM (now() - xact_start)) > 300  -- Transactions running > 5 minutes
        ORDER BY transaction_duration_seconds DESC
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_idle_connections() -> List[Dict[str, Any]]:
    """Find idle connections and idle-in-transaction sessions."""
    query = """
        SELECT 
            pid,
            usename,
            application_name,
            client_addr,
            client_port,
            backend_start,
            state_change,
            state,
            EXTRACT(epoch FROM (now() - state_change)) as idle_duration_seconds,
            EXTRACT(epoch FROM (now() - backend_start)) as connection_age_seconds,
            CASE 
                WHEN state = 'idle' THEN 'Safe to terminate'
                WHEN state = 'idle in transaction' THEN 'CAUTION: May be holding locks'
                WHEN state = 'idle in transaction (aborted)' THEN 'Should be terminated'
                ELSE 'Active'
            END as recommendation
        FROM pg_stat_activity
        WHERE state IN ('idle', 'idle in transaction', 'idle in transaction (aborted)')
        AND pid != pg_backend_pid()
        ORDER BY idle_duration_seconds DESC
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_important_settings() -> List[Dict[str, Any]]:
    """Get important PostgreSQL configuration parameters and their current values."""
    query = """
        SELECT 
            name,
            setting,
            unit,
            category,
            short_desc as description,
            context,
            vartype,
            source,
            min_val,
            max_val,
            boot_val,
            reset_val,
            pending_restart
        FROM pg_settings
        WHERE name IN (
            'shared_buffers', 'effective_cache_size', 'work_mem', 'maintenance_work_mem',
            'wal_buffers', 'checkpoint_timeout', 'checkpoint_completion_target',
            'max_connections', 'max_worker_processes', 'max_parallel_workers',
            'random_page_cost', 'seq_page_cost', 'cpu_tuple_cost', 'cpu_index_tuple_cost',
            'autovacuum', 'log_min_duration_statement', 'log_checkpoints',
            'log_connections', 'log_disconnections', 'log_lock_waits',
            'deadlock_timeout', 'lock_timeout', 'statement_timeout',
            'max_wal_size', 'min_wal_size', 'archive_mode', 'archive_command',
            'hot_standby', 'wal_level', 'synchronous_commit'
        )
        ORDER BY category, name
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_table_io_stats() -> List[Dict[str, Any]]:
    """Get I/O statistics for tables showing disk vs cache usage patterns."""
    query = """
        SELECT 
            st.schemaname,
            st.relname as tablename,
            st.heap_blks_read as table_disk_reads,
            st.heap_blks_hit as table_cache_hits,
            ROUND(100.0 * st.heap_blks_hit / GREATEST(st.heap_blks_hit + st.heap_blks_read, 1), 2) as table_hit_ratio,
            st.idx_blks_read as index_disk_reads,
            st.idx_blks_hit as index_cache_hits,
            ROUND(100.0 * st.idx_blks_hit / GREATEST(st.idx_blks_hit + st.idx_blks_read, 1), 2) as index_hit_ratio,
            st.toast_blks_read as toast_disk_reads,
            st.toast_blks_hit as toast_cache_hits,
            st.tidx_blks_read as toast_index_disk_reads,
            st.tidx_blks_hit as toast_index_cache_hits,
            pg_size_pretty(pg_total_relation_size(st.relid)) as total_size
        FROM pg_statio_user_tables st
        WHERE st.heap_blks_read + st.heap_blks_hit > 0
        ORDER BY (st.heap_blks_read + st.idx_blks_read) DESC
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_tablespace_usage() -> List[Dict[str, Any]]:
    """Get tablespace usage information and disk space statistics."""
    query = """
        SELECT 
            ts.spcname as tablespace_name,
            pg_catalog.pg_tablespace_location(ts.oid) as location,
            pg_size_pretty(pg_tablespace_size(ts.oid)) as size,
            pg_tablespace_size(ts.oid) as size_bytes,
            COALESCE(owner.rolname, 'Unknown') as owner,
            array_to_string(ts.spcacl, ', ') as permissions,
            COUNT(c.oid) as objects_count
        FROM pg_tablespace ts
        LEFT JOIN pg_authid owner ON ts.spcowner = owner.oid
        LEFT JOIN pg_class c ON c.reltablespace = ts.oid
        GROUP BY ts.oid, ts.spcname, owner.rolname, ts.spcacl
        ORDER BY pg_tablespace_size(ts.oid) DESC
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_role_attributes() -> List[Dict[str, Any]]:
    """Get detailed information about database roles and their attributes."""
    query = """
        SELECT 
            r.rolname as role_name,
            r.rolsuper as is_superuser,
            r.rolinherit as can_inherit,
            r.rolcreaterole as can_create_roles,
            r.rolcreatedb as can_create_databases,
            r.rolcanlogin as can_login,
            r.rolreplication as replication,
            r.rolbypassrls as bypass_rls,
            r.rolconnlimit as connection_limit,
            r.rolvaliduntil as password_expiry,
            array_to_string(r.rolconfig, ', ') as role_settings,
            string_agg(m.rolname, ', ') as member_of_roles
        FROM pg_roles r
        LEFT JOIN pg_auth_members am ON r.oid = am.member
        LEFT JOIN pg_roles m ON am.roleid = m.oid
        GROUP BY r.oid, r.rolname, r.rolsuper, r.rolinherit, r.rolcreaterole,
                 r.rolcreatedb, r.rolcanlogin, r.rolreplication, r.rolbypassrls,
                 r.rolconnlimit, r.rolvaliduntil, r.rolconfig
        ORDER BY r.rolname
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_temp_file_stats() -> Dict[str, Any]:
    """Get temporary file usage statistics indicating potential memory pressure."""
    query = """
        SELECT 
            SUM(temp_files) as total_temp_files,
            pg_size_pretty(SUM(temp_bytes)) as total_temp_bytes,
            SUM(temp_bytes) as total_temp_bytes_raw,
            AVG(temp_files::float) as avg_temp_files_per_db,
            pg_size_pretty(AVG(temp_bytes::float)::bigint) as avg_temp_bytes_per_db,
            COUNT(*) as databases_with_temp_files
        FROM pg_stat_database
        WHERE temp_files > 0
    """
    
    rows = await execute_query(query)
    return rows[0] if rows else {}

@mcp.tool()
async def PostgreSQL_get_logical_replication_stats() -> List[Dict[str, Any]]:
    """Get logical replication subscription and worker statistics."""
    query = """
        SELECT 
            s.subname as subscription_name,
            s.subowner::regrole as owner,
            s.subenabled as enabled,
            s.subconninfo as connection_info,
            s.subslotname as slot_name,
            s.subsynccommit as sync_commit,
            array_to_string(s.subpublications, ', ') as publications,
            w.pid as worker_pid,
            w.received_lsn,
            w.last_msg_send_time,
            w.last_msg_receipt_time,
            w.latest_end_lsn,
            w.latest_end_time
        FROM pg_subscription s
        LEFT JOIN pg_stat_subscription w ON s.oid = w.subid
        ORDER BY s.subname
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_memory_usage_stats() -> Dict[str, Any]:
    """Get memory-related statistics and buffer pool information."""
    query = """
        WITH memory_stats AS (
            SELECT 
                name,
                setting,
                unit
            FROM pg_settings 
            WHERE name IN ('shared_buffers', 'work_mem', 'maintenance_work_mem', 'effective_cache_size', 'wal_buffers')
        ),
        buffer_stats AS (
            SELECT 
                buffers_checkpoint,
                buffers_clean,
                buffers_backend,
                buffers_backend_fsync,
                buffers_alloc,
                maxwritten_clean
            FROM pg_stat_bgwriter
        )
        SELECT 
            json_object_agg(name, 
                CASE 
                    WHEN unit = '8kB' THEN (setting::bigint * 8192)
                    WHEN unit = 'kB' THEN (setting::bigint * 1024)
                    WHEN unit = 'MB' THEN (setting::bigint * 1024 * 1024)
                    ELSE setting::bigint
                END
            ) as memory_settings_bytes,
            (SELECT row_to_json(buffer_stats) FROM buffer_stats) as buffer_statistics
        FROM memory_stats
    """
    
    rows = await execute_query(query)
    return rows[0] if rows else {}

@mcp.tool()
async def PostgreSQL_get_buffer_cache_contents() -> List[Dict[str, Any]]:
    """Analyze what's currently in the PostgreSQL buffer cache (requires pg_buffercache extension)."""
    query = """
        SELECT 
            c.relname as relation_name,
            n.nspname as schema_name,
            COUNT(*) as buffer_count,
            pg_size_pretty(COUNT(*) * current_setting('block_size')::bigint) as cache_size,
            ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM pg_buffercache), 2) as cache_percentage,
            COUNT(*) FILTER (WHERE isdirty) as dirty_buffers,
            ROUND(100.0 * COUNT(*) FILTER (WHERE isdirty) / COUNT(*), 2) as dirty_percentage
        FROM pg_buffercache b
        LEFT JOIN pg_class c ON b.relfilenode = pg_relation_filenode(c.oid)
        LEFT JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE b.relfilenode IS NOT NULL
        GROUP BY c.relname, n.nspname
        ORDER BY buffer_count DESC
        LIMIT 20
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_buffer_hit_ratios_detailed() -> List[Dict[str, Any]]:
    """Get detailed buffer cache hit ratios by database and table."""
    query = """
        SELECT 
            d.datname as database_name,
            d.blks_read as disk_reads,
            d.blks_hit as cache_hits,
            d.blks_read + d.blks_hit as total_reads,
            ROUND(100.0 * d.blks_hit / GREATEST(d.blks_read + d.blks_hit, 1), 2) as hit_ratio,
            d.tup_returned as tuples_returned,
            d.tup_fetched as tuples_fetched,
            d.tup_inserted as tuples_inserted,
            d.tup_updated as tuples_updated,
            d.tup_deleted as tuples_deleted,
            d.conflicts as conflicts,
            d.temp_files as temp_files_created,
            pg_size_pretty(d.temp_bytes) as temp_bytes_written,
            d.deadlocks,
            d.checksum_failures,
            d.checksum_last_failure
        FROM pg_stat_database d
        WHERE d.datname IS NOT NULL
        ORDER BY hit_ratio ASC, total_reads DESC
    """
    
    rows = await execute_query(query)
    return rows

# Advanced PostgreSQL Diagnostic and Monitoring Tools

@mcp.tool()
async def analyze_query_plans() -> list[dict]:
    """Analyze active query execution plans and their performance characteristics."""
    query = """
        SELECT 
            p.pid,
            p.usename as username,
            p.application_name,
            p.client_addr,
            p.query_start,
            p.state,
            length(p.query) as query_length,
            left(p.query, 100) as query_snippet,
            CASE 
                WHEN p.query LIKE '%EXPLAIN%' THEN 'EXPLAIN query'
                WHEN p.query LIKE '%SELECT%' THEN 'SELECT'
                WHEN p.query LIKE '%INSERT%' THEN 'INSERT'
                WHEN p.query LIKE '%UPDATE%' THEN 'UPDATE'
                WHEN p.query LIKE '%DELETE%' THEN 'DELETE'
                ELSE 'OTHER'
            END as query_type,
            extract(epoch from (now() - p.query_start))::int as runtime_seconds
        FROM pg_stat_activity p
        WHERE p.state = 'active'
            AND p.query != '<IDLE>'
            AND p.query NOT LIKE '%pg_stat_activity%'
        ORDER BY p.query_start ASC
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def check_constraint_violations() -> list[dict]:
    """Check for potential constraint violations and data integrity issues."""
    query = """
        SELECT 
            schemaname,
            tablename,
            constraintname,
            constrainttype,
            constraintdef,
            CASE constrainttype
                WHEN 'c' THEN 'Check constraint'
                WHEN 'f' THEN 'Foreign key'
                WHEN 'p' THEN 'Primary key'
                WHEN 'u' THEN 'Unique'
                WHEN 'x' THEN 'Exclusion'
                ELSE 'Other'
            END as constraint_description
        FROM (
            SELECT 
                n.nspname as schemaname,
                t.relname as tablename,
                c.conname as constraintname,
                c.contype as constrainttype,
                pg_get_constraintdef(c.oid) as constraintdef
            FROM pg_constraint c
            JOIN pg_class t ON c.conrelid = t.oid
            JOIN pg_namespace n ON t.relnamespace = n.oid
            WHERE n.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        ) constraints
        ORDER BY schemaname, tablename, constraintname
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def analyze_sequence_usage() -> list[dict]:
    """Analyze sequence usage and predict when sequences might be exhausted."""
    query = """
        SELECT 
            schemaname,
            sequencename,
            last_value,
            start_value,
            increment_by,
            max_value,
            min_value,
            cache_value,
            is_cycled,
            CASE 
                WHEN max_value > 0 THEN 
                    round((last_value::numeric / max_value::numeric) * 100, 2)
                ELSE 0
            END as percent_used,
            CASE 
                WHEN max_value > 0 AND increment_by > 0 THEN 
                    (max_value - last_value) / increment_by
                ELSE NULL
            END as values_remaining
        FROM pg_sequences
        WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
        ORDER BY percent_used DESC, values_remaining ASC NULLS LAST
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def monitor_materialized_views() -> list[dict]:
    """Monitor materialized views status and freshness."""
    query = """
        SELECT 
            schemaname,
            matviewname,
            matviewowner,
            tablespace,
            hasindexes,
            ispopulated,
            definition,
            pg_size_pretty(pg_total_relation_size(schemaname||'.'||matviewname)) as size
        FROM pg_matviews
        WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
        ORDER BY schemaname, matviewname
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def analyze_vacuum_efficiency() -> list[dict]:
    """Analyze vacuum efficiency and recommend vacuum strategies."""
    query = """
        SELECT 
            schemaname,
            tablename,
            n_tup_ins as inserts,
            n_tup_upd as updates,
            n_tup_del as deletes,
            n_live_tup as live_tuples,
            n_dead_tup as dead_tuples,
            CASE 
                WHEN n_live_tup > 0 THEN 
                    round((n_dead_tup::numeric / (n_live_tup + n_dead_tup)::numeric) * 100, 2)
                ELSE 0
            END as dead_tuple_percent,
            last_vacuum,
            last_autovacuum,
            vacuum_count,
            autovacuum_count,
            CASE 
                WHEN last_autovacuum > last_vacuum OR last_vacuum IS NULL THEN last_autovacuum
                ELSE last_vacuum
            END as last_vacuum_any,
            pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as table_size
        FROM pg_stat_user_tables
        WHERE n_dead_tup > 0
        ORDER BY dead_tuple_percent DESC, n_dead_tup DESC
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def check_function_performance() -> list[dict]:
    """Analyze stored function and procedure performance statistics."""
    query = """
        SELECT 
            schemaname,
            funcname,
            calls,
            total_time,
            mean_time,
            self_time,
            CASE 
                WHEN calls > 0 THEN round(total_time / calls, 4)
                ELSE 0
            END as avg_time_per_call,
            CASE 
                WHEN total_time > 0 THEN round((self_time / total_time) * 100, 2)
                ELSE 0
            END as self_time_percent
        FROM pg_stat_user_functions
        WHERE calls > 0
        ORDER BY total_time DESC, calls DESC
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def analyze_transaction_wraparound() -> list[dict]:
    """Monitor transaction ID wraparound risks across databases."""
    query = """
        SELECT 
            datname as database_name,
            age(datfrozenxid) as xid_age,
            2147483648 - age(datfrozenxid) as xids_until_wraparound,
            round((age(datfrozenxid)::numeric / 2147483648::numeric) * 100, 2) as wraparound_percent,
            CASE 
                WHEN age(datfrozenxid) > 1500000000 THEN 'CRITICAL - Immediate vacuum needed'
                WHEN age(datfrozenxid) > 1000000000 THEN 'WARNING - Schedule vacuum soon'
                WHEN age(datfrozenxid) > 500000000 THEN 'CAUTION - Monitor closely'
                ELSE 'OK'
            END as risk_level
        FROM pg_database
        WHERE datname IS NOT NULL
        ORDER BY age(datfrozenxid) DESC
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def monitor_connection_patterns() -> list[dict]:
    """Analyze connection patterns and identify potential connection issues."""
    query = """
        SELECT 
            usename as username,
            application_name,
            client_addr,
            COUNT(*) as connection_count,
            COUNT(CASE WHEN state = 'active' THEN 1 END) as active_connections,
            COUNT(CASE WHEN state = 'idle' THEN 1 END) as idle_connections,
            COUNT(CASE WHEN state = 'idle in transaction' THEN 1 END) as idle_in_transaction,
            MIN(backend_start) as oldest_connection,
            MAX(backend_start) as newest_connection,
            CASE 
                WHEN COUNT(CASE WHEN state = 'idle in transaction' THEN 1 END) > 5 THEN 'HIGH idle-in-transaction'
                WHEN COUNT(*) > 50 THEN 'HIGH connection count'
                ELSE 'OK'
            END as status
        FROM pg_stat_activity
        WHERE state IS NOT NULL
        GROUP BY usename, application_name, client_addr
        ORDER BY connection_count DESC, idle_in_transaction DESC
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def analyze_index_effectiveness() -> list[dict]:
    """Analyze index effectiveness and identify unused or redundant indexes."""
    query = """
        SELECT 
            schemaname,
            tablename,
            indexname,
            idx_tup_read,
            idx_tup_fetch,
            CASE 
                WHEN idx_tup_read > 0 THEN 
                    round((idx_tup_fetch::numeric / idx_tup_read::numeric) * 100, 2)
                ELSE 0
            END as fetch_ratio,
            pg_size_pretty(pg_relation_size(schemaname||'.'||indexname)) as index_size,
            CASE 
                WHEN idx_scan = 0 THEN 'UNUSED - Consider dropping'
                WHEN idx_scan < 10 THEN 'RARELY USED - Review necessity'
                WHEN fetch_ratio < 1 THEN 'LOW EFFECTIVENESS - Review queries'
                ELSE 'GOOD'
            END as recommendation
        FROM pg_stat_user_indexes
        ORDER BY idx_scan ASC, fetch_ratio ASC
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def check_replication_lag_details() -> list[dict]:
    """Detailed analysis of replication lag across all replicas."""
    query = """
        SELECT 
            client_addr as replica_address,
            application_name,
            state,
            sent_lsn,
            write_lsn,
            flush_lsn,
            replay_lsn,
            write_lag,
            flush_lag,
            replay_lag,
            sync_priority,
            sync_state,
            CASE 
                WHEN state != 'streaming' THEN 'CONNECTION ISSUE'
                WHEN extract(epoch from replay_lag) > 60 THEN 'HIGH LAG - Investigate'
                WHEN extract(epoch from replay_lag) > 10 THEN 'MODERATE LAG - Monitor'
                ELSE 'OK'
            END as lag_status
        FROM pg_stat_replication
        ORDER BY sync_priority, extract(epoch from COALESCE(replay_lag, '0'::interval)) DESC
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def analyze_buffer_utilization() -> list[dict]:
    """Analyze shared buffer utilization patterns by relation."""
    query = """
        SELECT 
            c.relname as relation_name,
            n.nspname as schema_name,
            count(*) as buffer_count,
            pg_size_pretty(count(*) * 8192) as buffer_size,
            round(count(*) * 100.0 / 
                (SELECT setting::int FROM pg_settings WHERE name = 'shared_buffers'), 2
            ) as percent_of_shared_buffers,
            count(CASE WHEN isdirty THEN 1 END) as dirty_buffers,
            round(count(CASE WHEN isdirty THEN 1 END) * 100.0 / count(*), 2) as dirty_percent,
            count(CASE WHEN usagecount > 3 THEN 1 END) as hot_buffers,
            round(count(CASE WHEN usagecount > 3 THEN 1 END) * 100.0 / count(*), 2) as hot_percent
        FROM pg_buffercache b
        JOIN pg_class c ON b.relfilenode = pg_relation_filenode(c.oid)
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE c.relname IS NOT NULL
        GROUP BY c.relname, n.nspname
        HAVING count(*) > 10
        ORDER BY buffer_count DESC
        LIMIT 20
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def monitor_autovacuum_progress() -> list[dict]:
    """Monitor currently running autovacuum operations and their progress."""
    query = """
        SELECT 
            p.pid,
            p.usename,
            p.application_name,
            p.client_addr,
            p.backend_start,
            p.query_start,
            extract(epoch from (now() - p.query_start))::int as runtime_seconds,
            p.state,
            CASE 
                WHEN p.query LIKE '%autovacuum:%' THEN 
                    regexp_replace(p.query, '.*autovacuum: ([^ ]+).*', '\\1')
                ELSE 'unknown'
            END as operation_type,
            CASE 
                WHEN p.query LIKE '%autovacuum:%' THEN 
                    regexp_replace(p.query, '.*autovacuum: [^ ]+ ([^ .]+)\\.([^ ]+).*', '\\1.\\2')
                ELSE 'unknown'
            END as target_table,
            left(p.query, 120) as query_snippet
        FROM pg_stat_activity p
        WHERE p.query LIKE '%autovacuum:%'
            AND p.state != 'idle'
        ORDER BY p.query_start ASC
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def check_table_inheritance() -> list[dict]:
    """Analyze table inheritance hierarchies and partitioning structures."""
    query = """
        SELECT 
            parent_schema.nspname as parent_schema,
            parent_table.relname as parent_table,
            child_schema.nspname as child_schema,
            child_table.relname as child_table,
            child_table.relkind as child_type,
            CASE child_table.relkind
                WHEN 'r' THEN 'regular table'
                WHEN 'p' THEN 'partitioned table'
                WHEN 'f' THEN 'foreign table'
                WHEN 'v' THEN 'view'
                WHEN 'm' THEN 'materialized view'
                ELSE 'other'
            END as child_type_desc,
            pg_get_expr(child_table.relpartbound, child_table.oid) as partition_bound,
            pg_size_pretty(pg_total_relation_size(child_schema.nspname||'.'||child_table.relname)) as child_size
        FROM pg_inherits i
        JOIN pg_class parent_table ON i.inhparent = parent_table.oid
        JOIN pg_namespace parent_schema ON parent_table.relnamespace = parent_schema.oid
        JOIN pg_class child_table ON i.inhrelid = child_table.oid
        JOIN pg_namespace child_schema ON child_table.relnamespace = child_schema.oid
        WHERE parent_schema.nspname NOT IN ('information_schema', 'pg_catalog')
        ORDER BY parent_schema.nspname, parent_table.relname, child_schema.nspname, child_table.relname
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def analyze_query_complexity() -> list[dict]:
    """Analyze query complexity patterns from pg_stat_statements."""
    query = """
        SELECT 
            LEFT(query, 100) as query_snippet,
            calls,
            total_exec_time,
            mean_exec_time,
            rows as total_rows,
            CASE 
                WHEN calls > 0 THEN round(rows::numeric / calls::numeric, 2)
                ELSE 0
            END as avg_rows_per_call,
            shared_blks_hit,
            shared_blks_read,
            CASE 
                WHEN (shared_blks_hit + shared_blks_read) > 0 THEN 
                    round((shared_blks_hit::numeric / (shared_blks_hit + shared_blks_read)::numeric) * 100, 2)
                ELSE 0
            END as cache_hit_ratio,
            temp_blks_read + temp_blks_written as temp_blocks,
            CASE 
                WHEN query LIKE '%JOIN%' AND query LIKE '%WHERE%' THEN 'Complex Join+Filter'
                WHEN query LIKE '%JOIN%' THEN 'Join Query'
                WHEN query LIKE '%GROUP BY%' OR query LIKE '%ORDER BY%' THEN 'Aggregation/Sort'
                WHEN query LIKE '%UNION%' OR query LIKE '%INTERSECT%' THEN 'Set Operation'
                WHEN query LIKE '%SUBQUERY%' OR query LIKE '%EXISTS%' THEN 'Subquery'
                ELSE 'Simple Query'
            END as complexity_category
        FROM pg_stat_statements
        WHERE calls > 10
        ORDER BY mean_exec_time DESC, total_exec_time DESC
        LIMIT 50
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def monitor_wal_generation_rate() -> list[dict]:
    """Monitor WAL generation rate and predict disk space requirements."""
    query = """
        SELECT 
            pg_current_wal_lsn() as current_wal_lsn,
            pg_walfile_name(pg_current_wal_lsn()) as current_wal_file,
            CASE 
                WHEN pg_is_in_recovery() THEN 'STANDBY'
                ELSE 'PRIMARY'
            END as server_role,
            (SELECT setting::bigint FROM pg_settings WHERE name = 'max_wal_size') / (1024*1024) as max_wal_size_mb,
            (SELECT setting::bigint FROM pg_settings WHERE name = 'min_wal_size') / (1024*1024) as min_wal_size_mb,
            (SELECT setting FROM pg_settings WHERE name = 'wal_level') as wal_level,
            (SELECT setting::int FROM pg_settings WHERE name = 'max_wal_senders') as max_wal_senders,
            COUNT(*) as active_wal_senders
        FROM pg_stat_replication
        WHERE state = 'streaming'
        GROUP BY ()
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def check_database_encoding_collation() -> list[dict]:
    """Check database encoding, collation settings and potential issues."""
    query = """
        SELECT 
            d.datname as database_name,
            d.datdba as owner_oid,
            pg_catalog.pg_get_userbyid(d.datdba) as owner_name,
            pg_encoding_to_char(d.encoding) as encoding,
            d.datcollate as collation,
            d.datctype as character_type,
            d.datistemplate as is_template,
            d.datallowconn as allow_connections,
            d.datconnlimit as connection_limit,
            d.dattablespace as tablespace_oid,
            t.spcname as tablespace_name,
            CASE 
                WHEN d.datcollate != d.datctype THEN 'MISMATCH - Review collation settings'
                WHEN d.datcollate LIKE 'C%' THEN 'C locale - Good for performance'
                WHEN d.datcollate LIKE 'en_US%' THEN 'English locale'
                ELSE 'Custom locale'
            END as locale_analysis
        FROM pg_database d
        LEFT JOIN pg_tablespace t ON d.dattablespace = t.oid
        WHERE d.datname IS NOT NULL
        ORDER BY d.datname
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def analyze_trigger_performance() -> list[dict]:
    """Analyze trigger definitions and potential performance impacts."""
    query = """
        SELECT 
            n.nspname as schema_name,
            c.relname as table_name,
            t.tgname as trigger_name,
            p.proname as function_name,
            CASE t.tgtype & 2
                WHEN 0 THEN 'STATEMENT'
                ELSE 'ROW'
            END as trigger_level,
            CASE t.tgtype & 28
                WHEN 4 THEN 'INSERT'
                WHEN 8 THEN 'DELETE'
                WHEN 16 THEN 'UPDATE'
                WHEN 12 THEN 'INSERT/DELETE'
                WHEN 20 THEN 'INSERT/UPDATE'
                WHEN 24 THEN 'DELETE/UPDATE'
                WHEN 28 THEN 'INSERT/DELETE/UPDATE'
                ELSE 'OTHER'
            END as trigger_events,
            CASE t.tgtype & 1
                WHEN 0 THEN 'AFTER'
                ELSE 'BEFORE'
            END as trigger_timing,
            t.tgenabled as is_enabled,
            pg_get_triggerdef(t.oid) as trigger_definition
        FROM pg_trigger t
        JOIN pg_class c ON t.tgrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        JOIN pg_proc p ON t.tgfoid = p.oid
        WHERE n.nspname NOT IN ('information_schema', 'pg_catalog')
            AND NOT t.tgisinternal
        ORDER BY n.nspname, c.relname, t.tgname
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def monitor_checkpoint_efficiency() -> list[dict]:
    """Monitor checkpoint efficiency and timing patterns."""
    query = """
        SELECT 
            checkpoints_timed,
            checkpoints_req as checkpoints_requested,
            checkpoint_write_time,
            checkpoint_sync_time,
            buffers_checkpoint,
            buffers_clean,
            maxwritten_clean,
            buffers_backend,
            buffers_backend_fsync,
            buffers_alloc as buffers_allocated,
            stats_reset,
            round((checkpoint_write_time::numeric / (checkpoints_timed + checkpoints_req)::numeric), 2) as avg_write_time_per_checkpoint,
            round((checkpoint_sync_time::numeric / (checkpoints_timed + checkpoints_req)::numeric), 2) as avg_sync_time_per_checkpoint,
            CASE 
                WHEN checkpoints_req > checkpoints_timed THEN 'TOO FREQUENT - Increase checkpoint_segments or max_wal_size'
                WHEN maxwritten_clean > 0 THEN 'BACKGROUND WRITER STRESS - Consider tuning bgwriter'
                WHEN buffers_backend_fsync > 0 THEN 'BACKEND FSYNC - Increase shared_buffers'
                ELSE 'OK'
            END as recommendation
        FROM pg_stat_bgwriter
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def analyze_foreign_key_locks() -> list[dict]:
    """Analyze foreign key constraints that might cause locking issues."""
    query = """
        SELECT 
            tc.table_schema,
            tc.table_name,
            tc.constraint_name,
            kcu.column_name,
            ccu.table_schema AS foreign_table_schema,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name,
            rc.update_rule,
            rc.delete_rule,
            CASE 
                WHEN rc.update_rule = 'CASCADE' OR rc.delete_rule = 'CASCADE' THEN 'CASCADE - Can cause widespread locks'
                WHEN rc.update_rule = 'SET NULL' OR rc.delete_rule = 'SET NULL' THEN 'SET NULL - Moderate locking'
                WHEN rc.update_rule = 'RESTRICT' OR rc.delete_rule = 'RESTRICT' THEN 'RESTRICT - Minimal locking'
                ELSE 'NO ACTION - Standard locking'
            END as lock_impact_assessment
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu 
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage ccu 
            ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
        JOIN information_schema.referential_constraints rc 
            ON tc.constraint_name = rc.constraint_name
            AND tc.table_schema = rc.constraint_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY tc.table_schema, tc.table_name, tc.constraint_name
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_query_runtime_distribution() -> list[dict]:
    """Get distribution of query runtime in different time buckets."""
    query = """
        SELECT 
            CASE 
                WHEN total_exec_time < 100 THEN '< 100ms'
                WHEN total_exec_time < 1000 THEN '100ms - 1s'
                WHEN total_exec_time < 10000 THEN '1s - 10s'
                WHEN total_exec_time < 60000 THEN '10s - 1m'
                ELSE '> 1m'
            END as runtime_bucket,
            COUNT(*) as query_count,
            SUM(calls) as total_calls,
            ROUND(AVG(mean_exec_time)::numeric, 3) as avg_exec_time_ms,
            ROUND(SUM(total_exec_time)::numeric, 2) as total_time_ms,
            ROUND((SUM(total_exec_time) / SUM(SUM(total_exec_time)) OVER()) * 100, 2) as time_percentage
        FROM pg_stat_statements 
        WHERE calls > 0
        GROUP BY 
            CASE 
                WHEN total_exec_time < 100 THEN '< 100ms'
                WHEN total_exec_time < 1000 THEN '100ms - 1s'
                WHEN total_exec_time < 10000 THEN '1s - 10s'
                WHEN total_exec_time < 60000 THEN '10s - 1m'
                ELSE '> 1m'
            END
        ORDER BY 
            CASE 
                WHEN runtime_bucket = '< 100ms' THEN 1
                WHEN runtime_bucket = '100ms - 1s' THEN 2
                WHEN runtime_bucket = '1s - 10s' THEN 3
                WHEN runtime_bucket = '10s - 1m' THEN 4
                ELSE 5
            END
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_table_access_patterns() -> list[dict]:
    """Analyze table access patterns - sequential scans vs index usage."""
    query = """
        SELECT 
            schemaname,
            tablename,
            seq_scan as sequential_scans,
            seq_tup_read as sequential_tuples_read,
            idx_scan as index_scans,
            idx_tup_fetch as index_tuples_fetched,
            CASE 
                WHEN seq_scan + idx_scan = 0 THEN 'No Activity'
                WHEN seq_scan = 0 THEN '100% Index'
                WHEN idx_scan = 0 THEN '100% Sequential'
                ELSE ROUND((idx_scan::float / (seq_scan + idx_scan)) * 100, 2) || '% Index'
            END as access_pattern,
            CASE 
                WHEN seq_scan > idx_scan * 10 AND seq_tup_read > 100000 THEN 'Consider adding indexes'
                WHEN seq_scan = 0 AND idx_scan > 0 THEN 'Well optimized'
                WHEN seq_scan > 0 AND idx_scan = 0 THEN 'Missing indexes?'
                ELSE 'Balanced access'
            END as optimization_suggestion
        FROM pg_stat_user_tables 
        WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
        ORDER BY seq_tup_read DESC, idx_tup_fetch DESC
        LIMIT 50
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_concurrent_connection_analysis() -> list[dict]:
    """Analyze concurrent connections and their states over time."""
    query = """
        SELECT 
            state,
            COUNT(*) as connection_count,
            COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as percentage,
            AVG(EXTRACT(EPOCH FROM (now() - state_change))) as avg_state_duration_seconds,
            MAX(EXTRACT(EPOCH FROM (now() - state_change))) as max_state_duration_seconds,
            STRING_AGG(DISTINCT application_name, ', ') as applications,
            STRING_AGG(DISTINCT usename, ', ') as users
        FROM pg_stat_activity 
        WHERE pid != pg_backend_pid()
        GROUP BY state
        ORDER BY connection_count DESC
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_checkpoint_analysis() -> list[dict]:
    """Analyze checkpoint frequency and performance metrics."""
    query = """
        SELECT 
            'Checkpoints' as metric_type,
            checkpoints_timed as timed_checkpoints,
            checkpoints_req as requested_checkpoints,
            ROUND((checkpoints_req::float / NULLIF(checkpoints_timed + checkpoints_req, 0)) * 100, 2) as requested_percentage,
            checkpoint_write_time as write_time_ms,
            checkpoint_sync_time as sync_time_ms,
            checkpoint_write_time + checkpoint_sync_time as total_checkpoint_time_ms,
            buffers_checkpoint,
            buffers_clean,
            buffers_backend,
            buffers_backend_fsync,
            ROUND((buffers_backend_fsync::float / NULLIF(buffers_backend, 0)) * 100, 2) as backend_fsync_percentage
        FROM pg_stat_bgwriter
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_table_fragmentation_analysis() -> list[dict]:
    """Analyze table fragmentation and bloat estimation."""
    query = """
        SELECT 
            schemaname,
            tablename,
            pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size,
            n_tup_ins as inserts,
            n_tup_upd as updates,
            n_tup_del as deletes,
            n_live_tup as live_tuples,
            n_dead_tup as dead_tuples,
            CASE 
                WHEN n_live_tup + n_dead_tup = 0 THEN 0
                ELSE ROUND((n_dead_tup::float / (n_live_tup + n_dead_tup)) * 100, 2)
            END as dead_tuple_percentage,
            last_vacuum,
            last_autovacuum,
            last_analyze,
            last_autoanalyze,
            CASE 
                WHEN n_dead_tup > n_live_tup * 0.2 THEN 'High fragmentation - needs vacuum'
                WHEN n_dead_tup > n_live_tup * 0.1 THEN 'Moderate fragmentation'
                ELSE 'Low fragmentation'
            END as fragmentation_status
        FROM pg_stat_user_tables 
        WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
        ORDER BY n_dead_tup DESC, dead_tuple_percentage DESC
        LIMIT 30
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_query_plan_cache_analysis() -> list[dict]:
    """Analyze query plan cache usage and effectiveness."""
    query = """
        SELECT 
            'Generic Plans' as plan_type,
            COUNT(*) as query_count,
            SUM(calls) as total_calls,
            SUM(generic_calls) as generic_calls,
            SUM(custom_calls) as custom_calls,
            ROUND(AVG(generic_calls::float / NULLIF(calls, 0)) * 100, 2) as generic_plan_percentage,
            ROUND(AVG(mean_exec_time), 3) as avg_exec_time_ms,
            COUNT(*) FILTER (WHERE generic_calls > custom_calls) as queries_preferring_generic,
            COUNT(*) FILTER (WHERE custom_calls > generic_calls) as queries_preferring_custom
        FROM pg_stat_statements 
        WHERE calls > 10
        
        UNION ALL
        
        SELECT 
            'Plan Cache Summary' as plan_type,
            COUNT(*) as query_count,
            SUM(calls) as total_calls,
            SUM(generic_calls) as generic_calls,
            SUM(custom_calls) as custom_calls,
            ROUND((SUM(generic_calls)::float / NULLIF(SUM(calls), 0)) * 100, 2) as generic_plan_percentage,
            ROUND(AVG(mean_exec_time), 3) as avg_exec_time_ms,
            0 as queries_preferring_generic,
            0 as queries_preferring_custom
        FROM pg_stat_statements
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_sequence_usage_risks() -> list[dict]:
    """Identify sequences approaching their limits or with risky usage patterns."""
    query = """
        SELECT 
            schemaname,
            sequencename,
            data_type,
            start_value,
            min_value,
            max_value,
            increment_by,
            last_value,
            CASE 
                WHEN data_type = 'bigint' THEN ROUND(((last_value - min_value)::float / (max_value - min_value)) * 100, 4)
                WHEN data_type = 'integer' THEN ROUND(((last_value - min_value)::float / (max_value - min_value)) * 100, 4)
                ELSE 0
            END as usage_percentage,
            CASE 
                WHEN data_type = 'bigint' AND last_value > max_value * 0.8 THEN 'HIGH RISK - Near limit'
                WHEN data_type = 'integer' AND last_value > max_value * 0.8 THEN 'HIGH RISK - Near limit'  
                WHEN data_type = 'integer' AND last_value > 1000000000 THEN 'MEDIUM RISK - Consider bigint'
                WHEN increment_by < 0 AND last_value < min_value * 1.2 THEN 'HIGH RISK - Decrementing near min'
                ELSE 'LOW RISK'
            END as risk_assessment,
            is_cycled,
            is_called
        FROM pg_sequences 
        WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
        ORDER BY usage_percentage DESC, last_value DESC
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_trigger_performance_impact() -> list[dict]:
    """Analyze trigger performance impact on tables."""
    query = """
        SELECT 
            t.schemaname as schema_name,
            t.tablename as table_name,
            tg.trigger_name,
            tg.event_manipulation as trigger_event,
            tg.action_timing as trigger_timing,
            tg.action_statement as trigger_function,
            COALESCE(s.n_tup_ins, 0) as table_inserts,
            COALESCE(s.n_tup_upd, 0) as table_updates,
            COALESCE(s.n_tup_del, 0) as table_deletes,
            CASE 
                WHEN tg.event_manipulation = 'INSERT' THEN s.n_tup_ins
                WHEN tg.event_manipulation = 'UPDATE' THEN s.n_tup_upd
                WHEN tg.event_manipulation = 'DELETE' THEN s.n_tup_del
                ELSE s.n_tup_ins + s.n_tup_upd + s.n_tup_del
            END as relevant_operations,
            CASE 
                WHEN (s.n_tup_ins + s.n_tup_upd + s.n_tup_del) > 100000 THEN 'High activity - monitor trigger performance'
                WHEN (s.n_tup_ins + s.n_tup_upd + s.n_tup_del) > 10000 THEN 'Moderate activity'
                ELSE 'Low activity'
            END as activity_assessment
        FROM pg_stat_user_tables s
        RIGHT JOIN information_schema.triggers tg ON s.tablename = tg.table_name AND s.schemaname = tg.table_schema
        JOIN pg_stat_user_tables t ON t.tablename = tg.table_name AND t.schemaname = tg.table_schema
        WHERE tg.table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY relevant_operations DESC, t.schemaname, t.tablename
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_vacuum_analyze_recommendations() -> list[dict]:
    """Generate vacuum and analyze recommendations based on table activity."""
    query = """
        SELECT 
            schemaname,
            tablename,
            pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as table_size,
            n_dead_tup as dead_tuples,
            n_live_tup as live_tuples,
            ROUND((n_dead_tup::float / NULLIF(n_live_tup + n_dead_tup, 0)) * 100, 2) as dead_percentage,
            last_vacuum,
            last_autovacuum,
            EXTRACT(DAYS FROM (now() - COALESCE(last_vacuum, last_autovacuum))) as days_since_vacuum,
            last_analyze,
            last_autoanalyze,
            EXTRACT(DAYS FROM (now() - COALESCE(last_analyze, last_autoanalyze))) as days_since_analyze,
            n_tup_ins + n_tup_upd + n_tup_del as total_modifications,
            CASE 
                WHEN n_dead_tup > 10000 AND n_dead_tup > n_live_tup * 0.2 THEN 'URGENT: Manual VACUUM needed'
                WHEN n_dead_tup > 5000 AND n_dead_tup > n_live_tup * 0.1 THEN 'Recommend VACUUM'
                WHEN EXTRACT(DAYS FROM (now() - COALESCE(last_vacuum, last_autovacuum))) > 7 THEN 'Consider scheduled VACUUM'
                ELSE 'VACUUM OK'
            END as vacuum_recommendation,
            CASE 
                WHEN EXTRACT(DAYS FROM (now() - COALESCE(last_analyze, last_autoanalyze))) > 7 AND (n_tup_ins + n_tup_upd + n_tup_del) > 1000 THEN 'ANALYZE recommended'
                WHEN EXTRACT(DAYS FROM (now() - COALESCE(last_analyze, last_autoanalyze))) > 14 THEN 'ANALYZE overdue'
                ELSE 'ANALYZE OK'
            END as analyze_recommendation
        FROM pg_stat_user_tables 
        WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
        ORDER BY dead_percentage DESC, total_modifications DESC
        LIMIT 30
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_connection_pool_analysis() -> list[dict]:
    """Analyze connection pool efficiency and usage patterns."""
    query = """
        SELECT 
            application_name,
            usename as username,
            state,
            COUNT(*) as connection_count,
            ROUND(AVG(EXTRACT(EPOCH FROM (now() - state_change))), 2) as avg_state_duration_seconds,
            MIN(backend_start) as oldest_connection,
            MAX(backend_start) as newest_connection,
            COUNT(*) FILTER (WHERE state = 'idle') as idle_connections,
            COUNT(*) FILTER (WHERE state = 'active') as active_connections,
            COUNT(*) FILTER (WHERE state = 'idle in transaction') as idle_in_transaction,
            CASE 
                WHEN COUNT(*) FILTER (WHERE state = 'idle in transaction') > COUNT(*) * 0.3 THEN 'Warning: High idle-in-transaction ratio'
                WHEN COUNT(*) FILTER (WHERE state = 'idle') > COUNT(*) * 0.8 THEN 'Info: High idle connection ratio'
                WHEN COUNT(*) > 50 THEN 'Warning: High connection count for single application'
                ELSE 'OK'
            END as pool_assessment
        FROM pg_stat_activity 
        WHERE pid != pg_backend_pid()
            AND application_name IS NOT NULL
            AND application_name != ''
        GROUP BY application_name, usename, state
        ORDER BY connection_count DESC, application_name
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_query_error_analysis() -> list[dict]:
    """Analyze query errors and failure patterns from logs."""
    query = """
        SELECT 
            'Error Analysis' as analysis_type,
            'pg_stat_database' as source,
            datname as database_name,
            numbackends as active_connections,
            xact_commit as committed_transactions,
            xact_rollback as rolled_back_transactions,
            CASE 
                WHEN xact_commit + xact_rollback = 0 THEN 0
                ELSE ROUND((xact_rollback::float / (xact_commit + xact_rollback)) * 100, 2)
            END as rollback_percentage,
            blk_read_time as block_read_time_ms,
            blk_write_time as block_write_time_ms,
            temp_files,
            temp_bytes,
            deadlocks,
            CASE 
                WHEN xact_rollback > xact_commit * 0.1 THEN 'High rollback rate - investigate errors'
                WHEN deadlocks > 10 THEN 'Deadlock issues detected'
                WHEN temp_files > 1000 THEN 'High temp file usage - memory issues?'
                ELSE 'Normal error patterns'
            END as error_assessment
        FROM pg_stat_database 
        WHERE datname NOT IN ('template0', 'template1', 'postgres')
        ORDER BY rollback_percentage DESC, deadlocks DESC
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_index_maintenance_status() -> list[dict]:
    """Check index maintenance status and identify problematic indexes."""
    query = """
        SELECT 
            schemaname,
            tablename,
            indexname,
            idx_scan as index_scans,
            idx_tup_read as tuples_read,
            idx_tup_fetch as tuples_fetched,
            pg_size_pretty(pg_total_relation_size(schemaname||'.'||indexname)) as index_size,
            CASE 
                WHEN idx_scan = 0 THEN 'UNUSED - Consider dropping'
                WHEN idx_scan < 10 AND pg_total_relation_size(schemaname||'.'||indexname) > 1024*1024 THEN 'LOW USAGE - Large unused index'
                WHEN idx_tup_read > idx_tup_fetch * 10 THEN 'INEFFICIENT - High read/fetch ratio'
                WHEN idx_scan > 1000 AND idx_tup_fetch > 0 THEN 'ACTIVE - Well utilized'
                ELSE 'MODERATE USAGE'
            END as maintenance_status,
            ROUND((idx_tup_fetch::float / NULLIF(idx_tup_read, 0)) * 100, 2) as fetch_efficiency_percentage,
            pg_total_relation_size(schemaname||'.'||indexname) as size_bytes
        FROM pg_stat_user_indexes 
        WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
        ORDER BY 
            CASE 
                WHEN idx_scan = 0 AND pg_total_relation_size(schemaname||'.'||indexname) > 1024*1024 THEN 1
                WHEN idx_scan = 0 THEN 2
                WHEN idx_scan < 10 THEN 3
                ELSE 4
            END,
            size_bytes DESC
        LIMIT 50
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_transaction_age_monitoring() -> list[dict]:
    """Monitor transaction age and identify long-running or problematic transactions."""
    query = """
        SELECT 
            pid,
            usename,
            application_name,
            state,
            EXTRACT(EPOCH FROM (now() - xact_start)) as transaction_age_seconds,
            EXTRACT(EPOCH FROM (now() - query_start)) as query_age_seconds,
            EXTRACT(EPOCH FROM (now() - state_change)) as state_duration_seconds,
            LEFT(query, 100) as query_preview,
            wait_event_type,
            wait_event,
            CASE 
                WHEN EXTRACT(EPOCH FROM (now() - xact_start)) > 3600 THEN 'CRITICAL - Transaction over 1 hour'
                WHEN EXTRACT(EPOCH FROM (now() - xact_start)) > 1800 THEN 'WARNING - Transaction over 30 minutes'
                WHEN EXTRACT(EPOCH FROM (now() - xact_start)) > 600 THEN 'ATTENTION - Transaction over 10 minutes'
                WHEN state = 'idle in transaction' AND EXTRACT(EPOCH FROM (now() - state_change)) > 300 THEN 'WARNING - Idle in transaction over 5 minutes'
                ELSE 'NORMAL'
            END as transaction_status,
            backend_xmin,
            backend_xid
        FROM pg_stat_activity 
        WHERE pid != pg_backend_pid()
            AND xact_start IS NOT NULL
        ORDER BY transaction_age_seconds DESC
        LIMIT 30
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_write_ahead_log_analysis() -> list[dict]:
    """Analyze Write-Ahead Log (WAL) generation and archiving performance."""
    query = """
        SELECT 
            'WAL Statistics' as metric_category,
            pg_current_wal_lsn() as current_wal_location,
            pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0') / 1024 / 1024 as total_wal_generated_mb,
            CASE 
                WHEN pg_is_in_recovery() THEN 'REPLICA - In recovery mode'
                ELSE 'PRIMARY - Active WAL generation'
            END as wal_role,
            (SELECT COUNT(*) FROM pg_stat_replication) as connected_replicas,
            (SELECT setting FROM pg_settings WHERE name = 'wal_level') as wal_level,
            (SELECT setting FROM pg_settings WHERE name = 'archive_mode') as archive_mode,
            (SELECT setting FROM pg_settings WHERE name = 'archive_command') as archive_command,
            NULL::bigint as wal_files_archived,
            NULL::bigint as wal_files_failed,
            NULL::text as archival_status
            
        UNION ALL
        
        SELECT 
            'WAL Archiver Stats' as metric_category,
            NULL as current_wal_location,
            NULL as total_wal_generated_mb,
            NULL as wal_role,
            NULL as connected_replicas,
            NULL as wal_level,
            NULL as archive_mode, 
            NULL as archive_command,
            archived_count as wal_files_archived,
            failed_count as wal_files_failed,
            CASE 
                WHEN failed_count > archived_count * 0.1 THEN 'HIGH FAILURE RATE - Check archive command'
                WHEN failed_count > 0 THEN 'Some failures detected'
                WHEN archived_count > 0 THEN 'Archiving working normally'
                ELSE 'No archival activity'
            END as archival_status
        FROM pg_stat_archiver
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_memory_context_analysis() -> list[dict]:
    """Analyze PostgreSQL memory context usage and identify memory-intensive operations."""
    query = """
        SELECT 
            pid,
            usename,
            application_name,
            state,
            query_start,
            LEFT(query, 100) as query_preview,
            EXTRACT(EPOCH FROM (now() - query_start)) as query_duration_seconds,
            CASE 
                WHEN query ~* 'CREATE INDEX|REINDEX' THEN 'INDEX_OPERATION - High memory usage expected'
                WHEN query ~* 'VACUUM|ANALYZE' THEN 'MAINTENANCE - Memory for cleanup operations'
                WHEN query ~* 'SELECT.*ORDER BY.*LIMIT' AND query !~* 'INDEX' THEN 'SORT_OPERATION - May use work_mem'
                WHEN query ~* 'GROUP BY|DISTINCT|UNION' THEN 'AGGREGATION - Hash table memory usage'
                WHEN query ~* 'JOIN' AND query !~* 'INDEX' THEN 'JOIN_OPERATION - Hash join memory usage'
                WHEN query ~* 'CREATE TABLE.*AS|SELECT.*INTO' THEN 'BULK_OPERATION - Temporary memory usage'
                ELSE 'STANDARD_QUERY'
            END as memory_usage_category,
            wait_event_type,
            wait_event,
            CASE 
                WHEN wait_event_type = 'IO' AND wait_event IN ('DataFileRead', 'DataFileWrite') THEN 'Disk I/O - possible memory pressure'
                WHEN wait_event = 'BufferPin' THEN 'Buffer contention - memory allocation issue'
                WHEN state = 'active' AND EXTRACT(EPOCH FROM (now() - query_start)) > 300 THEN 'Long-running - monitor memory usage'
                ELSE 'Normal memory usage pattern'
            END as memory_analysis
        FROM pg_stat_activity 
        WHERE pid != pg_backend_pid()
            AND state != 'idle'
            AND query IS NOT NULL
        ORDER BY query_duration_seconds DESC
        LIMIT 20
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_extension_usage_analysis() -> list[dict]:
    """Analyze installed extensions and their usage patterns."""
    query = """
        SELECT 
            extname as extension_name,
            extversion as version,
            nspname as schema_name,
            extrelocatable as relocatable,
            (SELECT description FROM pg_description WHERE objoid = e.oid) as description,
            CASE 
                WHEN extname = 'pg_stat_statements' THEN 'Query monitoring - Check if being utilized'
                WHEN extname = 'pg_buffercache' THEN 'Buffer cache analysis - Memory optimization'
                WHEN extname = 'pgcrypto' THEN 'Cryptographic functions - Security enhancement'
                WHEN extname = 'uuid-ossp' THEN 'UUID generation - Identity management'
                WHEN extname = 'hstore' THEN 'Key-value storage - NoSQL-like functionality'
                WHEN extname = 'postgis' THEN 'Spatial data - GIS functionality'
                WHEN extname LIKE 'pg_trgm' THEN 'Text search optimization - Full-text search'
                WHEN extname LIKE 'plpg%' THEN 'Procedural language - Advanced programming'
                ELSE 'Extension functionality varies'
            END as usage_category,
            CASE 
                WHEN extname = 'pg_stat_statements' AND NOT EXISTS (SELECT 1 FROM pg_stat_statements LIMIT 1) THEN 'Installed but not configured'
                WHEN extname = 'pg_buffercache' THEN 'Available for buffer analysis'
                ELSE 'Active and available'
            END as status_assessment
        FROM pg_extension e
        JOIN pg_namespace n ON n.oid = e.extnamespace
        ORDER BY extname
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_backup_recovery_readiness() -> list[dict]:
    """Assess backup and recovery readiness of the PostgreSQL instance."""
    query = """
        SELECT 
            'Archive Settings' as readiness_category,
            (SELECT setting FROM pg_settings WHERE name = 'archive_mode') as archive_mode,
            (SELECT setting FROM pg_settings WHERE name = 'archive_command') as archive_command,
            (SELECT setting FROM pg_settings WHERE name = 'wal_level') as wal_level,
            NULL::bigint as archived_count,
            NULL::bigint as failed_count,
            CASE 
                WHEN (SELECT setting FROM pg_settings WHERE name = 'archive_mode') = 'off' THEN 'WARNING - Archiving disabled'
                WHEN (SELECT setting FROM pg_settings WHERE name = 'archive_command') = '' THEN 'WARNING - No archive command set'
                WHEN (SELECT setting FROM pg_settings WHERE name = 'wal_level') = 'minimal' THEN 'WARNING - WAL level minimal'
                ELSE 'Archive configuration OK'
            END as readiness_assessment
            
        UNION ALL
        
        SELECT 
            'Archiver Performance' as readiness_category,
            NULL as archive_mode,
            NULL as archive_command,
            NULL as wal_level,
            archived_count,
            failed_count,
            CASE 
                WHEN failed_count > 0 AND archived_count = 0 THEN 'CRITICAL - All archival attempts failing'
                WHEN failed_count > archived_count * 0.2 THEN 'WARNING - High archival failure rate'
                WHEN archived_count > 0 THEN 'Archive performance OK'
                ELSE 'No archival activity detected'
            END as readiness_assessment
        FROM pg_stat_archiver
        
        UNION ALL
        
        SELECT 
            'Replication Status' as readiness_category,
            NULL as archive_mode,
            NULL as archive_command,
            NULL as wal_level,
            COUNT(*)::bigint as archived_count,
            NULL as failed_count,
            CASE 
                WHEN COUNT(*) = 0 THEN 'No replicas configured'
                WHEN COUNT(*) > 0 THEN COUNT(*) || ' replica(s) connected - Good for DR'
                ELSE 'Replication status unknown'
            END as readiness_assessment
        FROM pg_stat_replication
        
        UNION ALL
        
        SELECT 
            'Recovery Settings' as readiness_category,
            (SELECT setting FROM pg_settings WHERE name = 'hot_standby') as archive_mode,
            (SELECT setting FROM pg_settings WHERE name = 'max_wal_senders') as archive_command,
            (SELECT setting FROM pg_settings WHERE name = 'wal_keep_size') as wal_level,
            NULL as archived_count,
            NULL as failed_count,
            CASE 
                WHEN (SELECT setting::int FROM pg_settings WHERE name = 'max_wal_senders') = 0 THEN 'No WAL senders configured'
                WHEN (SELECT setting FROM pg_settings WHERE name = 'hot_standby') = 'off' THEN 'Hot standby disabled'
                ELSE 'Recovery settings configured'
            END as readiness_assessment
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_constraint_violation_risks() -> list[dict]:
    """Identify potential constraint violation risks and integrity issues."""
    query = """
        SELECT 
            tc.table_schema,
            tc.table_name,
            tc.constraint_name,
            tc.constraint_type,
            cc.check_clause,
            CASE tc.constraint_type
                WHEN 'CHECK' THEN 'Data validation constraint'
                WHEN 'UNIQUE' THEN 'Uniqueness constraint'
                WHEN 'PRIMARY KEY' THEN 'Primary key constraint'
                WHEN 'FOREIGN KEY' THEN 'Referential integrity constraint'
                ELSE 'Other constraint type'
            END as constraint_purpose,
            CASE 
                WHEN tc.constraint_type = 'FOREIGN KEY' AND NOT tc.is_deferrable THEN 'Non-deferrable FK - can cause blocking'
                WHEN tc.constraint_type = 'CHECK' AND cc.check_clause ~* 'NOT NULL' THEN 'NOT NULL constraint via CHECK'
                WHEN tc.constraint_type = 'UNIQUE' THEN 'Uniqueness enforced - watch for violations'
                WHEN tc.constraint_type = 'PRIMARY KEY' THEN 'Critical for referential integrity'
                ELSE 'Standard constraint behavior'
            END as risk_assessment,
            tc.is_deferrable,
            tc.initially_deferred,
            s.n_tup_ins as recent_inserts,
            s.n_tup_upd as recent_updates
        FROM information_schema.table_constraints tc
        LEFT JOIN information_schema.check_constraints cc ON tc.constraint_name = cc.constraint_name
        LEFT JOIN pg_stat_user_tables s ON tc.table_name = s.tablename AND tc.table_schema = s.schemaname
        WHERE tc.table_schema NOT IN ('information_schema', 'pg_catalog')
            AND tc.constraint_type IN ('CHECK', 'UNIQUE', 'PRIMARY KEY', 'FOREIGN KEY')
        ORDER BY 
            CASE tc.constraint_type
                WHEN 'PRIMARY KEY' THEN 1
                WHEN 'FOREIGN KEY' THEN 2
                WHEN 'UNIQUE' THEN 3
                WHEN 'CHECK' THEN 4
                ELSE 5
            END,
            s.n_tup_ins + s.n_tup_upd DESC
        LIMIT 50
    """
    
    rows = await execute_query(query)
    return rows

@mcp.tool()
async def PostgreSQL_get_performance_regression_indicators() -> list[dict]:
    """Identify potential performance regression indicators in queries and operations."""
    query = """
        SELECT 
            LEFT(query, 80) as query_sample,
            calls,
            ROUND(total_exec_time::numeric, 2) as total_time_ms,
            ROUND(mean_exec_time::numeric, 3) as avg_time_ms,
            ROUND(stddev_exec_time::numeric, 3) as stddev_time_ms,
            ROUND(max_exec_time::numeric, 2) as max_time_ms,
            ROUND(min_exec_time::numeric, 2) as min_time_ms,
            rows as avg_rows_returned,
            shared_blks_hit + shared_blks_read as total_blocks_accessed,
            shared_blks_read as blocks_read_from_disk,
            CASE 
                WHEN shared_blks_hit + shared_blks_read = 0 THEN 0
                ELSE ROUND((shared_blks_hit::float / (shared_blks_hit + shared_blks_read)) * 100, 2)
            END as cache_hit_ratio,
            CASE 
                WHEN stddev_exec_time > mean_exec_time * 2 THEN 'HIGH VARIABILITY - Performance regression risk'
                WHEN max_exec_time > mean_exec_time * 10 THEN 'OUTLIER DETECTED - Investigate slow executions'
                WHEN shared_blks_read > shared_blks_hit THEN 'LOW CACHE HIT - I/O intensive query'
                WHEN mean_exec_time > 1000 AND calls > 100 THEN 'SLOW FREQUENT QUERY - Optimization needed'
                WHEN calls > 10000 AND mean_exec_time > 100 THEN 'HIGH VOLUME SLOW - Priority optimization'
                ELSE 'PERFORMANCE OK'
            END as regression_indicator
        FROM pg_stat_statements 
        WHERE calls > 10
        ORDER BY 
            CASE 
                WHEN stddev_exec_time > mean_exec_time * 2 THEN 1
                WHEN max_exec_time > mean_exec_time * 10 THEN 2
                WHEN shared_blks_read > shared_blks_hit THEN 3
                ELSE 4
            END,
            total_exec_time DESC
        LIMIT 30
    """
    
    rows = await execute_query(query)
    return rows

# Resources
@mcp.resource("postgres://tables/{schema}")
async def get_tables_resource(schema: str = "public") -> str:
    """Get a list of all tables in the specified schema."""
    tables = await PostgreSQL_list_tables(schema)
    return json.dumps([table.dict() for table in tables], indent=2)

@mcp.resource("postgres://table/{schema}/{table_name}")
async def get_table_resource(schema: str, table_name: str) -> str:
    """Get detailed information about a specific table."""
    columns = await PostgreSQL_describe_table(table_name, schema)
    return json.dumps([col.dict() for col in columns], indent=2)

# Prompts
@mcp.prompt()
async def analyze_table(table_name: str, schema_name: str = "public") -> str:
    """Generate a prompt for analyzing a database table.
    
    Args:
        table_name: Name of the table to analyze
        schema_name: Database schema name (default: public)
    """
    prompt_text = f"Please analyze the PostgreSQL table '{schema_name}.{table_name}'.\n\n"
    prompt_text += "I'd like you to:\n"
    prompt_text += "1. Examine the table structure and columns\n"
    prompt_text += "2. Suggest any potential improvements or optimizations\n"
    prompt_text += "3. Identify any data quality issues\n"
    prompt_text += "4. Recommend appropriate indexes if needed\n\n"
    prompt_text += "Use the available tools to gather information about this table."
    return prompt_text

@mcp.prompt()
async def query_builder(table_name: str, action: str = "select") -> str:
    """Generate a prompt for building SQL queries.
    
    Args:
        table_name: Target table name
        action: Type of query (select, insert, update, delete)
    """
    prompt_text = f"Help me build a {action.upper()} query for the table '{table_name}'.\n\n"
    prompt_text += "Please:\n"
    prompt_text += "1. First, examine the table structure using the describe_table tool\n"
    prompt_text += f"2. Based on the columns available, suggest an appropriate {action.upper()} query\n"
    prompt_text += "3. Explain the query and its potential impact\n"
    prompt_text += "4. If it's a destructive operation, warn about the consequences\n\n"
    prompt_text += f"Table: {table_name}\n"
    prompt_text += f"Action: {action.upper()}"
    return prompt_text

# Advanced PostgreSQL tools for performance monitoring and administration

@mcp.tool()
async def PostgreSQL_connection_pool_stats() -> str:
    """Get PostgreSQL connection pool statistics and backend process information."""
    query = """
    SELECT 
        state,
        count(*) as count,
        max(extract(epoch from (now() - state_change))) as max_duration_seconds,
        avg(extract(epoch from (now() - state_change))) as avg_duration_seconds
    FROM pg_stat_activity 
    WHERE pid != pg_backend_pid() 
    GROUP BY state 
    ORDER BY count DESC;
    """
    
    result = await execute_query(query)
    return json.dumps([dict(row) for row in result], default=str, indent=2)

@mcp.tool()
async def PostgreSQL_buffer_cache_hit_ratio() -> str:
    """Analyze buffer cache hit ratios for all databases and tables."""
    query = """
    SELECT 
        schemaname,
        tablename,
        heap_blks_read,
        heap_blks_hit,
        case 
            when heap_blks_read + heap_blks_hit = 0 then 0 
            else round(100.0 * heap_blks_hit / (heap_blks_read + heap_blks_hit), 2) 
        end as cache_hit_ratio,
        idx_blks_read,
        idx_blks_hit,
        case 
            when idx_blks_read + idx_blks_hit = 0 then 0 
            else round(100.0 * idx_blks_hit / (idx_blks_read + idx_blks_hit), 2) 
        end as index_cache_hit_ratio
    FROM pg_statio_user_tables 
    ORDER BY heap_blks_read + heap_blks_hit DESC;
    """
    
    result = await execute_query(query)
    return json.dumps([dict(row) for row in result], default=str, indent=2)

@mcp.tool()
async def PostgreSQL_checkpoint_activity() -> str:
    """Get detailed checkpoint activity and WAL statistics."""
    query = """
    SELECT 
        checkpoints_timed,
        checkpoints_req,
        checkpoint_write_time,
        checkpoint_sync_time,
        buffers_checkpoint,
        buffers_clean,
        maxwritten_clean,
        buffers_backend,
        buffers_backend_fsync,
        buffers_alloc,
        stats_reset
    FROM pg_stat_bgwriter;
    """
    
    result = await execute_query(query)
    return json.dumps([dict(row) for row in result], default=str, indent=2)

@mcp.tool()
async def PostgreSQL_wait_events_analysis() -> str:
    """Analyze current wait events and blocking processes."""
    query = """
    SELECT 
        pid,
        usename,
        application_name,
        client_addr,
        state,
        wait_event_type,
        wait_event,
        query_start,
        state_change,
        extract(epoch from (now() - query_start)) as query_duration_seconds,
        left(query, 100) as query_preview
    FROM pg_stat_activity 
    WHERE wait_event IS NOT NULL 
        AND state != 'idle'
    ORDER BY query_start;
    """
    
    result = await execute_query(query)
    return json.dumps([dict(row) for row in result], default=str, indent=2)

@mcp.tool()
async def PostgreSQL_table_size_growth() -> str:
    """Analyze table size growth and storage efficiency."""
    query = """
    SELECT 
        schemaname,
        tablename,
        pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size,
        pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as table_size,
        pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename) - pg_relation_size(schemaname||'.'||tablename)) as index_size,
        n_tup_ins,
        n_tup_upd,
        n_tup_del,
        n_live_tup,
        n_dead_tup,
        case 
            when n_live_tup + n_dead_tup = 0 then 0 
            else round(100.0 * n_dead_tup / (n_live_tup + n_dead_tup), 2) 
        end as dead_tuple_percentage,
        last_vacuum,
        last_autovacuum,
        last_analyze,
        last_autoanalyze
    FROM pg_stat_user_tables 
    ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
    """
    
    result = await execute_query(query)
    return json.dumps([dict(row) for row in result], default=str, indent=2)

@mcp.tool()
async def PostgreSQL_index_scan_efficiency() -> str:
    """Analyze index usage efficiency and identify unused indexes."""
    query = """
    SELECT 
        schemaname,
        tablename,
        indexname,
        idx_scan,
        idx_tup_read,
        idx_tup_fetch,
        case when idx_scan = 0 then 'UNUSED' 
             when idx_scan < 100 then 'LOW_USAGE' 
             else 'GOOD_USAGE' 
        end as usage_status,
        pg_size_pretty(pg_relation_size(indexrelid)) as index_size,
        case 
            when idx_scan = 0 then 0 
            else round(idx_tup_fetch::numeric / idx_scan, 2) 
        end as avg_tuples_per_scan
    FROM pg_stat_user_indexes 
    ORDER BY idx_scan ASC, pg_relation_size(indexrelid) DESC;
    """
    
    result = await execute_query(query)
    return json.dumps([dict(row) for row in result], default=str, indent=2)

@mcp.tool()
async def PostgreSQL_transaction_wraparound_monitoring() -> str:
    """Monitor transaction ID wraparound status for all databases."""
    query = """
    SELECT 
        datname,
        age(datfrozenxid) as xid_age,
        2147483648 - age(datfrozenxid) as xids_until_wraparound,
        round(100.0 * age(datfrozenxid) / 2147483648, 2) as percent_towards_wraparound,
        case 
            when age(datfrozenxid) > 1600000000 then 'CRITICAL' 
            when age(datfrozenxid) > 1400000000 then 'WARNING' 
            else 'OK' 
        end as status
    FROM pg_database 
    WHERE datallowconn 
    ORDER BY age(datfrozenxid) DESC;
    """
    
    result = await execute_query(query)
    return json.dumps([dict(row) for row in result], default=str, indent=2)

@mcp.tool()
async def PostgreSQL_memory_usage_analysis() -> str:
    """Analyze PostgreSQL memory usage and shared memory statistics."""
    query = """
    SELECT 
        name,
        setting,
        unit,
        category,
        short_desc,
        case 
            when unit = '8kB' then pg_size_pretty(setting::bigint * 8192)
            when unit = 'kB' then pg_size_pretty(setting::bigint * 1024)
            when unit = 'MB' then pg_size_pretty(setting::bigint * 1048576)
            else setting || ' ' || coalesce(unit, '')
        end as formatted_value
    FROM pg_settings 
    WHERE category LIKE '%Memory%' 
       OR name IN ('shared_buffers', 'work_mem', 'maintenance_work_mem', 'temp_buffers', 'max_connections')
    ORDER BY category, name;
    """
    
    result = await execute_query(query)
    return json.dumps([dict(row) for row in result], default=str, indent=2)

@mcp.tool()
async def PostgreSQL_backup_recovery_info() -> str:
    """Get backup and recovery related information including WAL archiving status."""
    query = """
    SELECT 
        'Archive Status' as metric_type,
        case when pg_is_in_recovery() then 'Standby/Recovery Mode' else 'Primary Mode' end as status,
        pg_current_wal_lsn() as current_wal_lsn,
        pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0') as total_wal_bytes,
        pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')) as total_wal_size,
        now() - pg_postmaster_start_time() as uptime
    UNION ALL
    SELECT 
        'Recovery Status' as metric_type,
        case when pg_is_in_recovery() then 'IN RECOVERY' else 'NORMAL' end as status,
        case when pg_is_in_recovery() then pg_last_wal_receive_lsn()::text else 'N/A' end as current_wal_lsn,
        case when pg_is_in_recovery() then pg_wal_lsn_diff(pg_last_wal_receive_lsn(), '0/0') else 0 end as total_wal_bytes,
        case when pg_is_in_recovery() then pg_size_pretty(pg_wal_lsn_diff(pg_last_wal_receive_lsn(), '0/0')) else 'N/A' end as total_wal_size,
        now() - pg_postmaster_start_time() as uptime;
    """
    
    result = await execute_query(query)
    return json.dumps([dict(row) for row in result], default=str, indent=2)

@mcp.tool()
async def PostgreSQL_autovacuum_tuning() -> str:
    """Analyze autovacuum settings and provide tuning recommendations."""
    query = """
    SELECT 
        schemaname,
        tablename,
        n_tup_ins,
        n_tup_upd,
        n_tup_del,
        n_live_tup,
        n_dead_tup,
        case 
            when n_live_tup = 0 then 0 
            else round(100.0 * n_dead_tup / n_live_tup, 2) 
        end as dead_tuple_ratio,
        last_vacuum,
        last_autovacuum,
        vacuum_count,
        autovacuum_count,
        case 
            when n_dead_tup > n_live_tup * 0.2 and last_autovacuum < now() - interval '1 day' then 'NEEDS_VACUUM'
            when n_dead_tup > n_live_tup * 0.1 then 'MONITOR_CLOSELY'
            else 'OK'
        end as vacuum_recommendation
    FROM pg_stat_user_tables 
    WHERE n_live_tup > 1000  -- Focus on tables with significant data
    ORDER BY dead_tuple_ratio DESC, n_dead_tup DESC;
    """
    
    result = await execute_query(query)
    return json.dumps([dict(row) for row in result], default=str, indent=2)

@mcp.tool()
async def PostgreSQL_query_plan_cache() -> str:
    """Analyze query plan cache statistics from pg_stat_statements if available."""
    query = """
    SELECT 
        query,
        calls,
        total_exec_time,
        mean_exec_time,
        stddev_exec_time,
        min_exec_time,
        max_exec_time,
        rows,
        100.0 * shared_blks_hit / nullif(shared_blks_hit + shared_blks_read, 0) AS hit_percent,
        shared_blks_read,
        shared_blks_hit,
        shared_blks_written,
        temp_blks_read,
        temp_blks_written
    FROM pg_stat_statements 
    WHERE calls > 10  -- Focus on frequently called queries
    ORDER BY total_exec_time DESC 
    LIMIT 20;
    """
    
    try:
        result = await execute_query(query)
        return json.dumps([dict(row) for row in result], default=str, indent=2)
    except Exception as e:
        return json.dumps({"error": "pg_stat_statements extension not available or not installed", "details": str(e)}, indent=2)

@mcp.tool()
async def PostgreSQL_constraint_violations() -> str:
    """Check for constraint violations and data integrity issues."""
    query = """
    SELECT 
        t.table_schema,
        t.table_name,
        t.constraint_name,
        t.constraint_type,
        case t.constraint_type
            when 'FOREIGN KEY' then 'Check for orphaned records'
            when 'CHECK' then 'Verify check constraint logic'
            when 'UNIQUE' then 'Look for duplicate values'
            when 'PRIMARY KEY' then 'Ensure primary key integrity'
            else 'General constraint check'
        end as check_recommendation,
        t.is_deferrable,
        t.initially_deferred
    FROM information_schema.table_constraints t
    WHERE t.table_schema NOT IN ('information_schema', 'pg_catalog')
        AND t.constraint_type IN ('FOREIGN KEY', 'CHECK', 'UNIQUE', 'PRIMARY KEY')
    ORDER BY t.table_schema, t.table_name, t.constraint_type;
    """
    
    result = await execute_query(query)
    return json.dumps([dict(row) for row in result], default=str, indent=2)

@mcp.tool()
async def PostgreSQL_extension_usage() -> str:
    """List installed extensions and their usage statistics."""
    query = """
    SELECT 
        e.extname,
        e.extversion,
        n.nspname as schema_name,
        e.extrelocatable,
        array_to_string(e.extconfig, ', ') as config_tables,
        pg_size_pretty(sum(pg_total_relation_size(c.oid))) as total_size
    FROM pg_extension e
    LEFT JOIN pg_namespace n ON n.oid = e.extnamespace
    LEFT JOIN pg_depend d ON d.objid = e.oid
    LEFT JOIN pg_class c ON c.oid = d.objid AND c.relkind = 'r'
    WHERE e.extname != 'plpgsql'  -- Exclude default extension
    GROUP BY e.extname, e.extversion, n.nspname, e.extrelocatable, e.extconfig
    ORDER BY e.extname;
    """
    
    result = await execute_query(query)
    return json.dumps([dict(row) for row in result], default=str, indent=2)

@mcp.tool()
async def PostgreSQL_disk_usage_forecast() -> str:
    """Analyze database growth patterns and forecast disk usage."""
    query = """
    SELECT 
        schemaname,
        tablename,
        pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as current_size,
        n_tup_ins as total_inserts,
        n_tup_upd as total_updates,
        n_tup_del as total_deletes,
        n_live_tup,
        case 
            when n_tup_ins > 0 then round(pg_total_relation_size(schemaname||'.'||tablename)::numeric / n_tup_ins, 2)
            else 0 
        end as bytes_per_row,
        case 
            when extract(epoch from (now() - coalesce(last_analyze, last_autoanalyze, now() - interval '30 days'))) > 0
            then round(n_tup_ins / (extract(epoch from (now() - coalesce(last_analyze, last_autoanalyze, now() - interval '30 days'))) / 86400), 2)
            else 0 
        end as avg_daily_inserts
    FROM pg_stat_user_tables
    WHERE n_live_tup > 1000
    ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
    """
    
    result = await execute_query(query)
    return json.dumps([dict(row) for row in result], default=str, indent=2)

@mcp.tool()
async def PostgreSQL_replication_lag_detailed() -> str:
    """Get detailed replication lag information for streaming replication."""
    query = """
    SELECT 
        client_addr,
        client_hostname,
        client_port,
        state,
        sent_lsn,
        write_lsn,
        flush_lsn,
        replay_lsn,
        pg_size_pretty(pg_wal_lsn_diff(sent_lsn, write_lsn)) as write_lag_bytes,
        pg_size_pretty(pg_wal_lsn_diff(sent_lsn, flush_lsn)) as flush_lag_bytes,
        pg_size_pretty(pg_wal_lsn_diff(sent_lsn, replay_lsn)) as replay_lag_bytes,
        write_lag,
        flush_lag,
        replay_lag,
        sync_priority,
        sync_state,
        reply_time
    FROM pg_stat_replication
    ORDER BY client_addr;
    """
    
    result = await execute_query(query)
    return json.dumps([dict(row) for row in result], default=str, indent=2)

@mcp.tool()
async def PostgreSQL_security_audit() -> str:
    """Perform basic security audit checks on database configuration."""
    query = """
    SELECT 
        'Password Authentication' as audit_category,
        case 
            when setting = 'on' then 'SECURE'
            else 'REVIEW_NEEDED'
        end as status,
        'password_encryption = ' || setting as details
    FROM pg_settings WHERE name = 'password_encryption'
    
    UNION ALL
    
    SELECT 
        'SSL Configuration' as audit_category,
        case 
            when setting = 'on' then 'SECURE'
            else 'INSECURE'
        end as status,
        'ssl = ' || setting as details
    FROM pg_settings WHERE name = 'ssl'
    
    UNION ALL
    
    SELECT 
        'Logging Configuration' as audit_category,
        case 
            when setting IN ('all', 'ddl', 'mod') then 'GOOD'
            else 'REVIEW_NEEDED'
        end as status,
        'log_statement = ' || setting as details
    FROM pg_settings WHERE name = 'log_statement'
    
    UNION ALL
    
    SELECT 
        'Connection Logging' as audit_category,
        case 
            when setting = 'on' then 'GOOD'
            else 'REVIEW_NEEDED'
        end as status,
        'log_connections = ' || setting as details
    FROM pg_settings WHERE name = 'log_connections';
    """
    
    result = await execute_query(query)
    return json.dumps([dict(row) for row in result], default=str, indent=2)

@mcp.tool()
async def PostgreSQL_temp_file_usage() -> str:
    """Monitor temporary file usage which indicates memory pressure."""
    query = """
    SELECT 
        datname,
        temp_files,
        pg_size_pretty(temp_bytes) as temp_bytes_formatted,
        temp_bytes,
        case 
            when temp_bytes > 1073741824 then 'HIGH_TEMP_USAGE'  -- > 1GB
            when temp_bytes > 268435456 then 'MODERATE_TEMP_USAGE'  -- > 256MB
            else 'LOW_TEMP_USAGE'
        end as temp_usage_level,
        stats_reset
    FROM pg_stat_database 
    WHERE datname IS NOT NULL
    ORDER BY temp_bytes DESC;
    """
    
    result = await execute_query(query)
    return json.dumps([dict(row) for row in result], default=str, indent=2)

@mcp.tool()
async def PostgreSQL_partition_maintenance() -> str:
    """Analyze partitioned tables and their maintenance status."""
    query = """
    SELECT 
        schemaname,
        tablename,
        partitionmethod,
        partitionkey,
        case 
            when partitionmethod = 'range' then 'Range Partitioned'
            when partitionmethod = 'list' then 'List Partitioned'
            when partitionmethod = 'hash' then 'Hash Partitioned'
            else 'Unknown Method'
        end as partition_type,
        (SELECT count(*) 
         FROM pg_inherits i 
         JOIN pg_class c ON i.inhrelid = c.oid 
         WHERE i.inhparent = (schemaname||'.'||tablename)::regclass
        ) as partition_count
    FROM pg_partitioned_tables
    ORDER BY schemaname, tablename;
    """
    
    try:
        result = await execute_query(query)
        return json.dumps([dict(row) for row in result], default=str, indent=2)
    except Exception as e:
        return json.dumps({"info": "No partitioned tables found or partitioning not supported in this PostgreSQL version", "details": str(e)}, indent=2)

@mcp.tool()
async def PostgreSQL_deadlock_analysis() -> str:
    """Analyze deadlock history and patterns from logs."""
    query = """
    SELECT 
        'Deadlock Monitoring Info' as analysis_type,
        'Check log_lock_waits, deadlock_timeout settings' as recommendation,
        setting as current_deadlock_timeout
    FROM pg_settings 
    WHERE name = 'deadlock_timeout'
    
    UNION ALL
    
    SELECT 
        'Lock Wait Logging' as analysis_type,
        'Enable log_lock_waits for detailed deadlock analysis' as recommendation,
        setting as current_setting
    FROM pg_settings 
    WHERE name = 'log_lock_waits'
    
    UNION ALL
    
    SELECT 
        'Current Blocking Queries' as analysis_type,
        'Active queries that may cause deadlocks' as recommendation,
        count(*)::text as blocking_count
    FROM pg_stat_activity 
    WHERE wait_event_type = 'Lock' AND state = 'active';
    """
    
    result = await execute_query(query)
    return json.dumps([dict(row) for row in result], default=str, indent=2)

@mcp.tool()
async def PostgreSQL_index_dead_tuples_analysis() -> str:
    """Analyze dead tuples per index for vacuum optimization."""
    query = """
    SELECT 
        schemaname,
        tablename,
        indexname,
        idx_scan as index_scans,
        idx_tup_read as tuples_read,
        idx_tup_fetch as tuples_fetched,
        CASE 
            WHEN idx_scan > 0 THEN round((idx_tup_read::numeric / idx_scan), 2)
            ELSE 0 
        END as avg_tuples_per_scan,
        pg_size_pretty(pg_relation_size(indexrelid)) as index_size
    FROM pg_stat_user_indexes 
    WHERE idx_scan > 0
    ORDER BY (idx_tup_read / GREATEST(idx_scan, 1)) DESC
    LIMIT 20;
    """
    
    result = await execute_query(query)
    return json.dumps([dict(row) for row in result], default=str, indent=2)

@mcp.tool()
async def PostgreSQL_vacuum_analyze_frequency_analysis() -> str:
    """Compare vacuum/analyze frequency with table modification rates."""
    query = """
    SELECT 
        schemaname,
        relname,
        n_tup_ins + n_tup_upd + n_tup_del as total_modifications,
        n_tup_ins as inserts,
        n_tup_upd as updates, 
        n_tup_del as deletes,
        last_vacuum,
        last_autovacuum,
        last_analyze,
        last_autoanalyze,
        CASE 
            WHEN last_autovacuum IS NOT NULL THEN 
                extract(epoch from (now() - last_autovacuum))/3600 
            ELSE NULL 
        END as hours_since_last_autovacuum,
        CASE 
            WHEN (n_tup_ins + n_tup_upd + n_tup_del) > 0 AND last_autovacuum IS NOT NULL THEN
                round((n_tup_ins + n_tup_upd + n_tup_del) / 
                      GREATEST(extract(epoch from (now() - last_autovacuum))/3600, 1), 2)
            ELSE 0
        END as modifications_per_hour_since_vacuum
    FROM pg_stat_user_tables 
    ORDER BY total_modifications DESC
    LIMIT 25;
    """
    
    result = await execute_query(query)
    return json.dumps([dict(row) for row in result], default=str, indent=2)

@mcp.tool()
async def PostgreSQL_seqscan_heavy_tables() -> str:
    """Identify tables with high sequential scans that might need indexes."""
    query = """
    SELECT 
        schemaname,
        relname,
        seq_scan,
        seq_tup_read,
        idx_scan,
        idx_tup_fetch,
        n_tup_ins + n_tup_upd + n_tup_del as total_modifications,
        CASE 
            WHEN seq_scan > 0 THEN round(seq_tup_read::numeric / seq_scan, 2)
            ELSE 0 
        END as avg_rows_per_seqscan,
        CASE 
            WHEN (seq_scan + COALESCE(idx_scan, 0)) > 0 THEN 
                round(seq_scan::numeric / (seq_scan + COALESCE(idx_scan, 0)) * 100, 2)
            ELSE 0
        END as seqscan_percentage,
        pg_size_pretty(pg_total_relation_size(relid)) as total_size
    FROM pg_stat_user_tables 
    WHERE seq_scan > 100 
    ORDER BY seq_scan DESC, avg_rows_per_seqscan DESC
    LIMIT 20;
    """
    
    result = await execute_query(query)
    return json.dumps([dict(row) for row in result], default=str, indent=2)

@mcp.tool()
async def PostgreSQL_index_bloat_maintenance_analysis() -> str:
    """Analyze index bloat and suggest maintenance actions."""
    query = """
    SELECT 
        schemaname,
        tablename,
        indexname,
        pg_size_pretty(pg_relation_size(indexrelid)) as current_size,
        idx_scan,
        idx_tup_read,
        idx_tup_fetch,
        CASE 
            WHEN idx_scan = 0 THEN 'UNUSED - Consider dropping'
            WHEN idx_scan < 10 THEN 'RARELY USED - Review necessity'
            WHEN pg_relation_size(indexrelid) > 100*1024*1024 AND idx_scan < 1000 THEN 'LARGE & UNDERUSED - Consider optimization'
            ELSE 'ACTIVELY USED'
        END as maintenance_recommendation,
        CASE 
            WHEN pg_relation_size(indexrelid) > 1024*1024*1024 THEN 'Consider REINDEX for large index'
            WHEN pg_relation_size(indexrelid) > 100*1024*1024 THEN 'Monitor for bloat'
            ELSE 'No immediate action needed'
        END as size_recommendation
    FROM pg_stat_user_indexes
    ORDER BY pg_relation_size(indexrelid) DESC
    LIMIT 20;
    """
    
    result = await execute_query(query)
    return json.dumps([dict(row) for row in result], default=str, indent=2)

@mcp.tool()
async def PostgreSQL_non_autovacuum_friendly_datatypes() -> str:
    """Identify tables with data types that may affect autovacuum efficiency."""
    query = """
    SELECT 
        t.schemaname,
        t.tablename,
        a.attname as column_name,
        ty.typname as data_type,
        CASE ty.typname
            WHEN 'text' THEN 'Variable length - may cause bloat'
            WHEN 'varchar' THEN 'Variable length - monitor for bloat'
            WHEN 'bytea' THEN 'Binary data - high TOAST usage likely'
            WHEN 'json' THEN 'JSON data - high TOAST usage likely'
            WHEN 'jsonb' THEN 'JSONB data - high TOAST usage likely'
            WHEN 'xml' THEN 'XML data - high TOAST usage likely'
            ELSE 'Standard type'
        END as autovacuum_impact,
        pg_size_pretty(pg_total_relation_size(c.oid)) as table_size,
        t.n_tup_upd,
        t.n_tup_del
    FROM pg_stat_user_tables t
    JOIN pg_class c ON c.relname = t.tablename 
    JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = t.schemaname
    JOIN pg_attribute a ON a.attrelid = c.oid 
    JOIN pg_type ty ON ty.oid = a.atttypid
    WHERE a.attnum > 0 
      AND NOT a.attisdropped
      AND ty.typname IN ('text', 'varchar', 'bytea', 'json', 'jsonb', 'xml')
      AND (t.n_tup_upd > 1000 OR t.n_tup_del > 1000)
    ORDER BY pg_total_relation_size(c.oid) DESC, t.n_tup_upd + t.n_tup_del DESC
    LIMIT 25;
    """
    
    result = await execute_query(query)
    return json.dumps([dict(row) for row in result], default=str, indent=2)

@mcp.tool()
async def PostgreSQL_query_cancellation_analysis() -> str:
    """Detect and analyze frequent query cancellations and their causes."""
    query = """
    SELECT 
        datname,
        usename,
        application_name,
        client_addr,
        state,
        wait_event_type,
        wait_event,
        query_start,
        state_change,
        extract(epoch from (now() - query_start)) as query_duration_seconds,
        extract(epoch from (now() - state_change)) as time_in_current_state,
        CASE 
            WHEN state = 'active' AND extract(epoch from (now() - query_start)) > 300 THEN 'LONG RUNNING'
            WHEN wait_event_type = 'Lock' THEN 'WAITING FOR LOCK'
            WHEN wait_event_type = 'IO' THEN 'IO INTENSIVE'
            WHEN state = 'idle in transaction' AND extract(epoch from (now() - state_change)) > 60 THEN 'IDLE IN TRANSACTION'
            ELSE 'NORMAL'
        END as potential_cancellation_risk,
        left(query, 100) as query_preview
    FROM pg_stat_activity 
    WHERE state NOT IN ('idle') 
      AND pid != pg_backend_pid()
    ORDER BY query_duration_seconds DESC
    LIMIT 20;
    """
    
    result = await execute_query(query)
    return json.dumps([dict(row) for row in result], default=str, indent=2)

@mcp.tool()
async def PostgreSQL_temporary_objects_usage() -> str:
    """Analyze temporary table and file usage patterns."""
    query = """
    SELECT 
        datname,
        temp_files,
        temp_bytes,
        pg_size_pretty(temp_bytes) as temp_bytes_formatted,
        CASE 
            WHEN temp_bytes > 1024*1024*1024 THEN 'HIGH - Review work_mem settings'
            WHEN temp_bytes > 100*1024*1024 THEN 'MODERATE - Monitor query complexity'
            WHEN temp_bytes > 0 THEN 'LOW - Normal usage'
            ELSE 'NONE'
        END as temp_usage_assessment,
        CASE 
            WHEN temp_files > 1000 THEN 'Many temp files - increase work_mem'
            WHEN temp_files > 100 THEN 'Moderate temp file usage'
            WHEN temp_files > 0 THEN 'Some temp files created'
            ELSE 'No temp files'
        END as temp_files_recommendation
    FROM pg_stat_database 
    WHERE datname NOT IN ('template0', 'template1')
    ORDER BY temp_bytes DESC;
    """
    
    result = await execute_query(query)
    return json.dumps([dict(row) for row in result], default=str, indent=2)

@mcp.tool()
async def PostgreSQL_wal_segment_recycling_analysis() -> str:
    """Detect excessive WAL segment recycling that might indicate tuning issues."""
    query = """
    SELECT 
        name,
        setting,
        unit,
        CASE name
            WHEN 'wal_buffers' THEN 
                CASE 
                    WHEN setting::int < 16384 THEN 'Consider increasing for write-heavy workloads'
                    ELSE 'Adequate setting'
                END
            WHEN 'checkpoint_completion_target' THEN 
                CASE 
                    WHEN setting::numeric < 0.7 THEN 'Consider increasing to 0.8-0.9'
                    ELSE 'Good setting'
                END
            WHEN 'max_wal_size' THEN 
                CASE 
                    WHEN setting::int < 1024 THEN 'May be too low for write-heavy workloads'
                    ELSE 'Reasonable setting'
                END
            ELSE 'Review based on workload'
        END as tuning_recommendation
    FROM pg_settings 
    WHERE name IN (
        'wal_buffers', 
        'checkpoint_completion_target', 
        'max_wal_size', 
        'min_wal_size',
        'checkpoint_timeout',
        'wal_writer_delay'
    )
    
    UNION ALL
    
    SELECT 
        'Current WAL Stats' as name,
        'N/A' as setting,
        'info' as unit,
        'Monitor pg_stat_wal for detailed WAL activity' as tuning_recommendation;
    """
    
    result = await execute_query(query)
    return json.dumps([dict(row) for row in result], default=str, indent=2)

@mcp.tool()
async def PostgreSQL_maintenance_window_activity() -> str:
    """Analyze database activity patterns during typical maintenance windows."""
    query = """
    SELECT 
        extract(hour from now()) as current_hour,
        count(*) as active_connections,
        count(*) FILTER (WHERE state = 'active') as active_queries,
        count(*) FILTER (WHERE state = 'idle in transaction') as idle_in_transaction,
        count(*) FILTER (WHERE wait_event_type IS NOT NULL) as waiting_queries,
        CASE 
            WHEN extract(hour from now()) BETWEEN 1 AND 5 THEN 'GOOD MAINTENANCE WINDOW'
            WHEN extract(hour from now()) BETWEEN 22 AND 24 OR extract(hour from now()) BETWEEN 0 AND 1 THEN 'POSSIBLE MAINTENANCE WINDOW'
            ELSE 'ACTIVE HOURS - AVOID MAINTENANCE'
        END as maintenance_window_suitability,
        array_agg(DISTINCT application_name) FILTER (WHERE application_name IS NOT NULL) as active_applications
    FROM pg_stat_activity 
    WHERE pid != pg_backend_pid()
    GROUP BY extract(hour from now())
    
    UNION ALL
    
    SELECT 
        NULL as current_hour,
        NULL as active_connections,
        NULL as active_queries, 
        NULL as idle_in_transaction,
        NULL as waiting_queries,
        'Recommended maintenance windows: 1-5 AM local time' as maintenance_window_suitability,
        ARRAY['Check your specific application patterns'] as active_applications;
    """
    
    result = await execute_query(query)
    return json.dumps([dict(row) for row in result], default=str, indent=2)

@mcp.tool()
async def PostgreSQL_long_execution_triggers() -> str:
    """Detect triggers with long average execution times that may impact performance."""
    query = """
    SELECT 
        schemaname,
        tablename, 
        'Trigger analysis requires pg_stat_user_functions extension' as info,
        'Enable track_functions = all in postgresql.conf' as recommendation,
        'Then query pg_stat_user_functions for trigger performance' as next_step
    FROM pg_stat_user_tables 
    WHERE n_tup_ins + n_tup_upd + n_tup_del > 1000
    LIMIT 10
    
    UNION ALL
    
    SELECT 
        t.trigger_schema as schemaname,
        t.event_object_table as tablename,
        'Trigger: ' || t.trigger_name as info,
        'Event: ' || string_agg(t.event_manipulation, ', ') as recommendation,
        'Action: ' || t.action_statement as next_step
    FROM information_schema.triggers t
    GROUP BY t.trigger_schema, t.event_object_table, t.trigger_name, t.action_statement
    ORDER BY t.trigger_schema, t.event_object_table;
    """
    
    result = await execute_query(query)
    return json.dumps([dict(row) for row in result], default=str, indent=2)

@mcp.tool()
async def PostgreSQL_prepared_transaction_retention() -> str:
    """Analyze prepared transaction retention times and potential issues."""
    query = """
    SELECT 
        gid,
        prepared,
        owner,
        database,
        extract(epoch from (now() - prepared))/3600 as hours_prepared,
        CASE 
            WHEN extract(epoch from (now() - prepared)) > 86400 THEN 'CRITICAL - Over 24 hours old'
            WHEN extract(epoch from (now() - prepared)) > 3600 THEN 'WARNING - Over 1 hour old' 
            WHEN extract(epoch from (now() - prepared)) > 300 THEN 'ATTENTION - Over 5 minutes old'
            ELSE 'RECENT'
        END as retention_status,
        'COMMIT PREPARED \'' || gid || '\'; or ROLLBACK PREPARED \'' || gid || '\';' as suggested_action
    FROM pg_prepared_xacts
    ORDER BY prepared ASC;
    """
    
    try:
        result = await execute_query(query)
        return json.dumps([dict(row) for row in result], default=str, indent=2)
    except Exception as e:
        return json.dumps({"info": "No prepared transactions found or feature not available", "details": str(e)}, indent=2)

@mcp.tool()
async def PostgreSQL_toast_table_excessive_usage() -> str:
    """Identify tables with excessively large TOAST table sizes relative to main table."""
    query = """
    SELECT 
        n.nspname as schema_name,
        c.relname as table_name,
        pg_size_pretty(pg_relation_size(c.oid)) as main_table_size,
        pg_size_pretty(pg_relation_size(t.oid)) as toast_table_size,
        pg_relation_size(t.oid) as toast_size_bytes,
        pg_relation_size(c.oid) as main_size_bytes,
        CASE 
            WHEN pg_relation_size(c.oid) > 0 THEN 
                round(pg_relation_size(t.oid)::numeric / pg_relation_size(c.oid), 2)
            ELSE 0
        END as toast_to_main_ratio,
        CASE 
            WHEN pg_relation_size(t.oid) > pg_relation_size(c.oid) * 2 THEN 'EXCESSIVE - Review large columns'
            WHEN pg_relation_size(t.oid) > pg_relation_size(c.oid) THEN 'HIGH - Monitor storage usage'
            WHEN pg_relation_size(t.oid) > 0 THEN 'NORMAL - TOAST in use'
            ELSE 'NONE - No TOAST usage'
        END as toast_assessment
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_class t ON t.oid = c.reltoastrelid
    WHERE c.relkind = 'r' 
      AND n.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
      AND c.reltoastrelid != 0
    ORDER BY toast_to_main_ratio DESC NULLS LAST
    LIMIT 20;
    """
    
    result = await execute_query(query)
    return json.dumps([dict(row) for row in result], default=str, indent=2)

@mcp.tool()
async def PostgreSQL_plan_invalidation_analysis() -> str:
    """Analyze common reasons for query plan invalidation and cache misses."""
    query = """
    SELECT 
        'Plan Cache Analysis' as analysis_type,
        setting as current_setting,
        CASE name 
            WHEN 'plan_cache_mode' THEN 'Controls prepared statement caching behavior'
            ELSE 'Related to plan caching'
        END as description
    FROM pg_settings 
    WHERE name IN ('plan_cache_mode')
    
    UNION ALL
    
    SELECT 
        'Statement Statistics' as analysis_type,
        'Available via pg_stat_statements' as current_setting,
        'Monitor calls vs plan_time for cache effectiveness' as description
        
    UNION ALL
    
    SELECT 
        'Cache Recommendations' as analysis_type,
        'Enable pg_stat_statements' as current_setting,
        'Track prepare/execute patterns for optimization' as description;
    """
    
    result = await execute_query(query)
    return json.dumps([dict(row) for row in result], default=str, indent=2)

@mcp.tool()
async def PostgreSQL_orphaned_prepared_transactions() -> str:
    """Detect large numbers of orphaned prepared transactions that need cleanup."""
    query = """
    SELECT 
        count(*) as total_prepared_transactions,
        count(*) FILTER (WHERE extract(epoch from (now() - prepared)) > 3600) as old_transactions_1h,
        count(*) FILTER (WHERE extract(epoch from (now() - prepared)) > 86400) as old_transactions_24h,
        count(*) FILTER (WHERE extract(epoch from (now() - prepared)) > 604800) as old_transactions_7d,
        CASE 
            WHEN count(*) FILTER (WHERE extract(epoch from (now() - prepared)) > 86400) > 0 THEN 
                'CRITICAL - Clean up old prepared transactions immediately'
            WHEN count(*) FILTER (WHERE extract(epoch from (now() - prepared)) > 3600) > 10 THEN 
                'WARNING - Monitor prepared transaction cleanup'
            WHEN count(*) > 50 THEN 
                'ATTENTION - High number of prepared transactions'
            ELSE 'NORMAL - Prepared transaction levels acceptable'
        END as cleanup_priority,
        'SELECT gid FROM pg_prepared_xacts WHERE extract(epoch from (now() - prepared)) > 86400;' as cleanup_query
    FROM pg_prepared_xacts;
    """
    
    try:
        result = await execute_query(query)
        return json.dumps([dict(row) for row in result], default=str, indent=2)
    except Exception as e:
        return json.dumps({"info": "No prepared transactions or feature not available", "details": str(e)}, indent=2)

@mcp.tool()
async def PostgreSQL_connection_churn_analysis() -> str:
    """Analyze connection churn and idle times per user/application."""
    query = """
    SELECT 
        usename,
        application_name,
        client_addr,
        count(*) as connection_count,
        count(*) FILTER (WHERE state = 'idle') as idle_connections,
        count(*) FILTER (WHERE state = 'active') as active_connections,
        count(*) FILTER (WHERE state = 'idle in transaction') as idle_in_transaction,
        avg(extract(epoch from (now() - backend_start))) as avg_connection_age_seconds,
        max(extract(epoch from (now() - backend_start))) as oldest_connection_seconds,
        CASE 
            WHEN count(*) FILTER (WHERE state = 'idle in transaction') > 5 THEN 
                'HIGH RISK - Many idle in transaction connections'
            WHEN avg(extract(epoch from (now() - backend_start))) < 300 THEN 
                'HIGH CHURN - Frequent reconnections detected'
            WHEN count(*) > 20 THEN 
                'HIGH COUNT - Consider connection pooling'
            ELSE 'NORMAL - Connection pattern looks healthy'
        END as connection_assessment,
        round(avg(extract(epoch from (now() - backend_start)))/60, 2) as avg_age_minutes
    FROM pg_stat_activity
    WHERE pid != pg_backend_pid()
    GROUP BY usename, application_name, client_addr
    ORDER BY connection_count DESC, avg_connection_age_seconds ASC
    LIMIT 15;
    """
    
    result = await execute_query(query)
    return json.dumps([dict(row) for row in result], default=str, indent=2)

@mcp.tool()
async def PostgreSQL_statistics_reset_frequency() -> str:
    """Detect frequent planner statistics resets that might indicate issues."""
    query = """
    SELECT 
        datname,
        stats_reset,
        extract(epoch from (now() - stats_reset))/86400 as days_since_reset,
        CASE 
            WHEN stats_reset IS NULL THEN 'NEVER RESET - Statistics may be stale'
            WHEN extract(epoch from (now() - stats_reset)) < 86400 THEN 'RECENT RESET - Within 24 hours'
            WHEN extract(epoch from (now() - stats_reset)) < 604800 THEN 'MODERATE - Within 1 week' 
            WHEN extract(epoch from (now() - stats_reset)) < 2592000 THEN 'OLD - Within 1 month'
            ELSE 'VERY OLD - Over 1 month ago'
        END as reset_status,
        CASE 
            WHEN extract(epoch from (now() - stats_reset)) < 3600 THEN 
                'WARNING - Very recent reset, investigate cause'
            WHEN stats_reset IS NULL THEN 
                'CONSIDER - Manual statistics reset for better planning'
            ELSE 'OK - Normal statistics age'
        END as recommendation
    FROM pg_stat_database
    WHERE datname NOT IN ('template0', 'template1')
    ORDER BY stats_reset DESC NULLS LAST;
    """
    
    result = await execute_query(query)
    return json.dumps([dict(row) for row in result], default=str, indent=2)

@mcp.tool()
async def PostgreSQL_unlogged_tables_analysis() -> str:
    """Analyze usage of unlogged tables and their implications."""
    query = """
    SELECT 
        n.nspname as schema_name,
        c.relname as table_name,
        c.relpersistence,
        CASE c.relpersistence
            WHEN 'u' THEN 'UNLOGGED - Not crash-safe'
            WHEN 'p' THEN 'PERMANENT - Standard table'
            WHEN 't' THEN 'TEMPORARY - Session-specific'
            ELSE 'UNKNOWN'
        END as persistence_type,
        pg_size_pretty(pg_total_relation_size(c.oid)) as total_size,
        pg_total_relation_size(c.oid) as size_bytes,
        s.n_tup_ins,
        s.n_tup_upd,
        s.n_tup_del,
        CASE 
            WHEN c.relpersistence = 'u' AND pg_total_relation_size(c.oid) > 1024*1024*1024 THEN 
                'RISK - Large unlogged table, data loss on crash'
            WHEN c.relpersistence = 'u' AND (s.n_tup_ins + s.n_tup_upd + s.n_tup_del) > 10000 THEN 
                'ACTIVE - High activity on unlogged table'
            WHEN c.relpersistence = 'u' THEN 
                'CAUTION - Unlogged table in use'
            ELSE 'NORMAL - Standard logged table'
        END as risk_assessment
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_stat_user_tables s ON s.schemaname = n.nspname AND s.relname = c.relname
    WHERE c.relkind = 'r'
      AND n.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
    ORDER BY 
        CASE WHEN c.relpersistence = 'u' THEN 0 ELSE 1 END,
        pg_total_relation_size(c.oid) DESC
    LIMIT 25;
    """
    
    result = await execute_query(query)
    return json.dumps([dict(row) for row in result], default=str, indent=2)

@mcp.tool()
async def PostgreSQL_foreign_key_orphaned_references() -> str:
    """Find tables with foreign keys that might reference deleted rows (integrity issues)."""
    query = """
    SELECT 
        tc.table_schema,
        tc.table_name,
        tc.constraint_name,
        kcu.column_name,
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column_name,
        'Check for orphaned references' as recommendation,
        'SELECT COUNT(*) FROM ' || tc.table_schema || '.' || tc.table_name || 
        ' t1 LEFT JOIN ' || ccu.table_schema || '.' || ccu.table_name || 
        ' t2 ON t1.' || kcu.column_name || ' = t2.' || ccu.column_name || 
        ' WHERE t2.' || ccu.column_name || ' IS NULL AND t1.' || kcu.column_name || ' IS NOT NULL;' as validation_query
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu 
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
    JOIN information_schema.constraint_column_usage ccu 
        ON ccu.constraint_name = tc.constraint_name
        AND ccu.table_schema = tc.table_schema
    WHERE tc.constraint_type = 'FOREIGN KEY'
      AND tc.table_schema NOT IN ('information_schema', 'pg_catalog')
    ORDER BY tc.table_schema, tc.table_name, tc.constraint_name
    LIMIT 20;
    """
    
    result = await execute_query(query)
    return json.dumps([dict(row) for row in result], default=str, indent=2)

@mcp.tool()
async def PostgreSQL_parameter_sniffing_detection() -> str:
    """Detect potential parameter sniffing issues in prepared statements."""
    query = """
    SELECT 
        'Parameter Analysis' as analysis_category,
        'Enable pg_stat_statements for detailed query analysis' as recommendation,
        setting as plan_cache_mode_setting
    FROM pg_settings 
    WHERE name = 'plan_cache_mode'
    
    UNION ALL
    
    SELECT 
        'Auto Explain Setup' as analysis_category,
        'Consider enabling auto_explain for plan analysis' as recommendation,
        'Load auto_explain in shared_preload_libraries' as plan_cache_mode_setting
        
    UNION ALL
    
    SELECT 
        'Query Monitoring' as analysis_category,
        'Monitor for plans that change significantly with parameters' as recommendation,
        'Check execution time variance in pg_stat_statements' as plan_cache_mode_setting
        
    UNION ALL
    
    SELECT 
        'Statement Preparation' as analysis_category,
        'Review PREPARE/EXECUTE patterns vs direct queries' as recommendation,
        'Consider DEALLOCATE for parameter-sensitive queries' as plan_cache_mode_setting;
    """
    
    result = await execute_query(query)
    return json.dumps([dict(row) for row in result], default=str, indent=2)

# Create Tools

class ColumnDefinition(BaseModel):
    """Definition for a table column."""
    name: str = Field(description="Name of the column")
    data_type: str = Field(description="PostgreSQL data type (e.g., INTEGER, VARCHAR(255), TEXT)")
    is_primary_key: bool = Field(default=False, description="Whether this column is a primary key")
    is_nullable: bool = Field(default=True, description="Whether the column can contain NULL values")
    default_value: Optional[str] = Field(default=None, description="Default value expression")

@mcp.tool()
async def PostgreSQL_create_table(
    table_name: str, 
    columns: List[ColumnDefinition], 
    ctx: Context,
    schema_name: str = "public"
) -> str:
    """
    Create a new table with specified columns.
    
    Args:
        table_name: Name of the table
        columns: List of column definitions
        schema_name: Schema to create the table in (default: public)
    """
    full_table_name = f"{schema_name}.{table_name}"
    await ctx.info(f"Creating table {full_table_name} with {len(columns)} columns...")

    if not table_name.replace('_', '').isalnum():
        raise ValueError("Table name must be alphanumeric")
    if not schema_name.replace('_', '').isalnum():
        raise ValueError("Schema name must be alphanumeric")

    col_defs = []
    for col in columns:
        if not col.name.replace('_', '').isalnum():
            raise ValueError(f"Column name '{col.name}' is invalid")
        
        def_part = f"{col.name} {col.data_type}"
        if col.is_primary_key:
            def_part += " PRIMARY KEY"
        elif not col.is_nullable:
            def_part += " NOT NULL"
        
        if col.default_value:
            def_part += f" DEFAULT {col.default_value}"
        
        col_defs.append(def_part)
    
    cols_sql = ", ".join(col_defs)
    query = f"CREATE TABLE {full_table_name} ({cols_sql})"
    
    await execute_non_query(query)
    
    await ctx.info(f"Successfully created table {full_table_name}")
    return f"Table '{full_table_name}' created successfully."

# Main entry point - ensure server runs correctly
if __name__ == "__main__":

    mcp.run(transport='stdio')
