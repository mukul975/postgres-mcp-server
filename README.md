# PostgreSQL MCP Server

A comprehensive Model Context Protocol (MCP) server for PostgreSQL database integration. This server provides extensive tools, resources, and prompts for complete PostgreSQL database management through AI assistants like Claude.

## Features

This server offers over 200 specialized PostgreSQL tools covering all aspects of database administration, monitoring, and optimization.

### 🔧 Core Database Operations
- **Database Management**: Create, list, and manage databases
- **Schema Operations**: Create, drop, and manage schemas
- **Table Operations**: List, describe, analyze tables with detailed statistics
- **Query Execution**: Safe SELECT queries and controlled UPDATE/INSERT/DELETE operations
- **Index Management**: Create, drop, analyze index usage and effectiveness

### 📊 Advanced Analytics & Monitoring
- **Performance Analysis**: Query performance, slow query analysis, execution plan examination
- **Resource Monitoring**: Buffer cache analysis, memory usage, I/O statistics
- **Lock Analysis**: Deadlock detection, lock contention monitoring, blocking queries
- **Replication Monitoring**: Replication status, lag analysis, slot management
- **Vacuum & Maintenance**: Autovacuum monitoring, table bloat analysis, maintenance recommendations

### 🛡️ Security & Administration
- **User Management**: Create, drop, manage users and roles with detailed permissions
- **Privilege Management**: Grant/revoke permissions, security auditing
- **Connection Management**: Monitor active connections, analyze connection patterns
- **Backup Analysis**: Backup status monitoring and recovery readiness assessment

### 🚀 Performance Optimization
- **Index Optimization**: Unused index detection, redundancy analysis, effectiveness metrics
- **Query Optimization**: Plan cache analysis, query complexity assessment
- **Buffer Pool Tuning**: Hit ratio analysis, cache efficiency metrics
- **Configuration Tuning**: Parameter recommendations and system health checks

### 📈 Advanced Diagnostics
- **Transaction Analysis**: Long-running transactions, wraparound monitoring
- **Checkpoint Monitoring**: Checkpoint efficiency and timing analysis
- **WAL Analysis**: Write-ahead log generation and archiving status
- **Extension Management**: Installed extensions and usage analysis

### 📊 Resources
- **postgres://tables/{schema}**: Get table information for a schema
- **postgres://table/{schema}/{table_name}**: Get detailed table structure

### 💬 Prompts
- **analyze_table**: Generate analysis prompts for database tables
- **query_builder**: Help build SQL queries with proper guidance

## Installation

1. Create a virtual environment (recommended):
```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up your database connection:
```bash
# Copy the example environment file
copy .env.example .env

# Edit .env with your PostgreSQL connection details
DATABASE_URL=postgresql://username:password@localhost:5432/database_name
```

## Usage

### Running the Server

#### Direct execution:
```bash
python postgres_server.py
```

#### With MCP development tools (if mcp CLI is installed):
```bash
mcp dev postgres_server.py
```

### Configuration with Claude Desktop

Add the following to your Claude Desktop configuration file (`claude_desktop_config.json`):

**Windows:**
```json
{
  "mcpServers": {
    "postgres": {
      "command": "python",
      "args": [
        "postgres_server.py"
      ],
      "cwd": "C:\\path\\to\\postgres-mcp-server",
      "env": {
        "DATABASE_URL": "postgresql://username:password@localhost:5432/database_name"
      }
    }
  }
}
```

**macOS/Linux:**
```json
{
  "mcpServers": {
    "postgres": {
      "command": "python",
      "args": [
        "postgres_server.py"
      ],
      "cwd": "/absolute/path/to/postgres-mcp-server",
      "env": {
        "DATABASE_URL": "postgresql://username:password@localhost:5432/database_name"
      }
    }
  }
}
```

## Tool Categories

### Database Structure & Metadata
- `PostgreSQL_list_tables` - List all tables in a schema
- `PostgreSQL_describe_table` - Get detailed table column information
- `PostgreSQL_list_schemas` - List all database schemas
- `PostgreSQL_list_indexes` - Show indexes for a table
- `PostgreSQL_get_foreign_keys` - Display foreign key relationships
- `PostgreSQL_get_table_constraints` - Show all table constraints

### Query Execution & Analysis
- `PostgreSQL_execute_select_query` - Execute SELECT queries safely
- `PostgreSQL_execute_update_query` - Execute INSERT/UPDATE/DELETE queries
- `PostgreSQL_explain_query` - Show query execution plans
- `PostgreSQL_get_slow_queries` - Analyze slow running queries
- `PostgreSQL_analyze_query_complexity` - Assess query performance characteristics

### Performance Monitoring
- `PostgreSQL_get_cache_hit_ratios` - Buffer cache performance metrics
- `PostgreSQL_get_index_usage_stats` - Index utilization analysis
- `PostgreSQL_get_table_statistics` - Comprehensive table statistics
- `PostgreSQL_check_long_running_queries` - Monitor active long queries
- `PostgreSQL_get_blocking_locks` - Identify blocking transactions

### Maintenance & Optimization
- `PostgreSQL_get_bloated_tables` - Find tables needing maintenance
- `PostgreSQL_get_unused_indexes` - Identify unused indexes
- `PostgreSQL_vacuum_analyze_table` - Perform table maintenance
- `PostgreSQL_get_autovacuum_activity` - Monitor autovacuum operations
- `PostgreSQL_analyze_database` - Update table statistics

### Security & User Management
- `PostgreSQL_create_user` - Create database users
- `PostgreSQL_grant_privileges` - Assign user permissions
- `PostgreSQL_get_active_connections` - Monitor user connections
- `PostgreSQL_list_users_and_roles` - Display all users and roles
- `PostgreSQL_get_table_permissions` - Show table-level permissions

### Replication & Backup
- `PostgreSQL_get_replication_status` - Monitor replication health
- `PostgreSQL_get_replication_slots` - Manage replication slots
- `PostgreSQL_get_backup_status` - Check backup configuration
- `PostgreSQL_get_wal_stats` - Write-ahead log statistics

## Example Interactions

### Database Exploration
```
"Show me all tables in the public schema"
"What's the structure of the users table?"
"List all foreign key relationships for the orders table"
```

### Performance Analysis
```
"Analyze the performance of my database and suggest optimizations"
"Show me the slowest queries from the last hour"
"Which indexes are not being used and can be dropped?"
"What's the buffer cache hit ratio?"
```

### Monitoring & Diagnostics
```
"Are there any blocking queries right now?"
"Show me tables with high bloat that need maintenance"
"Check the replication lag status"
"What connections are currently active?"
```

### Maintenance Operations
```
"Run VACUUM ANALYZE on the products table"
"Update statistics for the entire database"
"Show me autovacuum activity for today"
```

### Security Auditing
```
"Show me all users and their privileges"
"What permissions does user 'app_user' have?"
"List all active database connections"
```

## Advanced Use Cases

### Database Health Assessment
The server can perform comprehensive health checks including:
- Buffer pool efficiency analysis
- Index usage optimization
- Table bloat assessment
- Replication lag monitoring
- Connection pool analysis

### Performance Tuning
- Identify slow queries and suggest optimizations
- Analyze execution plans for query improvement
- Monitor resource utilization patterns
- Recommend index strategies

### Proactive Monitoring
- Set up monitoring for long-running transactions
- Track database growth trends
- Monitor backup and recovery readiness
- Analyze connection patterns and bottlenecks

## Security Features

- **Query Validation**: Only allows specific query types (SELECT for reads, INSERT/UPDATE/DELETE for writes)
- **Connection Pooling**: Efficient database connection management with proper cleanup
- **Parameter Sanitization**: Prevents SQL injection through parameterized queries
- **Error Handling**: Comprehensive error handling with informative messages
- **Logging**: Contextual logging for monitoring and debugging
- **Environment Variables**: Secure credential management
- **Permission Checks**: Respects database user permissions and access controls
