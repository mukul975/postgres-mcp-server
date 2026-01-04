# 🔄 PostgreSQL MCP Server – AI-Powered PostgreSQL Management & Monitoring

> A powerful, AI-integrated PostgreSQL Model Context Protocol (MCP) server for **automated database operations, monitoring, security, diagnostics, and optimization**. Seamlessly manage PostgreSQL with tools and prompts designed for **AI assistants like Claude and ChatGPT**.

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue.svg)](https://python.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Compatible-blue?logo=postgresql)](https://www.postgresql.org/)
[![MCP](https://img.shields.io/badge/MCP%20Server-PostgreSQL%20Integration-brightgreen)]()

**GitHub Repository**: [https://github.com/mukul975/postgres-mcp-server.git](https://github.com/mukul975/postgres-mcp-server.git)

---

## 📌 Why Use This?

* 🔍 Explore, diagnose & optimize PostgreSQL databases with **over 200 specialized tools**.
* 💬 Interact naturally using **AI prompts** for SQL generation, health checks, and performance reviews.
* 🛡️ Built-in security, permission validation, and error handling.
* ⚙️ Developer-ready integration for **Claude**, **ChatGPT**, and custom agents.

---

## 🔑 Features at a Glance

### 🔧 Core Database Operations

* ✅ Create/list databases & schemas
* ✅ Describe tables, analyze indexes
* ✅ Safe SELECT, UPDATE, DELETE execution

### 📊 Monitoring & Performance

* 📈 Query performance & execution plan insights
* 🔒 Lock analysis, replication lag checks
* 🚀 Index, query, and buffer pool optimization

### 🛡️ Security & Access Control

* 👥 User creation, role auditing
* 🔐 Privilege granting & connection monitoring

### ⚙️ Diagnostic Utilities

* 🧪 Table bloat check, autovacuum insights
* 💽 WAL logs, checkpoints, long transaction alerts

---

## 📚 Tool Categories

To help you navigate, tools are grouped under the following main categories:

- 🔧 General Database Operations
- 👥 User & Role Management
- 📈 Performance Monitoring & Tuning
- 🔁 Replication & Backup
- 🔒 Lock & Concurrency Management
- ✅ Constraint & Integrity Management
- 📊 Resource and Activity Monitoring
- 🧰 Index and Table Maintenance
- 🔬 Advanced Analysis & Diagnostics

---

## 🚀 Quick Start

### 1. Installation

```bash
python -m venv venv
source venv/bin/activate   # or venv\Scripts\activate on Windows
pip install -r requirements.txt
```

### 2. Configuration

```bash
cp .env.example .env  # or use copy on Windows
# Update with:
DATABASE_URL=postgresql://username:password@localhost:5432/db
```

### 3. Run the Server

```bash
python postgres_server.py
```

> ✅ For **MCP CLI users**:

```bash
mcp dev postgres_server.py
```

---

## 🧠 Integrate with Claude Desktop

#### Example config (`claude_desktop_config.json`):

<details>
<summary>Windows</summary>

```json
{
  "mcpServers": {
    "postgres": {
      "command": "python",
      "args": ["postgres_server.py"],
      "cwd": "C:\\path\\to\\postgres-mcp-server",
      "env": {
        "DATABASE_URL": "postgresql://username:password@localhost:5432/database"
      }
    }
  }
}
```

</details>

<details>
<summary>macOS/Linux</summary>

```json
{
  "mcpServers": {
    "postgres": {
      "command": "python",
      "args": ["postgres_server.py"],
      "cwd": "/absolute/path/to/postgres-mcp-server",
      "env": {
        "DATABASE_URL": "postgresql://username:password@localhost:5432/database"
      }
    }
  }
}
```

</details>

---

## 🔍 Tool Search & Complete Reference (238 Tools)

### 🔎 Tool Search Bar

> **Quick Tool Finder**: Use Ctrl+F (or Cmd+F) to search for specific tools by name, category, or functionality.

### 📊 Tools by Category Overview

| Category | Count | Description |
|----------|-------|-------------|
| 🧱 **Core Database** | 26 | Basic database operations, schema management |
| 👥 **User & Security** | 18 | User management, roles, permissions |
| 📈 **Performance** | 45 | Monitoring, analysis, optimization |
| 🔒 **Locks & Concurrency** | 22 | Lock analysis, blocking queries, deadlocks |
| 🛠️ **Maintenance** | 28 | VACUUM, ANALYZE, table maintenance |
| 📊 **Index Management** | 15 | Index creation, analysis, optimization |
| 🔄 **Replication & Backup** | 20 | Replication monitoring, backup status |
| 📋 **Table Operations** | 25 | Table statistics, constraints, data |
| 📦 **Extensions & Objects** | 18 | Extensions, functions, triggers |
| ⚙️ **Configuration** | 12 | Settings, variables, system info |
| 🧪 **Advanced Analysis** | 29 | Deep diagnostics, predictions, recommendations |

---

## 📚 Complete Tool Reference

### 🧱 Core Database Operations (25 tools)

<details>
<summary>Click to expand Core Database tools</summary>

- `PostgreSQL_analyze_database` - Run ANALYZE on entire database
- `PostgreSQL_create_schema` - Create new database schema
- `PostgreSQL_describe_table` - Get detailed table information
- `PostgreSQL_drop_schema` - Drop database schema
- `PostgreSQL_execute_select_query` - Execute SELECT queries
- `PostgreSQL_execute_update_query` - Execute INSERT/UPDATE/DELETE
- `PostgreSQL_explain_query` - Show query execution plan
- `PostgreSQL_get_database_config` - Get database configuration
- `PostgreSQL_get_database_growth_trend` - Analyze database growth
- `PostgreSQL_get_database_size` - Get database size information
- `PostgreSQL_get_database_size_by_tablespace` - Size by tablespace
- `PostgreSQL_get_estimated_row_counts` - Get estimated row counts
- `PostgreSQL_get_server_version` - Get PostgreSQL version info
- `PostgreSQL_get_table_count` - Get row count of table
- `PostgreSQL_get_table_size` - Get table size information
- `PostgreSQL_get_table_size_summary` - Comprehensive table sizes
- `PostgreSQL_get_tablespace_info` - Get tablespace information
- `PostgreSQL_get_tablespace_usage` - Tablespace usage statistics
- `PostgreSQL_list_databases` - List all databases
- `PostgreSQL_list_schemas` - List all schemas
- `PostgreSQL_list_tables` - List tables in schema
- `PostgreSQL_list_views` - List views in schema
- `PostgreSQL_use_database` - Switch to different database
- `PostgreSQL_vacuum_analyze_table` - VACUUM ANALYZE table
- `PostgreSQL_kill_connection` - Terminate database connection

</details>

### 👥 User & Security Management (18 tools)

<details>
<summary>Click to expand User & Security tools</summary>

- `PostgreSQL_create_user` - Create database user/role
- `PostgreSQL_drop_user` - Drop database user/role
- `PostgreSQL_get_column_privileges` - Get column-level privileges
- `PostgreSQL_get_role_attributes` - Get role attributes and details
- `PostgreSQL_get_table_permissions` - Get table permissions
- `PostgreSQL_grant_privileges` - Grant table privileges
- `PostgreSQL_list_roles_with_login` - List roles with login capability
- `PostgreSQL_list_roles_with_superuser` - List superuser roles
- `PostgreSQL_list_users_and_roles` - List all users and roles
- `PostgreSQL_revoke_privileges` - Revoke table privileges
- `PostgreSQL_security_audit` - Basic security audit
- `PostgreSQL_get_active_connections` - Get active connections
- `PostgreSQL_get_connection_limits` - Connection limits and usage
- `PostgreSQL_get_connection_pool_stats` - Connection pool statistics
- `PostgreSQL_get_idle_connections` - Find idle connections
- `PostgreSQL_connection_churn_analysis` - Analyze connection patterns
- `PostgreSQL_get_concurrent_connection_analysis` - Concurrent connections
- `PostgreSQL_analyze_connection_pool_efficiency` - Pool efficiency

</details>

### 📈 Performance Monitoring & Analysis (45 tools)

<details>
<summary>Click to expand Performance tools</summary>

- `PostgreSQL_analyze_query_complexity` - Analyze query complexity
- `PostgreSQL_buffer_cache_hit_ratio` - Cache hit ratios
- `PostgreSQL_check_long_running_queries` - Find long-running queries
- `PostgreSQL_get_active_transactions` - Get active transactions
- `PostgreSQL_get_buffer_cache_contents` - Buffer cache contents
- `PostgreSQL_get_buffer_hit_ratios_detailed` - Detailed hit ratios
- `PostgreSQL_get_cache_hit_ratio` - Cache hit ratios
- `PostgreSQL_get_cache_hit_ratios` - Buffer and index cache ratios
- `PostgreSQL_get_checkpoint_analysis` - Checkpoint analysis
- `PostgreSQL_get_checkpoint_info` - Checkpoint information
- `PostgreSQL_get_checkpoint_stats` - Checkpoint statistics
- `PostgreSQL_get_column_statistics` - Column statistics
- `PostgreSQL_get_long_running_transactions` - Long transactions
- `PostgreSQL_get_memory_context_analysis` - Memory context analysis
- `PostgreSQL_get_memory_usage_stats` - Memory usage statistics
- `PostgreSQL_get_query_error_analysis` - Query error analysis
- `PostgreSQL_get_query_plan_cache_analysis` - Query plan cache
- `PostgreSQL_get_query_plan_cache_stats` - Plan cache statistics
- `PostgreSQL_get_query_plans` - Query execution plans
- `PostgreSQL_get_query_runtime_distribution` - Query runtime stats
- `PostgreSQL_get_slow_queries` - Get slow queries
- `PostgreSQL_get_slow_query_patterns` - Slow query patterns
- `PostgreSQL_get_slow_query_statements` - Slow query statements
- `PostgreSQL_get_table_access_patterns` - Table access patterns
- `PostgreSQL_get_table_io_stats` - Table I/O statistics
- `PostgreSQL_get_table_statistics` - Table statistics
- `PostgreSQL_get_temp_file_stats` - Temporary file statistics
- `PostgreSQL_get_temp_files` - Temporary file usage
- `PostgreSQL_get_top_heavy_queries` - Top resource-heavy queries
- `PostgreSQL_get_transaction_age_monitoring` - Transaction age monitoring
- `PostgreSQL_get_wait_events` - Current wait events
- `PostgreSQL_analyze_advanced_buffer_usage` - Advanced buffer analysis
- `PostgreSQL_buffer_cache_relation_analysis` - Buffer cache per relation
- `PostgreSQL_checkpoint_activity` - Detailed checkpoint activity
- `PostgreSQL_connection_pool_stats` - Connection pool stats
- `PostgreSQL_get_connection_pool_analysis` - Connection pool analysis
- `PostgreSQL_get_performance_regression_indicators` - Performance regression
- `PostgreSQL_high_io_tables` - High I/O tables
- `PostgreSQL_memory_usage_analysis` - Memory usage analysis
- `PostgreSQL_table_io_patterns` - Table I/O patterns
- `PostgreSQL_temp_file_usage` - Temporary file usage monitoring
- `PostgreSQL_temporary_objects_usage` - Temporary objects analysis
- `PostgreSQL_active_temp_file_users` - Active temp file users
- `PostgreSQL_get_high_wait_events` - High wait events
- `PostgreSQL_query_cancellation_analysis` - Query cancellation analysis

</details>

### 🔒 Locks & Concurrency Management (22 tools)

<details>
<summary>Click to expand Lock & Concurrency tools</summary>

- `PostgreSQL_check_blocking_queries` - Get blocking queries
- `PostgreSQL_detect_conflicting_queries` - Detect conflicting queries
- `PostgreSQL_detect_foreign_key_lock_contention` - FK lock contention
- `PostgreSQL_detect_index_lock_waits` - Index lock waits
- `PostgreSQL_foreign_key_conflicts` - Foreign key conflicts
- `PostgreSQL_get_blocking_locks` - Blocking and blocked queries
- `PostgreSQL_get_lock_monitoring` - Monitor current locks
- `PostgreSQL_get_lock_statistics` - Lock statistics with wait times
- `PostgreSQL_get_lock_waits` - Current lock waits
- `PostgreSQL_get_locks_info` - Current database locks
- `PostgreSQL_deadlock_analysis` - Deadlock history and patterns
- `PostgreSQL_analyze_foreign_key_locks` - Foreign key lock analysis
- `PostgreSQL_predicate_lock_analysis` - Predicate lock analysis
- `PostgreSQL_longest_idle_transactions` - Longest idle transactions
- `PostgreSQL_long_running_prepared_transactions` - Long prepared transactions
- `PostgreSQL_orphaned_prepared_transactions` - Orphaned prepared transactions
- `PostgreSQL_prepared_transaction_retention` - Prepared transaction retention
- `PostgreSQL_get_prepared_transactions` - Get prepared transactions
- `PostgreSQL_analyze_transaction_wraparound` - Transaction wraparound
- `PostgreSQL_detect_transaction_wraparound_risk` - Wraparound risk
- `PostgreSQL_transaction_wraparound_monitoring` - Wraparound monitoring
- `PostgreSQL_vacuum_freeze_age_analysis` - Freeze age analysis

</details>

### 🛠️ Maintenance & Optimization (28 tools)

<details>
<summary>Click to expand Maintenance tools</summary>

- `PostgreSQL_analyze_autovacuum_efficiency` - Autovacuum efficiency
- `PostgreSQL_analyze_table_freeze_stats` - Table freeze statistics
- `PostgreSQL_autovacuum_tuning` - Autovacuum tuning recommendations
- `PostgreSQL_check_table_bloat` - Check table bloat
- `PostgreSQL_get_autovacuum_activity` - Autovacuum operations
- `PostgreSQL_get_autovacuum_settings` - Autovacuum settings
- `PostgreSQL_get_autovacuum_stats_per_table` - Autovacuum per table
- `PostgreSQL_get_bloated_tables` - Find bloated tables
- `PostgreSQL_get_table_bloat_estimation` - Table bloat estimation
- `PostgreSQL_get_table_fragmentation_analysis` - Table fragmentation
- `PostgreSQL_get_vacuum_analyze_recommendations` - VACUUM recommendations
- `PostgreSQL_get_vacuum_inefficiency_tables` - Inefficient VACUUM tables
- `PostgreSQL_get_vacuum_progress` - Monitor VACUUM progress
- `PostgreSQL_vacuum_analyze_table` - VACUUM ANALYZE table
- `PostgreSQL_vacuum_progress_monitoring` - VACUUM progress monitoring
- `PostgreSQL_analyze_vacuum_efficiency` - Vacuum efficiency analysis
- `PostgreSQL_detect_table_bloat_regression` - Table bloat regression
- `PostgreSQL_maintenance_window_activity` - Maintenance window analysis
- `PostgreSQL_monitor_autovacuum_progress` - Monitor autovacuum
- `PostgreSQL_non_autovacuum_friendly_datatypes` - Non-autovacuum datatypes
- `PostgreSQL_partition_maintenance` - Partition maintenance
- `PostgreSQL_vacuum_analyze_frequency_analysis` - VACUUM frequency
- `PostgreSQL_analyze_table_freeze_stats` - Freeze stats analysis
- `PostgreSQL_get_vacuum_inefficiency_tables` - VACUUM inefficiency
- `PostgreSQL_toast_table_excessive_usage` - TOAST table usage
- `PostgreSQL_unlogged_tables_analysis` - Unlogged tables analysis
- `PostgreSQL_statistics_reset_frequency` - Statistics reset frequency
- `PostgreSQL_parameter_sniffing_detection` - Parameter sniffing detection

</details>

### 📊 Index Management & Analysis (15 tools)

<details>
<summary>Click to expand Index tools</summary>

- `PostgreSQL_analyze_index_bloat` - Identify index bloat
- `PostgreSQL_create_index` - Create index on table
- `PostgreSQL_drop_index` - Drop index
- `PostgreSQL_get_index_maintenance_status` - Index maintenance status
- `PostgreSQL_get_index_usage` - Index usage statistics
- `PostgreSQL_get_index_usage_stats` - Comprehensive index usage
- `PostgreSQL_get_unused_indexes` - Find unused indexes
- `PostgreSQL_identify_index_redundancy` - Redundant indexes
- `PostgreSQL_index_bloat_maintenance_analysis` - Index bloat maintenance
- `PostgreSQL_index_dead_tuples_analysis` - Dead tuples per index
- `PostgreSQL_index_redundancy_detection` - Index redundancy detection
- `PostgreSQL_index_scan_efficiency` - Index scan efficiency
- `PostgreSQL_list_indexes` - List table indexes
- `PostgreSQL_analyze_index_effectiveness` - Index effectiveness
- `PostgreSQL_plan_invalidation_analysis` - Plan invalidation analysis

</details>

### 🔄 Replication & Backup Management (20 tools)

<details>
<summary>Click to expand Replication & Backup tools</summary>

- `PostgreSQL_backup_recovery_info` - Backup and recovery info
- `PostgreSQL_get_backup_details` - Last backup status
- `PostgreSQL_get_backup_recovery_readiness` - Backup readiness
- `PostgreSQL_get_backup_status` - Backup status information
- `PostgreSQL_get_logical_replication_stats` - Logical replication stats
- `PostgreSQL_get_publication_details` - Publication details
- `PostgreSQL_get_publication_subscription_details` - Pub/sub details
- `PostgreSQL_get_publication_tables` - Publication tables
- `PostgreSQL_get_publications` - Logical replication publications
- `PostgreSQL_get_replication_slot_details` - Replication slot details
- `PostgreSQL_get_replication_slot_infos` - Replication slot info
- `PostgreSQL_get_replication_slots` - Replication slots
- `PostgreSQL_get_replication_stats` - Replication statistics
- `PostgreSQL_get_replication_status` - Replication status
- `PostgreSQL_get_subscription_info` - Subscription information
- `PostgreSQL_get_wal_archiving_settings` - WAL archiving settings
- `PostgreSQL_get_wal_stats` - WAL statistics
- `PostgreSQL_get_write_ahead_log_analysis` - WAL analysis
- `PostgreSQL_diagnose_logical_replication_lag` - Replication lag diagnosis
- `PostgreSQL_logical_replication_slot_lag` - Logical replication lag

</details>

### 📋 Table Operations & Constraints (25 tools)

<details>
<summary>Click to expand Table Operations tools</summary>

- `PostgreSQL_check_constraint_violations` - Constraint violations
- `PostgreSQL_constraint_violations` - Data integrity issues
- `PostgreSQL_foreign_key_orphaned_references` - Orphaned references
- `PostgreSQL_foreign_keys_referencing_table` - Tables referencing table
- `PostgreSQL_get_constraint_violation_risks` - Constraint risks
- `PostgreSQL_get_constraint_violations` - Constraint violations
- `PostgreSQL_get_foreign_keys` - Foreign key relationships
- `PostgreSQL_get_table_constraints` - Table constraints
- `PostgreSQL_get_table_inheritance` - Table inheritance
- `PostgreSQL_get_table_rules` - Table rules
- `PostgreSQL_list_foreign_key_references` - Foreign key references
- `PostgreSQL_list_table_rules` - List table rules
- `PostgreSQL_check_table_inheritance` - Table inheritance analysis
- `PostgreSQL_get_partitioned_tables` - Partitioned tables info
- `PostgreSQL_get_partition_details` - Partitioning details
- `PostgreSQL_get_partition_info_detailed` - Detailed partition info
- `PostgreSQL_get_materialized_views` - List materialized views
- `PostgreSQL_get_materialized_view_stats` - Materialized view stats
- `PostgreSQL_monitor_materialized_views` - Monitor materialized views
- `PostgreSQL_refresh_materialized_view` - Refresh materialized view
- `PostgreSQL_get_toast_tables` - TOAST tables information
- `PostgreSQL_get_sequences` - All sequences with values
- `PostgreSQL_get_sequence_value` - Get sequence value
- `PostgreSQL_list_sequences` - List sequences in schema
- `PostgreSQL_reset_sequence` - Reset sequence value

</details>

### 📦 Extensions & Database Objects (18 tools)

<details>
<summary>Click to expand Extensions & Objects tools</summary>

- `PostgreSQL_extension_usage` - Extension usage statistics
- `PostgreSQL_get_detailed_foreign_tables` - Foreign tables details
- `PostgreSQL_get_event_triggers` - Event triggers
- `PostgreSQL_get_event_triggers_detailed` - Detailed event triggers
- `PostgreSQL_get_extension_list` - Installed extensions
- `PostgreSQL_get_extension_usage_analysis` - Extension usage analysis
- `PostgreSQL_get_foreign_tables` - Foreign tables and wrappers
- `PostgreSQL_get_full_text_search_configs` - Full-text search configs
- `PostgreSQL_get_functions` - User-defined functions
- `PostgreSQL_get_text_search_configs` - Text search configurations
- `PostgreSQL_get_trigger_performance_impact` - Trigger performance
- `PostgreSQL_get_triggers` - All triggers in database
- `PostgreSQL_list_event_triggers_detailed` - Detailed event triggers
- `PostgreSQL_list_foreign_tables_detailed` - Detailed foreign tables
- `PostgreSQL_list_functions` - Functions in schema
- `PostgreSQL_list_triggers` - Triggers on table
- `PostgreSQL_assess_trigger_performance_impact` - Trigger impact
- `PostgreSQL_long_execution_triggers` - Long execution triggers

</details>

### ⚙️ Configuration & System Info (12 tools)

<details>
<summary>Click to expand Configuration tools</summary>

- `PostgreSQL_get_database_config` - Database configuration
- `PostgreSQL_get_important_settings` - Important settings
- `PostgreSQL_get_server_version` - Server version info
- `PostgreSQL_checkpoint_activity` - Checkpoint activity
- `PostgreSQL_disk_usage_forecast` - Disk usage forecast
- `PostgreSQL_get_checkpoint_analysis` - Checkpoint analysis
- `PostgreSQL_get_checkpoint_info` - Checkpoint information
- `PostgreSQL_get_checkpoint_stats` - Checkpoint statistics
- `PostgreSQL_monitor_checkpoint_efficiency` - Checkpoint efficiency
- `PostgreSQL_monitor_wal_generation_rate` - WAL generation rate
- `PostgreSQL_wal_segment_recycling_analysis` - WAL recycling
- `PostgreSQL_check_database_encoding_collation` - Encoding/collation check

</details>

### 🧪 Advanced Analysis & Predictions (29 tools)

<details>
<summary>Click to expand Advanced Analysis tools</summary>

- `PostgreSQL_analyze_sequence_usage` - Sequence usage analysis
- `PostgreSQL_get_sequence_usage_risks` - Sequence exhaustion risks
- `PostgreSQL_get_sequence_usage_stats` - Sequence usage statistics
- `PostgreSQL_predict_sequence_exhaustion` - Predict sequence exhaustion
- `PostgreSQL_detect_table_bloat_regression` - Table bloat regression
- `PostgreSQL_detect_transaction_wraparound_risk` - Wraparound risk
- `PostgreSQL_diagnose_logical_replication_lag` - Replication lag diagnosis
- `PostgreSQL_disk_usage_forecast` - Disk usage forecasting
- `PostgreSQL_get_database_growth_trend` - Database growth trends
- `PostgreSQL_get_performance_regression_indicators` - Performance regression
- `PostgreSQL_get_query_error_analysis` - Query error patterns
- `PostgreSQL_maintenance_window_activity` - Maintenance patterns
- `PostgreSQL_memory_usage_analysis` - Memory usage analysis
- `PostgreSQL_monitor_connection_patterns` - Connection patterns
- `PostgreSQL_replication_lag_detailed` - Detailed replication lag
- `PostgreSQL_replication_slot_activity_analysis` - Slot activity
- `PostgreSQL_check_function_performance` - Function performance
- `PostgreSQL_check_replication_lag_details` - Replication lag details
- `PostgreSQL_analyze_buffer_utilization` - Buffer utilization
- `PostgreSQL_analyze_query_plans` - Query plan analysis
- `PostgreSQL_analyze_trigger_performance` - Trigger performance
- `PostgreSQL_monitor_checkpoint_efficiency` - Checkpoint efficiency
- `PostgreSQL_monitor_connection_patterns` - Connection patterns
- `PostgreSQL_monitor_wal_generation_rate` - WAL generation
- `PostgreSQL_plan_invalidation_analysis` - Plan invalidation
- `PostgreSQL_query_pattern_clustering` - Query pattern analysis
- `PostgreSQL_statistical_anomaly_detection` - Statistical anomalies
- `PostgreSQL_temporary_objects_usage` - Temporary objects
- `PostgreSQL_vacuum_freeze_age_analysis` - Freeze age analysis

</details>

---

## 🧰 Quick Tool Categories

---

## 💬 Prompt Examples for AI Agents

### 👁️ Database Insight

> "List all indexes in the public schema"
> "Show detailed structure of the `orders` table"

### 🚀 Performance

> "What are the slowest queries?"
> "Analyze unused indexes"

### ⚙ Maintenance

> "VACUUM ANALYZE the `products` table"
> "Find bloated tables in the database"

### 🔒 Security

> "List all users and their roles"
> "Show active connections and privilege levels"

---

## 📚 Use Cases

### ✅ Health & Diagnostics

* Full PostgreSQL system health check
* Query and index optimization

### ⚙ Admin & Automation

* AI-based schema exploration
* SQL query building with NLP

### 🔐 Security & Auditing

* Role auditing, connection analysis
* Secure query validation and permission check

---

## 🗞️ License

This project is licensed under the MIT License – see the [LICENSE](./LICENSE) file for details.

---

## ⭐ Contribute & Support

If you find this useful, please:

* ⭐ Star the repo
* 🍝 Fork it
* 🛠️ Submit issues or PRs

> 🧠 Let’s build smarter PostgreSQL automation – one prompt at a time.

---

## 🔍 GitHub Search Tags

`#PostgreSQL` `#MCP` `#AI_Database_Tools` `#Claude` `#Query_Optimizer` `#Database_Health_Check` `#Python_PostgreSQL` `#DevOps_AI` `#Database_Monitoring` `#OpenSource`
