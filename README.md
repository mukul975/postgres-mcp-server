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
#🔍 Search Tools

Use the search bar below to quickly find any of the 237+ PostgreSQL tools available in this project.
<input type="text" id="toolSearch" placeholder="Search PostgreSQL Tools..." style="width:100%;padding:10px;font-size:16px;border-radius:8px;border:1px solid #ccc;">
<ul id="toolResults" style="list-style:none;padding-left:0;"></ul>
<script>
  const tools = [
    "PostgreSQL_list_databases", "PostgreSQL_list_tables", "PostgreSQL_list_schemas", "PostgreSQL_describe_table", "PostgreSQL_execute_select_query", "PostgreSQL_execute_update_query", "PostgreSQL_explain_query", "PostgreSQL_vacuum_analyze_table", "PostgreSQL_analyze_database", "PostgreSQL_create_schema", "PostgreSQL_drop_schema", "PostgreSQL_create_index", "PostgreSQL_drop_index", "PostgreSQL_grant_privileges", "PostgreSQL_revoke_privileges", 
    "PostgreSQL_create_user", "PostgreSQL_drop_user", "PostgreSQL_list_users_and_roles", "PostgreSQL_get_role_attributes", "PostgreSQL_get_slow_queries", "PostgreSQL_get_active_connections", "PostgreSQL_buffer_cache_hit_ratio", "PostgreSQL_check_long_running_queries", "PostgreSQL_analyze_autovacuum_efficiency", "PostgreSQL_get_index_usage", "PostgreSQL_check_blocking_queries", "PostgreSQL_vacuum_inefficiency_tables", "PostgreSQL_analyze_index_bloat", "PostgreSQL_get_bloated_tables", "PostgreSQL_analyze_query_complexity", "PostgreSQL_get_replication_status", "PostgreSQL_get_wal_stats", "PostgreSQL_get_backup_status", "PostgreSQL_checkpoint_activity", "PostgreSQL_get_blocking_locks", "PostgreSQL_get_lock_waits", "PostgreSQL_detect_conflicting_queries", "PostgreSQL_detect_foreign_key_lock_contention", "PostgreSQL_check_constraint_violations", "PostgreSQL_get_foreign_keys", "PostgreSQL_get_table_constraints", "PostgreSQL_check_table_bloat", "PostgreSQL_list_event_triggers", "PostgreSQL_get_temp_files", "PostgreSQL_get_tablespace_usage", "PostgreSQL_analyze_buffer_utilization", "PostgreSQL_get_memory_usage_stats", "PostgreSQL_get_long_running_transactions", "PostgreSQL_analyze_sequence_usage", "PostgreSQL_identify_index_redundancy", "PostgreSQL_vacuum_progress", "PostgreSQL_get_unused_indexes", "PostgreSQL_get_index_usage_stats", "PostgreSQL_get_lock_statistics", "PostgreSQL_index_dead_tuples_analysis", "PostgreSQL_analyze_transaction_wraparound", "PostgreSQL_query_cancellation_analysis", "PostgreSQL_security_audit", "PostgreSQL_get_vacuum_analyze_recommendations", "PostgreSQL_get_table_statistics", "PostgreSQL_analyze_connection_pool_efficiency", "PostgreSQL_analyze_advanced_buffer_usage", "PostgreSQL_analyze_table_freeze_stats", "PostgreSQL_assess_trigger_performance_impact", "PostgreSQL_autovacuum_tuning", "PostgreSQL_backup_recovery_info", "PostgreSQL_buffer_cache_relation_analysis", "PostgreSQL_connection_churn_analysis", "PostgreSQL_connection_pool_stats", "PostgreSQL_constraint_violations", "PostgreSQL_deadlock_analysis", "PostgreSQL_detect_index_lock_waits", "PostgreSQL_detect_table_bloat_regression", "PostgreSQL_detect_transaction_wraparound_risk", "PostgreSQL_diagnose_logical_replication_lag", "PostgreSQL_disk_usage_forecast", "PostgreSQL_extension_usage", "PostgreSQL_foreign_key_conflicts", "PostgreSQL_foreign_key_orphaned_references", "PostgreSQL_foreign_keys_referencing_table", "PostgreSQL_get_active_transactions", "PostgreSQL_get_autovacuum_activity", "PostgreSQL_get_autovacuum_settings", "PostgreSQL_get_autovacuum_stats_per_table", "PostgreSQL_get_backup_details", "PostgreSQL_get_backup_recovery_readiness", "PostgreSQL_get_buffer_cache_contents", "PostgreSQL_get_buffer_hit_ratios_detailed", "PostgreSQL_get_cache_hit_ratio", "PostgreSQL_get_cache_hit_ratios", "PostgreSQL_get_checkpoint_analysis", "PostgreSQL_get_checkpoint_info", "PostgreSQL_get_checkpoint_stats", "PostgreSQL_get_column_privileges", "PostgreSQL_get_column_statistics", "PostgreSQL_get_concurrent_connection_analysis", "PostgreSQL_get_connection_limits", "PostgreSQL_get_connection_pool_analysis", "PostgreSQL_get_connection_pool_stats", "PostgreSQL_get_constraint_violation_risks", "PostgreSQL_get_constraint_violations", "PostgreSQL_get_database_config", "PostgreSQL_get_database_growth_trend", "PostgreSQL_get_database_size", "PostgreSQL_get_detailed_foreign_tables", "PostgreSQL_get_estimated_row_counts", "PostgreSQL_get_event_triggers", "PostgreSQL_get_event_triggers_detailed", "PostgreSQL_get_extension_list", "PostgreSQL_get_extension_usage_analysis", "PostgreSQL_get_foreign_tables", "PostgreSQL_get_full_text_search_configs", "PostgreSQL_get_functions", "PostgreSQL_get_high_wait_events", "PostgreSQL_get_idle_connections", "PostgreSQL_get_important_settings", "PostgreSQL_get_index_maintenance_status", "PostgreSQL_get_lock_monitoring", "PostgreSQL_get_locks_info", "PostgreSQL_get_logical_replication_stats", "PostgreSQL_get_materialized_view_stats", "PostgreSQL_get_materialized_views", "PostgreSQL_get_memory_context_analysis", "PostgreSQL_get_partition_details", "PostgreSQL_get_partition_info_detailed", "PostgreSQL_get_partit Ir_supported", "PostgreSQL_get_partitioned_tables", "PostgreSQL_get_performance_regression_indicators", "PostgreSQL_get_prepared_transactions", "PostgreSQL_get_publication_details", "PostgreSQL_get_publication_subscription_details", "PostgreSQL_get_publication_tables", "PostgreSQL_get_publications", "PostgreSQL_get_query_error_analysis", "PostgreSQL_get_query_plan_cache_analysis", "PostgreSQL_get_query_plan_cache_stats", "PostgreSQL_get_query_plans", "PostgreSQL_get_query_runtime_distribution", "PostgreSQL_get_replication_slot_details", "PostgreSQL_get_replication_slot_infos", "PostgreSQL_get_replication_slots", "PostgreSQL_get_replication_stats", "PostgreSQL_get_role_attributes", "PostgreSQL_get_sequence_usage_risks", "PostgreSQL_get_sequence_usage_stats", "PostgreSQL_get_sequence_value", "PostgreSQL_get_sequences", "PostgreSQL_get_server_version", "PostgreSQL_get_slow_query_patterns", "PostgreSQL_get_slow_query_statements", "PostgreSQL_get_subscription_info", "PostgreSQL_get_table_access_patterns", "PostgreSQL_get_table_bloat_estimation", "PostgreSQL_get_table_count", "PostgreSQL_get_table_fragmentation_analysis", "PostgreSQL_get_table_inheritance", "PostgreSQL_get_table_io_stats", "PostgreSQL_get_table_permissions", "PostgreSQL_get_table_rules", "PostgreSQL_get_table_size", "PostgreSQL_get_table_size_summary", "PostgreSQL_get_tablespace_info", "PostgreSQL_get_temp_file_stats", "PostgreSQL_get_text_search_configs", "PostgreSQL_get_toast_tables", "PostgreSQL_get_top_heavy_queries", "PostgreSQL_get_transaction_age_monitoring", "PostgreSQL_get_trigger_performance_impact", "PostgreSQL_get_triggers", "PostgreSQL_get_wait_events", "PostgreSQL_get_wal_archiving_settings", "PostgreSQL_get_write_ahead_log_analysis", "PostgreSQL_high_io_tables", "PostgreSQL_index_bloat_maintenance_analysis", "PostgreSQL_index_redundancy_detection", "PostgreSQL_index_scan_efficiency", "PostgreSQL_kill_connection", "PostgreSQL_list_event_triggers_detailed", "PostgreSQL_list_foreign_key_references", "PostgreSQL_list_foreign_tables_detailed", "PostgreSQL_list_functions", "PostgreSQL_list_indexes", "PostgreSQL_list_roles_with_login", "PostgreSQL_list_roles_with_superuser", "PostgreSQL_list_sequences", "PostgreSQL_list_table_rules", "PostgreSQL_list_triggers", "PostgreSQL_list_users_and_roles", "PostgreSQL_list_views", "PostgreSQL_logical_replication_slot_lag", "PostgreSQL_long_execution_triggers", "PostgreSQL_long_running_prepared_transactions", "PostgreSQL_longest_idle_transactions", "PostgreSQL_maintenance_window_activity", "PostgreSQL_memory_usage_analysis"
  ];
  document.getElementById('toolSearch').addEventListener('input', function () {
    const query = this.value.toLowerCase();
    const results = tools.filter(tool => tool.toLowerCase().includes(query));
    document.getElementById('toolResults').innerHTML = results.map(tool => `<li>${tool}</li>`).join('');
  });
</script>

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

## 🧰 Tooling Highlights

| Category        | Tools                                                |
| --------------- | ---------------------------------------------------- |
| 🧱 Structure    | `list_tables`, `describe_table`, `get_foreign_keys`  |
| 📄 Queries      | `execute_select_query`, `analyze_query_complexity`   |
| 📈 Monitoring   | `get_cache_hit_ratios`, `check_long_running_queries` |
| 🛠️ Maintenance | `get_bloated_tables`, `vacuum_analyze_table`         |
| 🔐 Security     | `create_user`, `grant_privileges`, `list_users`      |
| 🔁 Replication  | `get_replication_status`, `get_backup_status`        |

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
