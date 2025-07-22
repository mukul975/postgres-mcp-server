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
🔍 PostgreSQL Tool Search (All 237 Tools Included)
Search across 237+ PostgreSQL tools built for this MCP server using the live search bar:


<!-- Tool Search Input -->
<input type="text" id="toolSearch" placeholder="Search PostgreSQL Tools..." style="width:100%;padding:10px;font-size:16px;border-radius:8px;border:1px solid #ccc;">

<!-- Results List -->
<ul id="toolResults" style="list-style:none;padding-left:0;"></ul>

<!-- Search Script -->
<script>
  fetch('https://raw.githubusercontent.com/mukul975/postgres-mcp-server/main/tools/postgresql_tools_list.json')
    .then(response => response.json())
    .then(tools => {
      const input = document.getElementById('toolSearch');
      const results = document.getElementById('toolResults');
      input.addEventListener('input', function () {
        const query = this.value.toLowerCase();
        const filtered = tools.filter(tool => tool.toLowerCase().includes(query));
        results.innerHTML = filtered.map(tool => `<li>${tool}</li>`).join('');
      });
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
