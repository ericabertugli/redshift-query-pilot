# Redshift query pilot
An AI agent skill that assists in generating optimized SQL queries for Amazon Redshift and Redshift Spectrum.

## What is this?

It's a an AI Agent Skill designed to integrate with your favorite LLM-powered coding assistant (Claude Code, GitHub Copilot, Cursor, etc.) to help you write performant SQL queries. It provides:

- **Schema awareness** - The AI knows your table structures, column types, and relationships without you having to explain them
- **Partition optimization** - Automatic guidance on filtering partition keys to avoid expensive full S3 scans
- **Spectrum best practices** - Built-in knowledge of Redshift Spectrum limitations (read-only, no complex types in SELECT, nested data handling)
- **Query optimization tips** - Storage format awareness (Parquet vs JSON), predicate pushdown, and cost control

Instead of manually looking up table schemas or remembering Spectrum quirks, just ask your AI assistant to write a query and it will automatically look up the relevant schemas and apply best practices.

## How It Works

The tool consists of three components:

1. **Schema Sync** (`sync_catalog.py`) - Fetches table schemas from AWS Glue (Spectrum/Data Lake) and Redshift (Data Warehouse) and caches them in a local SQLite database. Must be run periodically to keep the cache up to date.
2. **MCP Server** (`mcp_server.py`) - Exposes the cached schemas to AI assistants via the [Model Context Protocol (MCP)](https://modelcontextprotocol.io/).
3. **LLM Skill** (`command.md`) - Custom instructions that teach the AI assistant how to write performant queries in Spectrum and Redshift, including partition filtering, predicate pushdown, and storage format optimization

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   AWS Glue      │     │    SQLite       │     │  AI Assistant   │
│   (Spectrum)    │────▶│    Cache        │◀────│  (Claude, etc)  │
├─────────────────┤     │  (catalog.db)   │     └─────────────────┘
│   Redshift      │────▶│                 │◀────── MCP Protocol
│   (DWH)         │     └─────────────────┘
└─────────────────┘
     sync_catalog.py         mcp_server.py
```

## Installation

### Prerequisites

- Python 3.10+
- [uv](https://docs.astral.sh/uv/) (recommended) or pip
- AWS credentials configured (for Glue access, e.g., via `aws sso login`)
