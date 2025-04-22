# tdengine-mcp
TDengine MCP Server.

# TDengine Query MCP Server

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A Model Context Protocol (MCP) server that provides **read-only** TDengine database queries for AI assistants. Execute queries, explore database structures, and investigate your data directly from your AI-powered tools.

## Supported AI Tools

This MCP server works with any tool that supports the Model Context Protocol, including:

- **Cursor IDE**: Set up in `.cursor/mcp.json`
- **Anthropic Claude**: Use with a compatible MCP client
- **Other MCP-compatible AI assistants**: Follow the tool's MCP configuration instructions

## Features & Limitations

### What It Does

- ✅ Execute **read-only** TDengine queries (SELECT, SHOW, DESCRIBE only)
- ✅ Work with predefined environments (local, development, staging, production)
- ✅ Provide database information and metadata
- ✅ List available database environments

### What It Doesn't Do

- ❌ Execute write operations (INSERT, UPDATE, DELETE, CREATE, ALTER, etc.)
- ❌ Provide database design or schema generation capabilities
- ❌ Function as a full database management tool

This tool is designed specifically for **data investigation and exploration** through read-only queries. It is not intended for database administration, schema management, or data modification.


## Quick Install

```bash
# Install globally with npm
uv pip install tdengine_mcp_server

# Or run directly with npx
uvx tdengine_mcp_server
```

## Configuration Options

| Environment Variable | Description | Default |
|---------------------|-------------|---------|
| LOG_LEVEL | Set the log level (DEBUG, INFO, WARN, ERROR) | INFO |
| TDENGINE_HOST | Database host for environment | - |
| TDENGINE_PORT | Database port | - |
| TDENGINE_USERNAME | Database username | - |
| TDENGINE_PASSWORD | Database password | - |
| TDENGINE_DATABASE | Database name | 3306 |
| TDENGINE_TIMEOUT | Set the connection timeout in seconds | 30 |

## Integration with AI Assistants

Your AI assistant can interact with TDengine databases through the MCP server. Here are some examples:

Example queries:

```
Can you use the query tool to show me the first 10 records from the database?
```

```
I need to analyze our sales data. Can you run a SQL query to get the total sales per region for last month from the development database?
```

```
Can you list all the available databases we have?
```

### Using TDengine MCP Tools

The TDengine Query MCP server provides three main tools that your AI assistant can use:

#### 1. query

Execute read-only SQL queries against a specific environment:

```
Use the query tool to run:

SELECT * FROM customers WHERE itemid > '2025-01-01' LIMIT 10;

on the development environment
```

#### 2. info

Get detailed information about your stable:

```
Use the info tool to check the meta info about the specified stable.
```

## Security Considerations

- ✅ Only read-only queries are allowed (SELECT, SHOW, DESCRIBE)
- ✅ Query timeouts prevent runaway operations

## Troubleshooting

### Connection Issues

If you're having trouble connecting:

1. Verify your database credentials in your MCP configuration
2. Ensure the TDengine server is running and accessible
3. Check for firewall rules blocking connections
4. Enable debug mode by setting `LOG_LEVEL` in your configuration

### Common Errors

**Error: No connection pool available for environment**

- Make sure you've defined all required environment variables for that environment

**Error: Query execution failed**

- Verify your SQL syntax
- Check that you're only using supported query types (SELECT, SHOW, DESCRIBE)
- Ensure your query is truly read-only

For more comprehensive troubleshooting, see the [Troubleshooting Guide](docs/TROUBLESHOOTING.md).

For examples of how to integrate with AI assistants, see the [Integration Examples](docs/INTEGRATION_EXAMPLE.md).

For implementation details about the MCP protocol, see the [MCP README](docs/MCP_README.md).

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

---

For more information or support, please [open an issue](https://github.com/devakone/TDengine-query-mcp-server/issues) on the GitHub repository. 
