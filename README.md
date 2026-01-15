# sql2graph

`sql2graph` is a Python library that migrates relational SQLite databases into
Neo4j graph databases using a schema-driven approach.

It automatically:
- Extracts table schemas (columns, primary keys, foreign keys)
- Maps tables to node labels
- Maps foreign keys to relationships
- Creates Neo4j constraints when possible
- Populates the graph efficiently using batch operations

---

## Features

- SQLite schema introspection
- Automatic node and relationship creation
- Primary key detection and constraint generation
- Configurable relationship naming
- Logging-friendly and debuggable
- Designed as a reusable library (not a one-off script)

---

## Installation (development mode)

```bash
pip install -e .
