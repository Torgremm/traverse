# Traverse

Traverse is a local-file graph data tool for small project datasets. A project is a directory with a schema, source data, optional scripts/templates, and a generated graph database stored as diffable JSONL files.

## Repository Layout

```text
cli/
  Rust CLI, graph engine, stdio API, and CLI tests
client/
  Native frontend plan and future client implementation
```

## Project Layout

```text
project/
  schema.json
  data/
    <label>.json
  graph/
    nodes.jsonl
    edges.jsonl
  scripts/
    <script>.json
  templates/
    <template files>
```

`schema.json` defines labels, properties, primary keys, and graph edges. Existing table names become node labels. Foreign key columns become directed edge labels.

`data/*.json` is the import/export format. `load` converts these files into the canonical graph files under `graph/`.

`graph/nodes.jsonl` and `graph/edges.jsonl` are the local graph database. They are plain text and written in deterministic order so Git diffs stay readable.

## Commands

Commands use the current working directory as the Traverse data project. Run them from a project directory.

When working from a project directory outside `cli/`, point Cargo at the CLI manifest:

```powershell
cargo run --manifest-path D:\repo\traverse\cli\Cargo.toml -- <command>
```

When working inside a project directory under `cli/`, plain `cargo run -- <command>` also works because Cargo can find `cli/Cargo.toml` by walking up parent directories.

```powershell
cargo run -- load
```

Loads `schema.json` and `data/*.json`, validates the data, and writes `graph/nodes.jsonl` and `graph/edges.jsonl`.

```powershell
cargo run -- query "MATCH <type> RETURN *"
```

Runs a graph query against the local graph files and prints one JSON object per returned row.

```powershell
cargo run -- types
```

Prints type metadata as JSON for UI clients. Each type includes its name, primary key, color, columns, declared outgoing edges, and node count.

```powershell
cargo run -- table <type>
```

Prints a table-view JSON payload for one type. The response includes columns and rows, with each row carrying `_id` plus the schema-defined fields.

```powershell
cargo run -- graph
```

Prints graph-view JSON for UI clients. The response includes type metadata, nodes with type colors, and edges.

```powershell
cargo run -- serve
```

Starts a long-lived line-delimited JSON API over stdio. This is the preferred integration point for native frontends because the backend can keep the project graph in memory between requests.

```powershell
cargo run -- query "MATCH <type> WHERE <primary_key> = <value> SET <field> = <value>"
```

Runs a mutating graph query and rewrites the graph files deterministically. The command prints the number of changed nodes.

```powershell
cargo run -- run <script>.json
```

Runs the selected file from `scripts/`, using templates from `templates/`, and prints the rendered result to the console. If graph files are missing, `run` loads the project data first.

```powershell
cargo run -- save
```

Exports the graph state back to `data/*.json`.

## Graph Query Syntax

Queries currently use a small graph DSL:

```text
MATCH <label> [WHERE <field> <op> <value>] [TRAVERSE <edge|*> [DEPTH n]] [RETURN <*|fields>] [SET <field> = <value>]
```

Supported operators:

```text
=
==
!=
```

Examples:

```text
MATCH <type> RETURN *
MATCH <type> WHERE <field> = <value> RETURN <field>,<field>
MATCH <type> WHERE <field> = <value> TRAVERSE <edge> DEPTH 1 RETURN *
MATCH <type> WHERE <field> = <value> TRAVERSE * DEPTH 2 RETURN *
MATCH <type> WHERE <field> = <value> SET <field> = <new_value>
```

Values may be bare words, quoted strings, integers, floats, or booleans.

## Scripts

A script is a JSON file under `scripts/`:

```json
{
  "fetch": "MATCH <type> RETURN *",
  "act": "{% include \"template.txt\" %}"
}
```

`fetch` is a graph query. `act` is a Tera template string. Templates from the project `templates/` directory can be included.

Script modes:

```json
{
  "fetch": "MATCH <type> RETURN *",
  "mode": "raw",
  "act": "{{ field_name }}"
}
```

`raw` exposes returned graph fields directly to the template.

```json
{
  "fetch": "MATCH <type> RETURN *",
  "mode": "scope",
  "act": "{{ edge_field_name }}"
}
```

`scope` starts from matched root nodes, follows schema-defined graph edges, and exposes flattened reachable fields to the template. If omitted, `mode` defaults to `scope`.

## Notes

- Run `load` before `query` or `save` when graph files do not exist yet. `run` auto-loads missing graph files before executing the script.
- `types`, `table`, `graph`, `query`, and `run` produce JSON or rendered text on stdout and are intended to be callable by a future native frontend.
- The graph database is scoped to the current project directory.
- The current CLI handles one query per invocation.
- The implementation loads the graph into memory for each command, which is simple and works well for small to medium local projects.

## Schema

The schema is still required. It is the type interface for both the CLI and a future frontend:

- table names define node types
- columns define table-view fields and value validation
- primary keys define stable graph node IDs
- foreign keys define graph edges
- type names drive deterministic frontend colors

Without schema, Traverse would have to infer types, primary keys, and edges from data files. That would make the frontend contract unstable and would remove validation before graph files are written.

## Stdio API

`serve` reads one JSON request per line from stdin and writes one JSON response per line to stdout.

Request shape:

```json
{"id":1,"method":"project.open","params":{"path":"D:/repo/traverse/cli/tests/test_dir"}}
```

Response shape:

```json
{"id":1,"ok":true,"result":{}}
```

Error response:

```json
{"id":1,"ok":false,"error":{"message":"No project is open"}}
```

Supported methods:

```text
project.open   { "path": string }
project.status {}
project.save   {}
types.list     {}
table.get      { "type": string }
graph.get      {}
query.run      { "query": string }
node.update    { "id": string, "properties": object }
script.run     { "script": string }
shutdown       {}
```

Example session:

```json
{"id":1,"method":"project.open","params":{"path":"D:/projects/example"}}
{"id":2,"method":"types.list","params":{}}
{"id":3,"method":"table.get","params":{"type":"asset"}}
{"id":4,"method":"node.update","params":{"id":"asset:A001","properties":{"status":"active"}}}
{"id":5,"method":"script.run","params":{"script":"report.json"}}
{"id":6,"method":"shutdown","params":{}}
```

`project.open` loads `schema.json` and graph JSONL files into memory. If graph files are missing, it imports from `data/*.json` first. `node.update` validates fields against schema, rebuilds affected graph edges in memory, and marks the project dirty. Dirty graph data is written to deterministic JSONL only on `project.save` or `shutdown`.
