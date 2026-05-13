# Frontend Implementation Plan

## Summary

Build `client/` as a native Rust desktop app using `eframe`/`egui`, avoiding web tooling. The client talks to the existing CLI through `traverse serve` over line-delimited JSON stdio. The CLI remains the source of truth for validation, mutations, graph rebuilds, script generation, save, and shutdown.

## Architecture

- Use Rust workspace member `client`.
- Add `client/Cargo.toml` with `eframe`, `egui_extras`, `serde`, `serde_json`, and process/stdout handling dependencies as needed.
- Client process starts the CLI sidecar:
  - dev path: workspace `target/debug/traverse`
  - later release path: bundled executable beside the client.
- Maintain one `ServeClient` abstraction:
  - owns child process stdin/stdout
  - sends `{id, method, params}` JSON lines
  - reads matching response lines
  - exposes typed request methods to the UI layer.
- Keep frontend state separate from backend truth:
  - UI caches last `types.list`, `table.get`, and `graph.get`
  - mutations only update UI after `ok: true`
  - save calls `project.save`
  - app close calls `shutdown`

## UI Structure

- Main window layout:
  - top toolbar: open project, save, dirty status, active project path
  - left sidebar: project overview, type list, graph view, scripts
  - central panel: selected view
  - right inspector: selected row/node details and editable fields
- Project open:
  - use native folder picker if available, otherwise text path input for the first pass
  - call `project.open`
  - then load `types.list`
- Type/table view:
  - one navigable entry per schema type
  - call `table.get { type }`
  - render with `egui_extras::TableBuilder`
  - selectable rows
  - inspector edits fields and calls `node.update`
- Graph view:
  - call `graph.get`
  - render nodes with returned type colors
  - render labeled edges
  - first implementation can use simple deterministic layout grouped by type
  - later add force-directed layout/pan/zoom
- Script view:
  - initially allow script filename input
  - call `script.run`
  - show rendered output in a scrollable text panel
  - later add `scripts.list` to CLI and replace manual filename entry
- Dirty state:
  - read from `project.status`
  - set UI dirty indicator after successful mutation
  - clear after `project.save` or `shutdown`

## API Contract

Use existing stdio methods:

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

Expected request/response:

```json
{"id":1,"method":"table.get","params":{"type":"asset"}}
{"id":1,"ok":true,"result":{"type":"asset","columns":[],"rows":[]}}
```

Errors must be shown from:

```json
{"id":1,"ok":false,"error":{"message":"..."}}
```

## Client Modules

- `main.rs`: eframe startup and app initialization.
- `app.rs`: `TraverseApp` state machine and top-level UI.
- `serve_client.rs`: stdio sidecar protocol, request IDs, JSON parsing, error handling.
- `models.rs`: typed structs for API responses: `TypeInfo`, `ColumnInfo`, `TableView`, `GraphView`, `NodeView`, `EdgeView`.
- `views/project.rs`: open/save/status UI.
- `views/table.rs`: type table rendering and selection.
- `views/graph.rs`: graph canvas rendering.
- `views/inspector.rs`: selected node/row editor.
- `views/scripts.rs`: script run/output UI.

## Important Behavior

- Do not write project files from the client.
- Do not infer schema in the client.
- Do not mutate cached UI data before backend success.
- Always call `shutdown` on app exit if a server is running.
- If `shutdown` fails, surface the error because unsaved dirty state may not have flushed.
- Treat fixture data names like `valves`, `io`, etc. as test-only. No client code should special-case any type or field names.

## Testing Plan

- Unit test `ServeClient` against a fake line-based server.
- Unit test response parsing for success and error responses.
- Unit test table model updates after `node.update`.
- Unit test dirty-state transitions:
  - clean after open
  - dirty after mutation
  - clean after save
- UI smoke tests where feasible:
  - app can start
  - project open populates type list
  - selecting a type loads table rows
- End-to-end dev test:
  - start real `traverse serve`
  - open `cli/tests/test_dir`
  - call `types.list`
  - call `table.get`
  - call `node.update`
  - call `project.save`
  - call `shutdown`

## Assumptions

- `cli` remains the backend authority for now.
- `client` starts by communicating over stdio, not by linking engine code directly.
- A future refactor may extract shared graph logic into an `engine/` crate, but that is not required for the first native client.
- First graph view prioritizes correctness and navigation over advanced layout quality.
