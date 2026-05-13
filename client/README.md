# Traverse Client

This directory is reserved for the future native frontend.

## Recommended Implementation

Use **Tauri 2 + React + TypeScript + Vite**.

Why this is the best fit:

- Cross-platform desktop support for Windows, macOS, and Linux.
- Small native shell compared with Electron.
- Rust-side integration can bundle and spawn the `traverse` CLI as a sidecar.
- The frontend can use mature web UI tooling for table views and graph visualization.
- The existing CLI already exposes a stable stdio API through `traverse serve`.

## Architecture

```text
client UI
  |
  | Tauri command bridge
  v
Tauri Rust shell
  |
  | spawn sidecar: traverse serve
  | line-delimited JSON over stdin/stdout
  v
cli graph engine
  |
  | deterministic JSONL flush on save/shutdown
  v
project graph files
```

The client should not own the database. It should keep UI state and delegate validation, mutation, graph rebuilds, script generation, and persistence to the CLI server.

## Frontend Views

- **Project Open View**: choose a local project directory and call `project.open`.
- **Type Navigation**: call `types.list`; render one entry per schema type using the returned color.
- **Table View**: call `table.get` for the selected type; render schema columns and row values.
- **Graph View**: call `graph.get`; render nodes with returned type colors and edges with labels.
- **Script View**: list known scripts from the project later; initially allow manual script selection and call `script.run`.
- **Dirty State**: poll or request `project.status`; show unsaved changes when `dirty` is true.

## Mutation Flow

Use `node.update` for edits from the table view or node inspector:

```json
{"id":10,"method":"node.update","params":{"id":"asset:A001","properties":{"status":"active"}}}
```

Expected client behavior:

- Optimistically update UI only after an `ok: true` response.
- On success, update the affected row/node from `result.node`.
- On failure, display `error.message` and leave UI data unchanged.
- Do not write graph files directly from the client.

Persistence:

- Call `project.save` when the user saves.
- Call `shutdown` when the app closes.
- The CLI server keeps mutations in memory and flushes deterministic JSONL only on save or shutdown.

## API Contract

The native client should use the stdio API documented in the repository root README.

Core methods:

```text
project.open
project.status
project.save
types.list
table.get
graph.get
query.run
node.update
script.run
shutdown
```

## Implementation Plan

1. Scaffold Tauri 2 with React, TypeScript, and Vite under `client/`.
2. Add a Tauri Rust sidecar manager that starts `cli` in `serve` mode.
3. Implement a typed request/response client in TypeScript.
4. Build project open and type navigation first.
5. Build table view with editable cells and `node.update`.
6. Build graph view using a canvas/SVG graph library that accepts explicit node colors.
7. Add save/shutdown lifecycle handling.
8. Add integration tests with a fake stdio server, then end-to-end tests against `traverse serve`.

## Deferred Decisions

- Graph visualization library. Prefer one with good incremental updates and layout controls.
- Script discovery API. The CLI should eventually expose `scripts.list`.
- Edge creation/deletion API. Add explicit `edge.create` and `edge.delete` only when the frontend workflow needs them.
- Batched mutations. Add `transaction.apply` if table editing becomes high-volume.
