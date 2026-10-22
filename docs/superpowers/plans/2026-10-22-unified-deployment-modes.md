# Unified Deployment Modes Implementation Plan

> **For agentic workers:** Use test-driven development for each owned subsystem and coordinate Maven runs. Preserve the existing uncommitted distributed-certification work.

**Goal:** Configure single, distributed and shared-backend mock execution in one TOML file, with named datasources and no fixed node-count cap.

**Architecture:** `config/txnsails.toml` declares shared datasource settings plus `[single]`, `[distributed]` and `[mock]` sections. `--mode` overrides the default mode; `--instance-id` chooses a configured owner. Runtime branch state uses maps keyed by arbitrary nonnegative integer IDs and opens business connections only when accessed. Distributed transaction identity consists of the full coordinator ID and a durable local sequence, rather than packing owner bits into a long.

**Tech Stack:** Java 21, Jackson TOML, JUnit 5, PostgreSQL prepared transactions.

## Configuration contract

Native format version 2: root `mode`, `benchmark`, `schema`, `validation`, `state_directory`; optional `[runtime]` tuning including `auxiliary_connections`; `[datasources.<name>]` with `url`, `username`, `password`, `driver`, `type`, reconnect settings; `[single]` with `datasource`, `client_port`, `concurrency_control`; `[distributed]` with `workload`, `nodes` and `tables`; `[mock]` with one shared `datasource`, `workload`, `nodes` and `tables`. Each node has `id`, `host`, `client_port`, `rpc_port`, `isolation`; distributed nodes additionally reference their datasource. Mode selection must control execution even with one distributed/mock node. Preserve explicit legacy XML and version-1 TOML loading for compatibility; examples migrate datasource definitions to the unified TOML file.

## Task 1: Mode-aware configuration

- [x] Add RED tests in `TomlDeploymentTest`: all three sections in one file, mode override, named datasource selection, settings migration, sparse IDs above 255 and more than eight nodes, invalid references/duplicates/mode combinations.
- [x] Add `DeploymentMode` and WorkloadConfiguration mode/auxiliary settings, preserving the legacy inference only when no mode is declared.
- [x] Implement `TomlDeployment.loadMode(Path, String, Integer)` and keep `load(Path, String, int)` for version-1 profiles. Run the focused loader tests GREEN.
- [x] Add `config/txnsails.toml` with all three modes and safe example datasource values.

## Task 2: Full coordinator identities

- [x] Add RED tests proving that equal local sequences on coordinators 0, 256 and a large sparse ID cannot share validation locks, finalization or replay watermarks.
- [x] Add serializable `TransactionIdentity(int coordinatorId, long sequence)`. Keep existing long-taking validation helpers as coordinator-zero compatibility wrappers.
- [x] Thread the full identity through ValidationRequest/FinalRequest, participant/validation locking, and RPC records/history/watermarks. Keep local manager lookups by local sequence.
- [x] Allocate durable local sequences without embedding node bits; include full coordinator ID in native prepared-transaction names. Verify restart monotonicity and focused RPC/certification tests.

## Task 3: Dynamic branch execution and startup

- [x] Add RED tests for explicit distributed/mock mode with one node and sparse/many node execution.
- [x] Replace OnlineWorker's fixed connection, participant, usage and result arrays with maps/sets. Open each business connection lazily on the first access to its owner.
- [x] Replace node-indexed result arrays in TransactionManager with maps; prepare/finalize with the full coordinator identity.
- [x] Use explicit mode in ClientHandler and TxnSailsServer; add `--mode`, default unified TOML configuration, configurable auxiliary connection count, and remove legacy eight-node checks from startup/catalogue loading.

## Task 4: Integration, migration and documentation

- [x] Add real PostgreSQL tests for single, distributed and mock selection from one file; a transaction across at least nine logical owners with sparse IDs must commit and rollback cleanly.
- [x] Move bundled XML datasource values into the canonical TOML file and remove credential defaults from legacy XML DTOs. Update README, deployment guide and example client to the three-mode workflow.
- [x] Run focused tests, complete regression suite, build the fat JAR and exercise the packaged server/client. Confirm no prepared transactions remain and stop the temporary PostgreSQL server.
- [x] Review the resulting diff for any remaining fixed node limits or packed coordinator extraction; run `git diff --check`.

## Verification results

- Full PostgreSQL-enabled Java suite: 161 tests, zero failures/errors/skips.
- Python datasource/generator migration: 6 tests passed.
- Packaged-JAR integration: single, distributed and nine-node mock tests passed.
- Supplied Python client passed all three modes against the packaged JAR.
- Nine-owner transactions used sparse IDs through Integer.MAX_VALUE and verified native branch count, commit, rollback and clean prepared-transaction state.
- Worker configuration with 4,096 sparse owners opened no untouched business connections.
- Final diff whitespace validation passed; all temporary test databases and PostgreSQL processes were cleaned up.
