# Distributed transaction repair

The distributed deployment routes primary-key point SELECT and UPDATE statements
to PostgreSQL participants. Every access, including transactions visiting only one
participant, is validated by the middleware owning that participant. Reads retain
their externally observed version and updates retain the version returned by the
database. Reads after an own write do not create an external dependency.

Validation takes class-aware read/write locks without waiting,
checks protected external read versions, and holds all locks until every participant has
committed or rolled back. Multiple participants use PostgreSQL two-phase commit.
Unknown commit/rollback outcomes retain transaction state and locks and return an
error rather than success. This repair does not implement durable coordinator decision or validation-lock
recovery. Restart requires all owners to be quiescent and unresolved transactions
to be reconciled; independently restarting an owner while peers continue is not
supported. Startup refuses prepared transactions in any configured database. All conflicting accesses must use this middleware.

Participant isolation is configured independently as RC, SI (PostgreSQL repeatable
read), or SER. Optimized certification is the default; use `-v full` for the
previous policy that strictly validates every external read. Both modes preserve
every conflict involving an actual multi-database transaction. See
[the optimization review](distributed-optimization.md) for the local policies and proof.
Distributed deployment uses explicit per-database isolation settings; the adaptive
single-database isolation switch is disabled in this mode. There is no globally consistent snapshot across independently started branches,
so the single-database SI rule of protecting only consecutive rw dependencies is
insufficient. Any rw anti-dependency in a global cycle can be vulnerable.

For a fixed complete workload, optional `-w` enables
[distributed static vulnerable-dependency analysis](distributed-static-analysis.md)
and collects only selected reads. All owners load the same closed catalogue,
and transactions begin with `begin#program` in that mode.

For example, T1 reads x=0 at A; T2 writes x=1 at A and y=1 at B and commits;
T1 then first accesses B and reads y=1. The global cycle is T1 --rw(x)--> T2
--wr(y)--> T1, despite each local history being serializable. Global validation
rejects T1 when its x version no longer matches.

## Supported statements and identity

Supported statements are single-table SELECT and UPDATE with equality parameters
for every primary-key column, using the benchmark's integer key domains. UPDATE
may change business columns, but not primary-key columns or `vid`. All accesses
must return exactly one existing row. Scans, joins, aggregates, subqueries,
additional predicates, ORDER BY, schema-qualified names, inserts, and deletes
are rejected with SQLSTATE `0A000`. Triggers or other hidden cross-row effects
are outside this protocol. Use the registered benchmark schemas and keep every
conflicting application transaction on this middleware.

Validation identity is the complete, canonical primary-key tuple. Benchmark
integer encodings select placement only; two TPC-C order lines sharing an order
ID retain separate locks and versions. Version checks use a parameterized SELECT
against the full primary key on the owning database, outside the business
transaction's snapshot. Versions use 64-bit values.

Transaction identity pairs the full coordinator ID with a local sequence allocated
from durable reserved blocks; coordinator IDs are not packed into sequence bits. Preserve the `-r` state directory across restarts. Remote
validation is idempotent for identical requests and rejects mismatched payloads;
finalization history is bounded, with old unknown IDs rejected conservatively.

## Running owners

Datasource definitions and all three execution modes are in `config/txnsails.toml`.
Select `--mode single`, `--mode distributed` or `--mode mock`; mock uses one shared
PostgreSQL datasource with real validation and 2PC. See the
[deployment guide](local-distributed-mode.md) for data loading and startup.

For distributed execution, configure named datasources and append
`[[distributed.nodes]]` entries with unique nonnegative integer IDs. IDs can be
sparse, including values above 255; there is no fixed node-count cap. Table owner
list order determines the benchmark key modulo placement. Each middleware process
has its own client and RPC endpoint and selects its ID with `--instance-id`.

```sh
java -jar target/txnSailsServer-1.0-SNAPSHOT-fat.jar \
  -c config/txnsails.toml --mode distributed --instance-id 0

java -jar target/txnSailsServer-1.0-SNAPSHOT-fat.jar \
  -c config/txnsails.toml --mode distributed --instance-id 1
```

The supplied closed catalogue uses `begin#program`, then `execute`, then `commit`
or `rollback`. A failed execution makes the transaction rollback-only. SQLSTATE
`40001` indicates a retryable validation failure; `08007` indicates an unresolved
outcome requiring reconciliation. Do not retry an unresolved outcome as a new
transaction.

For manual recovery, native prepared GIDs are
`<coordinator-id>-<local-sequence>-<owner-id>`. Preserve the state directory and
resolve all branches consistently before a coordinated quiescent restart.

## Verification

`mvn -Dfmt.skip=true test` runs unit and socket-RPC tests. To run the PostgreSQL
suite, provide `TXNSAILS_TEST_PG_PORT` for an isolated PostgreSQL server with a
trust-authenticated `txnsails_test` role allowed to create databases and with
`max_prepared_transactions >= 64`. Tests create/drop their own databases and
launch separate middleware JVMs.

```sh
TXNSAILS_TEST_PG_PORT=5433 mvn -Dfmt.skip=true test
TXNSAILS_TEST_PG_PORT=5433 TXNSAILS_TEST_A_ISOLATION=SI mvn -Dfmt.skip=true test
TXNSAILS_TEST_PG_PORT=5433 TXNSAILS_TEST_A_ISOLATION=SER mvn -Dfmt.skip=true test
TXNSAILS_TEST_PG_PORT=5433 TXNSAILS_TEST_A_ISOLATION=SER \
  TXNSAILS_TEST_VALIDATION_MODE=full mvn -Dfmt.skip=true test
```

These runs test RC/SI, SI/SI, SER/SI, and the full-validation fallback.
Tests cover two-phase commit, rollback, stale remote reads, fractured snapshots,
read-modify-write, read-your-writes, composite primary keys, RPC replay, bounded
history, restart-safe IDs, and injected unknown prepare/commit/rollback outcomes.

## Implementation checklist

- [x] Add regression tests for validation locks, row metadata, preparation,
  worker initialization, RPC, and transaction identity.
- [x] Repair strict distributed validation, merge repeated accesses, and release
  precisely the locks acquired by successful validation.
- [x] Repair participant routing, point SQL validation, template lifecycle, and
  independently configured connection isolation without changing local validation.
- [x] Connect preparation, commit, rollback, and acknowledged remote finalization;
  preserve in-doubt transactions and propagate execution and commit errors.
- [x] Supply optional distributed CLI configuration and a runnable example.
- [x] Run unit tests and real PostgreSQL tests for mixed isolation, atomic commit,
  rollback, stale reads, read-modify-write, and fractured distributed snapshots.
- [x] Obtain an independent code review and resolve material findings.

Verified on PostgreSQL 17.3: the current 110-test suite passed with RC/SI and
SER/SI configurations, including closed-catalogue pruning and full-validation
control cases. All 32 PostgreSQL integration tests passed using the packaged fat
JAR. `mvn package` passed.
Independent code review reported no remaining material
correctness findings within the scope above.

## Local TOML setup

For two middleware processes on one machine, including a shared PostgreSQL
database, see [the TOML deployment guide](local-distributed-mode.md). Deployment
profiles and the fixed workload catalogue can both be configured without XML.
