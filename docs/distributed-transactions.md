# Distributed transaction repair

The distributed deployment routes primary-key point SELECT and UPDATE statements
to PostgreSQL participants. Every access, including transactions visiting only one
participant, is validated by the middleware owning that participant. Reads retain
their externally observed version and updates retain the version returned by the
database. Reads after an own write do not create an external dependency.

Validation takes shared read locks or exclusive write locks without waiting,
checks external read versions, and holds all locks until every participant has
committed or rolled back. Multiple participants use PostgreSQL two-phase commit.
Unknown commit/rollback outcomes retain transaction state and locks and return an
error rather than success. This repair does not implement durable coordinator decision or validation-lock
recovery. Restart requires all owners to be quiescent and unresolved transactions
to be reconciled; independently restarting an owner while peers continue is not
supported. Startup refuses prepared transactions in any configured database. All conflicting accesses must use this middleware.

Participant isolation is configured independently as RC, SI (PostgreSQL repeatable
read), or SER. Global validation uses the same conservative policy for all three.
Distributed deployment uses explicit per-database isolation settings; the adaptive
single-database isolation switch is disabled in this mode. There is no globally consistent snapshot across independently started branches,
so the single-database SI rule of protecting only consecutive rw dependencies is
insufficient. Any rw anti-dependency in a global cycle can be vulnerable.

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

Transaction IDs contain an instance ID and a counter allocated from durable
reserved blocks. Preserve the `-r` state directory across restarts. Remote
validation is idempotent for identical requests and rejects mismatched payloads;
finalization history is bounded, with old unknown IDs rejected conservatively.

## Running two owners

1. Load the schema and partition data across two PostgreSQL databases. Enable
   `max_prepared_transactions` on both (a server restart may be needed).
2. Edit `config/distributed-example.xml` with JDBC URLs, credentials, RPC hosts,
   and isolation levels. IDs must be unique and between 0 and 7. The order in each
   table's location list determines the existing benchmark key modulo placement.
3. Prepare two workload XML files with `<instanceId>0</instanceId>` and
   `<instanceId>1</instanceId>`. Set `<benchmark>ycsb</benchmark>` for the example.
   JDBC credentials for each owner come from the distributed configuration.
4. Start one middleware server per owner. Give each a separate client port when
   running on the same host; RPC ports come from the distributed configuration.

```sh
mvn -Dfmt.skip=true package

java -jar target/txnSailsServer-1.0-SNAPSHOT-fat.jar \
  -c config/ycsb-owner0.xml -g config/distributed-example.xml \
  -s config/ycsb.sql -l 9876 -r .txnsails -d metas/owner0

java -jar target/txnSailsServer-1.0-SNAPSHOT-fat.jar \
  -c config/ycsb-owner1.xml -g config/distributed-example.xml \
  -s config/ycsb.sql -l 9877 -r .txnsails -d metas/owner1
```

Register templates on each coordinator that will execute them. Each transaction
uses `begin`, `execute`, then `commit` or `rollback`. A failed execution makes the
whole transaction rollback-only. SQLSTATE `40001` indicates a validation failure
that can be retried after rollback; `08007` indicates an unresolved outcome that
requires reconciliation, and must not be blindly retried as a new transaction.
For manual recovery, the PostgreSQL prepared GID is `<transaction-id>-<instance-id>`.
Resolve all branches consistently before restarting the quiescent deployment.

Omit `-g` to use the original single-database mode.

## Verification

`mvn -Dfmt.skip=true test` runs unit and socket-RPC tests. To run the PostgreSQL
suite, provide `TXNSAILS_TEST_PG_PORT` for an isolated PostgreSQL server with a
trust-authenticated `txnsails_test` role allowed to create databases and with
`max_prepared_transactions >= 64`. Tests create/drop their own databases and
launch separate middleware JVMs.

```sh
TXNSAILS_TEST_PG_PORT=5433 mvn -Dfmt.skip=true test
TXNSAILS_TEST_PG_PORT=5433 TXNSAILS_TEST_A_ISOLATION=SER mvn -Dfmt.skip=true test
```

The first run tests RC/SI participants; the second tests SER/SI participants.
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

Verified on PostgreSQL 17.3: 42 tests passed with RC/SI; 42 tests passed with
SER/SI, with middleware processes launched from the packaged fat JAR.
`mvn package` passed. Independent code review reported no remaining material
correctness findings within the scope above.
