# Single, distributed and mock execution

`config/txnsails.toml` contains all three execution modes and the datasource settings.
Java 21 is required. The example client and benchmark configuration generators
require Python 3.11 or newer.

| Mode | Execution | Datasource selection |
| --- | --- | --- |
| `single` | One middleware instance and one ordinary database transaction | `[single].datasource` |
| `distributed` | Logical owners use owner validation, RPC and distributed commit | Each `[[distributed.nodes]]` references a named datasource |
| `mock` | Real distributed branches share one PostgreSQL datasource | `[mock].datasource` is used by every logical owner |

Set root `mode` to choose the default, or pass `--mode single`, `--mode distributed`
or `--mode mock`. The mode controls execution even when a distributed/mock topology
contains only one node. A transaction uses native 2PC when it actually touches
more than one logical owner; one actual participant uses ordinary commit.

## One source for datasource settings

Edit `[datasources.primary]` and `[datasources.secondary]` in the TOML file to set
JDBC URLs, drivers, database types, usernames and passwords. Node sections refer
to these definitions by name. Runtime settings such as auxiliary connection count,
batch size, retry count, terminals and scale factor are in `[runtime]`.

The legacy benchmark-client XML generators read their connection values from this
same TOML file; `TXNSAILS_CONFIG` can select another unified TOML file for those
generators. Server startup uses TOML directly. Schema and closed workload catalogue
paths are relative to the configuration file.

The example sets owner 0 to native SER and owner 1 to SI. Set each node's isolation
to `RC`, `SI` or `SER`; PostgreSQL repeatable read implements SI. Single-mode
concurrency control is selected by `[single].concurrency_control`, including
`SERIALIZABLE`, `SI_TAILOR`, `RC_TAILOR` and `DYNAMIC`.

`validation = "optimized"` enables runtime optimization and static catalogue
pruning; `"full"` retains all read validation. Optimized native SER certification
requires PostgreSQL 15–18. The supplied pair catalogue demonstrates functional
commit; its cross-owner conflicts generally retain global read validation and it
is not a best-case static-pruning workload.

## Prepare PostgreSQL

For distributed or mock transactions with multiple actual owners, enable
`max_prepared_transactions`, for example 128, and restart each PostgreSQL server.
Size it for the maximum simultaneously prepared native branches. See the
[PostgreSQL configuration documentation](https://www.postgresql.org/docs/17/runtime-config-resource.html#GUC-MAX-PREPARED-TRANSACTIONS).
Single mode does not require prepared transactions.

For a **fresh, disposable** primary database, run from the repository root:

```sh
createdb -h 127.0.0.1 -p 5432 -U postgres txnsails_primary
psql -h 127.0.0.1 -p 5432 -U postgres -d txnsails_primary -f config/ycsb.sql
psql -h 127.0.0.1 -p 5432 -U postgres -d txnsails_primary -c \
  "INSERT INTO usertable (ycsb_key, vid, field1) SELECT k, 0, 'initial' FROM generate_series(0,999) AS k"
```

`config/ycsb.sql` drops existing YCSB tables, so use the new database above.
The middleware reads the schema for analysis and does not create tables or load
rows automatically. This primary database can run either `single` or `mock` mode.

For `distributed`, also create `txnsails_secondary` with the same schema. For the
example placement `usertable = [0, 1]`, primary needs even keys and secondary needs
odd keys. To compare modes using the same primary database, its unused odd rows
can remain; middleware still routes odd keys exclusively to secondary in
`distributed` mode. Nodes can reference databases in one PostgreSQL server or
separate servers by changing their datasource URLs.

## Start the selected mode

Build once:

```sh
mvn -Dfmt.skip=true -DskipTests package
```

Single mode uses the default configuration and default mode:

```sh
java -jar target/txnSailsServer-1.0-SNAPSHOT-fat.jar
```

For mock mode, run one command in each terminal:

```sh
java -jar target/txnSailsServer-1.0-SNAPSHOT-fat.jar \
  -c config/txnsails.toml --mode mock --instance-id 0
```

```sh
java -jar target/txnSailsServer-1.0-SNAPSHOT-fat.jar \
  -c config/txnsails.toml --mode mock --instance-id 1
```

For distributed mode, use the same commands with `--mode distributed`. Select
explicit instance IDs when launching multiple processes. Wait for every configured
owner's `listening for clients` message before starting the closed workload.
The example uses client ports 9876/9877 and RPC ports 23333/23334; each process on
the same host needs distinct client and RPC ports. Run one mode at a time with
these example ports. `-l`, `-r` and `-v` override client port, state directory and
validation mode respectively.

## Add nodes without a fixed application cap

Append node entries to the selected distributed or mock section, assign each a
unique nonnegative integer ID, and update table placement lists. IDs can be sparse,
including values greater than 255. There is no fixed eight-node array or packed
coordinator-ID field. Practical scale is bounded by available resources.

For placement `usertable = [7, 256, 1000000]`, the benchmark key modulo three routes
to owners 7, 256 and 1000000 respectively. Every row still has exactly one logical
owner. Business connections and participant state are created only when a client
session first accesses that owner, then reused by that session. Auxiliary
connections are configured per process with `runtime.auxiliary_connections`.

Every owner has a durable sequence checkpoint under the state directory. Preserve
these files across restarts. Transaction identity includes the full coordinator
ID and its local sequence; native prepared branch names also include the owner ID.
Drain transactions, resolve prepared branches and stop all owners before changing
SQL, placement, isolation or the transaction-identity protocol. Restart the complete
topology together; mixed old and new middleware versions are unsupported.

## Run a client

The supplied client supports all three modes:

```sh
python3 scripts/local-distributed-client.py --mode single
python3 scripts/local-distributed-client.py --mode mock
python3 scripts/local-distributed-client.py --mode distributed --coordinator 0
```

For distributed and mock modes, the client executes one write per owner in the
`usertable` placement, through each selected coordinator, then reads the results.
It uses the supplied `write_pair` and `read_pair` templates, repeating statement
slots as needed. The dataset must contain keys `0 .. number-of-owners-1`.
`--coordinator` accepts any configured node ID or `all`.

Each logical transaction stays on one connection. Its coordinator routes all
operations, validates owners and commits the actual native branches. Two
independently committed client connections do not form one distributed transaction.
Existing benchmark clients need explicit `begin#program` binding when using a
closed catalogue in distributed or mock mode.

Mock mode shares PostgreSQL CPU, storage, WAL, native locks and failure domain.
It exercises real routing, RPC, certification and 2PC, but does not measure
cross-machine latency or independent database failures. Native SSI can cause
additional conservative aborts when logical owners share physical pages or tables.
All conflicting accesses must follow the same middleware routing and supported
single-table full-primary-key SELECT/UPDATE restrictions.
