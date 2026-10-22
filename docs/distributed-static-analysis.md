# Static vulnerable dependencies across databases

Start every middleware owner with the same complete workload catalogue. The
`distributed.workload` and `mock.workload` settings in `config/txnsails.toml`
reference `config/distributed-workload.toml`. An owner is the middleware instance
responsible for a logical partition and its validation metadata. Different owners
can share one physical PostgreSQL database if every row still has a unique owner
under identical routing. Native isolation is fixed per logical owner. The
coordinator may execute a transaction against several owners. For a TOML-only
deployment and catalogue, see [Local distributed mode](local-distributed-mode.md).

This mode replaces broad read collection with a conservative static selection.
Only reads marked as potentially vulnerable become runtime validation items.
Every write remains tracked for locking and committed-version publication.

## Catalogue contract

The catalogue declares complete transaction programs, their SQL statement sites,
and the databases each site can access. All owners must agree on the same
programs, schema, placement order and native isolation levels. Example:

```toml
version = 1
closed_world = true

[[programs]]
name = "read_pair"
[[programs.statements]]
id = 0
operation = "read"
relation = "usertable"
sql = "SELECT field1 FROM usertable WHERE ycsb_key = ?"
owners = [0]
[[programs.statements]]
id = 1
operation = "read"
relation = "usertable"
sql = "SELECT field1 FROM usertable WHERE ycsb_key = ?"
owners = [1]

[[programs]]
name = "write_a"
[[programs.statements]]
id = 0
operation = "write"
relation = "usertable"
sql = "UPDATE usertable SET field1 = ? WHERE ycsb_key = ?"
owners = [0]
```

`owners` is optional and defaults to every configured location of the relation.
Explicit owner lists must be nonempty subsets of those locations. Statement IDs
start at zero and are contiguous; the existing template capacity is 15 distinct
statement sites per program. SQL retains the existing full-primary-key point
SELECT/UPDATE restrictions. A declared site may execute repeatedly, be omitted,
or execute in a different order: the analysis does not depend on cardinality or
statement order.

In closed-catalogue mode, templates are installed at startup. Use:

```text
begin#read_pair
execute#read_pair#0#0
execute#read_pair#1#1
commit
```

The example assumes keys 0 and 1 route to owners 0 and 1. Unknown programs,
mixing another program's statement into the transaction, invalid statement slots,
and accesses outside a declared owner domain are rejected before business SQL.
Execute violations make the entire transaction rollback-only. Online `register`
and `analysis` are disabled in this mode. Changing the catalogue requires a
coordinated quiescent restart.

Omit the selected mode's `workload` setting to use dynamic registration and the
existing certification policy. With a catalogue configured, `validation = "full"`
retains every read but still enforces the catalogue contract. An unknown transaction cannot silently fall back while other
transactions use static pruning: its WR/WW edges could close a cycle whose only
backward RW edge was already omitted between known transactions.

## Dependency graph and masks

The new analyser is separate from the legacy single-database cycle finder. A
vertex represents a complete program, implicitly joining its possible database
branches. Conflict edges are labelled by their database owner. It retains all
potential RW, WR and WW edges, including conflicts between different concurrent
instances of the same program. It never removes an RW edge merely because a WW
edge exists.

For every read statement and allowed owner, it computes these masks:

| Mask | Conservative criterion |
| --- | --- |
| Global | Its RW edge belongs to a strongly connected component containing dependency edges on at least two owners. |
| Local RC | Its RW edge is internal to a strongly connected component. |
| Local SI | Its RW edge is internal to a component, and an incoming RW edge enters its program on the same owner. |

SER needs no local middleware mask. A read is collected if its global mask is set,
or its local RC/SI mask applies to that owner's fixed isolation. Retained reads
use the existing actual global/local transaction classification and version
checks. All writes remain in the validation set. Omitted reads avoid full-key
identity construction, validation locks/checks and read-item RPC payloads; their
SQL still returns `vid` through the cached execution plan.

Only **dependency edge** owner labels count. A program merely having a branch on
a second database does not make a local dependency component global. In the TOML
example, writes occur only on owner 0. With owner 0 at SER and owner 1 at SI, both `read_pair` sites
can be omitted: all conflicting edges are on A, where native SSI suffices. Adding
a program that writes both owners creates potential cross-node cycles, so the
relevant read sites are retained.

Strongly connected components deliberately overapproximate feasible cycles. The
graph keeps complementary RW/WR alternatives and repeated program instances;
this can retain extra reads. It does not claim minimal vulnerable sets. Because
all generated dependency alternatives are bidirectional, union-find computes
the exact SCC partition without enumerating exponentially many simple cycles.
Dependency generation is quadratic in statement-site count in the worst case;
grouping sites by relation/owner is a further startup-cost optimization.

`ClosedWorkload.readMasks()` exposes immutable per-site results for review.

## Why omission is safe

Every admitted runtime transaction belongs to one catalogue program, and every
actual dependency maps to a retained static edge on its actual owner. Distinct
instances of one program map to self edges. Repeated or omitted statements do
not invalidate this mapping.

A cycle involving dependency edges on multiple databases projects into one SCC
with multiple owner labels. Thus its relevant RW read sites receive the global
mask, including reads from local transactions involved in L-to-G conflicts.
The [earliest-commit ordering argument](distributed-optimization.md#why-serializability-is-preserved)
then applies: protected global-endpoint RW edges advance the commit decision;
the remaining local RC edge or SI second-RW edge is retained by its local mask;
local SER dangerous structures are rejected by native SSI. A cycle confined to
one database is likewise covered by the RC/SI masks or native SSI. The SI pivot
necessarily writes, so its retained read gets strict runtime validation.

The PostgreSQL READ WRITE/native-two-phase contract remains essential for global
SER branches. Every actual database participant is preserved even when its
validation set is empty. Such global branches still validate through their owner
and execute native PREPARE/COMMIT PREPARED. Filtering read items never changes
actual participant count or releases locks early.

## Agreement and failure behavior

A SHA-256 fingerprint covers the analysis version, program SQL and owner domains,
schema columns/types/keys, routing order, database identities and resolved native
isolation. Credentials and the local coordinator ID are excluded. The coordinator
checks every configured owner before its first closed-mode BEGIN and caches only
successful agreement. Each validation RPC, including an empty-item request,
also carries the fingerprint. Missing or mismatched fingerprints are rejected.
Replay checks retain the fingerprint and certification context.

This is a fixed analysis epoch, not a rolling catalogue update protocol. The
existing quiescent-restart and uncertain-outcome recovery limitations still apply.
Dynamic catalogue changes would require an admission barrier, agreement on the
new graph, and drainage of transactions using old pruning decisions before new
programs become executable.

## Verification

On PostgreSQL 17.3, the full 110-test suite passed with RC/SI and SER/SI legacy
owners; each run also exercised the closed-catalogue SER/SI fixtures. The 12 new
real-database tests compare a safe omitted read with the full-validation control,
retain fractured-snapshot and SI-pivot failures, check repeated/omitted slots,
and reject unknown programs, mixed programs, wrong slots/owners, malformed execute,
online registration and mismatched catalogues. Another 29 unit/socket tests cover
the analyser, catalogue compiler, immutable masks and empty validation requests.

The pruning fixture selects 0 of 2 read-site/owner pairs. Adding cross-node writes
and an SI pivot selects 3 of 3. These counts demonstrate static selection; they
are not throughput measurements. Actual runtime reduction depends on which
declared statements and owner domains the workload uses.

The fat JAR builds, and all 32 PostgreSQL integration tests pass with owners
launched from that JAR. Independent review found no material correctness issue
within the fixed-catalogue and native PostgreSQL contract.
