# Distributed certification optimization

The proposal is correct under fixed per-database isolation, complete middleware
coverage, monotonic row versions and PostgreSQL native two-phase commit. The
implementation uses a conservative refinement of the proposed SI rule that does
not require a complete globally replicated template catalogue.

## Runtime policy

A global transaction visits more than one database. A local transaction visits
exactly one, even in a distributed deployment. Classification occurs after all
business statements and before the first validation lock. Earlier accesses remain
available if a later statement adds a participant or turns a read-only SI
transaction into a read/write transaction.

| Transaction | External reads certified against | Conflicting writers excluded |
| --- | --- | --- |
| Global, any native isolation | latest committed version | all writers |
| Local RC | latest committed version | all writers |
| Local SI with business writes | latest committed version | all writers |
| Local SI read-only | latest global overwrite | global writers |
| Local SER | latest global overwrite | global writers |

All writes publish the latest row version. Global writes also publish a separate
global-write watermark. A relaxed read of version `r` fails if the global-write
watermark exceeds `r`; a strict read fails if the latest version differs from
`r`. Relaxed reads do not need an auxiliary database SELECT on a cold entry.
Read/write items retain both roles. Write/write exclusion remains conservative.
Versions are published before locks are released, after all branches finish.

The watermark must survive subsequent local overwrites. For example, a global
write produces version 1, then a local write produces version 2. A local SER
reader of version 0 must still fail; a reader of version 1 can serialize before
the local writer. An entry with no current holders cannot simply be evicted:
an older active SI/SER snapshot may still need its global-write watermark.

## Why serializability is preserved

For any protected anti-dependency `U ->rw V` at participant `p`, validation
ensures `c_p(U) < v_p(V)`, where the effective Yes vote `v_p` includes both
middleware certification and successful native prepare. Let `C(T)` denote the
global commit decision (ordinary commit for a local transaction). Then
`C(U) <= c_p(U) < v_p(V) <= C(V)`. WR and WW edges also advance decision order:
the predecessor's local commit makes the value visible, and follows its decision.
This remains true when prepared branches finish at different times.

Suppose a committed global cycle exists, and choose its earliest-deciding
transaction `Y`. Its incoming edge `X -> Y` must be an unprotected RW edge.
Therefore X and Y are local to the same database. Under RC the implementation
protects this edge directly. Under SI, snapshot ordering forces the preceding
edge `Z -> X` to be RW as well. X necessarily writes, so every external read
of X is protected, including `X ->rw Y`, a contradiction. This covers the second
edge even when Z is global. Under SER, PostgreSQL SSI rejects the corresponding
dangerous structure, subject to the native SSI contract below. The usual
read-only timing refinement applies only to genuinely local read-only transactions.

The existing per-coordinator template analysis cannot establish that an edge is
absent from every global cycle. Reusing its local flags to skip distributed
certification would leave a coverage gap. Protecting all local SI read/write
reads is sufficient because every dangerous-structure pivot necessarily writes.
It may protect more edges than a complete global static analysis would require.

## PostgreSQL SSI and read-only branches

Every branch explicitly begins `READ WRITE`. Every branch of a global transaction,
including SELECT-only business branches, uses `PREPARE TRANSACTION` and
`COMMIT PREPARED`. PostgreSQL's two-phase SSI finish code conservatively sets
`MyXactDidWrite = true`, retaining these branches as writers for dependency
tracking. Explicit READ ONLY and ordinary-commit implicit-read-only optimizations
must not classify these global branches as local read-only transactions. Thus
no auxiliary guard INSERT is necessary under this native two-phase contract.

This behavior was inspected in PostgreSQL 15–18. Optimized mode refuses a SER
owner outside that range; `-v full` retains the conservative policy. Real database
tests use PostgreSQL 17.3. Relevant primary sources are
[PostgreSQL 17.3's predicate locking implementation](https://github.com/postgres/postgres/blob/REL_17_3/src/backend/storage/lmgr/predicate.c#L4872-L4892),
[native prepare's XID assignment](https://github.com/postgres/postgres/blob/REL_17_3/src/backend/access/transam/xact.c),
and [Serializable Snapshot Isolation in PostgreSQL](https://arxiv.org/pdf/1208.4179).

The proposal references `../origin_paper.pdf`, which is absent from this checkout.
That attribution could not be verified; the argument above was reviewed independently.

## Cost and remaining opportunities

The optimization reduces validation conflicts and fresh-version queries. It does
not eliminate recording all potentially relevant point accesses: arbitrary future
participants and global writers require that coverage. The owner stores one row
entry with two watermarks and active-holder counters; each transaction retains
one item per distinct row until completion. No explicit dependency graph is built.

Three additional costs were removed during implementation:

* ValidationSet uses a full-row-identity hash index alongside its ordered item
  list. Building/merging n accesses changes from quadratic comparisons to expected
  linear work; the ordered list preserves deterministic replay and indexed access.
* Dedicated row entries use aggregate counters for compatibility, replacing a
  scan over all active readers. Acquiring h read holders changes from quadratic
  aggregate work to expected linear work. Entries avoid the unused legacy local
  lease/lock-policy fields and reuse the acquired entry for publish/release.
* Immutable eligibility, rewritten SQL, sorted primary-key parameter positions
  and version-query plans are shared across template clones. Successful and
  rejected eligibility checks are cached per schema identity. Final RPCs carry
  only the decision; the owner already retains the validated items.

Standalone JVM microbenchmarks compare baseline `baceb9f` algorithms with the
current implementation, excluding JDBC and network work. Five warmup rounds and
the median of seven samples gave these representative costs on the development
machine. Merge inputs used prebuilt canonical full-primary-key identities and
two passes over n rows (one insertion pass, one repeated-read pass).

| Operation | n | Baseline ms | Optimized ms |
| --- | ---: | ---: | ---: |
| Build/merge validation set | 1,000 | 3.826 | 0.291 |
| Build/merge validation set | 10,000 | 389.883 | 1.670 |
| Acquire/release readers on one row | 1,000 | 0.866 | 0.162 |
| Acquire/release readers on one row | 10,000 | 77.235 | 0.991 |

These are isolated operation measurements, not transaction throughput claims.
Small sets can pay more hash-map/allocation overhead: the 100-row sample measured
0.043 ms baseline versus 0.167 ms indexed, with limited JIT warmup at that size.
A separate warmed plan benchmark measured about 223 microseconds per repeated
eligibility parse versus 8 nanoseconds per cached plan lookup; database round
trips will dominate much of the actual transaction cost. The index also adds
memory per distinct access, so small/large transaction distributions should be
measured before introducing a small-set hybrid threshold.

The next useful optimizations require broader measurements:

* One synchronized RPC client currently serializes requests to each remote owner.
  A bounded connection pool or multiplexed protocol could remove this bottleneck;
  it must preserve request identity, uncertain outcomes and replay behavior.
* Java object serialization still copies validation payloads and primary-key
  strings. A compact tuple/binary encoding could reduce allocation and bytes,
  provided it preserves complete key identity and immutable replay comparisons.
* Sequential participant validation/prepare adds round-trip latency. Parallelism
  needs careful all-branch completion tracking before rollback or release.
* Watermark retention can grow with the number of touched rows. Reclamation needs
  a coordinated oldest-snapshot/transaction horizon; idle-holder eviction is unsafe.
* The optional [closed global catalogue](distributed-static-analysis.md) now narrows
  read collection using distributed/static RC/SI masks. Finer key and column
  constraints could reduce its conservative overapproximation further.

Existing supported-SQL and recovery restrictions in
[distributed-transactions.md](distributed-transactions.md) still apply.
Upgrade all owners together during a quiescent restart: validation requests now
include the immutable certification context and are not a rolling-upgrade wire contract.

## Verification

All 69 tests passed independently with RC/SI, SI/SI and SER/SI owners, and with
the conservative `-v full` fallback on SER/SI. Tests include late scope/write
promotion, global watermark persistence across local overwrites, optimized RPC
disconnect/replay, and the proposal's four-transaction global cycle. Native SSI
rejects the local pivot and its connection is reusable afterward. A direct native
SSI test distinguishes ordinary read-only business COMMIT from READ WRITE native
two-phase completion. The database read-only-default test verifies that business
branches explicitly override that default.

This exposed and fixed a preexisting commit-error issue: a single-participant
native COMMIT rejected with `40001` or `40P01` is a definite abort, so the owner
can finalize without publishing writes and the client can retry. Unknown outcomes
and prepared branches still retain locks/state and report `08007`.

The fat JAR builds successfully. Independent reviews found no remaining material
correctness issue within the documented assumptions; finite tests supplement the
ordering argument and do not replace it.
