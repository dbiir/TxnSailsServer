# Distributed static vulnerable-dependency plan

**Goal:** Omit runtime read certification when a complete distributed workload proves the read unnecessary for serializability.

**Architecture:** Opt-in immutable startup catalogue (`-w`) binds each transaction to one declared program. Build a separate owner-labelled dependency graph including same-program concurrent instances and all RW/WR/WW alternatives. Its strongly connected components produce conservative global/RC/SI read masks. Keep every write and existing lock/version/publication/recovery semantics.

**Contract:** All owners use identical catalogue, schema, routing and isolation fingerprints. Before BEGIN the coordinator checks every owner; validation requests also carry the fingerprint, including empty-item requests. Closed mode rejects unknown or mixed programs before business SQL. Declared slots may execute in any order, repeat, or be omitted; owner domains are enforced. No file retains the dynamic protocol and its existing certification policy. Full mode preserves the catalogue contract but retains every access. Catalogue changes require the existing coordinated quiescent restart.

## Analysis and proof

Vertices represent complete program types, implicitly identifying their possible database branches. Dependency arcs carry the owner on which a conflict can occur. All self-template conflicts are retained. The old graph builder's omission of self edges and WW-based removal of RW edges are not used.

For each internal RW arc in an SCC, mark its reader RC-relevant. Mark its reader SI-relevant when an internal incoming RW arc enters that program on the same owner. Mark its reader globally relevant when the SCC contains internal dependency arcs on at least two owners. SER has no local middleware obligation. At runtime retain a read when its global mask or the applicable native RC/SI mask is set.

Every real multi-owner cycle projects into an SCC with at least two dependency owner labels; its needed global RW reads survive. Single-owner RC/SI cycles retain their cycle RW/second RW reads; PostgreSQL SSI handles single-owner SER cycles, including explicitly READ WRITE prepared global branches. Runtime classification and broad version buckets for retained reads remain unchanged. Keeping all writes avoids needing writer-class watermarks. SCC analysis deliberately overapproximates cycles and keeps complementary RW/WR alternatives; repeated statement sites therefore do not weaken the argument.

## Tasks

- [x] Add tests and implement immutable owner-labelled SCC masks, including same-template and mixed-node cycles.
- [x] Add closed XML catalogue parsing, startup validation, deterministic fingerprinting and template installation.
- [x] Add catalogue agreement RPC and replay fingerprint checks; reject mismatched deployments before BEGIN.
- [x] Bind begin/execute to declared programs and owner domains; filter only statically unmarked read items.
- [x] Test omitted reads with real PostgreSQL, retained global anomalies and local SI pivots, rejection paths, full mode and dynamic compatibility.
- [x] Run focused formatting, full tests/build and independent review; document scope, precision and remaining costs.

Run checks with `mvn -o -Dfmt.skip=true -Dmaven.gitcommitid.skip=true test`; PostgreSQL fixtures additionally use `TXNSAILS_TEST_PG_PORT` and `TXNSAILS_TEST_A_ISOLATION`. Preserve all preexisting working-tree changes and the user's `distributed_mode_opt.md`.
