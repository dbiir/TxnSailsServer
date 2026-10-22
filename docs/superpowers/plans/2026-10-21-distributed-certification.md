# Distributed certification implementation plan

**Goal:** Reduce distributed-deployment certification while preserving global serializability for fixed PostgreSQL RC, SI and SER owners.

**Architecture:** Freeze actual transaction scope and write status before certification. Protect every RW edge touching a multi-database transaction; use strict local reads at RC and for SI read/write transactions, and native SSI for local SER transactions. Preserve separate latest-any and latest-global version watermarks.

**Tech stack:** Java 21, PostgreSQL native two-phase commit, JUnit 5, Maven.

## Correctness review

The proposal's ordering argument is valid with complete access coverage and fixed per-owner isolation. A node-local template cycle analysis alone cannot identify edges that participate in a global cycle. This implementation therefore uses a conservative, catalogue-independent rule: every external read of a local SI transaction that writes is protected. Every SI pivot necessarily writes, so this covers the second edge of every required dangerous structure. Local SI read-only transactions and local SER transactions may omit local–local certification, while still conflicting with global writers.

Actual scope is the final participant count, not whether the deployment has multiple databases. Accesses remain collected until this scope is known. Certification context cannot change after validation starts. All writers publish versions before releasing locks; global-write watermarks survive local overwrites and idle periods. Removing idle entries without a snapshot horizon is unsafe.

PostgreSQL SELECT-only global branches must explicitly start READ WRITE and use native PREPARE/COMMIT PREPARED. PostgreSQL 15–18 conservatively treats these prepared branches as writers in SSI. Ordinary COMMIT is used only for actual single-participant transactions. No physical guard write is required with this contract. See [PostgreSQL 17.3 predicate.c](https://github.com/postgres/postgres/blob/REL_17_3/src/backend/storage/lmgr/predicate.c#L4872-L4892) and the [SSI paper](https://arxiv.org/pdf/1208.4179).

The proposal's referenced `../origin_paper.pdf` is absent; its claimed attribution cannot be verified from this checkout. This review evaluates the argument independently. Existing restrictions remain: existing-row full-primary-key point SELECT/UPDATE only, no bypass writers, and quiescent restart/reconciliation for uncertain outcomes.

## Tasks

- [x] Add certification-policy, compatibility, watermark and replay tests; run them and record missing behavior.
- [x] Add immutable CertificationContext and a dedicated DistributedValidationEntry with constant-time compatibility counters; retain both read and write roles for merged accesses.
- [x] Wire context through Participant, TransactionManager, ValidationRequest and owner replay checking; verify owner isolation matches. Add full/optimized startup selection and explicit BEGIN READ WRITE.
- [x] Add indexed ValidationSet merging and immutable shared SQL/primary-key plans, with regression tests and focused verification.
- [x] Exercise real two-owner PostgreSQL RC/SI and SER/SI schedules, including native SSI with prepared read-only business branches, delayed promotion and global write provenance.
- [x] Run full tests, package and focused formatting; independently review changes and document measured costs and further optimization candidates.

## Validation commands

Run focused tests with `mvn -o -Dfmt.skip=true -Dmaven.gitcommitid.skip=true -Dtest=DistributedCertificationTest test`. Run database schedules with `TXNSAILS_TEST_PG_PORT=<ephemeral-port> mvn -o -Dfmt.skip=true -Dmaven.gitcommitid.skip=true test`; repeat with `TXNSAILS_TEST_A_ISOLATION=SER`. Build using the same flags and `package -DskipTests`. Retain logs outside the repository and stop the temporary PostgreSQL cluster afterward.
