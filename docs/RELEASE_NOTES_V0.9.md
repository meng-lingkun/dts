# QMigration V0.9.0 Release Notes

## Theme

**Schema Dependency & Sequence Semantics**

V0.9 focuses on correctness of non-table schema object migration. It adds View dependency ordering, target drift protection, and PostgreSQL SERIAL/IDENTITY semantics on top of the V0.8 Schema Object Plan/Apply framework.

## Highlights

- MySQL/PostgreSQL View dependency discovery through `information_schema.view_table_usage`.
- Topological Safe Apply ordering for dependent Views.
- Dependency cycles or unavailable dependency metadata become `MANUAL`.
- Existing target Views are skipped only when definitions are provably equivalent.
- PostgreSQL Sequence discovery distinguishes standalone, SERIAL/`OWNED BY`, and IDENTITY-backed sequences.
- SERIAL sequence binding is mapped through QMigration Table/Column Mapping and restored with `OWNED BY` plus column `DEFAULT nextval(...)`.
- IDENTITY-backed source sequences are never converted to plain sequences automatically.
- Sequence binding metadata discovery failure is fail-safe and disables automatic Apply.
- Compatibility assessment and Vue Schema Object tab expose dependencies and binding semantics.

## Safety Rules

- Trigger / Function / Procedure remain `MANUAL`.
- Heterogeneous View SQL conversion remains `MANUAL`.
- IDENTITY auto-creation is not attempted; the target must already contain matching identity semantics.
- View equivalence uses conservative normalized-definition comparison, not an unsafe SQL semantic guess.

## Validation

Release validation includes Go unit tests, `go vet`, six backend binary builds, integration-suite compilation, and isolated Server/Worker smoke tests. Vue source is syntax-checked; full production build still depends on npm registry availability in the build environment.
