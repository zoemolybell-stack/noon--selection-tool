# Product Identity Migration Plan

Last Updated: 2026-03-29 23:59
Owner: main window
Status: phase-0 design locked, implementation staged

## Goal

Move stage identity from the current legacy model:

`products.product_id PRIMARY KEY`

to the agreed canonical identity:

`(platform, product_id)`

without breaking:
- current stage persistence
- current warehouse import
- current Web read models

## Current State

Current stage schema in `config/product_store.py` still uses:
- `products.product_id TEXT PRIMARY KEY`

Dependent stage tables currently reference `product_id` only:
- `price_history`
- `rank_history`
- `sales_snapshot`
- `crawl_observations`

Current compatibility guard already in place:
- cross-platform stage writes with the same `product_id` are blocked instead of silently overwriting existing rows
- observation duplicate checks already include `platform`

This means the current risk is contained, but the schema is still not converged.

## Target State

Target stage identity model:
- `products` keyed by `(platform, product_id)`
- downstream stage tables carry the same composite identity
- all joins between stage tables use both:
  - `platform`
  - `product_id`

Warehouse identity remains unchanged from the current contract:
- `product_identity PRIMARY KEY (platform, product_id)`

## Staged Migration

### Phase A — Preparation

Do now, without breaking runtime:
- keep the existing collision guard active
- keep platform-aware observation dedupe active
- document the migration path and affected tables
- add compatibility readers that prefer `(platform, product_id)` whenever available

Do not do yet:
- no destructive schema migration
- no table rename
- no foreign-key rewrite in place on the live stage DB

### Phase B — Additive Schema Expansion

When category and keyword runtime stabilization is complete:
- add platform-aware shadow structures or additive columns where required
- backfill `platform` into all identity-dependent stage tables
- add composite indexes for:
  - `(platform, product_id)`
  - lookup paths that currently depend on `product_id` only
- update stage reads and writes to use composite identity first

Compatibility rule during this phase:
- old `product_id`-only reads may remain only as compatibility fallback
- new writes must be composite-aware

### Phase C — Cutover

After all stage writers and readers are composite-aware:
- migrate `products` to true composite primary key
- remove reliance on `product_id`-only joins
- keep warehouse import contract stable

Cutover gate:
- keyword runtime
- category runtime
- warehouse import
- Web read path
must all already be compatible with `(platform, product_id)`

## Migration Constraints

- Do not rename stable warehouse public tables/views.
- Do not make Web depend on stage DB internals.
- Do not change keyword/category runtime ownership boundaries during this migration.
- Any shared-resource or schema mutation must be declared in `DEV_HANDOFF.md` first.

## Minimum Acceptance Criteria

The migration is not considered complete until:
- stage writes no longer risk cross-platform identity collision
- stage reads no longer require `product_id`-only joins
- warehouse import remains stable
- Web output contract remains unchanged
- all three windows can read the same migration status from `DEV_HANDOFF.md`
