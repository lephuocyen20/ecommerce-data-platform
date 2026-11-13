# Gold Iceberg dimensional model v1

Gold is the canonical lakehouse analytical layer: conformed dimensions and facts are canonical; report marts are derived.

| Table | Grain | Inputs |
|---|---|---|
| `gold.dim_date` | calendar date | deterministic generator |
| `gold.dim_customer` | customer (Type 1) | Silver customers |
| `gold.dim_product` | product (Type 1) | Silver products |
| `gold.fact_orders` | one row per order | Silver orders, order_items, customers |
| `gold.fact_order_items` | one row per order item | Silver order_items, products |
| `gold.fact_payments` | one row per payment | Silver payments (`status` → `payment_status`) |
| `gold.fact_shipments` | one row per shipment | Silver shipments |

`fact_orders` holds order-level measures only. Shipping fees remain shipment-grain and must not be copied onto order items. Unknown dimensions use surrogate key `-1`; Type 2 is explicitly out of scope for v1. All seven outputs have a declared business grain and merge key in `gold/implementation/dimensional/contracts.py`; measures are additive unless their contract says otherwise.

## Lifecycle revenue semantics

Base monetary measures remain non-negative and are never physically removed.
`fact_payments.signed_revenue` carries payment lifecycle direction: `REFUNDED`
is negative and other payment statuses are positive. `fact_orders` deliberately
does not contain `signed_revenue`; order cancellation/deletion sign ownership is
an unresolved business decision and must not be inferred or added without an
approved contract. Native transformations preserve raw/base amounts and do not
perform a second discount/refund subtraction.

## Semantic parity and sign-off

Dimensional reconciliation helpers compare deterministic rows at the declared
grain for orders, order items, payments, and shipments. They fail closed on
missing/extra columns, missing or duplicate keys, mismatched measures, and
invalid lifecycle signs. These are code-level assertions only; business and
analytics owners must still sign off metric ownership, additivity, and downstream
parity before consumer cutover.

`fact_payments.order_id` and `fact_shipments.order_id` are mandatory logical
foreign keys to `fact_orders.order_id`; quality validation rejects orphan rows.
Shipment lifecycle remains status-only and shipment-grain fees are not copied to
orders or order items.

# Native Iceberg writer

`gold.implementation.dimensional.writer` materializes the declared graph in the
fixed order `dim_date -> dim_customer -> dim_product -> fact_orders ->
fact_order_items -> fact_payments -> fact_shipments`. It reads only verified
Silver Parquet completion manifests, then writes the declared Gold targets as
Iceberg tables. It never reads a mutable Silver table head.

The writer defaults to `DRY_RUN`.  A real `WRITE` additionally requires
`GOLD_DIMENSIONAL_WRITE_ENABLED=true`; it creates only allowlisted
`lakehouse.gold` tables with Iceberg format version 2 and uses `MERGE` on each
declared grain.  `--dim-date-start` and `--dim-date-end` are mandatory and are
included in the deterministic graph identity.

Each written target has a content-addressed table manifest under
`gold/dimensional/tables/v1/`; the completed graph receives an immutable graph
run manifest (referencing all seven table manifests) under
`gold/dimensional/graphs/v1/`.  If a write fails after any table
manifest is published, treat the next attempt as `PARTIAL_REQUIRES_RECONCILIATION`;
only a graph manifest authorizes an `ALREADY_PUBLISHED` outcome.

Before `WRITE`, the runner preflights the graph manifest and deterministic
per-table markers. Any marker without a completed graph blocks all `MERGE`
operations. A create-once graph fence is then acquired and rechecked before
each table write and graph publication. Fences are intentionally not released
automatically: an interrupted writer requires explicit operator reconciliation.
An additional transient global fence serializes all graph identities because
they share the same physical `lakehouse.gold` targets; it is released only
after a verified completed graph manifest.

The executable adapter is `python -m gold.implementation.dimensional.run_pipeline`.
It requires `--bucket`, a lineage JSON path and both date arguments; Spark and
MinIO are initialized only when the adapter is actually invoked.

`tests/run_gold_dimensional_iceberg_integration.py` is an opt-in disposable
integration harness only.  It is intentionally not collected or executed in
CI; run it exclusively with an isolated Nessie ref and disposable bucket.

## Local runtime evidence

Local-only validation covered the seven-output graph with Gold `DRY_RUN`, first
`WRITE`, and same-lineage `ALREADY_PUBLISHED` retry evidence. The result is
**PASS WITH WARNINGS** for the bounded local scope. No production routing,
consumer cutover, shared bucket, or production certification is implied.
