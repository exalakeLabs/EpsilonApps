# Federal + state tax filing: Databricks + Sigma demo

Connection: Sigma connection **exalabs** (`115b0210-65d6-4604-ac51-b135df09df41`), schema `exalabs.tax`.

| File | Purpose |
| --- | --- |
| `ddl.sql` | Schema, 6 dimensions, `fact_return`, 2 large facts (liquid clustering, informational PK/FK `RELY`), 2 gold views |
| `generate_tax_data.py` | Databricks notebook: 750K employees x 5 years, ~56M federal + ~90M state rows (`EMPLOYEES` constant to scale) |

Differences from the planning sheet (deliberate): added `fact_return` (employee x year, ~3.75M rows) so
"complete / in review / amended" is a cheap count rather than COUNT DISTINCT over 150M rows; added `income_type_key`
and `load_ts` to the federal fact (needed for the Federal summary grain and the relative-date demo). Row counts are
generated, not measured: run the notebook's final `count()` cell and quote those.

## Run order
1. Databricks SQL: run `ddl.sql`.
2. Import `generate_tax_data.py`, attach multi-worker compute (smoke test with `EMPLOYEES = 50_000`), run all.
3. In Sigma, tell Claude the tables exist; it then builds the data model + workbook below on the exalabs connection.

## Built workbook
[Tax Filing Dashboard](https://staging.sigmacomputing.io/papercranestaging/workbook/Tax-Filing-Dashboard-4asDFNeSBw9bKXnxu711aA) on the exalabs connection.
Pages: Overview (Q1, Q2), Return status (Q3), Employee drill-down (Q4, default return `2025-1000`), Granular detail (demo 4,
defaults Tax year 2025 and entity Business Unit 001), Materialization targets (the `Federal summary` and `State summary` tables
to schedule for demo 1). Overview and Return status are live (the baseline); repoint them at the materialized
summaries after demo 1. Overview's effective-rate table and combined-rate KPI come from the data model's custom SQL element. Not built: row-level security and the Assistant. All workbook elements now read from the Tax Filing data model below (still live against Databricks, so the baseline for demo 1 is unchanged).

## Built data model
[Tax Filing](https://staging.sigmacomputing.io/papercranestaging/data-model/Tax-Filing-2YiEGHhwJA31MH8S9rRdw) (folder `IOT Apps Asssets`).
Elements: Federal detail, State detail, Returns, Effective rate by entity and year (custom SQL on Databricks, one row per
entity x year, the materialization candidate), plus the Entity, State, Tax line and Employee dimensions with relationships.
Metrics are ratios of sums: Federal tax / Gross income / Federal effective rate, State tax / Apportioned income / State
effective rate, Returns by status, and Combined effective rate (20.9% at last check; federal 17.9%, state 3.2% of apportioned
income). Tax line keys are hard-coded in the metrics (10 federal tax, 1-6 income lines, 106 apportioned, 109 state tax); a
line-code lookup would be cleaner. State share of total is left to the workbook (PercentOfTotal) because a metric version returned null per group.

## Sigma objects to build
**Data model `Tax Filing`**: elements `Federal detail` and `State detail` (relationships to dim_entity, dim_state,
dim_tax_year, dim_tax_line, dim_income_type, dim_employee, fact_return), plus `Federal summary`, `State summary`,
`Effective rate`. Metrics: Federal tax, State tax, Effective rate = (federal+state tax) / gross income (**ratio of sums,
never an average of rates**), State share of total, YoY change.
**Workbook `Tax Filing Dashboard`**: Overview (Q1, Q2), Return status (Q3), Employee drill-down (Q4), Granular detail (demo 4).

## Materialization decisions
| Element | Grain | Materialize? | Reasoning |
| --- | --- | --- | --- |
| Federal summary | entity x year x income type, ~60K | **Yes**, scheduled after the Delta load | Scans ~56M rows for a 60K-row answer; every Overview page reads it. Keep `entity_key` in the grain. |
| State summary | entity x state x year, ~90K | **Yes**, after the federal one | Reads ~90M rows; if it pulls federal AGI from the Federal summary, schedule it after its parent. |
| Effective-rate view | entity x year | **No (inherit)** | Tiny join of two materialized parents; its own copy adds a refresh to fail for no speed. |
| Granular detail views | line level | **No** | Dynamic filters/grouping mean no stable result to store, and 200M rows is expensive to copy. Keep live, require filters, rely on clustering. |
| Row-security / user-attribute elements | any | **Materialize the unfiltered parent; apply the filter in a child** | One shared copy serves everyone. Verify in your tenant how user-attribute filters interact with materialization before the demo. |
| Relative-date filters ("last 30 days") | any | **Materialize without the relative filter; filter in a child** | A relative window moves every day, so a baked-in copy goes stale. Fits `load_ts`/`filed_date`, not `tax_year`. |

## Demo scripts
**1. Materialization scope.** (a) Baseline: Federal summary live, record duration in Query history. (b) Materialize Federal
summary; re-run, compare. (c) Materialize State summary; confirm refresh order. (d) Add a child with an entity row filter;
show it reads the parent's materialization. (e) Put the Entity control on the parent vs the child: parent target reruns on
the warehouse; child target stays on the materialized copy.
**2. Caching.** Run the workbook as a service account (same identity = shared cache). Show Databricks serverless/universal
result cache hits in Query history ("from cache"), then the workbook's cache duration setting; change per-workbook duration
and show the effect. Note that per-user credentials or RLS that changes the SQL text defeat the cache.
**3. Failure / monitoring.** Break the source (rename a column in a copy of `v_federal_summary`, or revoke the service
principal's SELECT), trigger a materialization, show the failure, queries falling back to live data, auto-pause after
repeated failures, then fix and resume from Admin > Materializations.
**4. Granular detail.** Table over `Federal detail` + `State detail` with required filters (tax year, entity), grouping by
state/line, columns from unused dimensions dropped to show join pruning (Query history SQL has fewer joins), prefetch
settings, then open the Databricks query profile: files pruned (liquid clustering on `tax_year, entity_key`) vs read.
**5. Where logic lives**: see below.

## Where logic lives
| Layer | Put here | Why |
| --- | --- | --- |
| **Databricks** | Federal/state calculations, state-starts-from-federal lineage (`federal_agi_ref`), return status, SCD2 employee history, liquid clustering, stats, PK/FK `RELY`, UC row filters for hard security, scheduled loads; optional gold views if the SQL must be shared with other BI | Heavy, stable, shared by every tool; one definition of tax truth |
| **Sigma data model** | Relationships, friendly names, metrics (effective rate, share, YoY), summary elements + materialization schedules, user-attribute entity security for the app, controls, folder/column security | Business-facing, changes often, reviewable by analysts, needs no warehouse deploy |
| **Sigma workbook** | Layout, controls, required-filter UX, drill paths, view-level grouping | Presentation only; keep no business math here |
Rule of thumb: calculate the tax in Databricks, define *how to read it* in Sigma.
