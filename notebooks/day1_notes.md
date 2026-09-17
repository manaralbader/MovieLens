# Day 1 Notes — Environment, Raw Data, and Bronze

Goal for today: prove the environment actually works, look at the raw movie
data before touching it, and prove that Bronze can safely hold raw,
un-cleaned data forever. No cleaning yet — that's Day 2.

Mapped step by step to what the course's Day 1 slides actually cover.

## Step 0 — Start a Spark + Delta session

Booted a local Spark session (`local[2]`, just this laptop, no real cluster)
with Delta Lake turned on via the two `spark.sql.extensions` /
`spark.sql.catalog` config lines.

**Maps to:** Spark is the "elastic compute engine" the slides describe —
here it's running on one machine instead of a real cluster, but it's the
same engine PySpark wraps. Turning on Delta at session start is what makes
everything from here on a **Lakehouse** table instead of a plain file: Delta
adds the ACID transaction log (`_delta_log`) on top of ordinary Parquet
files, which is the whole difference between "data lake" and "lakehouse."

## Step 1 — Load the raw CSVs, look at the inferred schema

Read all 4 CSVs (`ratings`, `movies`, `tags`, `links`) with
`inferSchema=True` and printed what Spark guessed for each column.

**Maps to:** this is **schema-on-read** in action — Spark scans the file and
decides types *at read time*, instead of a schema being enforced *before*
the data is allowed to exist (that's schema-on-write, the warehouse way).
Confirmed exactly what the theory predicts a raw file looks like:
`timestamp` came in as a plain integer, not a real date, and `genres` came
in as one flat string (`Comedy|Romance`) instead of a list. Nothing has been
interpreted or cleaned — Spark just described what's already there.

**Caveat worth flagging:** `inferSchema=True` already makes Spark decide
`rating` is a `double` and `timestamp` is an `integer` before the data even
reaches Bronze. A stricter reading of "schema-on-read" would keep every
column as plain text and defer *all* typing decisions to Silver's explicit
type step (Day 2). We let Spark quietly make some of those calls early —
a reasonable simplification, but a real blurring of the line the theory
draws, not something to gloss over.

## Step 2 — Profile: row counts, missing values, key uniqueness

Counted rows, counted nulls per column, and checked whether each table's
"identity" column is actually unique.

**Result:** everything came back clean except `links.tmdbId`, which has 8
missing values (0.08%) — a real, small data-quality issue sitting in the
raw data, left untouched in Bronze on purpose. Key uniqueness held for
`movies.movieId`, `links.movieId`, and `ratings.(userId, movieId)` — 0
duplicates in all three.

**Maps to:** this is the "look before you touch" instinct the whole
Bronze/Silver/Gold split is built around — you can't design a sensible
Silver cleaning/dedup rule until you know what's actually wrong with the
raw data, rather than assuming.

## Step 3 — Write the "why lakehouse" note

Wrote a short paragraph explaining why we're layering Bronze → Silver →
Gold instead of one cleaning script: **auditability** (every stage is an
inspectable checkpoint) and **reproducibility** (a broken stage can be
fixed and rerun on its own, without redoing everything, and without needing
the original source data to still exist).

**Maps to:** this is a smaller, single-project version of the "Two-Tier
Problem" section — that section is really about *multiple downstream
consumers* (BI + AI) silently drifting apart. We only have one pipeline and
no second consumer to drift from, so that specific problem doesn't apply
here. What *does* carry over is the underlying principle: keeping the raw
layer around is what makes any later mistake fixable instead of permanent.

## Step 4 — Write Bronze Delta tables with ingestion metadata

Wrote 4 Delta tables (`bronze_ratings`, `bronze_movies`, `bronze_tags`,
`bronze_links`), each the original data plus 3 new columns: `batch_id`
(shared across all 4 tables for one ingestion run), `ingested_at` (real
timestamp of the write), and `source_file` (which CSV each row came from).

**Maps to:** this is **ELT**, not ETL — raw data landed in Bronze first,
structurally unmodified, and any transformation happens afterward on Spark,
not before. The `ingested_at` / `source_file` pattern is exactly the
`_ingested_at` / `_source_file` metadata the slides describe for Bronze.
Using `.format("delta")` instead of plain Parquet/CSV is what gives these
tables a real transaction log — confirmed by finding the actual
`_delta_log` folder on disk, and by testing that `RESTORE TABLE` /
`DELETE ... WHERE batch_id = ...` are real, working undo options.

## Step 5 — Replay the same batch, prove Bronze grows

Reran the exact same ingestion cell a second time and confirmed every
table's row count doubled (e.g. `ratings`: 100,836 → 201,672), with 2
distinct `batch_id`s showing up — nothing got deduped, overwritten, or
rejected.

**Maps to:** this is the "**never mutate Bronze — it's append-only**" rule,
demonstrated rather than just stated. Important distinction from the
theory's *other* rule, "every promotion is idempotent": that rule is about
**Bronze → Silver / Silver → Gold jobs**, not raw ingestion — rerunning a
promotion job should give the identical result every time, but rerunning
raw ingestion is *supposed* to grow Bronze. Day 1 tests the growth rule;
Day 2's dedup logic is what has to deliver the idempotency rule, once there's
an actual promotion step to rerun.

## What's next (Day 2)

- Explicit typing (real timestamps, `genres` split into an array) — the
  schema-on-read gap flagged above gets closed here.
- Dedup on `(userId, movieId)` with a documented precedence rule — this is
  where "every promotion is idempotent" actually gets built and tested.
- Referential integrity check against `movies`.

## Simplifications made on purpose

- **No two-tier BI/AI split** — one pipeline, one eventual Gold layer
  (Day 5). Not needed for a single-purpose project.
- **No decoupled storage/compute cost model** — everything runs on one
  laptop; there's no cloud bill to optimize. The medallion architecture
  itself doesn't depend on scale, which is exactly why it's meaningful to
  test at 100K rows.
- **No PDPL/governance/PII masking** — MovieLens `userId`s are already
  anonymized integers, nothing sensitive to protect.
- **No partitioning strategy** — each Bronze table is one unpartitioned
  Delta table, which is correct at this size; the "don't over-partition by
  high-cardinality `trip_id`" warning only bites at real scale.
