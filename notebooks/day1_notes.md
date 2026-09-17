# Day 1 Notes — Environment, Raw Data, and Bronze

Goal for today: prove the environment actually works, look at the raw movie
data before touching it, and prove that Bronze can safely hold raw,
un-cleaned data forever.

## Step 0 — Start a Spark + Delta session

Booted a local Spark session (`local[2]`, just this laptop, no real cluster)
with Delta Lake turned on via the two `spark.sql.extensions` /
`spark.sql.catalog` config lines.

**Why this matters:** Spark is a distributed processing engine — its whole
design is to split one big job into pieces and run them across many
machines at once. In a real company deployment, more machines get added
automatically when there's more work, and removed when there isn't ("elastic"
just means the amount of compute stretches to match demand instead of being
one fixed-size server running 24/7). Here we're deliberately using the
tiny, single-laptop version of that same engine (`local[2]` = 2 cores, one
machine) — same software, none of the scaling.

Turning on Delta at session start adds something Spark doesn't have by
default: a **transaction log**. Without it, writing a file to disk is just
"the bytes are there or they're not" — if a write gets interrupted halfway,
you can end up with a corrupted, half-written file and no way to know. A
transaction log records every change as an atomic, ordered entry (a
"version"), so a reader either sees the complete old version or the
complete new one, never something in between. That log is the actual
mechanical difference between plain files on disk and a proper database-like
table — it's what makes rollback, retry-safety, and "what did this table
look like yesterday" all possible.

## Step 1 — Load the raw CSVs, look at the inferred schema

Read all 4 CSVs (`ratings`, `movies`, `tags`, `links`) with
`inferSchema=True` and printed what Spark guessed for each column.

**Why this matters:** there are two different philosophies for when a
system checks that your data has the right shape (right column names, right
types):
- **Schema-on-write** — the check happens *before* data is allowed in at
  all. A traditional database works this way: try to insert text into a
  number column, and it's rejected on the spot.
- **Schema-on-read** — no check happens on the way in. Data just gets
  stored as-is (however messy), and whatever tool reads it later decides
  how to interpret it.

`inferSchema=True` is schema-on-read in action: Spark opens the file,
scans through the values, and *guesses* a type per column, after the fact.
Nothing was validated or rejected — Spark just described what it found.
This is exactly why our raw data still looks messy: `timestamp` came back
as a plain integer (not an actual date), and `genres` came back as one
string like `"Comedy|Romance"` (not a list of genres) — because nobody told
Spark those columns mean anything beyond "some numbers" / "some text."

**Caveat worth flagging:** `inferSchema=True` still makes Spark decide
`rating` is a `double` and `timestamp` is an `integer` before the data even
reaches Bronze. A more strictly "raw" Bronze layer would keep every column
as plain text and defer *all* typing decisions to a later, explicit step
(Day 2). We let Spark quietly make some of those calls early — a reasonable
shortcut for a learning project, but worth being honest that it's not
perfectly raw.

## Step 2 — Profile: row counts, missing values, key uniqueness

Counted rows, counted nulls per column, and checked whether each table's
"identity" column is actually unique (no two rows claiming to be the same
movie/rating).

**Result:** everything came back clean except `links.tmdbId`, which has 8
missing values (0.08%) — a real, small data-quality issue sitting in the
raw data, left untouched in Bronze on purpose. Key uniqueness held for
`movies.movieId`, `links.movieId`, and `ratings.(userId, movieId)` — 0
duplicates in all three.

**Why this matters:** you can't design a sensible cleaning or dedup rule
for data you haven't actually looked at. If we'd skipped straight to
"clean the data" without profiling first, we'd be guessing at problems
instead of fixing the ones that are actually there (like the missing
`tmdbId`s) and skipping effort on ones that aren't (there's no duplicate
problem to solve here yet — that's why Day 2 will *manufacture* one, on
purpose, to have something real to practice deduping).

## Step 3 — Write the "why lakehouse" note

Wrote a short paragraph explaining why we're layering Bronze → Silver →
Gold instead of one cleaning script: **auditability** (every stage is an
inspectable checkpoint) and **reproducibility** (a broken stage can be
fixed and rerun on its own, without redoing everything, and without needing
the original source data to still exist).

**Why this matters, and one thing that doesn't apply here:** a common
failure in real data platforms happens when two different teams — say, a
BI/dashboard team and a machine-learning team — each build their *own*
separate pipeline from the same original source, because neither trusts or
can easily reuse the other's cleaned data. The two pipelines inevitably
drift: different cleaning rules, different refresh schedules, so the
dashboard says one number and the model computes a slightly different one
from "the same" data, and nobody can say which is right. This project only
has one pipeline and one eventual consumer, so that specific failure mode
doesn't apply to us. What *does* carry over is the underlying fix: keeping
an untouched raw copy around, and layering cleaning on top of it in
inspectable stages, is what makes any single pipeline's mistakes fixable
instead of permanent — regardless of whether there's one consumer or five.

## Step 4 — Write Bronze Delta tables with ingestion metadata

Wrote 4 Delta tables (`bronze_ratings`, `bronze_movies`, `bronze_tags`,
`bronze_links`), each the original data plus 3 new columns: `batch_id`
(shared across all 4 tables for one ingestion run), `ingested_at` (real
timestamp of the write), and `source_file` (which CSV each row came from).

**Why this matters:** there are two orders you can do "extract, transform,
load" in. The older way transforms/cleans the data on a separate machine
*before* it's allowed to land anywhere permanent — nothing raw ever gets
kept, so if your cleaning logic turns out wrong later, you have to go back
to the original source (which might not even still have that exact data)
and do it all again. The newer way loads the raw data in first, completely
unmodified, and only transforms it afterward, in place, using whatever
compute you have. We're doing the newer way: nothing about `ratings_raw`
etc. was touched before it hit Bronze — the only additions are metadata
columns *about* the write itself (when, from where), not changes to the
actual data. That's a deliberate, standard pattern: it means if a cleaning
rule downstream turns out wrong, you fix the rule and rerun it against
Bronze — you never need to touch the original CSVs again.

## Step 5 — Replay the same batch, prove Bronze grows

Reran the exact same ingestion cell a second time and confirmed every
table's row count doubled (e.g. `ratings`: 100,836 → 201,672), with 2
distinct `batch_id`s showing up — nothing got deduped, overwritten, or
rejected.

**Why this matters:** Bronze is deliberately **append-only** — every
ingestion run adds to it, it never silently merges or overwrites, because
its whole job is to be a permanent record of *everything that was ever
received*, including accidental duplicate runs. This is different from
another important property, **idempotency** — a step is idempotent if
running it 5 times produces the exact same result as running it once (no
matter how many times you repeat it, nothing changes further). Idempotency
is what you want from a *cleaning/promotion* step (e.g., "rebuild the clean
Silver table from Bronze") — you should be able to safely rerun that job
after a crash without ending up with duplicated or drifted output. Bronze
itself is intentionally the opposite: it's supposed to grow every time you
feed it something, even the same thing twice. Today proved Bronze's
grow-on-replay behavior; Day 2's dedup logic is what has to actually
deliver idempotency, once there's a real promotion step to rerun.

## What's next (Day 2)

- Explicit typing (real timestamps, `genres` split into an array) — closes
  the schema-on-read gap flagged in Step 1.
- Dedup on `(userId, movieId)` with a documented precedence rule — this is
  where idempotency (see Step 5) actually gets built and tested.
- Referential integrity check against `movies`.

## Simplifications made on purpose

- **One pipeline, one consumer** — no separate BI-vs-AI split to keep in
  sync (see Step 3). Not needed for a single-purpose project.
- **No cloud cost model** — everything runs on one laptop, so there's no
  storage/compute bill to optimize by scaling one independently of the
  other. The Bronze/Silver/Gold pattern itself doesn't depend on scale,
  which is exactly why it's meaningful to test at 100K rows.
- **No PII/governance handling** — MovieLens `userId`s are already
  anonymized integers, nothing sensitive to mask or restrict access to.
- **No partitioning strategy** — each Bronze table is one unpartitioned
  Delta table. Splitting a table into many small partitioned files only
  pays off at a scale where a single file/folder would otherwise get
  unwieldy — not relevant at 100K rows.
