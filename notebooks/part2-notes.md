# Part 2 Notes — Silver: typed, deduplicated, trustworthy

Goal for this part: take the raw, growable Bronze tables and build a Silver
ratings table where every column has a real type, every (user, movie) pair
appears exactly once, and rebuilding it is safe to repeat.

Silver is where the "cleaning" of the medallion pattern happens. It has five
jobs: **type** the columns, **validate** them against rules, **deduplicate**,
**conform** (one timezone/currency/definition per concept), and **join** to
reference tables. This part covers type, deduplicate, and a light form of
validate. Conform and join are not needed for a single-source dataset like
this one.

Sections follow the order of the cells in `part2.ipynb`.

## Step 0 — Start Spark + Delta, read Bronze back

Same session setup as Part 1, plus one new path, `SILVER_DIR`, because this
part *produces* a table instead of only reading. Then we read `bronze_ratings`
and `bronze_movies` back from disk.

**Why this matters:** Silver is built from Bronze, not from the original CSVs.
That is the point of keeping a raw layer: the CSVs could be gone tomorrow and
Silver could still be rebuilt. It is also the "load first, transform after"
order (ELT): Bronze stored the data untouched, and every change happens here,
in a layer we are allowed to reshape.

Two things worth noticing in the output: `bronze_ratings` has 403,344 rows
(four full copies of the data, one per replay we ran in Part 1), and
`bronze_movies` still has `genres` as a plain string. Bronze changed nothing,
exactly as designed.

## Step 1 — Type the columns

- `timestamp` (a plain integer, "seconds since 1970") became `rated_at`, a real
  timestamp, using `F.timestamp_seconds`. The old integer column was dropped.
- `genres` (`"Comedy|Romance"`) became a real list, `["Comedy", "Romance"]`,
  using `F.split` with an escaped `\\|` (a bare `|` means "or" in the pattern
  language).

**Why this matters:** in Part 1, `inferSchema=True` showed the raw data has no
enforced meaning, only guesses. Typing is where we replace guesses with
decisions: `rated_at` is now something Spark knows how to sort and compare as
a moment in time. The rename also removes an ambiguity: we already have
`ingested_at` (when *our pipeline* received the row), and `rated_at` is when
the *user* rated the movie. Those are two different clocks.

Dropping the old `timestamp` here is safe because Bronze still holds the
original. Silver may reshape data; it may not be the only copy.

## Step 2 — Manufacture a synthetic duplicate batch

Real MovieLens has no repeated (userId, movieId) pairs, so there is nothing for
a dedup rule to act on. We took 3 real pairs and created "updated" versions:
rating set to 5.0, `rated_at` pushed one day later, labelled
`batch_id="synthetic_update"`. The 3 pairs then existed as 4 identical copies
from Part 1's replays plus 1 changed version.

**Why this matters:** it creates the second kind of duplicate that real
pipelines face. Identical copies come from re-ingesting the same file. A
*changed* row (same key, different values) comes from a real-world correction.
The dedup rule has to handle both, and it has to say which one wins.

## Step 3 — Deduplicate with a stated precedence rule

**The rule:** for each (userId, movieId), keep the row with the latest
`rated_at`.

We defined a window that groups rows by (userId, movieId) and sorts each group
newest-first, numbered each row with `row_number()`, kept only row 1, and
dropped the helper column. Result: 403,347 rows became 100,836, exactly one per
pair. We then checked the 3 synthetic pairs and confirmed the surviving row was
the synthetic one (rating 5.0), not an older copy. The count alone would not
have proved that.

**Why this matters:** two ideas sit under this step.

- **Grain.** Silver's stated grain is "one row = one user's current rating for
  one movie." Once that is written down, "how many ratings does this movie
  have" has one answer. Without a stated grain, dedup is arbitrary.
- **Event time vs. processing time.** The rule sorts by `rated_at` (when the
  user acted), not `ingested_at` (when we received it). Data can arrive late or
  out of order: if a user's *older* rating showed up in a later batch, sorting
  by arrival time would let stale data overwrite the newer decision. Sorting by
  the time the event actually happened is the correct clock for "current
  state." (Streaming systems use a watermark to decide how long to wait for late
  events before finalizing a time window. We are running batch, so we can just
  recompute everything and never need one.)

## Step 4 — Referential integrity check

A `left_anti` join keeps ratings whose `movieId` has no match in `movies`. It
returned 0.

**Why this matters:** this is a hand-written data quality check, run at the
Bronze-to-Silver step. That is the right place for it: check once, where data
is promoted, so that everything downstream inherits the guarantee and no
consumer has to re-check. Real teams write such checks as versioned code
(often with a framework such as Great Expectations) so that loosening a rule is
a reviewed, traceable decision.

Had it returned a nonzero count, we would have needed to choose deliberately:
- **Fail fast** (block the batch, promote nothing) is right when a problem
  breaks the table's foundation, such as missing or duplicate keys, because
  everything joined downstream inherits it.
- **Quarantine** (set the bad rows aside, promote the rest, and record and
  alert) is right for a small, partial defect, where blocking everything would
  starve consumers of good data.

Either way the outcome has to be recorded and someone alerted. Silently
promoting bad data after "catching" it is a quality opinion, not a quality
gate. We did not need to choose this time; Day 4 of our plan builds this
properly.

## Step 5 — Prove reruns of the Silver build don't duplicate

**Part 1 — first write.** `ratings_silver` written to `silver/silver_ratings`
with `mode("overwrite")`. Read back: 100,836 rows.

**Part 2 — late data arrives, rebuild.** We created 2 brand-new ratings
(userId 9999), unioned them on, reran the identical dedup, and overwrote the
table. Read back: 100,838 rows. Exactly +2: not doubled, not ignored.

**Part 3 — rerun with no new data.** Same dedup, same input, overwrite again.
Read back: 100,838 rows and 100,838 distinct (userId, movieId) pairs. Unchanged,
and no pair appears twice. (The 2 late rows use `current_timestamp()`, which is
recalculated each run, so their timestamps shift slightly; counts and keys are
identical.)

**Why this matters:**

- **Append vs. overwrite.** Bronze uses `append` because it is a permanent log
  of everything received. Silver uses `overwrite` because it represents current
  state, recomputed from Bronze each time. That difference is what makes the
  promotion *idempotent* (running it repeatedly gives the same table as running
  it once).
- **Why overwrite is safe here.** Delta writes are atomic: a write is recorded
  as one commit in the `_delta_log` folder, and readers only see files that
  belong to a completed commit. We checked the actual log for `silver_ratings`:
  version 0 is one commit adding 2 files (100,836 rows); version 1 is one
  commit that adds 2 new files and marks the 2 old ones as removed (100,838
  rows). A reader sees all of version 0 or all of version 1, never a
  half-written mix.
- **"Removed" doesn't mean deleted.** The old Parquet files are still sitting
  in the folder. A commit only *tombstones* them (marks them dead in the log).
  This is what allows time travel: because nothing is destroyed, the table can
  be read as it was at version 0. A later `VACUUM` command permanently deletes
  tombstoned files past a retention period, and the trade-off is a deliberate
  one: aggressive cleanup saves storage but weakens the audit trail and the
  ability to go back.
- **Why the `.cast("int")` mattered.** `spark.range` produces 64-bit integers,
  while the table stores 32-bit ones. Delta rejects a write whose column types
  don't match the table's schema, instead of quietly converting it (schema
  enforcement), so the mismatch would have failed at write time rather than
  corrupting anything downstream. That is the protection a plain folder of
  files lacks. (We expect this behavior but did not trigger the error here;
  rejecting a bad write is a planned Day 3 exercise.)
- **Volume sanity check.** Comparing counts before and after (403,347 → 100,836;
  100,836 → 100,838) is a hand-rolled volume check: "did roughly the expected
  number of rows show up?" Observability tools automate this, along with
  freshness (is the table updating?), schema drift, and distribution drift.

## Already possible with what exists (not done yet)

`silver_ratings` now has two versions, so Delta's time travel works on it today:
reading `VERSION AS OF 0` returns the 100,836-row table, `DESCRIBE HISTORY`
lists both writes with timestamps and operations, and `RESTORE` can roll back to
version 0. These are Day 3 topics in our plan; the table is already prepared
for them.

## Honest gaps and simplifications

- **Synthetic rows bypassed Bronze.** The 3 updates and 2 late ratings exist
  only in notebook code, not in `bronze_ratings`. So the current Silver cannot
  be rebuilt from Bronze alone: restart the kernel, and Silver v2 is not
  reproducible unless the notebook is rerun. In a real pipeline, new arrivals
  would be *appended to Bronze first*, and Silver would be rebuilt from there.
  I chose the in-memory shortcut to keep this part focused on Silver logic, and
  I should have said so explicitly at the time.
- **Ties are not deterministic.** `row_number()` on ties (two rows with the same
  `rated_at`) picks arbitrarily. Harmless here because the tied rows are
  identical replay copies. If two different ratings ever shared a timestamp,
  which one won could change between runs, which would break idempotency. A
  tiebreaker (for example `ingested_at` descending) would fix it.
- **The event-time choice isn't actually tested.** In our synthetic update, the
  newest `rated_at` is also the latest `ingested_at`, so sorting by either
  clock picks the same winner. A test with a *late-arriving older* rating would
  separate them.
- **Only ratings were written to Silver.** `movies_typed` exists in memory but
  no `silver_movies` table was saved yet.
- **Typing was only tested on good data.** We did not check how the typing step
  behaves when a value can't be converted; Silver is supposed to fail loudly
  rather than silently produce bad values.

## Parts of the theory that don't apply here

- **Streaming and Kafka** (topics, partitions, offsets, consumer groups,
  watermarks, exactly-once delivery): our data is batch, arriving as files.
  Exactly-once in streaming combines source offsets with atomic Delta commits;
  we only have the atomic-commit half, and get idempotency by recomputing.
  Simulated streaming is Day 4 of our plan.
- **Optimistic concurrency:** only one writer (this notebook) touches the
  tables, so there are no competing commits to retry.
- **CHECK constraints** (rules such as rating between 0.5 and 5.0 that Delta
  enforces on future writes): planned for Day 3; none exist yet.
- **Governance / PII / OPTIMIZE:** MovieLens IDs are anonymized, and the table
  is a few small files, so neither compaction nor access control is relevant.
