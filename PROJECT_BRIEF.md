# Movie Ratings Mini-Lakehouse — Project Brief

## Why this project exists

I (the user) went through the first parts of a 5-day "Modern Data Engineering"
course (Bronze/Silver/Gold lakehouse pattern, Spark + Delta Lake, streaming,
quality gates, BI serving) built around a synthetic trip-data fixture. I want
to verify I actually understand the ideas — not just followed a script — by
rebuilding the same pattern on a topic I actually care about (movies), on real
(if messy) data, making my own design decisions along the way.

This is a learning project. **I want to build it myself, one piece at a
time** — not have it generated all at once.

## How I want to work with you (the assistant)

- Give me the **skeleton first**: what files/tables we're building today and
  why, before any code.
- Then give me **one small step at a time** (one function, one cell, one
  check) — I will type or paste it myself and run it.
- **Do not write the full solution up front.** Wait for me to report back
  what happened (output or error) before giving the next step.
- When something errors, inspect the actual files/output in this repo
  directly rather than guessing — that's the whole point of doing this
  inside the project folder.
- Keep explanations jargon-light and tied to *why*, not just *what*.
- No bilingual tables, no verification.json/COMPLETION.md-style ceremony —
  just working code, a clean README, and short notes on the decisions I made.

## Environment (already proven to work, reuse as-is)

This mirrors the course's own verified local setup — no Docker needed:

- Python 3.11
- Java 17 (JDK)
- `pyspark==3.5.8`
- `delta-spark==3.3.3`
- `py4j==0.10.9.9`

## Dataset

**MovieLens `ml-latest-small`** (GroupLens) — ~100k ratings, ~9,000 movies,
~600 users. Files: `ratings.csv`, `movies.csv`, `tags.csv`, `links.csv`.

Status as of this brief: **not yet placed in this repo.** Day 1 step one is
downloading/placing the raw CSVs under `data/raw/`.

Note: real MovieLens ratings already have a unique `(userId, movieId)` pair —
there's no natural duplicate/correction case like the course's fixture has.
We will manufacture a small synthetic "user updated their rating" batch
ourselves on Day 2 (a handful of rows) — a disclosed simplification, not a
shortcut.

Note on Day 4: this project uses **file-based streaming** (drip files into a
watched folder), not Kafka, even though the course itself uses Kafka. The
Structured Streaming concepts that matter (checkpointing, incremental reads,
stop/restart recovery) are identical either way. Running an actual Kafka
broker would add operational skill (topics, producer/consumer clients) that
doesn't map onto understanding the lakehouse pattern this project exists to
verify — a deliberate choice, not a gap.

## The 5-day plan

| Day | Idea being verified | What gets built |
|---|---|---|
| 1 | Architecture choice + Bronze preserves raw, un-cleaned data | Inspect the raw CSVs; write a short "why lakehouse" note; write Bronze Delta tables with ingestion metadata (batch id, ingested-at, source file); replay the same batch to prove Bronze is allowed to grow |
| 2 | Silver = typed, deduped, one stated grain | Type columns (real timestamps, genres split into an array); dedupe ratings on `(userId, movieId)` with a documented precedence rule; check referential integrity against the movies table; split ratings into an "early" and "late" batch to prove reruns don't duplicate |
| 3 | ACID / Delta = real transactions | Reject a malformed write (e.g. rating outside 0.5–5.0); MERGE to apply an upsert; compare two table versions via time travel |
| 4 | Streaming + quality, simplified | Drip ratings into a watched folder as a simulated stream (no Kafka) with checkpointing + a stop/restart test; quarantine invalid rows with reasons |
| 5 | Gold + BI = answer a real question | Aggregate tables (avg rating by genre/month, top-rated movies with a minimum vote count); reconcile Silver vs. Gold row counts; one chart; final README tying it back to the design decisions |

## Definition of done for the whole project

- Each day's Delta tables + a short note on the one design decision that
  mattered that day (not a full report — a paragraph is enough).
- A final README that states: the use case, the architecture decision from
  day 1, the dedup/precedence rule from day 2, the one quality rule enforced
  on day 4, and what Gold answers on day 5.
- No raw scraped/redistributed data of questionable licensing — MovieLens is
  explicitly licensed for this kind of use, so that's a non-issue here.

Start here: **Day 1 — download the dataset into `data/raw/`, verify the
environment (Python/Java/pyspark/delta-spark versions above), then read the
CSVs and profile them (row counts, key uniqueness, missing values) before
writing anything to Bronze.**
