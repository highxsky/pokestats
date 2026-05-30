# Work Log

Append-only dev journal. **Newest entry on top.** One entry per work session.
Keep it to the four fields — the `Next:` line is for your future self after a gap.

> Companion files: `docs/implementation-plan.md` (what's done / left — checkboxes),
> git log (ground truth). Start a session by reading this top entry + `git log --oneline -5`.

---

## 2026-05-30
- **Done:** Decided the architecture — move off MotherDuck-for-everything to **local DuckDB** for build, with MotherDuck reserved as an optional *serving* layer for marts only. Wrote `docs/implementation-plan.md` (5 phases, chunked for short sessions). Set up this WORKLOG + the start/wrap-up ritual.
- **Stopped at:** Planning complete; no pipeline code changed yet. Still on `main`.
- **Next:** Phase 0 → **Chunk 0.1** (branch + confirm secrets gitignored), then 0.2 (export MotherDuck `raw.*` tables to Parquet as backup + archive seed). Do 0.2 *before* touching MotherDuck config.
- **Open Qs:** Phase 3 decision still pending — MotherDuck serving vs bundled snapshot file (see Chunk 3.1). Not needed until Phase 3.
