# OPERATIONAL_STATE.md is intentionally not in this repo

`docs/OPERATIONAL_STATE.md` holds the **live** operational record: current
positions and share counts, protective-stop levels, account state, and the
strategy roster (symbols, conIds, validated profit factors, tuned parameters,
and which strategies are armed).

This repository is public. That file is simultaneously the trading book's alpha
and a description of its open risk, so it is gitignored and kept only on the
machine that runs the stack. Docs that reference it — `CLAUDE.md`,
`docs/SAFETY_ROADMAP.md`, `skills/mmr-verification/SKILL.md` — are pointing at
that local file, not at anything you can fetch here.

If you are setting up a fresh clone, create it as you go; nothing in the code
reads it. It is a human operator's log.

Note: versions of the file from before 2026-07-24 remain in this repository's
git history, including the strategy roster. Removing it from tracking stops
future publication; it does not retract what was already pushed.
