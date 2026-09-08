# arbimon-aed

Audio event detection job for Arbimon.

---

## ⚠️ SUPERSEDED for rfcx-local (2026-09-08)

**The worker that runs in the self-hosted rfcx-local cluster is no longer built
from this repo.** It lives in:

> **`rfcx/arbimon-jobs-analysis`** → `workers/audio-event-detection/`
> (image `aed`, built and deployed by CI/CD on merge to `main`)

### What changed and why

This repo's `worker/` half had **silently diverged** from what production
actually runs. Measured 2026-09-08, upstream vs live:

| | this repo | what rfcx-local runs |
|---|---|---|
| `aed_run_job.py` | 189 lines | 221 lines |
| sharding (`WORKER_INDEX`/`WORKER_COUNT`) | absent | present |
| per-shard feature prefix | `_0` hardcoded | `_<worker_index>` |
| staggered shard start (s3 herd) | absent | present |
| queue path (`aed_consume.py`, `aed_drive.py`) | absent | present |
| `analysis_queue_lib` / `roi_lanes` / `mirror_delete` | absent | present |
| `roi_prewarm_publish` | absent | present |
| ROI-PNG write gate | absent | applied |
| sitecustomize S3 timeouts | absent | installed |

### The CD workflow was REMOVED, deliberately

`.github/workflows/cd-rfcx-local.yaml` used to build this repo's root
`Dockerfile` on every push to `main` and push it into the rfcx-local
in-cluster registry as `aed:rfcx-local-prod-<sha>` **and**
`aed:rfcx-local-prod-latest`.

Because the build above is **weaker than what production runs**, and because
the rfcx-local dispatcher defaults every worker tag to
`rfcx-local-prod-latest`, that workflow was a live-fire hazard: a routine push
here could republish `latest` and, if any pin were ever unset, feed a degraded
worker to the production analysis plane.

It has been removed. **Pushing to this repo no longer deploys anything.**

### To change the rfcx-local AED worker

Open a PR against **`rfcx/arbimon-jobs-analysis`** and merge it; CI/CD builds
and rolls `aed-consumer`. Never hand-build, and never `kubectl set image`
against production.

### The AWS half

`functions/conductor/`, `template.yaml` and `samconfig.toml` are the original
AWS Lambda/SAM deployment (conductor fan-out + worker Lambdas). They are not
used by rfcx-local and were not carried over.
