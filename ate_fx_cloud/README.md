# ATE FX Cloud Arbiter

This directory is the repository relay for ATE FX Classic 575.4.

- `requests/latest.json` is written by the local ATE application. It is intentionally redacted: no MT5 login/server or GitHub/OpenAI secrets.
- `.github/workflows/ate_fx_cloud_arbiter.yml` runs only when that request changes.
- `worker.py` calls the OpenAI Responses API with Structured Outputs and writes `decisions/latest.json`.
- `decisions/history/` is the append-only audit history.

Required repository secret: `OPENAI_API_KEY`.

The cloud worker never talks to MT5. A returned decision is advisory until the local ATE validator accepts request id, snapshot hash, TTL, pair, confidence, spread, cooldown and position limits. Broker execution is additionally hard-locked to MT5 DEMO.
