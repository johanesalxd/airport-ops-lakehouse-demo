# Documentation index

Start here. Each doc is single-purpose; use the table to jump to the one that
answers your question.

| Doc | What question it answers | Primary reader |
|---|---|---|
| [`design-philosophy.md`](design-philosophy.md) | Why bronze/silver/gold? Why is gold an **atomic star schema, not the semantic layer**? And **how do analyst-authored BigQuery Pipelines / Data Prep relate to this engineering repo** (two doors to one engine; Consolidate vs Federate)? | Architects, decision-makers |
| [`architecture.md`](architecture.md) | Tech stack, end-to-end flow, datasets, **two-repo design + the third path (UI-created pipelines)**, connections, the Composer DAG, Spark, Gemini, external tables. | Engineers building/operating it |
| [`streaming-ingestion.md`](streaming-ingestion.md) | How the optional Pub/Sub baggage stream works, how to run it, and how schema versioning/replay/backfill are handled. | Engineers building/operating streaming ingestion |
| [`data-sharing.md`](data-sharing.md) | How the **Analytics Hub hub-and-spoke** sharing works: curated `shr_*` views, publish/subscribe, subscriber whitelisting, cost isolation, subscription governance (list/revoke), audit. | Engineers/architects sharing data across projects |
| [`why-dataform-not-python.md`](why-dataform-not-python.md) | What Dataform *is*, and when to reach for Spark/Python instead. | Anyone new to Dataform |
| [`demo-script.md`](demo-script.md) | The workshop runbook: pre-flight checklist, minute-by-minute live flow, soundbites, **anticipated Q&A**, teardown. | The presenter |
| [`operations.md`](operations.md) | Runbook: **where logs live**, Composer 3 CLI caveats, idempotency & re-runs, known issues. | On-call / debugging |
| [`roadmap.md`](roadmap.md) | What's next (and what's already implemented): governance, streaming, continuous queries, data insights, **CI/CD & dev→prod environments**, public-release prep. | Planning the next iteration |
| [`slides/`](slides/) | The Marp workshop deck + [how to build/update it](slides/README.md). | The presenter |

Official Google Cloud documentation is linked **inline** at the point each topic
is discussed (rather than in a separate link map).
