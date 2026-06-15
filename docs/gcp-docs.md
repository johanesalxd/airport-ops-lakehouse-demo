# Official Google Cloud Documentation References

This file maps the demo design to official Google Cloud docs. Implementation agents should read these links before writing code for the corresponding component.

## Dataform

- Dataform SQL workflows  
  https://docs.cloud.google.com/dataform/docs/sql-workflows  
  Use for: Dataform workflow execution, service account requirement, dependency-ordered BigQuery execution, release/workflow configurations, Composer scheduling option.

- Dataform dependencies  
  https://docs.cloud.google.com/dataform/docs/dependencies  
  Use for: `ref()` dependency relationships, source declarations, tables, operations, assertions, dependency tree behavior.

- Dataform custom SQL operations  
  https://docs.cloud.google.com/dataform/docs/custom-sql  
  Use for: operations that call BigQuery procedures or run DDL/DML directly.

- Dataform assertions  
  https://docs.cloud.google.com/dataform/docs/assertions  
  Use for: built-in assertions (`nonNull`, `uniqueKey`, `uniqueKeys`, `rowConditions`) and manual assertion SQLX files.

- Dataform create tables / incremental tables / partitions  
  https://docs.cloud.google.com/dataform/docs/create-tables  
  Use for: table/view/incremental definitions, partitioning, clustering, `uniqueKey`, `updatePartitionFilter`, incremental filters, labels.

- Dataform schedule runs  
  https://docs.cloud.google.com/dataform/docs/schedule-runs  
  Use for: Composer integration and deciding when Composer is appropriate for complex pipelines with dependencies outside BigQuery.

## BigQuery ingestion and external data

- Loading CSV data from Cloud Storage  
  https://docs.cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv  
  Use for: native CSV loads, location constraints, compressed CSV limitations.

- Loading Parquet data from Cloud Storage  
  https://docs.cloud.google.com/bigquery/docs/loading-data-cloud-storage-parquet  
  Use for: native Parquet loads, schema inference, Parquet compression notes.

- Create Cloud Storage external tables  
  https://docs.cloud.google.com/bigquery/docs/external-data-cloud-storage  
  Use for: external table formats, `bq mkdef`, `bq mk`, external table limitations, GCS permissions.

- Query Cloud Storage data  
  https://docs.cloud.google.com/bigquery/docs/query-cloud-storage-data  
  Use for: temporary external table definitions and direct query patterns over GCS.

## BigLake

- BigLake introduction  
  https://docs.cloud.google.com/bigquery/docs/biglake-intro  
  Use for: BigLake concepts, access delegation, fine-grained table security over external data.

- Create Cloud Storage BigLake tables  
  https://docs.cloud.google.com/bigquery/docs/create-cloud-storage-table-biglake  
  Use for: Cloud Storage BigLake table creation, required roles, connection setup, location considerations.

## BigQuery Spark stored procedures

- Work with Spark stored procedures in BigQuery  
  https://docs.cloud.google.com/bigquery/docs/spark-procedures  
  Use for: creating/calling Spark stored procedures, custom service account invocation, logging, pricing and location considerations.

- Connect to Spark from BigQuery  
  https://docs.cloud.google.com/bigquery/docs/connect-to-spark  
  Use for: BigQuery Spark connection setup and permissions.

## Gemini / BigQuery ML remote models

- Generate text using `AI.GENERATE_TEXT`  
  https://docs.cloud.google.com/bigquery/docs/generate-text  
  Use for: creating a remote model over Gemini and generating text from BigQuery tables.

- `ML.GENERATE_TEXT` reference  
  https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-generate-text  
  Use for: detailed syntax and examples for classification, sentiment analysis, multimodal/object table support. Docs indicate `AI.GENERATE_TEXT` is recommended for new queries due to simplified output columns.

- Create remote model syntax  
  https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-create-remote-model  
  Use for: `CREATE MODEL ... REMOTE WITH CONNECTION ... OPTIONS(ENDPOINT=...)` syntax and supported endpoints.

- Gemini sentiment tutorial  
  https://docs.cloud.google.com/bigquery/docs/generate-text-tutorial-gemini  
  Use for: sentiment analysis example using a BigQuery remote model and `AI.GENERATE_TEXT`.

## Cloud Composer / Airflow

- Composer data analytics DAG with GCS, BigQuery, and Dataproc Serverless  
  https://docs.cloud.google.com/composer/docs/composer-3/run-data-analytics-dag-googlecloud  
  Use for: DAG patterns using `GCSToBigQueryOperator`, `DataprocCreateBatchOperator`, `BigQueryInsertJobOperator`, TaskGroups, service account and API prerequisites.

- Run Dataproc workloads from Composer  
  https://docs.cloud.google.com/composer/docs/composer-2/run-dataproc-workloads  
  Use for: Dataproc Serverless batch operator details if the optional non-procedure Spark path is added later.

## Lineage / Dataplex / Knowledge Catalog

- Composer lineage integration  
  https://docs.cloud.google.com/composer/docs/composer-3/lineage-integration  
  Use for: enabling Composer lineage integration, OpenLineage behavior, automatic supported-operator lineage, custom lineage events with inlets/outlets.

- Dataplex / Knowledge Catalog overview  
  https://docs.cloud.google.com/dataplex/docs/catalog-overview  
  Use for: catalog concepts, entries, aspects, entry links, glossary, search, data lineage context.

- Enrich entries with metadata / aspects  
  https://docs.cloud.google.com/dataplex/docs/enrich-entries-metadata  
  Use for: optional metadata enrichment and classification aspects.

- Reuse data quality rules  
  https://docs.cloud.google.com/dataplex/docs/reuse-data-quality-rules  
  Use for: optional governance-driven data quality rules and rule templates.

## Medallion / lakehouse architecture

- Google Cloud medallion architecture overview  
  https://cloud.google.com/discover/what-is-medallion-architecture  
  Use for: bronze/silver/gold definitions, Cloud Storage as raw/bronze landing zone, BigQuery as silver/gold engine, Spark for heavy transforms, Dataform for transformations, Composer for orchestration/governance.

- Google Cloud Lakehouse key concepts  
  https://docs.cloud.google.com/lakehouse/docs/key-concepts  
  Use for: medallion architecture in Google Cloud Lakehouse, open formats such as Apache Iceberg, BigQuery serving gold layer, open interoperability between Spark and BigQuery.

- Google Cloud data lakehouse overview  
  https://cloud.google.com/solutions/data-lakehouse  
  Use for: open and agentic lakehouse positioning, BigQuery/Spark/Knowledge Catalog lakehouse story.

## BigQuery governance: RLS, CLS, masking, and authorized views

- BigQuery row-level security introduction  
  https://docs.cloud.google.com/bigquery/docs/row-level-security-intro  
  Use for: row access policies, when to use RLS versus authorized views or separate tables.

- BigQuery column-level access control introduction  
  https://docs.cloud.google.com/bigquery/docs/column-level-security-intro  
  Use for: policy tags, taxonomy workflow, enforcement model, interaction with dataset ACLs.

- BigQuery column-level access control guide  
  https://docs.cloud.google.com/bigquery/docs/column-level-security  
  Use for: implementation options and caveat that `CREATE TABLE` DDL cannot specify policy tags directly.

- BigQuery authorized views  
  https://docs.cloud.google.com/bigquery/docs/authorized-views  
  Use for: exposing selected data products while preserving underlying RLS/CLS behavior.

## Streaming and real-time extensions

- Pub/Sub BigQuery subscriptions  
  https://docs.cloud.google.com/pubsub/docs/bigquery  
  Use for: direct Pub/Sub-to-BigQuery streaming ingestion, BigQuery Storage Write API behavior, at-least-once delivery caveat, dead-letter handling, CDC ingestion support.

- Dataflow Pub/Sub to BigQuery streaming tutorial  
  https://docs.cloud.google.com/dataflow/docs/tutorials/dataflow-stream-to-bigquery  
  Use for: alternative streaming path when messages require complex transformation, exactly-once-oriented Dataflow template behavior, and `gcloud dataflow jobs run` template examples.

- BigQuery continuous queries introduction  
  https://docs.cloud.google.com/bigquery/docs/continuous-queries-introduction  
  Use for: continuous query concepts, supported sources, supported outputs, AI function support, stateful operation caveats.

- BigQuery continuous queries guide/examples  
  https://docs.cloud.google.com/bigquery/docs/continuous-queries  
  Use for: `APPENDS`, `CHANGES`, writing continuous output to BigQuery, exporting to Pub/Sub, Bigtable, or Spanner, and continuous `AI.GENERATE_TEXT` examples.

## BigQuery data insights / automated documentation

- BigQuery data insights overview  
  https://docs.cloud.google.com/bigquery/docs/data-insights  
  Use for: table insights, dataset insights, generated descriptions, generated SQL recommendations, relationship graphs, pricing caveats.

- Generate table insights  
  https://docs.cloud.google.com/bigquery/docs/generate-table-insights  
  Use for: Dataplex `DATA_DOCUMENTATION` data scans, `generationScopes`, `catalogPublishingEnabled`, one-time TTL scans, polling scan jobs, BigLake/external table insight prerequisites.

- Generate dataset insights  
  https://docs.cloud.google.com/bigquery/docs/generate-dataset-insights  
  Use for: dataset-level relationship graph generation, required APIs, Gemini in BigQuery setup, data profile scan recommendation, Preview caveat.

## BigQuery data insights (Gemini-in-BigQuery, Dataplex DATA_DOCUMENTATION)

- Data insights overview  
  https://docs.cloud.google.com/bigquery/docs/data-insights  
  Use for: table vs dataset insights, what's generated (descriptions, NL questions + SQL, relationship graph), Gemini-in-BigQuery prerequisite, limitations (no `GEO`/`JSON` columns, 350-column cap, dataset insights Preview, regeneration overwrites), pricing.

- Generate table insights  
  https://docs.cloud.google.com/bigquery/docs/generate-table-insights  
  Use for: the programmatic REST flow via the Dataplex `DataScans` API with `type: DATA_DOCUMENTATION`; `generationScopes` (`ALL` / `TABLE_AND_COLUMN_DESCRIPTIONS` / `SQL_QUERIES`); `catalogPublishingEnabled`; one-time scan with TTL; polling `dataScans.jobs.get`; publishing results via `dataplex-data-documentation-published-*` table labels; required IAM roles; external/BigLake prerequisites.

- Generate dataset insights  
  https://docs.cloud.google.com/bigquery/docs/generate-dataset-insights  
  Use for: dataset-level relationship graph generation (Preview), required APIs, Gemini-in-BigQuery setup, profile-scan grounding recommendation.

- Create a data profile scan  
  https://docs.cloud.google.com/bigquery/docs/data-profile-scan  
  Use for: grounding insights — create/run a `DATA_PROFILE` scan and publish via `dataplex-dp-published-*` labels so Gemini grounds output in real values.

- Dataplex DataScans API reference  
  https://docs.cloud.google.com/dataplex/docs/reference/rest/v1/projects.locations.dataScans  
  Use for: `dataScans.create` / `run` / `get` request bodies, trigger types (`onDemand`, `oneTime` with TTL), and the `DataScan` resource schema.

## Conversational analytics & data agents

- Conversational analytics overview  
  https://docs.cloud.google.com/bigquery/docs/conversational-analytics  
  Use for: data agents over tables/views, context + instructions, verified ("golden") queries, BigQuery ML support, pricing, Preview caveat, global-only location.

- Create data agents  
  https://docs.cloud.google.com/bigquery/docs/create-data-agents  
  Use for: creating a data agent with knowledge sources and required roles; discovery/use in Gemini Enterprise.

- Conversational Analytics API overview  
  https://docs.cloud.google.com/gemini/docs/conversational-analytics-api/overview  
  Use for: building an embedded NL chat (`geminidataanalytics.googleapis.com`), supported sources, cost controls.

## Dataform code lifecycle / CI/CD & environments

- Manage the Dataform code lifecycle  
  https://docs.cloud.google.com/dataform/docs/managing-code-lifecycle  
  Use for: dev/staging/prod isolation strategies (by schema, by schema+project), PR-based promotion (`main` → `prod`).

- Configure Dataform compilation (workspace overrides, release configurations)  
  https://docs.cloud.google.com/dataform/docs/configure-compilation  
  Use for: workspace compilation overrides (schema suffix) and release configurations (compile a git commitish).

- Schedule Dataform executions (workflow configurations)  
  https://docs.cloud.google.com/dataform/docs/schedule-runs  
  Use for: workflow configurations to schedule compiled releases; Composer integration.

## Open table format (Apache Iceberg)

- Apache Iceberg managed tables  
  https://docs.cloud.google.com/bigquery/docs/biglake-iceberg-tables-in-bigquery  
  Use for: `file_format = PARQUET` + `table_format = ICEBERG` + `WITH CONNECTION`, schema evolution, time travel, storage optimization, load/export, metadata snapshots.

- Create Apache Iceberg external tables  
  https://docs.cloud.google.com/bigquery/docs/iceberg-external-tables  
  Use for: read-only Iceberg external tables with the Lakehouse runtime catalog, fine-grained access control.

- Create tables in Dataform workflows (Iceberg)  
  https://docs.cloud.google.com/dataform/docs/create-tables  
  Use for: creating Iceberg tables natively from Dataform so the transformation graph stays unchanged.

## Managed data quality (Dataplex auto data quality)

- Scan for data quality issues (BigQuery)  
  https://docs.cloud.google.com/bigquery/docs/data-quality-scan  
  Use for: creating/scheduling data quality scans from BigQuery, viewing results, publishing scores to Knowledge Catalog.

- Auto data quality (Dataplex)  
  https://docs.cloud.google.com/dataplex/docs/use-auto-data-quality  
  Use for: rule dimensions, `dataScans.create` for `DATA_QUALITY`, and profile-based rule recommendations (`generateDataQualityRules`).

- Data profiling overview  
  https://docs.cloud.google.com/dataplex/docs/data-profiling-overview  
  Use for: profiling concepts that feed DQ rule recommendations and insight grounding.

## Data sharing (Analytics Hub)

- Introduction to BigQuery data sharing  
  https://docs.cloud.google.com/bigquery/docs/analytics-hub-introduction  
  Use for: publisher/subscriber architecture, data exchanges, listings, linked datasets, data-egress controls.

- Manage data exchanges  
  https://docs.cloud.google.com/bigquery/docs/analytics-hub-manage-exchanges  
  Use for: creating exchanges and setting permissions.

- Manage listings  
  https://docs.cloud.google.com/bigquery/docs/analytics-hub-manage-listings  
  Use for: publishing a dataset as a listing, public vs private, sharing stored procedures.

## Vector search & embeddings

- Vector search introduction  
  https://docs.cloud.google.com/bigquery/docs/vector-search-intro  
  Use for: `AI.GENERATE_EMBEDDING`, autonomous embedding generation, `VECTOR_SEARCH` / `AI.SEARCH` / `AI.SIMILARITY`, pricing, edition caveats.

- Search embeddings with vector search (tutorial)  
  https://docs.cloud.google.com/bigquery/docs/vector-search  
  Use for: end-to-end create-index + search example, required permissions.

- Create a vector index  
  https://docs.cloud.google.com/bigquery/docs/vector-index  
  Use for: `CREATE VECTOR INDEX`, distance types, index management, table-size limits.

## Implementation reminder

If implementation behavior differs from this plan, prefer the current official docs over this README and update the README with the exact reason.
