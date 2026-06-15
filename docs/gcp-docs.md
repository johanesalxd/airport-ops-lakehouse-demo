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

## Implementation reminder

If implementation behavior differs from this plan, prefer the current official docs over this README and update the README with the exact reason.
