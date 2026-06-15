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

## Implementation reminder

If implementation behavior differs from this plan, prefer the current official docs over this README and update the README with the exact reason.
