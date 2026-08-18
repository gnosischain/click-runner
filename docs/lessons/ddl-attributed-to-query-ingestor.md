# ddl-attributed-to-query-ingestor

**Status:** observed

**Symptom:** operation metrics for `ingestor="query"` include failures that
belong to another ingestor; per-ingestor dashboards under-count DDL errors.

**Cause:** `BaseIngestor.execute_queries` hardcodes the metric label
`ingestor="query"`, and every ingestor's table creation goes through it.

**Rule:** when reading operation metrics, remember create-table activity from
ALL ingestors lands under "query". If fixing, thread the ingestor name through
execute_queries — do not fork the method per subclass.

**Evidence:** `ingestors/base.py` (execute_queries), any *_create.sql run.
