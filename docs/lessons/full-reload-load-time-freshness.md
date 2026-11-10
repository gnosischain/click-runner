# full-reload-load-time-freshness

**Status:** observed (data-date freshness row proposed for the alerting stack, not applied)

**Symptom:** a full-reload table looks fresh in the freshness query and the alert stays
quiet, yet its data stopped advancing months ago. Downstream marts end on a fixed date.

**Cause:** csv mode re-inserts the vendor's whole file on every run, and the freshness
row for such a table measured the load time (`max(version)` / `max(ingested_at)`).
The load time advances on every run, even when the file's content has frozen. A
green job and a fresh load time prove the job ran, not that new data arrived (the
"a green job is not fresh data" rule, one level down).

**Rule:** a full-reload table gets two freshness rows: the load time (the job ran) and
the newest DATA date of the series consumers depend on, with an SLA just above the
vendor's normal publication lag. For Ember: `maxIf(Date, Area = 'World' AND
Electricity source = 'Total generation')`, 2880 h (World is published about two months
after month end, and the cron loads on the 8th and 21st).

**Evidence:** Ember's long-format file froze at data to 2026-06: the 2026-10-21 load
reinserted 364,334 rows and added no key (378,066 rows FINAL before and after), while
`crawlers-data-stale` read `max(version)` (480 h) and stayed quiet. Every dbt ESG mart
stopped at 2026-06-30 unseen. It surfaced only when Ember retired the URL on
2026-11-02 (it now serves a two-column notice CSV) and the 2026-11-08 run failed
with Code 47 on `Area`. Found in the 2026-11-09 analytics morning check.
