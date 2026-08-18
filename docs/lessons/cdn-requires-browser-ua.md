# cdn-requires-browser-ua

**Status:** observed

**Symptom:** hopr-network job fails with non-retried non-200s (or JSON decode
errors on an HTML body) although network.hoprnet.org works in a browser.

**Cause:** a CDN fronts the dashboard API and serves an error page to clients
without a browser-like User-Agent and Referer. The ingestor ships spoofed
headers (`hopr_network_ingestor.py` HEADERS); a CDN policy change re-breaks it.

**Rule:** treat sudden non-200/HTML responses from this source as a CDN policy
change, not an API removal — verify in a browser before declaring the source
dead. Keep the headers in one constant.

**Evidence:** `ingestors/hopr_network_ingestor.py` module docstring + HEADERS.
