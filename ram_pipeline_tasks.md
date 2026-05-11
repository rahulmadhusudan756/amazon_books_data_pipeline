# RAM Market ETL Pipeline — Project Specification

## Task 1 — Infrastructure & Docker Compose

Modify `docker-compose.yml` to define four services on a shared internal bridge network:

- **Airflow** — DAG orchestration with LocalExecutor, a shared `/dags` volume, and `AIRFLOW__CORE__SQL_ALCHEMY_CONN` pointed at the Postgres service.
- **Postgres** — Single instance serving both as the Airflow metadata database and the pipeline data store. Expose port `5432`. Seed credentials via environment variables.
- **Metabase** — Expose on port `3000`. Pre-configure `MB_DB_TYPE=postgres` environment variables so it connects to the same Postgres instance on startup.
- **pgAdmin** — Expose on port `5050`. Set `PGADMIN_DEFAULT_EMAIL` and `PGADMIN_DEFAULT_PASSWORD` via environment variables.

All services must share a named Docker network. Use a `depends_on` chain: Metabase and pgAdmin depend on Postgres; Airflow depends on Postgres.

---

## Task 2 — Dynamic Search & Extraction (Airflow DAG)

In the `extract` Python callable of the DAG:

1. Read the search term dynamically from `context["dag_run"].conf.get("search_term", "DDR5 RAM")` — do not hardcode any query string.
2. Declare a `DAG Params` schema with a `search_term` field so the Airflow UI renders a text input when triggering the DAG.
3. Build the Amazon India search URL: `https://www.amazon.in/s?k={search_term}`.
4. Use `requests` with a rotating `User-Agent` header. Parse the response with `BeautifulSoup`.
5. For each result listing, extract and return as a list of dicts: `title`, `price` (strip `₹` and commas, cast to float), `rating` (cast to float), `url` (absolute link). Skip any listing missing `title` or `price`.

---

## Task 3 — Technical Transformation with Regex

In the `transform` Python callable, process the list of dicts from the extract step. Use the `re` module to parse each `title` string:

```python
import re

def parse_title(title: str) -> dict:
    capacity = re.search(r'(\d+)\s*[Gg][Bb]', title)
    speed    = re.search(r'(\d+)\s*(MHz|MT/s)', title, re.IGNORECASE)
    ddr_type = re.search(r'DDR[45]', title, re.IGNORECASE)
    return {
        "capacity": int(capacity.group(1)) if capacity else None,
        "speed":    int(speed.group(1))    if speed    else None,
        "ddr_type": ddr_type.group(0).upper() if ddr_type else None,
    }
```

Compute the value score only when all three fields are present and price is non-zero:

```
value_score = (capacity_gb × speed_mhz) / price_inr
```

If any field is `None` or price is `0`, set `value_score = None`. Add `capacity`, `speed`, `ddr_type`, and `value_score` as new keys on each dict.

---

## Task 4 — Postgres Schema & UPSERT Load

**DDL — run once at pipeline startup (e.g., in a `create_table` task):**

```sql
CREATE TABLE IF NOT EXISTS ram_market_data (
    id          SERIAL PRIMARY KEY,
    timestamp   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    title       TEXT NOT NULL,
    price       NUMERIC(10, 2),
    rating      NUMERIC(3, 1),
    capacity    INT,
    speed       INT,
    ddr_type    VARCHAR(10),
    value_score NUMERIC(12, 6),
    url         TEXT,
    UNIQUE (title, (timestamp::date))
);
```

**Load task — UPSERT logic:**

```sql
INSERT INTO ram_market_data (timestamp, title, price, rating, capacity, speed, ddr_type, value_score, url)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (title, (timestamp::date))
DO UPDATE SET
    price       = EXCLUDED.price,
    rating      = EXCLUDED.rating,
    value_score = EXCLUDED.value_score,
    timestamp   = EXCLUDED.timestamp;
```

Use `psycopg2` with `executemany()` for batch inserts. Read connection parameters from Airflow Variables or environment variables.

---

## Task 5 — Metabase Dashboard SQL Queries

### 1. Price-to-Performance Scatter Plot

```sql
SELECT title, speed AS "Speed (MHz)", price AS "Price (₹)", capacity AS "Capacity (GB)", ddr_type
FROM ram_market_data
WHERE value_score IS NOT NULL
ORDER BY value_score DESC;
```

Configure in Metabase: X-axis = `Speed (MHz)`, Y-axis = `Price (₹)`, bubble size = `Capacity (GB)`, color by `ddr_type`.

### 2. Daily Best Deals — Top 5 in Last 24 Hours

```sql
SELECT title, price AS "Price (₹)", capacity AS "Capacity (GB)", speed AS "Speed (MHz)",
       ddr_type, ROUND(value_score::numeric, 4) AS "Value Score", url
FROM ram_market_data
WHERE timestamp >= NOW() - INTERVAL '24 hours'
  AND value_score IS NOT NULL
ORDER BY value_score DESC
LIMIT 5;
```

### 3. DDR4 vs DDR5 Average Price Trend — Last 30 Days

```sql
SELECT DATE(timestamp) AS "Date",
       ddr_type         AS "DDR Type",
       ROUND(AVG(price)::numeric, 2) AS "Avg Price (₹)"
FROM ram_market_data
WHERE ddr_type IN ('DDR4', 'DDR5')
  AND timestamp >= NOW() - INTERVAL '30 days'
GROUP BY DATE(timestamp), ddr_type
ORDER BY "Date" ASC;
```

Configure as a multi-series line chart with `ddr_type` as the series breakout.

---

## DAG Flow

```
create_table → extract(search_term) → transform → load → [Metabase reads live]
```
