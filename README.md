# data_databricks

A Databricks data engineering pipeline that ingests CSV source data, enriches it through joins, and produces curated aggregations — all orchestrated as a single multi-task Databricks Job.

---

## Architecture

The pipeline follows a three-layer medallion architecture stored in Unity Catalog under `bloomsbdcatalog`:

```
GitHub CSVs (source)
       │
       ▼
┌─────────────────────────────────┐
│  RAW layer  (bloomsbdcatalog.raw)│
│  - bookings                     │
│  - passengers                   │
│  - airports                     │
└────────────┬────────────────────┘
             │
             ▼
┌─────────────────────────────────────────┐
│  ENR layer  (bloomsbdcatalog.enr)        │
│  - bookings_enriched                    │
│    (bookings ⟕ passengers ⟕ airports)  │
└────────────┬────────────────────────────┘
             │
             ▼
┌──────────────────────────────────────────┐
│  CUR layer  (bloomsbdcatalog.cur)         │
│  - bookings_per_city                     │
│    (aggregated booking count per city)   │
└──────────────────────────────────────────┘
```

---

## Pipeline Stages

### 1. Ingest — `ingest_csv_to_delta.py`
**Target schema:** `bloomsbdcatalog.raw`

Reads three CSV files directly from this GitHub repository using Pandas, converts them to Spark DataFrames, and writes them as Delta tables in the `raw` schema.

| Source CSV | Delta Table |
|---|---|
| `airports.csv` | `bloomsbdcatalog.raw.airports` |
| `bookings.csv` | `bloomsbdcatalog.raw.bookings` |
| `passengers.csv` | `bloomsbdcatalog.raw.passengers` |

**Table schemas:**

- `raw.bookings` — `booking_id`, `passenger_id`, `flight_id`, `airport_id`, `amount`, `booking_date`
- `raw.passengers` — `passenger_id`, `name`, `gender`, `nationality`
- `raw.airports` — `airport_id`, `airport_name`, `city`, `country`

---

### 2. Enrich — `enrich_bookings.py`
**Target schema:** `bloomsbdcatalog.enr`

Joins the three raw tables into one wide table:

- `bookings` LEFT JOIN `passengers` on `passenger_id`
- result LEFT JOIN `airports` on `airport_id`

**Output:** `bloomsbdcatalog.enr.bookings_enriched`

All columns from all three source tables are preserved in a single flat record per booking.

---

### 3. Curate — `bookings_per_city.py`
**Target schema:** `bloomsbdcatalog.cur`

Reads the enriched table and aggregates the total number of bookings per city, ordered by count descending.

**Output:** `bloomsbdcatalog.cur.bookings_per_city`

| Column | Description |
|---|---|
| `city` | Airport city |
| `booking_count` | Total number of bookings at that city |

---

## Job Orchestration

All three stages are orchestrated as tasks within a single Databricks Job named **`ingest_csv_to_delta`** (Job ID: `745120312112858`).

Tasks run sequentially — each depends on the success of the previous:

```
ingest_task  ──►  enrich_bookings  ──►  bookings_per_city
```

| Task | Script | Depends On |
|---|---|---|
| `ingest_task` | `ingest_csv_to_delta.py` | — |
| `enrich_bookings` | `enrich_bookings.py` | `ingest_task` |
| `bookings_per_city` | `bookings_per_city.py` | `enrich_bookings` |

---

## Unity Catalog Structure

```
bloomsbdcatalog
├── raw
│   ├── bookings
│   ├── passengers
│   └── airports
├── enr
│   └── bookings_enriched
└── cur
    └── bookings_per_city
```

---

## Source Data

CSV source files are hosted in this repository:

- `airports.csv`
- `bookings.csv`
- `passengers.csv`
