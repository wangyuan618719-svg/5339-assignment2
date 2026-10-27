# COMP5339 Assignment 2 — Member 3 Handoff

This package extends the Member 1/2 repository at commit
`f7826c68afcc314703f4245079e45ac20b7fb885` with stateful DuckDB storage, charging
session analytics, Plotly figures and a live frontend snapshot interface.

## Start here

Use Python 3.12 and run commands from the extracted package root.

```sh
python -m venv .venv
source .venv/bin/activate
# Windows PowerShell: .venv\Scripts\Activate.ps1
python -m pip install -r requirements-notebook.txt
jupyter lab Member3_Storage_Analytics.ipynb
```

The notebook has seven executable stages: inspect the input database, build
state, query current state, query history, create figures, export, and verify.
Its outputs are saved under `artifacts/runs/<run-id>/`; it works on a database
copy. Implementation details remain in `analytics/` and `sql/analytics.sql`.

The included `database/assignment2.duckdb` already contains the committed MQTT
inbox and derived state. `artifacts/dashboard.json` is an initial saved snapshot
for frontend development. Both describe received events, not the complete
synthetically generated day.

## Files and ownership

| Path | Purpose |
|---|---|
| `Member3_Storage_Analytics.ipynb` | Seven-stage storage and analytics workflow with saved outputs |
| `analytics/state.py` | Atomic backfill and incremental event-time state updates |
| `analytics/queries.py` | Parameterized queries and atomic JSON snapshot export |
| `analytics/charts.py` | Current-state, historical and active-session Plotly figures |
| `sql/analytics.sql` | Added tables, foreign keys, indexes and analytical views |
| `sql/ingestion.sql` | Existing Member 2 event inbox and rejection schema |
| `sql/a1_schema_reference.sql` | Original A1 schema reference |
| `ingestion/storage.py` | Inbox insertion and derived updates in one transaction |
| `subscriber.py` | Single database writer; commit before ACK; periodic snapshot publication |
| `database/assignment2.duckdb` | A1 data, received MQTT events and Member 3 state |
| `artifacts/dashboard.json` | Initial frontend snapshot |
| `artifacts/figures/` | Plotly JSON and standalone HTML figures |
| `verification/task3/` | Recorded full MQTT audit, tests and query plans |
| `verification/package/` | Checks performed for this handoff package |
| `report_member3.md` | Storage and analytics section for the group report |
| `AI_ASSISTANCE.md` | Assistance record for the required group genAI disclosure |

The package also includes the existing producer scripts, generated datasets,
MQTT broker setup and ingestion tests so the full pipeline can be run locally.
Original Assignment 1 records and Member 1/2 event payloads are retained.

## Member 4: frontend integration

Read `artifacts/dashboard.json` to build the UI against the supplied state.
For live operation, start a broker, then the subscriber, then the publisher.

```sh
# Option A: existing Docker/Mosquitto configuration
docker compose up -d
# Option B, in a separate terminal, instead of Docker:
# python -m pip install -r requirements-test.txt
# python tools/dev_broker.py
```

```sh
# Terminal 2: wait for READY before publishing
python subscriber.py listen --snapshot runtime/dashboard.json --snapshot-interval 1
```

```sh
# Terminal 3: replay all source events at the required minimum interval
python publisher.py data/window.jsonl --topic comp5339/TUT19-AssignmentGroup-08/events
```

Poll `runtime/dashboard.json` in the frontend, typically once per second. The
subscriber writes a temporary file, flushes it and atomically replaces the prior
snapshot, so readers do not see a partially written document. Do not open the
live DuckDB file from another process. The interface assumes a shared local
filesystem; a remote frontend needs an HTTP service or shared storage.

```python
import json
from pathlib import Path
from analytics.charts import current_state_figure, history_figure, live_curve_figure

snapshot = json.loads(Path('runtime/dashboard.json').read_text(encoding='utf-8'))
station = next(s for s in snapshot['stations'] if s['ev_id'] == 100)
current = current_state_figure(station)
history = history_figure([r for r in snapshot['daily_usage'] if r['ev_id'] == 100])
curves = [live_curve_figure(s) for s in snapshot['sessions']
          if s['ev_id'] == 100 and s['status'] == 'active']
# Streamlit: st.plotly_chart(current)
# Dash: dcc.Graph(figure=current)
```

### Snapshot contract (schema_version = 1)

| Field | Meaning |
|---|---|
| `published_at` | UTC wall-clock publication time; use to detect stale snapshots |
| `coverage` | Received event count and minimum/maximum event time |
| `stations` | All 433 DC records: A1 identity, coordinates, name/address, operator, region, plug types, price and metrics |
| `sessions` | Active/incomplete sessions, including received meter points in `meter_curve` |
| `daily_usage` | Completed-session counts, final energy/cost and mean occupancy duration by Sydney completion date |

Use `ev_id` to join all objects and to identify map markers. Preserve multiple
sessions at the same station. `null` price means no received price; an empty
plug list means no recorded plug type. Display incomplete/conflict flags rather
than silently presenting these records as reliable state. Handle a missing
snapshot during startup and display stale publication time when ingestion stops.

The packaged inbox is already populated. Replaying it deduplicates existing
messages and will not animate a fresh build from zero. For a fresh run use an
original, closed A1 database copy:

```sh
python subscriber.py init --a1-db /path/to/original_A1.duckdb --db database/fresh.duckdb
python subscriber.py listen --db database/fresh.duckdb --snapshot runtime/dashboard.json
```

The verification script also creates an isolated empty-inbox copy automatically.
Do not overwrite or clear the team's only database. When merging a newer team DB,
run the additive migration/backfill against it instead of replacing newer events
with this binary file. New deployment paths can be set with the CLI flags above.

## Schema and state rules

The immutable `core.mqtt_event` inbox remains the source of truth.
`core.meter_reading` uses the event ID as a primary/foreign key to the inbox and
stores typed telemetry. `core.charging_session` is keyed by `(ev_id,session_id)`;
`core.current_price` stores one latest event-time price per charger. All reference
the original A1 charger key. Missing starts are permitted for meter ingestion and
represented as incomplete session state until reconciled.

`Store` atomically rebuilds derived state on startup from committed inbox rows.
For new events, the inbox insert and affected derived updates share a transaction
before MQTT acknowledgement. Duplicate event IDs do not inflate history. Latest
prices/readings are chosen by event time, with event ID as a deterministic tie
break; late arrivals cannot regress a newer value. Rebuild cost grows with inbox
size and is a documented scaling limitation.

- A session stays active until its received `session_end` (unplugging), even if
  metering stopped earlier. Nine sessions remain active in the supplied window.
- Historical energy/cost sum complete-session final values, not cumulative meter
  readings. Active-session energy is excluded from completed history.
- Daily totals assign a whole session to its Sydney completion date; they are not
  time-apportioned daily electricity consumption.
- Utilisation divides observed occupied connector-seconds by available connector-
  seconds over the received interval (00:00–02:59 Sydney time here). It assumes
  complete receipt from the simulation origin and is unreliable with missing
  initial state, gaps or overlapping connector histories.
- AC/Upcoming A1 records remain in the database but are outside this DC simulation.
  Do not invent dynamic state for them or claim full-day event coverage.

Typed columns avoid repeated JSON casts; materialized session state reduces
repeat aggregation. Primary-key ART indexes and selective session/charger indexes
supplement columnar scans. Broad queries on this small dataset may scan tables;
no blanket index speedup or date partitioning is claimed. Actual warm query
measurements and EXPLAIN ANALYZE plans are included in the verification directory.

## Verification

The supplied window contains 2,452 events: 433 prices, 71 starts, 1,886 meter
readings and 62 ends. Expected derived state is 62 completed and nine active
sessions, with 1,674.91 kWh from completed sessions, across 433 DC records.

The earlier full MQTT integration audit used the same runtime implementation:
all payloads matched, no source messages were rejected, 2,929 concurrent snapshot
reads observed 818 revisions, and all 11 original A1 tables compared equal.
`verification/task3/mqtt_integration.json` records that run. The package-specific
notebook and unit-test results are recorded separately under `verification/package/`.

```sh
python -m unittest discover -s tests -v
python tools/benchmark_member3.py
# Full real MQTT verification, approximately 5 minutes:
python -m pip install -r requirements-test.txt
python tools/verify_member3_mqtt.py
```

The inherited persistence test requires `A1_TEST_DB` pointing to a closed A1 DB
with an empty inbox; otherwise that test skips. The packaged test log records
all 12 tests with an isolated empty-inbox copy and no skips.

Member 4 still needs to connect the snapshot to the actual map, marker pop-ups
and refresh controls, and capture the integrated dashboard for the group report.
