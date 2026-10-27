# Event Storage and Integration with Assignment 1 Database

The storage component extends the original Assignment 1 DuckDB in place. All
1,958 charger records and their operator, region, plug and augmentation relations
are retained. The 433 simulated DC facilities preserve `station_id = str(ev_id)`;
`core.mqtt_event.ev_id` references the existing charger primary key. Events from
the subscriber therefore join the original metadata rather than a reconstructed
station table. The immutable inbox preserves canonical JSON, topic, event time and
reception time, while invalid messages remain quarantined outside analytics.

Three additional tables support stateful analysis. `core.meter_reading` stores
typed voltage, current, power, cumulative energy and state-of-charge values, with
a primary/foreign key to the original inbox event. `core.charging_session` uses
`(ev_id, session_id)` as its key and records lifecycle timestamps, connector,
vehicle, latest telemetry, final energy/cost and state. `core.current_price`
retains the latest event-time price per charger. Each table references the A1
charger key. Meter readings deliberately do not require an existing session-start
row, allowing late starts to be reconciled without losing received telemetry.
Versioned SQL scripts document the schema, views and indexes.

The inbox insert and corresponding derived updates commit in one transaction
before MQTT acknowledgement. Stable event IDs make redelivery idempotent. At
startup, state is rebuilt atomically from committed MQTT events, including the
previously populated member 2 inbox. Subsequent messages update the affected
session or price. Ordering uses event time, so a late price or reading cannot
replace a newer value. Missing starts and conflicting lifecycle information are
marked incomplete; an eventual valid start can resolve missing-start state.
Connector occupancy ends at unplugging, not charging completion, and concurrent
sessions are tracked separately.

Dashboard queries join materialized state with A1 metadata and retain all 433 DC
records, including stations without sessions. Current metrics include price and
active/occupied connectors. Historical metrics include completed-session count,
final energy and cost, grouped by Sydney completion date. Meter energy is
cumulative and is never summed across samples. Utilisation is the observed
occupied connector-seconds divided by available connector-seconds over the
received event-time span; active sessions are clipped to the last received event.
These metrics assume complete receipt from the start of the simulated window.
They do not represent the entire generated day or arbitrary windows with unknown
initial occupancy.

Typed telemetry avoids repeated JSON extraction. Materialized state reduces
repeated session aggregation to one row per session. Primary-key ART indexes and
selective session/charger indexes supplement DuckDB's columnar scans; date
partitioning is unnecessary for this small dataset. Thirty warm local repetitions
of equivalent 71-row session queries measured a median of approximately 9.59 ms
for raw-event derivation and 0.73 ms for materialized state. Actual query plans
are retained with the benchmark; these measurements do not establish production
throughput or guarantee index use.

The subscriber atomically publishes a JSON snapshot containing station metrics,
daily history and active-session curves. The frontend can poll this file while
ingestion continues without opening DuckDB from another process. Plotly helpers
produce current-state indicators, historical energy/session charts and meter
curves. Wall-clock publication time and simulation event-time coverage remain
separate, allowing the frontend to show stale data explicitly.

The supplied inbox contains 2,452 events: 433 prices, 71 starts, 1,886 readings and
62 ends. Derived state contains 62 completed and nine active sessions, with
1,674.91 kWh attributed to completed sessions. Tests cover independent metric
calculations, late arrivals, missing starts, deduplication, transactional rollback
and restart. A fresh real-MQTT replay verified all 2,452 payloads with zero rejections,
2,929 concurrent snapshot reads across 818 revisions, and zero differences in all
11 original A1 tables.

## Challenges and Limitations

DuckDB's cross-process writer lock motivated a single owner with atomic frontend
snapshots. The live interface is local-filesystem based and may lag under load;
a remote deployment would need an API. Startup rebuilding is simple and
reproducible but scales with inbox size. Incomplete or contradictory telemetry is
flagged rather than silently converted into trustworthy history. Future work
includes versioned incremental migrations, richer connector conflict detection
and bounded history/curve retention. The final map, interactive pop-ups and their
screenshot remain member 4's integration work.

