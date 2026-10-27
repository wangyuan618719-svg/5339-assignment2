# COMP5339 Assignment 2 — Member 1

Tasks 1–2: data generation and MQTT publishing. Following the updated tutor FAQ
(Q1–3), the submission uses **all 433 DC records for one day**, generating
**10,392 hourly prices and 3,906 charging sessions**. The FAQ replaces the earlier
14-day requirement with at least 10,000 hourly prices and approximately 4,000
sessions (3,900 is acceptable). The complete one-day CSV outputs are included;
MQTT replays every event in an initial three-hour window.

## Files and submission scope

| Location | Purpose |
|---|---|
| `prepare_data.py` | Extract all A1 DC records and record power assumptions |
| `COMP5339_2026s2_Assignment2_data _generator/` | Official generator, with an AEMO coverage check |
| `build_events.py` | Construct ordered window events from the CSVs |
| `publisher.py` | Publish JSONL messages through MQTT |
| `data/a1_chargers.csv` | Unmodified A1 snapshot, including AC and Upcoming records |
| `data/stations_input.csv`, `data/assumptions.csv` | Generator input and six power assumptions |
| `data/stations.csv` | Generated facilities, parameters and base prices |
| `data/prices_hourly.csv` | All one-day hourly prices |
| `data/sessions.csv`, `data/session_metering.csv` | All sessions and minute readings, including tails after midnight |
| `data/aemo_cache/` | Original AEMO monthly wholesale-price CSV |
| `data/window.jsonl`, `data/window_summary.json` | Actual MQTT message list and its counts, boundaries and checksum |
| `verification/` | Checks and logs for this DC/day dataset |
| `report_member1.md` | Draft sections for the group report |

`member1_submission.zip` contains the member 1 handoff. It excludes the local
`backups/ac_dc_14days/` archive, virtual environment, Python caches and original
assignment ZIP/PDF. The backup retains the previous large dataset, its scripts,
documentation and verification; its results do not describe the new submission.
The group must still supply the subscriber-side DuckDB database and dashboard.

For GitHub collaboration, the source files, current `data/` and `verification/`
are tracked directly. ZIP files and `backups/` remain local and are not uploaded.
After cloning, follow the setup commands below; the included dataset is ready to
publish without regeneration. Each member should work on a separate branch and
merge changes through a pull request.

## Setup and reproduction

Tested with Python 3.12. Data preparation, generation and event construction use
the standard library; `paho-mqtt` is the only additional runtime dependency.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python prepare_data.py data/a1_chargers.csv
python "COMP5339_2026s2_Assignment2_data _generator/generate_dataset.py" --stations data/stations_input.csv --start 2026-09-01 --days 1 --seed 5339 --sessions-per-connector-day 3 2 --out data
python build_events.py --hours 3
```

These commands overwrite generated outputs. The included AEMO cache supports
offline regeneration after dependency setup; it is downloaded only if absent.
Use dynamic prices and one-minute readings. Generation took 2.04 seconds and
approximately 94 MiB peak resident memory on this machine; window construction
took 0.18 seconds. Timings vary by machine.

Start the agreed broker and subscriber before publishing:

```bash
python publisher.py data/window.jsonl --host localhost --port 1883 --topic comp5339/YOUR_GROUP/events
```

## Dataset and modelling assumptions

Generation covers **[2026-09-01 00:00, 2026-09-02 00:00)**, seed 5339, NSW1.
The urban/regional session-rate inputs are 3 and 2 per connector per day, as
suggested by the FAQ. The realised count also depends on demand, price and
connector occupancy. Other simulation parameters retain official defaults.

| Output | Count |
|---|---:|
| DC records (urban / regional) | 433 (252 / 181) |
| Simulated connectors | 1,658 |
| Hourly prices (24 per record) | 10,392 |
| Charging sessions | 3,906 |
| Meter readings | 118,030 |
| Sessions ending after midnight | 30 |

- `station_id = str(ev_id)` preserves A1 identity. Each source record is one
  simulated facility; records sharing coordinates are not merged. All 433 DC
  records are included. The 1,427 AC and 98 Upcoming records remain in the A1
  snapshot but receive no synthetic sessions or prices in this submission.
- Names use A1 names, with the address as fallback. Coordinates retain A1 latitude
  and longitude and its WGS84 assumption. `plugs_count` represents simulated
  simultaneous connectors, not verified shared-power limits or physical bays.
- A1 `power_kw` values are retained, as permitted by FAQ Q4. Five mixed DC ratings
  use A1's 350 kW maximum for every simulated connector; one unitless numeric
  rating retains A1's kW interpretation. `assumptions.csv` lists these six records.
  No power values are imputed, and the A1 source is unchanged.
- Greater Sydney maps to `urban`, Rest of NSW to `regional`; these are simulation
  categories. The original vehicle mix, derating, taper, battery-side voltage and
  current, and four-hour charging cap are unchanged. Telemetry is simulated,
  not measured grid-side data.
- Retail prices are hypothetical tariffs driven by AEMO wholesale deviations.
  Sessions use the price at their start; idle time is not charged. The only
  generator modification checks twelve distinct, aligned, finite five-minute
  AEMO observations per required hour, rejecting missing or invalid coverage.
  See `verification/generator.patch`; the random process is unchanged.
- Complete endings and readings are retained for all 30 cross-midnight sessions.
  Six facilities (IDs 124, 270, 352, 921, 959 and 969) have no session in this
  stochastic one-day run. They retain all 24 prices and their metadata; no
  sessions are fabricated to fill gaps.

## Replay window

The window is **[2026-09-01T00:00:00+10:00, 2026-09-01T03:00:00+10:00)**.
The CSVs retain naive times; JSON uses a fixed UTC+10 convention for September.
Only events inside the half-open window are sent.

| Event type | Count |
|---|---:|
| `price_update` | 433 |
| `session_start` | 71 |
| `meter` | 1,886 |
| `session_end` | 62 |
| **Total** | **2,452** |

All 433 facilities have an initial price. Retail prices stay unchanged throughout
this window, so repeated hourly prices are correctly omitted. Charging starts,
ends and meter readings provide dynamic occupancy and live session progress.
Nine sessions remain active at 03:00; do not fabricate their endings.

The minimum sending time is **245.1 seconds (about 4.1 minutes)**, plus connection,
acknowledgement and processing overhead. No window events are sampled or dropped.
`--hours` adjusts the end while always starting at the dataset origin. If a price
change demonstration is needed, a six-hour window includes the change at 05:00;
rebuild and publish its complete event list, then refresh verification for that
window. Arbitrary mid-period starts are not supported because they require
reconstructing earlier prices and active sessions.

## JSON and MQTT contract

One agreed topic carries all event types: `comp5339/<group>/events`. Messages are
UTF-8 JSON objects, QoS 1, `retain=False`.

| Fields | Meaning |
|---|---|
| `event_id` | Lowercase SHA-256 of canonical event JSON before adding the ID |
| `type` | One of the four types below |
| `ts` | Event time as ISO 8601 with `+10:00` |
| `station_id` | String representation of A1 `ev_id` |
| `session_id`, `connector_id` | Session identity and integer connector number; absent on price updates |

Canonical JSON uses sorted keys, UTF-8, unescaped Unicode and separators `,` and
`:` without spaces; non-finite numbers are forbidden. Replaying the same event
preserves its ID. A changed event payload has a different ID.

| Type | Additional fields |
|---|---|
| `price_update` | `price_per_kwh`, `wholesale_per_kwh` |
| `session_start` | `vehicle_model`, `price_per_kwh` |
| `meter` | `voltage_v`, `current_a`, `power_kw`, `energy_kwh`, `soc_pct` |
| `session_end` | `charge_end_ts`, `charge_minutes`, `energy_kwh`, `avg_kw`, `peak_kw`, `price_per_kwh`, `cost_aud` |

Numbers are JSON numbers; prices use AUD/kWh, cost AUD, power kW, energy kWh,
voltage V, current A and SoC percent. Meter `energy_kwh` is cumulative within a
session: **do not sum readings**. Final readings have three-decimal energy while
session totals have two decimals, so compare with the source rounding tolerance.

Start messages contain no future totals or ending times. A `session_end` occurs
at unplugging (`end_ts` in the source); `charge_end_ts` can be earlier, while the
connector remains occupied. Per-minute readings stop at charging completion.

Events sort by time, then `price_update`, `meter`, `session_end`, `session_start`,
then numeric station ID and session ID. This places a final reading before an
end and releases a connector before a simultaneous new start. The first price is
always sent; later prices are suppressed only when unchanged from the previous
published price at that facility. A return to an older price is a new change.

The publisher awaits each broker acknowledgement, then sleeps at least 0.1 seconds.
It stops on connection/publish failure or a 10-second acknowledgement timeout,
reporting the last confirmed event ID. It has no custom retry or resume mechanism.
After a failure, restart the window and let the consumer deduplicate by `event_id`.
The subscriber must be ready before replay: retained messages are not a history.

## Verification and team handoff

The new dataset was checked against the untouched A1 source: IDs, coordinates,
power, connectors, assumptions, all 24 prices per facility, session associations,
connector occupancy, minute-reading coverage, cumulative energy and rounded final
energy. All 2,452 window payloads, ordering and IDs match their source CSVs. A
second complete generation produced identical hashes for all four output CSVs.
`verification/data_checks.json` records counts, parameters and data checksums.

The local subscriber received all 2,452 messages with identical content and order.
The minimum measured send interval was 0.100723 seconds, and connection refusal
correctly stopped publishing. Results are recorded in `verification/mqtt.json`
and the publisher log. Validation utilities ran temporarily outside the project; no test files are
required to prepare or publish data. Archived tests and MQTT results under
`backups/` apply only to the old dataset.

For group integration:

1. Member 2 starts the agreed broker and subscriber before replay. QoS 1 can
   redeliver, so members 2–3 deduplicate by `event_id`.
2. Compare the expected IDs from `window.jsonl`, unique received IDs and stored
   DuckDB IDs, together with event counts and payloads. Generated events must
   enter the database through MQTT, not a direct CSV/JSONL bulk load.
3. Member 4 shows all 433 simulated DC records, including no-session facilities,
   joined to A1 by `ev_id`. If the map also shows AC or Upcoming records, label
   them as outside the simulation; do not invent dynamic state for them.
4. Historical metrics use received events only. They cannot claim coverage of
   the full generated day unless those events have actually been received.

A broker acknowledgement does not prove a DuckDB commit. Members 2–4's code is
not included here, so subscriber-to-DuckDB integration and map behaviour still
require joint verification. FAQ Q6 also requires the group's subscriber-side
DuckDB database in the final submission. Review and merge the report draft and
complete the required truthful contribution and genAI usage statements.


---

## Member 2 subscription delivery

This section adds MQTT subscription and ingestion to the producer described above.
It covers member 2's group allocation (Stream Gateway and Ingestion), which is
part of official assignment Task 3. The earlier setup and handoff notes describe
member 1's producer package; use the instructions below to run the subscriber.
The repository now includes a subscriber-side database, while the final session
analytics and dashboard still require integration by members 3 and 4. This English
section is the complete operational and technical handoff for the subscriber;
no separate guide is required to follow its setup or integration instructions.

Read this section in the following order:

- [Scope and data flow](#scope-and-data-flow)
- [Requirements and setup](#requirements-and-setup)
- [Added files](#added-files)
- [Choose a broker](#step-1-start-or-choose-one-mqtt-broker), [start the subscriber](#step-2-start-the-subscriber), then [publish](#step-3-start-the-publisher)
- [Create a fresh inbox](#optional-start-with-an-empty-event-inbox) for a new-ingestion demonstration
- [Tests and evidence](#optional-tests-and-recorded-evidence)
- [Ingestion behaviour](#ingestion-behaviour-and-delivery-guarantees) and [troubleshooting](#troubleshooting)
- [Member 3 handoff](#handoff-to-member-3-storage-and-analytics) and [joint acceptance](#member-3-and-member-4-joint-acceptance-checklist)

### Scope and data flow

Member 1 supplies generator inputs, the generated CSVs, the ordered event file and
the publisher. Member 2 receives those events through MQTT and produces a durable,
validated event inbox linked to the existing A1 database. Member 3 builds session
state, historical analytics and a query interface; member 4 uses that interface
for the map, charger details and live session curves.

```text
Member 1 publisher -> MQTT broker -> subscriber network callback
    -> bounded queue -> validation and A1 mapping -> DuckDB commit
    -> MQTT acknowledgement

Committed event inbox -> member 3 state/analytics and live read interface
    -> member 4 dashboard
```

The supplied replay is the half-open interval
`[2026-09-01T00:00:00+10:00, 2026-09-01T03:00:00+10:00)` and includes all 2,452
events in that interval. It is distinct from the full generated day: 10,392 hourly
prices, 3,906 sessions and 118,030 meter readings for 433 DC charger records.
The subscriber does not bulk-load those CSVs or import JSONL directly into its
inbox. `data/window.jsonl` is the publisher input and the verification reference.
The static `data/stations_input.csv` is used only to validate the simulation's
station identities and attributes against A1.

The included database retains 1,958 A1 charger records, including AC and Upcoming
records outside this DC simulation. Each A1 `ev_id` remains a separate record;
sharing a geographic location does not merge charger identities. The current
handoff implements ingestion, not a completed dashboard or analytical service.

### Requirements and setup

**Python 3.12 is recommended.** The implementation was verified with Python
3.12.14, paho-mqtt 2.1.0, DuckDB 1.5.6 and pytz 2026.5. Runtime dependencies are pinned in
`requirements.txt`; no particular environment manager or environment name is
required.

`pytz` supports fetching DuckDB timezone-aware timestamps into Python, including
the event-time columns used by the handoff query examples.

Run all commands from the repository root. Use an existing Python 3.12 virtual
environment, or create one:

```sh
python -m venv .venv
```

Activate it using the command for your shell:

```powershell
# Windows PowerShell
.\.venv\Scripts\Activate.ps1
```

```sh
# macOS / Linux
source .venv/bin/activate
```

Check that the selected interpreter is Python 3.12, then install the runtime
dependencies. Activate the environment again in every additional terminal used
below; the environment name itself is your choice:

```sh
python --version
```

Install the packages:

```sh
python -m pip install -r requirements.txt
```

The included dataset is ready to replay. Regenerating the producer CSVs or
rebuilding `data/window.jsonl` is not required for these instructions.

### Added files

| Location | Purpose |
|---|---|
| `subscriber.py` | init/listen CLI, MQTT callbacks, bounded queue and single database writer |
| `ingestion/` | Event validation, A1 identity checks, durable deduplication and quarantine |
| `sql/ingestion.sql` | Additive event/rejection tables and joined view in A1's `core` schema |
| `sql/handoff_queries.sql` | Executable read-only examples for current price, active sessions, history and meter curves |
| `sql/a1_schema_reference.sql` | Original A1 DDL reference; rebuilding it requires the spatial extension |
| `database/assignment2.duckdb` | A1 database extended with the 2,452 events already received through MQTT |
| `tools/dev_broker.py` | Optional Python MQTT broker for local testing and demonstrations |
| `compose.yaml`, `broker/mosquitto.conf` | Alternative local Mosquitto setup using Docker |
| `tests/`, `tools/test_mqtt_recovery.py` | Contract, persistence and isolated MQTT recovery tests |
| `tools/review_member1.py` | Producer handoff inventory and replay reconstruction check |
| `tools/verify_ingestion.py` | Read-only source-to-database verification |
| `tools/verify_a1_preserved.py` | Exact comparison of original A1 tables with the extended database |
| `verification/task2/` | Recorded test results; temporary scratch databases are excluded from Git |
| `report_member2.md` | Draft material to merge into the group report, not runtime instructions |

### Step 1 Start or choose one MQTT broker

A broker is required, but **Docker is optional**. Choose one of the following
options; do not start multiple brokers on the same host and port.

**Option A: Python broker for a local demonstration**

Install the optional test/demo dependencies, then start the included broker:

```sh
python -m pip install -r requirements-test.txt
python tools/dev_broker.py
```

Keep this terminal running. The broker listens on `127.0.0.1:1883`. It does not
persist sessions or offline messages to disk, so restart the replay if its state
is lost. `requirements-test.txt` includes amqtt 0.12.1 and is not required when
using another broker.

**Option B: An existing local or team MQTT broker**

Use a running Mosquitto or other compatible MQTT broker that accepts this
project's plain MQTT connection and QoS 1 subscription. If you choose local
Mosquitto, install and start it separately. For a broker on another machine, pass
its address using `--host` in both subscriber and publisher commands below. The
subscriber does not currently expose username/password or TLS options.

**Option C: Mosquitto using Docker**

If Docker Desktop or Docker Engine with Compose is installed and running:

```sh
docker compose up -d
```

This publishes the broker on `127.0.0.1:1883` and stores broker state in a Docker
volume. The supplied configuration is for local use; it does not expose a broker
for other computers to connect to. To stop it after use:

```sh
docker compose stop mqtt
```

### Step 2 Start the subscriber

Open a second terminal, activate the same Python environment and run from the
repository root:

```sh
python subscriber.py listen --host localhost --port 1883 --topic comp5339/TUT19-AssignmentGroup-08/events --summary verification/task2/local_summary.json
```

**Wait for `READY` before publishing.** This confirms the broker granted the QoS 1
subscription. Messages published before the first subscription are not recovered
from the non-retained stream.

The default database is `database/assignment2.duckdb`. It is already initialised
and contains the verified replay. You do not need the original A1 database merely
to run the subscriber against this included file. Do not delete the database to
"reset" it: the original A1 relations must remain present. Use `init` with a new
target when a fresh inbox is needed.

The listener is long-running by default. Its `READY` log includes the accepted
topic and client ID. During processing it logs cumulative `stored`, `duplicate`
and `rejected` outcomes every 250 processed deliveries, and logs quarantine
warnings immediately. The final `STOP` summary includes outcomes and failures;
`--summary` saves that summary when the listener exits, rather than continuously
updating a live status file.

Listener configuration:

| Option | Default | Meaning |
|---|---|---|
| `--db` | `database/assignment2.duckdb` | Existing A1-based database to extend/use |
| `--host` | `localhost` | Address of the chosen MQTT broker |
| `--port` | `1883` | Broker TCP port |
| `--topic` | `comp5339/TUT19-AssignmentGroup-08/events` | Exact topic; wildcards are not accepted |
| `--client-id` | `comp5339-group08-member2` | Stable MQTT session identity; do not share between running clients |
| `--queue-size` | `512` | Maximum pending in-memory deliveries before an explicit overflow failure |
| `--run-seconds` | `0` | Run until stopped; a positive value limits the run, including connection time |
| `--summary` | Not set | Optional JSON output path for the final run summary |

For a timed full replay, allow more than the minimum publish duration and leave
time for connection and final processing. A listener stopping before the
publisher finishes cannot establish complete consumption. If a timed run never
receives a successful QoS 1 subscription acknowledgement, it reports failure.

### Step 3 Start the publisher

Open a third terminal with the environment activated and run:

```sh
python publisher.py data/window.jsonl --host localhost --port 1883 --topic comp5339/TUT19-AssignmentGroup-08/events
```

Use the same host, port and exact topic in both commands. The example uses the
team topic; replace it consistently if your team agrees on another topic. Run only
one subscriber with a given client ID; separate subscriber instances require
separate `--client-id` values and must not write the same DuckDB file from
separate processes.

The full 2,452-message replay takes at least about 4.1 minutes plus overhead.
The three-hour event window has initial prices but no subsequent tariff changes;
charging-session events and meter readings provide the changing activity.

**Replaying into the included database produces duplicates rather than new rows.**
This is expected: `event_id` deduplication preserves one row per event. For a
fresh ingestion demonstration, use a new database as described below.

After publishing completes, stop the subscriber with Ctrl+C. It finishes queued
work before exiting. Then verify the database:

```sh
python tools/verify_ingestion.py
```

For the supplied window, verification should report 2,452 events, 433 chargers
with received prices, nine active sessions at the window boundary, zero rejected
source events and `passed: true`. The included completed database can pass this
check even before another replay; use a fresh database to demonstrate new inserts.
The verifier writes a JSON result and returns a nonzero exit status if its checks
fail. Its default output is `verification/task2/full_verification.json`; use
`--output` to preserve the supplied evidence when recording another run.

Expected unique rows for the supplied window:

| Event type | Count | Meaning |
|---|---:|---|
| `price_update` | 433 | Initial price for every simulated DC charger |
| `session_start` | 71 | Connector occupation begins |
| `meter` | 1,886 | Progress readings during charging |
| `session_end` | 62 | Connector released at unplugging |
| **Total** | **2,452** | Nine started sessions remain active at the window boundary |

If the publisher is interrupted, keep the original event file and restart the
whole replay after the subscriber is ready. Duplicate deliveries are expected and
safe for the current inbox. A producer `confirmed` count means the broker ACKed
the messages; compare the subscriber summary and the database verification before
claiming successful end-to-end ingestion.

### Optional Start with an empty event inbox

This requires the team's **original A1 DuckDB database**, not a CSV export or the
already populated A2 database. Close/checkpoint the original before copying it.
Replace `path/to/a1/ev_chargers.duckdb` below with its actual location:

```sh
python subscriber.py init --a1-db "path/to/a1/ev_chargers.duckdb" --db database/fresh_demo.duckdb
python subscriber.py listen --db database/fresh_demo.duckdb --host localhost --port 1883 --topic comp5339/TUT19-AssignmentGroup-08/events
```

`init` copies the original A1 database and adds the ingestion objects; it refuses
to overwrite an existing target. Start the broker first, wait for subscriber
`READY`, then run the publisher from Step 3. After publishing and stopping the
subscriber, verify the selected database explicitly:

```sh
python tools/verify_ingestion.py --db database/fresh_demo.duckdb --output verification/task2/fresh_demo_verification.json
```

A1 spatial queries and rebuilding the original DDL require the spatial extension
for the installed DuckDB version. Ingestion itself reads A1 scalar identifiers,
connector counts and coordinates.

### Optional Tests and recorded evidence

The basic contract tests can run with the runtime dependencies:

```sh
python -m unittest discover -s tests -v
```

The persistence test is skipped unless `A1_TEST_DB` points to an original A1
database. To include it, set that variable for your shell; substitute the actual
path in these examples:

```powershell
# Windows PowerShell
$env:A1_TEST_DB="path/to/a1/ev_chargers.duckdb"
```

```sh
# macOS / Linux
export A1_TEST_DB="path/to/a1/ev_chargers.duckdb"
```

Then rerun the unit tests. The isolated broker-recovery tests additionally need
`requirements-test.txt` and a free local port 18884:

```sh
python -m pip install -r requirements-test.txt
python tools/test_mqtt_recovery.py --a1-db "path/to/a1/ev_chargers.duckdb"
python tools/review_member1.py
```

Recorded evidence in `verification/task2/` covers full replay, payload equality,
A1 preservation, duplicates, malformed-message quarantine, wrong-topic isolation,
reconnection, process restart and recovery after queue overflow. Separate
Mosquitto tests cover all four event types and offline delivery after a broker
restart. These are recorded results, not a claim that the final dashboard has
been tested.

### Integration notes

The subscriber commits valid events to `core.mqtt_event` or saves invalid messages
to `core.mqtt_rejection` before acknowledging delivery. A foreign key links each
valid event to A1's `core.charger_record.ev_id`; QoS 1 redelivery is deduplicated
by `event_id`. Broker ACKs alone do not prove a database commit.

Stop the subscriber before opening its DuckDB file from another process. A live
dashboard needs an agreed shared-service or read-snapshot design; it cannot simply
open the file while a separate subscriber process holds its write lock. Members
3 and 4 still own session state, historical analytics, final query optimisation,
the map and live session curves.

### Ingestion behaviour and delivery guarantees

The important processing boundaries are implemented in these functions:

| Code | Responsibility |
|---|---|
| `subscriber.run` | Client setup, callbacks, queue, writer lifecycle and final summary |
| `on_connect` / `on_subscribe` inside `run` | Subscribe after connection and log READY only after QoS 1 is granted |
| `on_message` inside `run` | Copy delivery metadata and bytes into the bounded queue |
| `ingestion.validation.decode` | Validate the producer contract and canonical event hash |
| `Store.__init__` | Open the A1-based database, validate station mapping and apply additive ingestion DDL |
| `Store.receive` | Persist a new valid event, identify an existing event or quarantine invalid bytes |
| `ingestion.storage.initialise` | Copy an original A1 database into a new target and initialise the inbox |

For each delivery, the writer applies the following sequence:

1. Require a QoS 1 delivery without the retained-delivery flag. A retained value
   received on subscription is not treated as a historical replay.
2. Decode UTF-8 JSON as one object. Reject malformed text, duplicate JSON keys,
   non-finite numbers, unknown event types and missing or unexpected fields.
3. Validate timezone-aware timestamps, numeric types and ranges. Booleans are not
   accepted as numbers. Wholesale prices may be negative; other event numeric
   values must be nonnegative, and SoC cannot exceed 100 percent. A session end
   cannot place charging completion after unplugging.
4. Match the string `station_id` to a simulated station already checked against
   A1's DC `ev_id`, connector count and coordinates. Session connector IDs must be
   integers within that station's configured range.
5. Recompute the producer's canonical SHA-256 event identity. The ID is calculated
   from the event without `event_id`, with sorted JSON keys, compact separators
   and UTF-8 encoding. Invalid Unicode scalar content is quarantined as well.
6. Check `event_id` in the inbox. An identical committed event produces a
   `duplicate` outcome. A new event is inserted with its A1 foreign key and full
   canonical payload. An unexpected database payload conflict stops processing.
7. Return only after the database operation commits. The writer then requests the
   MQTT ACK if the delivery still belongs to the current connection. Validation
   failures are ACKed only after quarantine is committed; a storage failure does
   not ACK the failed operation.

The network callback does not write to DuckDB. One writer thread owns the write
connection and processes queued deliveries in arrival order. Incoming event times
are not used to discard late messages: member 3 must handle event ordering and
state reconciliation explicitly. New event inserts currently use DuckDB
statement-level autocommit; there is no automatic multi-table state transaction
until the storage component adds one.

QoS 1 permits redelivery. The guarantee provided here is **at-least-once transport
with an idempotent event inbox**, not a claim of end-to-end exactly-once delivery.
Deduplication survives subscriber restart because IDs are stored in DuckDB rather
than only in memory. The queue itself is not a durable journal.

The MQTT 3.1.1 client uses a persistent session and a stable client ID, reconnects
with a 1-to-30-second backoff and subscribes again after connection. Offline
recovery depends on the broker retaining that session and its queued messages.
The Python demo broker has no on-disk persistence; the Mosquitto Compose option
uses a persistent volume. Neither configuration provides unlimited offline
capacity or an absolute guarantee against sudden power loss. Replaying the
original window with inbox deduplication is the recovery fallback.

On queue overflow the listener reports failure and stops instead of silently
ignoring the condition. Already queued work may still commit during shutdown;
after fixing the cause, restart the subscriber and replay the window. The event
primary key handles whichever deliveries had already committed.

`core.mqtt_rejection` preserves the exact received bytes as Base64, the topic,
reason and reception time. It is an audit/quarantine table, not an analytics input.
Do not fix source events by editing inbox rows or by bypassing validation. Inspect
the reason, correct the producer/contract issue with the team and rerun an agreed
replay with fresh verification.

### Troubleshooting

| Symptom | Interpretation and action |
|---|---|
| No READY; broker unavailable warnings | Start the chosen broker and check host/port. The listener retries with backoff; do not start publishing yet. |
| Broker connection rejected or QoS 1 not granted | Check broker configuration. The current clients expose plain MQTT connection settings, not authentication or TLS arguments. |
| Address already in use | Another broker may already own the port. Use one broker, or agree on a different port at both ends. |
| Missing database or missing A1 tables | Use the included A2 database or initialise a new target from the original A1 database; an empty DuckDB file or CSV is insufficient. |
| A1 station mapping mismatch | Check that the generator inputs and A1 database are from the same team dataset; do not remove the checks or regenerate identifiers. |
| Mostly duplicate outcomes and no new rows | Expected when replaying into the populated supplied database. Use a new A1-based target to demonstrate first ingestion. |
| Quarantine warning | Read `core.mqtt_rejection.reason` after stopping the listener. Official window events should produce zero rejections. |
| DuckDB file lock error | Stop the other process using this database, or integrate a single writer service and a separate frontend read interface. |
| Bounded queue full or writer failed | Preserve the failure log, resolve the downstream error, restart and replay. Do not treat broker ACKs as proof of storage. |
| Timezone-aware timestamp fetch reports missing pytz | Install the complete `requirements.txt` in the active interpreter rather than only paho-mqtt and DuckDB. |
| Verification shows missing or extra events | Verify the correct database against the exact event file used. Partial or different replay windows require their own expected event list. |

### Verification evidence and limits

The recorded audit used a new copy of the original A1 database, a real local MQTT
broker and the complete original event window; it did not substitute a direct
JSONL import. The following files are the main evidence entry points:

| Evidence | Recorded result |
|---|---|
| [`audit_summary.json`](verification/task2/audit_summary.json) | Overall subscription audit summary and remaining group work |
| [`audit_full_verification.json`](verification/task2/audit_full_verification.json) | All 2,452 source IDs/payloads, arrival order, event counts and A1 joins match |
| [`audit_full_publisher.log`](verification/task2/audit_full_publisher.log) | All messages broker-confirmed; minimum measured send interval 0.1004622 seconds |
| [`audit_full_summary.json`](verification/task2/audit_full_summary.json) | Fresh subscriber run stored 2,452 events without processing failures |
| [`audit_packaged_database.json`](verification/task2/audit_packaged_database.json) | The supplied database also matches the reference window |
| [`a1_preservation.json`](verification/task2/a1_preservation.json) | All 11 original A1 tables compare equal, including original static data |
| [`audit_unit_tests.log`](verification/task2/audit_unit_tests.log) and [`audit_clean_tests.log`](verification/task2/audit_clean_tests.log) | Eight tests pass, including a clean Python 3.12 environment with runtime dependencies only |
| [`recovery_verification.json`](verification/task2/recovery_verification.json) | Duplicate, quarantine, restart, reconnect and queue-overflow recovery checks pass |
| [`handoff_query_verification.json`](verification/task2/handoff_query_verification.json) | Four read-only handoff queries produce the expected results |
| [`mosquitto_verification.json`](verification/task2/mosquitto_verification.json) and [`mosquitto_offline_recovery.json`](verification/task2/mosquitto_offline_recovery.json) | Separate Docker/Mosquitto smoke test and offline-message recovery after broker restart |

These records show what was executed against the recorded code and data. They do
not prove a later modified deployment is correct or replace a fresh integrated
test. The inbox and foreign key support the subscription requirement of correct
decoding and same-schema A1 integration. Final stateful analytics, physical design
for actual dashboard queries, dynamic mapping and live curves still require
members 3 and 4. No test here claims to simulate every power failure, disk failure
or unbounded load scenario, and no test result guarantees an assignment grade.

### Handoff to member 3 Storage and analytics

**Start from the included A2 database and the committed MQTT inbox, not from a
new station table or a direct bulk import of `window.jsonl`.** The original A1
tables and their keys are retained. `core.mqtt_event.ev_id` references
`core.charger_record.ev_id` in the same database and schema.

Keep the existing A1 operator, region, charger, plug and augmentation relations.
Do not recreate a reduced station table from CSV and discard their relationships.
The two added indexes in `sql/ingestion.sql` cover `(ev_id, event_ts)` and
`(session_id, event_ts)`; evaluate their usefulness for the actual analytical
queries instead of assuming they complete the physical design requirement.

The ingestion output contract is:

| Column/object | Contract |
|---|---|
| `core.mqtt_event.event_id` | VARCHAR primary key; stable producer SHA-256, one row per unique event |
| `ev_id` | BIGINT foreign key to the original A1 charger; wire `station_id` is its string form |
| `event_type` | `price_update`, `session_start`, `meter` or `session_end` |
| `event_ts` | TIMESTAMPTZ; use for event ordering and analytical state |
| `session_id`, `connector_id` | Session key and connector number; NULL for price events |
| `payload` | JSON with all original event fields and units, as documented in the producer contract above |
| `received_at` | TIMESTAMPTZ ingestion time; useful for auditing, not a replacement for event time |
| `topic` | Actual subscribed topic used for this delivery |
| `core.mqtt_rejection` | Quarantined bytes encoded in Base64, reason, topic and reception time; exclude from analytics |
| `core.mqtt_charger_events` | Inbox joined to A1 name, address, coordinates, type, connector count, operator ID and SA4 code |

Extend this schema with the session/state tables, views and dashboard queries you
need. Keep A1 identifiers and `event_id` deduplication. If deriving state during
ingestion, add it in the writer's processing path in `ingestion/storage.py` and
commit the inbox insert and all derived updates in **one transaction before the
existing writer acknowledges the MQTT message**. Do not implement the state
update after ACK or let a second process independently write this same file.
For an already populated inbox, derive state from its committed rows and then
continue incrementally; replay alone will return duplicates and skip new inserts.

Use `sql/handoff_queries.sql` as executable read-only examples after stopping the
subscriber. It demonstrates latest received prices, active sessions, completed
session counts and meter-curve fields. Its queries have been checked against the
included replay. They are a starting point, not a completed analytical service or
proof that these indexes optimise the final dashboard workload.

To inspect the read-only examples from Python after stopping the separate
subscriber, execute the following in a Python session or a temporary script from
the repository root. Change the database path if you used a fresh target:

```python
from pathlib import Path
import duckdb

with duckdb.connect("database/assignment2.duckdb", read_only=True) as connection:
    statements = connection.extract_statements(
        Path("sql/handoff_queries.sql").read_text(encoding="utf-8")
    )
    for number, statement in enumerate(statements, start=1):
        rows = connection.execute(statement.query).fetchall()
        print(f"Query {number}: {len(rows)} rows; sample={rows[:2]}")
```

The expected row counts are `433, 9, 433, 1886`. The third query returns a count
per station, with a total of 62 completed sessions. The first query retains all
DC stations and leaves a missing received price as NULL instead of zero. The
fourth returns all meter points; member 3's query interface should filter by
`ev_id` and `session_id` using SQL parameters. For timezone-aware display, choose
an explicit presentation timezone consistent with the replay's UTC+10 convention;
do not silently interpret UTC database query output as local simulation time.

For charger pop-ups, obtain additional A1 fields from the existing relations,
including operator and plug information; the inbox's convenience view does not
by itself include every requested detail. Handle missing or placeholder station
names explicitly, for example by displaying the original address as fallback.

Important state rules:

- Order by event time, not arrival time. Use the producer's documented tie order
  for chronological replay: price, meter, session end, session start. A late or
  duplicate event must not overwrite newer state incorrectly.
- Track occupancy by connector/session, not a single station boolean: a station
  can host several simultaneous sessions. Release a connector at `session_end`
  (unplugging), not at `charge_end_ts` or the final meter reading.
- `meter.energy_kwh` is cumulative within a session. Plot or take the latest
  reading; never sum meter readings. Prices are AUD/kWh and costs are AUD.
- The replay ends with nine sessions still active; preserve them. A different
  replay starting mid-session requires an explicit policy for unknown initial
  state or missing starts. Do not fabricate history or endings.
- Keep all 433 simulated DC stations in station-level results, including those
  without sessions. Dynamic values for unsimulated AC/Upcoming records are
  unknown, not zero. Metrics describe the received window, not the generated day.

Before handing your storage component to member 4, agree on a live read interface.
The recommended arrangement is one service process owning ingestion and derived
state, with separate connections in its query threads and an API for the frontend.
An atomically published read snapshot is another option. This repository does
**not** yet implement either interface. Merely stopping the subscriber to run
queries is useful for inspection but does not satisfy a live dashboard.

### Member 3 and member 4 joint acceptance checklist

After integrating their components, run a fresh database replay through MQTT and
confirm:

1. All 2,452 unique event IDs and payloads match `data/window.jsonl`; type counts
   are 433 prices, 71 starts, 1,886 meters and 62 ends, with zero rejected source
   messages. Replaying again must not inflate events, sessions or derived metrics.
2. All original A1 data remain intact; every received charger matches its A1 key.
   Session state shows nine active sessions at the final window boundary.
3. The dashboard provides at least one current-state metric and one historical
   metric from stored received events. A simple example is current price/active
   sessions plus completed-session count; other justified choices are allowed.
4. The map keeps correct A1 positions and charger details, changes dynamically as
   events arrive, and supports working marker pop-ups. A currently active session
   has a live-updated meter curve. Do not invent price changes in this window.
5. The frontend can read while ingestion continues, without DuckDB process-lock
   errors. Check the actual dashboard queries, document any physical design
   changes and capture the running dashboard for the report.
6. Include the final extended DuckDB, all new DDL, code/dependencies, generated
   inputs/outputs and replay events in the final package. Merge the report drafts
   within the six-page content limit and complete truthful contribution and genAI
   declarations. These are group acceptance steps, not completed member 2 tests.

The producer's only code adjustment is its interval measurement clock, from
`monotonic()` to `perf_counter()`, to avoid coarse timing measurements on
Windows/Python 3.12. Event contents, publish ACK handling and the minimum
0.1-second sleep are unchanged. The complete replay was repeated after this
adjustment.

### Team handoff and submission preparation

Member 3 should first inspect the populated inbox and query examples, agree on
session/state tables and a frontend read interface, and implement an idempotent
backfill of existing inbox rows before incremental processing. Member 4 should
build against that agreed read contract, not create another competing writer for
the same database. Both members should then perform the joint acceptance replay
above with an initially empty event inbox.

Coordinate ownership of `database/assignment2.duckdb` before merging independently
changed copies: a binary database cannot be resolved as an ordinary text merge.
Keep schema changes in versioned SQL and preserve a reproducible build/ingestion
procedure. Temporary database copies, Python environments and caches are not
runtime deliverables; the scratch directory used by tests is excluded from Git.

The English README, source, SQL and verification outputs provide the team-facing
handoff. `report_member2.md` is report source material to edit into the final
six-page group report, not a second standalone report or a substitute for setup
instructions. Record actual student contributions and acknowledge the substantive
OpenAI Codex assistance with code, validation and documentation in the required
genAI usage report. The final database, complete schema documentation and running
dashboard evidence must reflect the group's integrated implementation.


## Member 3: Storage and Analytics

See [Member 3 handoff](MEMBER3_HANDOFF.md) for the seven-stage
[notebook](Member3_Storage_Analytics.ipynb), stateful DuckDB storage,
Plotly figures and the Member 4 JSON snapshot interface.

Runtime updates extend the existing subscriber transaction. Original A1 records
and Member 2 inbox payloads are preserved. Read [AI assistance](AI_ASSISTANCE.md)
when preparing the group disclosure.
