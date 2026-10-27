-- Member 3 additive schema. Apply after sql/ingestion.sql in the original A1 DB.
CREATE TABLE IF NOT EXISTS core.meter_reading (
    event_id VARCHAR PRIMARY KEY REFERENCES core.mqtt_event(event_id),
    ev_id BIGINT NOT NULL REFERENCES core.charger_record(ev_id),
    session_id VARCHAR NOT NULL, connector_id INTEGER NOT NULL,
    event_ts TIMESTAMPTZ NOT NULL, voltage_v DOUBLE NOT NULL,
    current_a DOUBLE NOT NULL, power_kw DOUBLE NOT NULL,
    energy_kwh DOUBLE NOT NULL, soc_pct DOUBLE NOT NULL
);
CREATE TABLE IF NOT EXISTS core.charging_session (
    ev_id BIGINT NOT NULL REFERENCES core.charger_record(ev_id),
    session_id VARCHAR NOT NULL, connector_id INTEGER NOT NULL,
    started_at TIMESTAMPTZ, ended_at TIMESTAMPTZ, charge_end_ts TIMESTAMPTZ,
    vehicle_model VARCHAR, price_per_kwh DOUBLE, final_energy_kwh DOUBLE,
    cost_aud DOUBLE, latest_meter_ts TIMESTAMPTZ, latest_power_kw DOUBLE,
    latest_energy_kwh DOUBLE, latest_soc_pct DOUBLE,
    status VARCHAR NOT NULL CHECK(status IN ('active','completed','incomplete')),
    inconsistent BOOLEAN NOT NULL, PRIMARY KEY(ev_id,session_id)
);
CREATE TABLE IF NOT EXISTS core.current_price (
    ev_id BIGINT PRIMARY KEY REFERENCES core.charger_record(ev_id),
    event_id VARCHAR NOT NULL REFERENCES core.mqtt_event(event_id),
    event_ts TIMESTAMPTZ NOT NULL, price_per_kwh DOUBLE NOT NULL,
    wholesale_per_kwh DOUBLE NOT NULL
);
-- Primary keys supply ART indexes; broad aggregates use columnar scans.
CREATE INDEX IF NOT EXISTS meter_session ON core.meter_reading(session_id);
CREATE INDEX IF NOT EXISTS session_charger ON core.charging_session(ev_id);
CREATE OR REPLACE VIEW analytics.session_projection AS
WITH fields AS (
 SELECT ev_id, session_id, connector_id, event_type, event_ts, event_id,
   json_extract_string(payload,'$.vehicle_model') AS vehicle_model,
   CAST(json_extract_string(payload,'$.price_per_kwh') AS DOUBLE) AS price_per_kwh,
   CAST(json_extract_string(payload,'$.charge_end_ts') AS TIMESTAMPTZ) AS charge_end_ts,
   CAST(json_extract_string(payload,'$.energy_kwh') AS DOUBLE) AS energy_kwh,
   CAST(json_extract_string(payload,'$.cost_aud') AS DOUBLE) AS cost_aud,
   CAST(json_extract_string(payload,'$.power_kw') AS DOUBLE) AS power_kw,
   CAST(json_extract_string(payload,'$.soc_pct') AS DOUBLE) AS soc_pct
 FROM core.mqtt_event WHERE session_id IS NOT NULL
), grouped AS (
 SELECT ev_id,session_id,min(connector_id) AS connector_id,
   min(event_ts) FILTER(WHERE event_type='session_start') AS started_at,
   max(event_ts) FILTER(WHERE event_type='session_end') AS ended_at,
   first(charge_end_ts ORDER BY event_ts DESC,event_id DESC) FILTER(WHERE event_type='session_end') AS charge_end_ts,
   first(vehicle_model ORDER BY event_ts,event_id) FILTER(WHERE event_type='session_start') AS vehicle_model,
   first(price_per_kwh ORDER BY event_ts,event_id) FILTER(WHERE event_type='session_start') AS price_per_kwh,
   first(energy_kwh ORDER BY event_ts DESC,event_id DESC) FILTER(WHERE event_type='session_end') AS final_energy_kwh,
   first(cost_aud ORDER BY event_ts DESC,event_id DESC) FILTER(WHERE event_type='session_end') AS cost_aud,
   max(event_ts) FILTER(WHERE event_type='meter') AS latest_meter_ts,
   first(power_kw ORDER BY event_ts DESC,event_id DESC) FILTER(WHERE event_type='meter') AS latest_power_kw,
   first(energy_kwh ORDER BY event_ts DESC,event_id DESC) FILTER(WHERE event_type='meter') AS latest_energy_kwh,
   first(soc_pct ORDER BY event_ts DESC,event_id DESC) FILTER(WHERE event_type='meter') AS latest_soc_pct,
   count(DISTINCT connector_id)>1 OR count(*) FILTER(WHERE event_type='session_start')>1
       OR count(*) FILTER(WHERE event_type='session_end')>1 AS conflicting
 FROM fields GROUP BY ev_id,session_id
), checked AS (
 SELECT *, conflicting OR coalesce(ended_at<started_at,false)
   OR coalesce(charge_end_ts<started_at,false)
   OR coalesce(latest_meter_ts>ended_at,false) AS inconsistent FROM grouped
)
SELECT ev_id,session_id,connector_id,started_at,ended_at,charge_end_ts,vehicle_model,
 price_per_kwh,final_energy_kwh,cost_aud,latest_meter_ts,latest_power_kw,
 latest_energy_kwh,latest_soc_pct,
 CASE WHEN inconsistent OR started_at IS NULL THEN 'incomplete'
      WHEN ended_at IS NOT NULL THEN 'completed' ELSE 'active' END AS status,
 inconsistent FROM checked;

CREATE OR REPLACE VIEW analytics.received_window AS
SELECT min(event_ts) AS observed_from,max(event_ts) AS observed_through,
 count(*) AS received_events FROM core.mqtt_event;

CREATE OR REPLACE VIEW analytics.station_metrics AS
WITH totals AS (
 SELECT ev_id,count(*) FILTER(WHERE status='active') AS active_sessions,
 count(*) FILTER(WHERE status='completed') AS completed_sessions,
 count(*) FILTER(WHERE status='incomplete') AS incomplete_sessions,
 count(DISTINCT connector_id) FILTER(WHERE status='active') AS occupied_connectors,
 sum(final_energy_kwh) FILTER(WHERE status='completed') AS completed_energy_kwh,
 sum(cost_aud) FILTER(WHERE status='completed') AS completed_cost_aud,
 sum(epoch(least(coalesce(ended_at,w.observed_through),w.observed_through))
      -epoch(greatest(started_at,w.observed_from))) FILTER(WHERE status<>'incomplete') AS occupied_seconds
 FROM core.charging_session CROSS JOIN analytics.received_window w GROUP BY ev_id
)
SELECT c.ev_id,coalesce(nullif(nullif(trim(c.station_name),''),'Unknown'),c.station_address) AS display_name,
 c.station_address,c.latitude,c.longitude,c.plugs_count,c.power_kw,c.operator_id,
 o.operator_name,r.sa4_name,p.price_per_kwh,p.event_ts AS price_event_ts,
 coalesce(t.active_sessions,0) AS active_sessions,
 coalesce(t.occupied_connectors,0) AS occupied_connectors,
 coalesce(t.completed_sessions,0) AS completed_sessions,
 coalesce(t.incomplete_sessions,0) AS incomplete_sessions,
 coalesce(t.completed_energy_kwh,0) AS completed_energy_kwh,
 coalesce(t.completed_cost_aud,0) AS completed_cost_aud,
 100.0*coalesce(t.occupied_seconds,0)/nullif(c.plugs_count*(epoch(w.observed_through)-epoch(w.observed_from)),0) AS utilisation_pct,
 coalesce(t.active_sessions,0)>coalesce(t.occupied_connectors,0) AS connector_conflict,
 w.observed_from,w.observed_through
FROM core.charger_record c
JOIN core.operator o USING(operator_id) JOIN core.sa4_region r USING(sa4_code)
LEFT JOIN totals t USING(ev_id) LEFT JOIN core.current_price p USING(ev_id)
CROSS JOIN analytics.received_window w WHERE c.charger_type='DC';

CREATE OR REPLACE VIEW analytics.daily_usage AS
SELECT ev_id,CAST(timezone('Australia/Sydney',ended_at) AS DATE) AS completion_date,
 count(*) AS completed_sessions,sum(final_energy_kwh) AS energy_kwh,
 sum(cost_aud) AS cost_aud,
 avg(epoch(ended_at)-epoch(started_at))/60 AS mean_occupancy_minutes
FROM core.charging_session WHERE status='completed' GROUP BY ev_id,completion_date;
