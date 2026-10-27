"""Derived tables are rebuildable; the MQTT inbox remains the source of truth."""
from pathlib import Path

DDL = Path(__file__).resolve().parents[1] / 'sql/analytics.sql'
METER_SELECT = """SELECT event_id,ev_id,session_id,connector_id,event_ts,
 CAST(payload->>'voltage_v' AS DOUBLE),CAST(payload->>'current_a' AS DOUBLE),
 CAST(payload->>'power_kw' AS DOUBLE),CAST(payload->>'energy_kwh' AS DOUBLE),
 CAST(payload->>'soc_pct' AS DOUBLE) FROM core.mqtt_event WHERE event_type='meter'"""
PRICE_SELECT = """SELECT ev_id,event_id,event_ts,CAST(payload->>'price_per_kwh' AS DOUBLE),
 CAST(payload->>'wholesale_per_kwh' AS DOUBLE) FROM core.mqtt_event WHERE event_type='price_update'"""


def rebuild(db):
    """Atomic, idempotent migration/backfill from already committed MQTT rows."""
    db.execute('BEGIN')
    try:
        db.execute(DDL.read_text(encoding='utf-8'))
        for table in ('meter_reading', 'charging_session', 'current_price'):
            db.execute(f'DELETE FROM core.{table}')
        db.execute('INSERT INTO core.meter_reading ' + METER_SELECT + ' ORDER BY session_id,event_ts,event_id')
        db.execute('INSERT INTO core.charging_session SELECT * FROM analytics.session_projection')
        db.execute('INSERT INTO core.current_price ' + PRICE_SELECT +
                   ' QUALIFY row_number() OVER(PARTITION BY ev_id ORDER BY event_ts DESC,event_id DESC)=1')
        db.execute('COMMIT')
    except BaseException:
        db.execute('ROLLBACK')
        raise


def apply(db, event):
    """Called INSIDE the inbox transaction, before MQTT ACK. Event-time deterministic."""
    sid = int(event['station_id'])
    if event['type'] == 'price_update':
        db.execute('DELETE FROM core.current_price WHERE ev_id=?', [sid])
        db.execute('INSERT INTO core.current_price ' + PRICE_SELECT +
                   ' AND ev_id=? ORDER BY event_ts DESC,event_id DESC LIMIT 1', [sid])
    else:
        if event['type'] == 'meter':
            db.execute('INSERT INTO core.meter_reading ' + METER_SELECT + ' AND event_id=?', [event['event_id']])
        params = [sid, event['session_id']]
        db.execute('DELETE FROM core.charging_session WHERE ev_id=? AND session_id=?', params)
        db.execute('INSERT INTO core.charging_session SELECT * FROM analytics.session_projection WHERE ev_id=? AND session_id=?', params)
