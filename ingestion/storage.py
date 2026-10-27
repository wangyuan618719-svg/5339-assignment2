"""One writer owns DuckDB; only return to MQTT after a durable commit."""
import base64
import csv
import shutil
import uuid
from pathlib import Path

import duckdb

from .validation import InvalidEvent, canonical, decode
from analytics.state import apply, rebuild

ROOT = Path(__file__).resolve().parents[1]


def initialise(source, target):
    source, target = Path(source), Path(target)
    if source.resolve() == target.resolve():
        raise ValueError('use a new target; never modify the original A1 database')
    if target.exists():
        raise FileExistsError(target)
    if Path(str(source) + '.wal').exists():
        raise ValueError('close/checkpoint the A1 database before copying it')
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)
    with Store(target):
        pass


class Store:
    def __init__(self, path, station_path=ROOT / 'data/stations_input.csv'):
        if not Path(path).is_file():
            raise FileNotFoundError('initialise an A1 database copy first')
        self.db = duckdb.connect(str(path))
        try:
            self.db.execute("SET TimeZone='UTC'")
            known = {str(sid): (plugs, lat, lon) for sid, plugs, lat, lon in self.db.execute(
                "SELECT ev_id,plugs_count,latitude,longitude FROM core.charger_record WHERE charger_type='DC'").fetchall()}
            self.stations = {}
            with Path(station_path).open(encoding='utf-8', newline='') as stream:
                for row in csv.DictReader(stream):
                    sid = row['station_id']
                    if sid in self.stations or sid not in known:
                        raise ValueError(f'invalid A1 station mapping: {sid}')
                    plugs, lat, lon = known[sid]
                    if (plugs != int(row['connectors']) or abs(lat - float(row['lat'])) > 1e-8
                            or abs(lon - float(row['lon'])) > 1e-8):
                        raise ValueError(f'A1 station attributes mismatch: {sid}')
                    self.stations[sid] = plugs
            if not self.stations:
                raise ValueError('empty station contract')
            self.db.execute((ROOT / 'sql/ingestion.sql').read_text(encoding='utf-8'))
            rebuild(self.db)
        except BaseException:
            self.db.close()
            raise

    def receive(self, topic, payload, retained=False, qos=1):
        try:
            if retained or qos != 1:
                raise InvalidEvent('expected non-retained QoS 1 delivery')
            event = decode(payload, self.stations)
        except InvalidEvent as exc:
            # Poison messages are acknowledged only after their exact bytes are saved.
            self.db.execute('INSERT INTO core.mqtt_rejection(rejection_id,topic,payload_base64,reason) VALUES (?,?,?,?)',
                            [str(uuid.uuid4()), topic, base64.b64encode(payload).decode('ascii'), str(exc)])
            return 'rejected'
        existing = self.db.execute('SELECT payload FROM core.mqtt_event WHERE event_id=?',
                                   [event['event_id']]).fetchone()
        if existing:
            if existing[0] != canonical(event):
                raise RuntimeError('event ID conflict in database')
            return 'duplicate'
        self.db.execute('BEGIN')
        try:
            self.db.execute('INSERT INTO core.mqtt_event(event_id,ev_id,event_type,event_ts,session_id,connector_id,topic,payload) VALUES (?,?,?,?,?,?,?,?)',
                            [event['event_id'], int(event['station_id']), event['type'], event['ts'],
                             event.get('session_id'), event.get('connector_id'), topic, canonical(event)])
            apply(self.db, event)
            self.db.execute('COMMIT')
        except BaseException:
            self.db.execute('ROLLBACK')
            raise
        return 'stored'

    def close(self):
        self.db.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
