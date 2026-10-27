"""State semantics against real A1 metadata and independent event expectations."""
import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ingestion.storage import Store, ROOT
from analytics.queries import snapshot, publish_snapshot, session_curve
from analytics.state import rebuild
from test_ingestion import signed


class AnalyticsTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.path=Path(self.tmp.name)/'test.duckdb'
        shutil.copy2(ROOT/'database/assignment2.duckdb',self.path)
        self.store=Store(self.path)
        self.events=[json.loads(s) for s in (ROOT/'data/window.jsonl').read_text().splitlines()]

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def empty(self):
        for table in ('meter_reading','current_price','charging_session','mqtt_event','mqtt_rejection'):
            self.store.db.execute(f'DELETE FROM core.{table}')

    def test_full_backfill_and_independent_metrics(self):
        db=self.store.db
        starts=[e for e in self.events if e['type']=='session_start']
        ends=[e for e in self.events if e['type']=='session_end']
        data=snapshot(db)
        self.assertEqual(len(data['stations']),433)
        self.assertEqual(len(data['sessions']),len(starts)-len(ends))
        self.assertEqual(sum(s['completed_sessions'] for s in data['stations']),len(ends))
        self.assertAlmostEqual(sum(s['completed_energy_kwh'] for s in data['stations']),sum(e['energy_kwh'] for e in ends))
        self.assertEqual(db.execute('SELECT count(*) FROM core.meter_reading').fetchone()[0],1886)
        # Independent occupancy integral over the RECEIVED horizon, not an assumed 03:00.
        from datetime import datetime
        lo=min(datetime.fromisoformat(e['ts']) for e in self.events)
        hi=max(datetime.fromisoformat(e['ts']) for e in self.events)
        endings={(e['station_id'],e['session_id']):datetime.fromisoformat(e['ts']) for e in ends}
        for station in data['stations']:
            occupied=sum((endings.get((e['station_id'],e['session_id']),hi)-datetime.fromisoformat(e['ts'])).total_seconds()
                for e in starts if int(e['station_id'])==station['ev_id'])
            expected=100*occupied/((hi-lo).total_seconds()*station['plugs_count'])
            self.assertAlmostEqual(station['utilisation_pct'],expected)
        before=db.execute('SELECT * FROM core.charging_session ORDER BY ev_id,session_id').fetchall()
        rebuild(db)
        self.assertEqual(before,db.execute('SELECT * FROM core.charging_session ORDER BY ev_id,session_id').fetchall())
        for event in self.events:
            self.assertEqual(self.store.receive('test/events',signed(event)),'duplicate')
        self.assertEqual(data['coverage']['received_events'],2452)

    def test_out_of_order_missing_start_and_latest_meter(self):
        end=next(e for e in self.events if e['type']=='session_end')
        group=[e for e in self.events if e.get('session_id')==end['session_id'] and e['station_id']==end['station_id']]
        self.empty()
        self.store.receive('test/events',signed(end))
        self.assertEqual(self.store.db.execute('SELECT status FROM core.charging_session').fetchone()[0],'incomplete')
        for event in reversed(group):
            self.store.receive('test/events',signed(event))
        row=self.store.db.execute('SELECT status,final_energy_kwh,latest_energy_kwh FROM core.charging_session').fetchone()
        self.assertEqual(row[0],'completed')
        self.assertEqual(row[1],end['energy_kwh'])
        meters=[e for e in group if e['type']=='meter']
        self.assertEqual(row[2],max(meters,key=lambda e:e['ts'])['energy_kwh'])
        self.assertEqual(len(session_curve(self.store.db,int(end['station_id']),end['session_id'])),len(meters))
        self.assertEqual(session_curve(self.store.db,int(end['station_id']),"' OR 1=1 --"),[])

    def test_rollback_and_restart(self):
        self.empty()
        event=next(e for e in self.events if e['type']=='session_start')
        with patch('ingestion.storage.apply',side_effect=RuntimeError('injected failure')):
            with self.assertRaises(RuntimeError):
                self.store.receive('test/events',signed(event))
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM core.mqtt_event').fetchone()[0],0)
        self.store.receive('test/events',signed(event))
        self.store.close()
        self.store=Store(self.path)
        self.assertEqual(self.store.db.execute('SELECT status FROM core.charging_session').fetchone()[0],'active')
        self.assertEqual(self.store.receive('test/events',signed(event)),'duplicate')

    def test_late_price_and_missing_price(self):
        self.empty()
        base=next(e for e in self.events if e['type']=='price_update')
        later=dict(base,ts='2026-09-01T02:00:00+10:00',price_per_kwh=0.99)
        self.store.receive('test/events',signed(later))
        self.store.receive('test/events',signed(base))
        self.assertEqual(self.store.db.execute('SELECT price_per_kwh FROM core.current_price').fetchone()[0],0.99)
        data=snapshot(self.store.db)
        self.assertEqual(sum(s['price_per_kwh'] is None for s in data['stations']),432)
        path=Path(self.tmp.name)/'dashboard.json'
        publish_snapshot(self.store.db,path)
        self.assertEqual(json.loads(path.read_text())['coverage']['received_events'],2)
        self.assertFalse(list(path.parent.glob('*.tmp')))


if __name__=='__main__':
    unittest.main()
