"""Fresh real-MQTT replay plus concurrent frontend snapshot reads (about 5 minutes)."""
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import duckdb
from tools.verify_ingestion import verify


def main():
    out=ROOT/'verification/task3'
    out.mkdir(parents=True,exist_ok=True)
    with socket.socket() as sock:
        sock.bind(('127.0.0.1',0)); port=sock.getsockname()[1]
    with tempfile.TemporaryDirectory() as directory:
        tmp=Path(directory); db=tmp/'fresh.duckdb'; snap=tmp/'dashboard.json'
        shutil.copy2(ROOT/'database/assignment2.duckdb',db)
        with duckdb.connect(str(db)) as conn:
            for table in ('meter_reading','current_price','charging_session','mqtt_event','mqtt_rejection'):
                conn.execute(f'DELETE FROM core.{table}')
        logs=[]; processes=[]
        def launch(name,args):
            log=(out/(name+'.log')).open('w'); logs.append(log)
            p=subprocess.Popen([sys.executable,*args],cwd=ROOT,stdout=log,stderr=subprocess.STDOUT)
            processes.append(p); return p
        reads=0; observed=set(); began=time.monotonic()
        try:
            broker=launch('broker',['tools/dev_broker.py','--port',str(port)])
            for _ in range(100):
                try:
                    with socket.create_connection(('127.0.0.1',port),timeout=.2): break
                except OSError: time.sleep(.1)
            sub=launch('subscriber',['subscriber.py','listen','--port',str(port),'--db',str(db),
                 '--client-id','member3-verification','--snapshot',str(snap),
                 '--snapshot-interval','0.25','--summary',str(out/'subscriber_summary.json')])
            deadline=time.monotonic()+30
            while 'READY' not in (out/'subscriber.log').read_text():
                if sub.poll() is not None or time.monotonic()>deadline: raise RuntimeError('subscriber not ready')
                time.sleep(.1)
            pub=launch('publisher',['publisher.py','data/window.jsonl','--port',str(port),
                 '--topic','comp5339/TUT19-AssignmentGroup-08/events'])
            while pub.poll() is None:
                if sub.poll() is not None: raise RuntimeError('subscriber exited')
                if time.monotonic()-began>600: raise TimeoutError('replay')
                if snap.exists():
                    data=json.loads(snap.read_text())
                    reads+=1; observed.add(data['coverage']['received_events'])
                    assert len(data['stations'])==433
                time.sleep(.08)
            assert pub.returncode==0
            deadline=time.monotonic()+30
            while time.monotonic()<deadline:
                data=json.loads(snap.read_text())
                if data['coverage']['received_events']==2452: break
                time.sleep(.1)
            assert data['coverage']['received_events']==2452
            sub.send_signal(signal.SIGINT); assert sub.wait(timeout=30)==0
            result=verify(db,ROOT/'data/window.jsonl')
            assert result['passed'],result
            with duckdb.connect(str(db),read_only=True) as conn:
                state=dict(conn.execute('SELECT status,count(*) FROM core.charging_session GROUP BY status').fetchall())
                assert state=={'active':9,'completed':62},state
                assert conn.execute('SELECT count(*) FROM core.meter_reading').fetchone()[0]==1886
                # Preserve all original tables and inbox payloads compared with the package.
                source=str(ROOT/'database/assignment2.duckdb').replace("'","''")
                conn.execute(f"ATTACH '{source}' AS baseline (READ_ONLY)")
                tables=conn.execute("SELECT table_schema,table_name FROM information_schema.tables WHERE table_catalog='baseline' AND table_type='BASE TABLE' AND table_name NOT IN ('mqtt_event','mqtt_rejection','meter_reading','current_price','charging_session')").fetchall()
                preservation={}
                for schema,table in tables:
                    name=f'"{schema}"."{table}"'
                    n=conn.execute(f'SELECT count(*) FROM ((SELECT * FROM baseline.{name} EXCEPT ALL SELECT * FROM {name}) UNION ALL (SELECT * FROM {name} EXCEPT ALL SELECT * FROM baseline.{name}))').fetchone()[0]
                    preservation[f'{schema}.{table}']=n
                assert all(n==0 for n in preservation.values())
            result.update(session_states=state,snapshot_reads=reads,distinct_live_revisions=len(observed),
                          original_table_differences=preservation,elapsed_seconds=round(time.monotonic()-began,2))
            assert len(observed)>10
            (out/'mqtt_integration.json').write_text(json.dumps(result,indent=2)+'\n')
            print(json.dumps(result,indent=2))
        finally:
            for p in reversed(processes):
                if p.poll() is None:
                    p.send_signal(signal.SIGINT)
                    try: p.wait(timeout=10)
                    except subprocess.TimeoutExpired: p.kill(); p.wait()
            for log in logs: log.close()


if __name__=='__main__': main()
