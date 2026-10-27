"""Record comparable warm query timings and actual DuckDB plans; no index promises."""
import json
import statistics
import sys
import time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import duckdb

queries={
 'raw_session_projection':'SELECT * FROM analytics.session_projection ORDER BY ev_id,session_id',
 'materialized_sessions':'SELECT * FROM core.charging_session ORDER BY ev_id,session_id',
 'station_metrics':'SELECT * FROM analytics.station_metrics ORDER BY ev_id',
 'typed_curve':"SELECT event_ts,power_kw,energy_kwh FROM core.meter_reading WHERE ev_id=100 AND session_id=(SELECT session_id FROM core.charging_session WHERE ev_id=100 AND status='active' LIMIT 1) ORDER BY event_ts,event_id"
}
result={}
with duckdb.connect(str(ROOT/'database/assignment2.duckdb'),read_only=True) as db:
    assert db.execute(queries['raw_session_projection']).fetchall()==db.execute(queries['materialized_sessions']).fetchall()
    for name,sql in queries.items():
        db.execute(sql).fetchall()
        timings=[]
        for _ in range(30):
            start=time.perf_counter(); values=db.execute(sql).fetchall(); timings.append(1000*(time.perf_counter()-start))
        result[name]={'rows':len(values),'median_ms':statistics.median(timings),'samples':30,
                      'explain_analyze':db.execute('EXPLAIN ANALYZE '+sql).fetchall()}
path=ROOT/'verification/task3/query_benchmark.json'
path.write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({k:{a:b for a,b in v.items() if a!='explain_analyze'} for k,v in result.items()},indent=2))
