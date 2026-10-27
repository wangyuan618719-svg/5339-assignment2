"""Subscribe to member 1's stream; commit valid events or quarantine before ACK."""
import argparse
import json
import logging
import math
import queue
import signal
import threading
import time
from collections import Counter
from pathlib import Path

import paho.mqtt.client as mqtt

from ingestion.storage import Store, initialise
from analytics.queries import publish_snapshot

LOG = logging.getLogger('subscriber')


def run(args):
    pending = queue.Queue(maxsize=args.queue_size)
    stop, ready = threading.Event(), threading.Event()
    counters = Counter()
    failures = []
    state = {'connected': False, 'epoch': 0, 'subscribed': False, 'ever_ready': False}
    connection_lock = threading.Lock()
    # Persistent broker session + stable ID allows queued QoS 1 delivery on reconnect.
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=args.client_id,
                         clean_session=False, protocol=mqtt.MQTTv311, manual_ack=True)
    client.reconnect_delay_set(1, 30)
    client.max_inflight_messages_set(args.queue_size)

    def fail(exc):
        LOG.error('%s', exc)
        failures.append(str(exc))
        stop.set()

    def writer():
        try:
            with Store(args.db) as store:
                snapshot_path = getattr(args, 'snapshot', None)
                interval = getattr(args, 'snapshot_interval', 1.0)
                last_snapshot = time.monotonic()
                if snapshot_path:
                    publish_snapshot(store.db, snapshot_path)
                ready.set()
                while True:
                    try:
                        item = pending.get(timeout=interval)
                    except queue.Empty:
                        if snapshot_path:
                            publish_snapshot(store.db, snapshot_path)
                            last_snapshot = time.monotonic()
                        continue
                    try:
                        if item is None:
                            if snapshot_path:
                                publish_snapshot(store.db, snapshot_path)
                            return
                        epoch, mid, topic, payload, retained, qos = item
                        outcome = store.receive(topic, payload, retained, qos)
                        counters[outcome] += 1
                        if snapshot_path and time.monotonic() - last_snapshot >= interval:
                            publish_snapshot(store.db, snapshot_path)
                            last_snapshot = time.monotonic()
                        if outcome == 'rejected':
                            LOG.warning('message quarantined; packet=%s outcomes=%s', mid, dict(counters))
                        # Never ACK an old connection's packet ID on a new connection.
                        with connection_lock:
                            if state['connected'] and epoch == state['epoch']:
                                if client.ack(mid, qos) != mqtt.MQTT_ERR_SUCCESS:
                                    LOG.warning('ACK failed; broker may redeliver %s', mid)
                        if sum(counters.values()) % 250 == 0:
                            LOG.info('committed outcomes=%s queue=%d', dict(counters), pending.qsize())
                    finally:
                        pending.task_done()
        except BaseException as exc:
            fail(f'writer failed; uncommitted messages are not ACKed: {exc}')
            ready.set()

    def on_connect(client, userdata, flags, reason, properties):
        if reason.is_failure:
            fail(f'broker rejected connection: {reason}')
            return
        with connection_lock:
            state['connected'] = True
            state['epoch'] += 1
        state['subscribed'] = False
        rc, mid = client.subscribe(args.topic, qos=1)
        if rc != mqtt.MQTT_ERR_SUCCESS:
            fail(f'subscribe failed: {rc}')

    def on_subscribe(client, userdata, mid, reasons, properties):
        if not reasons or any(reason.is_failure or reason.value != 1 for reason in reasons):
            fail(f'QoS 1 subscription not granted: {reasons}')
            return
        state['subscribed'] = True
        state['ever_ready'] = True
        LOG.info('READY topic=%s qos=1 client_id=%s', args.topic, args.client_id)

    def on_disconnect(client, userdata, flags, reason, properties):
        with connection_lock:
            state['connected'] = False
        state['subscribed'] = False
        if not stop.is_set():
            LOG.warning('disconnected (%s); reconnect backoff 1..30 seconds', reason)

    def on_connect_fail(client, userdata):
        LOG.warning('broker unavailable; retrying with 1..30 second backoff')

    def on_message(client, userdata, message):
        try:
            pending.put_nowait((state['epoch'], message.mid, message.topic,
                               bytes(message.payload), message.retain, message.qos))
        except queue.Full:
            fail('bounded queue full: stop without ACK; restart same client ID or replay')

    client.on_connect, client.on_subscribe = on_connect, on_subscribe
    client.on_disconnect, client.on_message = on_disconnect, on_message
    client.on_connect_fail = on_connect_fail
    thread = threading.Thread(target=writer, name='duckdb-writer', daemon=True)
    thread.start()
    ready.wait()
    if failures:
        thread.join()
        return 1
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    started = time.monotonic()
    try:
        client.connect_async(args.host, args.port, keepalive=30)
        client.loop_start()
        while not stop.wait(0.2):
            if args.run_seconds and time.monotonic() - started >= args.run_seconds:
                break
    except (OSError, RuntimeError) as exc:
        fail(exc)
    finally:
        # Freeze reception, then finish queued commits. Any unACKed delivery is safe to replay.
        stop.set()
        client.disconnect()
        client.loop_stop()
        if thread.is_alive():
            try:
                pending.put(None, timeout=5)
            except queue.Full:
                failures.append('writer queue could not drain on shutdown')
            thread.join(timeout=30)
            if thread.is_alive():
                failures.append('writer did not stop within 30 seconds')
        if not state['ever_ready']:
            failures.append('subscriber never received a successful QoS 1 SUBACK')
        summary = dict(outcomes=dict(counters), failures=failures, db=str(args.db))
        if args.summary:
            args.summary.parent.mkdir(parents=True, exist_ok=True)
            args.summary.write_text(json.dumps(summary, indent=2) + '\n', encoding='utf-8')
        LOG.info('STOP %s', json.dumps(summary))
    return 1 if failures else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    init = sub.add_parser('init', help='copy the real A1 database and add inbox DDL')
    init.add_argument('--a1-db', type=Path, required=True)
    init.add_argument('--db', type=Path, default=Path('database/assignment2.duckdb'))
    listen = sub.add_parser('listen')
    listen.add_argument('--db', type=Path, default=Path('database/assignment2.duckdb'))
    listen.add_argument('--host', default='localhost')
    listen.add_argument('--port', type=int, default=1883)
    listen.add_argument('--topic', default='comp5339/TUT19-AssignmentGroup-08/events')
    listen.add_argument('--client-id', default='comp5339-group08-member2')
    listen.add_argument('--queue-size', type=int, default=512)
    listen.add_argument('--run-seconds', type=float, default=0, help='0 means until Ctrl+C')
    listen.add_argument('--summary', type=Path)
    listen.add_argument('--snapshot', type=Path, default=Path('runtime/dashboard.json'))
    listen.add_argument('--snapshot-interval', type=float, default=1.0)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    if args.command == 'init':
        initialise(args.a1_db, args.db)
        LOG.info('Initialised %s from unchanged A1 database', args.db)
        return 0
    if (not math.isfinite(args.snapshot_interval) or args.snapshot_interval <= 0 or args.queue_size < 1 or not math.isfinite(args.run_seconds) or args.run_seconds < 0 or not args.client_id
            or not 1 <= args.port <= 65535 or not args.topic
            or any(c in args.topic for c in '+#\0')):
        parser.error('use a positive queue/port, stable client ID and exact event topic')
    return run(args)


if __name__ == '__main__':
    raise SystemExit(main())
