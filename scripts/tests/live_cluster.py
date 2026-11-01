#!/usr/bin/env python3
"""Exercise a fresh real cluster, clean up child processes and preserve user data.

Run from the repository: python3 scripts/tests/live_cluster.py --nodes 5
Requires the debug binary (cargo build -p raftkv-server). Uses standard local
ports, so stop other local clusters first. Temporary data is removed on exit.
"""
import argparse
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[2]


def request(node, path='/api/status', method='GET', data=None):
    payload = None if data is None else json.dumps(data).encode()
    req = urllib.request.Request(f'http://127.0.0.1:{8079+node}{path}',
                                 data=payload, method=method,
                                 headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=7) as response:
        return json.load(response)


def resp(node, *args):
    values = [str(arg).encode() for arg in args]
    command = b'*' + str(len(values)).encode() + b'\r\n'
    for value in values:
        command += b'$' + str(len(value)).encode() + b'\r\n' + value + b'\r\n'
    with socket.create_connection(('127.0.0.1', 6378+node), timeout=8) as sock:
        sock.sendall(command)
        stream = sock.makefile('rb')
        line = stream.readline()
        if line.startswith(b'$') and int(line[1:-2]) >= 0:
            return stream.read(int(line[1:-2]))
        return line.strip()


def until(predicate, timeout=15):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            result = predicate()
            if result:
                return result
        except (OSError, ValueError, KeyError) as error:
            last_error = error
        time.sleep(.1)
    raise AssertionError(f'Condition not met after {timeout}s; last error: {last_error}')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--nodes', type=int, default=5, choices=range(3, 10))
    options = parser.parse_args()
    count = options.nodes
    processes, logs = {}, []
    # Never let this test replace a listener already owned by the user.
    for node in range(1, count+1):
        for base in (7000, 6378, 9100, 8079):
            with socket.socket() as sock:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.bind(('127.0.0.1', base+node))
    binary = ROOT / 'target/debug/raftkv-server'
    assert binary.is_file(), 'Build the debug server first.'
    with tempfile.TemporaryDirectory(prefix='raftkv-live-') as directory:
        subprocess.run([str(ROOT / 'scripts/run_cluster.sh'), '--nodes', str(count),
                        '--run-dir', directory, '--generate-only'], check=True)

        def start(node):
            log = open(Path(directory) / f'node{node}.log', 'ab')
            logs.append(log)
            processes[node] = subprocess.Popen(
                [str(binary), '--config', str(Path(directory) / f'node{node}.toml')],
                stdout=log, stderr=log, env={**os.environ, 'RUST_LOG': 'warn'},
            )

        def stop(node):
            process = processes[node]
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)

        def active_ids():
            return [node for node, process in processes.items() if process.poll() is None]

        def find_leader():
            statuses = [request(node) for node in active_ids()]
            leaders = [s for s in statuses if s['role'] == 'leader']
            if len(leaders) == 1 and all(s['leader_id'] == leaders[0]['node_id'] for s in statuses):
                return leaders[0]['node_id']
            return None

        try:
            for node in range(1, count+1):
                start(node)
            leader = until(find_leader)
            print(f'PASS {count}-node election, leader={leader}', flush=True)
            status = request(leader)
            assert status['health']['storage_healthy']
            assert status['config']['transport_concurrency_limit'] == 32
            with urllib.request.urlopen(f'http://127.0.0.1:{9100+leader}/metrics', timeout=3) as response:
                metrics = response.read().decode()
            assert f'raftkv_is_leader{{node_id="{leader}"}} 1' in metrics
            assert '# TYPE raftkv_proposals_committed_total counter' in metrics
            key = 'validation / λ?+#'
            path = '/api/keys/' + urllib.parse.quote(key, safe='')
            value = 'replicated value ' + 'x' * 12000
            assert request(leader, path, 'PUT', {'value': value})['ok']
            until(lambda: all(request(node, path)['value'] == value for node in active_ids()))
            assert all(request(node, path)['consistency'] == 'local' for node in active_ids())
            assert resp(leader, 'GET', key) == value.encode()
            log = request(leader, '/api/log')
            assert log['consistency'] == 'local'
            assert 0 < len(log['entries']) <= 100
            assert any(entry['kind'] == 'normal' and entry['payload_bytes'] > 12000 for entry in log['entries'])
            assert value not in json.dumps(log)
            print('PASS fragmented-size JSON, escaped UTF-8 key, replication, quorum-gated RESP read', flush=True)
            print('PASS live Prometheus metrics, health diagnostics, and retained-log metadata', flush=True)
            assert resp(leader, 'INCR', 'validation-counter') == b':1'
            until(lambda: all(request(node, '/api/keys/validation-counter')['value'] == '1' for node in active_ids()))
            # Observe bounded idle traffic; a continuous ack loop used to flood
            # the runtime even without client requests.
            before = request(leader)['counters']['raft_messages_sent']
            time.sleep(.5)
            after = request(leader)['counters']['raft_messages_sent']
            assert after - before < count * 40, f'Unexpected idle RPC loop: {after-before}'
            print(f'PASS bounded idle replication ({after-before} sends in 0.5s)', flush=True)
            if count % 2 == 0:
                # Leave exactly half the voters, including the current leader.
                stopped = [node for node in active_ids() if node != leader][:count//2]
                for node in stopped:
                    stop(node)
                commit_before = request(leader)['commit_index']
                result = resp(leader, 'SET', 'minority-candidate', 'pending')
                assert result.startswith(b'-'), f'Half the voters acknowledged a write: {result!r}'
                assert request(leader)['commit_index'] == commit_before
                print('PASS exactly half of an even voter set cannot commit a write', flush=True)
                for node in stopped:
                    start(node)
                leader = until(find_leader)
            stop(leader)
            replacement = until(find_leader)
            assert replacement != leader
            assert resp(replacement, 'GET', 'validation-counter') == b'1'
            print(f'PASS leader failover to node {replacement}', flush=True)
            for node in list(active_ids()):
                if node != replacement:
                    stop(node)
            isolated = resp(replacement, 'GET', 'validation-counter')
            assert isolated.startswith(b'-'), f'Isolated leader served an unconfirmed read: {isolated!r}'
            print('PASS isolated leader refuses read without a fresh quorum', flush=True)
            for node in range(1, count+1):
                if processes[node].poll() is not None:
                    start(node)
            restored_leader = until(find_leader)
            until(lambda: all(request(node, '/api/keys/validation-counter')['value'] == '1' for node in active_ids()))
            assert resp(restored_leader, 'GET', 'validation-counter') == b'1'
            print('PASS restart preserves INCR exactly once on clean committed replay', flush=True)
            assert request(restored_leader, path, 'DELETE')['removed']
            until(lambda: all(request(node, path)['value'] is None for node in active_ids()))
            statuses = [request(node) for node in active_ids()]
            until(lambda: len({request(node)['state_hash'] for node in active_ids()}) == 1)
            assert all(s['queues']['pending_reads'] == 0 for s in statuses)
            assert all(s['counters']['storage_errors'] == 0 for s in statuses)
            print('PASS replicated delete, matching history hashes, no pending reads/storage errors', flush=True)
        except BaseException:
            for node in processes:
                path = Path(directory) / f'node{node}.log'
                print(f'node{node} log tail: {path.read_text(errors="replace")[-2000:]}', flush=True)
            raise
        finally:
            for node in processes:
                stop(node)
            for log in logs:
                log.close()


if __name__ == '__main__':
    main()
