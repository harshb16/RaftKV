#!/usr/bin/env python3
"""Local process demo. Uses only Python's standard library."""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
RUN = ROOT / 'run'

def configs():
    return [json.loads(p.read_text()) for p in sorted((ROOT / 'configs').glob('node*.json'))]

def status(c):
    try:
        with urllib.request.urlopen('http://' + c['raft_addr'] + '/status', timeout=.4) as r:
            return json.load(r)
    except (OSError, ValueError):
        return None

def leader(timeout=8, excluded=None):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        found = [(c, status(c)) for c in configs() if c['node_id'] != excluded]
        found = [(c, s) for c, s in found if s and s['role'] == 'leader']
        if found:
            return max(found, key=lambda x: x[1]['term'])[0]
        time.sleep(.05)
    raise RuntimeError('no leader before deadline')

def start_node(c):
    RUN.mkdir(exist_ok=True)
    pidfile = RUN / (c['node_id'] + '.pid')
    if status(c):
        return
    with open(RUN / (c['node_id'] + '.log'), 'ab') as log:
        p = subprocess.Popen([str(ROOT / 'bin/raftkv'), '-config', 'configs/' + c['node_id'] + '.json'], stdout=log, stderr=log, start_new_session=True)
    pidfile.write_text(str(p.pid))
    time.sleep(.1)
    if p.poll() is not None:
        raise RuntimeError('node failed; see ' + str(RUN / (c['node_id'] + '.log')))

def kill_node(c, abrupt=False):
    pidfile = RUN / (c['node_id'] + '.pid')
    if not pidfile.exists():
        return
    pid = int(pidfile.read_text())
    # Guard against a stale PID file referring to an unrelated process.
    command = subprocess.run(['ps', '-p', str(pid), '-o', 'command='], capture_output=True, text=True).stdout
    if str(ROOT / 'bin/raftkv') in command and 'configs/' + c['node_id'] + '.json' in command:
        os.kill(pid, signal.SIGKILL if abrupt else signal.SIGTERM)
    pidfile.unlink(missing_ok=True)
    end = time.monotonic() + 5
    while status(c) and time.monotonic() < end:
        time.sleep(.05)

def redis(c, *args):
    host, port = c['client_addr'].rsplit(':', 1)
    out = subprocess.check_output(['redis-cli', '--raw', '-h', host, '-p', port, *args], text=True).strip()
    if out.startswith(('MOVED', 'CLUSTERDOWN', 'TRYAGAIN', 'ERR')):
        raise RuntimeError(out)
    print('redis-cli', *args, '=>', out)
    return out

def converge(timeout=8):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        states = [status(c) for c in configs()]
        if all(states) and len({(s['commit'], s['applied'], s['last_index'], s['state_hash']) for s in states}) == 1 and all(s['commit'] == s['applied'] == s['last_index'] for s in states):
            return states
        time.sleep(.05)
    raise RuntimeError('nodes did not converge')

def demo():
    for c in configs():
        start_node(c)
    old = leader()
    print('leader:', old['node_id'], old['client_addr'])
    redis(old, 'SET', 'hello', 'world')
    redis(old, 'MSET', 'a', '1', 'b', '2')
    redis(old, 'INCR', 'a')
    assert redis(old, 'GET', 'hello') == 'world'
    start = time.monotonic()
    print('SIGKILL', old['node_id'])
    kill_node(old, abrupt=True)
    new = leader(excluded=old['node_id'])
    redis(new, 'SET', 'after-failover', 'yes')
    print('failover to', new['node_id'], 'and successful write: %.3fs' % (time.monotonic() - start))
    assert redis(new, 'GET', 'hello') == 'world'
    print('restart', old['node_id'])
    start_node(old)
    states = converge()
    print('converged at index', states[0]['commit'], 'state SHA256', states[0]['state_hash'])
    subprocess.run([str(ROOT / 'bin/raftkv'), 'status'], check=True)
    print('Cluster remains running. Stop with: make stop')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['start', 'stop', 'leader', 'demo', 'converge'])
    args = parser.parse_args()
    if args.action == 'start':
        for c in configs():
            start_node(c)
        c = leader()
        print('leader:', c['node_id'], c['client_addr'])
    elif args.action == 'stop':
        for c in configs():
            kill_node(c)
    elif args.action == 'leader':
        print(leader()['client_addr'])
    elif args.action == 'converge':
        print(json.dumps(converge(), indent=2))
    elif args.action == 'demo':
        demo()
