#!/usr/bin/env python

"""
CopperEgg Agent.

Usage:
  copperegg-agent -h | --help
  copperegg-agent [--dry] [--verbose] --key <key> --num <num> --host <host> --prefix <pref>

Options:
  -h, --help             Show this help.
  -k, --key <key>        The API key
  -n, --num <num>        The number of backend processes
  -H, --host <host>      The hostname of the server or any other uniq id.
  -p, --prefix <prefix>  Custom metrics group prefix.
  --dry                  Dry run, do not change anything.
  -v, --verbose          Verbose mode.


Examples:

  copperegg-agent -k fgji3tydvehehf -n 4 -H webapp-01 -p production

"""
from collections import defaultdict

import gevent.monkey
gevent.monkey.patch_all

import gevent
from gevent.queue import Queue
from gevent import subprocess

import signal
import time
import json
import requests

POLL_URL = 'http://%s:%s/metrics/web-server-health'
POST_URL = 'http://api.copperegg.com/v2/revealmetrics/samples/%s.json'
PORT_BASE = 11001

NGINX_URL = 'http://127.0.0.1/nginx_status'

DRY_RUN = False
VERBOSE = False

def get_metrics_nginx(queue, s_time, id, url):
    while True:
        try:
            r = requests.get(url)
            r.raise_for_status()
        except requests.exceptions.RequestException as e:
            print "Nginx metrics", e
        else:
            data = {}
            for line in r.iter_lines():
                if line.startswith('Active'):
                    data['active'] = line.split(' ', 2)[2]
                elif line.startswith('Reading:'):
                    data['reading'], data['writing'], data['waiting'] = line.split(' ', 5)[1:6:2]
                elif line.startswith(' '):
                    data['accepted'], data['handled'], data['requests'] = line.lstrip().split(' ', 2)
            metrics = {}
            metrics['identifier'] = id
            metrics['timestamp'] = int(time.time())
            metrics['values'] = data
            queue.put_nowait(metrics)
        finally:
            gevent.sleep(s_time)


def get_metrics_connections(queue, s_time, id):
    while True:
        try:
            cmd = subprocess.Popen(
                ['/bin/ss', '-n', '-a', '-4'], stdout=subprocess.PIPE)
        except subprocess.CalledProcessError as e:
            print "Connections ", e.returncode
        else:
            data = defaultdict(int)
            for line in cmd.stdout:
                if line.startswith('ESTAB'):
                    data['established'] += 1
                elif line.startswith('TIME-WAIT'):
                    data['time_wait'] += 1
                elif line.startswith('CLOSE-WAIT'):
                    data['close_wait'] += 1
                elif line.startswith('LISTEN'):
                    data['listen'] += 1

            metrics = {}
            metrics['identifier'] = id
            metrics['timestamp'] = int(time.time())
            metrics['values'] = data
            queue.put_nowait(metrics)
        finally:
            gevent.sleep(s_time)


def get_metrics_cherrypy(queue, s_time, id, url):
    while True:
        try:
            r = requests.get(url)
            r.raise_for_status()
        except requests.exceptions.RequestException as e:
            print "CherryPy ", e
        else:
            metrics = {}
            metrics['identifier'] = id
            metrics['timestamp'] = int(time.time())
            # lower case all keys
            metrics['values'] = dict((k.lower(), v)
                                     for k, v in r.json().iteritems())
            queue.put_nowait(metrics)
        finally:
            gevent.sleep(s_time)


def post_metrics(queue, api_key, url):
    headers = {'content-type': 'application/json'}
    while True:
        metrics = {}
        metrics = queue.get()
        if DRY_RUN:
             print "OK: %s" % (json.dumps(metrics))
        else:
            try:
                if VERBOSE:
                    print "URL: %s" % (url)
                p = requests.post(url,
                                  auth=(api_key, 'U'),
                                  data=json.dumps(metrics),
                                  headers=headers)
                p.raise_for_status()
                if VERBOSE:
                    print "OK: %s" % (json.dumps(metrics))
            except requests.exceptions.RequestException as e:
                print "ERR: %s DATA: %s" % (e, json.dumps(metrics))


def main():
    global VERBOSE
    global DRY_RUN

    from docopt import docopt

    arguments = docopt(__doc__, version='CopperEgg Agent')

    api_key = arguments.get('--key', None)
    if not api_key:
        print 'Invalid API key'
        return 1

    host = arguments.get('--host', None)
    if not host:
        print 'Invalid hostname'
        return 1

    prefix = arguments.get('--prefix', None)
    if not prefix:
        print 'Invalid group prefix'
        return 1

    proc_count = int(arguments.get('--num', None))
    if not proc_count:
        print 'Invalid number of backends'
        return 1

    if arguments.get('--dry'):
        print 'DRY mode, no changes made.'
        DRY_RUN = True

    if arguments.get('--verbose'):
        print 'VERBOSE mode.'
        VERBOSE = True

    jobs = []

    q_cherrypy = Queue()
    metrics_group = "%s_%s_cherrypy" % (prefix, host)
    jobs.append(gevent.spawn(post_metrics,
                             q_cherrypy,
                             api_key,
                             POST_URL % (metrics_group)))
    for i in range(proc_count):
        port = PORT_BASE + i
        jobs.append(gevent.spawn(get_metrics_cherrypy,
                                 q_cherrypy,
                                 15,
                                 '%s_%s'  % ('cherrypy',  port),
                                 POLL_URL % ('127.0.0.1', port)))

    q_nginx = Queue()
    metrics_group = "%s_%s_nginx" % (prefix, host)
    jobs.append(gevent.spawn(post_metrics,
                             q_nginx,
                             api_key,
                             POST_URL % (metrics_group)))
    jobs.append(gevent.spawn(get_metrics_nginx,
                             q_nginx,
                             15,
                             'nginx',
                             NGINX_URL))

    q_network = Queue()
    metrics_group = "%s_%s_network" % (prefix, host)
    jobs.append(gevent.spawn(post_metrics,
                             q_network,
                             api_key,
                             POST_URL % (metrics_group)))
    jobs.append(gevent.spawn(get_metrics_connections,
                             q_network,
                             15,
                             'conns'))
    try:
        gevent.joinall(jobs)
    except KeyboardInterrupt:
        gevent.killall(jobs)

if __name__ == '__main__':
    gevent.signal(signal.SIGQUIT, gevent.kill)
    gevent.signal(signal.SIGQUIT, gevent.kill)
    main()
