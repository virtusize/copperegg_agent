#!/usr/bin/env python
"""
CopperEgg Agent.

Usage:
  nginx_agent -h | --help
  nginx_agent [--dry] [--verbose] --key <key> --host <host> --prefix <prefix>

Options:
  -h, --help             Show this help.

  -k, --key <key>        The API key

  -H, --host <host>      The hostname of the server or any other uniq id.
  -p, --prefix <prefix>  Custom metrics group prefix.
  --dry                  Dry run, do not change anything.
  -v, --verbose          Verbose mode.


Examples:

  nginx_agent -k fgji3tydvehehf -n 4 -H webapp-01 -p production

"""

from collections import defaultdict

import gevent.monkey
gevent.monkey.patch_all

import gevent
from gevent.queue import Queue

import signal
import time
import json
import sys
import requests

POST_URL = 'http://api.copperegg.com/v2/revealmetrics/samples/%s.json'

DRY_RUN = False
VERBOSE = False
HTTP_VERBS = ['HEAD', 'PUT', 'GET', 'POST']


def verbose(msg):
    if VERBOSE:
        print(msg)


def get_metrics_nginx_backend(queue):
    skip_list = ['/health-check/status']
    count = 0
    for line in sys.stdin:
        try:
            backend, request, status, response = line.rstrip().split('|', 3)
            if status != '200':
                verbose("Skipped %s %s" % (status, request))
                continue
            host, port = backend.split(':', 1)
            verb, url, http = request.split(' ', 2)
        except (TypeError, ValueError):
            verbose("Parser error %s" % line)
            continue
        if url in skip_list:
            verbose("Skipped: %s" % url)
            continue
        if verb not in HTTP_VERBS:
            verbose("Unknown http verb %s" % request)
            continue
        metrics = defaultdict(dict)
        metrics['identifier'] = "backend_%s_%s" % (port, verb.lower())
        metrics['values']['response_time'] = response
        if VERBOSE:
            count += 1
            if count >= 1000:
                print("%s queue size: %s" % (verb, queue[verb].qsize()))
                count = 0
        try:
            count += 1
            queue[verb].put_nowait(metrics)
        except gevent.queue.Full as e:
            print("%s queue congestion. Size: %s. Discarding data." %
                  (verb, queue[verb].qsize()))
        finally:
            gevent.sleep(0.1)

    for verb in HTTP_VERBS:
        queue[verb].put_nowait(None)  # put a poison pill
        verbose("Put poison pill for consumer")


def median(list):
#   verbose("list size %s" % len(list))
    sorts = sorted(list)
    length = len(sorts)
    if not length % 2:
        return (sorts[length / 2] + sorts[length / 2 - 1]) / 2.0
    return sorts[length / 2]


def mean(list):
    if len(list) > 0:
        return float(sum(list)/len(list))
    else:
        return None


def post_http(api_key, url, metrics):
    headers = {'content-type': 'application/json'}

    if DRY_RUN:
        print("OK: %s" % (json.dumps(metrics)))
    else:
        verbose("URL: %s" % (url))
        try:
            p = requests.post(url,
                              auth=(api_key, 'U'),
                              data=json.dumps(metrics),
                              headers=headers)
            p.raise_for_status()
            verbose("OK: %s" % (json.dumps(metrics)))
        except requests.exceptions.RequestException as e:
            print("ERR: %s DATA: %s" % (e, json.dumps(metrics)))


def post_metrics(queue, s_time, api_key, url):
    while True:
        data = {}
        buffer = []
        data = queue.get()
        if data is not None:  # it not a poison pill
            metrics = defaultdict(dict)
            metrics['identifier'] = data['identifier']
            buffer.append(float(data['values']['response_time']))
            while not queue.empty():
                data = queue.get()
                if data is not None:
                    buffer.append(float(data['values']['response_time']))
            metrics['timestamp'] = int(time.time())
            metrics['values']['response_time_median'] = median(buffer)
            metrics['values']['response_time_mean'] = mean(buffer)
            metrics['values']['response_time_min'] = min(buffer)
            metrics['values']['response_time_max'] = max(buffer)
            post_http(api_key, url, metrics)
        elif buffer:  # got a poison pill, push out collected data
            metrics = defaultdict(dict)
            metrics['identifier'] = data['identifier']
            metrics['timestamp'] = int(time.time())
            metrics['values']['response_time_median'] = median(buffer)
            metrics['values']['response_time_mean'] = mean(buffer)
            metrics['values']['response_time_min'] = min(buffer)
            metrics['values']['response_time_max'] = max(buffer)
            post_http(api_key, url, metrics)

        if data is None:  # got a poison pill, no data, exit
            verbose("Got poison pill. Exit.")
            return

        gevent.sleep(s_time)


def main():
    global VERBOSE
    global DRY_RUN

    from docopt import docopt

    arguments = docopt(__doc__, version='CopperEgg Agent')

    api_key = arguments.get('--key', None)
    if not api_key:
        print('Invalid API key')
        return 1

    host = arguments.get('--host', None)
    if not host:
        print('Invalid hostname')
        return 1

    prefix = arguments.get('--prefix', None)
    if not prefix:
        print('Invalid group prefix')
        return 1

    if arguments.get('--dry'):
        print('DRY mode, no changes made.')
        DRY_RUN = True

    if arguments.get('--verbose'):
        print('VERBOSE mode.')
        VERBOSE = True

    jobs = []

    queue = {}
    queue['GET'] = Queue(maxsize=10000)
    queue['POST'] = Queue(maxsize=10000)
    queue['HEAD'] = Queue()
    queue['PUT'] = Queue()

    metrics_group = "%s_%s_backend" % (prefix, host)
    jobs.append(gevent.spawn(get_metrics_nginx_backend, queue))

    for verb in HTTP_VERBS:
        jobs.append(gevent.spawn(post_metrics,
                                 queue[verb],
                                 5,  # report every 5 seconds
                                 api_key,
                                 POST_URL % (metrics_group)))

    try:
        gevent.joinall(jobs)
    except KeyboardInterrupt:
        gevent.killall(jobs)

if __name__ == '__main__':
    gevent.signal(signal.SIGQUIT, gevent.kill)
    gevent.signal(signal.SIGQUIT, gevent.kill)
    main()
