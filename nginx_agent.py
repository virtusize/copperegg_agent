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

from collections import defaultdict, deque
from threading import Thread, Event
import threading
import logging
import requests
import time
import json
import sys
import signal

POST_URL = 'http://api.copperegg.com/v2/revealmetrics/samples/{}.json'

DRY_RUN = False
VERBOSE = False
HTTP_VERBS = ['HEAD', 'PUT', 'GET', 'POST']

RUN_EVENT = None


def verbose(msg):
        logging.debug(msg)


def get_metrics_nginx_backend(queue):
    global RUN_EVENT
    skip_list = ['/health-check/status']
    count = 0
    for line in sys.stdin:
        if not RUN_EVENT.is_set():
            return
        try:
            backend, request, status, response = line.rstrip().split('|', 3)
            if status != '200':
                verbose("Skipped {} {}".format(status, request))
                continue
            verb, url, http = request.split(' ', 2)
        except (TypeError, ValueError):
            verbose("Parser error {}".format(line))
            continue
        if url in skip_list:
            verbose("Skipped: {}".format(url))
            continue
        if verb not in HTTP_VERBS:
            verbose("Unknown http verb {}".format(request))
            continue
        #  127.0.0.1:11004, 127.0.0.1:11003|GET /api/vs-widget/v2/ HTTP/1.1|200|1.492, 0.020
        for back, rtime in zip(backend.split(', '), response.split(' ,')):
                host, port = back.split(':', 1)
                metrics = defaultdict(lambda: defaultdict(int))
                metrics['identifier'] = "backend_{}_{}".format(port,
                                                               verb.lower())
                metrics['values']['response_time'] = rtime

                count += 1
                if count >= 100:
                        verbose("Producer {} queue size: {}".format(verb,
                                len(queue[verb])))
                        count = 0

                queue[verb].append(metrics)
                count += 1

                if len(queue[verb]) >= queue[verb].maxlen:
                        verbose("{} queue full {}".format(verb,
                                                          len(queue[verb])))
    else:  # got EOF
        RUN_EVENT.clear()


def median(list):
    verbose("List size {}".format(len(list)))
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
        print("OK: {}".format(json.dumps(metrics)))
    else:
        verbose("URL: {}".format(url))
        try:
            p = requests.post(url,
                              auth=(api_key, 'U'),
                              data=json.dumps(metrics),
                              headers=headers)
            p.raise_for_status()
            verbose("OK: {}".format(json.dumps(metrics)))
        except requests.exceptions.RequestException as e:
            print("ERR: {} DATA: {}".format(e, json.dumps(metrics)))


def post_metrics(queue, s_time, api_key, url):
    while RUN_EVENT.is_set():
        data = {}
        buffer = []
        try:
            data = queue.popleft()
            buffer.append(float(data['values']['response_time']))
        except (IndexError, ValueError):  # queue is empty, value extraction
            pass
        else:
            while True:
                try:
                    data = queue.popleft()
                    buffer.append(float(data['values']['response_time']))
                except IndexError:
                    break
                except ValueError:
                    pass

            verbose("Consumer queue size {}".format(len(queue)))
            metrics = defaultdict(lambda: defaultdict(int))
            metrics['identifier'] = data['identifier']
            metrics['timestamp'] = int(time.time())
            metrics['values']['response_time_median'] = median(buffer)
            metrics['values']['response_time_mean'] = mean(buffer)
            metrics['values']['response_time_min'] = min(buffer)
            metrics['values']['response_time_max'] = max(buffer)
            post_http(api_key, url, metrics)

        verbose("Sleep")
        time.sleep(s_time)


def signal_handler(signal, frame):
        global RUN_EVENT
        verbose("Recieving signal. Exit")
        RUN_EVENT.clear()


def main():
    global VERBOSE
    global DRY_RUN
    global RUN_EVENT

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
        logging.basicConfig(
            level=logging.DEBUG,
            format='[%(levelname)s] (%(threadName)-10s) %(message)s',)

    jobs = []
    RUN_EVENT = Event()
    RUN_EVENT.set()

    queue = {}
    queue['GET'] = deque(maxlen=10000)
    queue['POST'] = deque(maxlen=10000)
    queue['HEAD'] = deque()
    queue['PUT'] = deque()

    t = Thread(name="get_metrics",
               target=get_metrics_nginx_backend,
               args=(queue,))
    t.start()
    jobs.append(t)

    metrics_group = "{}_{}_backend".format(prefix, host)
    for verb in HTTP_VERBS:
        t = Thread(name="post_{}".format(verb),
                   target=post_metrics,
                   args=(queue[verb],
                         5,
                         api_key,
                         POST_URL.format(metrics_group),))
        t.start()
        jobs.append(t)

    main_thread = threading.currentThread()

    try:
        while RUN_EVENT.is_set():
                for t in threading.enumerate():
                        if not t.isAlive():
                                state = 'terminated'
                        else:
                                state = 'alive'
                        verbose("Thread {} is {} ".format(t.getName(), state))

                time.sleep(5)
    except (KeyboardInterrupt, SystemExit):
        print "Attempting to close threads"
        RUN_EVENT.clear()

    for t in threading.enumerate():
        if t is main_thread:
                continue
        verbose('joining {}'.format(t.getName()))
        t.join()


if __name__ == '__main__':
        signal.signal(signal.SIGQUIT, signal_handler)
        signal.signal(signal.SIGHUP, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
        main()
