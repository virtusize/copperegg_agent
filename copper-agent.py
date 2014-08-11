#!/usr/bin/env python

"""
CopperEgg Agent.

Usage:
  copperegg-agent -h | --help
  copperegg-agent [--dry] --key <key> --num <num> --host <host>

Options:
  -h, --help            Show this help.
  -k, --key <key>       The API key
  -n, --num <num>       The number of backend processes
  -H, --host <host>     The hostname of the server or any other uniq id.
  --dry                 Dry run, do not change anything.


Examples:

  copperegg-agent -k fgji3tydvehehf -n 4 -H demo

"""

import requests
import time
import json

METRICS_GROUP = "web-server"

POLL_URL = 'http://%s:%s/metrics/web-server-health'
POST_URL = 'http://api.copperegg.com/v2/revealmetrics/samples/%s.json'
PORT_BASE = 11001


def get_metrics(url):
    r = requests.get(url)
    r.raise_for_status()
    # lower case all keys
    return dict((k.lower(), v) for k, v in r.json().iteritems())


def post_metrics(url, api_key, id, metrics, dry_run=False):
    payload = {
        "identifier": id,
        "timestamp": int(time.time()),
        "values": metrics
    }
    headers = {'content-type': 'application/json'}

    if dry_run:
        print json.dumps(payload)
    else:
        p = requests.post(url,
                          auth=(api_key, 'U'),
                          data=json.dumps(payload),
                          headers=headers)
        p.raise_for_status()


def main():
    dry_run = False

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

    proc_count = int(arguments.get('--num', None))
    if not proc_count:
        print 'Invalid number of backends'
        return 1

    if arguments.get('--dry'):
        print 'DRY mode, no changes made.'
        dry_run = True

    for i in range(proc_count):
        port = PORT_BASE + i
        id = "%s_%s" % (host, port)

        try:
            metrics = get_metrics(POLL_URL % ('127.0.0.1', port))
            if dry_run:
                print json.dumps(metrics)

            post_metrics(POST_URL % (METRICS_GROUP), api_key, id,
                         metrics, dry_run)

        except requests.exceptions.RequestException as e:
            print e
            continue

if __name__ == '__main__':
    main()
