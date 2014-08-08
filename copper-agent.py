#!/usr/bin/python 

import requests
import time
import json

HOST = "{{ inventory_hostname_short }}"
API_KEY = "{{ copperegg_api_key }}"
PROC_COUNT = {{ backend_proc_count }} 

METRICS_GROUP = "web-server"

POLL_URL='http://%s:%s/metrics/web-server-health'
POST_URL = "http://api.copperegg.com/v2/revealmetrics/samples/%s.json" % (METRICS_GROUP)

PORT_BASE = 11001

def get_metrics(url):
  r = requests.get(url)
  r.raise_for_status()
  # lower case all keys
  return dict((k.lower(), v) for k, v in r.json().iteritems())

def post_metrics(url, id, metrics):
  payload = { "identifier": id, "timestamp": int(time.time()), "values": metrics }
  headers = { 'content-type': 'application/json' }

  p = requests.post(url, auth=(API_KEY, 'U'), 
                    data=json.dumps(payload), 
                    headers=headers)
  p.raise_for_status()

for i in range(PROC_COUNT):
  port = PORT_BASE + i
  id = "%s_%s" % (HOST, port)

  try:
    metrics = get_metrics(POLL_URL % ('127.0.0.1', port))
    post_metrics(POST_URL, id, metrics)
  except requests.exceptions.RequestException as e:
    print e
    continue  
