#!/usr/bin/env python
import ast
import sys
import pprint
import itertools
from operator import itemgetter, attrgetter

events = [
            { 'timestamp' : 100,
                'content'   : 'rsyslog has shutdown' },
            { 'timestamp' : 200,
                'content'   : 'rsyslog has shutdown' },
            { 'timestamp' : 300,
                'content'   : 'rsyslog has shutdown' },
            { 'timestamp' : 101,
                'content'   : 'rsyslog has startup' },
            { 'timestamp' : 202,
                'content'   : 'rsyslog has startup' },
            { 'timestamp' : 301,
                'content'   : 'rsyslog has startup' },
            { 'timestamp' : 150,
                'content'   : 'running maintenance' },
            { 'timestamp' : 175,
                'content'   : 'running maintenance' },
            { 'timestamp' : 250,
                'content'   : 'running maintenance' },
            { 'timestamp' : 275,
                'content'   : 'running maintenance' },
            { 'timestamp' : 350,
                'content'   : 'running maintenance' },
            { 'timestamp' : 375,
                'content'   : 'running maintenance' },
            { 'timestamp' : 150,
                'content'   : 'running bak' },
            { 'timestamp' : 175,
                'content'   : 'running bak' },
            { 'timestamp' : 250,
                'content'   : 'running bak' },
            { 'timestamp' : 350,
                'content'   : 'running bak' },
            { 'timestamp' : 375,
                'content'   : 'running bak' },
            { 'timestamp' : 151,
                'sip'   : '10.0.0.4', 'dip' :   '10.1.0.1' },
            { 'timestamp' : 176,
                'sip'   : '10.0.0.4', 'dip' :   '10.1.0.1' },
            { 'timestamp' : 250,
                'sip'   : '10.0.0.4', 'dip' :   '10.1.0.1' },
            { 'timestamp' : 275,
                'sip'   : '10.0.0.4', 'dip' :   '74.1.0.1' },
            { 'timestamp' : 351,
                'sip'   : '10.0.0.4', 'dip' :   '10.1.0.1' },
            { 'timestamp' : 376,
                'sip'   : '10.0.0.4', 'dip' :   '10.1.0.1' },
            { 'timestamp' : 151,
                'sip'   : '10.0.0.5', 'dip' :   '10.1.0.1' },
            { 'timestamp' : 176,
                'sip'   : '10.0.0.5', 'dip' :   '10.1.0.1' },
            { 'timestamp' : 250,
                'sip'   : '10.0.0.5', 'dip' :   '10.1.0.1' },
            { 'timestamp' : 275,
                'sip'   : '10.0.0.5', 'dip' :   '74.1.0.1' },
            { 'timestamp' : 351,
                'sip'   : '10.0.0.5', 'dip' :   '10.1.0.1' },
            { 'timestamp' : 376,
                'sip'   : '10.0.0.5', 'dip' :   '10.1.0.1' },
            {'timestamp':151, 'dom' : 'back.com', 'ip' : '10.1.0.1' },
            {'timestamp':176, 'dom' : 'back.com', 'ip' : '10.1.0.1' },
            {'timestamp':250, 'dom' : 'back.com', 'ip' : '10.1.0.1' },
            {'timestamp':275, 'dom' : 'back.com', 'ip' : '74.1.0.1' },
            {'timestamp':351, 'dom' : 'back.com', 'ip' : '10.1.0.1' },
            {'timestamp':376, 'dom' : 'back.com', 'ip' : '10.1.0.1' }
        ]

def at(x):
    return x['timestamp']

#events2 = sorted(events, key=lambda record: record['timestamp'])
#events2 = sorted(events, key=lambda record: at(record))
#pprint.pprint(events2)
events.sort(key=lambda record: at(record))

pprint.pprint(events)

