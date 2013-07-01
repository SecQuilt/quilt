#!/usr/bin/env python
import ast
import sys
import pprint
import itertools
from heapq import merge


rhsevents = [
    {'timestamp': 151,
     'sip': '10.0.0.4', 'dip': '10.1.0.1'},
    {'timestamp': 175,
     'sip': '10.0.0.4', 'dip': '10.1.0.1'},
    {'timestamp': 251,
     'sip': '10.0.0.4', 'dip': '10.1.0.1'},
    {'timestamp': 275,
     'sip': '10.0.0.4', 'dip': '74.1.0.1'},
    {'timestamp': 351,
     'sip': '10.0.0.4', 'dip': '10.1.0.1'},
    {'timestamp': 377,
     'sip': '10.0.0.4', 'dip': '10.1.0.1'},
    {'timestamp': 151,
     'sip': '10.0.0.5', 'dip': '10.1.0.1'},
    {'timestamp': 177,
     'sip': '10.0.0.5', 'dip': '10.1.0.1'},
    {'timestamp': 251,
     'sip': '10.0.0.5', 'dip': '10.1.0.1'},
    {'timestamp': 275,
     'sip': '10.0.0.5', 'dip': '74.1.0.1'},
    {'timestamp': 351,
     'sip': '10.0.0.5', 'dip': '10.1.0.1'},
    {'timestamp': 375,
     'sip': '10.0.0.5', 'dip': '10.1.0.1'}
]

rhsevents.sort(key=lambda x: x['timestamp'])

lhsevents = [
    {'timestamp': 150, 'dom': 'back.com', 'ip': '10.1.0.1'},
    {'timestamp': 176, 'dom': 'back.com', 'ip': '10.1.0.1'},
    {'timestamp': 250, 'dom': 'back.com', 'ip': '10.1.0.1'},
    {'timestamp': 274, 'dom': 'back.com', 'ip': '74.1.0.1'},
    {'timestamp': 352, 'dom': 'back.com', 'ip': '10.1.0.1'},
    {'timestamp': 376, 'dom': 'back.com', 'ip': '10.1.0.1'},
]


def field(events, field):
    for e in events:
        yield e[field]


def even():
    yield 2
    yield 4


def odd():
    yield 1
    yield 3
    yield 5


def bin(v, rmin, rmax, nbins):
    s = nbins / (rmax - rmin)
    return (v - rmin) / s


class bin:
    def __init__(self, starti, startv, tol):
        self.starti = starti
        self.lastv = startv
        self.len = 1
        self.tol = tol
        print 'creating bin starting', startv

    def append(self, curv):
        if curv - self.lastv > self.tol:
            return False
        self.lastv = curv
        self.len += 1
        # print 'appending bin', curv, 'newlen', self.len
        return True

    def nextevent(self):
        i = iter(self.starti)
        l = self.len
        while l >= 0:
            v = next(i)
            l -= 1
            yield v

    def __str__(self):
        i = iter(self.starti)
        l = self.len
        bincol=[]
        while (l>0):
            bincol.append(next(i))
        return str(bincol)




def nextbin(lhs, rhs, tol):
    start = merge(field(lhs, 'timestamp'), field(rhs, 'timestamp'))
    print str(dir(start))
    cur = iter(start)
    curbin = None
    retbin = None

    while True:
        try:

            last = iter(cur)
            v = next(cur)
            print 'checking val', v

            # if v % 2 == 0:
            # isrhs = True

            if curbin is None:
                 curbin = bin(last, v, tol)
            elif not curbin.append(v):

                 retbin = curbin
                 curbin = bin(last, v, tol)
#                 print str(retbin)

                 yield retbin
            # else:
            #     print 'continuing bin', curbin.

        except StopIteration:
            yield retbin
            raise StopIteration


if __name__ == "__main__":
    v = None
    last = None

    i = 0
    bins = []
    for b in nextbin(lhsevents, rhsevents, 1):
        print 'got back', str(type(b))
        bins.append(b)
        # print '-'
        # for e in b.nextevent():
        #     print i, e
        #     i = i + 1
    print '1', bins[0],bins[0]











