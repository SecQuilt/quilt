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


class binerator:
    def __init__(self, lhs, rhs, tol, startfunc, elementfunc, endfunc):
        self.lhs = lhs
        self.rhs = rhs
        self.tol = tol
        self.startfunc = startfunc
        self.elementfunc = elementfunc
        self.endfunc = endfunc

    def startbin(self, startv):
        self.lastv = startv
        self.startfunc()

    def append(self, curv):
        if curv - self.lastv > self.tol:
            return False
        self.lastv = curv
        # print 'appending bin', curv, 'newlen', self.len
        return True


    def nextbin(self):
        lhs = self.lhs
        rhs = self.rhs
        tol = self.tol
        start = merge(field(lhs, 'timestamp'), field(rhs, 'timestamp'))
        print str(dir(start))
        cur = iter(start)
        curbin = None
        retbin = None
        first = True

        while True:
            try:

                v = next(cur)

                # if v % 2 == 0:
                # isrhs = True

                if first:
                    self.startbin(v)
                    first = False
                elif not self.append(v):

                    retbin = curbin
                    self.endfunc()

                    self.startbin(v)

                    # yield v
                    # else:
                    #     print 'continuing bin', curbin.

                print 'checking val', v

            except StopIteration:
                # yield retbin
                # raise StopIteration
                self.endfunc()
                return

    def nextbin2(self):
        lhs = self.lhs
        rhs = self.rhs
        tol = self.tol
        i = merge(field(lhs, 'timestamp'), field(rhs, 'timestamp'))

        l = None
        self.startfunc()
        for v in i:
            if l is not None and v - l > tol:
                self.endfunc()
            self.elementfunc(v)
            l = v
        self.endfunc()

    def do_it(self):
        self.nextbin2()


def bin_start():
    print 'bin start'


def bin_stop():
    print 'bin stop'


def bin_element(v):
    print v


class bop:
    def startfunc(self):
        print 'bin start'

    def elementfunc(self, v):
        print 'val', v

    def endfunc(self):
        print 'bin end'


def bins(lhs, rhs, tol, bop):
    print 'bin stop'

    def nextbin2(self):
        i = merge(field(lhs, 'timestamp'), field(rhs, 'timestamp'))

        l = None
        bop.startfunc()
        for v in i:
            if l is not None and v - l > tol:
                bop.endfunc()
            bop.elementfunc(v)
            l = v


if __name__ == "__main__":
    b = binerator(lhsevents, rhsevents, 2, bin_start, bin_element, bin_stop)
    b.do_it()












