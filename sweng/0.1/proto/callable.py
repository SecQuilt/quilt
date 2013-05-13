#!/usr/bin/env python
import sys
import itertools

a=[1,2,3,4,5,6,7,8,9]
b=[2,4,6,8]
c=[4,8]
_all=[a,b,c]

def checkEqual1(iterator):
    try:
        iterator = iter(iterator)
        first = next(iterator)
        return all(first == rest for rest in iterator)
    except StopIteration:
        return True

x = itertools.ifilter( checkEqual1, itertools.product(*_all))

print list(x)
