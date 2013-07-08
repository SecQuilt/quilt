#!/usr/bin/env python

import sys

cnt = int(sys.argv[1])

mthd = int(sys.argv[2])

limit = int(sys.argv[3])

col = []
col2 = []

for i in xrange(cnt):
    col.append(i)

if mthd == 1:

    for x in col:
        if mthd == 1:
            if x < limit:
                col2.append(x)
else:
    for x in col:
        if x < limit:
            col2.append(x)


