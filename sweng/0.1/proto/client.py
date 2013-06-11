#!/usr/bin/env python
import Pyro4
import sys
import random
import time

uri = sys.argv[1]

for i in range(10):
    with Pyro4.Proxy(uri) as msgs:
        print "uploading", i

        msgs.put(i)
        time.sleep(random.random()*10)

        print "downloading", msgs.get()


