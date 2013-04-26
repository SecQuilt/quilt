#!/usr/bin/env python

class a:

    data = "self data"
    class b:
        def __init__(self, aref):
            self._a = aref
        def bfunc(self):
            print "accessing parent data"
            print self._a.data 
        

    def __init__(self):
        self._b = a.b(self)


# mya = a();
# mya._b.bfunc()

