#!/usr/bin/env python
import unittest


class SomeTest(unittest.TestCase):

#    def __enter__(self):
#        pass

    def get(self):
        if self.lazy == None:
            self.lazy = "Initied"
            print "init"
        return self.lazy

    def __init__(self, testName):
        unittest.TestCase.__init__(self,testName)
        self.lazy = None

    def test_one(self):
        self.get()
        print "one"
            
    def test_two(self):
        self.get()
        print "two"
        
    def __del__(self):
        print "exiting"
        if self.lazy != None:
            print "destroying"
            self.lazy = None

if __name__ == "__main__":
    unittest.main()
