#!/usr/bin/env python
import os
import sys
import logging
import unittest
import subprocess
import quilt_test_core
import sei_core
import quilt_core
import time
import query_master
import random
import quilt_data
import re

firstTime=True

class BasicSourceTestcase(unittest.TestCase):


    def setUp(self):
        """Setup the query master with some patterns used by the tests"""

        # ISSUE007
        # TODO, pyunit's bright idea is to call setup before each test.  It
        # was defining multiple patterns which was annoying but not a problem.
        # The cleaneast way to do things is probably to remove patterns after
        # the test, but we don't have that functionality.  For now just create
        # one pattern to avoid confusion, but do it by hacking in a global
        # variable
    
        global firstTime

        if firstTime != True:
            return
        firstTime = False

        # call quilt status and parse out the name of the syslog source
        cmd = os.path.join(quilt_test_core.get_quilt_lib_dir(),"quilt_status.py") + " | grep syslog | head -n 1 | awk '{print $1}' | sed  -e \"s/{'//\" -e \"s/'://\" "
        srcName = sei_core.run_process(cmd, whichReturn=sei_core.STDOUT, 
            logToPython=False, shell=True)
        
#        logging.debug("Determined source name as: " + srcName)

        o = quilt_test_core.call_quilt_script('quilt_define.py',[
            '-n', 'test_pattern',
            '-v', 'SEARCHSTRING', 'the Search string',
            '-m', 'SEARCHSTRING', srcName, 'grep', 'OPTIONS'])
        logging.debug("Defined test_pattern")


    def query(self,searchString):
        o = quilt_test_core.call_quilt_script('quilt_submit.py',[
            '-y', '-v', 'SEARCHSTRING', searchString, 'test_pattern'])
        # capture query_id from std out 
        a = o.index("Query ID is: ") + len(str("Query ID is: "))
        qid = o[a:]
        self.assertTrue(len(qid) > 0)
        # sleep 1 second
        time.sleep(1)
        return qid

    def check(self,qid):
        o = quilt_test_core.call_quilt_script('quilt_history.py')
        # check it contains query_id
        self.assertTrue(qid in o)
        # call quilt_history query_id
        o = quilt_test_core.call_quilt_script('quilt_history.py',[qid])
        # check it shows good state (completed)
        self.assertTrue(quilt_data.STATE_COMPLETED in o)

    def test_status(self):
        # check for the query pattern
        # call quilt_status 
        # check errorcode and output contains 
        #   "test_pattern" and "syslog"
        o = quilt_test_core.call_quilt_script('quilt_status.py')
        self.assertTrue('test_pattern' in o)
            
    def test_valid_query_one_result(self):
        # issue a valid query
        # call quilt_submit test_pattern -y -v SEARCHSTRING Occurs_1_time
        qid = self.query("Occurs_1_time")

        # check that the query is in the history showing good state
        self.check(qid)

        # call quilt_history query_id
        o = quilt_test_core.call_quilt_script('quilt_history.py',[qid])
        # check it shows good state (completed)
        self.assertTrue(quilt_data.STATE_COMPLETED in o)
        #   text "Occurs_1_time"
        #   assure only one result
        occurences = (
            len([m.start() for m in re.finditer('Occurs_1_time', o)]))

        # have to +1 because the search variable is also in the stdout
        self.assertTrue(occurences == 1 + 1)
        
    def test_valid_query_multi_result(self):


        # issue a valid query
        # call quilt_submit test_pattern -y -v SEARCHSTRING Occurs_1_time
        qid = self.query("Occurs_3_times")

        # check that the query is in the history showing good state
        self.check(qid)

        # call quilt_history query_id
        o = quilt_test_core.call_quilt_script('quilt_history.py',[qid])
        # check it shows good state (completed)
        self.assertTrue(quilt_data.STATE_COMPLETED in o)
        #   text "Occurs_3_times"
        #   assure only three results
        occurences = (
            len([m.start() for m in re.finditer('Occurs_3_times', o)]))
        # have to +1 because the search variable is also in the stdout
        self.assertTrue(occurences == 3 + 1)


    def test_valid_query_no_results(self):
        # issue a valid query
        # call quilt_submit test_pattern -y -v SEARCHSTRING Occurs_1_time
        qid = self.query("Occurs_no_times")

        # check that the query is in the history showing good state
        self.check(qid)

        # call quilt_history query_id
        o = quilt_test_core.call_quilt_script('quilt_history.py',[qid])
        # check it shows good state (completed)
        self.assertTrue(quilt_data.STATE_COMPLETED in o)
        #   text "Occurs_no_times"
        #   assure no results
        occurences = (
            len([m.start() for m in re.finditer('Occurs_no_times', o)]))
        self.assertTrue(occurences == 0 + 1)

    def test_valid_query_all_results(self):
        # issue a valid query
        # call quilt_submit test_pattern -y -v SEARCHSTRING Occurs_1_time
        qid = self.query(".*")

        # check that the query is in the history showing good state
        self.check(qid)

        # call quilt_history query_id
        o = quilt_test_core.call_quilt_script('quilt_history.py',[qid])
        # check it shows good state (completed)
        self.assertTrue(quilt_data.STATE_COMPLETED in o)
        #   assure there are many results
        occurences = (
            len([m.start() for m in re.finditer('\n', o)]))
        self.assertTrue(occurences > 4 + 1)

if __name__ == "__main__":
    quilt_test_core.unittest_main_helper(
        "Run integration test for one basic source",sys.argv)
    unittest.main()
