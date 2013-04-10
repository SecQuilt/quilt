#!/usr/bin/env python
#REVIEW
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

class BasicSourceTestcase(unittest.TestCase):

    def setUp(self):
        """Setup the query master with some patterns used by the tests"""
        o = quilt_test_core.call_quilt_script('quilt_define.py',[
            '-n', 'test_pattern',
            '-v', 'SEARCHSTRING' 'the Search string'
            '-m', 'SEARCHSTRING', 'syslog', 'grep', 'OPTIONS'])
        logging.debug("Defined test_pattern")

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
        o = quilt_test_core.call_quilt_script('quilt_submit.py',[
            '-y', '-v', 'SEARCHSTRING', 'Occurs_1_time'])
        # capture query_id from std out 
        a = o.index("Query ID is: ") + len(str("Query ID is: "))
        qid = o[a:]
        self.assertTrue(len(qid) > 0)
        # sleep 1 second
        time.sleep(1)

        # check that the query is in the history showing good state
        # call quilt_history 
        # asuure success, captrue stdout, 
        o = quilt_test_core.call_quilt_script('quilt_history.py')
        # check it contains query_id
        self.assertTrue(qid in o)
        # call quilt_history query_id
        o = quilt_test_core.call_quilt_script('quilt_history.py',[qid])
        # check it shows good state (completed)
        self.assertTrue("COMPLETE" in o)
        #   text "Occurs_1_time"
        #   assure only one result
        occurences = 
            len([m.start() for m in re.finditer('Occurs_1_time', o)])
        self.assertTrue(occurences == 1)
        
    def test_valid_query_multi_result(self):
        pass

    def test_valid_query_no_results(self):
        pass

    def test_valid_query_all_results(self):
        pass

if __name__ == "__main__":
    quilt_test_core.unittest_main_helper("Run the most basic of test cases",sys.argv)
    unittest.main()
