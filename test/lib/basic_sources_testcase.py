#!/usr/bin/env python
#REVIEW
import sys
import unittest
import quilt_test_core
import quilt_data
import re
import pickle
import logging

firstTime=True

class BasicSourcesTestcase(unittest.TestCase):


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

        syslog = quilt_test_core.get_source_name("syslog")
        multisource = quilt_test_core.get_source_name("multipattern")
        logging.debug("Determined syslog source name as: " + syslog)
        logging.debug("Determined multisource source name as: " + multisource)

        #TODO REad the pattern id from the std output then query that one
        # See ISSUE007 and ISSUE008
        quilt_test_core.call_quilt_script('quilt_define.py',[
            '-n', 'bigpattern',
            '-v', 'SEARCHSTRING1', 'the Search string1',
            '-v', 'SEARCHSTRING2', 'the Search string2',
            '-v', 'SEARCHSTRING3', 'the Search string3',
            '-v', 'SEARCHSTRING4', 'the Search string4',
            '-v', 'SEARCHSTRING5', 'the Search string5',
            '-v', 'SEARCHSTRING6', 'the Search string6',
            '-m', 'SEARCHSTRING1', syslog, 'grep', 'OPTIONS',
            '-m', 'SEARCHSTRING2', syslog, 'grep', 'OPTIONS', 'fooInst',
            '-m', 'SEARCHSTRING3', multisource, 'pat1', 'PAT1SRCVAR1', 
            '-m', 'SEARCHSTRING4', multisource, 'pat1', 'PAT1SRCVAR2', 
            '-m', 'SEARCHSTRING5', multisource, 'pat2', 'PAT2SRCVAR1', 
            '-m', 'SEARCHSTRING6', multisource, 'pat2', 'PAT2SRCVAR2'
            ])
            
    def test_multi_sources(self): #ERRORING OUT
        """
        check multiple variables, and multiple patterns are functioning
        """

        # defaults specified in source patterns for SEARCHSTRING[4,6]
        o = quilt_test_core.call_quilt_script('quilt_submit.py',[
            'bigpattern',
            '-y', 
            '-v', 'SEARCHSTRING1', "Occurs_1_time",
            '-v', 'SEARCHSTRING2', "Occurs_3_times",
            '-v', 'SEARCHSTRING3', "word-regexp",
            '-v', 'SEARCHSTRING5', "word-regexp"
            ])
        objs = quilt_test_core.retrieve_objects(o)
        quilt_test_core.log_objs("test_multi_sources:quilt_submit.py", objs)

        # sleep a small ammount
        quilt_test_core.sleep_large()

        qid = objs[1]
        self.assertTrue(qid != None and len(qid) > 1)

        # issue a valid query

        # call quilt_history query_id
        o = quilt_test_core.call_quilt_script('quilt_history.py',[qid])
        objs = quilt_test_core.retrieve_objects(o)
        quilt_test_core.log_objs("test_multi_sources:quilt_history.py", objs)
        obj = objs[0]

        # check it shows good state (completed)
        self.assertTrue(quilt_test_core.pattern_found("state", dict(obj)))
        pattern = quilt_test_core.get_pattern("state", dict(obj))
        quilt_test_core.log_objs("test_equals_literal:quilt_submit.py", obj)
        logging.debug("pattern = " + str(pattern))
        self.assertTrue(pattern == quilt_data.STATE_COMPLETED)

        # find some particulars in the results
        # assure output contains "Occurs_1_time"
        occurrences = quilt_test_core.get_pattern_occurrences("Occurs_1_time", obj)
        logging.debug("Occurs_1_time should = 1 and = " + str(occurrences))
        self.assertTrue(occurrences == 1)

        occurrences = quilt_test_core.get_pattern_occurrences("Occurs_3_times", obj)
        logging.debug("Occurs_3_times should = 3 and = " + str(occurrences))
        self.assertTrue(occurrences == 3)

        # have no + 1, these are defaults set from src pattern
        occurrences = quilt_test_core.get_pattern_occurrences(
                "src default for pat2 occurs twice", obj)
        logging.debug("src default for pat2 occurs twice should = 2 and = " + str(occurrences))
        self.assertTrue(occurrences == 2)

        occurrences = quilt_test_core.get_pattern_occurrences(
                "src default for pat1 occurs once", obj)
        logging.debug("src default for pat1 occurs once should = 1 and = " + str(occurrences))
        self.assertTrue(occurrences == 1)

if __name__ == "__main__":
    quilt_test_core.unittest_main_helper(
        "Run integration test for multiple basic source",sys.argv)
    unittest.main()
