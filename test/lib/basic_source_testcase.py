#!/usr/bin/env python
import sys
import logging
import unittest
import quilt_test_core
import quilt_data
import re
import marshal

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
        srcName = quilt_test_core.get_source_name("syslog")
        
        logging.debug("Determined source name as: " + srcName)

        #TODO REad the pattern id from the std output then query that one
        # See ISSUE007 and ISSUE008
        quilt_test_core.call_quilt_script('quilt_define.py',[
            '-n', 'test_pattern',
            '-v', 'SEARCHSTRING', 'the Search string',
            '-m', 'SEARCHSTRING', srcName, 'grep', 'OPTIONS'])
        logging.debug("Defined test_pattern")


    def query(self,searchString):
        o = quilt_test_core.call_quilt_script('quilt_submit.py',[
            '-y', '-v', 'SEARCHSTRING', searchString, 'test_pattern'])
        objs = quilt_test_core.retrieve_objects(o)
        quilt_test_core.log_objs("query:quilt_submit.py", objs)
        #self.assertTrue(o != None)
        #self.assertTrue(o != None and len(o) > 1)

        # capture query_id from std out 
        #a = o.index("Query ID is: ") + len(str("Query ID is: "))
        #qid = o[a:]
        qid = objs[1]
        self.assertTrue(len(qid) > 0)
        # sleep a small ammount
        quilt_test_core.sleep_medium()
        return qid

    def check(self,qid):
        o = quilt_test_core.call_quilt_script('quilt_history.py')
        objs = quilt_test_core.retrieve_objects(o)
        quilt_test_core.log_objs("check:quilt_history.py1", objs)

        # check it contains query_id
        self.assertTrue(quilt_test_core.pattern_found(qid, dict(objs[0])))

        # call quilt_history query_id
        o = quilt_test_core.call_quilt_script('quilt_history.py',[qid])
        objs = quilt_test_core.retrieve_objects(o)
        quilt_test_core.log_objs("check:quilt_history.py2", objs)

        # check it shows good state (completed)
        self.assertTrue(quilt_test_core.pattern_found("state", dict(objs[0])))
        pattern = quilt_test_core.get_pattern("state", dict(objs[0]))
        self.assertTrue(pattern == quilt_data.STATE_COMPLETED)

    def test_status(self): #PASSING
        # check for the query pattern
        # call quilt_status 
        # check errorcode and output contains 
        #   "test_pattern" and "syslog"
        o = quilt_test_core.call_quilt_script('quilt_status.py')
        objs = quilt_test_core.retrieve_objects(o)
        quilt_test_core.log_objs("test_status:quilt_status.py", objs)
        patterns = dict(objs[3])
        self.assertTrue(quilt_test_core.pattern_found("test_pattern", patterns))

    def test_valid_query_one_result(self): #FAILING
        # issue a valid query
        # call quilt_submit test_pattern -y -v SEARCHSTRING Occurs_1_time
        qid = self.query("Occurs_1_time")

        # check that the query is in the history showing good state
        self.check(qid)

        # call quilt_history query_id
        o = quilt_test_core.call_quilt_script('quilt_history.py',[qid])
        objs = quilt_test_core.retrieve_objects(o)
        quilt_test_core.log_objs("test_valid_query_one_result:quilt_history.py", objs)
        obj = objs[0]

        # check it shows good state (completed)
        self.assertTrue(quilt_test_core.pattern_found("state", dict(objs[0])))
        pattern = quilt_test_core.get_pattern("state", dict(objs[0]))
        self.assertTrue(pattern == quilt_data.STATE_COMPLETED)

        #   text "Occurs_1_time"
        #   assure only one result
        occurrences = quilt_test_core.get_pattern_occurrences('Occurs_1_time', dict(obj))
        logging.debug("Occurs_1_time should = 1 and = " + str(occurrences))
        self.assertTrue(occurrences == 1)
        
    def test_valid_query_multi_result(self): #FAILING


        # issue a valid query
        # call quilt_submit test_pattern -y -v SEARCHSTRING Occurs_1_time
        qid = self.query("Occurs_3_times")

        # check that the query is in the history showing good state
        self.check(qid)

        # call quilt_history query_id
        o = quilt_test_core.call_quilt_script('quilt_history.py',[qid])
        objs = quilt_test_core.retrieve_objects(o)
        quilt_test_core.log_objs("test_valid_query_multi_result:quilt_history.py", objs)
        obj = objs[0]

        # check it shows good state (completed)
        self.assertTrue(quilt_test_core.pattern_found("state", dict(obj)))
        pattern = quilt_test_core.get_pattern("state", dict(obj))
        self.assertTrue(pattern == quilt_data.STATE_COMPLETED)

        #   text "Occurs_3_times"
        #   assure only three results
        occurrences = quilt_test_core.get_pattern_occurrences('Occurs_3_times', dict(obj))
        logging.debug("Occurs_3_times should = 3 and = " + str(occurrences))
        self.assertTrue(occurrences == 3)

    def test_valid_query_no_results(self): #FAILING
        # issue a valid query
        # call quilt_submit test_pattern -y -v SEARCHSTRING Occurs_1_time
        qid = self.query("Occurs_no_times")

        # check that the query is in the history showing good state
        self.check(qid)

        # call quilt_history query_id
        o = quilt_test_core.call_quilt_script('quilt_history.py',[qid])
        objs = quilt_test_core.retrieve_objects(o)
        quilt_test_core.log_objs("test_valid_query_no_results:quilt_history.py", objs)
        obj = objs[0]

        # check it shows good state (completed)
        self.assertTrue(quilt_test_core.pattern_found("state", dict(obj)))
        pattern = quilt_test_core.get_pattern("state", dict(obj))
        self.assertTrue(pattern == quilt_data.STATE_COMPLETED)

        #   text "Occurs_no_times"
        #   assure no results
        occurrences = quilt_test_core.get_pattern_occurrences('Occurs_no_times', dict(obj))
        logging.debug("Occurs_no_times should = 0 and = " + str(occurrences))
        self.assertTrue(occurrences == 0)

    def test_valid_query_all_results(self): #FAILING
        # issue a valid query
        # call quilt_submit test_pattern -y -v SEARCHSTRING Occurs_1_time
        qid = self.query(".")

        # check that the query is in the history showing good state
        self.check(qid)

        # call quilt_history query_id
        o = quilt_test_core.call_quilt_script('quilt_history.py',[qid])
        objs = quilt_test_core.retrieve_objects(o)
        quilt_test_core.log_objs("test_valid_query_all_results:quilt_history.py", objs)
        obj = objs[0]

        # check it shows good state (completed)
        self.assertTrue(quilt_test_core.pattern_found("state", dict(obj)))
        pattern = quilt_test_core.get_pattern("state", dict(obj))
        self.assertTrue(pattern == quilt_data.STATE_COMPLETED)

        #   assure there are many results
        occurrences = quilt_test_core.get_pattern_occurrences('times', dict(obj))
        logging.debug("times should > 1 and = " + str(occurrences))
        self.assertTrue(occurrences > 1)

if __name__ == "__main__":
    quilt_test_core.unittest_main_helper(
        "Run integration test for one basic source",sys.argv)
    unittest.main()
