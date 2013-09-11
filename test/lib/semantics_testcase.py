#!/usr/bin/env python
import sys
import unittest
import quilt_data
import quilt_test_core
import re
import pickle
import logging

firstTime=True

class SemanticsTestcase(unittest.TestCase):

    def check_query_and_get_results(self, objs):
        # sleep a small ammount
        quilt_test_core.sleep_small()

        #self.assertTrue(o != None and len(o) > 1)
        qid = objs[1] 

        # call quilt_history query_id
        o = quilt_test_core.call_quilt_script('quilt_history.py',[qid])
        objs = quilt_test_core.retrieve_objects(o)
        quilt_test_core.log_objs("check_query_and_get_results:quilt_history.py", objs)

        # check it shows good state (completed)
        self.assertTrue(quilt_test_core.pattern_found("state", dict(objs[0])))
        pattern = quilt_test_core.get_pattern("state", dict(objs[0]))
        self.assertTrue(pattern == quilt_data.STATE_COMPLETED)

        return objs

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

        christian_holidays = quilt_test_core.get_source_name(
                "christian_holidays")
        secular_holidays = quilt_test_core.get_source_name(
                "secular_holidays")

        #TODO REad the pattern id from the std output then query that one
        # See ISSUE007 and ISSUE008
        quilt_test_core.call_quilt_script('quilt_define.py',[
            'source("' + secular_holidays + '","grep")',
            '-n', 'semantics_one_source',
            '-v', 'WHICH_HOLIDAY', 
            '-v', 'UNUSED',
            '-m', 'WHICH_HOLIDAY', secular_holidays, 'grep', 'GREP_ARGS',
            '-m', 'UNUSED', christian_holidays, 'grep', 'GREP_ARGS'
            ])


        #TODO REad the pattern id from the std output then query that one
        # See ISSUE007 and ISSUE008
        quilt_test_core.call_quilt_script('quilt_define.py',[
            'at(source("' + christian_holidays + '","grep"))==12',
            '-n', 'semantics_equals_literal'
            ])

        #TODO REad the pattern id from the std output then query that one
        # See ISSUE007 and ISSUE008
        quilt_test_core.call_quilt_script('quilt_define.py',[
            "concurrent(source('" + christian_holidays + "','grep')," +
                        "source('" + secular_holidays   + "','grep'))",
                        '-n', 'semantics_concurrent'])

        #TODO REad the pattern id from the std output then query that one
        # See ISSUE007 and ISSUE008
        quilt_test_core.call_quilt_script('quilt_define.py',[
            "concurrent(source('" + christian_holidays + "','grep')," +
                        "source('" + secular_holidays   + "','grep')['major']=='True')",
                        '-n', 'semantics_nested'])

            
        #TODO REad the pattern id from the std output then query that one
        # See ISSUE007 and ISSUE008
        quilt_test_core.call_quilt_script('quilt_define.py',[
            'follows(1,' +
                "source('" + secular_holidays   + "','grep')," +
                "source('" + christian_holidays   + "','grep'))",
            '-n', 'semantics_follows'
            ])

    def test_one_source(self): #FAILING
        """
        This test assures that a simple case of semantics is tested
        where only one thing is referenced, the results of one source
        query.  Additionally a red herring second varible is specified
        in the pattern, but is not mentioned in the pattern's code, only 
        in the query's variables.  We want to make sure quilt is smart
        enough not to make a useless source query.  An additional query
        is made on the pattern where the second unused variable is
        given a value.  Quilt should still be smart enough not to run
        this source query because it is not mentioned in the pattern code
        """

        # issue a valid query
        o = quilt_test_core.call_quilt_script('quilt_submit.py',[
            'semantics_one_source', '-y', '-v', 'WHICH_HOLIDAY', "newyears"
            ])
        objs = quilt_test_core.retrieve_objects(o)
        objs = self.check_query_and_get_results(objs)
        quilt_test_core.log_objs("test_one_source:quilt_submit.py1", objs)
        obj = objs[0]

        # find some particulars in the results
        occurrences = quilt_test_core.get_pattern_occurrences2("newyears", obj)
        logging.debug("newyears should = 1 and = " + str(occurrences))
        self.assertTrue(occurrences == 1)

        occurrences = quilt_test_core.get_pattern_occurrences2("easter", obj)
        logging.debug("easter should = 0 and = " + str(occurrences))
        self.assertTrue(occurrences == 0)

        occurrences = quilt_test_core.get_pattern_occurrences2("christmass", obj)
        logging.debug("christmass should = 0 and = " + str(occurrences))
        self.assertTrue(occurrences == 0)

        # issue a valid query
        o = quilt_test_core.call_quilt_script('quilt_submit.py',[
            'semantics_one_source', '-y', '-v', 'UNUSED', "valentines"
            ])
        objs = quilt_test_core.retrieve_objects(o)
        objs = self.check_query_and_get_results(objs)
        quilt_test_core.log_objs("test_one_source:quilt_submit.py2", objs)
        obj = objs[0]

        occurrences = quilt_test_core.get_pattern_occurrences2("newyears", obj)
        logging.debug("newyears should = 1 and = " + str(occurrences))
        self.assertTrue(occurrences == 1)

        occurrences = quilt_test_core.get_pattern_occurrences2("thanksgiving", obj)
        logging.debug("thanksgiving should = 1 and = " + str(occurrences))
        self.assertTrue(occurrences == 1)

    def test_equals_literal(self): #FAILING
        """
        This test assures a case where a results list is matched against
        a literal string.  
        """

        # issue valid query for christian_holidays
        # call quilt_submit christian_holidays -y 
        o = quilt_test_core.call_quilt_script('quilt_submit.py',[
            'semantics_equals_literal', '-y'
            ])
        objs = quilt_test_core.retrieve_objects(o)

        # Check results
        # call quilt_history query_id
        # capture stdout, assure good exitcode
        objs = self.check_query_and_get_results(objs)
        quilt_test_core.log_objs("test_equals_literal:quilt_submit.py", objs)
        obj = objs[0]

        # find some particulars in the results
        # assure output contains "christmass"
        occurrences = quilt_test_core.get_pattern_occurrences2("christmass", obj)
        logging.debug("christmass should = 1 and = " + str(occurrences))
        self.assertTrue(occurrences == 1)

        # assure output contains no secular_holidays
        occurrences = quilt_test_core.get_pattern_occurrences2("boxingday", obj)
        logging.debug("boxingday should = 0 and = " + str(occurrences))
        self.assertTrue(occurrences == 0)

    def test_concurrent(self): #FAILING
        """
        This test covers the simplest case for a temporal operator
        function call.  Also covers case where pattern has no mapped
        variables, but still references sources
        """
        
        # issue valid query for concurrent_holidays
        # call quilt_submit semantics_concurrent -y 
        o = quilt_test_core.call_quilt_script('quilt_submit.py',[
            'semantics_concurrent', '-y'
            ])
        objs = quilt_test_core.retrieve_objects(o)
        quilt_test_core.log_objs("test_concurrent:quilt_submit.py", objs)

        # Check result, call quilt_history query_id
        # capture stdout, assure good exitcode
        objs = self.check_query_and_get_results(objs)
        obj = objs[0]

        # assure output contains laborday and ashwednesday
        # assure output contains no christmass
        occurrences = quilt_test_core.get_pattern_occurrences2("boxingday", obj)
        logging.debug("boxingday should = 1 and = " + str(occurrences))
        self.assertTrue(occurrences == 1)

        occurrences = quilt_test_core.get_pattern_occurrences2("christmass", obj)
        logging.debug("christmass should = 1 and = " + str(occurrences))
        self.assertTrue(occurrences == 1)

        occurrences = quilt_test_core.get_pattern_occurrences2("newyears", obj)
        logging.debug("newyears should = 0 and = " + str(occurrences))
        self.assertTrue(occurrences == 0)

    def test_nested_concurrent(self): #FAILING
        """
        This test covers a nested case where operator operates on
        results of a literal comparison.
        Also covers case where pattern has no mapped
        variables, but still references sources
        """

        # issue valid query for concurrent_holidays
        # call quilt_submit semantics_concurrent -y 
        o = quilt_test_core.call_quilt_script('quilt_submit.py',[
            'semantics_nested', '-y'
            ])
        objs = quilt_test_core.retrieve_objects(o)
        quilt_test_core.log_objs("test_nested_concurrent:quilt_submit.py", objs)

        # Check results
        # call quilt_history query_id
        # capture stdout, assure good exitcode
        objs = self.check_query_and_get_results(objs)
        obj = objs[0]

        # assure output contains laborday and ashwednesday
        occurrences = quilt_test_core.get_pattern_occurrences2("ashwednesday", obj)
        logging.debug("ashwednesday should = 1 and = " + str(occurrences))
        self.assertTrue(occurrences == 1)

        # assure output contains no christmass
        occurrences = quilt_test_core.get_pattern_occurrences2("christmass", obj)
        logging.debug("christmass should = 0 and = " + str(occurrences))
        self.assertTrue(occurrences == 0)

    def test_follows(self): #FAILING
        """
        This test assures the operation of the 'follows' quilt language
        function.  It uses a pattern which selects christian holidays that
        follow secular holidays within 1 month.  We expect to see
        ashwednesday as it follows newyears by one month, 
        """
        # issue valid query for concurrent_holidays
        # call quilt_submit semantics_concurrent -y 
        o = quilt_test_core.call_quilt_script('quilt_submit.py',[
            'semantics_follows', '-y'
            ])
        objs = quilt_test_core.retrieve_objects(o)
        quilt_test_core.log_objs("test_follows:quilt_submit.py", objs)

        # Check results
        # call quilt_history query_id
        # capture stdout, assure good exitcode
        objs = self.check_query_and_get_results(objs)
        obj = objs[0]

        # assure output contains ashednesday
        occurrences = quilt_test_core.get_pattern_occurrences2("ashwednesday", obj)
        logging.debug("ashwednesday should = 1 and = " + str(occurrences))
        self.assertTrue(occurrences == 1)

        # assure output contains no christmass
        occurrences = quilt_test_core.get_pattern_occurrences2("christmass", obj)
        logging.debug("christmass should = 1 and = " + str(occurrences))
        self.assertTrue(occurrences == 1)

        # assure output contains no valentines
        occurrences = quilt_test_core.get_pattern_occurrences2("valentines", obj)
        logging.debug("valentines should = 0 and = " + str(occurrences))
        self.assertTrue(occurrences == 0)

        # assure output contains no easter
        occurrences = quilt_test_core.get_pattern_occurrences2("easter", obj)
        logging.debug("easter should = 0 and = " + str(occurrences))
        self.assertTrue(occurrences == 0)

        # assure output contains no boxingday
        occurrences = quilt_test_core.get_pattern_occurrences2("boxingday", obj)
        logging.debug("boxingday should = 0 and = " + str(occurrences))
        self.assertTrue(occurrences == 0)

if __name__ == "__main__":
    quilt_test_core.unittest_main_helper(
        "Run integration test for multiple basic source",sys.argv)
    unittest.main()
