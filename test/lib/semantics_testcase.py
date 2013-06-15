#!/usr/bin/env python
import sys
import unittest
import quilt_data
import quilt_test_core
import re
from string import Template

firstTime=True

class SemanticsTestcase(unittest.TestCase):

    # TODO see ISSUE008  We want to move this to test_core when there is a
    # less hacky way to do it
    def check_query_and_get_results(self, submitStdout):
        # sleep a small ammount
        quilt_test_core.sleep_small()

        o = submitStdout
        # capture query_id from std out 
        a = o.index("Query ID is: ") + len(str("Query ID is: "))
        qid = o[a:]

        # call quilt_history query_id
        o = quilt_test_core.call_quilt_script('quilt_history.py',[qid])
        # check it shows good state (completed)
        self.assertTrue(quilt_data.STATE_COMPLETED in o)

        return o



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

        #TODO REad the pattern id from the std output then query that one
        # See ISSUE007 and ISSUE008
        quilt_test_core.call_quilt_script('quilt_define.py',[
            'until(' +
                "source('" + secular_holidays   + "','grep')," +
                "source('" + christian_holidays   + "','grep'))",
            '-n', 'semantics_until'
            ])
    def test_one_source(self):
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
        o = str(quilt_test_core.call_quilt_script('quilt_submit.py',[
            'semantics_one_source', '-y', '-v', 'WHICH_HOLIDAY', "newyears"
            ]))

        o = self.check_query_and_get_results(o)

        # find some particulars in the results
        occurences = (
            len([m.start() for m in re.finditer(
                "newyears", o)]))
        # + 2 because it occurs in var specs
        self.assertTrue(occurences == 1 + 2)

        occurences = (
            len([m.start() for m in re.finditer(
                "easter", o)]))
        self.assertTrue(occurences == 0)


        occurences = (
            len([m.start() for m in re.finditer(
                "christmass", o)]))
        self.assertTrue(occurences == 0)


        # issue a valid query
        o = str(quilt_test_core.call_quilt_script('quilt_submit.py',[
            'semantics_one_source', '-y', '-v', 'UNUSED', "valentines"
            ]))

        o = self.check_query_and_get_results(o)
        
        # find some particulars in the results
        occurences = (
            len([m.start() for m in re.finditer(
                "newyears", o)]))
        self.assertTrue(occurences == 1)

        occurences = (
            len([m.start() for m in re.finditer(
                "thanksgiving", o)]))
        self.assertTrue(occurences == 1)

    def test_equals_literal(self):
        """
        This test assures a case where a results list is matched against
        a literal string.  
        """

        # issue valid query for christian_holidays
        # call quilt_submit christian_holidays -y 
        o = str(quilt_test_core.call_quilt_script('quilt_submit.py',[
            'semantics_equals_literal', '-y'
            ]))

        # Check results
        # call quilt_history query_id
        # capture stdout, assure good exitcode
        o = self.check_query_and_get_results(o)

        # find some particulars in the results
        # assure output contains "christmass"
        occurences = (
            len([m.start() for m in re.finditer(
                "christmass", o)]))
        self.assertTrue(occurences == 1)

        # assure output contains no secular_holidays
        occurences = (
            len([m.start() for m in re.finditer(
                "boxingday", o)]))
        self.assertTrue(occurences == 0)

    def test_concurrent(self):
        """
        This test covers the simplest case for a temporal operator
        function call.  Also covers case where pattern has no mapped
        variables, but still references sources
        """
        
        # issue valid query for concurrent_holidays
        # call quilt_submit semantics_concurrent -y 
        o = str(quilt_test_core.call_quilt_script('quilt_submit.py',[
            'semantics_concurrent', '-y'
            ]))

        # Check result, call quilt_history query_id
        # capture stdout, assure good exitcode
        o = self.check_query_and_get_results(o)

        # assure output contains laborday and ashwednesday
        # assure output contains no christmass
        occurences = (
            len([m.start() for m in re.finditer(
                "boxingday", o)]))
        self.assertTrue(occurences == 1)
        occurences = (
            len([m.start() for m in re.finditer(
                "christmass", o)]))
        self.assertTrue(occurences == 1)
        occurences = (
            len([m.start() for m in re.finditer(
                "newyears", o)]))
        self.assertTrue(occurences == 0)


    def test_nested_concurrent(self):
        """
        This test covers a nested case where operator operates on
        results of a literal comparison.
        Also covers case where pattern has no mapped
        variables, but still references sources
        """

        # issue valid query for concurrent_holidays
        # call quilt_submit semantics_concurrent -y 
        o = str(quilt_test_core.call_quilt_script('quilt_submit.py',[
            'semantics_nested', '-y'
            ]))

        # Check results
        # call quilt_history query_id
        # capture stdout, assure good exitcode
        o = self.check_query_and_get_results(o)

        # assure output contains laborday and ashwednesday
        occurences = (
            len([m.start() for m in re.finditer(
                "ashwednesday", o)]))
        self.assertTrue(occurences == 1)

        # assure output contains no christmass
        occurences = (
            len([m.start() for m in re.finditer(
                "christmass", o)]))
        self.assertTrue(occurences == 0)

    def test_follows(self):
        """
        This test assures the operation of the 'follows' quilt language
        function.  It uses a pattern which selects christian holidays that
        follow secular holidays within 1 month.  We expect to see
        ashwednesday as it follows newyears by one month, 
        """
        # issue valid query for concurrent_holidays
        # call quilt_submit semantics_concurrent -y 
        o = str(quilt_test_core.call_quilt_script('quilt_submit.py',[
            'semantics_follows', '-y'
            ]))

        # Check results
        # call quilt_history query_id
        # capture stdout, assure good exitcode
        o = self.check_query_and_get_results(o)

        # assure output contains ashednesday
        occurences = (
            len([m.start() for m in re.finditer(
                "ashwednesday", o)]))
        self.assertTrue(occurences == 1)
        # assure output contains no christmass
        occurences = (
            len([m.start() for m in re.finditer(
                "christmass", o)]))
        self.assertTrue(occurences == 1)


        # assure output contains no valentines
        occurences = (
            len([m.start() for m in re.finditer(
                "valentines", o)]))
        self.assertTrue(occurences == 0)

        # assure output contains no easter
        occurences = (
            len([m.start() for m in re.finditer(
                "easter", o)]))
        self.assertTrue(occurences == 0)

        # assure output contains no boxingday
        occurences = (
            len([m.start() for m in re.finditer(
                "boxingday", o)]))
        self.assertTrue(occurences == 0)

    def test_until(self):
        """
        This test assures the operation of the 'until' quilt language
        function.  It uses a pattern which selects secular holidays 
        until a christian holiday occurs.  We expect to see newyears, 
        and valentines
        """
        # issue valid query for concurrent_holidays
        # call quilt_submit semantics_concurrent -y 
        o = str(quilt_test_core.call_quilt_script('quilt_submit.py',[
            'semantics_until', '-y'
            ]))

        # Check results
        # call quilt_history query_id
        # capture stdout, assure good exitcode
        o = self.check_query_and_get_results(o)

        # assure output contains ashednesday
        occurences = (
            len([m.start() for m in re.finditer(
                "valentines", o)]))
        self.assertTrue(occurences == 1)
        # assure output contains no christmass
        occurences = (
            len([m.start() for m in re.finditer(
                "newyears", o)]))
        self.assertTrue(occurences == 1)


        # assure output contains no valentines
        occurences = (
            len([m.start() for m in re.finditer(
                "easter", o)]))
        self.assertTrue(occurences == 0)

        # assure output contains no boxingday
        occurences = (
            len([m.start() for m in re.finditer(
                "boxingday", o)]))
        self.assertTrue(occurences == 0)

if __name__ == "__main__":
    quilt_test_core.unittest_main_helper(
        "Run integration test for semantic processing",sys.argv)
    unittest.main()
