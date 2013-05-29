#!/usr/bin/env python
import sys
import unittest
import quilt_test_core
import quilt_data
import re

firstTime=True

class SemanticsTestcase(unittest.TestCase):


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
        # sleep a small ammount
        quilt_test_core.sleep_small()

        # capture query_id from std out 
        a = o.index("Query ID is: ") + len(str("Query ID is: "))
        qid = o[a:]


        # call quilt_history query_id
        o = quilt_test_core.call_quilt_script('quilt_history.py',[qid])
        # check it shows good state (completed)
        self.assertTrue(quilt_data.STATE_COMPLETED in o)

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


#       # issue a valid query
#       o = str(quilt_test_core.call_quilt_script('quilt_submit.py',[
#           'secular_holidays', '-y', '-v', 'UNUSED', "valentines"
#           ]))
#       # sleep a small ammount
#       quilt_test_core.sleep_small()

#       # capture query_id from std out 
#       a = o.index("Query ID is: ") + len(str("Query ID is: "))
#       qid = o[a:]


#       # call quilt_history query_id
#       o = quilt_test_core.call_quilt_script('quilt_history.py',[qid])
#       # check it shows good state (completed)
#       self.assertTrue(quilt_data.STATE_COMPLETED in o)

#       # find some particulars in the results
#       occurences = (
#           len([m.start() for m in re.finditer(
#               "newyears", o)]))
#       self.assertTrue(occurences == 1)

#       occurences = (
#           len([m.start() for m in re.finditer(
#               "valentines", o)]))
#       self.assertTrue(occurences == 1)


if __name__ == "__main__":
    quilt_test_core.unittest_main_helper(
        "Run integration test for multiple basic source",sys.argv)
    unittest.main()
