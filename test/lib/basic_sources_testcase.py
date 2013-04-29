#!/usr/bin/env python
#REVIEW
import sys
import unittest
import quilt_test_core
import time
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

        syslog = quilt_test_core.get_source_name("syslog")
        multisource = quilt_test_core.get_source_name("multisource")

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
            '-m', 'SEARCHSTRING6', multisource, 'pat2', 'PAT2SRCVAR2',
            "(SEARCHSTRING6/SEARCHSTRING5)-(SEARCHSTRING4^SEARCHSTRING3)*" + 
                "((SEARCHSTRING2+SEARCHSTRING1))"
            ])



            
    def test_multi_sources(self):
        """
        check multiple variables, and multiple patterns are functioning
        """

        # defaults specified in source patterns for SEARCHSTRING[4,6]
        o = str(quilt_test_core.call_quilt_script('quilt_submit.py',[
            '-y', 
            '-v', 'SEARCHSTRING1', "Occurs_1_time",
            '-v', 'SEARCHSTRING2', "Occurs_3_times",
            '-v', 'SEARCHSTRING3', "-l",
            '-v', 'SEARCHSTRING5', "-w"
            ]))
        time.sleep(1)

        # capture query_id from std out 
        qid = o.index("Query ID is: ") + len(str("Query ID is: "))
        # issue a valid query

        # call quilt_history query_id
        o = quilt_test_core.call_quilt_script('quilt_history.py',[qid])
        # check it shows good state (completed)
        self.assertTrue(quilt_data.STATE_COMPLETED in o)

        # find some particulars in the results
        occurences = (
            len([m.start() for m in re.finditer(
                "Occurs_1_time", o)]))

        # have to +1 because the search variable is also in the stdout
        self.assertTrue(occurences == 1 + 1)

        occurences = (
            len([m.start() for m in re.finditer(
                "Occurs_3_times", o)]))

        # have to +1 because the search variable is also in the stdout
        self.assertTrue(occurences == 1 + 3)

        occurences = (
            len([m.start() for m in re.finditer(
                "src default for pat1 occurs once", o)]))

        # have to +1 because the search variable is also in the stdout
        self.assertTrue(occurences == 1 + 1)
        
        occurences = (
            len([m.start() for m in re.finditer(
                "src default for pat2 occurs twice", o)]))

        # have to +1 because the search variable is also in the stdout
        self.assertTrue(occurences == 1 + 2)


if __name__ == "__main__":
    quilt_test_core.unittest_main_helper(
        "Run integration test for multiple basic source",sys.argv)
    unittest.main()