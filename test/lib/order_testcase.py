#!/usr/bin/env python
import sys
import unittest
import quilt_data
import quilt_test_core
import logging

firstTime = True


class OrderTestcase(unittest.TestCase):
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

        # get the full source name for even and odd sources
        out_of_order_numbers = quilt_test_core.get_source_name(
            "out_of_order_numbers")

        # TODO REad the pattern id from the std output then query that one
        # See ISSUE007 and ISSUE008
        # call quilt_define with the pattern code and name query
        #   dups_follows
        quilt_test_core.call_quilt_script('quilt_define.py', ['-n',
            'out_of_order',
            'source("' + out_of_order_numbers + '","grep")'])


    # TODO see ISSUE008  We want to move this to test_core when there is a
    # less hacky way to do it
    def check_query_and_get_results3(self, submitStdout):
        # sleep a small ammount
        quilt_test_core.sleep_small()

        o = submitStdout
        # capture query_id from std out 
        a = o.index("Query ID is: ") + len(str("Query ID is: "))
        qid = o[a:]

        # call quilt_history query_id
        o = quilt_test_core.call_quilt_script('quilt_history.py', [qid])
        # check it shows good state (completed)
        self.assertTrue(quilt_data.STATE_COMPLETED in o)

        return o

    def test_order(self):
        """
        This test queries a source whose data is out fo order.  The
        configuration of the source pattern indicates that the data is out
        of order and quilt should transform it to ordered behind the
        scenes.  We simply performa  query in the normal matter and assure
        result is sorted
        """

        # issue a valid query
        # Assure proper execution, and get results from quilt_history
        o = str(quilt_test_core.call_quilt_script('quilt_submit.py', [
            '-y', 'out_of_order']))

        o = self.check_query_and_get_results3(o)

        # Check results
        #   assure that results are in order
        l = []
        for i in xrange(1, 6):
            searchStr = "{'timestamp': " + str(i) + '}'
            index = o.find(searchStr)
            logging.debug("looking for string: " + searchStr)
            self.assertTrue(index != -1)
            l.append(index)

        isSorted = all(l[i] <= l[i + 1] for i in xrange(len(l) - 1))
        self.assertTrue(isSorted)


if __name__ == "__main__":
    quilt_test_core.unittest_main_helper(
        "Run integration test for assuring unorderd sorces are ordered by the system",
        sys.argv)
    unittest.main()
