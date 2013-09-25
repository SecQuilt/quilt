#!/usr/bin/env python
# Copyright 2013 Carnegie Mellon University 
#  
# This material is based upon work funded and supported by the Department of Defense under Contract No. FA8721-
# 05-C-0003 with Carnegie Mellon University for the operation of the Software Engineering Institute, a federally 
# funded research and development center. 
#  
# Any opinions, findings and conclusions or recommendations expressed in this material are those of the author(s) and 
# do not necessarily reflect the views of the United States Department of Defense. 
#  
# NO WARRANTY. THIS CARNEGIE MELLON UNIVERSITY AND SOFTWARE ENGINEERING INSTITUTE 
# MATERIAL IS FURNISHEDON AN "AS-IS" BASIS. CARNEGIE MELLON UNIVERSITY MAKES NO 
# WARRANTIES OF ANY KIND, EITHER EXPRESSED OR IMPLIED, AS TO ANY MATTER INCLUDING, 
# BUT NOT LIMITED TO, WARRANTY OF FITNESS FOR PURPOSE OR MERCHANTABILITY, 
# EXCLUSIVITY, OR RESULTS OBTAINED FROM USE OF THE MATERIAL. CARNEGIE MELLON 
# UNIVERSITY DOES NOT MAKE ANY WARRANTY OF ANY KIND WITH RESPECT TO FREEDOM FROM 
# PATENT, TRADEMARK, OR COPYRIGHT INFRINGEMENT. 
#  
# This material has been approved for public release and unlimited distribution except as restricted below. 
#  
# Internal use:* Permission to reproduce this material and to prepare derivative works from this material for internal 
# use is granted, provided the copyright and "No Warranty" statements are included with all reproductions and 
# derivative works. 
#  
# External use:* This material may be reproduced in its entirety, without modification, and freely distributed in 
# written or electronic form without requesting formal permission. Permission is required for any other external and/or 
# commercial use. Requests for permission should be directed to the Software Engineering Institute at 
# permission@sei.cmu.edu. 
#  
# * These restrictions do not apply to U.S. government entities. 
#  
# Carnegie Mellon(r), CERT(r) and CERT Coordination Center(r) are registered marks of Carnegie Mellon University. 
#  
# DM-0000632 
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
        # The cleanest way to do things is probably to remove patterns after
        # the test, but we don't have that functionality.  For now just create
        # one pattern to avoid confusion, but do it by hacking in a global
        # variable

        global firstTime

        if not firstTime:
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
        # sleep a small amount
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
        scenes.  We simply perform a query in the normal matter and assure
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
        "Run integration test for assuring unordered sources are ordered by "
        "the system",
        sys.argv)
    unittest.main()
