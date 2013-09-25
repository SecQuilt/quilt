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
import re
from string import Template

firstTime = True


class DuplicateEventsTestcase(unittest.TestCase):
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
        even_numbers = quilt_test_core.get_source_name(
            "even_numbers")
        odd_numbers = quilt_test_core.get_source_name(
            "odd_numbers")

        # define template pattern code string
        followsTemplate = "follows(5, source('$EVEN','grep'),source('$ODD','grep'))"

        # replace EVEN and ODD variables in the template with full names
        replacements = {'EVEN': even_numbers, 'ODD': odd_numbers}
        followsTemplate = Template(followsTemplate)
        followsPatCode = followsTemplate.safe_substitute(replacements)


        # TODO REad the pattern id from the std output then query that one
        # See ISSUE007 and ISSUE008
        # call quilt_define with the pattern code and name query
        #   dups_follows
        quilt_test_core.call_quilt_script('quilt_define.py', ['-n',
            'dups_follows', followsPatCode])

        # define template pattern code string
        concurrentTemplate = "concurrent(source('$EVEN','grep'),source('$EVEN','grep'), source('$EVEN','grep'))"

        # replace EVEN variable in the template with full source name
        concurrentTemplate = Template(concurrentTemplate)
        concurrentPatCode = concurrentTemplate.safe_substitute(replacements)

        # TODO REad the pattern id from the std output then query that one
        # See ISSUE007 and ISSUE008
        # call quilt_define with the pattern code and name query
        #   dups_concurrent

        quilt_test_core.call_quilt_script('quilt_define.py', ['-n',
            'dups_concurrent', concurrentPatCode])

    def contains_once(
            self,
            text_body,
            search_string):
        """
        Assert a test failure if search_string iff does not occur one time
        in the text_body
        """

        if (text_body is None or text_body == '' or
                search_string is None or search_string == ''):
            raise Exception("Invalid string for use in contains_once")


        # use regular expression to count the number of occurrences
        # assert an error if it did not occur once
        occurences = (
            len([m.start() for m in re.finditer(
                search_string, text_body)]))
        self.assertTrue(occurences == 1)


    # TODO see ISSUE008  We want to move this to test_core when there is a
    # less hacky way to do it
    def check_query_and_get_results2(self, submitStdout):
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

    def test_follows(self):
        """
        submits the dups_follows pattern and assures that no duplicates
        are reported Also assures that correct values are placed in
        results
        """

        # issue a valid query
        # Assure proper execution, and get results from quilt_history
        o = str(quilt_test_core.call_quilt_script('quilt_submit.py', [
            '-y', 'dups_follows']))

        o = self.check_query_and_get_results2(o)

        self.contains_once(o, "{'timestamp': 3}")
        self.contains_once(o, "{'timestamp': 5}")
        self.contains_once(o, "{'timestamp': 7}")
        self.contains_once(o, "{'timestamp': 9}")


    def test_concurrent(self):
        """
        submits the dups_concurrent pattern and assures that no duplicates
        are reported Also assures that correct values are placed in
        results
        """

        # issue a valid query
        # Assure proper execution, and get results from quilt_history
        o = str(quilt_test_core.call_quilt_script('quilt_submit.py', [
            '-y', 'dups_concurrent']))

        o = self.check_query_and_get_results2(o)

        self.contains_once(o, "{'timestamp': 2}")
        self.contains_once(o, "{'timestamp': 4}")
        self.contains_once(o, "{'timestamp': 6}")
        self.contains_once(o, "{'timestamp': 8}")
        self.contains_once(o, "{'timestamp': 10}")


if __name__ == "__main__":
    quilt_test_core.unittest_main_helper(
        "Run integration test for duplicate event prevention", sys.argv)
    unittest.main()
