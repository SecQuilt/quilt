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
#REVIEW
import sys
import unittest
import quilt_test_core
import quilt_data
import re

firstTime = True


class BasicSourcesTestcase(unittest.TestCase):
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

        syslog = quilt_test_core.get_source_name("syslog")
        multisource = quilt_test_core.get_source_name("multipattern")

        #TODO REad the pattern id from the std output then query that one
        # See ISSUE007 and ISSUE008
        quilt_test_core.call_quilt_script('quilt_define.py', [
            '-n', "bigpattern",
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


    def test_multi_sources(self):
        """
        check multiple variables, and multiple patterns are functioning
        """

        # defaults specified in source patterns for SEARCHSTRING[4,6]
        o = str(quilt_test_core.call_quilt_script('quilt_submit.py', [
            'bigpattern',
            '-y',
            '-v', 'SEARCHSTRING1', "Occurs_1_time",
            '-v', 'SEARCHSTRING2', "Occurs_3_times",
            '-v', 'SEARCHSTRING3', "word-regexp",
            '-v', 'SEARCHSTRING5', "word-regexp"
        ]))
        # sleep a small amount
        quilt_test_core.sleep_large()

        # capture query_id from std out 
        a = o.index("Query ID is: ") + len(str("Query ID is: "))
        qid = o[a:]

        # issue a valid query

        # call quilt_history query_id
        o = quilt_test_core.call_quilt_script('quilt_history.py', [qid])
        # check it shows good state (completed)
        self.assertTrue(quilt_data.STATE_COMPLETED in o)

        # find some particulars in the results
        occurences = (
            len([m.start() for m in re.finditer(
                "Occurs_1_time", o)]))

        # have to +2 because the search variable 
        # and src query spec is also in the stdout
        self.assertTrue(occurences == 1 + 2)

        occurences = (
            len([m.start() for m in re.finditer(
                "Occurs_3_times", o)]))
        self.assertTrue(occurences == 1 + 4)


        # have no + 1, these are defaults set from src pattern

        occurences = (
            len([m.start() for m in re.finditer(
                "src default for pat2 occurs twice", o)]))
        self.assertTrue(occurences == 3)

        occurences = (
            len([m.start() for m in re.finditer(
                "src default for pat1 occurs once", o)]))
        self.assertTrue(occurences == 2)


if __name__ == "__main__":
    quilt_test_core.unittest_main_helper(
        "Run integration test for multiple basic source", sys.argv)
    unittest.main()
