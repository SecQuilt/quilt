#!/usr/bin/env python
# Copyright (c) 2013 Carnegie Mellon University.
# All Rights Reserved.
# Redistribution and use in source and binary forms, with or without 
# modification, are permitted provided that the following conditions are met:
# 1. Redistributions of source code must retain the above copyright notice, 
# this list of conditions and the following acknowledgments and disclaimers.
# 2. Redistributions in binary form must reproduce the above copyright notice, 
# this list of conditions and the following acknowledgments and disclaimers in 
# the documentation and/or other materials provided with the distribution.
# 3. Products derived from this software may not include "Carnegie Mellon 
# University," "SEI" and/or "Software Engineering Institute" in the name of 
# such derived product, nor shall "Carnegie Mellon University," "SEI" and/or 
# "Software Engineering Institute" be used to endorse or promote products 
# derived from this software without prior written permission. For written 
# permission, please contact permission@sei.cmu.edu.
# Acknowledgments and disclaimers:
# This material is based upon work funded and supported by the Department of 
# Defense under Contract No. FA8721-05-C-0003 with Carnegie Mellon University 
# for the operation of the Software Engineering Institute, a federally funded 
# research and development center. 
#  
# Any opinions, findings and conclusions or recommendations expressed in this 
# material are those of the author(s) and do not necessarily reflect the views 
# of the United States Department of Defense. 
#  
# NO WARRANTY. THIS CARNEGIE MELLON UNIVERSITY AND SOFTWARE ENGINEERING 
# INSTITUTE MATERIAL IS FURNISHEDON AN "AS-IS" BASIS.  CARNEGIE MELLON 
# UNIVERSITY MAKES NO WARRANTIES OF ANY KIND, EITHER EXPRESSED OR IMPLIED, AS 
# TO ANY MATTER INCLUDING, BUT NOT LIMITED TO, WARRANTY OF FITNESS FOR PURPOSE 
# OR MERCHANTABILITY, EXCLUSIVITY, OR RESULTS OBTAINED FROM USE OF THE 
# MATERIAL. CARNEGIE MELLON UNIVERSITY DOES NOT MAKE ANY WARRANTY OF ANY KIND 
# WITH RESPECT TO FREEDOM FROM PATENT, TRADEMARK, OR COPYRIGHT INFRINGEMENT. 
#  
# This material has been approved for public release and unlimited distribution.
#
# Carnegie Mellon(r), CERT(r) and CERT Coordination Center(r) are registered 
# marks of Carnegie Mellon University. 
#  
# DM-0000632
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
