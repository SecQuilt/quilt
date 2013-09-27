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
import re

import quilt_data
import quilt_test_core


firstTime = True


class SemanticsTestcase(unittest.TestCase):
    # TODO see ISSUE008  We want to move this to test_core when there is a
    # less hacky way to do it
    def check_query_and_get_results(self, submitStdout):
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

        christian_holidays = quilt_test_core.get_source_name(
            "christian_holidays")
        secular_holidays = quilt_test_core.get_source_name(
            "secular_holidays")

        #TODO REad the pattern id from the std output then query that one
        # See ISSUE007 and ISSUE008
        quilt_test_core.call_quilt_script('quilt_define.py', [
            'source("' + secular_holidays + '","grep")',
            '-n', 'semantics_one_source',
            '-v', 'WHICH_HOLIDAY',
            '-v', 'UNUSED',
            '-m', 'WHICH_HOLIDAY', secular_holidays, 'grep', 'GREP_ARGS',
            '-m', 'UNUSED', christian_holidays, 'grep', 'GREP_ARGS'
        ])


        #TODO REad the pattern id from the std output then query that one
        # See ISSUE007 and ISSUE008
        quilt_test_core.call_quilt_script('quilt_define.py', [
            'at(source("' + christian_holidays + '","grep"))==12',
            '-n', 'semantics_equals_literal'
        ])

        #TODO REad the pattern id from the std output then query that one
        # See ISSUE007 and ISSUE008
        quilt_test_core.call_quilt_script('quilt_define.py', [
            "concurrent(source('" + christian_holidays + "','grep')," +
            "source('" + secular_holidays + "','grep'))",
            '-n', 'semantics_concurrent'])

        #TODO REad the pattern id from the std output then query that one
        # See ISSUE007 and ISSUE008
        quilt_test_core.call_quilt_script('quilt_define.py', [
            "concurrent(source('" + christian_holidays + "','grep')," +
            "source('" + secular_holidays + "','grep')['major']=='True')",
            '-n', 'semantics_nested'])


        #TODO REad the pattern id from the std output then query that one
        # See ISSUE007 and ISSUE008
        quilt_test_core.call_quilt_script('quilt_define.py', [
            'follows(1,' +
            "source('" + secular_holidays + "','grep')," +
            "source('" + christian_holidays + "','grep'))",
            '-n', 'semantics_follows'
        ])

        #TODO REad the pattern id from the std output then query that one
        # See ISSUE007 and ISSUE008
        quilt_test_core.call_quilt_script('quilt_define.py', [
            'until(' +
            "source('" + secular_holidays + "','grep')," +
            "source('" + christian_holidays + "','grep'))",
            '-n', 'semantics_until'
        ])

        small_numbers = quilt_test_core.get_source_name(
            "small_numbers")
        med_numbers = quilt_test_core.get_source_name(
            "med_numbers")

        #TODO REad the pattern id from the std output then query that one
        # See ISSUE007 and ISSUE008
        quilt_test_core.call_quilt_script('quilt_define.py', [
            'qand(' +
            "source('" + small_numbers + "','grep')," +
            "source('" + med_numbers + "','grep'))",
            '-n', 'semantics_qand'
        ])

        #TODO REad the pattern id from the std output then query that one
        # See ISSUE007 and ISSUE008
        quilt_test_core.call_quilt_script('quilt_define.py', [
            'qor(' +
            "source('" + small_numbers + "','grep')," +
            "source('" + med_numbers + "','grep'))",
            '-n', 'semantics_qor'
        ])

    def test_one_source(self):
        """
        This test assures that a simple case of semantics is tested
        where only one thing is referenced, the results of one source
        query.  Additionally a red herring second variable is specified
        in the pattern, but is not mentioned in the pattern's code, only 
        in the query's variables.  We want to make sure quilt is smart
        enough not to make a useless source query.  An additional query
        is made on the pattern where the second unused variable is
        given a value.  Quilt should still be smart enough not to run
        this source query because it is not mentioned in the pattern code
        """

        # issue a valid query
        o = str(quilt_test_core.call_quilt_script('quilt_submit.py', [
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
        o = str(quilt_test_core.call_quilt_script('quilt_submit.py', [
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
        o = str(quilt_test_core.call_quilt_script('quilt_submit.py', [
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
        o = str(quilt_test_core.call_quilt_script('quilt_submit.py', [
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
        o = str(quilt_test_core.call_quilt_script('quilt_submit.py', [
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
        o = str(quilt_test_core.call_quilt_script('quilt_submit.py', [
            'semantics_follows', '-y'
        ]))

        # Check results
        # call quilt_history query_id
        # capture stdout, assure good exitcode
        o = self.check_query_and_get_results(o)

        # assure output contains ashwednesday
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
        o = str(quilt_test_core.call_quilt_script('quilt_submit.py', [
            'semantics_until', '-y'
        ]))

        # Check results
        # call quilt_history query_id
        # capture stdout, assure good exitcode
        o = self.check_query_and_get_results(o)

        # assure output contains ashwednesday
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


    def test_qand(self):
        """
        This test assures the operation of the 'qand' quilt language
        and function.  It uses a pattern which selects small and medium
        number sources.  it 'quand's' them together which takes the
        intersection.  The result should be the only number where the two
        sets of numbers intersect
        """

        # issue the query using the qand pattern
        o = str(quilt_test_core.call_quilt_script('quilt_submit.py', [
            'semantics_qand', '-y'
        ]))
        # assure the query is successful, and get results
        o = self.check_query_and_get_results(o)

        # check that results contain one correct number
        occurences = (
            len([m.start() for m in re.finditer(
                "{'timestamp': 10}", o)]))
        self.assertTrue(occurences == 1)
        # check than number from LHS is not in intersection

        occurences = (
            len([m.start() for m in re.finditer(
                "{'timestamp': 9}", o)]))
        self.assertTrue(occurences == 0)

    def test_qor(self):
        """
        This test assures the operation of the 'qor' quilt language
        or function.  It uses a pattern which selects small and medium
        number sources.  it 'qor's' them together which takes the
        union.  The result should be the contents of the union of the
        two input sets
        """

        # issue the query using the qor pattern
        o = str(quilt_test_core.call_quilt_script('quilt_submit.py', [
            'semantics_qor', '-y'
        ]))
        # assure the query is successful, and get results
        o = self.check_query_and_get_results(o)

        # check that results contain one number only in small
        occurences = (
            len([m.start() for m in re.finditer(
                "{'timestamp': 9}", o)]))
        self.assertTrue(occurences == 1)

        # check that results contain one number only in med
        occurences = (
            len([m.start() for m in re.finditer(
                "{'timestamp': 20}", o)]))
        self.assertTrue(occurences == 1)
        # check that results contain one number only from both
        occurences = (
            len([m.start() for m in re.finditer(
                "{'timestamp': 10}", o)]))
        self.assertTrue(occurences == 1)


if __name__ == "__main__":
    quilt_test_core.unittest_main_helper(
        "Run integration test for semantic processing", sys.argv)
    unittest.main()
