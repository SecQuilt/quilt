#!/usr/bin/env python
import sys
import unittest
import quilt_data
import quilt_test_core
import re
from string import Template

firstTime = True


class OperatorsTestcase(unittest.TestCase):
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
        christian_holidays = quilt_test_core.get_source_name(
            "christian_holidays")
        secular_holidays = quilt_test_core.get_source_name(
            "secular_holidays")

        # define template pattern code string
        lessThanTemplate = (
            "(source('$SECULAR','grep')['paradesize'] < source('$CHRISTIAN', "
            "'grep')['paradesize'])['paradesize'] < 100")

        # replace EVEN and ODD variables in the template with full names
        replacments = {'CHRISTIAN': christian_holidays,
                       'SECULAR': secular_holidays}
        lessThanTemplate = Template(lessThanTemplate)
        patCode = lessThanTemplate.safe_substitute(replacments)

        # TODO REad the pattern id from the std output then query that one
        # See ISSUE007 and ISSUE008
        # call quilt_define with the pattern code and name query
        #   operators_lessthan
        quilt_test_core.call_quilt_script(
            'quilt_define.py', ['-n', 'operators_lessthan', patCode])

        # define template pattern code string
        template = (
            "(source('$SECULAR','grep')['paradesize'] $OPERATOR source("
            "'$CHRISTIAN', 'grep')['paradesize'])")

        template = Template(template)
        patCode = template.safe_substitute(replacments)

        # TODO REad the pattern id from the std output then query that one
        # See ISSUE007 and ISSUE008
        # call quilt_define with the pattern code and name query
        #   define a variable for which binary operator to use
        quilt_test_core.call_quilt_script(
            'quilt_define.py', ['-n', 'operators_comparefields', patCode,
                                '-v', 'OPERATOR'])

        # define template pattern code string
        template = (
            "(source('$SECULAR','grep')['paradesize'] $OPERATOR $VALUE")

        template = Template(template)
        patCode = template.safe_substitute(replacments)

        # TODO REad the pattern id from the std output then query that one
        # See ISSUE007 and ISSUE008
        # call quilt_define with the pattern code and name query
        #   define a variable for which binary operator to use
        #   define a variable for what value to use on RHS
        quilt_test_core.call_quilt_script('quilt_define.py',
            ['-n', 'operators_comparevalue', patCode, '-v', 'OPERATOR', '-v',
             "VALUE"])


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


def contains(
        self,
        text_body,
        search_string,
        noccurences):
    """
    Assert a test failure if search_string iff does not occur n times
    in the text_body
    """

    if (text_body is None or text_body == '' or
                search_string is None or search_string == ''):
        raise Exception("Invalid string for use in contains")

    # use regular expression to count the number of occurences
    # assert an error if it did not occur n times
    occurences = (
        len([m.start() for m in re.finditer(
            search_string, text_body)]))
    self.assertTrue(occurences == noccurences)


def test_lessthan(self):
    """
    submits the operators_lessthan pattern.  Checks that that the
    result only contains boxing day
    """

    # issue a valid query
    # Assure proper execution, and get results from quilt_history
    o = str(quilt_test_core.call_quilt_script('quilt_submit.py', [
        '-y', 'operators_lessthan']))

    o = self.check_query_and_get_results3(o)
    self.contains(o, 'boxingday', 1)
    self.contains(o, 'valentines', 0)


def test_compare_fields(self):
    """
    Test will submit a query for each comparison operator and verify
    results.  This will use a field on the lhs and rhs of the operator
    """

    # initialize a dictionary, where key's are the comparison operators
    # and value is a string that should appear in the results
    cases = {
        '<': 'boxingday',
        '<=': 'memorialday',
        '>': 'stpatricks',
        '>=': 'independenceday'
    }

    # for each key and value pair
    for key, value in cases:
        # submit the operators_comparefields query
        #   pass key as value for operator variable
        o = str(quilt_test_core.call_quilt_script('quilt_submit.py', [
            '-y', 'operators_comparefields', '-v', 'OPERATOR', key]))

        # Assure proper execution, and get results from quilt_history
        o = self.check_query_and_get_results3(o)
        # check results contain one instance of "value"
        self.contains(o, value, 1)


def test_compare_value(self):
    """
    Test will submit a query for each comparison operator and verify
    results.  This will use a field on the lhs and a value on the rhs
    of the operator
    """

    # initialize a dictionary, where key's are the comparison operators
    #   and value is a tuple.  The first element of the tuple is
    #   the value to use in the query code, and the second element
    #   of the tuple is the  string that should appear in the results
    cases = {
        '<': (11, 'boxingday'),
        '<=': (10, 'boxingday'),
        '>': (100000, 'stpatricks'),
        '>=': (100000, 'independenceday'),
        '==': (10000, 'newyears'),
        '!=': (100001, 'boxingday')
    }

    # for each key and value pair
    for operator, (value, answer) in cases:
        # submit the operators_comparefields query
        #   pass key as value for OPERATOR variable
        #   pass first element of value tuple as VALUE variable
        o = str(quilt_test_core.call_quilt_script('quilt_submit.py', [
            '-y', 'operators_comparefields', '-v', 'OPERATOR', operator,
            '-v', 'VALUE', value]))
        # Assure proper execution, and get results from quilt_history
        o = self.check_query_and_get_results3(o)
        # check results contain one instance of second part of value
        #   tuple
        self.contains(o, value, 1)


if __name__ == "__main__":
    quilt_test_core.unittest_main_helper(
        "Run integration test for duplicate event prevention", sys.argv)
    unittest.main()