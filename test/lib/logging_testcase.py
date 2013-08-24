#!/usr/bin/env python
import os
import sys
import unittest
import sei_core
import quilt_test_core
import random
from os import path


class LoggingTestcase(unittest.TestCase):
    def test_logging(self):
        """
        calls quilt_status with an invalid key and a specification
        for a log file, then Checks that the log file exists
        """

        # create a randomize log file name in /tmp
        rnum = random.randint(10000, 99999)
        logname = "log" + str(rnum) + ".log"
        logpath = os.path.join('/tmp', logname)

        # call quilt submit -y logging_not_a_pattern -l DEBUG --log-file
        #   random_log_file
        queryStr = 'logging_not_a_pattern'
        quilt_test_core.call_quilt_script('quilt_submit.py',
                                          [queryStr, '--log-file', logpath],
                                          whichReturn=sei_core.EXITCODE,
                                          checkCall=False)

        # check resulting log file contains "logging_not_a_pattern"
        exists = 0 == sei_core.run_process(
            'grep "' + queryStr + '" "' + logpath + '" > /dev/null', shell=True,
            whichReturn=sei_core.EXITCODE, checkCall=False)
        self.assertTrue(exists)

        # delete the temp log file
        os.remove(logpath)


if __name__ == "__main__":
    quilt_test_core.unittest_main_helper(
        "Run integration test that verifies at least some logic functionality"
        " is behaving as desired",
        sys.argv)
    unittest.main()
