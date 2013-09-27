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
