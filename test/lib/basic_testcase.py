#!/usr/bin/env python
import os
import sys
import logging
import unittest
import subprocess
import quilt_test_core

class BasicTestcase(unittest.TestCase):
    def test_basic_status(self):

        quilt_lib_dir = quilt_test_core.get_quilt_lib_dir()
        # assemble the filename for query master
        quilt_status_file = os.path.join(quilt_lib_dir,'quilt_status.py')

        logging.debug("Running: " + quilt_status_file)
        output = subprocess.check_call([quilt_status_file,'-l','DEBUG'])
        print 'TODO Finish, output is', str(output)

