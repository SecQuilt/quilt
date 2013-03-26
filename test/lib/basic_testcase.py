#!/usr/bin/env python
import os
import sys
import logging
import unittest
import subprocess
import quilt_test_core
import sei_core
import quilt_core

class BasicTestcase(unittest.TestCase):
    def test_basic_status(self):

        quilt_lib_dir = quilt_test_core.get_quilt_lib_dir()
        # assemble the filename for query master
        quilt_status_file = os.path.join(quilt_lib_dir,'quilt_status.py')

        # call quilt_status (check return code, capture output)
        out = sei_core.run_process([quilt_status_file,'-l','DEBUG'],
            whichReturn=sei_core.STDOUT)
        
        # assure that name of the test source manager appears in the
        # output, test source name specified in testing 
        # config smd.d dir
        cfg = quilt_core.QuiltConfig()
        smgrs = cfg.GetSourceManagers()
        for smgr in smgrs:
            self.assertTrue(smgr in out)
        
        # check qmd to be sure that all quilt_status's have 
        # unregistered when process exits
        with sei_core.GetQueryMasterProxy(cfg) as qm:
            qs = qm.GetClients("QuiltStatus")
            self.assertTrue( len(qs) == 0 )

        
if __name__ == "__main__":
    quilt_test_core.unittest_main_helper("Run the most basic of test cases",sys.argv)
    unittest.main()
