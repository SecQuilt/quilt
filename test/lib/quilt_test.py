#!/usr/bin/env python
import os
import sys
import logging
import quilt_core
import argparse
import quilt_test_core
import subprocess
import atexit
from daemon import runner
import time
import glob
import unittest
import lockfile
import pprint

# set environment variable for testing configuration directory
cfgdir = quilt_test_core.get_test_cfg_dir()
os.environ[quilt_core.QuiltConfig.QUILT_CFG_DIR_VAR] = cfgdir 

def at_exit():

    logging.info("Thank you for testing with quilt_test")

def filename_to_modulename(scriptsHome,filename):
    if filename.startswith(scriptsHome):
            filename = filename.replace(scriptsHome,'',1)
    else:
        raise Exception('Unexpected filename: ' + filename)

    if filename[0] == '/':
        filename = filename[1:]

    filename = filename.replace('/','.')
    filename = filename[:-3]
    return filename

class QuiltTest(object):
        
    def main(self, args):

        # assemble the filename for query master
        quilt_lib_dir = quilt_test_core.get_quilt_lib_dir()
        quilt_reg_file = os.path.join(quilt_lib_dir,'quilt_registrar.py')
        quilt_qmd_file = os.path.join(quilt_lib_dir,'quilt_qmd.py')

        # start the query master daemon
        atexit.register(at_exit)



        # the directory containing test scripts        
        quilt_test_lib_dir = quilt_test_core.get_quilt_test_lib_dir()

        # Repeat Forever if necessarry
        while True:
            # read sleep value again in case user changed
            cfg = quilt_core.QuiltConfig()
            logging.info("Begin Itegration Testing iteration")
            
            # Read quilt config (should be quilt test config)
            cfg = quilt_core.QuiltConfig()
            # access value "testing", "includes"
            # TODO fix eval security problem
            testGlobs = eval(cfg.GetValue(
                "testing", "includes", "['*_testcase.py']"))
            logging.debug('testing globs are: ' + str(testGlobs))
       
            # value part of the dictionary is not really used, set may 
            # be more suitable
            testFiles = {}
            # iterate each glob in inclusions, add files to use as tests
            for testGlob in testGlobs:
                logging.debug('looking for testGlob: ' + str(testGlob) + 
                    ", in: " + str(quilt_test_lib_dir))
                fullGlob = os.path.join(quilt_test_lib_dir,testGlob)
                logging.debug('looking for fullGlob: ' + fullGlob)
                curFiles = glob.glob(fullGlob)
                logging.debug('found test files: ' + str(curFiles))
                for curFile in curFiles:
                    testFiles[curFile] = "Specified"
            
            # load unit tests from those files
            tests = []
            for testFile,testStatus in testFiles.items():
                moduleName = filename_to_modulename(
                    quilt_test_lib_dir, testFile)
                curtest = unittest.defaultTestLoader.loadTestsFromName(
                    moduleName) 
                tests.append(curtest)
                testFiles[testFile] = "Loaded"
            
            # run the tests
            testSuite = unittest.TestSuite(tests)
            runner = unittest.TextTestRunner().run(testSuite)

            logging.info("End Itegration Testing iteration")

            # raise exception if tests failed
            if (not runner.wasSuccessful()):
                exit(1)
            

            # sleep before beginning
            cfg = quilt_core.QuiltConfig()
            sleepSecs = int(cfg.GetValue("testing","sleep","0"))
            if sleepSecs <= 0:
                break
            logging.debug("sleeping before next iteration")
            time.sleep(sleepSecs)

def main(argv):
    
    # setup command line interface
    parser =  quilt_core.main_helper('qtst',"""Provide an 
        Integration Testing Platform.""",argv)
    QuiltTest().main(parser.parse_args())

if __name__ == "__main__":        
    main(sys.argv[1:])
