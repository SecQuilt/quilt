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
import logging
import quilt_core
import quilt_test_core
import atexit
import time
import glob
import unittest

# set environment variable for testing configuration directory
cfgdir = quilt_test_core.get_test_cfg_dir()
os.environ[quilt_core.QuiltConfig.QUILT_CFG_DIR_VAR] = cfgdir


def at_exit():
    logging.info("Thank you for testing with quilt_test")


def filename_to_modulename(scriptsHome, filename):
    if filename.startswith(scriptsHome):
        filename = filename.replace(scriptsHome, '', 1)
    else:
        raise Exception('Unexpected filename: ' + filename)

    if filename[0] == '/':
        filename = filename[1:]

    filename = filename.replace('/', '.')
    filename = filename[:-3]
    return filename


class QuiltTest(object):
    # noinspection PyUnusedLocal
    def main(self, args):
        args = args # reserved for future use pylint!

        # start the query master daemon
        atexit.register(at_exit)



        # the directory containing test scripts        
        quilt_test_lib_dir = quilt_test_core.get_quilt_test_lib_dir()

        # Repeat Forever if necessary
        while True:
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
                fullGlob = os.path.join(quilt_test_lib_dir, testGlob)
                logging.debug('looking for fullGlob: ' + fullGlob)
                curFiles = glob.glob(fullGlob)
                logging.debug('found test files: ' + str(curFiles))
                for curFile in curFiles:
                    testFiles[curFile] = "Specified"

            # load unit tests from those files
            tests = []
            for testFile in testFiles.keys():
                moduleName = filename_to_modulename(
                    quilt_test_lib_dir, testFile)
                curtest = unittest.defaultTestLoader.loadTestsFromName(
                    moduleName)
                tests.append(curtest)
                testFiles[testFile] = "Loaded"

            # run the tests
            testSuite = unittest.TestSuite(tests)
            myrunner = unittest.TextTestRunner().run(testSuite)

            logging.info('End Itegration Testing iteration')

            # raise exception if tests failed
            if not myrunner.wasSuccessful():
                exit(1)


            # sleep before beginning
            cfg = quilt_core.QuiltConfig()
            sleepSecs = int(cfg.GetValue("testing", "sleep", "0"))
            if sleepSecs <= 0:
                break
            logging.debug("sleeping before next iteration")
            time.sleep(sleepSecs)


def main(argv):
    # setup command line interface
    parser = quilt_core.main_helper('qtst', """Provide an
        Integration Testing Platform.""", argv)
    QuiltTest().main(parser.parse_args())


if __name__ == "__main__":
    main(sys.argv[1:])
