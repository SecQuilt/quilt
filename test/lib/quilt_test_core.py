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
import sys
import os
import argparse
import time

import quilt_core
import sei_core


def get_quilt_test_lib_dir():
    """grab the location of quilt test script's"""
    return os.path.dirname(__file__)


def get_quilt_lib_dir():
    # grab the location of quilt
    quilt_core_file = quilt_core.__file__
    return os.path.dirname(quilt_core_file)


def get_test_cfg_dir():
    """Get the location of the quilt testing configuration file"""
    d = os.path.dirname(__file__)
    d = os.path.dirname(d)
    d = os.path.join(d, 'etc')
    d = os.path.join(d, 'quilt')
    return d


def unittest_main_helper(description='', argv=sys.argv):
    # setup the argument parser
    parser = argparse.ArgumentParser(description)
    parser.add_argument('-l', '--log-level', nargs='?',
        help='logging level (DEBUG,INFO,WARN,ERROR) default: WARN')
    args, unknown = parser.parse_known_args(argv)
    # noinspection PyUnusedLocal
    unknown = unknown # pylint thing

    level = None
    if args.log_level is not None:
        # if log level was specified, set the log level
        level = args.log_level

        # now strip off the log arguments before we pass on to unit 
        # test main
        slot = sys.argv.index('-l')
        if slot is -1:
            slot = sys.argv.index('--log-level')
        sys.argv.pop(slot)
        sys.argv.pop(slot)

    quilt_core.common_init(os.path.basename(sys.argv[0]), level)


def call_quilt_script(scriptName, args=None, checkCall=True,
                      whichReturn=sei_core.STDOUT):
    """
    returns the standard output of the script, checks for bad error code and
    throws exception
        scriptName          # base filename of the script in the quilt lib
                            #   directory
        args                # arguments to pass to the script):
    """
    if args is None:
        args = []

    quilt_lib_dir = get_quilt_lib_dir()
    # assemble the filename for query master
    script_file = os.path.join(quilt_lib_dir, scriptName)

    # call quilt_status (check return code, capture output)
    args = [script_file, '-l', 'DEBUG'] + args
    # print("Executing under test: " + str(args))
    out = sei_core.run_process(args,
        whichReturn=whichReturn, logToPython=False,
        checkCall=checkCall)

    return out


def get_source_name(partialName):
    # call quilt status and parse out the name of the syslog source
    # fix with ISSUE008
    cmd = os.path.join(get_quilt_lib_dir(), "quilt_status.py") + (
        " | grep " + partialName +
        " | head -n 1 | awk '{print $1}' | sed  -e \"s/{'//\" -e \"s/'://\" -e \"s/'//g\" ")
    srcName = sei_core.run_process(cmd, whichReturn=sei_core.STDOUT,
        logToPython=False, shell=True)
    return srcName


def sleep_small():
    time.sleep(0.5)


def sleep_medium():
    for i in range(4):
        # noinspection PyUnusedLocal
        i = i
        sleep_small()


def sleep_large():
    for i in range(4):
        # noinspection PyUnusedLocal
        i = i
        sleep_medium()

