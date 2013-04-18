#!/usr/bin/env python
import sys
import os
import argparse
import logging
import quilt_core
import subprocess
import sei_core

def get_quilt_test_lib_dir():
    """grab the location of quilt test scritps"""
    return os.path.dirname(__file__)

def get_quilt_lib_dir():
    # grab the location of quilt
    quilt_core_file = quilt_core.__file__
    return os.path.dirname(quilt_core_file)

def get_test_cfg_dir():
    """Get the location of the quilt testing configuration file"""
    d = os.path.dirname(__file__);
    d = os.path.dirname(d);
    d = os.path.join(d,'etc');
    d = os.path.join(d,'quilt');
    return d
    


def unittest_main_helper(description='',argv=sys.argv):
    # setup the argument parser
    parser = argparse.ArgumentParser(description)
    parser.add_argument('-l','--log-level',nargs='?',
            help='logging level (DEBUG,INFO,WARN,ERROR) default: WARN')
    args,unknown = parser.parse_known_args(argv)

    level = None
    if (args.log_level is not None):
        # if log level was specified, set the log level
        level = args.log_level

        # now strip off the log arguments before we pass on to unit 
        # test main
        slot = sys.argv.index('-l')
        if (slot is -1):
            slot = sys.argv.index('--log-level')
        sys.argv.pop(slot)
        sys.argv.pop(slot)

    quilt_core.common_init(os.path.basename(sys.argv[0]), level)

def call_quilt_script( scriptName, args = [], checkCall=True):
    """
    returns the stdoutput of the script, checks for bad error code and
    throws exception
        scriptName          # base filename of the script in the quilt lib
                            #   directory
        args                # arguments to pass to the script):
    """
    
    quilt_lib_dir = get_quilt_lib_dir()
    # assemble the filename for query master
    script_file = os.path.join(quilt_lib_dir,scriptName)

    # call quilt_status (check return code, capture output)
    args = [script_file, '-l', 'DEBUG'] + args
    out = sei_core.run_process(args,
        whichReturn=sei_core.STDOUT, logToPython=False,
        checkCall=checkCall)

    return out
