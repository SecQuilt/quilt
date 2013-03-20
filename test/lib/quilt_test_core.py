#!/usr/bin/env python
import sys
import os
import argparse
import logging
import quilt_core

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
    

def unittest_main_helper(description=''):
    # setup the argument parser
    parser = argparse.ArgumentParser(description)
    parser.add_argument('-l','--log-level',nargs='?',
            help='logging level (DEBUG,INFO,WARN,ERROR)')
    args = parser.parse_args()

    if (args.log_level is not None):
        # if log level was specified, set the log level
        strlevel = 'logging.' + args.log_level
        logging.basicConfig(level=eval(strlevel))

        # now strip off the log arguments before we pass on to unit 
        # test main
        slot = sys.argv.index('-l')
        if (slot is -1):
            slot = sys.argv.index('--log-level')
        sys.argv.pop(slot)
        sys.argv.pop(slot)
    return args


