#!/usr/bin/env python
import sys
import os
import argparse
import quilt_core
import sei_core
import time
import logging
import pickle

def get_quilt_test_lib_dir():
    """grab the location of quilt test scritps"""
    return os.path.dirname(__file__)

def get_quilt_lib_dir():
    # grab the location of quilt
    quilt_core_file = quilt_core.__file__
    return os.path.dirname(quilt_core_file)

def get_test_cfg_dir():
    """Get the location of the quilt testing configuration file"""
    d = os.path.dirname(__file__)
    d = os.path.dirname(d)
    d = os.path.join(d,'etc')
    d = os.path.join(d,'quilt')
    return d
    


def unittest_main_helper(description='',argv=sys.argv):
    # setup the argument parser
    parser = argparse.ArgumentParser(description)
    parser.add_argument('-l','--log-level',nargs='?',
            help='logging level (DEBUG,INFO,WARN,ERROR) default: WARN')
    args,unknown = parser.parse_known_args(argv)
    unknown=unknown # pylint thing

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

def retrieve_objects(script_output):
    objs = []
    fileNames = script_output.split("\n")
    for fileName in fileNames:
        try:
            fileName = fileName.strip("'")
            fp = open(fileName, "rb")
            while 1:
                try:
                    obj = pickle.load(fp)
                    objs.append(obj)
                except (EOFError):
                    break
        except (IOError):
            pass
    return objs

def call_quilt_script( scriptName, args = None, checkCall=True):
    """
    returns the stdoutput of the script, checks for bad error code and
    throws exception
        scriptName          # base filename of the script in the quilt lib
                            #   directory
        args                # arguments to pass to the script):
    """
    if args == None:
        args = []

    quilt_lib_dir = get_quilt_lib_dir()
    # assemble the filename for query master
    script_file = os.path.join(quilt_lib_dir,scriptName)

    # call quilt_status (check return code, capture output)
    args = [script_file, '-l', 'DEBUG'] + args
    # print("Executing under test: " + str(args))
    out = sei_core.run_process(args,
        whichReturn=sei_core.STDOUT, logToPython=False,
        checkCall=checkCall)

    return out

def log_objs(header, objs):
    for obj in objs:
        try:
            logging.debug(header + ": " + str(obj))
        except:
            pass

def pattern_found(pattern, dictionary):
    try:
        for name, value in dictionary.items():
            if name.startswith(pattern):
                return True
    except:
        pass
    return False

def get_pattern(pattern, dictionary):
    try:
        for name, value in dictionary.items():
            if name.startswith(pattern):
                return value
    except:
        pass
    return ''

def get_pattern_occurrences2(pattern, dictionary):
    occurrences = 0
    try:
        if 'results' in dictionary:
            results = dictionary['results']
            for result in results:
                if 'holiday' in result:
                    holiday = result['holiday']
                    if holiday == pattern:
                        occurrences = occurrences + 1
    except:
        pass
    return occurrences

def get_pattern_occurrences(pattern, dictionary):
    occurrences = 0
    try:
        if 'results' in dictionary:
            results = dictionary['results']
            for result in results:
                if 'content' in result:
                    content = result['content']
                    if pattern in content:
                        occurrences = occurrences + 1
    except:
        pass
    return occurrences

def get_source_name(partialName):
    srcName = ''
    # call quilt status and parse out the name of the syslog source
    # fix with ISSUE008
    cmd1 = os.path.join(get_quilt_lib_dir(), "quilt_status.py")
    o = sei_core.run_process(cmd1, whichReturn=sei_core.STDOUT, 
        logToPython=False, shell=True)
    objs = retrieve_objects(o)
    for obj in objs:
        try:
            objasdict = dict(obj)
            for name, value in objasdict.items():
                if name.startswith(partialName):
                    srcName = name
                    break
        except:
            pass

    #cmd2 = ("cat " + fileName + " | grep " + partialName + 
    #    " | sed 's/.*" + partialName + "/" + partialName + 
    #    "/' | cut -d\"'\" -f1 | uniq ")
    #" | head -n 1 | awk '{print $1}' | sed  -e \"s/{'//\" -e \"s/'://\" -e \"s/'//g\" ")
    #logging.debug("get_source_name cmd2 = " + cmd2)
    #srcName = sei_core.run_process(cmd2, whichReturn=sei_core.STDOUT, 
    #    logToPython=False, shell=True)

    return srcName

def sleep_small():
    time.sleep(0.5)
def sleep_medium():
    for i in range(4): 
        i=i
        sleep_small() 
def sleep_large():
    for i in range(4): 
        i=i
        sleep_medium() 
