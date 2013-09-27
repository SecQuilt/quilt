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
import os, sys
import logging
import subprocess
import fcntl
import time


def log_line(line, prefix='', postfix='', seperator=' ',
             loglevel='logging.DEBUG'):
    """Output a line to the log"""
    o = []
    if prefix:
        o.append(prefix)

    o.append(line[:-1])

    if postfix:
        o.append(postfix)

    logging.log(eval(loglevel), seperator.join(o))


def log_stream(stream, prefix='', postfix='', seperator=' ',
               loglevel='logging.DEBUG'):
    """output the specified stream to the log"""
    for line in stream:
        log_line(line, prefix, postfix, seperator, loglevel)


STDOUT = 1
EXITCODE = 2


def log_process(
        process, prefix='', postfix='', seperator=' ', loglevel='logging.DEBUG',
        whichReturn=STDOUT, logToPython=True, outFunc=None, outObj=None):
    """perform non blocking streaming of output from stderr and stdout of the
     specified process to the log"""

    # because it needs to be convenient to grab the stdout of a process (like for
    # short cmd line tools) and because we wanted the calling function's logic
    # to be simpler, when not requesting to log through python, we still run through
    # this function.

    # TODO
    # Wow, it was a it of a pain to get streaming IO to work.  The correct way to do it is probably
    # to start a thread.  But if you ask me the python io api is misleading.  This solution was
    # gleaned from stack overflow, it relies on catching exceptions, which is always a sign of
    # a bad design, but it is working, and it took several hours

    out = process.stdout
    err = process.stderr

    #convert the streams to non blocking (warning unix specific)
    fl = fcntl.fcntl(out.fileno(), fcntl.F_GETFL)
    fcntl.fcntl(out, fcntl.F_SETFL, fl | os.O_NONBLOCK)

    fl = fcntl.fcntl(err.fileno(), fcntl.F_GETFL)
    fcntl.fcntl(err, fcntl.F_SETFL, fl | os.O_NONBLOCK)

    grabOutput = whichReturn == STDOUT
    s = ''
    exitCode = -1

    while True:
        somethingHappened = False
        try:
            out.flush()
            lineos = out.readline()
            handled = False
            if grabOutput:
                s += lineos
                handled = True
            if lineos:
                if logToPython:
                    log_line(lineos, prefix + ':stdout', postfix, seperator,
                             loglevel)
                    handled = True

                if outFunc is not None:
                    outFunc(lineos[:-1], outObj)
                    handled = True

                if not handled:
                    print lineos[:-1]

                somethingHappened = True

        except IOError:
            pass

        try:
            err.flush()
            linees = err.readline()
            if linees:
                if logToPython:
                    log_line(linees, prefix + ':stderr', postfix, seperator,
                             loglevel)
                else:
                    print >> sys.stderr, linees[:-1]

                somethingHappened = True


        except IOError:
            pass

        exitCode = process.poll()
        if (exitCode is not None) and (not somethingHappened):
            break

        if not somethingHappened:
            time.sleep(.1)
        else:
            sys.stderr.flush()
            sys.stdout.flush()

    if grabOutput:
        return s[:-1]

    return exitCode


def run_process(cmd, shell=False, whichReturn=EXITCODE, checkCall=True,
                logToPython=True, outFunc=None, outObj=None):
    """run the specified process and wait for completion, throw exception if nonzero exit occurs, log output of process to the logging module, return stdout as string"""
    if type(cmd) != str:
        logging.debug("Executing: " + str(cmd)[1:-1].replace(',', ''))
    else:
        logging.debug("Executing: " + cmd)

    p = subprocess.Popen(cmd, shell=shell, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    o = log_process(p, whichReturn=whichReturn,
                    logToPython=logToPython, outFunc=outFunc, outObj=outObj)
    exitCode = p.poll()
    if checkCall and exitCode != 0:
        raise RuntimeError(
            'cmd: ' + str(cmd) + ' returned non zero exit code: ' + str(
                exitCode))
        #TODO when we run out of memory, make it optional to return a giant std out
    return o


def run_process_lite(cmd, shell=False, checkCall=True,
                     outFunc=None, outObj=None):
    logging.debug('run_process {begin}' + str(cmd) + '{end}')
    p = subprocess.Popen(cmd, shell=shell, stdout=subprocess.PIPE)

    exitCode = None
    while True:
        exitCode = p.poll()
        p.stdout.flush()
        oline = p.stdout.readline()

        if outFunc is not None and oline is not None and oline != '':
            outFunc(oline[:-1], outObj)
        if exitCode is not None:
            break

    if checkCall and exitCode != 0:
        raise RuntimeError(
            'cmd: ' + str(cmd) + ' returned non zero exit code: ' + str(
                exitCode))
    return exitCode


def shell_cmd_output_iter(command):
    p = subprocess.Popen(command, shell=True,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)
    # noinspection PyUnusedLocal
    for line in iter(p.stdout.readline, ''):
        return iter(p.stdout.readline, b'')
