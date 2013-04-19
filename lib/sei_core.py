#!/usr/bin/env python

import os, sys
import logging
import subprocess
import fcntl
import time

def log_line(line, prefix='', postfix='', seperator=' ', loglevel='logging.DEBUG'):
    """Output a line to the log"""
    o = []
    if prefix:
        o.append(prefix)

    
    o.append(line[:-1])

    if postfix:
        o.append(postfix)

    logging.log(eval(loglevel),seperator.join(o))

def log_stream(stream, prefix='', postfix='',seperator=' ',loglevel='logging.DEBUG'):
    """output the specified stream to the log"""
    for line in stream:
        log_line(line, prefix, postfix, seperator, loglevel)

STDOUT=1
EXITCODE=2

def log_process(
    process,prefix='', postfix='',seperator=' ',loglevel='logging.DEBUG',
    whichReturn=STDOUT,logToPython=True, outFunc=None):
    """perform nonblocking streaming of output from stderr and stdout of the specified process to the log"""

# because it needs to be conenient to grab the stdout of a process (like for
# short cmd line tools) and because we wanted the calling function's logic
# to be simpler, when not requesting to log through python, we still run through
# this function. 

# TODO
# Wow, it was a it of a pain to get streaming IO to work.  The correct way to do it is probably
# to start a thread.  But if you ask me the python io api is misleading.  This solution was
# gleaned from stack overflow, it reliese on catching exceptions, which is always a sign of
# a bad design, but it is working, and it took several hours

    out=process.stdout
    err=process.stderr

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
            handled=False
            if grabOutput:
                s += lineos;
                handled = True
            if lineos: 
                if logToPython:
                    log_line(lineos, prefix + ':stdout', postfix, seperator, loglevel)
                    handled=True

                if outFunc != None:
                    outFunc(lineos[:-1])
                    handled=True
                
                if not handled:
                    print lineos[:-1]

                somethingHappened = True

        except:
            pass

        try:
            err.flush()
            linees = err.readline()
            if linees :
                if logToPython:
                    log_line(linees, prefix + ':stderr', postfix, seperator, loglevel)
                else:
                    print >> sys.stderr, linees[:-1]

                somethingHappened = True


        except:
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
    logToPython=True, outFunc=None):
    """run the specified process and wait for completion, throw exception if nonzero exit occurs, log output of process to the logging module, return stdout as string"""
    logging.debug('run_process {begin}' + str(cmd) + '{end}')
    p = subprocess.Popen( cmd, shell=shell, stdout=subprocess.PIPE, stderr=subprocess.PIPE )
    o = log_process(p,whichReturn=whichReturn,
        logToPython=logToPython,outFunc=outFunc)
    exitCode = p.poll()
    if checkCall and exitCode != 0:
        raise RuntimeError('cmd: ' + str(cmd) + ' returned non zero exit code: ' + str(exitCode))
#TODO when we run out of memory, make it optional to return a giant std out
    return o
