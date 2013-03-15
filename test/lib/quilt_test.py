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

# We want a debug log level for testing
logging.basicConfig(level=logging.DEBUG)

def at_exit():

    logging.info("Testing daemon is ending, stopping Query Master")
    quilt_lib_dir = quilt_test_core.get_quilt_lib_dir()
    # assemble the filename for query master
    quilt_reg_file = os.path.join(quilt_lib_dir,'quilt_registrar.py')
    quilt_qmd_file = os.path.join(quilt_lib_dir,'quilt_qmd.py')
    # stop the query master
    ret = subprocess.call([quilt_qmd_file, 'stop' ])
    if (ret != 0):
        logging.warning("Query master daemon exited with code: " + 
            str(ret))

    ret = subprocess.call([quilt_reg_file, 'stop' ])
    if (ret != 0):
        logging.warning("Query registrar daemon exited with code: " + 
            str(ret))

class Qtd(quilt_core.QuiltDaemon):
    def __init__(self):
        self.setup_process('quilt_test')

    def run(_self):
        
        quilt_lib_dir = quilt_test_core.get_quilt_lib_dir()
        # assemble the filename for query master
        quilt_reg_file = os.path.join(quilt_lib_dir,'quilt_registrar.py')
        quilt_qmd_file = os.path.join(quilt_lib_dir,'quilt_qmd.py')
        
        # start the query master daemon
        atexit.register(at_exit)
        logging.debug('Integration test starting: ' + quilt_reg_file)
        subprocess.check_call([quilt_reg_file, 'start'])
        logging.debug('Integration test starting: ' + quilt_qmd_file)
        subprocess.call([quilt_qmd_file, 'start'])
        
        while True:
            time.sleep(5)
            logging.info("Testing...")

        


daemon_runner = runner.DaemonRunner(Qtd())
daemon_runner.do_action()
