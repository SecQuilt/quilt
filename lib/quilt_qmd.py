#!/usr/bin/env python
import os
import logging
from daemon import runner
import quilt_core

class Qmd(object):
    def __init__(self):
        self.stdin_path = '/dev/null'
        self.stdout_path = '/dev/tty'
        self.stderr_path = '/dev/tty'
        self.pidfile_path =  '/tmp/qmd.pid'
        self.pidfile_timeout = 5
    def run(_self):
        pass

qmd = Qmd()
quilt_core.query_master_client_main_helper(
    { Qmd() : "asd" }
