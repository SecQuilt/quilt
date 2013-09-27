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
import sys
import logging
import quilt_core
import Pyro4
import query_master

# logging.basicConfig(level=logging.DEBUG)

class Qmd(quilt_core.QuiltDaemon):
    def __init__(self, args):
        quilt_core.QuiltDaemon.__init__(self)
        self.args = args
        self.setup_process("qmd")

    def run(self):

        # Use QuiltConfig to read in configuration
        cfg = quilt_core.QuiltConfig()
        # access the registrar's host and port number from config
        registrarHost = cfg.GetValue('registrar', 'host', None)
        registrarPort = cfg.GetValue('registrar', 'port', None, int)

        # access the query master's name from the config file
        qmname = cfg.GetValue(
            'query_master', 'name', 'QueryMaster')

        logging.debug("Creating query master")
        qm = query_master.QueryMaster(self.args)

        #TODO This is a hack, it will be fixed soon
        try:
            daemon = Pyro4.Daemon(Pyro4.socketutil.getIpAddress())
        except:
            daemon = Pyro4.Daemon()

        with daemon:
            # register the query master with the local PyRo Daemon with
            uri = daemon.register(qm)

            logging.debug("Registering: " + qmname + ", at: " + str(uri))

            with Pyro4.locateNS(registrarHost, registrarPort) as ns:
                # use the key name as the object name
                ns.register(qmname, uri)

            # start the Daemon's event loop
            daemon.requestLoop()


def main(argv):
    # setup command line interface
    parser = quilt_core.daemon_main_helper("qmd", "Query master daemon", argv)

    args = parser.parse_args()

    # start the daemon
    Qmd(args).main(argv)


if __name__ == "__main__":
    main(sys.argv[1:])
