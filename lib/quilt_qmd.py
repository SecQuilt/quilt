#!/usr/bin/env python
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

        with Pyro4.Daemon() as daemon:
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
