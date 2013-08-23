#!/usr/bin/env python
import sys
import quilt_core
import Pyro4
import logging



class Registrar(quilt_core.QuiltDaemon):
    def __init__(self, args):
        quilt_core.QuiltDaemon.__init__(self)
        self.args = args
        self.setup_process("reg")

    def run(self):
        # Use QuiltConfig to read in configuration
        cfg = quilt_core.QuiltConfig()
        # access the registrar's host and port number from config
        registrarHost = cfg.GetValue(
            'registrar', 'host', None)
        registrarPort = cfg.GetValue(
            'registrar', 'port', None, int)

        # start the name server
        Pyro4.naming.startNSloop(registrarHost, registrarPort)


def main(argv):
    # setup command line interface
    parser = quilt_core.daemon_main_helper("qreg",
        "a meta server for quilt objects",
        argv)

    args = parser.parse_args(argv)

    # start the daemon
    Registrar(args).main(argv)


if __name__ == "__main__":
    main(sys.argv[1:])

