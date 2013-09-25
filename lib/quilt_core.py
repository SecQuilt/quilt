#!/usr/bin/env python
import os
import sys
import ConfigParser
import logging
import logging.handlers
import Pyro4
from os import listdir
from os.path import isfile, join
import threading
from daemon import runner
import argparse
import select
import time
import quilt_data
import lockfile
import pprint


class QuiltConfig:
    """Responsible for access to quilt configuration"""

    # The name of the environment variable that defines the location
    # of the quilt config directory.  This will be the filesystem path to a
    # directory containing a quilt.cfg file.  ALso it may have a smd.d
    # subdirectory where sourcemaster configurations may be
    QUILT_CFG_DIR_VAR = 'QUILT_CFG_DIR'

    # ConfigParser for quilt.cfg
    _config = None

    def GetCfgDir(self):
        """Get the configuration dir which is to contain config files"""
        # default configuration location to /etc/quilt
        cfgdir = '/etc/quilt'

        # if QUILT_CFG_DIR_VAR set, set location off of that value
        if self.QUILT_CFG_DIR_VAR in os.environ:
            cfgdir = os.environ[self.QUILT_CFG_DIR_VAR]

        return cfgdir

    def __init__(self):
        """Construct the object, read in the main quilt.cfg file"""

        # if config file exists at location, read it in
        quiltcfg = os.path.join(self.GetCfgDir(), 'quilt.cfg')

        if not os.path.exists(quiltcfg):
            logging.warning("quilt config not found at: " + quiltcfg)
        else:
            self._config = ConfigParser.ConfigParser()
            self._config.read(quiltcfg)

    def GetValue(# [out] string
                 self,
                 sectionName, # [in] string, name of section
                 valueName, # [in] string, name of value
                 default, # [in] value of default
                 retType=None):    # [in] type, returned type of value
        """
        Access a configuration value.  Configuration values can be
        specified with a section and a name.  The configuration value is
        returned.  If the specified value is not present the passed in
        default is returned.  If no return type is supplied the generic type
        # passed through from the config parser is used

        """

        if (self._config is None or
                not self._config.has_section(sectionName) or
                not self._config.has_option(sectionName, valueName)):
            val = default
        else:
            # get the variable from config in the generic type,
            val = self._config.get(sectionName, valueName)

        # if value is not present return None
        if val is None:
            return val

        # perform casting logic
        if retType is not None:
            #  convert to the return type and return
            return retType(val)

        return val

    def GetSourceManagersUtil(self, which):
        """get list of all defined source managers"""

        # iterate through the source managers defined in the configuration
        smdcfgdir = self.GetValue('source_managers', 'config_dir',
                                  os.path.join(self.GetCfgDir(), 'smd.d'))

        smdcfgdir = os.path.expandvars(smdcfgdir)
        if not os.path.exists(smdcfgdir):
            logging.warning(
                "Source Manager config file directory does not exist" +
                smdcfgdir)

        smdcfgs = [f for f in listdir(smdcfgdir)
                   if isfile(join(smdcfgdir, f))]


        # read all sections from all config file sin the smd directory
        names = which == 'names'

        if names:
            smds = []
        else:
            smds = quilt_data.src_specs_create()

        for f in smdcfgs:
            c = ConfigParser.ConfigParser()
            c.read(join(smdcfgdir, f))
            sections = c.sections()
            for s in sections:
                #logging.debug("Reading section: " + s)
                if names:
                    smds.append(s)
                else:
                    specStr = c.get(s, 'sourceSpec')
                    quilt_data.src_specs_add(smds,
                                             quilt_data.src_spec_create(
                                                 cfgStr=specStr, cfgSection=s))

        return smds

    def GetSourceManagers(self):
        return self.GetSourceManagersUtil("names")

    def GetSourceManagerSpecs(self):
        return self.GetSourceManagersUtil("specs")


def GetQueryMasterProxyDEPRECATED(config=None):
    """Access configuration to find query master, return proxy to it"""

    if config is None:
        config = QuiltConfig()
    qmhost = config.GetValue("query_master", "registrar_host", None)
    qmport = config.GetValue("query_master", "registrar_port", None, int)

    # access the Query Master's instance name, create a proxy to it
    qmname = config.GetValue("query_master", "name", "QueryMaster")
    logging.debug("Locating name server for query master: " + str(qmhost) +
                  ", " + str(qmport))
    with Pyro4.locateNS(qmhost, qmport) as ns:
        uri = ns.lookup(qmname)

    return Pyro4.Proxy(uri)


class QuiltDaemon(object):
    def __init__(self):
        self.stdin_path = None
        self.stdout_path = None
        self.stderr_path = None
        self.pidfile_path = None
        self.pidfile_timeout = None
        self.args = None

    name = ''


    def setup_process(self, name):
        self.name = name
        outdev = '/dev/null'
# if having trouble debugging exceptions happenign very early in initialization
# you may want to uncomment this
#       if sys.stdin.isatty():
#           outdev = '/dev/tty'
#       else:
#           outdev = '/dev/null'
        self.stdin_path = outdev
        self.stdout_path = outdev
        self.stderr_path = outdev
        self.pidfile_timeout = 5


        # if arguments are specfied, and arguments have pid file
        if self.args is not None and self.args.pid_file is not None:
            # set pid file name to value from command line
            self.pidfile_path = self.args.pid_file
        # otherwise generate a pid file path
        else:
            # get the path to this python file
            # convert all of the '/' to '_'
            f = os.path.dirname(os.path.dirname(__file__)).replace('/','_')
            # append name and '.pid'
            f += "_" + name + '.pid'
            # preppend '/tmp'
            self.pidfile_path = os.path.join('/tmp',f)

        logging.info('pid path is ' + str(self.pidfile_path))

    def main(self, argv):
        logging.debug('"' + '" "'.join(argv) + '"')
        try:
            daemon_runner = runner.DaemonRunner(self)


            # setup loggging for the daemon
            # get the currently configured logger
            logger = logging.getLogger()
            handle = None

            # if daemon has arguments and has log file
            if self.args is not None and self.args.log_file is not None:
                # NOTE special treatment is needed.  We want logging before and
                # after we daemonize, so though this was setup in common_init
                # we do it again because all filehandles get closed for us by
                # running the daemon
                # http://stackoverflow.com/questions/13180720/maintaining-logging-and-or-stdout-stderr-in-python-daemon

                # We have already set a handler in common_init, guaranteed to
                # be called before this, so grab the file handle
                handler = logger.handlers[0]
                handle = handler.stream
            # otherwise
            else:
                # NOTE: logging is setup here trying to copy what is done in 
                # common_init, when changing logging, you may want to look in
                # bothplaces

                # NOTE: Before you go crazy, no DEBUG level is not going to syslog
                # I do not know why, I can only guess it is by that log handler's
                # design.  Makes sense, if you want to debug, point it to a file

                # add a syslog log handler to the logger
                handler = logging.handlers.SysLogHandler()

                format = self.name + '%(process)d:%(levelname)s:%(message)s'

                # preserve the logging file handle
                # http://bugs.python.org/issue17981
                handle = handler.socket.fileno()

                # set logging format
                if format is not None:
                    formatter = logging.Formatter(format)
                    handler.setFormatter(formatter)

                # set the logging level for the handler
                strlevel = 'WARN'
                if self.args is not None and self.args.log_level is not None:
                    strlevel = self.args.log_level
                logger.setLevel(eval('logging.' + strlevel))

                # add the logging file handler to the logger
                logger.addHandler(handler)

            # initialze the context of the daemon runner with the logger's
            #   file handler
            if daemon_runner.daemon_context.files_preserve is None:
                daemon_runner.daemon_context.files_preserve = []
            daemon_runner.daemon_context.files_preserve.append(handle)

            daemon_runner.do_action()


        except lockfile.LockTimeout as e:
            logging.warning(self.name + " Lockfile exists: " + str(e))
        except runner.DaemonRunnerStopFailureError as e:
            logging.warning(self.name + " Failed to stop daemon: " + str(e))
        except:
            raise


def GetQueryMasterProxy(config=None):
    if config is None:
        config = QuiltConfig()

    qmhost = config.GetValue(
        "query_master", "registrar_host", None)
    qmport = config.GetValue(
        "query_master", "registrar_port", None, int)

    # access the Query Master's instance name, 
    #   create a proxy to it
    qmname = config.GetValue(
        "query_master", "name", "QueryMaster")
    logging.debug("Locating name server for query master: " +
                  str(qmhost) + ", " + str(qmport))

    qmuri = get_uri(qmhost, qmport, qmname)

    return Pyro4.Proxy(qmuri)


class QueryMasterClient:
    _lock = threading.Lock()

    def __init__(self, basename):
        """
        use user name and pid in the name of the object
        to generate a unique enough name for this machine
        """
        hostStr=''
        if 'HOSTNAME' in os.environ:
            hostStr = os.environ['HOSTNAME']

        self.localname = basename + '_' + hostStr + str(os.getpid())
        self._remotename = None
        self._config = None
        self._qmuri = None
        self.uri = None


    def GetType(self):
        raise Exception("""Abstract function is required to be implemented by
            subclass""")

    def RegisterWithQueryMaster(self):
        """Connect to the query master, register this client"""


        # Access the QueryMaster's registrar's host and port from the config
        config = self.GetConfig()

        # register the client with the query master, record the name
        # record the name the master assigned us as a member variable
        rport = config.GetValue("registrar", "port", None, int)
        if rport is not None:
            rport = int(rport)
        rhost = config.GetValue("registrar", "host", None)
        logging.debug("Registering " + self.localname + ", to query master" +
                      ", via registrar: " + str(rhost) + ":" + str(rport))
        with self.GetQueryMasterProxy() as qm:
            self._remotename = qm.RegisterClient(
                rhost, rport, self.localname, self.GetType())

        # connection complete, call our notification function
        logging.info("Connection completed for " + self.localname)

    def OnRegisterEnd(self):
        """
        virtual callback invoked when registration of this client is complete.
        Intended for optional overriding by the implementing client.
        called in main thread before event loop begins
        return false to prevent event loop from running
        """
        return True

    def OnFirstEventLoop(self):
        """
        virtual function callback for when a client's event loop first begins
        return false to prevent event loop from running
        """
        return True

    def OnEventLoopBegin(self):
        """
        function called when client's owning daemon begins an event loop
        iteration, function called in event loop main thread
        return False to end participation in event loop
        """
        return True


    def OnEventLoopEnd(self):
        """
        function called when client's owning daemon ends an event loop
        iteration, function called in event loop main thread
        return False to end participation in event loop
        """
        return True

    def UnregisterFromQueryMaster(self):
        if self._remotename is not None:
            with self.GetQueryMasterProxy() as qm:
                qm.UnRegisterClient(self.GetType(), self._remotename)

            logging.info("Unregistration completed for " + self.localname)

    def GetConfig(self):
        """
        Uses object's lock to safely initialize a config parser and store it
        as member data
        """
        # see design notes on ISSUE012
        # NOTE: this pattern of access is not completely safe, if reference 
        #   assignment is not atomic
        if self._config is None:
            # I may be paranoid, but I am constructing config object outside
            # of the lock because it might take a while
            c = QuiltConfig()
            with self._lock:
                if self._config is None:
                    self._config = c
        return self._config

    def GetQueryMasterProxy(self):
        """
        Return a proxy object to the query master for this client
        """
        # see design notes on ISSUE012
        # NOTE: this pattern of access is not completely safe, if string 
        #   assignment is not atomic
        if self._qmuri is None:
            config = self.GetConfig()
            with self._lock:
                if self._qmuri is None:
                    qmhost = config.GetValue(
                        "query_master", "registrar_host", None)
                    qmport = config.GetValue(
                        "query_master", "registrar_port", None, int)

                    # access the Query Master's instance name, 
                    #   create a proxy to it
                    qmname = config.GetValue(
                        "query_master", "name", "QueryMaster")
                    logging.debug("Locating name server for query master: " +
                                  str(qmhost) + ", " + str(qmport))
                    self._qmuri = get_uri(qmhost, qmport, qmname)

        return Pyro4.Proxy(self._qmuri)


def unregister_clients(daemonObjs, delDaemonObjs):
    # remove objects from object list
    # unregister clients from query master
    for name, obj in delDaemonObjs.items():
        del daemonObjs[name]
        obj.UnregisterFromQueryMaster()


def query_master_client_main_helper(
        clientObjectDict       # map of instances to names of objects to
        # host as pyro objects
):
    """
    Used to publish the client as a remote object, and complete the
    connection with the query master
    """

    # Use QuiltConfig to read in configuration
    cfg = QuiltConfig()
    # access the registrar's host and port number from config
    registrarHost = cfg.GetValue(
        'registrar', 'host', None)
    registrarPort = cfg.GetValue(
        'registrar', 'port', None, int)

    #TODO Hardening, make sure when exceptions are thrown that clients are removed
    # HACK FIXME, need to specify this from config
    try:
        daemon = Pyro4.Daemon(Pyro4.socketutil.getIpAddress())
    except:
        daemon = Pyro4.Daemon()


    with Pyro4.locateNS(registrarHost, registrarPort) as ns:
        # iterate the names and objects in clientObjectDict
        for name, obj in clientObjectDict.items():
            # register the clientObject with the local PyRo Daemon with
            obj.uri = daemon.register(obj)
            # use the key name as the object name
            ns.register(name, obj.uri)
            # call the ConnectToQueryMaster to complete registration
            obj.RegisterWithQueryMaster()

    daemonObjs = {}
    # iterate the names and objects in clientObjectDic
    for name, obj in clientObjectDict.items():
        # keep track of objects that
        #   desire to have the event loop (who return true)
        #   replace clientObjectDic with participating objects
        if obj.OnRegisterEnd():
            daemonObjs[name] = obj
        else:
            obj.UnregisterFromQueryMaster()


    # start the Daemon's event loop
    firstTime = True

    # continue looping while there are daemon objects
    while len(daemonObjs) > 0:

        if firstTime:
            firstTime = False
            delDaemonObjs = {}
            # iterate the names and objects in clientObjectDic
            # this function will return false if it wants to be
            # removed
            for name, obj in daemonObjs.items():
                if not obj.OnFirstEventLoop():
                    delDaemonObjs[name] = obj

            # remove objects from object list
            # unregister clients from query master
            unregister_clients(daemonObjs, delDaemonObjs)

            # if all objects have been removed break out
            if len(daemonObjs) == 0:
                break

        delDaemonObjs = {}
        # iterate the names and objects in clientObjectDic
        # this function will return false if it wants to be
        # removed
        for name, obj in daemonObjs.items():
            if not obj.OnEventLoopBegin():
                delDaemonObjs[name] = obj


        # remove objects from object list
        # unregister clients from query master
        unregister_clients(daemonObjs, delDaemonObjs)

        # if all objects have been removed break out
        if len(daemonObjs) == 0:
            break


        # TODO: Maintain contract, do not process events
        #   for object that declared they wanted to be
        #   removed by returning false.  For now it does
        #   not matter because we only ever have one object
        #   in the list
        s, _, _ = select.select(daemon.sockets, [], [], 0.05)

        if s:
            logging.debug("executing " + str(len(s)) + " events in main loop")
            daemon.events(s)
        else:
            time.sleep(1.05)

        delDaemonObjs = {}
        # iterate the names and objects in clientObjectDic
        # this function will return false if it wants to be
        # removed
        for name, obj in daemonObjs.items():
            if not obj.OnEventLoopEnd():
                delDaemonObjs[name] = obj

        # remove objects from object list
        # unregister clients from query master
        unregister_clients(daemonObjs, delDaemonObjs)

        # if all objects have been removed break out
        if len(daemonObjs) == 0:
            break


def common_init(name, args):
    """
    Common site for logging configuration, always call as first function
    from main and other initialization
    """
    # NOTE: Daemon's have a special case when using the system log, any changes to
    # defualt logging should also be canidates to be made in QuiltDaemon.main
    strlevel = args.log_level
    logfile = args.log_file
    strformat = '%(asctime)s:' + name + '%(process)d:%(levelname)s:%(message)s'
    if strlevel is None:
        strlevel = 'WARN'
    strlevel = 'logging.' + strlevel
    logging.basicConfig(level=eval(strlevel), filename=logfile, format=strformat)
    # common init stuff together
    Pyro4.config.HMAC_KEY = "Itsnotmuchofacheeshopisit"


def main_helper(name, description, argv):
    """
    Quilt helper function for main to do common things

    description                 # prose description of functionality
    argv                        # input arguments
    """
    argparser = argparse.ArgumentParser(description)

    argparser.add_argument('-l', '--log-level', nargs='?',
                           help='logging level (DEBUG,INFO,WARN,'
                                'ERROR) default: WARN')
    argparser.add_argument('-lf', '--log-file', nargs='?',
                           help='path to log file, default will be syslog or '
                                'stderr')

    args, unknownArgs = argparser.parse_known_args(argv)
    # noinspection PyUnusedLocal

    common_init(name, args)

    return argparser


def exception_to_string(error):
    """
    Display a string with information about an exception
    Useful for logging exceptions that come from other processes who do not
    have a stack trace
    """
    return str(type(error)) + " : " + str(error)


def get_uri(registrarHost, registrarPort, objName):
    """
    Get a string that specifies the absolute location of an object
    """
    # locate the nameserver at given host and port
    with Pyro4.locateNS(registrarHost, registrarPort) as ns:
        # lookup the URI for the given object name
        uri = ns.lookup(objName)
        # return the uri
        return uri


def debug_obj(obj, prefix='Object Info'):
    """
    Log information about an object for debugging
    """
    logging.debug(prefix + ":\n" +
                  str(type(obj)) + "\n" +
                  pprint.pformat(obj))


def daemon_main_helper(name, description, argv):
    """
    common initialization for daemon processes
    @description the description of the process shown when accessing the
    cmd line help for the process
    @argv the cmd line arguments for the process, starting with the first
    argument
    """
    # get parser by calling the regular main helper
    # setup command line interface
    parser = main_helper(name, description, argv)

    # add specification of the start, stop, and restart actions
    parser.add_argument('action', choices=['start', 'stop', 'restart'])

    # add specification of the pid file
    parser.add_argument('-p', '--pid-file', nargs='?',
                        help='Path to file used to make sure only one '
                             'instance of the daemon is created')

    # return the  argument oarser
    return parser
