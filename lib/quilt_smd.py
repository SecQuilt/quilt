#!/usr/bin/env python
import sys
import logging
import quilt_core
from string import Template
import sei_core
import quilt_data
import Pyro4

class SourceManager(quilt_core.QueryMasterClient):


    def __init__(self, args, sourceName, sourceSpec):
        # chain to call super class constructor 
        quilt_core.QueryMasterClient.__init__(self,sourceName)
        self._args = args
        self._sourceName = sourceName
        self._sourceSpec = sourceSpec
        self._sourceResults = {}
        self._lastQuery = None

    def Query(self, queryId, sourceQuerySpec,
            queryClientName,
            queryClientNamseServerHost,
            queryClientNamseServerPort):
        """
        Query the source, currently hardcoded to behave as calling a cmdline
        tool that outputs source results on each output line
        """

        try:
            with self._lock:
                # set last query member to queryId
                self._lastQuery = queryId

            # _sourceName should not change, set at init, so ok to read
            # without lock
            logging.info("Source Manager: " + str(self._sourceName) + 
                " recieved query: " + str(queryId))

            # get the sourcePatternSpec from the pattern name in the
            #   sourceQuerySpec
            # pattern spec should be read only and safe to access without
            #   a lock
            srcPatSpecs = quilt_data.src_spec_get(self._sourceSpec, 
                sourcePatterns=True)
            srcPatSpec = quilt_data.src_pat_specs_get(srcPatSpecs, 
                quilt_data.src_query_spec_get(sourceQuerySpec, 
                srcPatternName=True))
    
            # get the variables in the query
            srcQueryVars = quilt_data.src_query_spec_get(sourceQuerySpec, 
                variables=True)
            
            # iterate src query variables map, create a simple var name to
            #   var value map
            varNameValueDict = {}
            for srcVarName, srcVarSpec in srcQueryVars.items():
                varNameValueDict[srcVarName] = quilt_data.var_spec_get(
                    srcVarSpec, value=True)

            # create cmd line for the source
            # use the template in the sourcePatternSpec, and use the values
            #   provided for the variables, and environment variables
            replacments = {}
            # replacments = os.environ.copy()
            for k,v in varNameValueDict.items():
                replacments[k] = v

            # "template" was not added as oficial schema member because it is 
            # specific to this command line case
            templateCmd = srcPatSpec['template']
            template = Template(templateCmd)
            logging.info ("Ready to use : " + templateCmd + 
                " with replacments: " + str(replacments))
            cmdline = template.safe_substitute(replacments)

            # setup context for the cmdline stdout callback
            srcQueryId = quilt_data.src_query_spec_get(
                    sourceQuerySpec,name=True)
            context = { 'queryId' : queryId, 'srcQueryId' : srcQueryId  } 

            # use run_process to execute cmd, give callback per line
            #   processing function
            sei_core.run_process(cmdline, shell=True,
                whichReturn=sei_core.EXITCODE, 
                outFunc=self.OnGrepLine, outObj=context, logToPython=False)
            
            # Set query result events list in query master using query id
            results = []
            with self._lock:
                if queryId in self._sourceResults:
                    if srcQueryId in self._sourceResults[queryId]:
                        results = list(self._sourceResults[queryId][srcQueryId])

            ns = Pyro4.locateNS(
                    queryClientNamseServerHost, 
                    queryClientNamseServerPort,)
            uri = ns.lookup(queryClientName)
            with Pyro4.Proxy(uri) as query:
                query.AppendSourceQueryResults(srcQueryId, results)
                query.CompleteSrcQuery(srcQueryId)
     
    # catch exception! 
        except Exception, error:
            try:

                # attempt to report error to the query client
                ns = Pyro4.locateNS(
                        queryClientNamseServerHost, 
                        queryClientNamseServerPort,)
                uri = ns.lookup(queryClientName)
                srcQueryId = quilt_data.src_query_spec_get(
                        sourceQuerySpec,name=True)

                with Pyro4.Proxy(uri) as query:
                    query.OnSourceQueryError(srcQueryId,error)

            except Exception, error2:
                logging.error("Unable to send source query error to " +
                    "query master")
                logging.exception(error2)
            finally:
                logging.error("Failed to execute source query")
                logging.exception(error)
                

             
    def GetLastQuery(self):
        with self._lock:
            return self._lastQuery

    def OnGrepLine(self, line, contextData):
        # assemble a jason string for an object representing an event
        # based on eventSpec and eventSpec meta data
        # convert that string to a python event object
        # append event to list of events member
        queryId = contextData['queryId']
        srcQueryId = contextData['srcQueryId']
        srcRes = []
        with self._lock:
            
            # list in query master using query id and srcQuery Id

            if queryId not in self._sourceResults:
                queryRes = {}
                self._sourceResults[queryId] = queryRes
            else:
                queryRes = self._sourceResults[queryId]

            if srcQueryId not in queryRes:
                self._sourceResults[queryId][srcQueryId] = srcRes
            else:
                srcRes = self._sourceResults[queryId][srcQueryId]

        # only one source will be writing to the source event list at a
        #   time, so we can do so outside of the lock
        #TODO fix security problem with eval
        srcRes.append(eval(line))

    def GetType(self):
        return "smd"
        

    def GetSourcePatterns(self):
        """Returns a list of names of defined source patterns"""
        try:
            # non need to lock because noone shuld be writing to a source spec
            # after init

            # iterate sourceSpec member's patterns
            # append returning list with pattern names
            srcPatSpecs = quilt_data.src_spec_tryget(self._sourceSpec, 
                sourcePatterns=True)
           
            if srcPatSpecs == None:
                return []
            
            return srcPatSpecs.keys()

        # we log exception because this was likely called from another process
        except Exception, error:
            logging.exception(error)
            raise

    def GetSourcePattern(self, patternName):
        """return the specified source pattern specification dict"""
        # non need to lock because noone shuld be writing to a source spec
        # after init

        # access the source pattern spec with the specified key in the
        # sourceSpec, return a copy
        return quilt_data.src_pat_specs_get(quilt_data.src_spec_get(
            self._sourceSpec,sourcePatterns=True), patternName)


class Smd(quilt_core.QuiltDaemon):
    def __init__(self, args):
        quilt_core.QuiltDaemon.__init__(self)
        self.setup_process("smd")
        self._args = args

    def run(self):

        cfg = quilt_core.QuiltConfig()
        smspecs = cfg.GetSourceManagerSpecs()

        logging.debug("Source manager specs: " + str(smspecs))

        objs = {}
        # iterate through source manager configurations
        for smname,smspec in smspecs.items():
            logging.debug(smname + " specified from configuration")
            # create each source manager object
            sm = SourceManager(self._args, smname, smspec)
            objs[sm.localname] = sm
        
        logging.info("Creating source managers: " + str(objs))
            
        # start the client with all the source managers
        quilt_core.query_master_client_main_helper(objs)

def main(argv):
    
    # setup command line interface
    parser =  quilt_core.main_helper('smd',"""Source manager
       daemon""",
        argv)

    # setup argument parser in accordance with funcitonal specification
    parser.add_argument('action', choices=['start', 'stop', 'restart'])
    args = parser.parse_args()

    # start the daemon
    Smd(args).main(argv)


        
if __name__ == "__main__":
    main(sys.argv[1:])

