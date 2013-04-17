#!/usr/bin/env python
import os
import sys
import logging
import quilt_core
import argparse
from string import Template
import sei_core

class SourceManager(quilt_core.QueryMasterClient):


    def __init__(self, args, sourceName, sourceSpec):
        # chain to call super class constructor 
        quilt_core.QueryMasterClient.__init__(self,sourceName)
        self._args = args
        self._sourceName = sourceName
        self._sourceSpec = sourceSpec
        self._sourceResults = []

    def Query(self, queryId, sourceQuerySpec):
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
            logging.info("Source Manager: " + self._sourceName + 
                " recieved query: " + queryId + " as: " + sourceQuerySpec)

            # get the sourcePatternSpec from the pattern name in the
            #   sourceQuerySpec
            # pattern spec should be read only and safe to access without
            #   a lock
            srcPatSpecs = quilt_data.src_spec_get(sourcePatterns=True)
            srcPatSpec = quilt_data.src_pat_specs_get(srcPatSpecs, 
                quilt_data.src_query_spec_get(sourceQuerySpec, name=True))
    
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
            replacments = os.environ.copy()
            for k,v in varNameValueDict.items():
                replacments[k] = v
            template = Template(replacments)
            cmdline = template.safe_substitute(varNameValueDict)

            # use run_process to execute cmd, give callback per line
            #   processing function
            sei_core.run_process(cmdline, shell=True,
                whichReturn=sei_core.EXITCODE, outObj=self, 
                outFunc=OnGrepLine)

            # Set query result events list in query master using query id
            _qm.SetQueryResults(queryId, self._sourceResults)
     
    # catch exception! 
        except:
            error = sys.exc_info()[0] 
            try:
                _qm.OnSourceQueryError(
                    self._remotename, queryId, error)
            except:
                error2 = sys.exc_info()[0] 
                logging.error("Unable to send source query error to " +
                    "query master")
                logging.error(str(error2))
            finally:
                logging.error("Failed to execute source query")
                logging.error(str(error))
                

             
    def GetLastQuery(self):
        with self._lock:
            return self._lastQuery
        
    def OnGrepLine(self, line):
        # assemble a jason string for an object representing an event
        # based on eventSpec and eventSpec meta data
        # convert that string to a python event object
        # append event to list of events member
        #TODO fix security problem
        self._sourceResults.append(eval(line))

    def GetType(self):
        logging.debug("Returning SourceManager client type")
        return "SourceManager"
        

    def GetSourcePatterns(self):
        """Returns a list of names of defined source patterns"""
        # non need to lock because noone shuld be writing to a source spec
        # after init

        # iterate sourceSpec member's patterns
        # append returning list with pattern names
        srcPatSpecs = quilt_data.src_spec_tryget(self._sourceSpec, 
            sourcePatterns=True)
       
        if srcPatSpecs == None:
            return []
        
        return srcPatSpecs.keys

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

        objs = {}
        # iterate through source manager configurations
        for smname,smspec in smspecs.items():
            logging.debug(smname + " specified from configuration")
            # create each source manager object
            sm = SourceManager(self._args, smname, smspec)
            objs[sm._localname] = sm
            
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

