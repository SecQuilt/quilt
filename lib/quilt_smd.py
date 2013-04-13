#!/usr/bin/env python
import os
import sys
import logging
import quilt_core
import argparse

class SourceManager(quilt_core.QueryMasterClient):


    #REVIEW
    def __init__(self, args, sourceName, sourceSpec):
        # chain to call super class constructor 
        quilt_core.QueryMasterClient.__init__(self,sourceName)
        self._args = args
        self._sourceName = sourceName
        self._sourceSpec = sourceSpec

    #REVIEW
    def Query(self, queryId, sourceQuerySpec):

        try:
            with self._lock:
                # set last query member to queryId
                self._lastQuery = queryId

            # _sourceName should not change, set at init, so ok to read
            # without lock
            logging.info("Source Manager: " + self._sourceName + 
                " recieved query: " + queryId + " as: " + query)

            # get the sourcePatternSpec from the pattern name in the
            #   sourceQuerySpec

            # create cmd line for grep
            # use the template in the sourcePatternSpec, and use the values
            #   provided for the variables
            #   use string.Template as per install_config.py example

            # use run_process to execute grep, give callback per line
            #   processing function

            # Set query result events list in query master using query id
        
        def OnGrepLine(string line)


            # assemble a jason string for an object representing an event
            # based on eventSpec and eventSpec meta data
            # convert that json object to a python event object
            # append event to list of events member
           
             
    def GetLastQuery(self):
        with self._lock:
            return self._lastQuery
        

    def GetType(self):
        logging.debug("Returning SourceManager client type")
        return "SourceManager"
        

    #REVIEW
    def GetSourcePatterns(self):
        """Returns a list of defined source patterns"""
        # non need to lock because noone shuld be writing to a source spec
        # after init

        # iterate sourceSpec member's patterns
        # append returning list with pattern names
        if 'sourcePatterns' in self._sourceSpec:
            patterns = self._sourceSpec['sourcePatterns']
            return patterns.keys
        return []

    #REVIEW
    def GetSourcePattern(self, patternName):
        """return the specified patternSpec dict"""
        # non need to lock because noone shuld be writing to a source spec
        # after init

        # access the source pattern spec with the specified key in the
        # sourceSpec, return a copy
        return self._sourceSpec['patterns'][patternName]


class Smd(quilt_core.QuiltDaemon):
    def __init__(self, args):
        quilt_core.QuiltDaemon.__init__(self)
        self.setup_process("smd")
        self._args = args

    #REVIEW
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

