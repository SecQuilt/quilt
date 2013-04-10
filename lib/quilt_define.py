#!/usr/bin/env python
#REVIEW

import os
import sys
import logging
import quilt_core
import Pyro4
import query_master
import argparse
import pprint

class QuiltDefine(quilt_core.QueryMasterClient):

    def __init__(self, args):
        # chain to call super class constructor 
        # super(QuiltDefine, self).__init__(GetType())
        quilt_core.QueryMasterClient.__init__(self,self.GetType())
        self._args = args

    def OnRegisterEnd(self):
        """create a patternSpec dict from arguments (name, 
        VARIEABLEs, and VARIABLE to SOURCE_VARIABLE mappings), register
        this pattern with the query master"""

        # create the spec with it's name
        patternSpec={ 'name' : self._args.name }

        # create the specs for the variables
        for v in self._args.variable:
            # create the variables section if it does not exist
            if 'variables' not in patternSpec:
                patternSpec['variables'] = {}

            # first position of the cmd line argument is name of variable
            varName = v[0]
            
            # create new entry for the variable, or use a previous one
            # (no reason to do this really)
            if varName in patternSpec['variables']:
                vSpec = patternSpec['variables'][varName]
            else:
                vSpec = { 'name' : varName }
                patternSpec['variables'][varName] = vSpec

            # if description was specified on cmd line, load it in the spec
            if len(v) > 1:
                varDesc = v[1]
                vSpec['description'] = varDesc

            # if default was specified on cmd line, load it in spec
            if len(v) > 2:
                varDef = v[2]
                vSpec['default'] = varDef
        
        # create the specs for the variable mappings
        for m in self._args.mapping:
            varName = m[0]
            src = m[1]
            srcPat = m[2]
            srcVar = m[3]

            vSpec = patternSpec['variables'][varName]
    
            # query variables are allowed to map to multiple
            # source variables.  Initalize a blank list if it isn't present
            # then append the new mapping information
            if 'sourceMapping' in vSpec:
                mappings = vSpec['sourceMapping']
            else
                mappings = []
                vSpec['sourceMapping'] = mappings 
            
            mappings.append( {
                'sourceName' : src,
                'sourcePattern' : srcPat,
                'sourceVariable' : srcVar } )
            
            

        # define patternSpec in the query master as a syncronous call
        # return will be the pattern name
        string patName = self._qm.DefinePattern(patternSpec)

        # print out pattern Name
        print 'Pattern', patName, ' defined'
    
        # return false (prevent event loop from beginning)

        return False

    def GetType(self):
        return "QuiltDefine"
        



def main(argv):
    
    # setup command line interface
    parser =  quilt_core.main_helper('qdef',"""
        Define a pattern to the query master.  A pattern is the template for a
        query.  If specified the name of the pattern may be modified so as to
        become unique.  The official name of the pattern will be displayed.
        quilt_define will not return until pattern is defined or an error has
        occured.  It will output the finalized (unique) name for the pattern
        """,
        argv)

    parser.add_argument('-n','--name',nargs='1',
        help="suggested name of the pattern")

    parser.add_argument('-v','--variable',nargs='+',action='append',
        help="""VARIABLE [DESCRIPTION [DEFAULT]]]  The variable that are 
            part of the pattern, and an optional text description of the 
            purpose of the variable, and the oprional default value of the 
            variable""")

    parser.add_argument('-m','--mapping',nargs='4',action='append',
        help="""VARIABLE SOURCE SOURCE_PATTERN SOURCE_VARIABLE  Provide 
            mapping from a pattern variable to a source variable.
            pattern variavles are described witht he -v, --variable command.
            source variavles are described in the data source manger's 
            configuration files""")

    args = parser.parse_args(argv)

    # create the object and begin its life
    client = QuiltDefine(args)

    # start the client
    quilt_core.query_master_client_main_helper({
        client._localname : client})
        

if __name__ == "__main__":
    main(sys.argv[1:])

