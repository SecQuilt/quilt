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
import quilt_core


class QuiltHistory(quilt_core.QueryMasterClient):
    def __init__(self, args):
        # chain to call super class constructor 
        quilt_core.QueryMasterClient.__init__(self, self.GetType())
        self._args = args

    def OnRegisterEnd(self):
        with self.GetQueryMasterProxy() as qm:
            o = qm.GetQueryHistoryStats(self._args.query_id)

        if o is not None:
            print o

        # return false (prevent event loop from beginning)
        return False

    def GetType(self):
        return "qhist"


def main(argv):
    # setup command line interface
    parser = quilt_core.main_helper('qhist', """
        Quilt history will display information about completed queries.  If ID
        is provided, the Query's information will describe the variables used 
        it its definition, the state of the query, and any results that are
        available.  If there are no completed queries with the specified ID 
        present, An error occurs.""",
        argv)

    parser.add_argument('query_id', nargs='?',
        help="a query ID for a query in the history")

    args = parser.parse_args(argv)

    # create the object and begin its life
    client = QuiltHistory(args)

    # start the client
    quilt_core.query_master_client_main_helper({
        client.localname: client})


if __name__ == "__main__":
    main(sys.argv[1:])

