#!/usr/bin/env python
# Copyright 2013 Carnegie Mellon University 
#  
# This material is based upon work funded and supported by the Department of Defense under Contract No. FA8721-
# 05-C-0003 with Carnegie Mellon University for the operation of the Software Engineering Institute, a federally 
# funded research and development center. 
#  
# Any opinions, findings and conclusions or recommendations expressed in this material are those of the author(s) and 
# do not necessarily reflect the views of the United States Department of Defense. 
#  
# NO WARRANTY. THIS CARNEGIE MELLON UNIVERSITY AND SOFTWARE ENGINEERING INSTITUTE 
# MATERIAL IS FURNISHEDON AN "AS-IS" BASIS. CARNEGIE MELLON UNIVERSITY MAKES NO 
# WARRANTIES OF ANY KIND, EITHER EXPRESSED OR IMPLIED, AS TO ANY MATTER INCLUDING, 
# BUT NOT LIMITED TO, WARRANTY OF FITNESS FOR PURPOSE OR MERCHANTABILITY, 
# EXCLUSIVITY, OR RESULTS OBTAINED FROM USE OF THE MATERIAL. CARNEGIE MELLON 
# UNIVERSITY DOES NOT MAKE ANY WARRANTY OF ANY KIND WITH RESPECT TO FREEDOM FROM 
# PATENT, TRADEMARK, OR COPYRIGHT INFRINGEMENT. 
#  
# This material has been approved for public release and unlimited distribution except as restricted below. 
#  
# Internal use:* Permission to reproduce this material and to prepare derivative works from this material for internal 
# use is granted, provided the copyright and "No Warranty" statements are included with all reproductions and 
# derivative works. 
#  
# External use:* This material may be reproduced in its entirety, without modification, and freely distributed in 
# written or electronic form without requesting formal permission. Permission is required for any other external and/or 
# commercial use. Requests for permission should be directed to the Software Engineering Institute at 
# permission@sei.cmu.edu. 
#  
# * These restrictions do not apply to U.S. government entities. 
#  
# Carnegie Mellon(r), CERT(r) and CERT Coordination Center(r) are registered marks of Carnegie Mellon University. 
#  
# DM-0000632 
import sys
import quilt_core


class QuiltQueue(quilt_core.QueryMasterClient):
    def __init__(self, args):
        # chain to call super class constructor 
        # super(QuiltQueue, self).__init__(GetType())
        quilt_core.QueryMasterClient.__init__(self, self.GetType())
        self._args = args

    def OnRegisterEnd(self):

        with self.GetQueryMasterProxy() as qm:
            if self._args.query_id is None:
                o = qm.GetQueryQueueStats()
            else:
                o = qm.TryGetQueryStats(self._args.query_id)

        if o is not None:
            print o

        # return false (prevent event loop from beginning)
        return False

    def GetType(self):
        return "qque"


def main(argv):
    # setup command line interface
    parser = quilt_core.main_helper('qque', """Quilt Queue will display
        information about queries in the queue.  If id is specified, only 
        that query's information will be displayed, if that id is not present
        nothing is displayed""",
        argv)

    parser.add_argument('query_id', nargs='?',
        help="a query ID for a query in the quilt queue")

    args = parser.parse_args(argv)

    # create the object and begin its life
    client = QuiltQueue(args)

    # start the client
    quilt_core.query_master_client_main_helper({
        client.localname: client})


if __name__ == "__main__":
    main(sys.argv[1:])

