#!/usr/bin/env python
import os
import sys
import unittest
import quilt_test_core
import sei_core
import quilt_core
import query_master
import random


class BasicTestcase(unittest.TestCase):
    def test_basic_query(self):
        qstr = 'find me again ' + str(random.random())
        # call quilt_submit "find me again" (check return code)
        quilt_test_core.call_quilt_script('quilt_submit.py',
            [qstr, '-y'], checkCall=False)
        # sleep a small ammount
        quilt_test_core.sleep_small()
        # Use QuiltConfig, Call GetSourceManagers, for each one
        # use pyro, create proxy for the smd
        # call GetLastQuery()
        # assure it is equal to "find me again [uniqueval]"
        cfg = quilt_core.QuiltConfig()
        with quilt_core.GetQueryMasterProxy(cfg) as qm:
            smgrs = qm.GetClients("SourceManager")

            for smgr in smgrs:
                clientRec = qm.GetClientRec("SourceManager", smgr)
                with query_master.get_client_proxy(clientRec) as smgrClient:
                    lastQuery = smgrClient.GetLastQuery()
                    # after more functionality was added sources won't be called
                    # unless referenced in the query.  we should no longer
                    # see the unique query string down on the source
                    # we wouldn't anyway because it isn't the ID.  The 
                    # last query stuff was just a hack to get testing in
                    # 0.1.c1, and is added to issue list to remove
                    # TODO see ISSUE004
                    self.assertTrue(lastQuery != qstr)

            # check qmd to be sure that all quilt_submit's have unregistered
            # do this by accessing Config, finding qmd name,
            qs = qm.GetClients("QuiltSubmit")
            # make sure list is empty
            self.assertTrue(len(qs) == 0)


    def test_basic_queue(self):

        qstr = 'find me again ' + str(random.random())

        # define the query
        o = quilt_test_core.call_quilt_script('quilt_define.py',
            ['-n', qstr])

        # call quilt_submit "find me again" (check return code)
        o = str(quilt_test_core.call_quilt_script('quilt_submit.py',
            [qstr, '-y']))
        # sleep a small ammount
        quilt_test_core.sleep_small()

        a = o.index("Query ID is: ") + len(str("Query ID is: "))
        qid = o[a:]

        self.assertTrue(len(qid) > 0)

        # call quilt_q (check return code, capture output)
        o = quilt_test_core.call_quilt_script('quilt_q.py')


        # call quilt_q -q query_id (check return code, capture output)
        o = quilt_test_core.call_quilt_script('quilt_q.py', [qid])

        # assure output contains same query id as before
        self.assertTrue(len(o) > 0)

        # call quilt_q -q FAKEid (check return code, capture output)
        o = quilt_test_core.call_quilt_script('quilt_q.py', [
            qid + 'FAKE'])

        # assure no output
        self.assertTrue(o is None or len(o) == 0)

        # check qmd to be sure that all quilt_q's have unregistered
        # do this by accessing Config, finding qmd name,
        # create pyro proxy for qmd, call getRegistedObjects(type(QuiltQ))
        # make sure list is empty

        with quilt_core.GetQueryMasterProxy() as qm:
            # check qmd to be sure that all quilt_q's have unregistered
            qs = qm.GetClients("QuiltQueue")
            # make sure list is empty
            self.assertTrue(len(qs) == 0)


    def test_basic_status(self):

        quilt_lib_dir = quilt_test_core.get_quilt_lib_dir()
        # assemble the filename for query master
        quilt_status_file = os.path.join(quilt_lib_dir, 'quilt_status.py')

        # call quilt_status (check return code, capture output)
        out = sei_core.run_process([quilt_status_file, '-l', 'DEBUG'],
            whichReturn=sei_core.STDOUT, logToPython=False)

        # assure that name of the test source manager appears in the
        # output, test source name specified in testing 
        # config smd.d dir
        cfg = quilt_core.QuiltConfig()
        smgrs = cfg.GetSourceManagers()
        for smgr in smgrs:
            self.assertTrue(smgr in out)

        # check qmd to be sure that all quilt_status's have 
        # unregistered when process exits
        with quilt_core.GetQueryMasterProxy(cfg) as qm:
            qs = qm.GetClients("QuiltStatus")
            self.assertTrue(len(qs) == 0)


if __name__ == "__main__":
    quilt_test_core.unittest_main_helper("Run the most basic of test cases",
        sys.argv)
    unittest.main()
