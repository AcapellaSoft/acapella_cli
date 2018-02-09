import unittest

from acapella_api import AcapellaApi
from acapella_api.codebase import NewSnapshotMeta, ExecutorType
from acapella_api.vm import TransactionState


class VmApiTest(unittest.TestCase):
    def test_start(self):
        acapella = AcapellaApi()

        sn_name = "test-snapshot"
        snapshot: NewSnapshotMeta = acapella.codebase.create_snapshot(sn_name)
        self.assertIsNotNone(snapshot)
        self.assertEqual(snapshot.name, sn_name)
        print('snapshot created: ' + snapshot.tag)

        acapella.codebase.upload(snapshot.name, snapshot.tag,
                                 path="test/hello.py",
                                 code='print("Hello World!")',
                                 execTypes=[ExecutorType.CPYTHON])

        acapella.codebase.freeze_snapshot(snapshot.name, snapshot.tag)

        start_res = acapella.vm.call('TEST_USER', snapshot.name, snapshot.tag, "test/hello.py")
        self.assertIsNotNone(start_res)

        status = acapella.vm.wait_transaction(start_res.transaction_id)
        self.assertNotEqual(status.state, TransactionState.RUNNING)


if __name__ == '__main__':
    unittest.main()
