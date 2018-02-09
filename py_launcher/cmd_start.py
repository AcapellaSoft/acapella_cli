import argparse
import json
import sys
from typing import List

from acapella_api.vm import TransactionParameters, ExecutionTimeout, TransactionStatus, TransactionState
from .cmd_upload import parse_fr_ref
from .context import ap
from .formatters import to_duration, to_date, format_size
from .presets import preset_names, presets


class StartCommand:
    doc = 'start transaction'
    name = 'start'
    need_auth = True

    def __init__(self):
        self.parser = argparse.ArgumentParser(description=self.doc, prog=f'acapella {self.name}', formatter_class=argparse.RawTextHelpFormatter)

        self.parser.add_argument('fname', type=str,
                            help='full fragment reference. Format: `<SnapshotOwner>/<SnapshotName>/<SnapshotTag>:path/to/fragment.lua`')

        self.parser.add_argument('--trid', type=str, default=None, dest='trid',
                            help='custom transaction ID')
        self.parser.add_argument('--args', type=str,  dest='dict_of_args', default=None,
                            help='map of root fragment arguments: {"p1":"v1", "p2":"v2"}')
        self.parser.add_argument('--kvio', action='store_true',
                            help='allow TVM KV IO')
        self.parser.add_argument('--log', type=str, dest='log', default="offline",
                            help='logging mode: realtime, offline, none')
        self.parser.add_argument('--preset', type=str, dest='preset', default="transactional",
                                 help= preset_names + '.\nYou can add your custom preset: just put \'*.json\' file of preset to the launcher folder')

    def handle(self, args: List[str]):
        self.run(self.parser.parse_args(args))

    def run(self, args):
        parse_fr_ref(args.fname)

        if args.log and not (args.log in ['realtime', 'offline', 'none']):
            print(f"invalid 'log' argument value: '{args.log}'\n"
                  f"available logging modes: 'realtime', 'offline', 'none'", file=sys.stderr)
            sys.exit(-1)

        tr_params = self.get_preset(args.preset)
        tr_params.arguments = self.parse_fr_args(args.dict_of_args) if args.dict_of_args else {}
        tr_params.beginKvTransaction = args.kvio
        tr_params.transactionId = args.trid
        tr_params.fragment = args.fname

        self.start_transaction(args, tr_params)

    def start_transaction(self, args, tr_params: TransactionParameters):
        print(ap.vm.get_version())
        print("using '" + args.preset + "' preset")
        print("start fragment:", tr_params.fragment)

        tr_id = ap.vm.start_transaction(tr_params).transaction_id

        print("transaction started:", tr_id)

        if args.log == "realtime":
            print("log:\n")
            ap.logs.read_tr_log(tr_id, log_id="log")
            print()

        try:
            status = ap.vm.wait_transaction(tr_id)
        except ExecutionTimeout:
            print("execution timeout", file=sys.stderr)
            return

        self.print_result(status)
        if status.state != TransactionState.FINISHED.value:
            return

        if args.log == "offline":
            print("log:\n")
            ap.logs.read_tr_log(tr_id, log_id="log")
            print()


    def parse_fr_args(self, dict_of_args) -> dict:
        try:
            dict_of_args = json.loads(dict_of_args)
        except Exception:
            example = {'p1': 'v1', 'p2': 'v2'}
            print(f"malformed fragment arguments: '{tr_args_json}'\n"
                  f"must be json dictionary. E.g.: {json.dumps(example)}", file=sys.stderr)
            sys.exit(-1)

        if type(dict_of_args) != dict:
            example = {'p1': 'v1', 'p2': 'v2'}
            print(f"invalid fragment arguments: '{tr_args_json}'\n"
                  f"must be dictionary. E.g.: {json.dumps(example)}", file=sys.stderr)
            sys.exit(-1)

        return dict_of_args

    def print_result(self, status: TransactionStatus):
        state = status.state
        if state == TransactionState.FINISHED.value:
            stats = status.statistics

            print("transaction completed\n")
            print("execution time:")
            print("    {:28} {}".format("without overheads (worker):", to_duration(int(stats.workerExecTime))))
            print("    {:28} {}".format("total (worker):",             to_duration(int(stats.workerExecTimeTotal))))
            print("    {:28} {}".format("total (node):",               to_duration(int(stats.nodeExecTime))))

            print("transaction timestamps: ")
            print("    {:15} {}".format("start: ",                     to_date(int(stats.startTimestamp))))
            ioStartTimestamp = int(stats.ioStartTimestamp)
            if ioStartTimestamp > 0:
                print("    {:15} {}".format("IO start:",               to_date(ioStartTimestamp)))
            print("    {:15} {}".format("end:",                        to_date(int(stats.endTimestamp))))

            print("counters: ")
            print("    {:18} {}".format("total conflicts:",            int(stats.totalConflicts)))
            print("    {:18} {}".format("total restarts:",             int(stats.totalRestarts)))
            print("    {:18} {}".format("async calls:",                int(stats.asyncCalls)))
            print("    {:18} {}".format("sync calls:",                 int(stats.syncCalls)))
            print("    {:18} {}".format("TVM reads:",                  int(stats.tvmReads)))
            print("    {:18} {}".format("TVM writes:",                 int(stats.tvmWrites)))

            print("TVM traffic: ")
            print("    {:18} {}".format("TVM bytes read:",             format_size(int(stats.bytesRead))))
            print("    {:18} {}".format("TVM bytes write:",            format_size(int(stats.bytesWrite))))
            return
        elif state == TransactionState.ERROR.value:
            print("transaction completed with errors:\n")
            print(status.error)
            return
        elif state == TransactionState.NOT_FOUND.value:
            print(status.error, file=sys.stderr)
        else:
            raise RuntimeError(f"unexpected state '{state}'")

    def get_preset(self, name) -> TransactionParameters:
        try:
            return presets[name]
        except KeyError:
            print(f"invalid preset: '{args.preset}'\navailable: {preset_names}", file=sys.stderr)
            sys.exit(-1)