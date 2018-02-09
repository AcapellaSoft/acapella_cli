import argparse
from typing import List

from acapella_api.vm import TransactionState
from .context import ap


class TransactionsCommand:
    doc = 'list, stop, remove transactions'
    name = 'transactions'
    need_auth = True

    def __init__(self):
        self.parser = argparse.ArgumentParser(description=self.doc, prog=f'acapella {self.name}', formatter_class=argparse.RawTextHelpFormatter)

        self.parser.add_argument('--remove', type=str, default=None, dest='remove_tr_id',
                                 help="remove transaction by ID")

    def print_property(self, name, value):
        print(f'    {name}:'.ljust(20), value)

    def handle(self, args: List[str]):
        args = self.parser.parse_args(args)

        if args.remove_tr_id:
            ap.vm.remove_transaction(args.remove_tr_id)
            return

        transactions = ap.vm.get_transactions()

        for tr in transactions:
            print(tr.id)
            self.print_property('state', tr.status.state)
            if tr.status.state == TransactionState.FINISHED.value:
                if tr.status.result:
                    self.print_property('result', tr.status.result)
            print()