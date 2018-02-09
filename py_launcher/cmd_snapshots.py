import argparse
from typing import List

from acapella_api.common import AccessLevel

from .context import ap
from .formatters import to_date


class SnapshotsCommand:
    doc = 'snapshot management'
    name = 'snapshots'
    need_auth = True

    def __init__(self):
        self.parser = argparse.ArgumentParser(description=self.doc, prog=f'acapella {self.name}', formatter_class=argparse.RawTextHelpFormatter)

        self.parser.add_argument('--name', '-n', type=str, default=None, dest='sn_name',
                                 help="filter by snapshot name")

        self.parser.add_argument('--owner', type=str, default=None, dest='sn_owner',
                                 help="specify snapshot owner ID")

        self.parser.add_argument('--shared', dest='only_shared', action='store_true',
                                 help="show only snapshots with 'VISIBLE' access and higher")

    def print_property(self, name, value):
        print(f'    {name}:'.ljust(20), value)

    def handle(self, args: List[str]):
        args = self.parser.parse_args(args)

        snapshots = ap.codebase.get_snapshots(
            name = args.sn_name,
            owner = args.sn_owner
        )

        if args.only_shared:
            snapshots = filter(lambda s: s.accessLevel != AccessLevel.INVISIBLE.value, snapshots)

        for sn in snapshots:
            print(sn.owner + '/' + sn.name + '/' + sn.tag)
            self.print_property('frozen', sn.frozen)
            self.print_property('created', to_date(sn.created))
            self.print_property('expireAt', to_date(sn.expireAt))
            self.print_property('accessLevel', sn.accessLevel)
            print()