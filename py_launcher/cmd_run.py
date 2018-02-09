import argparse
import os
import sys
from typing import List

from .cmd_start import StartCommand
from .cmd_upload import UploadCommand
from .context import dir_path


class RunCommand:
    doc = 'upload directory and start transaction'
    name = 'run'
    need_auth = True

    def __init__(self):
        self.upload_cmd = UploadCommand()
        self.start_cmd = StartCommand()

        self.parser = argparse.ArgumentParser(
            description = self.doc, prog=f'acapella {self.name}',
            parents = [self.start_cmd.parser],
            conflict_handler = 'resolve',
            formatter_class=argparse.RawTextHelpFormatter)

        for a in self.parser._actions:
            if a.dest == 'fname':
                a.help = 'fragment file path relative to current snapshot path (by default its current path). Example: path/to/fragment.lua'
                break

        self.parser.add_argument('--path', type=str, default=None, dest='path',
                                 help='specify root directory of the snapshot')
        self.parser.add_argument('--sn_name', type=str, default=None, dest='sn_name',
                                 help='name of the new snapshot')

    def handle(self, args: List[str]):
        args = self.parser.parse_args(args)

        search_path = args.path if args.path else dir_path
        fragments = self.upload_cmd.search_fragment_files(
            files = os.listdir(args.path if args.path else dir_path)
        )

        if len(fragments) == 0:
            print(f"fragments not found in '{search_path}'", file=sys.stderr)
            sys.exit(-1)

        rel_paths = [f.rel_path for f in fragments]

        if not (args.fname in rel_paths):
            fr_paths = '\n  '.join(rel_paths)
            print(f"fragment not found: '{args.fname}'\nAvailable:\n  {fr_paths}", file=sys.stderr)
            sys.exit(-1)

        sn_id = self.upload_cmd.upload_fragments(fragments, args.sn_name)

        args.fname = str(sn_id) + ':' + args.fname

        self.start_cmd.run(args)