import argparse
from typing import List

from .presets import dump_presets, save_preset, print_preset


class PresetsCommand:
    doc = 'dump deafult presets'
    name = 'presets'
    need_auth = False

    def __init__(self):
        self.parser = argparse.ArgumentParser(description=self.doc, prog=f'acapella {self.name}', formatter_class=argparse.RawTextHelpFormatter)

        self.parser.add_argument('--name', '-n', type=str, default='*', dest='name_of_preset',
                                 help="specify name of default preset. Use '*' to dump all presets")

        self.parser.add_argument('--file', '-f', default=None, dest='dump_to_files', action="store_true",
                                 help='dump all default presets to <preset_name>.json files')

    def handle(self, args: List[str]):
        args = self.parser.parse_args(args)

        dumper = save_preset if args.dump_to_files else print_preset
        dump_presets(args.name_of_preset, dumper)