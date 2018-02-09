import argparse
from typing import List

from .context import ap
from .netrc_util import clear_sessions


class LogoutCommand():
    doc = "log out and remove session data from '`/.netrc'"
    name = 'logout'
    need_auth = False

    def __init__(self):
        self.parser = argparse.ArgumentParser(description=self.doc, prog=f'acapella {self.name}', formatter_class=argparse.RawTextHelpFormatter)

        self.parser.add_argument('--nonetrc', dest='disable_netrc', default=False, action='store_true',
                                 help="forbid launcher to remove login name and token from the .netrc file")

    def handle(self, args: List[str]):
        args = self.parser.parse_args(args)

        ap.auth.logout()

        if not args.disable_netrc:
            clear_sessions()