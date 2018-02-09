import argparse
import sys
import time
from getpass import getpass
from typing import List, Optional

from acapella_api.common import UserId
from acapella_api.context import ApiError
from .context import ap
from .netrc_util import clear_sessions, save_session


class LoginCommand:
    doc = "login and store session data in '`/.netrc'"
    name = 'login'
    need_auth = False

    def __init__(self):
        self.parser = argparse.ArgumentParser(description=self.doc, prog=f'acapella {self.name}', formatter_class=argparse.RawTextHelpFormatter)

        self.parser.add_argument('--username', '-u', type=str, dest='username', default=None)

        self.parser.add_argument('--expire', type=str, dest='expire', default=None,
                                 help="format: <N>s/<N>m/<N>h/<N>d (seconds, minutes, hours, days). "
                                      "After this period session will be removed. ")

        self.parser.add_argument('--nonetrc', dest='disable_netrc', default=False, action='store_true',
                                 help="forbid launcher to save login name and token in the .netrc file in the user's home directory")

        self.parser.add_argument('--clear', dest='invalidate_all_sessions', action='store_true', default=False,
                                 help='invalidate all active sessions')



    @staticmethod
    def __parse_period(period: Optional[str]) -> Optional[int]: # retrn seconds
        if period is None: return None

        time_units = {
            's': 1,
            'm': 60,
            'h': 60 * 60,
            'd': 60 * 60 * 24
        }

        max_expire = (31, 'd')

        def illegal_format():
            print('invalid period:', period, file=sys.stderr)
            print('format:\n  <N>' + '\n  <N>'.join(time_units.keys()), file=sys.stderr)
            sys.exit(-1)


        period = period.strip()
        for name, mult in time_units.items():
            if period.endswith(name):
                count = period[:-len(name)].strip()

                seconds = None
                try:
                    seconds = int(count) * mult
                except ValueError:
                    illegal_format()

                max_expire_sec = max_expire[0] * time_units[max_expire[1]]
                if seconds > max_expire_sec:
                    print('max expire period is', str(max_expire[0]) + max_expire[1], file=sys.stderr)
                    sys.exit(-1)

                return seconds

        illegal_format()


    @staticmethod
    def request_login(username: Optional[UserId] = None, disable_netrc = False, invalidate_old = False, expire: int = None):
        while True:
            if not username:
                # `input` can't be redirected to stderr
                sys.stdout = sys.stderr
                username = input('username: ')
                sys.stderr = sys.__stderr__

            password = getpass('password: ', stream=sys.stderr)

            if (not disable_netrc) and invalidate_old:
                clear_sessions(username)

            try:
                ap.auth.login(username, password,
                              invalidate_old = invalidate_old,
                              expireSec = expire)
                break
            except ApiError as e:
                if e.error_code == 0:
                    username = None
                    password = None
                    time.sleep(1)
                    print('invalid username or password', file=sys.stderr)
                else:
                    raise e

        if not disable_netrc:
            save_session(username, ap.auth.token)

    def handle(self, args: List[str]):
        args = self.parser.parse_args(args)
        expire_sec = LoginCommand.__parse_period(args.expire)
        self.request_login(args.username, args.disable_netrc, args.invalidate_all_sessions, expire_sec)
        print('logged in as ' + ap.auth.user_id)
