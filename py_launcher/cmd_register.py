import locale
import re
import sys
import time
from getpass import getpass
from textwrap import dedent
from typing import List

from acapella_api.auth import UserInfo
from acapella_api.context import ApiError
from .context import ap


class RegisterCommand:
    doc = "sign up for Acapella"
    name = 'register'
    need_auth = False

    # def __init__(self):
    #     self.parser = argparse.ArgumentParser(description=self.doc, prog=f'acapella {self.name}', formatter_class=argparse.RawTextHelpFormatter)

    def handle(self, args: List[str]):
        # args = self.parser.parse_args(args)
        info = UserInfo(None, None, None)
        while True:
            info = self.input_user_info(info.name, info.password)
            print("please wait...")
            try:
                ap.auth.signup(info)
                break
            except ApiError as e:
                if e.error_code == 1 or e.error_code == 6:
                    time.sleep(0.5)
                    info.name = None
                    info.password = None
                    print('username is exist', file=sys.stderr)
                elif e.error_code == 2:
                    time.sleep(0.5)
                    print('e-mail is exist', file=sys.stderr)
                elif e.error_code == 4:
                    time.sleep(0.5)
                    print('wrong E-mail', file=sys.stderr)
                else:
                    raise e

        print(dedent(f"""
            Thank you for registering
            Check out {info.email}
            Be sure to check your spam folder before next registration attempt
        """))

    def __check_password(self, password: str) -> bool:
        if len(password) < 8:
            print('password is too short (min length = 8)', file=sys.stderr)
            return False
        if not re.match('^[\w.-]{8,}$', password):
            print('illegal password', file=sys.stderr)
            return False
        return True

    def input_user_info(self, username = None, password = None, email = None):
        if username is None:
            username = input('username: ')

        while email is None:
            email = input('e-mail: ')
            if not ('@' in email):
                print('illegal e-mail address', file=sys.stderr)
                email = None
            else:
                break

        password1, password2 = password, None
        while password1 is None:
            while True:
                password1 = getpass('password: ')
                if self.__check_password(password1): break
            password2 = getpass('retry password: ')
            if password1 == password2:
                break
            print('password mismatch. Try again...', file=sys.stderr)

        lang_tag = locale.getdefaultlocale()[0]

        return UserInfo(
            name = username,
            password = password1,
            email = email,
            systemLocale = lang_tag,
            displayLocale = lang_tag
        )
