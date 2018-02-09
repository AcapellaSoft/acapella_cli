import argparse
import sys
from typing import List

from acapella_api.logs import LogScope, LogRef
from .context import ap


class LogCommand:
    doc = 'print log (terminated/online)'
    name = 'log'
    need_auth = True

    def __init__(self):
        self.parser = argparse.ArgumentParser(description=self.doc, prog=f'acapella {self.name}', formatter_class=argparse.RawTextHelpFormatter)

        self.parser.add_argument('logref', type=str,
                                 help='reference to log. You can spceify user ID of log owner by prefixing reference with `@username/`.\n'
                                      'Examples:'
                                      '  logId\n'
                                      '  trId/logId\n'
                                      '  trId/frId/logId\n'
                                      '  trId/frId/dam/logId\n'
                                      '  @user/logId\n'
                                      '  @user/trId/logId\n'
                                      '  @user/trId/frId/logId\n'
                                      '  @user/trId/frId/dam/logId')

    def handle(self, args: List[str]):
        args = self.parser.parse_args(args)
        ref = self.parse_log_ref(args.logref)
        # print(ref.scope, ref.user, ref.trId, ref.frPath, ref.dam, ref.logId)
        ap.logs.read_log(ref)

    def __invalid_ref(self, ref: str):
        print(f"invalid log reference: {ref}", file=sys.stderr)
        exit(-1)

    def parse_log_ref(self, ref: str):
        if ref.find("//") != -1: self.__invalid_ref(ref)
        if ref[1:].find("@") != -1: self.__invalid_ref(ref)

        # разделяем строку по слешам и удаляем пробелы
        parts = ref.split('/')
        parts = map(str.strip, parts)
        parts = list(filter(lambda p: len(p) > 0, parts))

        # если строка стартует с `@` значит первая часть ссылки - это идентификатор пользователя
        # удаляем эту часть если она есть, перезаписывая userid в переменную user
        user = ap.auth.user_id
        with_user = ref.startswith('@')
        if with_user:
            if len(parts) == 1:
                self.__invalid_ref(ref) # указан только user
            user = parts[0][1:]
            parts = parts[1:]


        # по количеству частей ссылки можно определить скоуп
        size = len(parts)
        if size > 4:
            self.__invalid_ref(ref)

        # /logId
        if size == 1: return LogRef(LogScope.USER, user,        parts[0])
        # /trId/logId
        if size == 2: return LogRef(LogScope.TRANSACTION, user, parts[1], trId=parts[0])
        # /trId/frId/logId
        if size == 3: return LogRef(LogScope.FRAGMENT, user,    parts[2], trId=parts[0], frPath=parts[1])
        # /trId/frId/dam/logId
        if size == 4: return LogRef(LogScope.EXECUTION, user,   parts[3], trId=parts[0], frPath=parts[1], dam=parts[2])





