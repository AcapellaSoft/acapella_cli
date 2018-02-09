import io
import sys
from enum import Enum
from typing import Dict, Optional

from .codebase import FragmentPath
from .common import TransactionId, JsonObject, UserId, CompactDam
from .context import ApiContext

StreamId = str
LogName = str


class LogScope(Enum):
    EXECUTION = "EXECUTION"
    FRAGMENT = "FRAGMENT"
    TRANSACTION = "TRANSACTION"
    USER = "USER"
    GLOBAL = "GLOBAL"


class LogOrdering(Enum):
    NONE = "NONE"
    PARTIAL = "PARTIAL"
    STRICT = "STRICT"


class LogParameters(JsonObject):
    def __init__(self,
                 id: LogName,
                 scope: LogScope,
                 ordering = LogOrdering.PARTIAL):
        self.id = id
        self.scope = scope
        self.ordering = ordering


class LoggingParameters(JsonObject):
    def __init__(self, redirections: Dict[StreamId, LogParameters], allowCreateLogs: bool = False):
        """
        :param redirections: перенаправления потоков из фрагмента. Ключ - id потока, значение - параметры лога, в который происходит вывод
        :param allowCreateLogs: разрешить фрагментам самим создавать перенаправления
        """
        self.allowCreateLogs = allowCreateLogs
        self.redirections = redirections


class LogRef(JsonObject):
    def __init__(self,
                 scope: LogScope,
                 user: UserId,
                 logId: str,
                 trId: Optional[TransactionId] = None,
                 frPath: Optional[FragmentPath] = None,
                 dam: Optional[CompactDam] = None):
        self.scope = scope
        self.user = user
        self.logId = logId
        self.trId = trId
        self.frPath = frPath
        self.dam = dam

# todo pass user
class LoggingApi(object):
    def __init__(self, api_context: ApiContext):
        self._ctx = api_context

    def __read_log(self, output, path: str) -> bool:
        resp = self._ctx.http_get(path, timeout=1000.0, stream=True, ignore_errors=True)
        if resp.status_code == 404:
            return False
        self._ctx.raise_if_failed(resp)
        for chunk in resp.iter_content():
            output.write(chunk.decode(encoding="utf-8"))
        return True

    def read_log(self, ref: LogRef, output: io.RawIOBase = sys.stdout):
        if ref.scope == LogScope.USER:
            self.read_user_log(ref.logId, output)
        elif ref.scope == LogScope.TRANSACTION:
            self.read_tr_log(ref.trId, ref.logId, output)
        elif ref.scope == LogScope.FRAGMENT:
            self.read_fragment_log(ref.trId, ref.frPath, ref.logId, output)
        elif ref.scope == LogScope.EXECUTION:
            self.read_execution_log(ref.trId, ref.frPath, ref.dam, ref.logId, output)

    def read_user_log(self, log_id: LogName, output: io.RawIOBase = sys.stdout) -> bool:
        """Чтение лога log_id со скоупом USER"""
        return self.__read_log(output, f'/vm/logs/{log_id}')

    def read_tr_log(self, tr_id: TransactionId, log_id: LogName, output: io.RawIOBase = sys.stdout) -> bool:
        """Чтение лога log_id со скоупом TRANSACTION"""
        return self.__read_log(output, f'/vm/transactions/{tr_id}/logs/{log_id}')

    def read_fragment_log(self, tr_id: TransactionId, fr_path: FragmentPath, log_id: LogName, output: io.RawIOBase = sys.stdout) -> bool:
        """Чтение лога log_id со скоупом FRAGMENT"""
        return self.__read_log(output, f'/vm/transactions/{tr_id}/fragments/{fr_path}/logs/{log_id}')

    def read_execution_log(self, tr_id: TransactionId, fr_path: FragmentPath, dam: str, log_id: LogName, output: io.RawIOBase = sys.stdout) -> bool:
        """Чтение лога log_id со скоупом EXECUTION"""
        return self.__read_log(output, f'/vm/transactions/{tr_id}/fragments/{fr_path}/executions/{dam}/logs/{log_id}')