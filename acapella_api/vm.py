import time
from enum import Enum
from typing import Dict, Optional, List

from .codebase import SnapshotName, SnapshotTag, FragmentPath
from .common import TransactionId, FragmentReference, UserId, JsonObject
from .context import ApiContext
from .logs import LoggingParameters, LogParameters, LogOrdering, LogScope


class ExecutionTimeout(Exception):
    pass


class TransactionState(Enum):
    RUNNING = 'running'
    ERROR = 'error'
    NOT_FOUND = 'notFound'
    FINISHED = 'finished'


class TransactionStartResult(JsonObject):
    def __init__(self, tr_id: TransactionId):
        self.transaction_id = tr_id


class TransactionParameters(JsonObject):
    def __init__(self,
                 fragment: FragmentReference,
                 arguments: Optional[Dict[str, str]] = None,
                 logging: Optional[LoggingParameters] = None,
                 tvmCount: int = 0,
                 # Значение False несовместимо со зачением True параметров resolveConflicts, failover */
                 allowRestart: bool = False,
                 allowSubFragments: bool = False,
                 allowConvertSyncToAsync: bool = False,
                 allowConvertAsyncToSync: bool = False,
                 # Значение True несовместимо со зачением False парамета allowRestart */
                 resolveConflicts: bool = False,
                 # Значение True несовместимо со зачением False парамета allowRestart */
                 failover: bool = False,
                 syncTvmIo: bool = True,
                 beginKvTransaction: bool = False):
        self.beginKvTransaction = beginKvTransaction
        self.syncTvmIo = syncTvmIo
        self.failover = failover
        self.resolveConflicts = resolveConflicts
        self.allowConvertAsyncToSync = allowConvertAsyncToSync
        self.allowConvertSyncToAsync = allowConvertSyncToAsync
        self.allowSubFragments = allowSubFragments
        self.allowRestart = allowRestart
        self.tvmCount = tvmCount
        self.logging = logging
        self.arguments = arguments if arguments else {}
        self.fragment = fragment


class TransactionStatistics(JsonObject):
    def __init__(self,
                 tvmReads: int,
                 tvmWrites: int,

                 bytesWrite: int,
                 bytesRead: int,

                 asyncCalls: int,
                 syncCalls: int,
                 totalRestarts: int,
                 totalConflicts: int,

                 # millis
                 startTimestamp: int,
                 ioStartTimestamp: int,
                 endTimestamp: int,

                 workerExecTime: int,
                 workerExecTimeTotal: int,
                 nodeExecTime: int):
        self.tvmReads = tvmReads
        self.tvmWrites = tvmWrites
        self.bytesWrite = bytesWrite
        self.bytesRead = bytesRead
        self.asyncCalls = asyncCalls
        self.syncCalls = syncCalls
        self.totalRestarts = totalRestarts
        self.totalConflicts = totalConflicts
        self.startTimestamp = startTimestamp
        self.ioStartTimestamp = ioStartTimestamp
        self.endTimestamp = endTimestamp
        self.workerExecTime = workerExecTime
        self.workerExecTimeTotal = workerExecTimeTotal
        self.nodeExecTime = nodeExecTime


class TransactionStatus(JsonObject):
    def __init__(self,
                state: TransactionState,
                result: Optional[str] = None,
                statistics: Optional[TransactionStatistics] = None,
                error: Optional[str] = None):
        self.state = state
        self.result = result
        self.statistics = statistics
        self.error = error


class TransactionInfo(JsonObject):
    def __init__(self,
                 id: TransactionId,
                 params: TransactionParameters,
                 status: TransactionStatus):
        self.id = id
        self.params = params
        self.status = status


class VmApi(object):
    def __init__(self, api_context: ApiContext, transaction_timeout_ms: int = 2 * 60 * 1000):
        self._ctx = api_context
        self.transaction_timeout_ms = transaction_timeout_ms

    def call(self,
             sn_owner: UserId,
             sn_name: SnapshotName,
             sn_tag: SnapshotTag,
             fr_path: FragmentPath,
             args: Optional[Dict[str, str]] = None) -> TransactionStartResult:
        fr_ref = f"{sn_owner}/{sn_name}/{sn_tag}:{fr_path}"
        return self.call_by_fr_ref(fr_ref, args)

    def call_by_fr_ref(self,
                       fr_ref: FragmentReference,
                       args: Optional[Dict[str, str]] = None,
                       tr_log_name: Optional[str] = None
                       ) -> TransactionStartResult:
        """
        Запуск 'нетранзакционного' фрагмента. Функция не дожидается исполнения транзакции
        а просто возвращает transaction ID.
        Для ожидания исполнения нужно вызвать `wait_transaction(tr_id)`.

        :param fr_ref: ссылка на фрагмент в формате `<UserId>/<SnapshotName>/<SnapshotTag>:path/to/fragment.py`
        :param args: аргументы фрагмента (доступны через ap.args)
        :param tr_log_name: имя общего лога транзакции, куда будут сваливаться все стримы фрагментов (stdout/stderr)
        :return: ID запущенной транзакции
        """
        if tr_log_name:
            shared_log = LogParameters(
                id = 'log',
                scope = LogScope.TRANSACTION,
                ordering = LogOrdering.PARTIAL
            )

            logging_params = LoggingParameters(
                redirections = {
                    'stderr': shared_log,
                    'stdout': shared_log
                },
                allowCreateLogs = False
            )
        else:
            logging_params = LoggingParameters(
                redirections = {},
                allowCreateLogs = False
            )

        tr_params = TransactionParameters(
            fragment = fr_ref,
            arguments = args,
            logging = logging_params,
            tvmCount = 0,
            failover = False,
            allowRestart = False,
            allowSubFragments = False,
            allowConvertSyncToAsync = False,
            allowConvertAsyncToSync = False,
            resolveConflicts = False,
            syncTvmIo = False
        )

        return self.start_transaction(tr_params)

    def start_transaction(self, params: TransactionParameters) -> TransactionStartResult:
        return self.start_transaction_with_raw_params(params.to_json())

    def start_transaction_with_raw_params(self, tr_params_json: str) -> TransactionStartResult:
        response = self._ctx.http_post(f'/vm/start', data=tr_params_json)
        return TransactionStartResult(response.json()['transactionId'])

    def stop_transaction(self, tr_id: TransactionId) -> None:
        self._ctx.http_post(f'/vm/stop', data={'trId': tr_id})

    @staticmethod
    def __parse_tr_status(json: Optional[dict]) -> TransactionStatus:
        if json is None:
            return TransactionStatus(TransactionState.RUNNING)
        return JsonObject.decode_from_json_dict(TransactionStatus, json)

    def get_transaction_status(self, tr_id: TransactionId) -> Optional[TransactionStatus]:
        json = self._ctx.http_get(f'/vm/transactions/{tr_id}/status').json()
        if json is None:
            return None
        return self.__parse_tr_status(json)

    def get_transactions(self) -> List[TransactionInfo]:
        json = self._ctx.http_get(f'/vm/transactions').json()
        return [JsonObject.decode_from_json_dict(TransactionInfo, tr) for tr in json]

    def remove_transaction(self, tr_id: TransactionId):
        self._ctx.http_delete(f'/vm/transactions/{tr_id}')

    def wait_transaction(self, tr_id: TransactionId) -> TransactionStatus:
        """Ожидание завершения транзакции."""
        timeout = 0.001
        deadline = time.time() + self.transaction_timeout_ms
        while time.time() < deadline:
            response = self._ctx.http_get(f'/vm/transactions/{tr_id}/wait',
                                          timeout=self.transaction_timeout_ms)
            status = self.__parse_tr_status(response.json())
            if status.state == TransactionState.RUNNING:
                time.sleep(timeout)
                timeout = min(5.0, timeout * 2.0)
            else:
                return status

        raise ExecutionTimeout()

    def get_version(self) -> str:
        response = self._ctx.http_get(f'/vm/version')
        return response.text
