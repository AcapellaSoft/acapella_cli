from urllib.parse import urlparse

from .auth import AuthApi
from .codebase import CodeBaseApi
from .context import ApiContext
from .logs import LoggingApi
from .vm import VmApi


class AcapellaApi:
    def __init__(self, address: str = 'http://api.acapella.ru:5678', http_timeout: int = 2000):
        if address.find('://') == -1:
            address = 'http://' + address

        self.__api_ctx = ApiContext(address, http_timeout)
        self.address = address
        self.url = urlparse(address)

        self.auth = AuthApi(self.__api_ctx)
        self.vm = VmApi(self.__api_ctx)
        self.codebase = CodeBaseApi(self.__api_ctx)
        self.logs = LoggingApi(self.__api_ctx)
