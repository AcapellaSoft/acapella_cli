import re

import requests
from requests import Response
from requests.auth import HTTPBasicAuth

from .common import UserId, JsonObject


class ApiContext(object):
    def __init__(self, address: str = 'http://localhost:5678', http_timeout: int = 2000):
        self.address = address
        self.http_timeout = http_timeout

        self.user_id: UserId = "$TEST_USER"
        self.token: str = None

        self.__id_pattern = re.compile('^[\w\-.]+$')

    def raise_if_failed(self, response: Response) -> None:
        if response.status_code != 200:
            if response.status_code == 400:
                json = response.json()
                raise ApiError(
                    error_code=json['code'],
                    msg=json['message']
                )
            raise HttpError(response.status_code, response.text)

    def validate_id(self, id, name = "id") -> None:
        if not self.__id_pattern.match(id):
            raise Exception(f'invalid {name}: "{id}"')

    def http_request(self, method, path, **kwargs):
        assert path.startswith("/")

        json = kwargs.get('json')
        if json and isinstance(json, JsonObject):
            kwargs['json'] = None
            kwargs['data'] = json.to_json()

        if self.token:
            kwargs['auth'] = HTTPBasicAuth(self.user_id, self.token)

        kwargs.setdefault('timeout', self.http_timeout)
        kwargs.setdefault('allow_redirects', True)

        ignore_errors = False
        if kwargs.get("ignore_errors"):
            ignore_errors = True
            del kwargs["ignore_errors"]

        resp = requests.request(method, self.address + path, **kwargs)

        if not ignore_errors:
            self.raise_if_failed(resp)

        return resp

    def http_get(self, path: str, **kwargs) -> Response:
        return self.http_request('get', path, **kwargs)

    def http_post(self, path: str, **kwargs) -> Response:
        return self.http_request('post', path, **kwargs)

    def http_delete(self, path: str, **kwargs) -> Response:
        return self.http_request('delete', path, **kwargs)


class ApiError(Exception):
    def __init__(self, error_code: int, msg: str = None):
        super().__init__(error_code, msg)
        self.error_code = error_code
        self.msg = msg

    def __str__(self):
        return f'ApiError(code = {self.error_code}, message = \'{self.msg}\')'


class HttpError(Exception):
    def __init__(self, status_code: int, msg: str = None):
        super().__init__(status_code, msg)
        self.status_code = status_code
        self.msg = msg

    def __str__(self):
        return f'HttpError({self.status_code}, {self.msg})'
