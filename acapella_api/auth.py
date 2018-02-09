import base64
from typing import Optional

from .common import UserId, JsonObject
from .context import ApiContext


class UserInfo(JsonObject):
    def __init__(self,
                 name: UserId,
                 password: str,
                 email: str,
                 firstName: Optional[str] = None,
                 lastName: Optional[str] = None,
                 displayLocale: str = 'en',
                 systemLocale: str = 'en'):
        self.name = name
        self.password = password
        self.email = email
        self.firstName = firstName
        self.lastName = lastName
        self.displayLocale = displayLocale
        self.systemLocale = systemLocale


class AuthApi(object):
    def __init__(self, api_context: ApiContext):
        self._ctx = api_context

    def create_session(self, name: str, password: str, invalidate_old: bool = False, expireSec: Optional[int] = None) -> str:
        resp = self._ctx.http_post("/auth/login", data = {
            'loginAndPassword': base64.b64encode((name + ':' + password).encode('utf-8')),
            'invalidateOld': str(invalidate_old),
            'expireSec': expireSec,
        })
        return resp.json()['token']

    def login(self, name: str, password: str, invalidate_old: bool = False, expireSec: Optional[int] = None):
        self._ctx.token = self.create_session(name, password, invalidate_old, expireSec)
        self._ctx.user_id = name

    def logout(self):
        if self._ctx.token is None:
            return
        self._ctx.http_post("/auth/logout")
        self._ctx.token = None
        self._ctx.user_id = "$TEST_USER"

    def signup(self, info: UserInfo):
        self._ctx.http_post('/auth/signup', json=info)

    @property
    def token(self) -> str: return self._ctx.token
    @token.setter
    def token(self, value): self._ctx.token = value

    @property
    def user_id(self) -> UserId: return self._ctx.user_id
    @user_id.setter
    def user_id(self, value): self._ctx.user_id = value