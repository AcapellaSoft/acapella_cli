from typing import Optional, Tuple

from acapella_api.common import UserId

from .context import ap
from .tinynetrc import Netrc


def read_session(username: Optional[UserId] = None) -> Optional[Tuple[str, str]]:
    """
    Read session data from `~/.netrc` file. Hostname is coming from `acapella_api.AcapellaApi.url.netloc`

    :param username: optional
    :return: (login, password) or None
    """
    netrc = Netrc()  # parse ~/.netrc
    session = netrc[ap.url.netloc]
    netrc_user = session['login']
    if username and (netrc_user != username):
        return None
    return netrc_user, session['password']


def save_session(username: UserId, token: str):
    netrc = Netrc()  # parse ~/.netrc
    session = netrc[ap.url.netloc]
    session['login'] = username
    session['password'] = token
    netrc.save()
    print('session data saved in ~/.netrc')


def clear_sessions(username: Optional[UserId] = None):
    netrc = Netrc()  # parse ~/.netrc
    session = netrc[ap.url.netloc]
    netrc_user = session['login']
    if (not netrc_user) or (username and (netrc_user != username)):
        return
    del netrc[ap.url.netloc]
    netrc.save()
    print('session data removed from ~/.netrc')
