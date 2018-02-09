import argparse
import sys

import requests

from acapella_api.context import HttpError
from .cmd_log import LogCommand
from .cmd_login import LoginCommand
from .cmd_logout import LogoutCommand
from .cmd_presets import PresetsCommand
from .cmd_register import RegisterCommand
from .cmd_run import RunCommand
from .cmd_snapshots import SnapshotsCommand
from .cmd_start import StartCommand
from .cmd_transactions import TransactionsCommand
from .cmd_upload import UploadCommand
from .cmd_version import VersionCommand
from .context import ap, cli_name
from .netrc_util import read_session, clear_sessions
from .presets import load_presets

commands = [
    UploadCommand,
    StartCommand,
    RunCommand,
    LoginCommand,
    LogoutCommand,
    RegisterCommand,
    PresetsCommand,
    SnapshotsCommand,
    TransactionsCommand,
    LogCommand,
    VersionCommand
]

command_by_name = dict((cmd.name, cmd) for cmd in commands)

command_docs = "\n".join((name.ljust(15) + cmd.doc) for name, cmd in command_by_name.items())


def run_cmd(cmd, args = sys.argv[2:]):
    try:
        cmd().handle(args)
    except KeyboardInterrupt:
        sys.exit(-1)
    except requests.ConnectionError:
        print("connection error: " + ap.url.netloc, file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(
        description=cli_name,
        usage=f'acapella <command> [<args>]\nAvailable commands:\n{command_docs}\n')

    parser.add_argument('command', help =f'subcommand to run: {", ".join(sorted(command_by_name.keys()))}')

    args = parser.parse_args(sys.argv[1:2])
    cmd = command_by_name.get(args.command)
    if not cmd:
        print('unrecognized command', file=sys.stderr)
        parser.print_help()
        exit(1)

    load_presets()

    while True:
        if cmd.need_auth:
            ap.auth.user_id, ap.auth.token = read_session()
            if ap.auth.token is None:
                run_cmd(LoginCommand, args=[])

        try:
            run_cmd(cmd)
            break
        except HttpError as e:
            if e.status_code == 401:
                clear_sessions()
                ap.auth.user_id = None
                ap.auth.token = None
                continue
            raise e


if __name__ == '__main__':
    main()