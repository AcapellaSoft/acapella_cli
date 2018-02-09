import os
import sys
from copy import copy
from os.path import isfile, join, basename, normpath

from acapella_api.common import JsonObject
from acapella_api.logs import LogParameters, LoggingParameters, LogScope
from acapella_api.vm import TransactionParameters

from .context import launcher_path

_common_log = LogParameters(
    id = 'log',
    scope = LogScope.TRANSACTION
)

_common_log_params = LoggingParameters(
    redirections = {
        'stderr': _common_log,
        'stdout': _common_log
    },
    allowCreateLogs = False
)

default_presets = {
    'transactional': TransactionParameters(
        fragment = "TEST_USER/Unnamed/default:main.lua",
        arguments = {},
        logging = _common_log_params,
        tvmCount = 3,
        failover = False,
        allowRestart = True,
        allowSubFragments = True,
        allowConvertSyncToAsync = False,
        allowConvertAsyncToSync = False,
        resolveConflicts = False,
        syncTvmIo = True,
        beginKvTransaction = False
    ),
    'acapella': TransactionParameters(
        fragment = "TEST_USER/Unnamed/default:main.lua",
        arguments = {},
        logging = _common_log_params,
        tvmCount = 3,
        failover = False,
        allowRestart = True,
        allowSubFragments = True,
        allowConvertSyncToAsync = True,
        allowConvertAsyncToSync = True,
        resolveConflicts = True,
        syncTvmIo = True,
        beginKvTransaction = False
    ),
    'single': TransactionParameters(
        fragment = "TEST_USER/Unnamed/default:main.lua",
        arguments = {},
        logging = _common_log_params,
        tvmCount = 0,
        failover = False,
        allowRestart = False,
        allowSubFragments = False,
        allowConvertSyncToAsync = False,
        allowConvertAsyncToSync = False,
        resolveConflicts = False,
        syncTvmIo = False,
        beginKvTransaction = False
    ),
}

presets = copy(default_presets)

preset_names = ", ".join(f"'{name}'" for name in presets.keys())


def __get_filename(full_path):
    return basename(normpath(full_path))


def load_presets():
    def is_preset(path):
        return isfile(path) and path.endswith(".json")

    preset_paths = list(join(launcher_path, f) for f in os.listdir(launcher_path))
    preset_paths = list(filter(is_preset, preset_paths))

    for pr_path in preset_paths:
        with open(pr_path, 'r') as preset:
            name = __get_filename(pr_path)
            json = preset.read()
            presets[name] = JsonObject.decode_from_json(TransactionParameters, json)


def dump_presets(preset_name, dumper):
    if preset_name == '*':
        for name, preset in default_presets.items():
            dumper(name, preset.to_json(formatted=True))
    else:
        try:
            dumper(preset_name, default_presets[preset_name].to_json(formatted=True))
        except KeyError:
            print("default preset not found: " + preset_name, file=sys.stderr)
            sys.exit(-1)


def save_preset(name: str, preset: str):
    with open(join(launcher_path, name + ".json"), "w") as preset_file:
        preset_file.write(preset)


def print_preset(name: str, preset: str):
    print(preset)