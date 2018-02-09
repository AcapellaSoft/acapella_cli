import os

from acapella_api import AcapellaApi

version = '0.77'
cli_name = 'Acapella CLI ' + version

launcher_path = os.path.dirname(os.path.realpath(__file__))
dir_path = os.getcwd()

ap = AcapellaApi()