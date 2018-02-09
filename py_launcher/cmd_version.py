from typing import List

from .context import ap, cli_name


class VersionCommand():
    doc = "print CPVM and CLI launcher versions"
    name = 'version'
    need_auth = False

    # def __init__(self):
    #     self.parser = argparse.ArgumentParser(description=self.doc, prog=f'acapella {self.name}', formatter_class=argparse.RawTextHelpFormatter)

    def handle(self, args: List[str]):
        # args = self.parser.parse_args(args)

        print(cli_name)
        print(ap.vm.get_version())
