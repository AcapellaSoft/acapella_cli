import argparse
import hashlib
import os
import sys
from typing import List, Optional, Set, Mapping, Tuple

from acapella_api.codebase import SnapshotName, SnapshotId, ExecutorType, SnapshotMeta, SnapshotTag
from acapella_api.common import UserId, FragmentReference
from .context import dir_path, launcher_path, ap

execTypesByExt: Mapping[str, Set[ExecutorType]] = {
    "py": [ExecutorType.CPYTHON],
    "lua": [ExecutorType.VM_LUAJ],
}


class FragmentFile:
    def __init__(self, path: str, rel_path: str, ext: str, exec_types: Set[ExecutorType]):
        self.path = path
        self.rel_path = rel_path
        self.ext = ext
        self.exec_types = exec_types
        self.hash = sha1_digest(path)


def sha1_digest(path: str) -> str:
    sha1 = hashlib.sha1()
    with open(path, 'rb') as source:
        block = source.read(2**16)
        while len(block) != 0:
            sha1.update(block)
            block = source.read(2**16)
    return sha1.hexdigest()


class MalformedSnapshotId(Exception):
    def __init__(self, sn_id):
        super().__init__(self, 'malformed snapshot ID: ' + sn_id)
        self.sn_id = sn_id


class MalformedFragmentReference(Exception):
    def __init__(self, fr_ref):
        super().__init__(self, 'malformed fragment reference: ' + fr_ref)
        self.fr_ref = fr_ref


def parse_snapshot_id(sn_id: str) -> SnapshotId:
    parts = sn_id.split('/')
    if not (len(parts) in [2, 3]):
        raise MalformedSnapshotId(sn_id)
    owner: UserId = parts[0]
    name: SnapshotName = parts[1]
    tag: SnapshotTag = parts[2]
    return SnapshotId(owner, name, tag)


def parse_fr_ref(fr_ref: FragmentReference) -> Tuple[SnapshotId, str]:
    parts = fr_ref.split(':')
    if len(parts) != 2:
        raise MalformedFragmentReference(fr_ref)
    sn_id = parse_snapshot_id(parts[0])
    fr_path = parts[1]
    return sn_id, fr_path


class UploadCommand:
    doc = "upload fragments to CodeBase"
    name = 'upload'
    need_auth = True

    def __init__(self):
        self.parser = argparse.ArgumentParser(description=self.doc, prog=f'acapella {self.name}', formatter_class=argparse.RawTextHelpFormatter)

        self.parser.add_argument('files', type=str, action='append',
                                 help='fragment files')

        self.parser.add_argument('--sn_id', type=str, default=None, dest='sn_id',
                                 help='snapshot ID. Format: <SnapshotOwner>/<SnapshotName>/<SnapshotTag>')

        self.parser.add_argument('--sn_name', type=str, default=None, dest='sn_name',
                                 help='name of the new snapshot')

        self.parser.add_argument('--nofreeze', dest='no_freeze', action='store_true',
                                 help='do not freeze snapshot after upload')

    def handle(self, args: List[str]):
        args = self.parser.parse_args(args)

        if args.sn_name and args.sn_id:
            print("parameters 'sn_name' and 'sn_id' are incompatible", file=sys.stderr)
            sys.exit(-1)

        if args.no_freeze and args.sn_id:
            print("option 'no_freeze' is useless when 'sn_id' is specified", file=sys.stderr)

        sn_id = parse_snapshot_id(args.sn_id) if args.sn_id else None
        self.upload(args.files, sn_name=args.sn_name, sn_id=sn_id, freeze=(not args.no_freeze))

    def upload_fragment(self, f, sn_id: SnapshotId):
        with open(f.path, 'r') as fr_file:
            src = fr_file.read()
            ap.codebase.upload(sn_id.name, sn_id.tag, f.rel_path, code=src, exec_types=f.exec_types)

    def add_fragments_to_snapshot(self, fragments: List[FragmentFile], sn_id: SnapshotId):
        if len(fragments) > 1:
            print('upload fragments:')
            for f in fragments:
                print(' ', f.rel_path)
                self.upload_fragment(f, sn_id)
        else:
            f = fragments[0]
            print('upload fragment:', f.rel_path)
            self.upload_fragment(f, sn_id)

    def get_or_create_snapshot(self,
                               fragments: List[FragmentFile],
                               sn_name: Optional[SnapshotName],
                               freeze = True) -> SnapshotMeta:
        if sn_name is None:
            sn_name = 'cli-launcher'

        fr_hashes = dict((f.rel_path, f.hash) for f in fragments)
        resp = ap.codebase.create_snapshot(sn_name, fragmentHashes=fr_hashes)

        snapshot = resp.snapshot
        sn_id = SnapshotId(snapshot.owner, snapshot.name, snapshot.tag)

        if snapshot.frozen or (len(resp.notFound) == 0):
            print('matches with existing snapshot:', sn_id)
            return snapshot

        print('snapshot created:', sn_id)

        not_found = [s.lower() for s in resp.notFound]
        frs_to_upload = list(filter(lambda f: f.hash in not_found, fragments))
        if len(frs_to_upload) > 0:
            print(f'{len(frs_to_upload)} fragments not found in CodeBase')
            self.add_fragments_to_snapshot(frs_to_upload, sn_id)

        if freeze:
            ap.codebase.freeze_snapshot(sn_id.name, sn_id.tag)
            print('snapshot is ready')
        return snapshot

    def get_fr_list(self, file_paths: List[Tuple[str, str]]) -> List[FragmentFile]:
        result = []
        for paths in file_paths:
            path = paths[0]
            rel_path = paths[1]

            dot_pos = path.rfind('.')
            ext = '' if dot_pos == -1 else path[dot_pos+1:]
            exec_types = execTypesByExt.get(ext)
            if exec_types is None:
                continue

            result.append(FragmentFile(path, rel_path, ext, exec_types))
        return result

    def search_files(self, paths: List[str]) -> List[Tuple[str, str]]: # -> (abspath, relpath)
        file_paths = []
        paths = [os.path.join(dir_path, p) for p in paths]

        for p in paths:
            if os.path.isfile(p):
                file_paths.append((p, os.path.relpath(p, dir_path)))
            else:
                if not os.path.exists(p):
                    print(f"not found: '{p}'", file=sys.stderr)
                    sys.exit(-1)

                abs_filepaths = [
                    os.path.join(root, name)
                    for root, dirs, files in os.walk(p)
                    for name in files]

                for afp in abs_filepaths:
                    file_paths.append((afp, os.path.relpath(afp, dir_path)))

        file_paths = list(filter(lambda f: not f[0].startswith(launcher_path), file_paths))

        return file_paths

    def search_fragment_files(self, files: List[str]):
        fr_paths = self.search_files(files)
        fragments = self.get_fr_list(fr_paths)
        return fragments

    def upload_fragments(self,
                         fragments: List[FragmentFile],
                         sn_name: Optional[SnapshotName] = None,
                         sn_id: Optional[SnapshotId] = None,
                         freeze = True) -> SnapshotId:
        if sn_id is None:
            sn_meta = self.get_or_create_snapshot(fragments, sn_name=sn_name, freeze=freeze)
            sn_id = SnapshotId(sn_meta.owner, sn_meta.name, sn_meta.tag)
        else:
            self.add_fragments_to_snapshot(fragments, sn_id)

        return sn_id

    def upload(self,
               files: List[str],
               sn_name: Optional[SnapshotName] = None,
               sn_id: Optional[SnapshotId] = None,
               freeze = True) -> SnapshotId:
        """

        :param files: названия файлов или папок, которые нужно загрузить
        :return: sn_id
        """
        fragments = self.search_fragment_files(files)

        if len(fragments) == 0:
            print(f"no fragments in {', '.join(files)}", file=sys.stderr)
            sys.exit(-1)

        return self.upload_fragments(fragments, sn_name=sn_name, sn_id=sn_id, freeze=freeze)
