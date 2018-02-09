from enum import Enum
from typing import Optional, Dict, Set, List, Mapping

from .common import AccessLevel, UserId, JsonObject
from .context import ApiContext

SnapshotName = str
SnapshotTag = str
FragmentPath = str


class SnapshotId(object):
    def __init__(self,
                 owner: UserId,
                 name: SnapshotName,
                 tag: SnapshotTag):
        self.owner = owner
        self.name = name
        self.tag = tag

    def __str__(self):
        return "/".join([self.owner, self.name, self.tag])


class ExecutorType(Enum):
    VM_LUAJ = "VmLuaJ"
    LUAJ = "LuaJ"
    LUAC = "LuaC"
    LUAJIT = "LuaJit"
    VM_JYTHON = "VmJython"
    CPYTHON = "CPython"


class NewSnapshotMeta(JsonObject):
    def __init__(self,
                 name: Optional[SnapshotName],
                 tag: Optional[SnapshotTag],
                 removeAfterExecute: bool,
                 expirationTimeSec: Optional[int] = None,
                 accessLevel: AccessLevel = AccessLevel.INVISIBLE,
                 accessPerUser: Optional[Dict[UserId, AccessLevel]] = None,
                 fragmentHashes: Optional[Mapping[FragmentPath, str]] = None):
        self.name = name
        self.tag = tag
        self.removeAfterExecute = removeAfterExecute
        self.expirationTimeSec = expirationTimeSec
        self.accessLevel = accessLevel
        self.accessPerUser = accessPerUser
        self.fragmentHashes = fragmentHashes


class SnapshotMeta(JsonObject):
    def __init__(self,
                 name: Optional[SnapshotName],
                 tag: Optional[SnapshotTag],
                 frozen: bool,
                 removed: bool,
                 created: int,
                 expireAt: int,
                 owner: UserId,
                 accessLevel: AccessLevel = AccessLevel.INVISIBLE,
                 accessPerUser: Optional[Dict[UserId, AccessLevel]] = None):
        self.name = name
        self.tag = tag
        self.frozen = frozen
        self.removed = removed
        self.created = created
        self.expireAt = expireAt
        self.owner = owner
        self.accessLevel = accessLevel
        self.accessPerUser = accessPerUser


class NewSnapshotResponse(JsonObject):
    def __init__(self, snapshot: SnapshotMeta, notFound: Optional[List[str]] = None):
        self.snapshot = snapshot
        self.notFound = notFound


class FragmentMetadata(JsonObject):
    def __init__(self,
                isTextSource: bool,
                executorTypes: Set[ExecutorType],
                owner: UserId = "",  # при заливе фрагмента это поле игнорируется
                access: AccessLevel = AccessLevel.INVISIBLE,
                accessPerUser: Optional[Dict[UserId, AccessLevel]] = None,
                requireTvm: Optional[bool] = None,
                requireSubFragments: Optional[bool] = None,
                allowRestart: Optional[bool] = None):
        self.isTextSource = isTextSource
        self.executorTypes = executorTypes
        self.owner = owner
        self.access = access
        self.accessPerUser = accessPerUser
        self.requireTvm = requireTvm
        self.requireSubFragments = requireSubFragments
        self.allowRestart = allowRestart


class FragmentCodeAndMeta(JsonObject):
    def __init__(self,
                 metadata: FragmentMetadata,
                 sourceCode: Optional[str] = None,
                 byteCode: Optional[str] = None):
        self.metadata = metadata
        self.sourceCode = sourceCode
        self.byteCode = byteCode


class CodeBaseApi(object):
    def __init__(self, api_context: ApiContext):
        self._ctx = api_context

    @staticmethod
    def validate_sn_tag(sn_tag: SnapshotTag, optional=False):
        if sn_tag is None:
            if not optional:
                raise Exception("snapshot tag can't be None")
            return

        if '/' in sn_tag:
            raise Exception("invalid snapshot tag: `" + sn_tag + "`")

    def upload(self,
               sn_name: SnapshotName,
               sn_tag: SnapshotTag,
               path: FragmentPath,
               code: str,
               exec_types: Set[ExecutorType]):
        """
        Загрузка кода фрагмента.
        :param sn_name: имя снапшота
        :param sn_tag: тег снапшота
        :param path: путь фрагмента (начальный слеш не нужен)
        :param code: исходный код фрагмента
        """
        self._ctx.validate_id(sn_name)
        self.validate_sn_tag(sn_tag, optional=True)

        code_and_meta = FragmentCodeAndMeta(
            sourceCode = code,
            metadata = FragmentMetadata(
                isTextSource = True,
                executorTypes = exec_types
            )
        )

        if path.startswith("/"): path = path[1:]

        self._ctx.http_post(f'/cb/snapshots/{sn_name}/{sn_tag}/fragments/{path}', json=code_and_meta)

    def get_snapshots(self, name: Optional[SnapshotName] = None, owner: Optional[UserId] = None) -> List[SnapshotMeta]:
        if not owner:
            owner = self._ctx.user_id
        options = {}
        if name:
            options['snName'] = name
        response = self._ctx.http_get(f'/cb/users/{owner}/snapshots', data = options)
        sn_list = response.json()
        return [JsonObject.decode_from_json_dict(SnapshotMeta, sn) for sn in sn_list]

    def create_snapshot(self,
                        name: SnapshotName,
                        tag: Optional[SnapshotTag] = None,
                        fragmentHashes: Optional[Mapping[FragmentPath, str]] = None
                        ) -> NewSnapshotResponse:
        """
        Создание пустого снапшота.

        :param tag: если не указать будет сгенерирован
        :param fragmentHashes: хеши фрагментов которые в могут быть автоматически добавлены в новый снапшот.
        Ненайденные фрагменты будут указаны в поле ответа `notFound`
        """
        self._ctx.validate_id(name)
        self.validate_sn_tag(tag, optional=True)

        snapshot_meta = NewSnapshotMeta(
            name = name,
            tag = tag,
            removeAfterExecute = False,
            fragmentHashes = fragmentHashes
        )

        response = self._ctx.http_post('/cb/newSnapshot', json=snapshot_meta)
        json = response.json()
        sn_json = json['snapshot']

        apu_json = sn_json.get('accessPerUser')
        access_pre_user = None if (apu_json is None) else dict((k, AccessLevel(v)) for k, v in apu_json.pairs())

        return NewSnapshotResponse(
            snapshot = SnapshotMeta(
                name = sn_json['name'],
                tag = sn_json['tag'],
                frozen = sn_json.get('frozen'),
                removed = sn_json.get('removed'),
                created = sn_json.get('created'),
                expireAt = sn_json.get('expireAt'),
                owner = sn_json.get('owner'),
                accessLevel = AccessLevel(sn_json.get('accessLevel')),
                accessPerUser = access_pre_user
            ),
            notFound = json.get('notFound')
        )

    def freeze_snapshot(self, name: SnapshotName, tag: SnapshotTag):
        """
        "Заморозка" снапшота. После этого он становится неизменяемым и готовым к исполнению.
        """
        self._ctx.http_post(f'/cb/snapshots/{name}/{tag}/freeze')
