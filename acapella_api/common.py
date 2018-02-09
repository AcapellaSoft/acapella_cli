import inspect
import json
from enum import Enum
from typing import Optional, Dict, Any, Mapping, Type, Union


class JsonObject(object):
    def repr_json(self): return dict(filter(lambda x: not (x[1] is None), self.__dict__.items()))

    def to_json(self, formatted = False): return JsonObject.__encode_to_json(self, formatted)

    @staticmethod
    def __encode_to_json(obj: Any, formatted = False):
        class ComplexEncoder(json.JSONEncoder):
            def default(self, obj):
                if isinstance(obj, Enum):
                    return obj.value
                elif isinstance(obj, JsonObject):
                    return obj.repr_json()
                else:
                    return json.JSONEncoder.default(self, obj)

        if formatted:
            return json.dumps(obj.repr_json(), cls=ComplexEncoder, indent=4, sort_keys=True)
        else:
            return json.dumps(obj.repr_json(), cls=ComplexEncoder)

    @staticmethod
    def decode_from_json(cls, json_str: str):
        return JsonObject.decode_from_json_dict(cls, json.loads(json_str))

    __cache: Mapping[Type, inspect.FullArgSpec] = {}

    @staticmethod
    def __default_value_for(spec: inspect.FullArgSpec, arg_name: str):
        dlen = len(spec.defaults)
        if dlen == 0:
            return None
        index = spec.args.index(arg_name)
        offset = len(spec.args) - dlen
        def_index = index - offset
        if def_index >= 0:
            return spec.defaults[def_index]
        return None


    @staticmethod
    def decode_from_json_dict(cls, json_dict: dict):
        spec = JsonObject.__cache.get(cls)
        if spec is None:
            spec = inspect.getfullargspec(cls.__init__)
            JsonObject.__cache[cls] = spec

        if len(spec.annotations) != (len(spec.args) - 1):
            not_marked = []
            for a in spec.args:
                if (a != 'self') and (spec.annotations.get(a) is None):
                    not_marked.append(a)
            raise Exception('not annotated arguments in __init__ of ' + str(cls) + ': ' + ', '.join(not_marked))

        for name, t in spec.annotations.items():
            if t.__class__ == Union.__class__:
                if hasattr(t, '__union_params__'):
                    union_params = t.__union_params__ # Python 3.5
                else:
                    union_params = t.__args__ # Python 3.6+
                union_params = list(filter(lambda x: x != None.__class__, union_params))
                if len(union_params) > 1:
                    raise Exception('"Union" tags are not supported: ' + str(cls))
                t = union_params[0]

            val = json_dict.get(name)
            if (type(val) == dict) and (t.__class__ != Dict.__class__):
                val = JsonObject.decode_from_json_dict(t, val)
            if (val is None):
                val = JsonObject.__default_value_for(spec, name)
            json_dict[name] = val

        return cls(**json_dict)



UserId = str
TransactionId = str

# `User/SnapshotId/SnapshotTag:path/to/fragment.py`
FragmentReference = str

# Example: `255.0.13.17x20.10`, 17x20 == '17.17.17. ... .17' (x 20)
CompactDam = str


class AccessLevel(Enum):
    INVISIBLE = "Invisible"
    VISIBLE = "Visible"
    READONLY = "ReadOnly"
    READWRITE = "ReadWrite"
    READWRITEDELETE = "ReadWriteDelete"
    FULLACCESS = "FullAccess"


class SharableResource(JsonObject):
    def __init__(self,
                 owner: UserId,
                 access = AccessLevel.INVISIBLE,
                 accessPerUser: Optional[Dict[UserId, AccessLevel]] = None):
        self.owner = owner
        self.access = access
        self.accessPerUser = accessPerUser
