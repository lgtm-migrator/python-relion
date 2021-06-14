from _typeshed import NoneType
import functools


class Propagate:
    def __init__(self):
        self.store = {}
        self.released = False

    def __getitem__(self, key):
        return self.store[key]

    def __setitem__(self, key, value):
        self.store[key] = value
        if not self.released:
            self.released = True

    def keys(self):
        if self.released:
            return self.store.keys()


class Escalate:
    def __init__(self):
        self.store = Environment()
        self.released = False

    def __getitem__(self, key):
        return self.store[key]

    def __setitem__(self, key, value):
        self.store[key] = value
        if not self.released:
            self.released = True


class Environment:
    def __init__(self, base=None):
        set_base(base, self)
        self.propagate = Propagate()
        self.escalate = Escalate()
        self.step()

    def __getitem__(self, key):
        if key in self.base.keys():
            return self.base[key]
        if key in self.temp.keys():
            return self.temp[key]
        if self.propagate.released:
            if key in self.propagate.keys():
                return self.propagate[key]
        if self.escalate.released:
            return self.escalate[key]

    def __setitem__(self, key, value):
        self.base[key] = value

    def step(self):
        try:
            self.temp = next(self.iterator)
            return True
        except StopIteration:
            return False

    def update(self, traffic):
        if isinstance(traffic, dict):
            self.base.update(traffic)
            return
        elif isinstance(traffic, list):
            if self.iterator == [{}]:
                self.iterator = traffic
                return
            if len(traffic) != len(self.iterator):
                raise ValueError(
                    "Attempted to update ProtoNode Environment with a list that was a different size to the pre-existing iterator"
                )
            for i, tr in enumerate(traffic):
                self.iterator[i].update(tr)


@functools.singledispatch
def set_base(base, env: Environment):
    raise TypeError(
        "For a ProtoNode Environment the base must be a dictionary or a list"
    )


@set_base.register(NoneType)
def _(base: NoneType, env: Environment):
    env.base = {}
    env.iterator = [{}]


@set_base.register(dict)
def _(base: dict, env: Environment):
    env.base = base
    env.iterator = [{}]


@set_base.register(list)
def _(base: list, env: Environment):
    env.base = {}
    env.iterator = base
