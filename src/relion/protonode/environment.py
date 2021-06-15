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
        else:
            return []

    def update(self, in_dict):
        self.store.update(in_dict)
        if not self.released:
            self.released = True


class Escalate:
    def __init__(self):
        self.store = {}
        self.released = False

    def __getitem__(self, key):
        return self.store[key]

    def __setitem__(self, key, value):
        if self.released:
            self.store[key] = value

    def start(self, esc):
        self.store = esc
        self.released = True


class Environment:
    def __init__(self, base=None):
        set_base(base, self)
        self.propagate = Propagate()
        self.escalate = Escalate()
        self.temp = {}

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
        if key in self.base.keys():
            self.base[key] = value
            return
        if key in self.temp.keys():
            self.temp[key] = value
            return
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
            if list(self.iterator) == [{}]:
                self.iterator = iter(traffic)
                return
            if len(list(traffic)) != len(list(self.iterator)):
                raise ValueError(
                    "Attempted to update ProtoNode Environment with a list that was a different size to the pre-existing iterator"
                )
            for i, tr in enumerate(traffic):
                self.iterator[i].update(tr)

    def set_escalate(self, esc):
        self.escalate.start(esc)

    def update_prop(self, prop):
        self.propagate.update(prop)

    def reset(self):
        self.iterator = iter([{}])

    # this only collects the environment from current level and one level above
    def dictionary(self, remove=None):
        res = {}
        if remove is None:
            remove = []
        if self.escalate.released:
            res.update(
                {
                    x: self.escalate.store.temp[x]
                    for x in self.escalate.store.temp.keys()
                    if x not in remove
                }
            )
            res.update(
                {
                    x: self.escalate.store.base[x]
                    for x in self.escalate.store.base.keys()
                    if x not in remove
                }
            )
        if self.propagate.released:
            res.update(
                {
                    x: self.propagate.store[x]
                    for x in self.propagate.store.keys()
                    if x not in remove
                }
            )
        res.update({x: self.temp[x] for x in self.temp.keys() if x not in remove})
        res.update({x: self.base[x] for x in self.base.keys() if x not in remove})
        return res


@functools.singledispatch
def set_base(base, env: Environment):
    raise TypeError(
        "For a ProtoNode Environment the base must be a dictionary or a list"
    )


@set_base.register(type(None))
def _(base: type(None), env: Environment):
    env.base = {}
    env.iterator = iter([{}])


@set_base.register(dict)
def _(base: dict, env: Environment):
    env.base = base
    env.iterator = iter([{}])


@set_base.register(list)
def _(base: list, env: Environment):
    env.base = {}
    env.iterator = iter(base)
