import functools
import uuid


@functools.total_ordering
class ProtoNode:
    def __init__(self, name, **kwargs):
        self._name = name
        self.nodeid = str(uuid.uuid4())
        self._out = []
        self._in = []
        self._completed = []
        self.attributes = {}
        self.environment = {}
        self._link_traffic = {}
        self._delayed_traffic = {}
        for key, value in kwargs.items():
            self.attributes[key] = value

    def __eq__(self, other):
        if isinstance(other, ProtoNode):
            if self.name == other.name and len(self._out) == len(other._out):
                for n in self._out:
                    if n not in other._out:
                        return False
                return True
        return False

    def __hash__(self):
        return hash(
            ("relion.protonode.protonode.ProtoNode", self._name, tuple(self._out))
        )

    def __repr__(self):
        return f"Node({repr(str(self._name))})"

    def __iter__(self):
        return iter(self._out)

    def __len__(self):
        return len(self._out)

    def __lt__(self, other):
        if self._is_child(other):
            return True
        return False

    def __rshift__(self, other):
        if isinstance(other, ProtoNode):
            self.link_to(other)

    def __call__(self, **kwargs):
        for node in self._out:
            node._completed.append(self)

    @property
    def name(self):
        return str(self._name)

    def change_name(self, new_name):
        self._name = new_name

    def link_to(self, next_node, traffic={}, block=True, result_as_traffic=False):
        if next_node not in self._out:
            self._out.append(next_node)
            next_node._in.append(self)
            if traffic != {}:
                self._link_traffic[(next_node.name, next_node.nodeid)] = traffic
            if not block:
                for k, v in traffic.items():
                    next_node.environment[k] = v
            if result_as_traffic:
                self._link_traffic[(next_node.name, next_node.nodeid)] = None

    def unlink_from(self, next_node):
        if next_node in self._out:
            self._out.remove(next_node)

    def _is_child_checker(self, possible_child, checks):
        if self == possible_child:
            checks.extend([True])
        for child in self:
            checks.extend(child._is_child_checker(possible_child, checks=checks))
        return checks

    def _is_child(self, possible_child):
        if True in self._is_child_checker(possible_child, checks=[]):
            return True
        else:
            return False
