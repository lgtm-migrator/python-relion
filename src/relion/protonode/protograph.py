from relion.protonode.protonode import ProtoNode

try:
    from graphviz import Digraph
except ImportError:
    pass


class ProtoGraph(ProtoNode):
    def __init__(self, name, node_list, auto_connect=False):
        super().__init__(name)
        self._node_list = node_list
        try:
            self.origins = [node_list[0]]
        except IndexError:
            self.origins = []
        self._call_returns = {}
        self._called_nodes = []
        self._traversed = []
        if auto_connect:
            self._check_connections()

    def __eq__(self, other):
        if isinstance(other, ProtoGraph):
            if len(self) == len(other):
                for n in self:
                    if n not in other:
                        return False
                return True
        return False

    def __hash__(self):
        return hash(("relion.protonode.protograph.ProtoGraph", iter(self._node_list)))

    def __len__(self):
        return len(self._node_list)

    def __getitem__(self, index):
        if not isinstance(index, int):
            raise ValueError("Index of ProtoGraph must be an integer")
        return self._node_list[index]

    def __call__(self, **kwargs):
        self.environment.update(kwargs)
        self._call_returns = {}
        self.traverse()
        self._traversed = []
        super().__call__()
        self._called_nodes = []
        if self._call_returns == {}:
            return
        else:
            return self._call_returns

    def _check_connections(self):
        for node in self._node_list:
            for i_node in node._in:
                if i_node not in self._node_list:
                    i_node.link_to(self)
                    i_node._link_traffic[(self.name, self.nodeid)].update(
                        i_node._link_traffic[(node.name, node.nodeid)]
                    )

    def extend(self, other):
        if not isinstance(other, ProtoGraph):
            raise ValueError("Can only extend a ProtoGraph with another ProtoGraph")
        self._node_list.extend(other._node_list)

    def index(self, node):
        return self._node_list.index(node)

    def link_from_to(self, from_node, to_node):
        self[self.index(from_node)].link_to(to_node)

    def node_explore(self, node, explored):
        if not isinstance(node, ProtoNode):
            raise ValueError(
                "ProtoGraph.node_explore must be called with a ProtoNode as the starting point; a string or similar is insufficient"
            )
        if node not in explored:
            explored.append(node)
        for next_node in node:
            self.node_explore(next_node, explored)

    def add_node(self, new_node, auto_connect=False):
        if isinstance(new_node, ProtoNode):
            self._node_list.append(new_node)
            if auto_connect:
                for i_node in new_node._in:
                    if i_node not in self._node_list:
                        i_node.link_to(self)
        else:
            raise ValueError("Attempted to add a node that was not a ProtoNode")

    def remove_node(self, node_name):
        behind_nodes = []
        for currnode in self:
            if node_name in currnode:
                behind_nodes.append(currnode)
                currnode.unlink_from(node_name)
            if node_name in currnode._in:
                currnode._in.remove(node_name)
        for bnode in behind_nodes:
            for next_node in self._node_list[self._node_list.index(node_name)]:
                bnode.link_to(next_node)
        self._node_list.remove(node_name)

    def find_origins(self):
        child_nodes = []
        for node in self:
            child_nodes.extend([next_node for next_node in node])
        origins = [p for p in self if p not in child_nodes]
        return origins

    def merge(self, other):
        node_names = [p._name for p in self]
        other_names = [p._name for p in other]
        if len(set(node_names).intersection(set(other_names))) > 0:
            for new_node in other:
                if new_node._name not in self:
                    self.add_node(new_node)
                else:
                    for next_node in new_node:
                        if next_node._name not in self[self.index(new_node._name)]:
                            self[self.index(new_node._name)].link_to(next_node)
            return True
        else:
            return False

    def traverse(self):
        for o in self.origins:
            self._follow(o, traffic={})

    def _follow(self, node, traffic):
        called = False
        if node not in self._node_list:
            return
        if node.nodeid in self._called_nodes:
            called = True
        if (
            all(n in node._completed for n in node._in)
            and node.nodeid not in self._called_nodes
        ):
            called = True
            if traffic == {} and isinstance(node._delayed_traffic, dict):
                self._call_returns[node.name + "-" + node.nodeid] = node(
                    **{
                        **self.environment,
                        **node.environment,
                        **node._delayed_traffic,
                        **self._propagate,
                        **node._propagate,
                    }
                )
                self._called_nodes.append(node.nodeid)
            elif isinstance(traffic, dict) and isinstance(node._delayed_traffic, dict):
                for key, value in traffic.items():
                    node.environment[key] = value
                self._call_returns[node.name + "-" + node.nodeid] = node(
                    **{
                        **self.environment,
                        **node.environment,
                        **traffic,
                        **node._delayed_traffic,
                        **self._propagate,
                        **node._propagate,
                    }
                )
                self._called_nodes.append(node.nodeid)
            elif isinstance(traffic, list) and isinstance(node._delayed_traffic, dict):
                self._call_returns[node.name + "-" + node.nodeid] = []
                for titem in traffic:
                    for key, value in titem.items():
                        node.environment[key] = value
                    self._call_returns[node.name + "-" + node.nodeid].append(
                        node(
                            **{
                                **self.environment,
                                **node.environment,
                                **titem,
                                **node._delayed_traffic,
                                **self._propagate,
                                **node._propagate,
                            }
                        )
                    )
                self._called_nodes.append(node.nodeid)
            elif isinstance(traffic, dict) and isinstance(node._delayed_traffic, list):
                self._call_returns[node.name + "-" + node.nodeid] = []
                for titem in node._delayed_traffic:
                    for key, value in titem.items():
                        node.environment[key] = value
                    self._call_returns[node.name + "-" + node.nodeid].append(
                        node(
                            **{
                                **self.environment,
                                **node.environment,
                                **titem,
                                **traffic,
                                **self._propagate,
                                **node._propagate,
                            }
                        )
                    )
                self._called_nodes.append(node.nodeid)
        elif isinstance(traffic, dict):
            if isinstance(node._delayed_traffic, list):
                node._delayed_traffic = [
                    {**r, **traffic} for r in node._delayed_traffic
                ]
            else:
                node._delayed_traffic.update(traffic)
        elif isinstance(traffic, list):
            if isinstance(node._delayed_traffic, dict):
                node._delayed_traffic = []
            node._delayed_traffic.extend(traffic)
        for next_node in node._out:
            next_node._propagate.update(node._propagate)
            try:
                next_traffic = node._link_traffic[(next_node.name, next_node.nodeid)]
            except KeyError:
                next_traffic = {}
            if next_traffic is None:
                next_traffic = self._call_returns[node.name + "-" + node.nodeid]
            if (
                (node.name, node.nodeid),
                (next_node.name, next_node.nodeid),
            ) not in self._traversed and called:
                self._traversed.append(
                    ((node.name, node.nodeid), (next_node.name, next_node.nodeid))
                )
                self._follow(next_node, next_traffic)
            elif not called:
                self._follow(next_node, next_traffic)

    def show(self):
        try:
            digraph = Digraph(format="svg", strict=True)
            digraph.attr(rankdir="LR", splines="ortho")
            for node in self._node_list:
                if isinstance(node, ProtoGraph):
                    digraph.node(name=str(node.name), shape="box")
                    for gnode in node._node_list:
                        digraph.edge(str(node.name), str(gnode.name), style="dashed")
                        for next_gnode in gnode:
                            digraph.edge(str(gnode.name), str(next_gnode.name))
                else:
                    digraph.node(name=str(node.name))
                for next_node in node:
                    if next_node in self._node_list:
                        digraph.edge(str(node.name), str(next_node.name))
            digraph.render(f"{self.name}.gv")
        except Exception:
            raise Warning(
                "Failed to create nodes display. Your environment may not have graphviz available."
            )
