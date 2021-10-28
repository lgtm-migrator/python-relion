import copy
import threading

# import time
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from relion.node import Node

try:
    from graphviz import Digraph
except ImportError:
    pass


class Graph(Node):
    def __init__(
        self, name, node_list, auto_connect=False, pool_size=5, node_type=Node
    ):
        super().__init__(name)
        self._start_node = node_type(f"{name}_start")
        self._end_node = node_type(f"{name}_end")
        self._node_list = [self._start_node] + node_list + [self._end_node]
        try:
            self.origins = self.find_origins()
        except IndexError:
            self.origins = []
        self._in = self._start_node._in
        self._completed = self._start_node._completed
        self._out = self._end_node._out
        [self._start_node.link_to(o) for o in self.origins]
        [
            n.link_to(self._end_node)
            for n in self._node_list
            if not n._out and n != self._end_node
        ]
        self._call_returns = {}
        self._called_nodes = []
        self._called_nodes_names = []
        self._traversed = []
        self._pool_size = pool_size
        self._running = []
        if auto_connect:
            self._check_connections()

    def __eq__(self, other):
        if isinstance(other, Graph):
            if len(self.nodes) == len(other.nodes):
                for n in self.nodes:
                    if n not in other.nodes:
                        return False
                return True
        return False

    def __hash__(self):
        return hash(
            ("relion.node.graph.Graph", self._name, iter(self._node_list), self.nodeid)
        )

    def __len__(self):
        return len(self._node_list)

    def __getitem__(self, index):
        if not isinstance(index, int):
            raise ValueError("Index of Graph must be an integer")
        return self._node_list[index]

    def __call__(self, *args, **kwargs):
        for node in self._node_list:
            node.environment.set_escalate(self.environment)
        res = super().__call__(*args, **kwargs)
        return res

    @property
    def nodes(self):
        return self._node_list

    def func(self, *args, **kwargs):
        self._call_returns = {}
        if self._in_multi_call:
            for node in self.origins:
                node._completed = self._completed
        if kwargs.get("__lock__"):
            lock = kwargs["__lock__"]
        else:
            lock = threading.RLock()
        self.traverse(lock, pool=kwargs.get("__pool__"))
        self._traversed = []
        self._running = []
        self._called_nodes = []
        for node in self.nodes:
            node.environment.reset()
            node._completed = []
        if self._call_returns == {}:
            return None
        else:
            return self._call_returns

    def _check_connections(self):
        for node in self._node_list:
            for i_node in node._in:
                if i_node not in self._node_list:
                    i_node.link_to(self)
                    i_node._link_traffic[self.nodeid].update(
                        i_node._link_traffic[node.nodeid]
                    )

    def strip(self):
        stripped = copy.deepcopy(self)
        # print("stripping", stripped._node_list)
        stripped.remove_node(stripped._start_node.name)
        stripped.remove_node(stripped._end_node.name)
        return stripped

    def extend(self, other):
        if not isinstance(other, Graph):
            raise ValueError("Can only extend a Graph with another Graph")
        self._node_list.extend(other._node_list)
        try:
            self.remove_node(other._start_node.name)
            self.remove_node(other._end_node.name)
        except ValueError:
            pass

    def index(self, node):
        try:
            return self._node_list.index(node)
        except ValueError:
            for i, n in enumerate(self._node_list):
                if node.name == n.name:
                    return i

    def link_from_to(self, from_node, to_node):
        self[self.index(from_node)].link_to(to_node)
        for n in self._start_node:
            self._start_node.unlink_from(n)
        [self._start_node.link_to(o) for o in self.find_origins()]
        for n in self._end_node._in:
            n.unlink_from(self._end_node)
        [
            n.link_to(self._end_node)
            for n in self._node_list
            if not n._out and n != self._end_node
        ]

    def node_explore(self, node, explored):
        if not isinstance(node, Node):
            raise ValueError(
                "Graph.node_explore must be called with a Node as the starting point; a string or similar is insufficient"
            )
        if node not in explored and node not in (self._start_node, self._end_node):
            explored.append(node)
        for next_node in node:
            self.node_explore(next_node, explored)

    def add_node(self, new_node, auto_connect=False):
        if isinstance(new_node, Node):
            if new_node not in self._node_list:
                self._node_list.append(new_node)
            if auto_connect:
                for i_node in new_node._in:
                    if i_node not in self._node_list:
                        i_node.link_to(self)
            new_node.environment.set_escalate(self.environment)
            for n in self._start_node:
                self._start_node.unlink_from(n)
            [self._start_node.link_to(o) for o in self.find_origins()]
            for n in self._end_node._in:
                n.unlink_from(self._end_node)
            [
                n.link_to(self._end_node)
                for n in self._node_list
                if not n._out and n != self._end_node
            ]
        else:
            raise ValueError("Attempted to add a node that was not a Node")

    def remove_node(self, node_name, advance=False):
        behind_nodes = []
        for currnode in self._node_list:
            if currnode.name == str(node_name):
                if currnode.environment.propagate.released:
                    for next_node in currnode:
                        next_node.environment.update_prop(
                            currnode.environment.propagate.store
                        )
                        if advance:
                            next_node.environment.update(
                                currnode.environment.propagate.store
                            )
            if str(node_name) in [n.name for n in currnode]:
                behind_nodes.append(currnode)
                currnode.unlink_from(
                    self._node_list[self._node_list.index(node_name)], use_name=True
                )
            if node_name in currnode._in:
                currnode._in.remove(node_name)
        for bnode in behind_nodes:
            for next_node in self._node_list[self._node_list.index(node_name)]:
                bnode.link_to(next_node)
        # print("in remove_node", self._node_list, node_name)
        self._node_list.remove(self._node_list[self._node_list.index(node_name)])

    def find_origins(self, verbose=False):
        child_nodes = []
        for node in self.nodes:
            # print("is it start or end?", node, node not in (self._start_node, self._end_node))
            if node not in (self._start_node, self._end_node):
                child_nodes.extend(node)
        if verbose:
            print("not origins", self, child_nodes)
        origins = [
            p
            for p in self.nodes
            if p not in child_nodes and p not in (self._start_node, self._end_node)
        ]
        return origins

    def _top_and_tail(self) -> list:
        for n in self._start_node:
            try:
                n._in.remove(self._start_node)
            except ValueError:
                print(f"{self._start_node} not in {n._in}", self)
                raise
        for n in self._end_node._in:
            n._out.remove(self._end_node)
        return [n for n in self.nodes if n not in (self._start_node, self._end_node)]

    def merge(self, other):
        node_names = [p._name for p in self.nodes]
        other_names = [p._name for p in other.nodes]
        if len(set(node_names).intersection(set(other_names))) > 0:
            for new_node in other._top_and_tail():
                if new_node not in self.nodes:
                    self.add_node(new_node)
                else:
                    for next_node in new_node:
                        if next_node not in self.nodes[self.index(new_node)]:
                            self.nodes[self.index(new_node)].link_to(next_node)
            [self._start_node.link_to(o) for o in other.origins]
            return True
        else:
            return False

    def traverse(
        self,
        lock: threading.RLock,
        pool: Optional[ThreadPoolExecutor] = None,
        run_within=None,
    ):
        if pool is None:
            with ThreadPoolExecutor(max_workers=self._pool_size) as _pool:
                # self._running = [
                #    (
                #        _pool.submit(
                #            self._follow, o, {}, [], _pool, lock, append=o._can_append
                #        ),
                #        o,
                #    )
                #    for o in self.origins
                # ]
                print("running graph", self)
                self._running = [
                    (
                        _pool.submit(
                            self._follow, self._start_node, {}, [], _pool, lock, self
                        ),
                        self._start_node,
                    )
                ]
                _start = 0
                unfinished = True

                while unfinished:
                    # print("waiting for", [p[1] for p in self._running[:_start]], unfinished)
                    # runners = self._running[_start:]
                    runners = self._running
                    print("waiting for", [p[1] for p in runners])
                    for r in runners:
                        r[0].result()
                        if r[1] == self._end_node:
                            # print("while finishing", self._running[_start:])
                            # time.sleep(5)
                            unfinished = False
                        _start += 1

        else:
            # self._running = [
            #    (
            #        pool.submit(
            #            self._follow, o, {}, [], pool, lock, append=o._can_append
            #        ),
            #        o,
            #    )
            #    for o in self.origins
            # ]
            if run_within is None:
                # print("running graph", self)
                self._running = [
                    (
                        pool.submit(
                            self._follow, self._start_node, {}, [], pool, lock, self
                        ),
                        self._start_node,
                    )
                ]
            else:
                print("run_within provided")
                self._running = [
                    (
                        pool.submit(
                            self._follow,
                            self._start_node,
                            {},
                            [],
                            pool,
                            lock,
                            run_within,
                        ),
                        self._start_node,
                    )
                ]

    def _follow(
        self, node, traffic, share, pool, lock, run_within, run=True, append=False
    ):
        called = False
        if node not in self.nodes:
            return
        node.environment.update(traffic, can_append_list=append)

        for sh in share:
            node.environment[sh[1]] = sh[0]

        if (
            all(n in node._completed for n in node._in)
            and node.nodeid not in self._called_nodes
            and run
        ):
            called = True

            self._call_returns[node.nodeid] = node(__lock__=lock, __pool__=pool)
            self._called_nodes.append(node.nodeid)
            self._called_nodes_names.append(node.name)

        if node.name == "2DClassificationTables_start":
            print(
                "calling?",
                all(n in node._completed for n in node._in),
                node.nodeid not in self._called_nodes,
                run,
                node._completed,
                node._in,
                self._completed,
            )
        if node.name == "3DClassificationTables":
            print(
                "node seen",
                node,
                node._out,
                run,
                all(n in node._completed for n in node._in),
                node._completed,
                node._in,
                node.nodeid not in self._called_nodes,
            )
        for next_node in node:
            if node.name == "2DClassificationTables_start":
                print(
                    "not going to run",
                    next_node,
                    len([p for p in self._traversed if p[1] == next_node.nodeid]),
                    len([p for p in run_within._traversed if p[1] == next_node.nodeid]),
                    next_node._in,
                    (node.nodeid, next_node.nodeid) not in self._traversed,
                    called,
                )
            with lock:
                # print(node, next_node, "in lock")
                next_node.environment.update_prop(node.environment.propagate)
                next_traffic = node._link_traffic.get(next_node.nodeid, {})
                if next_traffic is None:
                    next_traffic = self._call_returns.get(node.nodeid) or {}
                next_share = []
                if node._share_traffic.get(next_node.nodeid) is not None:
                    for sh in node._share_traffic[next_node.nodeid]:
                        next_share.append((node.environment[sh[0]], sh[1]))
                # print(node, next_node, "out lock")
                if (node.nodeid, next_node.nodeid) not in self._traversed and called:
                    run_next = (
                        len([p for p in self._traversed if p[1] == next_node.nodeid])
                        == len(next_node._in) - 1
                    )
                    self._traversed.append((node.nodeid, next_node.nodeid))
                    if run_next:
                        print("append", next_node)
                        run_within._running.append(
                            (
                                pool.submit(
                                    self._follow,
                                    next_node,
                                    next_traffic,
                                    next_share,
                                    pool,
                                    lock,
                                    run_within,
                                    run=run_next,
                                    append=node._can_append,
                                ),
                                next_node,
                            )
                        )
                        # print("now", run_within._running)
                    else:
                        pool.submit(
                            self._follow,
                            next_node,
                            next_traffic,
                            next_share,
                            pool,
                            lock,
                            run_within,
                            run=run_next,
                            append=node._can_append,
                        )

    def show(self):
        try:
            digraph = Digraph(format="svg", strict=True)
            digraph.attr(rankdir="LR", splines="ortho")
            for node in self._node_list:
                if isinstance(node, Graph):
                    digraph.node(name=str(node.name), shape="box")
                    for gnode in node._node_list:
                        if isinstance(gnode, Graph):
                            digraph.node(name=str(gnode.name), shape="box")
                        else:
                            digraph.node(name=str(gnode.name), shape=gnode.shape)
                        digraph.edge(str(node.name), str(gnode.name), style="dashed")
                        for next_gnode in gnode:
                            digraph.edge(str(gnode.name), str(next_gnode.name))
                else:
                    digraph.node(name=str(node.name), shape=node.shape)
                for next_node in node:
                    if next_node in self._node_list:
                        digraph.edge(str(node.name), str(next_node.name))
            digraph.render(f"{self.name}.gv")
        except Exception:
            raise Warning(
                "Failed to create nodes display. Your environment may not have graphviz available."
            )
