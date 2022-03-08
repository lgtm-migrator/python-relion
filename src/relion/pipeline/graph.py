from __future__ import annotations

import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from queue import Queue
from threading import RLock
from typing import Any, Callable, Dict, List, Optional, Union


def _absorb(*args):
    return {}


class GraphElement:
    def __init__(
        self,
        thread_pool: ThreadPoolExecutor,
        name: str = "",
        operation: Optional[Callable[[dict], Union[dict, bool]]] = None,
    ):
        self.name = name or str(uuid.uuid4())[:8]
        self._in: List[Queue] = []
        self._out: Dict[str, Queue] = {}
        self._lock = RLock()
        self._operation = operation or _absorb
        self._next: List[GraphElement] = []
        self._thread_pool = thread_pool
        self._futures: List[Future] = []

    def _connect_in(self, other: GraphElement):
        q = Queue()
        with other._lock:
            other._out.update({self.name: q})
            other._next.append(self)
        self._in.append(q)

    def __rshift__(self, other: GraphElement):
        other._connect_in(self)

    def __call__(self, **kwargs):
        with self._lock:
            if kwargs:
                fut = self._thread_pool.submit(self._f, **kwargs)
            else:
                try:
                    fut = self._thread_pool.submit(self._f)
                except Exception as e:
                    print(f"exception raised {e}")
            self._futures.append(fut)

    def _f(self, **kwargs):
        raise NotImplementedError(
            "Should not use GraphElement, just subclasses that implement _f method"
        )


class HyperEdge(GraphElement):
    # only run the HyperEdge if it hasn't been run before
    def __call__(self, **kwargs):
        with self._lock:
            if not self._futures:
                super().__call__(**kwargs)

    def _f(self, gatherer: Optional[Dict[str, List[dict]]] = None):
        _kwargs = {}
        for q in self._in:
            pd = q.get()
            if not isinstance(pd, dict):
                raise TypeError(
                    f"Only dictionaries may be placed on Graph queues: found {pd} of type {type(pd)}"
                )
            _kwargs.update(pd)
        for oq in self._out.values():
            oq.put(_kwargs)
        for n in self._next:
            n(queue=self._out[n.name], gatherer=gatherer)


class HyperEdgeBundle(GraphElement):
    def __init__(
        self,
        thread_pool: ThreadPoolExecutor,
        generator_key: Any,
        name: str = "",
        operation: Optional[Callable[[dict], Union[dict, bool]]] = None,
    ):
        super().__init__(thread_pool, name=name, operation=operation)
        self._generator_key = generator_key

    def _f(self, gatherer: Optional[Dict[str, List[dict]]] = None):
        _kwargs = {}
        for q in self._in:
            pd = q.get()
            if not isinstance(pd, dict):
                raise TypeError(
                    f"Only dictionaries may be placed on Graph queues: found {pd} of type {type(pd)}"
                )
            _kwargs.update(pd)
        edges = _kwargs.pop(self._generator_key)
        for e in edges:
            for oq in self._out.values():
                oq.put({**_kwargs, self._generator_key: e})
            for n in self._next:
                n(queue=self._out[n.name], gatherer=gatherer)


class Vertex(GraphElement):
    def _f(
        self,
        queue: Optional[Queue] = None,
        gatherer: Optional[Dict[str, List[dict]]] = None,
    ) -> dict:
        if queue:
            if queue not in self._in:
                raise ValueError
            kwargs = queue.get()
        else:
            kwargs = {}
        res = self._operation(kwargs)
        for oq in self._out.values():
            oq.put(res)
        for n in self._next:
            n(gatherer=gatherer)
        if gatherer is not None:
            try:
                gatherer[self.name].append(res)
            except KeyError:
                gatherer[self.name] = [res]
        return res


class DecisionVertex(GraphElement):
    def __init__(
        self,
        thread_pool: ThreadPoolExecutor,
        name: str = "",
        operation: Optional[Callable[[dict], bool]] = None,
    ):
        def _always_false(*args, **kwargs) -> bool:
            return False

        super().__init__(thread_pool, name=name, operation=operation or _always_false)

    def _f(
        self,
        queue: Optional[Queue] = None,
        gatherer: Optional[Dict[str, List[dict]]] = None,
    ):
        if queue:
            if queue not in self._in:
                raise ValueError
            kwargs = queue.get()
        else:
            kwargs = {}
        res = self._operation(kwargs)
        for oq in self._out.values():
            oq.put({})
        if res:
            for n in self._next:
                n(gatherer=gatherer)
        else:
            _remove_futures(self)


def _remove_futures(dv: GraphElement):
    for n in dv._next:
        if isinstance(n, TerminalVertex):
            dv._out[n.name].put({})
            if not n._futures:
                n(queue=dv._out[n.name])
            continue
        _remove_futures(n)


class TerminalVertex(GraphElement):
    def __init__(
        self,
        thread_pool: ThreadPoolExecutor,
        name: str = "",
        operation: Optional[Callable[[dict], Union[dict, bool]]] = None,
    ):
        super().__init__(thread_pool, name=name, operation=operation)
        self._seen_queues: List[Queue] = []

    def _f(self, queue: Optional[Queue] = None, **kwargs):
        if queue:
            if queue not in self._in:
                raise ValueError
            if queue in self._seen_queues:
                raise ValueError(f"Queue {queue} has already been seen by {self}")
            queue.get()
            self._seen_queues.append(queue)
            if len(self._seen_queues) == len(self._in):
                for oq in self._out.values():
                    oq.put(True)

    def wait(self) -> bool:
        completed: List[bool] = []
        for oq in self._out.values():
            completed.append(oq.get())
        return all(completed)


class Graph:
    def __init__(self, origin: GraphElement):
        self._origin = origin
        self._elements: List[GraphElement] = self._generate_element_list()
        tpe = ThreadPoolExecutor(max_workers=1)
        self._terminal: TerminalVertex = TerminalVertex(tpe)
        self._gatherer: Dict[str, List[dict]] = {}
        for e in self._elements:
            if not e._out:
                he_out = HyperEdge(tpe)
                e >> he_out
                he_out >> self._terminal
                q = Queue()
                self._terminal._out.update({he_out.name: q})

    def __call__(self):
        self._origin(gatherer=self._gatherer)

    def _generate_element_list(
        self,
        element: Optional[List[GraphElement]] = None,
        el: Optional[List[GraphElement]] = None,
    ) -> List[GraphElement]:
        if not el:
            el = []
        if not element:
            element = self._origin
            el = [self._origin]
        for n in element._next:
            if n not in el:
                el.append(n)
                self._generate_element_list(element=n, el=el)
        return el

    def wait(self) -> Dict[List[dict]]:
        completed = self._terminal.wait()
        if not completed:
            raise ValueError(f"Terminal for {self} did not complete correctly")
        for el in self._elements:
            for fut in el._futures:
                fut.result()
        return self._gatherer
