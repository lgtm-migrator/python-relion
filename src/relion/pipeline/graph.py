from __future__ import annotations

import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from queue import Queue
from threading import RLock
from typing import Callable, Dict, List, Optional


def _absorb(**kwargs):
    pass


class GraphElement:
    def __init__(
        self,
        thread_pool: ThreadPoolExecutor,
        name: str = "",
        operation: Optional[Callable[[dict], dict]] = None,
    ):
        self.name = name or str(uuid.uuid4())[:8]
        self._in: List[Queue] = []
        self._out: Dict[Queue] = {}
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
    def _f(self):
        _kwargs = {}
        for q in self._in:
            pd = q.get()
            if not isinstance(pd, dict):
                raise TypeError(
                    f"Only dictionaries may be placed on Graph queues: found {pd} of type {type(pd)}"
                )
            _kwargs.update(pd)
        for oq in self._out:
            oq.put(_kwargs)
        for n in self._next:
            n(queue=self._out[n.name])


class Vertex(GraphElement):
    def _f(self, queue: Optional[Queue] = None):
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
            n()


class Graph:
    def __init__(self, origin: GraphElement):
        self._origin = origin
        self._elements: List[GraphElement] = self._generate_element_list()

    def __call__(self):
        self._origin()

    def _generate_element_list(
        self,
        element: Optional[List[GraphElement]] = None,
        el: Optional[List[GraphElement]] = None,
    ) -> List[GraphElement]:
        if not el:
            el = []
        if not element:
            element = self._origin
        for n in element._next:
            if n not in el:
                el.append(n)
                self._generate_element_list(element=n, el=el)
        return el

    def wait(self) -> List[List[dict]]:
        wait_results = []
        for e in self._elements:
            element_results = []
            for f in e._futures:
                element_results.append(f.result())
            wait_results.append(element_results)
        return wait_results
