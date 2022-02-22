from __future__ import annotations

import uuid
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from threading import RLock
from typing import Callable, Dict, List, Optional


def _absorb(*args):
    pass


class GraphElement:
    def __init__(
        self,
        thread_pool: ThreadPoolExecutor,
        name: str = "",
        operation: Optional[Callable[[dict], dict]] = None,
    ):
        self.name = name or uuid.uuid4()[:8]
        self._in: List[Queue] = []
        self._out: Dict[Queue] = {}
        self._lock = RLock()
        self._operation = operation or _absorb
        self._next: List[GraphElement] = []
        self._thread_pool = thread_pool

    def _connect_in(self, other: GraphElement):
        q = Queue()
        with other._lock:
            other._out.update({self.name: q})
            other._next.append(self)
        self._in.append(q)

    def __rshift__(self, other: GraphElement):
        other._connect_in(self)

    def __call__(self, *args):
        with self._thread_pool as e:
            e.submit(self._f, *args)

    def _f(self, *args):
        raise NotImplementedError(
            "Should not use GraphElement, just subclasses that implement _f method"
        )


class HyperEdge(GraphElement):
    def _f(self, *args):
        kwargs = {}
        for q in self._in:
            pd = q.get()
            if not isinstance(pd, dict):
                raise TypeError(
                    f"Only dictionaries may be placed on Graph queues: found {pd} of type {type(pd)}"
                )
            kwargs.update(pd)
        res = self._operation(kwargs)
        for oq in self._out:
            oq.put(res)
        for n in self._next:
            n(self._out[n.name])


class Vertex(GraphElement):
    def _f(self, queue: Queue):
        if queue not in self._in:
            raise ValueError
        kwargs = queue.get()
        res = self._operation(kwargs)
        for oq in self._out:
            oq.put(res)
        for n in self._next:
            n()
