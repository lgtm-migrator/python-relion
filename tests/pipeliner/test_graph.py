from concurrent.futures import ThreadPoolExecutor

from relion.pipeline.graph import Graph, HyperEdge, Vertex


def test_a_simple_graph_run():
    def _a(*args) -> dict:
        return {"a": 1, "b": 2}

    def _b(input: dict) -> dict:
        return {"res": input["a"] + input["b"]}

    vtp = ThreadPoolExecutor(max_workers=1)
    etp = ThreadPoolExecutor(max_workers=1)
    o = Vertex(vtp, operation=_a)
    e = HyperEdge(etp)
    v = Vertex(vtp, operation=_b)
    o >> e
    e >> v
    assert o._next == [e]
    g = Graph(o)
    g()
    results = g.wait()
    assert [{"res": 3}] in results


def test_a_graph_with_two_inputs_on_hyperedge():
    def _a() -> dict:
        return {"a": 1}

    def _b() -> dict:
        return {"b": 2}

    def _c(input: dict) -> dict:
        return {"res": input["a"] + input["b"]}

    vtp = ThreadPoolExecutor(max_workers=1)
    etp = ThreadPoolExecutor(max_workers=1)
    o = Vertex(vtp)
    eo = HyperEdge(etp)
    o_01 = Vertex(vtp, operation=_a)
    o_02 = Vertex(vtp, operation=_b)
    e = HyperEdge(etp)
    v = Vertex(vtp, operation=_c)
    o >> eo
    eo >> o_01
    eo >> o_02
    o_01 >> e
    o_02 >> e
    e >> v
    g = Graph(o)
    g()
    g.wait()
