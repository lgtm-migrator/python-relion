from concurrent.futures import ThreadPoolExecutor

from relion.pipeline.graph import (
    DecisionVertex,
    Graph,
    HyperEdge,
    HyperEdgeBundle,
    Vertex,
)


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
    assert [{"res": 3}] in results.values()


def test_a_graph_with_two_inputs_on_hyperedge():
    def _a(*args) -> dict:
        return {"a": 1}

    def _b(*args) -> dict:
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
    results = g.wait()
    assert [{"res": 3}] in results.values()


def test_a_graph_that_uses_a_hyper_edge_bundle():
    def _a(*args) -> dict:
        return {"a": [1, 2], "b": 2}

    def _b(input: dict) -> dict:
        return {"res": input["a"] + input["b"]}

    vtp = ThreadPoolExecutor(max_workers=1)
    etp = ThreadPoolExecutor(max_workers=1)
    o = Vertex(vtp, operation=_a)
    e = HyperEdgeBundle(etp, "a")
    v = Vertex(vtp, operation=_b)
    o >> e
    e >> v
    assert o._next == [e]
    g = Graph(o)
    g()
    results = g.wait()
    assert [{"res": 3}, {"res": 4}] in results.values()


def test_a_graph_that_uses_a_decision_node_and_fails():
    vtp = ThreadPoolExecutor(max_workers=1)
    etp = ThreadPoolExecutor(max_workers=1)
    o = Vertex(vtp)
    e_01 = HyperEdge(etp)
    d = DecisionVertex(vtp)
    e_02 = HyperEdge(etp)
    v = Vertex(vtp)
    o >> e_01
    e_01 >> d
    d >> e_02
    e_02 >> v
    g = Graph(o)
    g()
    results = g.wait()
    assert len(results) == 1


def test_a_graph_that_uses_a_decision_node_which_passes():
    vtp = ThreadPoolExecutor(max_workers=1)
    etp = ThreadPoolExecutor(max_workers=1)
    o = Vertex(vtp)
    e_01 = HyperEdge(etp)

    def _always_true(*args, **kwargs) -> bool:
        return True

    d = DecisionVertex(vtp, operation=_always_true)
    e_02 = HyperEdge(etp)
    v = Vertex(vtp)
    o >> e_01
    e_01 >> d
    d >> e_02
    e_02 >> v
    g = Graph(o)
    g()
    results = g.wait()
    assert len(results) == 2
