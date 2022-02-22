from concurrent.futures import ThreadPoolExecutor

from relion.pipeline.graph import Graph, HyperEdge, Vertex


def test_a_simple_graph_run():
    def _a() -> dict:
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
    g = Graph(o)
    g()
    g.wait()
