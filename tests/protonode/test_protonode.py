import pytest
from relion.protonode.protonode import ProtoNode


@pytest.fixture
def node_with_links():
    node_A = ProtoNode("A")
    node_B = ProtoNode("B")
    node_C = ProtoNode("C")
    node_A.link_to(node_B)
    node_A.link_to(node_C)
    return node_A


def test_node_call_fills_completed_correctly():
    node_A = ProtoNode("A")
    node_B = ProtoNode("B")
    node_C = ProtoNode("C")
    node_A.link_to(node_B)
    node_A.link_to(node_C)
    node_A()
    assert node_B._completed == [node_A]
    assert node_C._completed == [node_A]


def test_node_environment_insert():
    node = ProtoNode("A")
    node.environment["test"] = 0
    assert node["test"] == 0


def test_unlink_from():
    node_A = ProtoNode("A")
    node_B = ProtoNode("B")
    node_A.link_to(node_B)
    assert node_A._out == [node_B]
    node_A.unlink_from(node_B)
    assert node_A._out == []


def test_node_call_adds_completed_status_to_out_nodes():
    node_A = ProtoNode("A")
    node_B = ProtoNode("B")
    node_C = ProtoNode("C")
    node_A.link_to(node_B)
    node_A.link_to(node_C)
    node_A()
    assert node_B._completed == [node_A]
    assert node_C._completed == [node_A]


def test_propagate_adds_correctly_to_environment():
    node_A = ProtoNode("A")
    node_A.environment["a"] = 1
    node_A.propagate(("a", "b"))
    assert node_A.environment["b"] == 1
    assert node_A.environment.propagate.store == {"b": 1}
