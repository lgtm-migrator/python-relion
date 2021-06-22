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


# def test_
