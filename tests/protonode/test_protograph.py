from relion.protonode.protonode import ProtoNode
from relion.protonode.protograph import ProtoGraph


def test_initialisation_with_list_of_nodes():
    node_A = ProtoNode("A")
    node_B = ProtoNode("B")
    node_C = ProtoNode("C")
    node_A.link_to(node_B)
    node_A.link_to(node_C)
    graph = ProtoGraph("G", [node_A, node_B, node_C])
    assert len(graph._node_list) == 3
    assert graph._node_list[0] == node_A
    assert graph._node_list[-1] == node_C


def test_add_node_to_graph():
    node_A = ProtoNode("A")
    graph = ProtoGraph("G", [node_A])
    assert len(graph._node_list) == 1
    node_B = ProtoNode("B")
    node_A.link_to(node_B)
    graph.add_node(node_B)
    assert len(graph._node_list) == 2
    assert graph._node_list[0] == node_A
    assert graph._node_list[1] == node_B
