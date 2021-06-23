from relion.dbmodel.dbnode import DBNode
from relion.dbmodel.modeltables import MotionCorrectionTable
from relion.dbmodel.dbgraph import DBGraph


def test_initialisation_of_dbgraph_with_list_of_nodes():
    node_A = DBNode("A", [MotionCorrectionTable()])
    node_B = DBNode("B", [MotionCorrectionTable()])
    node_C = DBNode("C", [MotionCorrectionTable()])
    node_A.link_to(node_B)
    node_A.link_to(node_C)
    graph = DBGraph("G", [node_A, node_B, node_C])
    assert len(graph._node_list) == 3
    assert graph._node_list[0] == node_A
    assert graph._node_list[-1] == node_C
    assert graph.environment["source"] == "G"
