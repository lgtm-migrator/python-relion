from unittest import mock

import pytest

from relion.node import Node
from relion.node.graph import Graph


@pytest.fixture
def _node():
    return Node("A")


@pytest.fixture
def _next_node_01():
    return Node("B")


@pytest.fixture
def _next_node_02():
    return Node("C")


@pytest.fixture
def _node_with_links(_node, _next_node_01, _next_node_02):
    _node.link_to(_next_node_01)
    _node.link_to(_next_node_02)
    return _node


@pytest.fixture
def graph(_node_with_links, _next_node_01, _next_node_02):
    node_links = [_node_with_links, _next_node_01, _next_node_02]
    return Graph("G", node_links)


@pytest.fixture
def overlapping_graph(_node):
    next_node = Node("D")
    _node.link_to(next_node)
    node_links = [_node, next_node]
    return Graph("OvG", node_links)


@pytest.fixture
def new_origin_graph(_next_node_02):
    node = Node("D")
    node.link_to(_next_node_02)
    node_links = [node, _next_node_02]
    return Graph("NOG", node_links)


@pytest.fixture
def no_link_graph(_next_node_01, _next_node_02):
    node = Node("A")
    node_links = [node, _next_node_01, _next_node_02]
    return Graph("G", node_links)


def test_process_graph_equality(graph, no_link_graph):
    assert not graph == no_link_graph
    no_link_graph._node_list[1].link_to(no_link_graph._node_list[2])
    no_link_graph._node_list[1].link_to(no_link_graph._node_list[3])
    no_link_graph._node_list[1].unlink_from(no_link_graph._end_node)
    no_link_graph._start_node.unlink_from(no_link_graph._node_list[2])
    no_link_graph._start_node.unlink_from(no_link_graph._node_list[3])
    assert graph == no_link_graph


def test_process_graph_length(graph):
    assert len(graph) == 5


def test_process_graph_access_to_elements_via_index(graph, _next_node_01):
    assert graph[2] == _next_node_01


def test_process_graph_extend_behaves_like_a_list_extend(graph):
    old_node_list = graph._node_list
    external_node_01 = Node("EN01")
    external_node_02 = Node("EN02")
    old_node_list.extend([external_node_01, external_node_02])
    graph.extend(Graph("temp", [external_node_01, external_node_02]))
    assert graph._node_list == old_node_list


def test_process_graph_can_get_the_index_of_a_provided_element(graph, _next_node_01):
    assert graph.index(_next_node_01) == 2
    assert graph.index(Node("B")) == 2


def test_process_graph_node_explore_collects_all_nodes_from_provided_node_onwards(
    graph, _node_with_links, _next_node_01, _next_node_02
):
    explored = []
    graph.node_explore(_next_node_01, explored)
    assert explored == [_next_node_01]
    explored = []
    graph.node_explore(_node_with_links, explored)
    assert explored == [_node_with_links, _next_node_01, _next_node_02]
    explored = []
    with pytest.raises(ValueError):
        graph.node_explore("A", explored)


def test_process_graph_add_node(graph, _node_with_links, _next_node_01, _next_node_02):
    new_node = Node("D")
    graph.add_node(new_node)
    assert graph._node_list == [
        graph._start_node,
        _node_with_links,
        _next_node_01,
        _next_node_02,
        graph._end_node,
        new_node,
    ]


def test_process_graph_remove_node_without_any_links(
    graph, _node_with_links, _next_node_01, _next_node_02
):
    graph.remove_node(_next_node_01)
    assert graph.nodes == [
        graph._start_node,
        _node_with_links,
        _next_node_02,
        graph._end_node,
    ]
    graph.remove_node(_next_node_02)
    assert graph.nodes == [graph._start_node, _node_with_links, graph._end_node]


def test_process_graph_link_from_to_does_the_linking_correctly(
    no_link_graph, _next_node_01
):
    node = Node("A")
    no_link_graph.link_from_to(node, _next_node_01)
    assert list(no_link_graph[0])[0] == _next_node_01


def test_process_graph_remove_node_and_check_links_still_work(
    _next_node_01, _next_node_02
):
    # Can't use graph fixture here as linking a child node to a new node wouldn't change the parent node in the way required for a fixture
    node = Node("A")
    node.link_to(_next_node_01)
    node.link_to(_next_node_02)
    new_graph = Graph("G01", [node, _next_node_01, _next_node_02])
    last_node = Node("D")
    new_graph.link_from_to(_next_node_01, last_node)
    new_graph.add_node(last_node)
    assert list(list(new_graph[0])[0])[1] == last_node
    new_graph.remove_node(new_graph[1])
    assert new_graph.nodes == [node, _next_node_02, last_node]
    assert list(new_graph[0])[1] == last_node


def test_process_graph_find_origins_finds_nodes_without_parents(
    graph, _node_with_links
):
    orgs = graph.find_origins()
    assert orgs == [_node_with_links]
    new_node = Node("D")
    graph.add_node(new_node)
    orgs = graph.find_origins()
    assert orgs == [_node_with_links, new_node]


def test_process_graph_merge_with_new_origin(graph, new_origin_graph):
    merged = graph.merge(new_origin_graph)
    assert merged
    assert len(graph) == 4
    assert list(graph[0])[1] == list(graph[3])[0]


def test_process_graph_merge_with_common_origin(graph, overlapping_graph):
    print(overlapping_graph[0]._out)
    print(overlapping_graph[0])
    print(graph[0]._out)
    print(graph[0])
    merged = graph.merge(overlapping_graph)
    assert merged
    assert len(graph) == 4
    assert len(graph[0]) == 3
    assert overlapping_graph[1] in graph[0]


def test_process_graph_merge_does_not_merge_if_merging_graph_is_not_connected_to_original_graph(
    graph,
):
    new_graph = Graph("new", [Node("E")])
    merged = graph.merge(new_graph)
    assert not merged


def test_calling_graph_gives_results_for_all_nodes(_node, graph):
    graph()
    assert len(graph._call_returns.values()) == len(graph.nodes)
    assert graph._call_returns[_node.nodeid] is None


@mock.patch("relion.node.graph.Digraph")
def test_process_graph_show_all_nodes(mock_Digraph, graph):
    graph.show()
    mock_Digraph.assert_called_once()
    mock_Digraph.return_value.attr.assert_called_once()
    nodecalls = [
        mock.call(name="A", shape="oval"),
        mock.call(name="B", shape="oval"),
        mock.call(name="C", shape="oval"),
    ]
    mock_Digraph.return_value.node.assert_has_calls(nodecalls)
    edgecalls = [
        mock.call("A", "B"),
        mock.call("A", "C"),
    ]
    mock_Digraph.return_value.edge.assert_has_calls(edgecalls)
    mock_Digraph.return_value.render.assert_called_once()
