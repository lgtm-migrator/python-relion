from relion.protonode.protograph import ProtoGraph


class DBGraph(ProtoGraph):
    def __init__(self, name, node_list, auto_connect=False):
        super().__init__(name, node_list, auto_connect=auto_connect)
        self.environment["source"] = self.name
        for n in self._node_list:
            for tab in n.tables:
                tab._last_update[self.name] = 0

    def update_times(self, source=None):
        times = []
        for n in self._node_list:
            times.extend(n.update_times(source))
        return times

    def message(self, constructor=None):
        messages = []
        for node in self._node_list:
            messages.extend(node.message, constructor)
        return messages
