from relion.node import Node


class DecompNode(Node):
    def __hash__(self):
        return hash(("relion._parser.DecompNode", self.name))

    def func(self, *args, **kwargs):
        if self.environment.get("unpack") is None:
            return None
        if (
            self.environment["labels"] is None
            or self.environment["meta_labels"] is None
        ):
            return None
        if self.environment.get("data") is None:
            return []
        result = []
        for u in getattr(self.environment["data"], self.environment["unpack"]):
            this_res = {}
            for ml in self.environment["meta_labels"]:
                this_res[ml] = getattr(self.environment["data"], ml)
            for l in self.environment["labels"]:
                this_res[l] = getattr(u, ml)
            result.append(this_res)
        return result
