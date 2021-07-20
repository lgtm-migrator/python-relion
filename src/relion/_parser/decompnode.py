from relion.node import Node


class DecompNode(Node):
    def __hash__(self):
        return hash(("relion._parser.DecompNode", self.name))

    def func(self, *args, **kwargs):
        print(
            "decomp:",
            self.environment["data"],
            self.environment.get("unpack"),
            self.environment["job"],
        )  # , self.environment["data"][self.environment["job"]])
        if self.environment.get("unpack") is None:
            return None
        if (
            self.environment["labels"] is None
            or self.environment["meta_labels"] is None
        ):
            return None
        if self.environment.get("data") is None:
            return []
        job_data = self.environment["data"][self.environment["job"]]
        result = []
        for jd in job_data:
            for u in getattr(jd, self.environment["unpack"]):
                this_res = {}
                for ml in self.environment["meta_labels"]:
                    this_res[ml] = getattr(jd, ml)
                for l in self.environment["labels"]:
                    this_res[l] = getattr(u, l)
                result.append(this_res)
        return result
