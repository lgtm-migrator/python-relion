from relion.node import Node


class ClusterNode(Node):
    def __init__(self, name, cluster_job_info, **kwargs):
        super().__init__(name, **kwargs)
        self._cluster_job_info = cluster_job_info

    def func(self, *args, **kwargs):
        res = []
        for info in self._cluster_job_info:
            for job_id, count in zip(info.job_ids, info.micrograph_counts):
                this_res = {}
                this_res["job_id"] = job_id
                this_res["micrograph_count"] = count
                this_res["job_name"] = info.job_name
                this_res["per_micrograph"] = info.per_micrograph
                res.append(this_res)
        return res
