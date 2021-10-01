from pipeliner.api.api_utils import write_default_jobstar
from pipeliner.api.manage_project import PipelinerProject

from relion.node import Node


class MotionCorrJobNode(Node):
    def __init__(self, name: str, project: PipelinerProject, **kwargs):
        super().__init__(name)
        self._project = project
        self._job_path = ""

    def func(self, *args, **kwargs):
        if self._job_path:
            self._proj.continue_job(self._job_path)
        else:
            _job_star = write_default_jobstar("motioncorr")
            self._job_path = self._proj.run_job(_job_star)
