"""
Relion Python API
https://github.com/DiamondLightSource/python-relion
"""

import functools
import pathlib
from gemmi import cif
from relion._parser.ctffind import CTFFind
from relion._parser.motioncorrection import MotionCorr
from relion._parser.autopick import AutoPick
from relion._parser.cryolo import Cryolo
from relion._parser.class2D import Class2D
from relion._parser.initialmodel import InitialModel
from relion._parser.class3D import Class3D
from relion._parser.relion_pipeline import RelionPipeline
import time
import os

try:
    from relion.cryolo_relion_it.cryolo_relion_it import RelionItOptions
except ModuleNotFoundError:
    pass
from relion.protonode.protograph import ProtoGraph
from relion.dbmodel import DBModel
from relion.dbmodel.modeltables import construct_message

import logging

logger = logging.getLogger("relion.Project")

__all__ = []
__author__ = "Diamond Light Source - Scientific Software"
__email__ = "scientificsoftware@diamond.ac.uk"
__version__ = "0.5.4"
__version_tuple__ = tuple(int(x) for x in __version__.split("."))

pipeline_lock = ".relion_lock"


class Project(RelionPipeline):
    """
    Reads information from a Relion project directory and makes it available in
    a structured, object-oriented, and pythonic fashion.
    """

    def __init__(self, path, database="ISPyB", run_options=None):
        """
        Create an object representing a Relion project.
        :param path: A string or file system path object pointing to the root
                     directory of an existing Relion project.
        """
        self.basepath = pathlib.Path(path)
        super().__init__(
            "Import/job001", locklist=[self.basepath / "default_pipeline.star"]
        )
        if not self.basepath.is_dir():
            raise ValueError(f"path {self.basepath} is not a directory")
        self._data_pipeline = ProtoGraph("DataPipeline", [])
        self._db_model = DBModel(database)
        self._drift_cache = {}
        if run_options is None:
            self.run_options = RelionItOptions()
        else:
            self.run_options = run_options
        try:
            self.load()
        except (FileNotFoundError, RuntimeError):
            pass
            # raise RuntimeWarning(
            #    f"Relion Project was unable to load the relion pipeline from {self.basepath}/default_pipeline.star"
            # )

    @property
    def _plock(self):
        return PipelineLock(self.basepath / pipeline_lock)

    def __eq__(self, other):
        if isinstance(other, Project):
            return self.basepath == other.basepath
        return False

    def __hash__(self):
        return hash(("relion.Project", self.basepath))

    def __repr__(self):
        return f"relion.Project({repr(str(self.basepath))})"

    def __str__(self):
        return f"<relion.Project at {self.basepath}>"

    @property
    def _results_dict(self):
        resd = {
            "CtfFind": self.ctffind,
            "MotionCorr": self.motioncorrection,
            "AutoPick": self.autopick,
            "External:crYOLO": self.cryolo,
            "Class2D": self.class2D,
            "InitialModel": self.initialmodel,
            "Class3D": self.class3D,
        }
        return resd

    @property
    @functools.lru_cache(maxsize=1)
    def ctffind(self):
        """access the CTFFind stage of the project.
        Returns a dictionary-like object with job names as keys,
        and lists of CTFMicrograph namedtuples as values."""
        return CTFFind(self.basepath / "CtfFind")

    @property
    @functools.lru_cache(maxsize=1)
    def motioncorrection(self):
        """access the motion correction stage of the project.
        Returns a dictionary-like object with job names as keys,
        and lists of MCMicrograph namedtuples as values."""
        return MotionCorr(self.basepath / "MotionCorr", self._drift_cache)

    @property
    @functools.lru_cache(maxsize=1)
    def autopick(self):
        return AutoPick(self.basepath / "AutoPick")

    @property
    @functools.lru_cache(maxsize=1)
    def cryolo(self):
        return Cryolo(self.basepath / "External")

    @property
    @functools.lru_cache(maxsize=1)
    def class2D(self):
        """access the 2D classification stage of the project.
        Returns a dictionary-like object with job names as keys,
        and lists of Class2DParticleClass namedtuples as values."""
        return Class2D(self.basepath / "Class2D")

    @property
    @functools.lru_cache(maxsize=1)
    def initialmodel(self):
        return InitialModel(self.basepath / "InitialModel")

    @property
    @functools.lru_cache(maxsize=1)
    def class3D(self):
        """access the 3D classification stage of the project.
        Returns a dictionary-like object with job names as keys,
        and lists of Class3DParticleClass namedtuples as values."""
        return Class3D(self.basepath / "Class3D")

    def origin_present(self):
        try:
            self.load_nodes_from_star(self.basepath / "default_pipeline.star")
        except (TypeError, FileNotFoundError, RuntimeError):
            return False
        if len(self._nodes) == 0:
            return False
        return (self.basepath / self.origin / "RELION_JOB_EXIT_SUCCESS").is_file()

    def load(self):
        self._jobs_collapsed = False
        self._data_pipeline = ProtoGraph("DataPipeline", [])
        self.load_nodes_from_star(self.basepath / "default_pipeline.star")
        self.check_job_node_statuses(self.basepath)
        self.collect_job_times(
            list(self.schedule_files), self.basepath / "pipeline_PREPROCESS.log"
        )
        for jobnode in self:
            if self._results_dict.get(jobnode.name) and jobnode.name != "InitialModel":
                jobnode.environment["result"] = self._results_dict[jobnode.name]
                jobnode.environment["extra_options"] = self.run_options
                self._db_model[jobnode.name].environment[
                    "extra_options"
                ] = self.run_options
                self._db_model[jobnode.name].environment[
                    "message_constructor"
                ] = construct_message
                jobnode.link_to(
                    self._db_model[jobnode.name],
                    result_as_traffic=True,
                    share=[("end_time", "end_time")],
                )
                self._data_pipeline.add_node(jobnode)
                self._data_pipeline.add_node(self._db_model[jobnode.name])
                if jobnode.name == "AutoPick":
                    jobnode.propagate(("job_string", "parpick_job_string"))
            elif jobnode.name == "InitialModel":
                jobnode.environment["result"] = self._results_dict[jobnode.name]
                jobnode.link_to(
                    self._db_model[jobnode.name],
                    result_as_traffic=True,
                    share=[("end_time", "end_time")],
                )
                self._data_pipeline.add_node(jobnode)
                jobnode.propagate(("ini_model_job_string", "ini_model_job_string"))
            elif "crYOLO" in jobnode.environment.get("alias"):
                jobnode.environment["result"] = self._results_dict[
                    f"{jobnode._path}:crYOLO"
                ]
                jobnode.environment["extra_options"] = self.run_options
                self._db_model[f"{jobnode._path}:crYOLO"].environment[
                    "extra_options"
                ] = self.run_options
                self._db_model[f"{jobnode._path}:crYOLO"].environment[
                    "message_constructor"
                ] = construct_message
                jobnode.propagate(("job_string", "parpick_job_string"))
                jobnode.link_to(
                    self._db_model[f"{jobnode._path}:crYOLO"],
                    result_as_traffic=True,
                    share=[("end_time", "end_time")],
                )
                self._data_pipeline.add_node(jobnode)
                self._data_pipeline.add_node(self._db_model[f"{jobnode._path}:crYOLO"])
            else:
                self._data_pipeline.add_node(jobnode)
                if jobnode.name == "Import":
                    self._data_pipeline.origins = [jobnode]

    def show_job_nodes(self):
        self.load()
        super().show_job_nodes(self.basepath)

    @property
    def schedule_files(self):
        return self.basepath.glob("pipeline*.log")

    @property
    def messages(self):
        self._clear_caches()
        msgs = []
        results = self._data_pipeline()
        if results is None:
            return msgs
        for node in self._db_model.db_nodes:
            print(node)
            try:
                if results[node.name + "-" + node.nodeid] is not None:
                    msgs.append(results[node.name + "-" + node.nodeid])
            except KeyError:
                logger.debug(
                    f"No results found for {node.name}: probably the job has not completed yet"
                )
        return msgs

    @property
    def current_jobs(self):
        self.load()
        currj = super().current_jobs
        if currj is None:
            return None
        else:
            for n in currj:
                n.change_name(self.basepath / n.name)
            return currj

    @staticmethod
    def _clear_caches():
        Project.motioncorrection.fget.cache_clear()
        Project.ctffind.fget.cache_clear()
        Project.autopick.fget.cache_clear()
        Project.cryolo.fget.cache_clear()
        Project.class2D.fget.cache_clear()
        Project.initialmodel.fget.cache_clear()
        Project.class3D.fget.cache_clear()

    def get_imported(self):
        try:
            import_job_path = self.basepath / self.origin
            gemmi_readable_path = os.fspath(import_job_path / "movies.star")
            star_doc = cif.read_file(gemmi_readable_path)
            for index, block in enumerate(star_doc):
                if list(block.find_loop("_rlnMicrographMovieName")):
                    block_index = index
                    break
            else:
                return []
            data_block = star_doc[block_index]
            values = list(data_block.find_loop("_rlnMicrographMovieName"))
            if not values:
                return []
            return values
        except (FileNotFoundError, RuntimeError):
            return []


# helper class for dealing with the default_pipeline.star lock
class PipelineLock:
    def __init__(self, lockdir):
        self.lockdir = lockdir
        self.fail_count = 0
        self.obtained = False

    def __enter__(self):
        while self.fail_count < 20:
            try:
                self.lockdir.mkdir()
                self.obtained = True
                break
            except FileExistsError:
                time.sleep(0.1)
                self.fail_count += 1
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if self.obtained:
            self.lockdir.rmdir()
        self.obtained = False
