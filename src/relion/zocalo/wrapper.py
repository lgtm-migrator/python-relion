import datetime
import enum
import logging
import os
import pathlib
import relion
import threading
import time
import zocalo.util.symlink
import zocalo.wrapper
from pprint import pprint
from relion.cryolo_relion_it import cryolo_relion_it, dls_options

logger = logging.getLogger("relion.zocalo.wrapper")

RelionStatus = enum.Enum("RelionStatus", "RUNNING SUCCESS FAILURE")

# to test:
# - create an empty directory
# - run following command to generate a recipe-wrapper:
#   dlstbx.find_in_ispyb -p 6844019 -f /dls_sw/apps/zocalo/live/recipes/ispyb-relion.json --recipe-pointer=2 --out=rw && replace "/dls/m12/data/2021/cm28212-1/processed" "$(pwd)/processed" "/dls/m12/data/2021/cm28212-1/tmp" "$(pwd)/tmp" '"$ispyb_autoprocprogram_id"' "83862530" < rw > rw-local
# - run the wrapper:
#   dlstbx.wrap --recipewrapper=rw-local --wrap=relion --offline -v


class RelionWrapper(zocalo.wrapper.BaseWrapper):
    def run(self):
        # Enable log messages for relion.*
        logging.getLogger("relion").setLevel(logging.INFO)

        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        self.params = self.recwrap.recipe_step["job_parameters"]
        self.working_directory = pathlib.Path(self.params["working_directory"])
        self.results_directory = pathlib.Path(self.params["results_directory"])

        # Here we cheat. Ultimately we want to run the processing inside the
        # 'working' directory, and then copy relevant output into the 'results'
        # directory as we go along. In the first instance we will just run Relion
        # inside the 'results' directory, and ignore the 'working' directory
        # completely.
        self.working_directory = self.results_directory

        # Create working and results directory
        self.working_directory.mkdir(parents=True, exist_ok=True)
        self.results_directory.mkdir(parents=True, exist_ok=True)
        if self.params.get("create_symlink"):
            # Create symbolic link above directories
            # We want users to go to the most recent execution, as they can stop
            # and restart processing from SynchWeb. Thus we overwrite symlinks.
            zocalo.util.symlink.create_parent_symlink(
                self.working_directory,
                self.params["create_symlink"],
                overwrite_symlink=True,
            )
            zocalo.util.symlink.create_parent_symlink(
                self.results_directory,
                self.params["create_symlink"],
                overwrite_symlink=True,
            )

        # Relion needs to have a 'Movies' link inside the working directory
        # pointing to the image files
        movielink = "Movies"
        os.symlink(self.params["image_directory"], self.working_directory / movielink)
        self.params["ispyb_parameters"]["import_images"] = os.path.join(
            movielink,
            pathlib.Path(self.params["ispyb_parameters"]["import_images"]).relative_to(
                self.params["image_directory"]
            ),
        )

        # Debug output
        pprint(self.params)

        # Select specific Cryolo version if so desired
        if self.params.get("cryolo_version"):
            os.environ["CRYOLO_VERSION"] = self.params["cryolo_version"]
            logger.info("Selected cryolo version %s", self.params["cryolo_version"])

        # Initialise number of imported files to 0
        imported_files = []
        mc_job_time_all_processed = None

        # Start Relion
        self._relion_subthread = threading.Thread(
            target=self.start_relion, name="relion_subprocess_runner", daemon=True
        )
        self._relion_subthread.start()

        relion_started = time.time()

        preprocess_check = self.results_directory / "RUNNING_PIPELINER_PREPROCESS"
        all_process_check = self.results_directory / "RUNNING_RELION_IT"

        relion_prj = relion.Project(self.working_directory)

        while not relion_prj.origin_present() or not preprocess_check.is_file():
            time.sleep(0.5)
            if time.time() - relion_started > 10 * 60:
                break

        relion_prj.load()

        preproc_recently_run = False
        processing_ended = False

        while (
            self._relion_subthread.is_alive() or preprocess_check.is_file()
        ) and False not in [n.environment["status"] for n in relion_prj]:
            time.sleep(1)

            # logger.info("Looking for results")

            ispyb_command_list = []

            if pathlib.Path(self.params["stop_file"]).is_file():
                relion_prj.load()
                for job_path in relion_prj._job_nodes:
                    (
                        self.results_directory
                        / job_path.name
                        / "RELION_JOB_EXIT_ABORTED"
                    ).touch()
                for p in self.results_directory.glob("RUNNING_*"):
                    p.unlink()

            relion_prj.load()

            for job_msg in relion_prj.messages:
                logger.info(f"Found results that look like this: {job_msg[0]}")
                ispyb_command_list.extend(job_msg)

            if ispyb_command_list:
                logger.info(
                    "Sending commands like this: %s", str(ispyb_command_list[0])
                )
                self.recwrap.send_to(
                    "ispyb", {"ispyb_command_list": ispyb_command_list}
                )
                logger.info("Sent %d commands to ISPyB", len(ispyb_command_list))

            # if Relion has been running too long stop loop of preprocessing jobs
            try:
                most_recent_movie = max(
                    p.stat().st_mtime
                    for p in pathlib.Path(self.params["image_directory"]).glob("**/*")
                )
            # if a file vanishes for some reason make sure that there is no crash and no exit
            except FileNotFoundError:
                most_recent_movie = time.time()

            # check if all imported files have been motion corrected
            # if they have then get the time stamp of the motion correction job
            # so that it can be checked all preprocessing jobs have run after it
            # only do this if the number of imported files has changed
            new_imported_files = relion_prj.get_imported()
            if new_imported_files != imported_files or not mc_job_time_all_processed:
                imported_files = new_imported_files
                mc_job_time_all_processed = self.check_processing_of_imports(
                    relion_prj, imported_files
                )

            if len(relion_prj._job_nodes) != 0 and len(relion_prj.preprocess) != 0:
                for job in relion_prj.preprocess:
                    job_end_time = job.environment["end_time_stamp"]
                    if job_end_time is None:
                        break
                    if (
                        datetime.datetime.timestamp(job_end_time) < most_recent_movie
                        or job.environment["job_count"] < 1
                    ):
                        break
                    # don't need to check if Import job has run recently for this bit
                    if mc_job_time_all_processed and "Import" not in job.name:
                        if (
                            datetime.datetime.timestamp(job_end_time)
                            < mc_job_time_all_processed
                        ):
                            preproc_recently_run = False
                            break
                else:
                    preproc_recently_run = True

            currtime = time.time()
            if (
                currtime - most_recent_movie
                > int(self.params["latest_movie_timeout"]) * 60
                and currtime - relion_started > 10 * 60
                and preprocess_check.is_file()
                and preproc_recently_run
                and mc_job_time_all_processed
            ):
                preprocess_check.unlink()

            processing_ended = self.check_whether_ended(relion_prj)
            if (
                currtime - most_recent_movie
                > int(self.params["latest_movie_timeout"]) * 60
                and currtime - relion_started > 10 * 60
                and all_process_check.is_file()
                and processing_ended
                and mc_job_time_all_processed
            ):
                all_process_check.unlink()

        logger.info("Done.")
        success = True

        if preprocess_check.is_file():
            preprocess_check.unlink()

        if all_process_check.is_file():
            all_process_check.unlink()

        return success

    @staticmethod
    def check_processing_of_imports(relion_prj, imported):
        try:
            checked_key = "job002"
            checks = [False for _ in range(len(imported))]
            for i, f in enumerate(imported):
                for key in relion_prj.res._cache.keys():
                    if any(
                        f.split(".")[0] in p.split(".")[0]
                        for p in relion_prj.res._cache[key]
                    ):
                        checks[i] = True
                        checked_key = key
                        break
            if all(checks):
                return datetime.datetime.timestamp(
                    relion_prj._job_nodes.get_by_name(
                        "MotionCorr/" + checked_key
                    ).environment["end_time_stamp"]
                )
            return
        except (KeyError, AttributeError, RuntimeError, FileNotFoundError) as e:
            logger.debug(
                f"Exception encountered while checking whether imported files have been processed: {e}",
                exc_info=True,
            )
            return

    def start_relion(self):
        print("Running RELION wrapper - stdout")
        logger.info("Running RELION wrapper - logger.info")

        for k, v in self.params["ispyb_parameters"].items():
            if v.isnumeric():
                self.params["ispyb_parameters"][k] = int(v)
            elif v.lower() == "true":
                self.params["ispyb_parameters"][k] = True
            elif v.lower() == "false":
                self.params["ispyb_parameters"][k] = False
            else:
                try:
                    self.params["ispyb_parameters"][k] = float(v)
                except ValueError:
                    pass
        pprint(self.params["ispyb_parameters"])

        # Prepare options object
        opts = cryolo_relion_it.RelionItOptions()
        opts.update_from(vars(dls_options))
        opts.update_from(self.params["ispyb_parameters"])

        self.opts = opts

        # Write options to disk for a record of parameters used
        options_file = self.working_directory / cryolo_relion_it.OPTIONS_FILE
        logger.info(f"Writing all options to {options_file}")
        if os.path.isfile(options_file):
            logger.info(
                f"File {options_file} already exists; renaming old copy to {options_file}~"
            )
            os.rename(options_file, f"{options_file}~")
        with open(options_file, "w") as optfile:
            opts.print_options(optfile)

        success = False
        oldpwd = os.getcwd()
        try:
            os.chdir(self.working_directory)
            cryolo_relion_it.run_pipeline(opts)
            success = True
        except Exception as ex:
            logger.error(ex)
        finally:
            os.chdir(oldpwd)

        logger.info("Done.")
        return success

    def check_whether_ended(self, proj):
        if len(proj._job_nodes) == 0 or None in [j.environment["status"] for j in proj]:
            return False
        check_time = time.time()
        return all(
            check_time - datetime.datetime.timestamp(j.environment["end_time_stamp"])
            > 10 * 60
            for j in proj
        )

    def create_synchweb_stop_file(self):
        pathlib.Path(self.params["stop_file"]).touch()
