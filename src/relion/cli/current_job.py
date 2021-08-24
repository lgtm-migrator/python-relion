import argparse
import pathlib
import subprocess

from relion import Project


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("proj_path")
    args = parser.parse_args()
    relion_dir = pathlib.Path(args.proj_path)
    proj = Project(relion_dir, cluster=True)
    current_jobs = proj.current_jobs
    if current_jobs is None:
        print("There are not any currently running jobs")
    else:
        print("These jobs are currently running: \n")
        for current_job in current_jobs:
            alias = current_job.environment["alias"]
            if alias is not None:
                print(f"Current job: {current_job._path} [alias={alias}]")
            else:
                print(f"Current job: {current_job._path}")
            print(f"Job started at: {current_job.environment['start_time_stamp']}")
            print(f"Job has been run {current_job.environment['job_count']} time(s)")
            if current_job.environment["cluster_job_id"]:
                print(
                    f"Job running with cluster id: {current_job.environment['cluster_job_id']}"
                )
                qstat = subprocess.run(
                    ["qstat", "-j", current_job.environment["cluster_job_id"]],
                    capture_output=True,
                )
                qstat_output = qstat.stdout.decode("utf-8")
                job_state = None
                cluster_start_time = None
                for prop in qstat_output.split("\n"):
                    if "job_state" in prop:
                        job_state = prop.split()[-1]
                    if "start_time" in prop:
                        cluster_start_time = " ".join(prop.split()[-2:])
                if job_state == "r":
                    print(f"cluster job running with start time {cluster_start_time}")
                elif job_state == "q":
                    print("cluster job queued")
                elif job_state:
                    print(f"cluster job has status: {job_state}")
                else:
                    print(
                        f"cluster job {current_job.environment['cluster_job_id']} not found"
                    )
            print()


if __name__ == "__main__":
    run()
