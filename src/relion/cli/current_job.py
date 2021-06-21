import argparse
import pathlib

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
            print(f"Job started at: {current_job.attributes['start_time_stamp']}")
            print(f"Job has been run {current_job.attributes['job_count']} time(s)")
            if current_job.attributes["cluster_job_id"]:
                print(
                    f"Job running with cluster id: {current_job.attributes['cluster_job_id']}"
                )
            print()


if __name__ == "__main__":
    run()
