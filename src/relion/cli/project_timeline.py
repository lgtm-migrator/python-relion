import argparse
import pathlib

from relion import Project


def run() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("proj_path")
    args = parser.parse_args()
    relion_dir = pathlib.Path(args.proj_path)
    proj = Project(relion_dir, cluster=True)
    # mc_jobs = proj._job_nodes.nodes[1].environment["cluster_job_ids"]
    job_times = []
    for job in proj._job_nodes.nodes:
        job_times.extend([(t, job.name) for t in job.environment["job_start_times"]])
    job_times = sorted(job_times, key=lambda x: x[0])
    print(job_times)
