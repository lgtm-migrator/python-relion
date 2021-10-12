import argparse
import pathlib

import plotly.express as px

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
    starts = [p[0] for p in job_times[:-1]]
    ends = [p[0] for p in job_times[1:]]
    hover_names = [str(p[1]._path.parent) for p in job_times[:-1]]
    timeline = px.timeline(x_start=starts, x_end=ends, hover_name=hover_names)
    timeline.write_html("./relion_project_timeline.html")
