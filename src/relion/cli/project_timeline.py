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
        if job.environment["job_start_times"] and (
            job.environment["alias"] is None
            or "Icebreaker_group_batch" not in job.environment["alias"]
        ):
            job_times.extend(
                [(t, job.name) for t in job.environment["job_start_times"]]
            )
    job_times = sorted(job_times, key=lambda x: x[0])
    preproc = [
        "Import",
        "MotionCorr",
        "CtfFind",
        "External",
        "AutoPick",
        "Extract",
        "Select",
    ]
    preproc_job_times = [p for p in job_times if p[1].split("/")[0] in preproc]
    starts = [p[0] for p in preproc_job_times[:-1]]
    ends = [p[0] for p in preproc_job_times[1:]]
    hover_names = [p[1].split("/")[0] for p in preproc_job_times[:-1]]
    timeline = px.timeline(x_start=starts, x_end=ends, hover_name=hover_names)
    timeline.write_html("./relion_project_preprocessing_timeline.html")
