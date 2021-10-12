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
            if "External" in job.name:
                tag = job.environment["alias"].split("/")[0]
            else:
                tag = job.name
            job_times.extend([(t, tag) for t in job.environment["job_start_times"]])
    job_times = sorted(job_times, key=lambda x: x[0])
    preproc = (
        "Import",
        "MotionCorr",
        "CtfFind",
        "crYOLO_AutoPick",
        "Icebreaker_G",
        "Icebreaker_F",
        "AutoPick",
        "Extract",
        "Select",
    )
    preproc_colours = {
        "Import": "#1f77b4",
        "MotionCorr": "#ff7f0e",
        "CtfFind": "#2ca02c",
        "AutoPick": "#9467bd",
        "Extract": "#8c564b",
        "Select": "#e377c2",
        "crYOLO_AutoPick": "#d62728",
        "Icebreaker_G": "#bcbd22",
        "Icebreaker_F": "#17becf",
    }
    preproc_job_times = [p for p in job_times if p[1].split("/")[0] in preproc]
    starts = [p[0] for p in preproc_job_times[:-1]]
    ends = [p[0] for p in preproc_job_times[1:]]
    hover_names = [p[1].split("/")[0] for p in preproc_job_times[:-1]]
    colours = [preproc_colours[p] for p in hover_names]
    timeline = px.timeline(
        x_start=starts, x_end=ends, hover_name=hover_names, color=colours
    )
    timeline.write_html("./relion_project_preprocessing_timeline.html")
