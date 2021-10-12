import argparse
import pathlib
from datetime import datetime

import plotly.express as px

from relion import Project


def run() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("proj_path")
    parser.add_argument("-o", "--out_dir", dest="out_dir", default="./")
    args = parser.parse_args()
    relion_dir = pathlib.Path(args.proj_path)
    proj = Project(relion_dir, cluster=True)
    # mc_jobs = proj._job_nodes.nodes[1].environment["cluster_job_ids"]
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
    preproc_job_times = []
    other_job_times = []
    for job in proj._job_nodes.nodes:
        if "External" in job.name:
            tag = job.environment["alias"].split("/")[1]
        else:
            tag = job.name
        if job in proj.preprocess:
            if job.environment["job_start_times"] and (
                job.environment["alias"] is None
                or "Icebreaker_group_batch" not in job.environment["alias"]
            ):
                preproc_job_times.extend(
                    [(t, tag) for t in job.environment["job_start_times"]]
                )
        else:
            other_job_times.append(
                (
                    job.environment["start_time_stamp"],
                    job.environment["end_time_stamp"],
                    tag,
                )
            )
    preproc_job_times = sorted(preproc_job_times, key=lambda x: x[0])
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
    starts = [p[0] for p in preproc_job_times[:-1]]
    ends = [p[0] for p in preproc_job_times[1:]]
    hover_names = [p[1].split("/")[0] for p in preproc_job_times[:-1]]
    colours = [preproc_colours[p] for p in hover_names]
    timeline = px.timeline(
        x_start=starts,
        x_end=ends,
        hover_name=hover_names,
        color=colours,
        labels=hover_names,
    )
    timeline.write_html(
        pathlib.Path(args.out_dir) / "relion_project_preprocessing_timeline.html"
    )
    time_spent = [
        datetime.timestamp(te) - datetime.timestamp(ts) for ts, te in zip(starts, ends)
    ]
    cumulative_time_spent = {key: [0] for key in preproc}
    for ts, h in zip(time_spent, hover_names):
        cumulative_time_spent[h][0] += ts
    cumulative_time = px.bar(cumulative_time_spent, x=preproc)
    cumulative_time.write_html(
        pathlib.Path(args.out_dir) / "cumulative_preprcoessing_job_time.html"
    )
