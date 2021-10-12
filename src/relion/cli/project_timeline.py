import argparse
import pathlib
from datetime import datetime

import pandas as pd
import plotly.express as px

from relion import Project


def run() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("proj_path")
    parser.add_argument("-o", "--out_dir", dest="out_dir", default="./")
    args = parser.parse_args()
    relion_dir = pathlib.Path(args.proj_path)
    proj = Project(relion_dir, cluster=True)

    preproc_job_times = {"start_time": [], "end_time": [], "job": []}
    other_job_times = {"start_time": [], "end_time": [], "job": []}
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
                preproc_job_times["start_time"].extend(
                    job.environment["job_start_times"]
                )
                preproc_job_times["job"].extend(
                    [tag.split("/")[0] for _ in job.environment["job_start_times"]]
                )
        else:
            other_job_times["start_time"].append(job.environment["start_time_stamp"])
            other_job_times["end_time"].append(job.environment["end_time_stamp"])
            other_job_times["job"].append(tag.split("/")[0])
    sorted_times = sorted(preproc_job_times["start_time"])
    end_times = {ts: sorted_times[i + 1] for ts, i in enumerate(sorted_times[:-1])}
    preproc_job_times["start_time"] = preproc_job_times["start_time"][:-1]
    preproc_job_times["job"] = preproc_job_times["job"][:-1]
    preproc_job_times["end_time"] = [
        end_times[t] for t in preproc_job_times["start_time"]
    ]
    class2d_job_times = [p for p in other_job_times if "Class2D" in p[2]]
    class3d_job_times = [p for p in other_job_times if "Class3D" in p[2]]
    # preproc_colours = {
    #    "Import": "#1f77b4",
    #    "MotionCorr": "#ff7f0e",
    #    "CtfFind": "#2ca02c",
    #    "AutoPick": "#9467bd",
    #    "Extract": "#8c564b",
    #    "Select": "#e377c2",
    #    "crYOLO_AutoPick": "#d62728",
    #    "Icebreaker_G": "#bcbd22",
    #    "Icebreaker_F": "#17becf",
    # }
    starts = [p[0] for p in preproc_job_times[:-1]]
    ends = [p[0] for p in preproc_job_times[1:]]
    hover_names = [p[1].split("/")[0] for p in preproc_job_times[:-1]]
    df = pd.DataFrame(preproc_job_times)
    timeline = px.timeline(
        preproc_job_times,
        x_start="start_time",
        x_end="end_time",
        hover_name="job",
        color="job",
    )
    class2d_trace = px.timeline(
        x_start=[t[0] for t in class2d_job_times] + [t[0] for t in class3d_job_times],
        x_end=[t[1] for t in class2d_job_times] + [t[1] for t in class3d_job_times],
        hover_name=["Class2D" for _ in class2d_job_times]
        + ["Class3D" for _ in class2d_job_times],
        color=["#ff7f0e" for _ in class2d_job_times]
        + ["#8c564b" for _ in class2d_job_times],
    )
    timeline.write_html(
        pathlib.Path(args.out_dir) / "relion_project_preprocessing_timeline.html"
    )
    class2d_trace.write_html(
        pathlib.Path(args.out_dir) / "relion_project_classification_timeline.html"
    )
    time_spent = [
        datetime.timestamp(te) - datetime.timestamp(ts) for ts, te in zip(starts, ends)
    ]
    other_time_spent = [
        datetime.timestamp(t[1]) - datetime.timestamp(t[0]) for t in other_job_times
    ]
    cumulative_time_spent = {"time": [], "job": [], "colour": []}

    for ts, h in zip(time_spent, hover_names):
        cumulative_time_spent["time"].append(ts)
        cumulative_time_spent["job"].append(h)
        cumulative_time_spent["colour"].append("#ff9000")
    for t, h in zip(other_time_spent, [p[2] for p in other_job_times]):
        h = h.split("_batch")[0]
        cumulative_time_spent["time"].append(t)
        cumulative_time_spent["job"].append(h.split("/")[0])
        cumulative_time_spent["colour"].append("#2ec4b6")
    df = pd.DataFrame(cumulative_time_spent)
    cumulative_time = px.bar(df, x="job", y="time", color="colour")
    cumulative_time.write_html(
        pathlib.Path(args.out_dir) / "cumulative_preprcoessing_job_time.html"
    )
