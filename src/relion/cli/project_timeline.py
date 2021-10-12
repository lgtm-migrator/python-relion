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
            tag = tag.split("_batch")[0]
            other_job_times["start_time"].append(job.environment["start_time_stamp"])
            other_job_times["end_time"].append(job.environment["end_time_stamp"])
            other_job_times["job"].append(tag.split("/")[0])
    sorted_times = sorted(preproc_job_times["start_time"])
    drop_index = preproc_job_times["start_time"].index(sorted_times[-1])
    end_times = {ts: sorted_times[i + 1] for i, ts in enumerate(sorted_times[:-1])}
    preproc_job_times["start_time"].pop(drop_index)
    preproc_job_times["job"].pop(drop_index)
    preproc_job_times["end_time"] = [
        end_times[t] for t in preproc_job_times["start_time"]
    ]
    preproc_job_times["total_time"] = [
        datetime.timestamp(te) - datetime.timestamp(ts)
        for ts, te in zip(
            preproc_job_times["start_time"], preproc_job_times["end_time"]
        )
    ]

    other_job_times["total_time"] = [
        datetime.timestamp(te) - datetime.timestamp(ts)
        for ts, te in zip(other_job_times["start_time"], other_job_times["end_time"])
    ]

    df = pd.DataFrame(preproc_job_times)
    df_other = pd.DataFrame(other_job_times)
    timeline = px.timeline(
        df,
        x_start="start_time",
        x_end="end_time",
        hover_name="job",
        color="job",
    )
    class2d_trace = px.timeline(
        df_other,
        x_start="start_time",
        x_end="end_time",
        hover_name="job",
        color="job",
    )
    timeline.write_html(
        pathlib.Path(args.out_dir) / "relion_project_preprocessing_timeline.html"
    )
    class2d_trace.write_html(
        pathlib.Path(args.out_dir) / "relion_project_classification_timeline.html"
    )

    df_all = pd.concat(df, df_other)

    cumulative_time = px.bar(df_all, x="job", y="total_time")
    cumulative_time.write_html(
        pathlib.Path(args.out_dir) / "cumulative_preprcoessing_job_time.html"
    )
