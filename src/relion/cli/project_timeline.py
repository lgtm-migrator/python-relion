import argparse
import pathlib
from datetime import datetime

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from relion import Project


def run() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("proj_path")
    parser.add_argument("-o", "--out_dir", dest="out_dir", default="./")
    args = parser.parse_args()
    relion_dir = pathlib.Path(args.proj_path)
    proj = Project(relion_dir, cluster=True)

    preproc_job_times = {
        "start_time": [],
        "end_time": [],
        "job": [],
        "schedule": [],
        "cluster_id": [],
        "num_mics": [],
    }
    other_job_times = {
        "start_time": [],
        "end_time": [],
        "job": [],
        "schedule": [],
        "cluster_id": [],
        "num_mics": [],
    }
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
                preproc_job_times["schedule"].extend(
                    ["preprocess" for _ in job.environment["job_start_times"]]
                )
                if job.environment["cluster_job_ids"]:
                    preproc_job_times["cluster_id"].extend(
                        job.environment["cluster_job_ids"]
                    )
                else:
                    preproc_job_times["cluster_id"].extend(
                        ["N/A" for _ in job.environment["job_start_times"]]
                    )
                if job.environment["cluster_job_mic_counts"]:
                    preproc_job_times["num_mics"].extend(
                        job.environment["cluster_job_mic_counts"]
                    )
                else:
                    preproc_job_times["num_mics"].extend(
                        ["N/A" for _ in job.environment["job_start_times"]]
                    )
        else:
            tag = tag.split("_batch")[0]
            other_job_times["start_time"].append(job.environment["start_time_stamp"])
            other_job_times["end_time"].append(job.environment["end_time_stamp"])
            other_job_times["job"].append(tag.split("/")[0])
            other_job_times["schedule"].append(tag.split("/")[0])
            if job.environment["cluster_job_ids"]:
                other_job_times["cluster_id"].append(
                    job.environment["cluster_job_ids"][0]
                )
            else:
                other_job_times["cluster_id"].append("N/A")
            other_job_times["num_mics"].append("N/A")
    sorted_times = sorted(preproc_job_times["start_time"])
    drop_index = preproc_job_times["start_time"].index(sorted_times[-1])
    end_times = {ts: sorted_times[i + 1] for i, ts in enumerate(sorted_times[:-1])}
    preproc_job_times["start_time"].pop(drop_index)
    preproc_job_times["job"].pop(drop_index)
    preproc_job_times["cluster_id"].pop(drop_index)
    preproc_job_times["schedule"].pop(drop_index)
    preproc_job_times["num_mics"].pop(drop_index)
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
    df_all = pd.concat([df, df_other])
    timeline = px.timeline(
        df,
        x_start="start_time",
        x_end="end_time",
        hover_name="job",
        hover_data=["start_time", "end_time", "cluster_id", "num_mics", "total_time"],
        color="job",
    )
    full_timeline = px.timeline(
        df_all,
        x_start="start_time",
        x_end="end_time",
        y="job",
        hover_name="job",
        hover_data=["start_time", "end_time", "cluster_id", "num_mics", "total_time"],
        color="job",
    )

    timeline.write_html(
        pathlib.Path(args.out_dir) / "relion_project_preprocessing_timeline.html"
    )
    full_timeline.write_html(
        pathlib.Path(args.out_dir) / "relion_project_timeline.html"
    )

    df_all.sort_values("start_time")

    fig = make_subplots(shared_xaxes=True)

    cumulative_time = go.Bar(
        df_all,
        x="job",
        y="total_time",
        color="schedule",
        hover_data=["start_time", "end_time", "cluster_id", "num_mics", "total_time"],
    )
    job_count = go.Bar(
        df_all,
        x="job",
        color="schedule",
        hover_data=["start_time", "end_time", "cluster_id", "num_mics", "total_time"],
    )

    fig.add_trace(cumulative_time)
    fig.add_trace(job_count)

    fig.write_html(
        pathlib.Path(args.out_dir) / "cumulative_preprcoessing_job_time.html"
    )
