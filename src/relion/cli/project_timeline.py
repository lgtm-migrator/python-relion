import argparse
import pathlib
from datetime import datetime
from typing import Optional, Tuple

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from relion import Project


def _bar(
    name: str,
    data: pd.DataFrame,
    x: str,
    y: str,
    require: Tuple[str, str],
    hover_data: Optional[dict] = None,
    width: float = 0.8,
) -> go.Bar:
    restriced_data = data[getattr(data, require[0]).isin([require[1]])]
    custom_data = []
    hover_template = ""
    if hover_data:
        for i, (k, v) in enumerate(hover_data.items()):
            if isinstance(getattr(restriced_data, v)[0], pd.Timestamp):
                custom_data.append(
                    [t.to_pydatetime() for t in getattr(restriced_data, v)]
                )
                hover_template += f"{k}: %{{customdata[{i}]|%Y/%m/%d %H:%M:%S}} <br>"
            else:
                custom_data.append(getattr(restriced_data, v))
                hover_template += f"{k}: %{{customdata[{i}]}} <br>"
    return go.Bar(
        name=name,
        x=getattr(restriced_data, x),
        y=getattr(restriced_data, y),
        width=width,
        customdata=np.transpose(custom_data),
        hovertemplate=hover_template,
    )


def run() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("proj_path")
    parser.add_argument("-o", "--out_dir", dest="out_dir", default="./")
    parser.add_argument("-t", "--tag", dest="tag", default="")
    args = parser.parse_args()
    relion_dir = pathlib.Path(args.proj_path)
    proj = Project(relion_dir, cluster=True)

    preproc_job_times = {
        "start_time": [],
        "end_time": [],
        "job": [],
        "schedule": [],
        "cluster_id": [],
        "cluster_start_time": [],
        "num_mics": [],
    }
    other_job_times = {
        "start_time": [],
        "end_time": [],
        "job": [],
        "schedule": [],
        "cluster_id": [],
        "cluster_start_time": [],
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
                    preproc_job_times["cluster_start_time"].extend(
                        job.environment["cluster_job_start_times"]
                    )
                else:
                    preproc_job_times["cluster_id"].extend(
                        ["N/A" for _ in job.environment["job_start_times"]]
                    )
                    preproc_job_times["cluster_start_time"].extend(
                        job.environment["job_start_times"]
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
                other_job_times["cluster_start_time"].append(
                    job.environment["cluster_job_start_times"][0]
                )
            else:
                other_job_times["cluster_id"].append("N/A")
                other_job_times["cluster_start_time"].append(
                    job.environment["start_time_stamp"]
                )
            other_job_times["num_mics"].append("N/A")
    sorted_times = sorted(preproc_job_times["start_time"])
    drop_index = preproc_job_times["start_time"].index(sorted_times[-1])
    end_times = {ts: sorted_times[i + 1] for i, ts in enumerate(sorted_times[:-1])}
    preproc_job_times["start_time"].pop(drop_index)
    preproc_job_times["job"].pop(drop_index)
    preproc_job_times["cluster_id"].pop(drop_index)
    preproc_job_times["cluster_start_time"].pop(drop_index)
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
    preproc_job_times["run_time"] = [
        datetime.timestamp(te) - datetime.timestamp(ts)
        if isinstance(ts, datetime)
        else None
        for ts, te in zip(
            preproc_job_times["cluster_start_time"], preproc_job_times["end_time"]
        )
    ]
    preproc_job_times["queue_time"] = [
        tt - rt
        for tt, rt in zip(
            preproc_job_times["total_time"], preproc_job_times["run_time"]
        )
    ]

    other_job_times["total_time"] = [
        datetime.timestamp(te) - datetime.timestamp(ts)
        for ts, te in zip(other_job_times["start_time"], other_job_times["end_time"])
    ]
    other_job_times["run_time"] = [
        datetime.timestamp(te) - datetime.timestamp(ts)
        if isinstance(ts, datetime)
        else None
        for ts, te in zip(
            other_job_times["cluster_start_time"], other_job_times["end_time"]
        )
    ]
    other_job_times["queue_time"] = [
        tt - rt
        for tt, rt in zip(other_job_times["total_time"], other_job_times["run_time"])
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
        y="schedule",
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

    fig_times = make_subplots(
        rows=1, cols=2, shared_yaxes=True, subplot_titles=("Total job time", "Run time")
    )

    hover_data = {
        "cluster ID": "cluster_id",
        "start time": "start_time",
        "end time": "end_time",
    }
    for i, ydata in enumerate(("total_time", "run_time")):
        [
            fig_times.add_trace(
                _bar(
                    n,
                    df_all,
                    "job",
                    ydata,
                    ("schedule", r),
                    hover_data=hover_data,
                ),
                row=1,
                col=i + 1,
            )
            for n, r in [
                ("Preprocessing", "preprocess"),
                ("Icebreaker group", "Icebreaker_group"),
                ("Class2D", "Class2D"),
                ("Initial model", "InitialModel"),
                ("Class3D", "Class3D"),
            ]
        ]

    fig_times.update_yaxes(title_text="Time [s]", row=1, col=1)

    job_count = px.bar(
        df_all,
        x="job",
        color="schedule",
        hover_data=["start_time", "end_time", "cluster_id", "num_mics", "total_time"],
        labels={"job": "Job count"},
    )

    job_count.write_html(pathlib.Path(args.out_dir) / "job_counts.html")

    jobs_gpu = [
        j
        for j in df_all.job.unique()
        if j
        not in ("Icebreaker_G", "Icebreaker_F", "Icebreaker_group", "Import", "Select")
    ]
    jobs_cpu = list(
        {
            j for j in df_all.job.unique() if j not in ("Import", "Select")
        }.symmetric_difference(set(jobs_gpu))
    )
    summed_run_times_gpu = [
        sum(p[1]["run_time"] for p in df_all.iterrows() if p[1]["job"] == j)
        for j in jobs_gpu
    ]
    summed_queue_times_gpu = [
        sum(p[1]["queue_time"] for p in df_all.iterrows() if p[1]["job"] == j)
        for j in jobs_gpu
    ]
    summed_run_times_cpu = [
        sum(p[1]["run_time"] for p in df_all.iterrows() if p[1]["job"] == j)
        for j in jobs_cpu
    ]
    summed_queue_times_cpu = [
        sum(p[1]["queue_time"] for p in df_all.iterrows() if p[1]["job"] == j)
        for j in jobs_cpu
    ]
    fig = go.Figure(
        data=[
            go.Bar(
                name="Run time (GPU)",
                x=jobs_gpu,
                y=summed_run_times_gpu,
                marker={"color": "#6883ba"},
            ),
            go.Bar(
                name="Queue time (GPU)",
                x=jobs_gpu,
                y=summed_queue_times_gpu,
                marker={"color": "#ff5666"},
            ),
            go.Bar(
                name="Run time (CPU)",
                x=jobs_cpu,
                y=summed_run_times_cpu,
                marker={"color": "#6883ba"},
                marker_pattern_shape="x",
            ),
            go.Bar(
                name="Queue time (CPU)",
                x=jobs_cpu,
                y=summed_queue_times_cpu,
                marker={"color": "#ff5666"},
                marker_pattern_shape="x",
            ),
        ],
    )
    fig.update_layout(barmode="group")
    # fig.write_html(pathlib.Path(args.out_dir) / "queue_times.html")

    jobs_gpu = [
        j
        for j in df.job.unique()
        if j
        not in (
            "Icebreaker_G",
            "Icebreaker_F",
            "Icebreaker_group",
            "Import",
            "Select",
            "Extract",
        )
    ]
    summed_run_times_useful_gpu = [
        sum(
            p[1]["run_time"]
            for p in df_all.iterrows()
            if p[1]["job"] == j and p[1]["num_mics"]
        )
        for j in jobs_gpu
    ]
    summed_run_times_useless_gpu = [
        sum(
            p[1]["run_time"]
            for p in df_all.iterrows()
            if p[1]["job"] == j and p[1]["num_mics"] == 0
        )
        for j in jobs_gpu
    ]
    fig_run_use = go.Figure(
        data=[
            go.Bar(
                name="Useful",
                x=jobs_gpu,
                y=summed_run_times_useful_gpu,
                marker={"color": "#6883ba"},
            ),
            go.Bar(
                name="Useless",
                x=jobs_gpu,
                y=summed_run_times_useless_gpu,
                marker={"color": "#ff5666"},
            ),
        ],
    )
    fig_run_use.update_layout(barmode="group")
    # fig.write_html(pathlib.Path(args.out_dir) / "run_times_useful_vs_useless.html")

    jobs_gpu = [
        j
        for j in df.job.unique()
        if j
        not in (
            "Icebreaker_G",
            "Icebreaker_F",
            "Icebreaker_group",
            "Import",
            "Select",
            "Extract",
        )
    ]
    summed_times_useful_gpu = [
        sum(
            p[1]["total_time"]
            for p in df_all.iterrows()
            if p[1]["job"] == j and p[1]["num_mics"]
        )
        for j in jobs_gpu
    ]
    summed_times_useless_gpu = [
        sum(
            p[1]["total_time"]
            for p in df_all.iterrows()
            if p[1]["job"] == j and p[1]["num_mics"] == 0
        )
        for j in jobs_gpu
    ]
    fig_total_use = go.Figure(
        data=[
            go.Bar(
                name="Useful",
                x=jobs_gpu,
                y=summed_times_useful_gpu,
                marker={"color": "#6883ba"},
            ),
            go.Bar(
                name="Useless",
                x=jobs_gpu,
                y=summed_times_useless_gpu,
                marker={"color": "#ff5666"},
            ),
        ],
    )
    fig_total_use.update_layout(barmode="group")
    # fig.write_html(pathlib.Path(args.out_dir) / "total_times_useful_vs_useless.html")
    with open(pathlib.Path(args.out_dir) / "cluster_stats.html", "w") as f:
        f.write(fig_times.to_html(full_html=False, include_plotlyjs="cdn"))
        f.write(fig.to_html(full_html=False, include_plotlyjs="cdn"))
        f.write(fig_run_use.to_html(full_html=False, include_plotlyjs="cdn"))
        f.write(fig_total_use.to_html(full_html=False, include_plotlyjs="cdn"))
