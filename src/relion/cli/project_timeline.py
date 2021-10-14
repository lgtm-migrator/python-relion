import argparse
import pathlib

# from datetime import datetime
from typing import Any, Optional, Tuple

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
    require: Tuple[str, Any],
    hover_data: Optional[dict] = None,
    do_sum: bool = False,
    **kwargs,
) -> go.Bar:
    restricted_data = data[getattr(data, require[0]).isin([require[1]])]
    custom_data = []
    hover_template = ""
    if hover_data:
        for i, (k, v) in enumerate(hover_data.items()):
            if isinstance(getattr(restricted_data, v).iloc(0), pd.Timestamp):
                custom_data.append(
                    [t.to_pydatetime() for t in getattr(restricted_data, v)]
                )
                hover_template += f"{k}: %{{customdata[{i}]|%Y/%m/%d %H:%M:%S}} <br>"
            else:
                custom_data.append(getattr(restricted_data, v))
                hover_template += f"{k}: %{{customdata[{i}]}} <br>"
    if do_sum:
        xdata = getattr(restricted_data, x).unique()
        ydata = [
            sum(p[1][y] for p in restricted_data.iterrows() if p[1][x] == j)
            for j in xdata
        ]
    else:
        xdata = getattr(restricted_data, x)
        ydata = getattr(restricted_data, y)
    return go.Bar(
        name=name,
        x=xdata,
        y=ydata,
        customdata=np.transpose(custom_data),
        hovertemplate=hover_template,
        **kwargs,
    )


def run() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("proj_path")
    parser.add_argument("-o", "--out_dir", dest="out_dir", default="./")
    parser.add_argument("-t", "--tag", dest="tag", default="")
    args = parser.parse_args()
    relion_dir = pathlib.Path(args.proj_path)
    proj = Project(relion_dir, cluster=True)

    job_info = {
        "start_time": [],
        "end_time": [],
        "job": [],
        "schedule": [],
        "cluster_id": [],
        "cluster_type": [],
        "cluster_start_time": [],
        "num_mics": [],
        "useful": [],
    }

    preproc_end_times = []
    for job in proj._job_nodes.nodes:
        if "External" in job.name:
            tag = job.environment["alias"].split("/")[1]
        else:
            tag = job.name
        if job in proj.preprocess:
            preproc_end_times.append(job.environment["end_time_stamp"])
            if job.environment["job_start_times"] and (
                job.environment["alias"] is None
                or "Icebreaker_group_batch" not in job.environment["alias"]
            ):
                job_info["start_time"].extend(job.environment["job_start_times"])
                job_info["end_time"].extend(
                    [None for _ in job.environment["job_start_times"]]
                )
                job_info["job"].extend(
                    [tag.split("/")[0] for _ in job.environment["job_start_times"]]
                )
                job_info["schedule"].extend(
                    ["preprocess" for _ in job.environment["job_start_times"]]
                )
                if job.environment["cluster_job_ids"]:
                    job_info["cluster_id"].extend(job.environment["cluster_job_ids"])
                    job_info["cluster_start_time"].extend(
                        job.environment["cluster_job_start_times"]
                    )
                else:
                    job_info["cluster_id"].extend(
                        ["N/A" for _ in job.environment["job_start_times"]]
                    )
                    job_info["cluster_start_time"].extend(
                        job.environment["job_start_times"]
                    )
                if job.environment["cluster_job_mic_counts"]:
                    job_info["num_mics"].extend(
                        job.environment["cluster_job_mic_counts"]
                    )
                    if tag.split("/")[0] == "Extract" or "Icebreaker" in tag:
                        job_info["useful"].extend(
                            [None for _ in job.environment["job_start_times"]]
                        )
                    else:
                        job_info["useful"].extend(
                            [bool(p) for p in job.environment["cluster_job_mic_counts"]]
                        )
                else:
                    job_info["num_mics"].extend(
                        ["N/A" for _ in job.environment["job_start_times"]]
                    )
                    job_info["useful"].extend(
                        [None for _ in job.environment["job_start_times"]]
                    )
                if "Icebreaker" in tag:
                    job_info["cluster_type"].extend(
                        ["cpu" for _ in job.environment["job_start_times"]]
                    )
                elif tag.split("/")[0] in ("Import", "Select"):
                    job_info["cluster_type"].extend(
                        [None for _ in job.environment["job_start_times"]]
                    )
                else:
                    job_info["cluster_type"].extend(
                        ["gpu" for _ in job.environment["job_start_times"]]
                    )
        else:
            tag = tag.split("_batch")[0]
            job_info["start_time"].append(job.environment["start_time_stamp"])
            job_info["end_time"].append(job.environment["end_time_stamp"])
            job_info["job"].append(tag.split("/")[0])
            job_info["schedule"].append(tag.split("/")[0])
            if job.environment["cluster_job_ids"]:
                job_info["cluster_id"].append(job.environment["cluster_job_ids"][0])
                job_info["cluster_start_time"].append(
                    job.environment["cluster_job_start_times"][0]
                )
            else:
                job_info["cluster_id"].append("N/A")
                job_info["cluster_start_time"].append(
                    job.environment["start_time_stamp"]
                )
            job_info["num_mics"].append("N/A")
            job_info["useful"].append(None)
            if "Icebreaker" in tag:
                job_info["cluster_type"].append("cpu")
            else:
                job_info["cluster_type"].append("gpu")

    df = pd.DataFrame(job_info)
    df.sort_values("start_time", ignore_index=True, inplace=True)

    preproc_locs = list(df.loc[df["schedule"] == "preprocess"].index.values)
    for i, l in enumerate(preproc_locs[:-1]):
        df.loc[l, "end_time"] = df.loc[preproc_locs[i + 1], "start_time"]

    df.loc[preproc_locs[-1], "end_time"] = max(preproc_end_times)

    df["total_time"] = df["end_time"] - df["start_time"]
    df["run_time"] = df["end_time"] - df["cluster_start_time"]
    df["queue_time"] = df["total_time"] - df["run_time"]

    # timeline = px.timeline(
    #    df,
    #    x_start="start_time",
    #    x_end="end_time",
    #    hover_name="job",
    #    hover_data=["start_time", "end_time", "cluster_id", "num_mics", "total_time"],
    #    color="job",
    # )
    # full_timeline = px.timeline(
    #    df_all,
    #    x_start="start_time",
    #    x_end="end_time",
    #    y="schedule",
    #    hover_name="job",
    #    hover_data=["start_time", "end_time", "cluster_id", "num_mics", "total_time"],
    #    color="job",
    # )

    # timeline.write_html(
    #    pathlib.Path(args.out_dir) / "relion_project_preprocessing_timeline.html"
    # )
    # full_timeline.write_html(
    #    pathlib.Path(args.out_dir) / "relion_project_timeline.html"
    # )

    figs = []

    figs.append(
        make_subplots(
            rows=1,
            cols=2,
            shared_yaxes=True,
            subplot_titles=("Total job time", "Run time"),
        )
    )

    hover_data = {
        "cluster ID": "cluster_id",
        "start time": "start_time",
        "end time": "end_time",
    }
    for i, ydata in enumerate(("total_time", "run_time")):
        [
            figs[0].add_trace(
                _bar(
                    n,
                    df,
                    "job",
                    ydata,
                    ("schedule", r),
                    hover_data=hover_data,
                    width=0.8,
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

    figs[0].update_yaxes(title_text="Time [s]", row=1, col=1)

    job_count = px.bar(
        df,
        x="job",
        color="schedule",
        hover_data=["start_time", "end_time", "cluster_id", "num_mics", "total_time"],
        labels={"job": "Job count"},
    )

    job_count.write_html(pathlib.Path(args.out_dir) / "job_counts.html")

    figs.append(go.Figure())

    for ydata in ("run_time", "queue_time"):
        [
            figs[1].add_trace(
                _bar(
                    n,
                    df,
                    "job",
                    ydata,
                    ("cluster_type", r),
                    width=0.5,
                ),
            )
            for n, r in [
                (f"{'Run time' if ydata == 'run_time' else 'Queue time'} (GPU)", "gpu"),
                (f"{'Run time' if ydata == 'run_time' else 'Queue time'} (GPU)", "cpu"),
            ]
        ]

    figs[1].update_layout(barmode="group")

    figs.append(go.Figure())

    for ydata in ("total_time", "run_time"):
        [
            figs[2].add_trace(
                _bar(
                    n,
                    df,
                    "job",
                    ydata,
                    ("useful", r),
                    width=0.2,
                ),
            )
            for n, r in [
                (f"{'Run time' if ydata == 'run_time' else 'Total time'} useful", True),
                (
                    f"{'Run time' if ydata == 'run_time' else 'Total time'} useless",
                    False,
                ),
            ]
        ]

    figs[2].update_layout(barmode="group")

    with open(pathlib.Path(args.out_dir) / "cluster_stats.html", "w") as f:
        [f.write(fig.to_html(full_html=False, include_plotlyjs="cdn")) for fig in figs]
