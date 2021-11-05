import argparse
import pathlib

# from datetime import datetime
from typing import Any, Optional, Tuple

import mrcfile
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from relion import Project


def _get_dataframe(proj: Project) -> pd.DataFrame:
    job_info = {
        "start_time": [],
        "end_time": [],
        "job": [],
        "schedule": [],
        "cluster_id": [],
        "cluster_type": [],
        "num_mics": [],
        "useful": [],
        "cluster_start_time": [],
        "image_size": [],
    }
    df = pd.DataFrame(job_info)

    try:
        for job in proj._job_nodes.nodes:
            if "MotionCorr" in job.name:
                micrograph_glob = (proj.basepath / job.name / "Movies").glob("**/*.mrc")
                mic = next(micrograph_glob)
                with mrcfile.open(mic) as mrc:
                    image_size = mrc.data.shape[:2]
    except StopIteration:
        return pd.DataFrame({})

    preproc_end_times = []
    for job in proj._job_nodes.nodes:
        if (
            job.environment["end_time_stamp"]
            and job.environment["start_time_stamp"]
            and job.environment["end_time_stamp"] > job.environment["start_time_stamp"]
        ):
            if "External" in job.name:
                tag = job.environment["alias"].split("/")[1]
            else:
                tag = job.name
            cluster = bool(job.environment["cluster_job_ids"])
            if job in proj.preprocess:
                preproc_end_times.append(job.environment["end_time_stamp"])
                for i, st in enumerate(job.environment["job_start_times"]):
                    cji = job.environment["cluster_job_ids"][i] if cluster else "N/A"
                    mc = (
                        job.environment["cluster_job_mic_counts"][i]
                        if cluster and not ("Extract" in tag or "Icebreaker" in tag)
                        else None
                    )
                    cs = (
                        job.environment["cluster_job_start_times"][i] if cluster else st
                    )
                    useful = bool(mc) if mc is not None else None
                    if "Icebreaker" in tag:
                        cltype = "cpu"
                    elif tag.split("/")[0] in ("Import", "Select"):
                        cltype = None
                    else:
                        cltype = "gpu"
                    row = {
                        "start_time": st,
                        "end_time": None,
                        "job": tag.split("/")[0],
                        "schedule": "preprocess",
                        "cluster_id": cji,
                        "cluster_type": cltype,
                        "num_mics": mc,
                        "useful": useful,
                        "cluster_start_time": cs,
                        "image_size": image_size,
                    }
                    df = df.append(row, ignore_index=True)
            else:
                tag = tag.split("_batch")[0]
                row = {
                    "start_time": job.environment["start_time_stamp"],
                    "end_time": job.environment["end_time_stamp"],
                    "job": tag.split("/")[0],
                    "schedule": tag.split("/")[0],
                    "cluster_id": job.environment["cluster_job_ids"][0]
                    if cluster
                    else "N/A",
                    "cluster_type": "cpu" if "Icebreaker" in tag else "gpu",
                    "num_mics": "N/A",
                    "useful": None,
                    "cluster_start_time": job.environment["cluster_job_start_times"][-1]
                    if cluster
                    else job.environment["start_time_stamp"],
                    "image_size": image_size,
                }
                df = df.append(row, ignore_index=True)

    df.sort_values("start_time", ignore_index=True, inplace=True)

    preproc_locs = list(df.loc[df["schedule"] == "preprocess"].index.values)
    for i, l in enumerate(preproc_locs[:-1]):
        df.loc[l, "end_time"] = df.loc[preproc_locs[i + 1], "start_time"]

    if not preproc_end_times:
        return pd.DataFrame({})

    df.loc[preproc_locs[-1], "end_time"] = max(preproc_end_times)
    df["end_time"] = pd.to_datetime(df["end_time"])

    df["total_time"] = df["end_time"] - df["start_time"]
    df["run_time"] = df["end_time"] - df["cluster_start_time"]
    df["queue_time"] = df["total_time"] - df["run_time"]

    return df


def _bar(
    name: str,
    data: pd.DataFrame,
    x: str,
    y: str,
    require: Tuple[str, Any],
    hover_data: Optional[dict] = None,
    do_sum: bool = False,
    base: Optional[str] = None,
    **kwargs,
) -> go.Bar:
    restricted_data = data[getattr(data, require[0]).isin([require[1]])]
    custom_data = []
    hover_template = ""
    if hover_data:
        for i, (k, v) in enumerate(hover_data.items()):
            if isinstance(getattr(restricted_data, v).iloc[0], pd.Timestamp):
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
        if isinstance(getattr(restricted_data, x).iloc[0], pd.Timestamp):
            xdata = [t.to_pydatetime() for t in getattr(restricted_data, x)]
        else:
            xdata = getattr(restricted_data, x)
        if isinstance(getattr(restricted_data, y).iloc[0], pd.Timestamp):
            ydata = [t.to_pydatetime() for t in getattr(restricted_data, y)]
        else:
            ydata = getattr(restricted_data, y)
    return go.Bar(
        name=name,
        x=xdata,
        y=ydata,
        customdata=np.transpose(custom_data),
        hovertemplate=hover_template,
        base=getattr(restricted_data, base) if base else None,
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

    df = _get_dataframe(proj)

    if df.empty:
        return

    figs = []

    hover_data = {
        "cluster ID": "cluster_id",
        "start time": "start_time",
        "end time": "end_time",
    }

    figs.append(
        make_subplots(
            rows=1,
            cols=2,
            shared_yaxes=True,
            subplot_titles=("Total job time", "Run time"),
        )
    )

    for i, ydata in enumerate(("total_time", "run_time")):
        [
            figs[-1].add_trace(
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

    figs[-1].update_yaxes(title_text="Time [s]", row=1, col=1)

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
            figs[-1].add_trace(
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

    figs[-1].update_layout(barmode="group")
    figs[-1].update_yaxes(title_text="Time [s]")

    figs.append(go.Figure())

    for ydata in ("total_time", "run_time"):
        [
            figs[-1].add_trace(
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

    figs[-1].update_layout(barmode="group")
    figs[-1].update_yaxes(title_text="Time [s]")

    with open(pathlib.Path(args.out_dir) / "cluster_stats.html", "w") as f:
        [f.write(fig.to_html(full_html=False, include_plotlyjs="cdn")) for fig in figs]
