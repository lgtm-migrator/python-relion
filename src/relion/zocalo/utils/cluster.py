import functools
import json
import os
from itertools import chain
from operator import and_
from typing import Any, Dict, List, Optional, Tuple

import matplotlib.pyplot as plt
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import Load
from sqlalchemy.orm.session import sessionmaker

from relion.zocalo.alchemy import ClusterJobInfo, RelionJobInfo, RelionPipelineInfo

_jobs_of_interest = [
    "MotionCorr",
    "Icebreaker_G",
    "Icebreaker_F",
    "CtfFind",
    "AutoPick",
    "Extract",
    "Icebreaker_group",
    "Class2D",
    "InitialModel",
    "Class3D",
    "crYOLO_AutoPick",
    "Icebreaker_5fig",
]


def _get_sessionmaker(
    credentials_file: Optional[str] = None,
) -> sessionmaker:
    credentials = credentials_file or os.getenv("CLUSTER_DB_CREDENTIALS")
    if not credentials:
        raise AttributeError("No credentials file specified")
    with open(credentials, "r") as f:
        creds = json.load(f)
    sqlalchemy_url = "mysql+mysqlconnector://{user}:{passwd}@{host}:{port}/{db}".format(
        **creds
    )
    _sessionmaker = sessionmaker(
        bind=create_engine(sqlalchemy_url, connect_args={"use_pure": True})
    )
    return _sessionmaker


def get_all_usage_data(
    session_maker: Optional[sessionmaker] = None,
) -> pd.DataFrame:
    if session_maker:
        _sessionmaker: sessionmaker = session_maker
    else:
        _sessionmaker: sessionmaker = _get_sessionmaker()
    with _sessionmaker() as session:
        query = (
            session.query(RelionJobInfo, RelionPipelineInfo, ClusterJobInfo)
            .join(ClusterJobInfo, ClusterJobInfo.cluster_id == RelionJobInfo.cluster_id)
            .join(
                RelionPipelineInfo,
                RelionPipelineInfo.pipeline_id == RelionJobInfo.pipeline_id,
            )
        )
        df = pd.read_sql(query.statement, session.bind)
    df["job_time"] = (df["end_time"] - df["relion_start_time"]).dt.total_seconds()
    df["run_time"] = (df["end_time"] - df["start_time"]).dt.total_seconds()
    df["queue_time"] = df["job_time"] - df["run_time"]
    df["pipeline_id"] = df["pipeline_id"].astype(str)
    preproc = (
        "MotionCorr",
        "CtfFind",
        "Icebreaker_F",
        "Icebreaker_G",
        "crYOLO_AutoPick",
        "AutoPick",
        "Extract",
    )

    def schedule(row):
        if row["job_name"] in preproc:
            return "Preprocessing"
        else:
            return row["job_name"]

    df["schedule"] = df.apply(lambda row: schedule(row), axis=1)
    return df


def get_cluster_usage_df(
    columns: List[str],
    values: Optional[dict] = None,
    session_maker: Optional[sessionmaker] = None,
) -> pd.DataFrame:
    if session_maker:
        _sessionmaker: sessionmaker = session_maker
    else:
        _sessionmaker: sessionmaker = _get_sessionmaker()
    values = values or {}
    pipeline_columns = [
        c for c in columns if c in RelionPipelineInfo.__table__.columns.keys()
    ]
    pipeline_values = {
        k: v
        for k, v in values.items()
        if k in RelionPipelineInfo.__table__.columns.keys()
    }
    job_columns = [c for c in columns if c in RelionJobInfo.__table__.columns.keys()]
    job_values = {
        k: v for k, v in values.items() if k in RelionJobInfo.__table__.columns.keys()
    }
    cluster_columns = [
        c for c in columns if c in ClusterJobInfo.__table__.columns.keys()
    ]
    cluster_values = {
        k: v for k, v in values.items() if k in ClusterJobInfo.__table__.columns.keys()
    }
    extras = [
        c
        for c, p in zip(
            ["cluster_id", "pipeline_id"], [cluster_columns, pipeline_columns]
        )
        if p
    ]
    tables = [
        c
        for c, p in zip(
            [RelionJobInfo, RelionPipelineInfo, ClusterJobInfo],
            [True, pipeline_columns, cluster_columns],
        )
        if p
    ]
    filters = (
        [getattr(RelionJobInfo, k) == v for k, v in job_values.items()]
        + [getattr(ClusterJobInfo, k) == v for k, v in cluster_values.items()]
        + [getattr(RelionPipelineInfo, k) == v for k, v in pipeline_values.items()]
    )
    with _sessionmaker() as session:
        query = session.query(*tables).options(
            Load(RelionJobInfo).load_only(*(job_columns + extras))
        )
        if pipeline_columns:
            query = query.options(
                Load(RelionPipelineInfo).load_only(
                    *(pipeline_columns + ["pipeline_id"])
                ),
            ).join(
                RelionPipelineInfo,
                RelionPipelineInfo.pipeline_id == RelionJobInfo.pipeline_id,
            )
        if cluster_columns:
            query = query.options(
                Load(ClusterJobInfo).load_only(*(cluster_columns + ["cluster_id"])),
            ).join(
                ClusterJobInfo,
                ClusterJobInfo.cluster_id == RelionJobInfo.cluster_id,
            )

        if values:
            query = query.filter(*filters)

        df = pd.read_sql(query.statement, session.bind)
    return df


def make_plot(
    x_key: str,
    y_key: str,
    df: pd.DataFrame,
    allow: Optional[Dict[str, Any]] = None,
    errors: bool = False,
    x_label: Optional[str] = None,
    y_label: Optional[str] = None,
    save_to: str = "",
):
    x_vals = list(df[x_key].unique())
    if allow:
        conditions = [df[k] == v for k, v in allow.items()]
    else:
        conditions = []
    y_vals = [
        df[functools.reduce(and_, conditions + [df[x_key] == x])][y_key].mean()
        for x in x_vals
    ]
    if errors:
        y_errs = [
            df[functools.reduce(and_, conditions + [df[x_key] == x])][y_key].std()
            for x in x_vals
        ]
    plt.rcParams.update(
        {
            "text.usetex": True,
            "font.family": "serif",
            "font.serif": ["Computer Modern"],
            "font.size": 14,
        }
    )
    plt.figure(figsize=((4.5, 4.5 / 1.618)))

    fig, ax = plt.subplots()
    if errors:
        ax.errorbar(x_vals, y_vals, yerr=y_errs, fmt="o", mfc="none")
    else:
        ax.plot(x_vals, y_vals, "o")
    ax.tick_params(axis="both", direction="in", top=True, right=True)
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    plt.tight_layout()
    if save_to:
        plt.savefig(save_to)
    plt.show()


def make_pie_chart(
    label_key: str,
    key: str,
    df: pd.DataFrame,
    allow: Optional[List[Any]] = None,
    save_to: str = "",
):
    vals = list(df[label_key].unique())
    vals = [v for v in vals if v in allow]
    sizes = [df[df[label_key] == pid][key].sum() for pid in vals]
    _val_sizes = {k: v for k, v in zip(vals, sizes)}
    sizes.sort()
    vals.sort(key=lambda e: _val_sizes[e])
    plt.rcParams.update(
        {
            "text.usetex": True,
            "font.family": "serif",
            "font.serif": ["Computer Modern"],
            "font.size": 14,
        }
    )
    plt.figure(figsize=((4.5, 4.5 / 1.618)))

    fig, ax = plt.subplots()
    ax.pie(
        sizes,
        labels=[t.replace("_", r"\_") for t in vals],
        autopct="%1.1f%%",
        shadow=True,
    )
    ax.axis("equal")
    plt.tight_layout()
    if save_to:
        plt.savefig(save_to)
    plt.show()


def make_histogram(
    key: str,
    df: pd.DataFrame,
    key_filter: Optional[Dict[str, List[Any]]] = None,
    split_filter_key: str = "",
    x_label: Optional[str] = None,
    y_label: Optional[str] = None,
    hist_range: Optional[Tuple[float, float]] = None,
    save_to: str = "",
):
    legend_labels = []
    if key_filter:
        data = []
        if split_filter_key:
            conditions = [
                df[k].isin(v) for k, v in key_filter.items() if k != split_filter_key
            ]
            if conditions:
                _data = df[functools.reduce(and_, conditions)]
            else:
                _data = df
            data.extend(
                _data[_data[split_filter_key] == val][key]
                for val in key_filter[split_filter_key]
            )
            legend_labels.extend(key_filter[split_filter_key])
        else:
            for k, v in key_filter.items():
                data.extend(chain.from_iterable(df[df[k] == val][key] for val in v))
    else:
        data = df[key]

    plt.rcParams.update(
        {
            "text.usetex": True,
            "font.family": "serif",
            "font.serif": ["Computer Modern"],
            "font.size": 14,
        }
    )
    plt.figure(figsize=((4.5, 4.5 / 1.618)))

    fig, ax = plt.subplots()
    if legend_labels:
        ax.hist(data, range=hist_range, label=legend_labels)
        ax.legend(frameon=False)
    else:
        ax.hist(data, range=hist_range)
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    plt.tight_layout()
    if save_to:
        plt.savefig(save_to)
    plt.show()


def make_bar_chart(
    x_key: str,
    y_key: str,
    df: pd.DataFrame,
    x_label: Optional[str] = None,
    y_label: Optional[str] = None,
    count: bool = False,
    restrict_job_types: List[str] = _jobs_of_interest,
    group: Optional[List[List[str]]] = None,
    save_to: str = "",
):
    x_vals = list(df[x_key].unique())
    if x_key == "job_name":
        x_vals = [_ for _ in x_vals if _ in restrict_job_types]
    if group:
        y_vals_group = []
        for xkg in group:
            if count:
                y_vals_group.append([len(df[df[x_key] == pid]) for pid in xkg])
            else:
                y_vals_group.append([df[df[x_key] == pid][y_key].sum() for pid in xkg])
    else:
        if count:
            y_vals = [len(df[df[x_key] == pid]) for pid in x_vals]
        else:
            y_vals = [df[df[x_key] == pid][y_key].sum() for pid in x_vals]
    x = range(1, len(x_vals) + 1)

    plt.rcParams.update(
        {
            "text.usetex": True,
            "font.family": "serif",
            "font.serif": ["Computer Modern"],
            "font.size": 14,
        }
    )
    plt.figure(figsize=((4.5, 4.5 / 1.618)))

    fig, ax = plt.subplots()
    if group:
        for _y_vals, _group_vals in zip(y_vals_group, group):
            _x = [x_vals.index(_g) + 1 for _g in _group_vals]
            ax.bar(_x, _y_vals)
    else:
        ax.bar(x, y_vals)
    ax.set_xticks(x)
    ax.set_xticklabels(
        [t.replace("_", r"\_") for t in x_vals], rotation="330", ha="left"
    )
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    plt.tight_layout()
    if save_to:
        plt.savefig(save_to)
    plt.show()


def max_of(key: str, df: pd.DataFrame) -> Any:
    return df[key].max()


def min_of(key: str, df: pd.DataFrame) -> Any:
    return df[key].min()


def options(key: str, df: pd.DataFrame) -> List[Any]:
    return list(df[key].unique())
