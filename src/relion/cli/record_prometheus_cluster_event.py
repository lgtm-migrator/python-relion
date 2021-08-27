import argparse

import prometheus_client

_job_count = prometheus_client.Counter(
    "cluster_job_count",
    "Total number of cluster jobs that started",
    ["command", "job_id"],
)

_current_job_count = prometheus_client.Gauge(
    "cluster_current_job_count",
    "Number of currently running cluster jobs",
    ["command", "job_id"],
)


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--start", action="store_true", dest="start")
    parser.add_argument("-e", "--end", action="store_true", dest="end")
    parser.add_argument("-c", "--command", dest="command")
    parser.add_argument("--id", dest="job_id")
    args = parser.parse_args()
    if args.start:
        _job_count.labels(args.command, args.job_id).inc()
        _current_job_count.labels(args.command, args.job_id).inc()
    if args.end:
        _current_job_count.labels(args.command, args.job_id).dec()
