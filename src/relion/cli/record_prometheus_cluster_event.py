import argparse

import prometheus_client

_job_count = prometheus_client.Counter(
    "cluster_job_count", "Total number of cluster jobs that started"
)

_current_job_count = prometheus_client.Gauge(
    "cluster_current_job_count", "Number of currently running cluster jobs"
)


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--start", action="store_true", dest="start")
    parser.add_argument("-e", "--end", action="store_true", dest="end")
    args = parser.parse_args()
    if args.start:
        _job_count.inc()
        _current_job_count.inc()
    if args.end:
        _current_job_count.dec()
