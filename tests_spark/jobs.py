"""Counts the Spark jobs of a block of driver code, through the status tracker and a job group."""

import itertools
from contextlib import contextmanager
from typing import Any, Iterator, List

_groups = itertools.count()


class Jobs:
    def __init__(self) -> None:
        self.count = 0


@contextmanager
def count_jobs(spark: Any) -> Iterator[Jobs]:
    sc = spark.sparkContext
    group = f"s2d-jobs-{next(_groups)}"
    sc.setJobGroup(group, group)
    jobs = Jobs()
    try:
        yield jobs
    finally:
        ids: List[int] = list(sc.statusTracker().getJobIdsForGroup(group))
        jobs.count = len(ids)
        sc.setLocalProperty("spark.jobGroup.id", None)
