"""This module contains utilities and wrappers that instrument tracing over our RayRunner's task scheduling + execution

These utilities are meant to provide light wrappers on top of Ray functionality (e.g. remote functions, actors, ray.get/ray.wait)
which allow us to intercept these calls and perform the necessary actions for tracing the interaction between Daft and Ray.
"""

from __future__ import annotations

import contextlib
import dataclasses
import json
import pathlib
import time
from datetime import datetime
from typing import TYPE_CHECKING, Any, TextIO

try:
    import ray
except ImportError:
    raise

from daft.execution.execution_step import PartitionTask
from daft.runners import ray_metrics

if TYPE_CHECKING:
    from daft import ResourceRequest
    from daft.execution.physical_plan import MaterializedPhysicalPlan


# We add the trace by default to the latest session logs of the Ray Runner
# This lets us access our logs via the Ray dashboard when running Ray jobs
DEFAULT_RAY_LOGS_LOCATION = pathlib.Path("/tmp") / "ray" / "session_latest"
DEFAULT_DAFT_TRACE_LOCATION = DEFAULT_RAY_LOGS_LOCATION / "daft"

# PIDs to reserve for custom visualization
RESERVED_PIDS = 100


@contextlib.contextmanager
def ray_tracer(job_id: str):
    metrics_actor = ray_metrics.get_metrics_actor(job_id)

    # Dump the RayRunner trace if we detect an active Ray session, otherwise we give up and do not write the trace
    if pathlib.Path(DEFAULT_RAY_LOGS_LOCATION).exists():
        trace_filename = (
            f"trace_RayRunner.{job_id}.{datetime.replace(datetime.now(), microsecond=0).isoformat()[:-3]}.json"
        )
        daft_trace_location = pathlib.Path(DEFAULT_DAFT_TRACE_LOCATION)
        daft_trace_location.mkdir(exist_ok=True, parents=True)
        filepath = DEFAULT_DAFT_TRACE_LOCATION / trace_filename
    else:
        filepath = None

    tracer_start = time.time()

    if filepath is not None:
        with open(filepath, "w") as f:
            # Initialize the JSON file
            f.write("[")

            # Yield the tracer
            runner_tracer = RunnerTracer(f, tracer_start)
            yield runner_tracer

            # Retrieve metrics from the metrics actor
            metrics = ray.get(metrics_actor.collect_task_metrics.remote())

            task_locations = {metric.task_id: (metric.node_id, metric.worker_id) for metric in metrics}
            nodes_to_workers: dict[str, set[str]] = {}
            nodes = set()
            for node_id, worker_id in task_locations.values():
                nodes.add(node_id)
                if node_id not in nodes_to_workers:
                    nodes_to_workers[node_id] = set()
                nodes_to_workers[node_id].add(worker_id)
            nodes_to_pid_mapping = {node_id: i + RESERVED_PIDS for i, node_id in enumerate(nodes)}
            nodes_workers_to_tid_mapping = {
                node_id: {worker_id: i for i, worker_id in enumerate(worker_ids)}
                for node_id, worker_ids in nodes_to_workers.items()
            }
            parsed_task_node_locations = {
                task_id: (nodes_to_pid_mapping[node_id], nodes_workers_to_tid_mapping[node_id][worker_id])
                for task_id, (node_id, worker_id) in task_locations.items()
            }

            for node_id, pid in nodes_to_pid_mapping.items():
                f.write(
                    json.dumps(
                        {
                            "name": "process_name",
                            "ph": "M",
                            "pid": pid,
                            "args": {"name": f"Node {node_id[:8]}"},
                            "sort_index": 3,
                        }
                    )
                )
                f.write(",\n")
                for worker_id in nodes_workers_to_tid_mapping[node_id]:
                    tid = nodes_workers_to_tid_mapping[node_id][worker_id]
                    f.write(
                        json.dumps(
                            {
                                "name": "thread_name",
                                "ph": "M",
                                "pid": pid,
                                "tid": tid,
                                "args": {"name": f"Worker {worker_id[:8]}"},
                            }
                        )
                    )
                    f.write(",\n")

            for metric in metrics:
                runner_tracer.write_task_metric(metric, parsed_task_node_locations.get(metric.task_id))

            stage_id_to_start_end: dict[int, tuple[float, float]] = {}
            for metric in metrics:
                if metric.stage_id not in stage_id_to_start_end:
                    stage_id_to_start_end[metric.stage_id] = (metric.start, metric.end)
                else:
                    old_start, old_end = stage_id_to_start_end[metric.stage_id]
                    stage_id_to_start_end[metric.stage_id] = (min(old_start, metric.start), max(old_end, metric.end))
            runner_tracer.write_stages(stage_id_to_start_end)

            # Add the final touches to the file
            f.write(
                json.dumps(
                    {"name": "process_name", "ph": "M", "pid": 1, "args": {"name": "Scheduler"}, "sort_index": 2}
                )
            )
            f.write(",\n")
            f.write(
                json.dumps(
                    {"name": "thread_name", "ph": "M", "pid": 1, "tid": 1, "args": {"name": "_run_plan dispatch loop"}}
                )
            )
            f.write(",\n")
            f.write(
                json.dumps(
                    {
                        "name": "process_name",
                        "ph": "M",
                        "pid": 2,
                        "args": {"name": "Tasks (Grouped by Stage ID)"},
                        "sort_index": 1,
                    }
                )
            )
            f.write("\n]")
    else:
        runner_tracer = RunnerTracer(None, tracer_start)
        yield runner_tracer


class RunnerTracer:
    def __init__(self, file: TextIO | None, start: float):
        self._file = file
        self._start = start
        self._stage_start_end: dict[int, tuple[int, int]] = {}

    def _write_event(self, event: dict[str, Any], ts: int | None = None) -> int:
        ts = int((time.time() - self._start) * 1000 * 1000) if ts is None else ts
        if self._file is not None:
            self._file.write(
                json.dumps(
                    {
                        **event,
                        "ts": ts,
                    }
                )
            )
            self._file.write(",\n")
        return ts

    def write_stages(self, stages: dict[int, tuple[float, float]]):
        for stage_id in stages:
            start, end = stages[stage_id]
            self._write_event(
                {
                    "name": f"stage-{stage_id}",
                    "ph": "B",
                    "pid": 2,
                    "tid": stage_id,
                },
                ts=int((start - self._start) * 1000 * 1000),
            )
            self._write_event(
                {
                    "name": f"stage-{stage_id}",
                    "ph": "E",
                    "pid": 2,
                    "tid": stage_id,
                },
                ts=int((end - self._start) * 1000 * 1000),
            )

    def write_task_metric(self, metric: ray_metrics.TaskMetric, node_id_worker_id: tuple[int, int] | None):
        # Write to the Async view (will group by the stage ID)
        self._write_event(
            {
                "id": metric.task_id,
                "category": "task",
                "name": "task_remote_execution",
                "ph": "b",
                "pid": 2,
                "tid": metric.stage_id,
            },
            ts=int((metric.start - self._start) * 1000 * 1000),
        )
        if metric.end is not None:
            self._write_event(
                {
                    "id": metric.task_id,
                    "category": "task",
                    "name": "task_remote_execution",
                    "ph": "e",
                    "pid": 2,
                    "tid": metric.stage_id,
                },
                ts=int((metric.end - self._start) * 1000 * 1000),
            )

        # Write to the node/worker view
        if node_id_worker_id is not None:
            pid, tid = node_id_worker_id
            self._write_event(
                {
                    "name": "task_remote_execution",
                    "ph": "B",
                    "pid": pid,
                    "tid": tid,
                },
                ts=int((metric.start - self._start) * 1000 * 1000),
            )
            if metric.end is not None:
                self._write_event(
                    {
                        "name": "task_remote_execution",
                        "ph": "E",
                        "pid": pid,
                        "tid": tid,
                    },
                    ts=int((metric.end - self._start) * 1000 * 1000),
                )

    @contextlib.contextmanager
    def dispatch_wave(self, wave_num: int):
        self._write_event(
            {
                "name": f"wave-{wave_num}",
                "pid": 1,
                "tid": 1,
                "ph": "B",
                "args": {"wave_num": wave_num},
            }
        )

        metrics = {}

        def metrics_updater(**kwargs):
            metrics.update(kwargs)

        yield metrics_updater

        self._write_event(
            {
                "name": f"wave-{wave_num}",
                "pid": 1,
                "tid": 1,
                "ph": "E",
                "args": metrics,
            }
        )

    def count_inflight_tasks(self, count: int):
        self._write_event(
            {
                "name": "dispatch_metrics",
                "ph": "C",
                "pid": 1,
                "tid": 1,
                "args": {"num_inflight_tasks": count},
            }
        )

    ###
    # Tracing the dispatch batching: when the runner is retrieving enough tasks
    # from the physical plan in order to put them into a batch.
    ###

    @contextlib.contextmanager
    def dispatch_batching(self):
        self._write_event(
            {
                "name": "dispatch_batching",
                "pid": 1,
                "tid": 1,
                "ph": "B",
            }
        )
        yield
        self._write_event(
            {
                "name": "dispatch_batching",
                "pid": 1,
                "tid": 1,
                "ph": "E",
            }
        )

    def mark_noop_task_start(self):
        """Marks the start of running a no-op task"""
        self._write_event(
            {
                "name": "no_op_task",
                "pid": 1,
                "tid": 1,
                "ph": "B",
            }
        )

    def mark_noop_task_end(self):
        """Marks the start of running a no-op task"""
        self._write_event(
            {
                "name": "no_op_task",
                "pid": 1,
                "tid": 1,
                "ph": "E",
            }
        )

    def mark_handle_none_task(self):
        """Marks when the underlying physical plan returns None"""
        self._write_event(
            {
                "name": "Physical Plan returned None, needs more progress",
                "ph": "i",
                "pid": 1,
                "tid": 1,
            }
        )

    def mark_handle_materialized_result(self):
        """Marks when the underlying physical plan returns Materialized Result"""
        self._write_event(
            {
                "name": "Physical Plan returned Materialized Result",
                "ph": "i",
                "pid": 1,
                "tid": 1,
            }
        )

    def mark_handle_task_add_to_batch(self):
        """Marks when the underlying physical plan returns a task that we need to add to the dispatch batch"""
        self._write_event(
            {
                "name": "Physical Plan returned Task to add to batch",
                "ph": "i",
                "pid": 1,
                "tid": 1,
            }
        )

    ###
    # Tracing the dispatching of tasks
    ###

    @contextlib.contextmanager
    def dispatching(self):
        self._write_event(
            {
                "name": "dispatching",
                "pid": 1,
                "tid": 1,
                "ph": "B",
            }
        )
        yield
        self._write_event(
            {
                "name": "dispatching",
                "pid": 1,
                "tid": 1,
                "ph": "E",
            }
        )

    def mark_dispatch_task(self):
        self._write_event(
            {
                "name": "dispatch_task",
                "ph": "i",
                "pid": 1,
                "tid": 1,
            }
        )

    def mark_dispatch_actor_task(self):
        self._write_event(
            {
                "name": "dispatch_actor_task",
                "ph": "i",
                "pid": 1,
                "tid": 1,
            }
        )

    ###
    # Tracing the waiting of tasks
    ###

    @contextlib.contextmanager
    def awaiting(self, waiting_for_num_results: int, wait_timeout_s: float | None):
        name = f"awaiting {waiting_for_num_results} (timeout={wait_timeout_s})"
        self._write_event(
            {
                "name": name,
                "pid": 1,
                "tid": 1,
                "ph": "B",
                "args": {
                    "waiting_for_num_results": waiting_for_num_results,
                    "wait_timeout_s": str(wait_timeout_s),
                },
            }
        )
        yield
        self._write_event(
            {
                "name": name,
                "pid": 1,
                "tid": 1,
                "ph": "E",
            }
        )

    ###
    # Tracing the PhysicalPlan
    ###

    @contextlib.contextmanager
    def get_next_physical_plan(self):
        self._write_event(
            {
                "name": "next(tasks)",
                "pid": 1,
                "tid": 1,
                "ph": "B",
            }
        )

        args = {}

        def update_args(**kwargs):
            args.update(kwargs)

        yield update_args

        self._write_event(
            {
                "name": "next(tasks)",
                "pid": 1,
                "tid": 1,
                "ph": "E",
                "args": args,
            }
        )

    ###
    # Tracing each individual task as an Async Event
    ###

    def task_created(self, task_id: str, stage_id: int, resource_request: ResourceRequest, instructions: str):
        created_ts = self._write_event(
            {
                "id": task_id,
                "category": "task",
                "name": f"task_execution.stage-{stage_id}",
                "ph": "b",
                "args": {
                    "task_id": task_id,
                    "resource_request": {
                        "num_cpus": resource_request.num_cpus,
                        "num_gpus": resource_request.num_gpus,
                        "memory_bytes": resource_request.memory_bytes,
                    },
                    "stage_id": stage_id,
                    "instructions": instructions,
                },
                "pid": 2,
                "tid": 1,
            }
        )

        if stage_id not in self._stage_start_end:
            self._stage_start_end[stage_id] = (created_ts, created_ts)

    def task_dispatched(self, task_id: str):
        self._write_event(
            {
                "id": task_id,
                "category": "task",
                "name": "task_dispatch",
                "ph": "b",
                "pid": 2,
                "tid": 1,
            }
        )

    def task_not_ready(self, task_id: str):
        self._write_event(
            {
                "id": task_id,
                "category": "task",
                "name": "task_awaited_not_ready",
                "ph": "n",
                "pid": 2,
                "tid": 1,
            }
        )

    def task_received_as_ready(self, task_id: str, stage_id: int):
        self._write_event(
            {
                "id": task_id,
                "category": "task",
                "name": "task_dispatch",
                "ph": "e",
                "pid": 2,
                "tid": 1,
            }
        )
        new_end = self._write_event(
            {
                "id": task_id,
                "category": "task",
                "name": f"task_execution.stage-{stage_id}",
                "ph": "e",
                "pid": 2,
                "tid": 1,
            }
        )

        assert stage_id in self._stage_start_end
        old_start, _ = self._stage_start_end[stage_id]
        self._stage_start_end[stage_id] = (old_start, new_end)


@dataclasses.dataclass(frozen=True)
class RayFunctionWrapper:
    """Wrapper around a Ray remote function that allows us to intercept calls and record the call for a given task ID"""

    f: ray.remote_function.RemoteFunction

    def with_tracing(self, runner_tracer: RunnerTracer, task: PartitionTask) -> RayRunnableFunctionWrapper:
        return RayRunnableFunctionWrapper(f=self.f, runner_tracer=runner_tracer, task=task)

    def options(self, *args, **kwargs) -> RayFunctionWrapper:
        return dataclasses.replace(self, f=self.f.options(*args, **kwargs))

    @classmethod
    def wrap(cls, f: ray.remote_function.RemoteFunction):
        return cls(f=f)


@dataclasses.dataclass(frozen=True)
class RayRunnableFunctionWrapper:
    """Runnable variant of RayFunctionWrapper that supports `.remote` calls"""

    f: ray.remote_function.RemoteFunction
    runner_tracer: RunnerTracer
    task: PartitionTask

    def options(self, *args, **kwargs) -> RayRunnableFunctionWrapper:
        return dataclasses.replace(self, f=self.f.options(*args, **kwargs))

    def remote(self, *args, **kwargs):
        self.runner_tracer.task_dispatched(self.task.id())
        return self.f.remote(*args, **kwargs)


@dataclasses.dataclass(frozen=True)
class MaterializedPhysicalPlanWrapper:
    """Wrapper around MaterializedPhysicalPlan that hooks into tracing capabilities"""

    plan: MaterializedPhysicalPlan
    runner_tracer: RunnerTracer

    def __next__(self):
        with self.runner_tracer.get_next_physical_plan() as update_args:
            item = next(self.plan)

            update_args(
                next_item_type=type(item).__name__,
            )
            if isinstance(item, PartitionTask):
                instructions_description = "-".join(type(i).__name__ for i in item.instructions)
                self.runner_tracer.task_created(
                    item.id(),
                    item.stage_id,
                    item.resource_request,
                    instructions_description,
                )
                update_args(
                    partition_task_instructions=instructions_description,
                )

        return item


@contextlib.contextmanager
def collect_ray_task_metrics(job_id: str, task_id: str, stage_id: int):
    """Context manager that will ping the metrics actor to record various execution metrics about a given task"""
    import time

    runtime_context = ray.get_runtime_context()

    metrics_actor = ray_metrics.get_metrics_actor(job_id)
    metrics_actor.mark_task_start.remote(
        task_id, time.time(), runtime_context.get_node_id(), runtime_context.get_worker_id(), stage_id
    )
    yield
    metrics_actor.mark_task_end.remote(task_id, time.time())
