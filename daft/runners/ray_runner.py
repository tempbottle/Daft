from __future__ import annotations

import contextlib
import logging
import threading
import time
import uuid
from datetime import datetime
from queue import Full, Queue
from typing import TYPE_CHECKING, Any, Generator, Iterable, Iterator

# The ray runner is not a top-level module, so we don't need to lazily import pyarrow to minimize
# import times. If this changes, we first need to make the daft.lazy_import.LazyImport class
# serializable before importing pa from daft.dependencies.
import pyarrow as pa  # noqa: TID253
import ray.experimental  # noqa: TID253

from daft.arrow_utils import ensure_array
from daft.context import execution_config_ctx, get_context
from daft.daft import PyTable as _PyTable
from daft.dependencies import np
from daft.expressions import ExpressionsProjection
from daft.runners.progress_bar import ProgressBar
from daft.series import Series, item_to_series
from daft.table import Table

logger = logging.getLogger(__name__)

try:
    import ray
except ImportError:
    logger.error(
        "Error when importing Ray. Please ensure that getdaft was installed with the Ray extras tag: getdaft[ray] (https://www.getdaft.io/projects/docs/en/latest/learn/install.html)"
    )
    raise

from daft.daft import (
    FileFormatConfig,
    FileInfos,
    IOConfig,
    PyDaftExecutionConfig,
    ResourceRequest,
    extract_partial_stateful_udf_py,
)
from daft.datatype import DataType
from daft.execution.execution_step import (
    FanoutInstruction,
    Instruction,
    MultiOutputPartitionTask,
    PartitionTask,
    ReduceInstruction,
    ScanWithTask,
    SingleOutputPartitionTask,
    StatefulUDFProject,
)
from daft.filesystem import glob_path_with_stats
from daft.runners import runner_io
from daft.runners.partitioning import (
    MaterializedResult,
    PartID,
    PartitionCacheEntry,
    PartitionMetadata,
    PartitionSet,
)
from daft.runners.profiler import profiler
from daft.runners.pyrunner import LocalPartitionSet
from daft.runners.runner import Runner
from daft.table import MicroPartition

if TYPE_CHECKING:
    import dask
    import pandas as pd
    from ray.data.block import Block as RayDatasetBlock
    from ray.data.dataset import Dataset as RayDataset

    from daft.logical.builder import LogicalPlanBuilder
    from daft.plan_scheduler import PhysicalPlanScheduler

_RAY_FROM_ARROW_REFS_AVAILABLE = True
try:
    from ray.data import from_arrow_refs
except ImportError:
    _RAY_FROM_ARROW_REFS_AVAILABLE = False

from daft.logical.schema import Schema

RAY_VERSION = tuple(int(s) for s in ray.__version__.split(".")[0:3])

_RAY_DATA_ARROW_TENSOR_TYPE_AVAILABLE = True
try:
    from ray.data.extensions import ArrowTensorArray, ArrowTensorType
except ImportError:
    _RAY_DATA_ARROW_TENSOR_TYPE_AVAILABLE = False

_RAY_DATA_EXTENSIONS_AVAILABLE = True
_TENSOR_EXTENSION_TYPES = []
try:
    import ray
except ImportError:
    _RAY_DATA_EXTENSIONS_AVAILABLE = False
else:
    _RAY_VERSION = tuple(int(s) for s in ray.__version__.split(".")[0:3])
    try:
        # Variable-shaped tensor column support was added in Ray 2.1.0.
        if _RAY_VERSION >= (2, 2, 0):
            from ray.data.extensions import (
                ArrowTensorType,
                ArrowVariableShapedTensorType,
            )

            _TENSOR_EXTENSION_TYPES = [ArrowTensorType, ArrowVariableShapedTensorType]
        else:
            from ray.data.extensions import ArrowTensorType

            _TENSOR_EXTENSION_TYPES = [ArrowTensorType]
    except ImportError:
        _RAY_DATA_EXTENSIONS_AVAILABLE = False


@ray.remote
def _glob_path_into_file_infos(
    paths: list[str],
    file_format_config: FileFormatConfig | None,
    io_config: IOConfig | None,
) -> MicroPartition:
    file_infos = FileInfos()
    file_format = file_format_config.file_format() if file_format_config is not None else None
    for path in paths:
        path_file_infos = glob_path_with_stats(path, file_format=file_format, io_config=io_config)
        if len(path_file_infos) == 0:
            raise FileNotFoundError(f"No files found at {path}")
        file_infos.extend(path_file_infos)

    return MicroPartition._from_pytable(file_infos.to_table())


@ray.remote
def _make_ray_block_from_micropartition(partition: MicroPartition) -> RayDatasetBlock:
    try:
        daft_schema = partition.schema()
        arrow_tbl = partition.to_arrow()

        # Convert arrays to Ray Data's native ArrowTensorType arrays
        new_arrs = {}
        for idx, field in enumerate(arrow_tbl.schema):
            if daft_schema[field.name].dtype._is_fixed_shape_tensor_type():
                assert isinstance(field.type, pa.FixedShapeTensorType)
                new_dtype = ArrowTensorType(field.type.shape, field.type.value_type)
                arrow_arr = arrow_tbl[field.name].combine_chunks()
                storage_arr = arrow_arr.storage
                list_size = storage_arr.type.list_size
                new_storage_arr = pa.ListArray.from_arrays(
                    pa.array(
                        list(range(0, (len(arrow_arr) + 1) * list_size, list_size)),
                        pa.int32(),
                    ),
                    storage_arr.values,
                )
                new_arrs[idx] = (
                    field.name,
                    pa.ExtensionArray.from_storage(new_dtype, new_storage_arr),
                )
            elif daft_schema[field.name].dtype._is_tensor_type():
                assert isinstance(field.type, pa.ExtensionType)
                new_arrs[idx] = (field.name, ArrowTensorArray.from_numpy(partition.get_column(field.name).to_pylist()))
        for idx, (field_name, arr) in new_arrs.items():
            arrow_tbl = arrow_tbl.set_column(idx, pa.field(field_name, arr.type), arr)

        return arrow_tbl
    except pa.ArrowInvalid:
        return partition.to_pylist()


def _series_from_arrow_with_ray_data_extensions(
    array: pa.Array | pa.ChunkedArray, name: str = "arrow_series"
) -> Series:
    if isinstance(array, pa.Array):
        # TODO(desmond): This might be dead code since `ArrayTensorType`s are `numpy.ndarray` under
        # the hood and are not instances of `pyarrow.Array`. Should follow up and check if this code
        # can be removed.
        array = ensure_array(array)
        if _RAY_DATA_EXTENSIONS_AVAILABLE and isinstance(array.type, ArrowTensorType):
            storage_series = _series_from_arrow_with_ray_data_extensions(array.storage, name=name)
            series = storage_series.cast(
                DataType.fixed_size_list(
                    _from_arrow_type_with_ray_data_extensions(array.type.scalar_type),
                    int(np.prod(array.type.shape)),
                )
            )
            return series.cast(DataType.from_arrow_type(array.type))
        elif _RAY_DATA_EXTENSIONS_AVAILABLE and isinstance(array.type, ArrowVariableShapedTensorType):
            return Series.from_numpy(array.to_numpy(zero_copy_only=False), name=name)
    return Series.from_arrow(array, name)


def _micropartition_from_arrow_with_ray_data_extensions(arrow_table: pa.Table) -> MicroPartition:
    assert isinstance(arrow_table, pa.Table)
    non_native_fields = []
    for arrow_field in arrow_table.schema:
        dt = _from_arrow_type_with_ray_data_extensions(arrow_field.type)
        if dt == DataType.python() or dt._is_tensor_type() or dt._is_fixed_shape_tensor_type():
            non_native_fields.append(arrow_field.name)
    if non_native_fields:
        # If there are any contained Arrow types that are not natively supported, convert each
        # series while checking for ray data extension types.
        logger.debug("Unsupported Arrow types detected for columns: %s", non_native_fields)
        series_dict = dict()
        for name, column in zip(arrow_table.column_names, arrow_table.columns):
            series = (
                _series_from_arrow_with_ray_data_extensions(column, name)
                if isinstance(column, (pa.Array, pa.ChunkedArray))
                else item_to_series(name, column)
            )
            series_dict[name] = series._series
        return MicroPartition._from_tables([Table._from_pytable(_PyTable.from_pylist_series(series_dict))])
    return MicroPartition.from_arrow(arrow_table)


@ray.remote
def _make_daft_partition_from_ray_dataset_blocks(
    ray_dataset_block: pa.MicroPartition, daft_schema: Schema
) -> MicroPartition:
    return _micropartition_from_arrow_with_ray_data_extensions(ray_dataset_block)


@ray.remote(num_returns=2)
def _make_daft_partition_from_dask_dataframe_partitions(
    dask_df_partition: pd.DataFrame,
) -> tuple[MicroPartition, pa.Schema]:
    vpart = MicroPartition.from_pandas(dask_df_partition)
    return vpart, vpart.schema()


def _to_pandas_ref(df: pd.DataFrame | ray.ObjectRef[pd.DataFrame]) -> ray.ObjectRef[pd.DataFrame]:
    """Ensures that the provided pandas DataFrame partition is in the Ray object store."""
    import pandas as pd

    if isinstance(df, pd.DataFrame):
        return ray.put(df)
    elif isinstance(df, ray.ObjectRef):
        return df
    else:
        raise ValueError("Expected a Ray object ref or a Pandas DataFrame, " f"got {type(df)}")


class RayPartitionSet(PartitionSet[ray.ObjectRef]):
    _results: dict[PartID, RayMaterializedResult]

    def __init__(self) -> None:
        super().__init__()
        self._results = {}

    def items(self) -> list[tuple[PartID, MaterializedResult[ray.ObjectRef]]]:
        return [(pid, result) for pid, result in sorted(self._results.items())]

    def _get_merged_micropartition(self) -> MicroPartition:
        ids_and_partitions = self.items()
        assert ids_and_partitions[0][0] == 0
        assert ids_and_partitions[-1][0] + 1 == len(ids_and_partitions)
        all_partitions = ray.get([part.partition() for id, part in ids_and_partitions])
        return MicroPartition.concat(all_partitions)

    def _get_preview_micropartitions(self, num_rows: int) -> list[MicroPartition]:
        ids_and_partitions = self.items()
        preview_parts = []
        for _, mat_result in ids_and_partitions:
            ref: ray.ObjectRef = mat_result.partition()
            part: MicroPartition = ray.get(ref)
            part_len = len(part)
            if part_len >= num_rows:  # if this part has enough rows, take what we need and break
                preview_parts.append(part.slice(0, num_rows))
                break
            else:  # otherwise, take the whole part and keep going
                num_rows -= part_len
                preview_parts.append(part)
        return preview_parts

    def to_ray_dataset(self) -> RayDataset:
        if not _RAY_FROM_ARROW_REFS_AVAILABLE:
            raise ImportError(
                "Unable to import `ray.data.from_arrow_refs`. Please ensure that you have a compatible version of Ray >= 1.10 installed."
            )

        blocks = [
            _make_ray_block_from_micropartition.remote(self._results[k].partition()) for k in self._results.keys()
        ]
        # NOTE: although the Ray method is called `from_arrow_refs`, this method works also when the blocks are List[T] types
        # instead of Arrow tables as the codepath for Dataset creation is the same.
        return from_arrow_refs(blocks)

    def to_dask_dataframe(
        self,
        meta: (pd.DataFrame | pd.Series | dict[str, Any] | Iterable[Any] | tuple[Any] | None) = None,
    ) -> dask.DataFrame:
        import dask
        import dask.dataframe as dd
        from ray.util.dask import ray_dask_get

        dask.config.set(scheduler=ray_dask_get)

        @dask.delayed
        def _make_dask_dataframe_partition_from_micropartition(partition: MicroPartition) -> pd.DataFrame:
            return partition.to_pandas()

        ddf_parts = [
            _make_dask_dataframe_partition_from_micropartition(self._results[k].partition())
            for k in self._results.keys()
        ]
        return dd.from_delayed(ddf_parts, meta=meta)

    def get_partition(self, idx: PartID) -> ray.ObjectRef:
        return self._results[idx].partition()

    def set_partition(self, idx: PartID, result: MaterializedResult[ray.ObjectRef]) -> None:
        assert isinstance(result, RayMaterializedResult)
        self._results[idx] = result

    def delete_partition(self, idx: PartID) -> None:
        del self._results[idx]

    def has_partition(self, idx: PartID) -> bool:
        return idx in self._results

    def __len__(self) -> int:
        return sum(result.metadata().num_rows for result in self._results.values())

    def size_bytes(self) -> int | None:
        size_bytes_ = [result.metadata().size_bytes for result in self._results.values()]
        size_bytes: list[int] = [size for size in size_bytes_ if size is not None]
        if len(size_bytes) != len(size_bytes_):
            return None
        else:
            return sum(size_bytes)

    def num_partitions(self) -> int:
        return len(self._results)

    def wait(self) -> None:
        deduped_object_refs = {r.partition() for r in self._results.values()}
        ray.wait(list(deduped_object_refs))


def _from_arrow_type_with_ray_data_extensions(arrow_type: pa.lib.DataType) -> DataType:
    if _RAY_DATA_EXTENSIONS_AVAILABLE and isinstance(arrow_type, tuple(_TENSOR_EXTENSION_TYPES)):
        scalar_dtype = _from_arrow_type_with_ray_data_extensions(arrow_type.scalar_type)
        shape = arrow_type.shape if isinstance(arrow_type, ArrowTensorType) else None
        return DataType.tensor(scalar_dtype, shape)
    return DataType.from_arrow_type(arrow_type)


class RayRunnerIO(runner_io.RunnerIO):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def glob_paths_details(
        self,
        source_paths: list[str],
        file_format_config: FileFormatConfig | None = None,
        io_config: IOConfig | None = None,
    ) -> FileInfos:
        # Synchronously fetch the file infos, for now.
        return FileInfos.from_table(
            ray.get(_glob_path_into_file_infos.remote(source_paths, file_format_config, io_config=io_config))
            .to_table()
            ._table
        )

    def partition_set_from_ray_dataset(
        self,
        ds: RayDataset,
    ) -> tuple[RayPartitionSet, Schema]:
        arrow_schema = ds.schema(fetch_if_missing=True)
        if not isinstance(arrow_schema, pa.Schema):
            # Convert Dataset to an Arrow dataset.
            extra_kwargs = {}
            if RAY_VERSION >= (2, 3, 0):
                # The zero_copy_batch kwarg was added in Ray 2.3.0.
                extra_kwargs["zero_copy_batch"] = True
            ds = ds.map_batches(
                lambda x: x,
                batch_size=None,
                batch_format="pyarrow",
                **extra_kwargs,
            )
            arrow_schema = ds.schema(fetch_if_missing=True)

            # Ray 2.5.0 broke the API by using its own `ray.data.dataset.Schema` instead of PyArrow schemas
            if RAY_VERSION >= (2, 5, 0):
                arrow_schema = pa.schema({name: t for name, t in zip(arrow_schema.names, arrow_schema.types)})

        daft_schema = Schema._from_field_name_and_types(
            [
                (arrow_field.name, _from_arrow_type_with_ray_data_extensions(arrow_field.type))
                for arrow_field in arrow_schema
            ]
        )
        block_refs = ds.get_internal_block_refs()

        # NOTE: This materializes the entire Ray Dataset - we could make this more intelligent by creating a new RayDatasetScan node
        # which can iterate on Ray Dataset blocks and materialize as-needed
        daft_micropartitions = [
            _make_daft_partition_from_ray_dataset_blocks.remote(block, daft_schema) for block in block_refs
        ]
        pset = RayPartitionSet()

        for i, obj in enumerate(daft_micropartitions):
            pset.set_partition(i, RayMaterializedResult(obj))
        return (
            pset,
            daft_schema,
        )

    def partition_set_from_dask_dataframe(
        self,
        ddf: dask.DataFrame,
    ) -> tuple[RayPartitionSet, Schema]:
        import dask
        from ray.util.dask import ray_dask_get

        partitions = ddf.to_delayed()
        if not partitions:
            raise ValueError("Can't convert an empty Dask DataFrame (with no partitions) to a Daft DataFrame.")
        persisted_partitions = dask.persist(*partitions, scheduler=ray_dask_get)
        parts = [_to_pandas_ref(next(iter(part.dask.values()))) for part in persisted_partitions]
        daft_micropartitions, schemas = zip(
            *(_make_daft_partition_from_dask_dataframe_partitions.remote(p) for p in parts)
        )
        schemas = ray.get(list(schemas))
        # Dask shouldn't allow inconsistent schemas across partitions, but we double-check here.
        if not all(schemas[0] == schema for schema in schemas[1:]):
            raise ValueError(
                "Can't convert a Dask DataFrame with inconsistent schemas across partitions to a Daft DataFrame:",
                schemas,
            )

        pset = RayPartitionSet()

        for i, obj in enumerate(daft_micropartitions):
            pset.set_partition(i, RayMaterializedResult(obj))
        return (
            pset,
            schemas[0],
        )


def _get_ray_task_options(resource_request: ResourceRequest) -> dict[str, Any]:
    options = {}
    # FYI: Ray's default resource behaviour is documented here:
    # https://docs.ray.io/en/latest/ray-core/tasks/resources.html
    if resource_request.num_cpus is not None:
        # Ray worker pool will thrash if a request comes in for fractional cpus,
        # so we floor the request to at least 1 cpu here.
        options["num_cpus"] = max(1, resource_request.num_cpus)
    if resource_request.num_gpus:
        options["num_gpus"] = resource_request.num_gpus
    if resource_request.memory_bytes:
        # Note that lower versions of Ray do not accept a value of 0 here,
        # so the if-clause is load-bearing.
        options["memory"] = resource_request.memory_bytes
    return options


def build_partitions(
    instruction_stack: list[Instruction], partial_metadatas: list[PartitionMetadata], *inputs: MicroPartition
) -> list[list[PartitionMetadata] | MicroPartition]:
    partitions = list(inputs)
    for instruction in instruction_stack:
        partitions = instruction.run(partitions)

    assert len(partial_metadatas) == len(partitions), f"{len(partial_metadatas)} vs {len(partitions)}"

    metadatas = [PartitionMetadata.from_table(p).merge_with_partial(m) for p, m in zip(partitions, partial_metadatas)]

    return [metadatas, *partitions]


# Give the same function different names to aid in profiling data distribution.


@ray.remote
def single_partition_pipeline(
    daft_execution_config: PyDaftExecutionConfig,
    instruction_stack: list[Instruction],
    partial_metadatas: list[PartitionMetadata],
    *inputs: MicroPartition,
) -> list[list[PartitionMetadata] | MicroPartition]:
    with execution_config_ctx(
        config=daft_execution_config,
    ):
        return build_partitions(instruction_stack, partial_metadatas, *inputs)


@ray.remote
def fanout_pipeline(
    daft_execution_config: PyDaftExecutionConfig,
    instruction_stack: list[Instruction],
    partial_metadatas: list[PartitionMetadata],
    *inputs: MicroPartition,
) -> list[list[PartitionMetadata] | MicroPartition]:
    with execution_config_ctx(config=daft_execution_config):
        return build_partitions(instruction_stack, partial_metadatas, *inputs)


@ray.remote(scheduling_strategy="SPREAD")
def reduce_pipeline(
    daft_execution_config: PyDaftExecutionConfig,
    instruction_stack: list[Instruction],
    partial_metadatas: list[PartitionMetadata],
    inputs: list,
) -> list[list[PartitionMetadata] | MicroPartition]:
    import ray

    with execution_config_ctx(config=daft_execution_config):
        return build_partitions(instruction_stack, partial_metadatas, *ray.get(inputs))


@ray.remote(scheduling_strategy="SPREAD")
def reduce_and_fanout(
    daft_execution_config: PyDaftExecutionConfig,
    instruction_stack: list[Instruction],
    partial_metadatas: list[PartitionMetadata],
    inputs: list,
) -> list[list[PartitionMetadata] | MicroPartition]:
    import ray

    with execution_config_ctx(config=daft_execution_config):
        return build_partitions(instruction_stack, partial_metadatas, *ray.get(inputs))


@ray.remote
def get_metas(*partitions: MicroPartition) -> list[PartitionMetadata]:
    return [PartitionMetadata.from_table(partition) for partition in partitions]


def _ray_num_cpus_provider(ttl_seconds: int = 1) -> Generator[int, None, None]:
    """Helper that gets the number of CPUs from Ray

    Used as a generator as it provides a guard against calling ray.cluster_resources()
    more than once per `ttl_seconds`.

    Example:
    >>> p = _ray_num_cpus_provider()
    >>> next(p)
    """
    last_checked_time = time.time()
    last_num_cpus_queried = int(ray.cluster_resources().get("CPU", 0))
    while True:
        currtime = time.time()
        if currtime - last_checked_time < ttl_seconds:
            yield last_num_cpus_queried
        else:
            last_checked_time = currtime
            last_num_cpus_queried = int(ray.cluster_resources().get("CPU", 0))
            yield last_num_cpus_queried


class Scheduler:
    def __init__(self, max_task_backlog: int | None, use_ray_tqdm: bool) -> None:
        """
        max_task_backlog: Max number of inflight tasks waiting for cores.
        """

        # As of writing, Ray does not seem to be guaranteed to support
        # more than this number of pending scheduling tasks.
        # Ray has an internal proto that reports backlogged tasks [1],
        # and each task proto can be up to 10 MiB [2],
        # and protobufs have a max size of 2GB (from errors empirically encountered).
        #
        # https://github.com/ray-project/ray/blob/8427de2776717b30086c277e5e8e140316dbd193/src/ray/protobuf/node_manager.proto#L32
        # https://github.com/ray-project/ray/blob/fb95f03f05981f232aa7a9073dd2c2512729e99a/src/ray/common/ray_config_def.h#LL513C1-L513C1
        self.max_task_backlog = max_task_backlog if max_task_backlog is not None else 180

        self.reserved_cores = 0

        self.execution_configs_objref_by_df: dict[str, ray.ObjectRef] = dict()
        self.threads_by_df: dict[str, threading.Thread] = dict()
        self.results_by_df: dict[str, Queue] = {}
        self.active_by_df: dict[str, bool] = dict()
        self.results_buffer_size_by_df: dict[str, int | None] = dict()

        self._actor_pools: dict[str, RayRoundRobinActorPool] = {}

        self.use_ray_tqdm = use_ray_tqdm

    def next(self, result_uuid: str) -> RayMaterializedResult | StopIteration:
        # Case: thread is terminated and no longer exists.
        # Should only be hit for repeated calls to next() after StopIteration.
        if result_uuid not in self.threads_by_df:
            return StopIteration()

        # Case: thread needs to be terminated
        if not self.active_by_df.get(result_uuid, False):
            return StopIteration()

        # Common case: get the next result from the thread.
        result = self.results_by_df[result_uuid].get()

        return result

    def start_plan(
        self,
        plan_scheduler: PhysicalPlanScheduler,
        psets: dict[str, ray.ObjectRef],
        result_uuid: str,
        daft_execution_config: PyDaftExecutionConfig,
        results_buffer_size: int | None = None,
    ) -> None:
        self.execution_configs_objref_by_df[result_uuid] = ray.put(daft_execution_config)
        self.results_by_df[result_uuid] = Queue(maxsize=1 if results_buffer_size is not None else -1)
        self.active_by_df[result_uuid] = True
        self.results_buffer_size_by_df[result_uuid] = results_buffer_size

        t = threading.Thread(
            target=self._run_plan,
            name=result_uuid,
            kwargs={
                "plan_scheduler": plan_scheduler,
                "psets": psets,
                "result_uuid": result_uuid,
            },
        )
        t.start()
        self.threads_by_df[result_uuid] = t

    def active_plans(self) -> list[str]:
        return [r_uuid for r_uuid, is_active in self.active_by_df.items() if is_active]

    def stop_plan(self, result_uuid: str) -> None:
        if result_uuid in self.active_by_df:
            # Mark df as non-active
            self.active_by_df[result_uuid] = False
            # wait till thread gracefully completes
            self.threads_by_df[result_uuid].join()
            # remove thread and history of df
            del self.threads_by_df[result_uuid]
            del self.active_by_df[result_uuid]
            del self.results_by_df[result_uuid]
            del self.results_buffer_size_by_df[result_uuid]

    def get_actor_pool(
        self,
        name: str,
        resource_request: ResourceRequest,
        num_actors: int,
        projection: ExpressionsProjection,
        execution_config: PyDaftExecutionConfig,
    ) -> str:
        actor_pool = RayRoundRobinActorPool(name, num_actors, resource_request, projection, execution_config)
        self._actor_pools[name] = actor_pool
        self._actor_pools[name].setup()
        return name

    def teardown_actor_pool(self, name: str) -> None:
        if name in self._actor_pools:
            self._actor_pools[name].teardown()
            del self._actor_pools[name]

    def _run_plan(
        self,
        plan_scheduler: PhysicalPlanScheduler,
        psets: dict[str, ray.ObjectRef],
        result_uuid: str,
    ) -> None:
        # Get executable tasks from plan scheduler.
        results_buffer_size = self.results_buffer_size_by_df[result_uuid]
        tasks = plan_scheduler.to_partition_tasks(
            psets,
            # Attempt to subtract 1 from results_buffer_size because the return Queue size is already 1
            # If results_buffer_size=1 though, we can't do much and the total buffer size actually has to be >= 2
            # because we have two buffers (the Queue and the buffer inside the `materialize` generator)
            None if results_buffer_size is None else max(results_buffer_size - 1, 1),
        )

        daft_execution_config = self.execution_configs_objref_by_df[result_uuid]
        inflight_tasks: dict[str, PartitionTask[ray.ObjectRef]] = dict()
        inflight_ref_to_task: dict[ray.ObjectRef, str] = dict()
        pbar = ProgressBar(use_ray_tqdm=self.use_ray_tqdm)
        num_cpus_provider = _ray_num_cpus_provider()

        start = datetime.now()
        profile_filename = (
            f"profile_RayRunner.run()_"
            f"{datetime.replace(datetime.now(), second=0, microsecond=0).isoformat()[:-3]}.json"
        )

        def is_active():
            return self.active_by_df.get(result_uuid, False)

        def place_in_queue(item):
            while is_active():
                try:
                    self.results_by_df[result_uuid].put(item, timeout=0.1)
                    break
                except Full:
                    pass

        with profiler(profile_filename):
            try:
                next_step = next(tasks)

                while is_active():  # Loop: Dispatch -> await.
                    while is_active():  # Loop: Dispatch (get tasks -> batch dispatch).
                        tasks_to_dispatch: list[PartitionTask] = []

                        # TODO: improve control loop code to be more understandable and dynamically adjust backlog
                        cores: int = max(
                            next(num_cpus_provider) - self.reserved_cores, 1
                        )  # assume at least 1 CPU core for bootstrapping clusters that scale from zero
                        max_inflight_tasks = cores + self.max_task_backlog
                        dispatches_allowed = max_inflight_tasks - len(inflight_tasks)
                        dispatches_allowed = min(cores, dispatches_allowed)

                        # Loop: Get a batch of tasks.
                        while len(tasks_to_dispatch) < dispatches_allowed and is_active():
                            if next_step is None:
                                # Blocked on already dispatched tasks; await some tasks.
                                break

                            elif isinstance(next_step, MaterializedResult):
                                # A final result.
                                place_in_queue(next_step)
                                next_step = next(tasks)

                            # next_step is a task.

                            # If it is a no-op task, just run it locally immediately.
                            elif len(next_step.instructions) == 0:
                                logger.debug("Running task synchronously in main thread: %s", next_step)
                                assert (
                                    len(next_step.partial_metadatas) == 1
                                ), "No-op tasks must have one output by definition, since there are no instructions to run"
                                [single_partial] = next_step.partial_metadatas
                                if single_partial.num_rows is None:
                                    [single_meta] = ray.get(get_metas.remote(next_step.inputs))
                                    accessor = PartitionMetadataAccessor.from_metadata_list(
                                        [single_meta.merge_with_partial(single_partial)]
                                    )
                                else:
                                    accessor = PartitionMetadataAccessor.from_metadata_list(
                                        [
                                            PartitionMetadata(
                                                num_rows=single_partial.num_rows,
                                                size_bytes=single_partial.size_bytes,
                                                boundaries=single_partial.boundaries,
                                            )
                                        ]
                                    )

                                next_step.set_result(
                                    [RayMaterializedResult(partition, accessor, 0) for partition in next_step.inputs]
                                )
                                next_step = next(tasks)

                            else:
                                # Add the task to the batch.
                                tasks_to_dispatch.append(next_step)
                                next_step = next(tasks)

                        # Dispatch the batch of tasks.
                        logger.debug(
                            "%ss: RayRunner dispatching %s tasks",
                            (datetime.now() - start).total_seconds(),
                            len(tasks_to_dispatch),
                        )

                        if not is_active():
                            break

                        for task in tasks_to_dispatch:
                            if task.actor_pool_id is None:
                                results = _build_partitions(daft_execution_config, task)
                            else:
                                actor_pool = self._actor_pools.get(task.actor_pool_id)
                                assert actor_pool is not None, "Ray actor pool must live for as long as the tasks."
                                results = _build_partitions_on_actor_pool(task, actor_pool)
                            logger.debug("%s -> %s", task, results)
                            inflight_tasks[task.id()] = task
                            for result in results:
                                inflight_ref_to_task[result] = task.id()

                            pbar.mark_task_start(task)

                        if dispatches_allowed == 0 or next_step is None:
                            break

                    # Await a batch of tasks.
                    # (Awaits the next task, and then the next batch of tasks within 10ms.)

                    dispatch = datetime.now()
                    completed_task_ids = []
                    for wait_for in ("next_one", "next_batch"):
                        if not is_active():
                            break

                        if wait_for == "next_one":
                            num_returns = 1
                            timeout = None
                        elif wait_for == "next_batch":
                            num_returns = len(inflight_ref_to_task)
                            timeout = 0.01  # 10ms

                        if num_returns == 0:
                            break

                        readies, _ = ray.wait(
                            list(inflight_ref_to_task.keys()),
                            num_returns=num_returns,
                            timeout=timeout,
                            fetch_local=False,
                        )

                        for ready in readies:
                            if ready in inflight_ref_to_task:
                                task_id = inflight_ref_to_task[ready]
                                completed_task_ids.append(task_id)
                                # Mark the entire task associated with the result as done.
                                task = inflight_tasks[task_id]
                                if isinstance(task, SingleOutputPartitionTask):
                                    del inflight_ref_to_task[ready]
                                elif isinstance(task, MultiOutputPartitionTask):
                                    for partition in task.partitions():
                                        del inflight_ref_to_task[partition]

                                pbar.mark_task_done(task)
                                del inflight_tasks[task_id]

                    logger.debug(
                        "%ss to await results from %s", (datetime.now() - dispatch).total_seconds(), completed_task_ids
                    )

                    if next_step is None:
                        next_step = next(tasks)

            except StopIteration as e:
                place_in_queue(e)

            # Ensure that all Exceptions are correctly propagated to the consumer before reraising to kill thread
            except Exception as e:
                place_in_queue(e)
                pbar.close()
                raise

        pbar.close()


SCHEDULER_ACTOR_NAME = "scheduler"
SCHEDULER_ACTOR_NAMESPACE = "daft"


@ray.remote(num_cpus=1)
class SchedulerActor(Scheduler):
    def __init__(self, *n, **kw) -> None:
        super().__init__(*n, **kw)
        self.reserved_cores = 1


def _build_partitions(
    daft_execution_config_objref: ray.ObjectRef, task: PartitionTask[ray.ObjectRef]
) -> list[ray.ObjectRef]:
    """Run a PartitionTask and return the resulting list of partitions."""
    ray_options: dict[str, Any] = {"num_returns": task.num_results + 1, "name": task.name()}

    if task.resource_request is not None:
        ray_options = {**ray_options, **_get_ray_task_options(task.resource_request)}

    if isinstance(task.instructions[0], ReduceInstruction):
        build_remote = (
            reduce_and_fanout
            if task.instructions and isinstance(task.instructions[-1], FanoutInstruction)
            else reduce_pipeline
        )
        build_remote = build_remote.options(**ray_options)
        [metadatas_ref, *partitions] = build_remote.remote(
            daft_execution_config_objref, task.instructions, task.partial_metadatas, task.inputs
        )

    else:
        build_remote = (
            fanout_pipeline
            if task.instructions and isinstance(task.instructions[-1], FanoutInstruction)
            else single_partition_pipeline
        )
        if task.instructions and isinstance(task.instructions[0], ScanWithTask):
            ray_options["scheduling_strategy"] = "SPREAD"
        build_remote = build_remote.options(**ray_options)
        [metadatas_ref, *partitions] = build_remote.remote(
            daft_execution_config_objref, task.instructions, task.partial_metadatas, *task.inputs
        )

    metadatas_accessor = PartitionMetadataAccessor(metadatas_ref)
    task.set_result(
        [
            RayMaterializedResult(
                partition=partition,
                metadatas=metadatas_accessor,
                metadata_idx=i,
            )
            for i, partition in enumerate(partitions)
        ]
    )

    return partitions


def _build_partitions_on_actor_pool(
    task: PartitionTask[ray.ObjectRef],
    actor_pool: RayRoundRobinActorPool,
) -> list[ray.ObjectRef]:
    """Run a PartitionTask on an actor pool and return the resulting list of partitions."""
    [metadatas_ref, *partitions] = actor_pool.submit(task.instructions, task.partial_metadatas, task.inputs)
    metadatas_accessor = PartitionMetadataAccessor(metadatas_ref)
    task.set_result(
        [
            RayMaterializedResult(
                partition=partition,
                metadatas=metadatas_accessor,
                metadata_idx=i,
            )
            for i, partition in enumerate(partitions)
        ]
    )
    return partitions


@ray.remote
class DaftRayActor:
    def __init__(self, daft_execution_config: PyDaftExecutionConfig, uninitialized_projection: ExpressionsProjection):
        self.daft_execution_config = daft_execution_config
        partial_stateful_udfs = {
            name: psu
            for expr in uninitialized_projection
            for name, psu in extract_partial_stateful_udf_py(expr._expr).items()
        }
        logger.info("Initializing stateful UDFs: %s", ", ".join(partial_stateful_udfs.keys()))

        self.initialized_stateful_udfs = {}
        for name, (partial_udf, init_args) in partial_stateful_udfs.items():
            if init_args is None:
                self.initialized_stateful_udfs[name] = partial_udf.func_cls()
            else:
                args, kwargs = init_args
                self.initialized_stateful_udfs[name] = partial_udf.func_cls(*args, **kwargs)

    @ray.method(num_returns=2)
    def run(
        self,
        uninitialized_projection: ExpressionsProjection,
        partial_metadatas: list[PartitionMetadata],
        *inputs: MicroPartition,
    ) -> list[list[PartitionMetadata] | MicroPartition]:
        with execution_config_ctx(config=self.daft_execution_config):
            assert len(inputs) == 1, "DaftRayActor can only process single partitions"
            assert len(partial_metadatas) == 1, "DaftRayActor can only process single partitions (and single metadata)"
            part = inputs[0]
            partial = partial_metadatas[0]

            # Bind the ExpressionsProjection to the initialized UDFs
            initialized_projection = ExpressionsProjection(
                [e._bind_stateful_udfs(self.initialized_stateful_udfs) for e in uninitialized_projection]
            )
            new_part = part.eval_expression_list(initialized_projection)

            return [
                [PartitionMetadata.from_table(new_part).merge_with_partial(partial)],
                new_part,
            ]


class RayRoundRobinActorPool:
    """Naive implementation of an ActorPool that performs round-robin task submission to the actors"""

    def __init__(
        self,
        pool_id: str,
        num_actors: int,
        resource_request: ResourceRequest,
        projection: ExpressionsProjection,
        execution_config: PyDaftExecutionConfig,
    ):
        self._actors: list[DaftRayActor] | None = None
        self._task_idx = 0

        self._execution_config = execution_config
        self._num_actors = num_actors
        self._resource_request_per_actor = resource_request
        self._id = pool_id
        self._projection = projection

    def setup(self) -> None:
        self._actors = [
            DaftRayActor.options(name=f"rank={rank}-{self._id}").remote(self._execution_config, self._projection)  # type: ignore
            for rank in range(self._num_actors)
        ]

    def teardown(self):
        assert self._actors is not None, "Must have active Ray actors on teardown"

        # Delete the actors in the old pool so Ray can tear them down
        old_actors = self._actors
        self._actors = None
        del old_actors

    def submit(
        self, instruction_stack: list[Instruction], partial_metadatas: list[ray.ObjectRef], inputs: list[ray.ObjectRef]
    ) -> list[ray.ObjectRef]:
        assert self._actors is not None, "Must have active Ray actors during submission"

        assert (
            len(instruction_stack) == 1
        ), "RayRoundRobinActorPool can only handle single StatefulUDFProject instructions"
        instruction = instruction_stack[0]
        assert isinstance(instruction, StatefulUDFProject)
        projection = instruction.projection

        # Determine which actor to schedule on in a round-robin fashion
        idx = self._task_idx % self._num_actors
        self._task_idx += 1
        actor = self._actors[idx]

        return actor.run.remote(projection, partial_metadatas, *inputs)


class RayRunner(Runner[ray.ObjectRef]):
    def __init__(
        self,
        address: str | None,
        max_task_backlog: int | None,
    ) -> None:
        super().__init__()
        if ray.is_initialized():
            if address is not None:
                logger.warning(
                    "Ray has already been initialized, Daft will reuse the existing Ray context and ignore the "
                    "supplied address: %s",
                    address,
                )
        else:
            ray.init(address=address)

        # Check if Ray is running in "client mode" (connected to a Ray cluster via a Ray client)
        self.ray_client_mode = ray.util.client.ray.get_context().is_connected()

        if self.ray_client_mode:
            # Run scheduler remotely if the cluster is connected remotely.
            self.scheduler_actor = SchedulerActor.options(  # type: ignore
                name=SCHEDULER_ACTOR_NAME,
                namespace=SCHEDULER_ACTOR_NAMESPACE,
                get_if_exists=True,
            ).remote(  # type: ignore
                max_task_backlog=max_task_backlog,
                use_ray_tqdm=True,
            )
        else:
            self.scheduler = Scheduler(
                max_task_backlog=max_task_backlog,
                use_ray_tqdm=False,
            )

    def active_plans(self) -> list[str]:
        if self.ray_client_mode:
            return ray.get(self.scheduler_actor.active_plans.remote())
        else:
            return self.scheduler.active_plans()

    def _start_plan(
        self,
        plan_scheduler: PhysicalPlanScheduler,
        daft_execution_config: PyDaftExecutionConfig,
        results_buffer_size: int | None = None,
    ) -> str:
        psets = {k: v.values() for k, v in self._part_set_cache.get_all_partition_sets().items()}
        result_uuid = str(uuid.uuid4())
        if self.ray_client_mode:
            ray.get(
                self.scheduler_actor.start_plan.remote(
                    daft_execution_config=daft_execution_config,
                    plan_scheduler=plan_scheduler,
                    psets=psets,
                    result_uuid=result_uuid,
                    results_buffer_size=results_buffer_size,
                )
            )
        else:
            self.scheduler.start_plan(
                daft_execution_config=daft_execution_config,
                plan_scheduler=plan_scheduler,
                psets=psets,
                result_uuid=result_uuid,
                results_buffer_size=results_buffer_size,
            )
        return result_uuid

    def _stream_plan(self, result_uuid: str) -> Iterator[RayMaterializedResult]:
        try:
            while True:
                if self.ray_client_mode:
                    result = ray.get(self.scheduler_actor.next.remote(result_uuid))
                else:
                    result = self.scheduler.next(result_uuid)

                if isinstance(result, StopIteration):
                    break
                elif isinstance(result, Exception):
                    raise result

                yield result
        finally:
            # Generator is out of scope, ensure that state has been cleaned up
            if self.ray_client_mode:
                ray.get(self.scheduler_actor.stop_plan.remote(result_uuid))
            else:
                self.scheduler.stop_plan(result_uuid)

    def run_iter(
        self, builder: LogicalPlanBuilder, results_buffer_size: int | None = None
    ) -> Iterator[RayMaterializedResult]:
        # Grab and freeze the current DaftExecutionConfig
        daft_execution_config = get_context().daft_execution_config

        # Optimize the logical plan.
        builder = builder.optimize()

        if daft_execution_config.enable_aqe:
            adaptive_planner = builder.to_adaptive_physical_plan_scheduler(daft_execution_config)
            while not adaptive_planner.is_done():
                source_id, plan_scheduler = adaptive_planner.next()
                # don't store partition sets in variable to avoid reference
                result_uuid = self._start_plan(
                    plan_scheduler, daft_execution_config, results_buffer_size=results_buffer_size
                )
                del plan_scheduler
                results_iter = self._stream_plan(result_uuid)
                # if source_id is None that means this is the final stage
                if source_id is None:
                    yield from results_iter
                else:
                    cache_entry = self._collect_into_cache(results_iter)
                    adaptive_planner.update(source_id, cache_entry)
                    del cache_entry
        else:
            # Finalize the logical plan and get a physical plan scheduler for translating the
            # physical plan to executable tasks.
            plan_scheduler = builder.to_physical_plan_scheduler(daft_execution_config)

            result_uuid = self._start_plan(
                plan_scheduler, daft_execution_config, results_buffer_size=results_buffer_size
            )

            yield from self._stream_plan(result_uuid)

    def run_iter_tables(
        self, builder: LogicalPlanBuilder, results_buffer_size: int | None = None
    ) -> Iterator[MicroPartition]:
        for result in self.run_iter(builder, results_buffer_size=results_buffer_size):
            yield ray.get(result.partition())

    @contextlib.contextmanager
    def actor_pool_context(
        self, name: str, resource_request: ResourceRequest, num_actors: PartID, projection: ExpressionsProjection
    ) -> Iterator[str]:
        execution_config = get_context().daft_execution_config
        if self.ray_client_mode:
            try:
                yield ray.get(
                    self.scheduler_actor.get_actor_pool.remote(name, resource_request, num_actors, projection)
                )
            finally:
                self.scheduler_actor.teardown_actor_pool.remote(name)
        else:
            try:
                yield self.scheduler.get_actor_pool(name, resource_request, num_actors, projection, execution_config)
            finally:
                self.scheduler.teardown_actor_pool(name)

    def _collect_into_cache(self, results_iter: Iterator[RayMaterializedResult]) -> PartitionCacheEntry:
        result_pset = RayPartitionSet()

        for i, result in enumerate(results_iter):
            result_pset.set_partition(i, result)

        pset_entry = self._part_set_cache.put_partition_set(result_pset)

        return pset_entry

    def run(self, builder: LogicalPlanBuilder) -> PartitionCacheEntry:
        results_iter = self.run_iter(builder)
        return self._collect_into_cache(results_iter)

    def put_partition_set_into_cache(self, pset: PartitionSet) -> PartitionCacheEntry:
        if isinstance(pset, LocalPartitionSet):
            new_pset = RayPartitionSet()
            metadata_accessor = PartitionMetadataAccessor.from_metadata_list([v.metadata() for v in pset.values()])
            for i, (pid, py_mat_result) in enumerate(pset.items()):
                new_pset.set_partition(
                    pid, RayMaterializedResult(ray.put(py_mat_result.partition()), metadata_accessor, i)
                )
            pset = new_pset
        return self._part_set_cache.put_partition_set(pset=pset)

    def runner_io(self) -> RayRunnerIO:
        return RayRunnerIO()


class RayMaterializedResult(MaterializedResult[ray.ObjectRef]):
    def __init__(
        self,
        partition: ray.ObjectRef,
        metadatas: PartitionMetadataAccessor | None = None,
        metadata_idx: int | None = None,
    ):
        self._partition = partition
        if metadatas is None:
            assert metadata_idx is None
            metadatas = PartitionMetadataAccessor(get_metas.remote(self._partition))
            metadata_idx = 0
        self._metadatas = metadatas
        self._metadata_idx = metadata_idx

    def partition(self) -> ray.ObjectRef:
        return self._partition

    def micropartition(self) -> MicroPartition:
        return ray.get(self._partition)

    def metadata(self) -> PartitionMetadata:
        return self._metadatas.get_index(self._metadata_idx)

    def cancel(self) -> None:
        return ray.cancel(self._partition)

    def _noop(self, _: ray.ObjectRef) -> None:
        return None


class PartitionMetadataAccessor:
    """Wrapper class around Remote[List[PartitionMetadata]] to memoize lookups."""

    def __init__(self, ref: ray.ObjectRef) -> None:
        self._ref: ray.ObjectRef = ref
        self._metadatas: None | list[PartitionMetadata] = None

    def _get_metadatas(self) -> list[PartitionMetadata]:
        if self._metadatas is None:
            self._metadatas = ray.get(self._ref)
        return self._metadatas

    def get_index(self, key) -> PartitionMetadata:
        return self._get_metadatas()[key]

    @classmethod
    def from_metadata_list(cls, meta: list[PartitionMetadata]) -> PartitionMetadataAccessor:
        ref = ray.put(meta)
        accessor = cls(ref)
        accessor._metadatas = meta
        return accessor


###
# ShuffleService
###


from daft.execution.physical_plan_shuffles import (
    HashPartitionRequest,
    PartitioningSpec,
    PartitionRequest,
    ShuffleServiceInterface,
)

ray.ObjectRef = ray.ObjectRef


@ray.remote
class ShuffleServiceActor:
    """An actor that is part of the ShuffleService. This is meant to be spun up one-per-node"""

    def __init__(self, partition_spec: PartitioningSpec):
        assert partition_spec.type_ == "hash", "Only hash partitioning is currently supported"
        self._output_partitioning = partition_spec
        hash_spec = self._output_partitioning.to_hash_pspec()

        # TODO: These can be made much more sophisticated, performing things such as disk/remote spilling
        # as necessary.
        self._unpartitioned_data_buffer: list[MicroPartition] = []
        self._partitioned_data_buffer: dict[PartitionRequest, list[MicroPartition]] = {
            HashPartitionRequest(type_="hash", bucket=i): [] for i in range(hash_spec.num_partitions)
        }

    @ray.method(num_returns=2)
    def read(self, request: PartitionRequest, chunk_size_bytes: int) -> tuple[MicroPartition | None, int]:
        """Retrieves ShuffleData from the shuffle service for the specified partition"""
        if len(self._unpartitioned_data_buffer) > 0:
            self._run_partitioning()

        assert request in self._partitioned_data_buffer, f"PartitionRequest must be in the buffer: {request}"

        buffer = self._partitioned_data_buffer[request]
        if buffer:
            result = buffer.pop(0)

            # Perform merging of result with the next results in the buffer
            result_size: int = result.size_bytes()  # type: ignore
            assert (
                result_size is not None
            ), "We really need the size here, but looks like lazy MPs without stats don't give us the size"

            while result_size < chunk_size_bytes and buffer:
                next_partition = buffer[0]
                next_partition_size = next_partition.size_bytes()

                assert (
                    next_partition_size is not None
                ), "We really need the size here, but looks like lazy MPs without stats don't give us the size"

                if result_size + next_partition_size <= chunk_size_bytes:
                    result = MicroPartition.concat([result, buffer.pop(0)])
                    result_size = result.size_bytes()  # type: ignore
                else:
                    break

            # Split the result if it is too large (best-effort)
            if result_size > chunk_size_bytes:
                rows_per_byte = len(result) / result_size
                target_rows = int(chunk_size_bytes * rows_per_byte)
                result, remaining = result.slice(0, target_rows), result.slice(target_rows, len(result))
                buffer.insert(0, remaining)

            return (result, result.size_bytes())  # type: ignore
        else:
            return (None, 0)

    def ingest(self, data: list[MicroPartition]) -> None:
        """Ingest data into the shuffle service"""
        self._unpartitioned_data_buffer.extend(data)
        return None

    def _run_partitioning(self) -> None:
        from daft import col

        assert self._output_partitioning.type_ == "hash", "Only hash partitioning is currently supported"
        hash_spec = self._output_partitioning.to_hash_pspec()

        # Drain `self._unpartitioned_data_buffer`, partitioning the data and placing it into `self._partitioned_data_buffer`
        while self._unpartitioned_data_buffer:
            micropartition = self._unpartitioned_data_buffer.pop(0)
            partitions = micropartition.partition_by_hash(
                ExpressionsProjection([col(c) for c in hash_spec.columns]), hash_spec.num_partitions
            )
            for partition_request, partition in zip(self._partitioned_data_buffer.keys(), partitions):
                self._partitioned_data_buffer[partition_request].append(partition)


class RayPerNodeActorFullyMaterializingShuffleService(ShuffleServiceInterface[ray.ObjectRef, ray.ObjectRef]):
    """A ShuffleService implementation in Ray that utilizes Ray Actors on each node to perform a shuffle

    This is nice because it lets us `.ingest` data into each node's Actor before actually performing the shuffle,
    reducing the complexity of the operation to O(num_nodes^2) instead of O(input_partitions * output_partitions)
    """

    # Default amount of bytes to request from Ray for each ShuffleActor
    DEFAULT_SHUFFLE_ACTOR_MEMORY_REQUEST_BYTES: int = 1024 * 1024 * 1024

    def __init__(
        self,
        output_partition_spec: PartitioningSpec,
        per_actor_memory_request_bytes: int = DEFAULT_SHUFFLE_ACTOR_MEMORY_REQUEST_BYTES,
    ):
        self._input_stage_completed = False
        self._output_partitioning_spec = output_partition_spec

        # Mapping of {node_id (str): Actor}, create one Actor per node
        self._actors: dict[str, ShuffleServiceActor] = {}
        self._placement_groups: dict[str, ray.util.placement_group.PlacementGroup] = {}
        nodes = ray.nodes()
        for node in nodes:
            node_id = node["NodeID"]
            pg = ray.util.placement_group(
                [
                    {"CPU": 0.01, "memory": per_actor_memory_request_bytes}
                ],  # Use minimal CPU and 100MB memory to avoid interfering with other tasks
                strategy="STRICT_SPREAD",
            )
            ray.get(pg.ready())
            self._placement_groups[node_id] = pg
        for node_id, pg in self._placement_groups.items():
            actor = ShuffleServiceActor.options(  # type: ignore
                num_cpus=0.01, placement_group=pg, placement_group_bundle_index=0
            ).remote(self._output_partitioning_spec)
            self._actors[node_id] = actor

    def teardown(self) -> None:
        for actor in self._actors.values():
            ray.kill(actor)
        for pg in self._placement_groups.values():
            ray.util.remove_placement_group(pg)
        self._actors.clear()
        self._placement_groups.clear()

    def ingest(self, data: Iterator[ray.ObjectRef]) -> list[ray.ObjectRef[None]]:
        """Receive some data

        NOTE: This will throw an error if called after `.close_ingest` has been called.
        """
        if self._input_stage_completed:
            raise RuntimeError("Cannot ingest data after input stage is completed.")

        data_list = list(data)
        best_effort_object_locations = ray.experimental.get_object_locations(data_list)

        # Get the corresponding actor for this node
        ingestion_results = []
        for objref in data_list:
            best_effort_node_id = best_effort_object_locations.get(objref)
            if best_effort_node_id is None:
                raise NotImplementedError(
                    "Need to implement fallback logic when location information unavailable in Ray for a partition. We can probably just do a separate round-robin assignment of shuffle Actors. This is expected to happen when the partition is small I think (< 100KB) since then it wouldn't be located in the plasma store, and won't have location info."
                )
            if best_effort_node_id not in self._actors:
                raise RuntimeError(f"No ShuffleServiceActor found for node {best_effort_node_id}")
            actor = self._actors[best_effort_node_id]
            ingestion_results.append(actor.ingest.remote(data_list))  # type: ignore

        return ingestion_results

    def set_input_stage_completed(self) -> None:
        """Inform the ShuffleService that all data from the previous stage has been ingested"""
        self._input_stage_completed = True

    def is_input_stage_completed(self) -> bool:
        """Query whether or not the previous stage has completed ingestion"""
        return self._input_stage_completed

    def read(self, request: PartitionRequest, chunk_size_bytes: int) -> Iterator[ray.ObjectRef]:
        """Retrieves ShuffleData from the shuffle service for the specified partition.

        NOTE: This will throw an error if called before `set_output_partitioning` is called.
        """
        # TODO: We currently enforce full materialization of the previous stage's outputs before allowing
        # The subsequent stage to start reading data.
        #
        # This is also called a "pipeline breaker".
        if not self.is_input_stage_completed():
            return None

        # Validate that the PartitionRequest matches the provided spec
        if self._output_partitioning_spec.type_ != request.type_:
            raise ValueError(
                f"PartitionRequest type '{request.type_}' does not match the partitioning spec type '{self._output_partitioning_spec.type_}'"
            )

        # Validate the incoming PartitionRequest
        hash_spec = self._output_partitioning_spec.to_hash_pspec()
        hash_request = request.to_hash_request()
        if hash_request.bucket >= hash_spec.num_partitions:
            raise ValueError(
                f"Requested bucket {hash_request.bucket} is out of range for {hash_spec.num_partitions} partitions"
            )

        # Iterate through all actors and yield ray.ObjectRef
        for actor in self._actors.values():
            while True:
                # Request data from the actor
                (micropartition_ref, actual_bytes_read) = actor.read.remote(request, chunk_size_bytes)

                # TODO: Pretty sure this is really slow. Not sure what a good workaround is though since
                # There is't a great mechanism to query the actors quickly regarding the remaining state
                #
                # Because we know that `self.is_input_stage_completed() == True`,
                # we know that there won't be any more data coming in. So we can YOLO and stop
                # the iteration.
                actual_bytes_read = ray.get(actual_bytes_read)
                if actual_bytes_read == 0:
                    break

                yield micropartition_ref

        # After all actors are exhausted, we're done
        return


class RayShuffleServiceFactory:
    @contextlib.contextmanager
    def fully_materializing_shuffle_service_context(
        self,
        num_partitions: int,
        columns: list[str],
    ) -> Iterator[RayPerNodeActorFullyMaterializingShuffleService]:
        from daft.execution.physical_plan_shuffles import HashPartitioningSpec

        shuffle_service = RayPerNodeActorFullyMaterializingShuffleService(
            HashPartitioningSpec(type_="hash", num_partitions=num_partitions, columns=columns)
        )
        yield shuffle_service
        shuffle_service.teardown()

    @contextlib.contextmanager
    def push_based_shuffle_service_context(
        self,
        num_partitions: int,
        partition_by: ExpressionsProjection,
    ) -> Iterator[RayPushBasedShuffle]:
        num_cpus = int(ray.cluster_resources()["CPU"])

        # Number of mappers is ~2x number of mergers
        num_map_tasks = num_cpus // 3
        num_merge_tasks = num_map_tasks * 2

        yield RayPushBasedShuffle(num_map_tasks, num_merge_tasks, num_partitions, partition_by)


@ray.remote
def map_fn(
    map_input: MicroPartition, num_mergers: int, partition_by: ExpressionsProjection, num_partitions: int
) -> list[list[MicroPartition]]:
    """Returns `N` number of inputs, where `N` is the number of mergers"""
    # Partition the input data based on the partitioning spec
    partitioned_data = map_input.partition_by_hash(partition_by, num_partitions)

    outputs: list[list[MicroPartition]] = [[] for _ in range(num_mergers)]

    # Distribute the partitioned data across the mergers
    for partition_idx, partition in enumerate(partitioned_data):
        merger_idx = partition_idx // (num_partitions // num_mergers)
        if merger_idx >= num_mergers:
            merger_idx = num_mergers - 1
        outputs[merger_idx].append(partition)

    return outputs


@ray.remote
def merge_fn(*merger_inputs_across_mappers: list[MicroPartition]) -> list[MicroPartition]:
    """Returns `P` number of inputs, where `P` is the number of reducers assigned to this merger"""
    num_partitions_for_this_merger = len(merger_inputs_across_mappers[0])
    merged_partitions = []
    for partition_idx in range(num_partitions_for_this_merger):
        partitions_to_merge = [data[partition_idx] for data in merger_inputs_across_mappers]
        merged_partition = MicroPartition.concat(partitions_to_merge)
        merged_partitions.append(merged_partition)
    return merged_partitions


@ray.remote
def reduce_fn(*reduce_inputs_across_rounds: MicroPartition) -> MicroPartition:
    """Returns 1 output, which is the reduced data across rounds"""
    # Concatenate all input MicroPartitions across rounds
    reduced_partition = MicroPartition.concat(list(reduce_inputs_across_rounds))

    # Return the result as a list containing a single MicroPartition
    return reduced_partition


class RayPushBasedShuffle:
    def __init__(self, num_mappers: int, num_mergers: int, num_reducers: int, partition_by: ExpressionsProjection):
        self._num_mappers = num_mappers
        self._num_mergers = num_mergers
        self._num_reducers = num_reducers
        self._partition_by = partition_by

    def _num_reducers_for_merger(self, merger_idx: int) -> int:
        base_num = self._num_reducers // self._num_mergers
        if merger_idx < (self._num_reducers % self._num_mergers):
            return base_num + 1
        return base_num

    def _get_reducer_inputs_location(self, reducer_idx: int) -> tuple[int, int]:
        """Returns the (merger_idx, offset) of where the inputs to a given reducer should live"""
        for merger_idx in range(self._num_mergers):
            num_reducers = self._num_reducers_for_merger(merger_idx)
            if num_reducers > reducer_idx:
                return merger_idx, reducer_idx
            else:
                reducer_idx - num_reducers
        raise ValueError(f"Cannot find merger for reducer_idx: {reducer_idx}")

    def _merger_options(self, merger_idx: int) -> dict[str, Any]:
        # TODO: populate the nth merger's options. Place the nth merge task on the (n % NUM_NODES)th node
        #
        # node_strategies = {
        #     node_id: {
        #         "scheduling_strategy": NodeAffinitySchedulingStrategy(
        #             node_id, soft=True
        #         )
        #     }
        #     for node_id in set(merge_task_placement)
        # }
        # self._merge_task_options = [
        #     node_strategies[node_id] for node_id in merge_task_placement
        # ]
        return {}

    def _reduce_options(self, reducer_idx: int) -> dict[str, Any]:
        # TODO: populate the nth merger's options. Place the nth merge task on the (n % NUM_NODES)th node
        #
        # node_strategies = {
        #     node_id: {
        #         "scheduling_strategy": NodeAffinitySchedulingStrategy(
        #             node_id, soft=True
        #         )
        #     }
        #     for node_id in set(merge_task_placement)
        # }
        # self._merge_task_options = [
        #     node_strategies[node_id] for node_id in merge_task_placement
        # ]
        return {}

    def run(self, materialized_inputs: list[ray.ObjectRef]) -> list[ray.ObjectRef]:
        """Runs the Mappers and Mergers in a 2-stage pipeline until all mergers are materialized

        There are `R` reducers.
        There are `N` mergers. Each merger is "responsible" for `R / N` reducers.
        Each Mapper then should run partitioning on the data into `N` chunks.
        """
        # [N_ROUNDS, N_MERGERS, N_REDUCERS_PER_MERGER] list of outputs
        merge_results: list[list[list[ray.ObjectRef]]] = []
        map_results_buffer: list[ray.ObjectRef] = []

        # Keep running the pipeline while there is still work to do
        while materialized_inputs or map_results_buffer:
            # Drain the map_results_buffer, running merge tasks
            per_round_merge_results = []
            while map_results_buffer:
                map_results = map_results_buffer.pop()
                assert len(map_results) == self._num_mergers
                for merger_idx, merger_input in enumerate(map_results):
                    merge_results = merge_fn.options(
                        **self._merger_options(merger_idx), num_returns=self._num_reducers_for_merger(merger_idx)
                    ).remote(*merger_input)
                    per_round_merge_results.append(merge_results)
            if per_round_merge_results:
                merge_results.append(per_round_merge_results)

            # Run map tasks:
            for i in range(self._num_mappers):
                if len(materialized_inputs) == 0:
                    break
                else:
                    map_input = materialized_inputs.pop(0)
                    map_results = map_fn.options(num_returns=self._num_mergers).remote(
                        map_input, self._num_mergers, self._partition_by, self._num_reducers
                    )
                    map_results_buffer.append(map_results)

            # Wait for all tasks in this wave to complete
            ray.wait(per_round_merge_results)
            ray.wait(map_results_buffer)

        # INVARIANT: At this point, the map/merge step is done
        # Start running all the reduce functions
        # TODO: we could stagger this by num CPUs as well, but here we just YOLO run all
        reduce_results = []
        for reducer_idx in range(self._num_reducers):
            assigned_merger_idx, offset = self._get_reducer_inputs_location(reducer_idx)
            reducer_inputs = [merge_results[round][assigned_merger_idx][offset] for round in range(len(merge_results))]
            res = reduce_fn.options(**self._reduce_options(reducer_idx)).remote(*reducer_inputs)
            reduce_results.append(res)
        return reduce_results
