use std::{collections::HashMap, sync::Arc};

use daft_core::prelude::Series;
use daft_logical_plan::LogicalPlanBuilder;
use daft_micropartition::MicroPartition;
use daft_scan::builder::{parquet_scan, ParquetScanBuilder};
use daft_schema::schema::Schema;
use daft_table::Table;
use eyre::{bail, ensure, Context};
use spark_connect::{
    read,
    read::{DataSource, ReadType},
    relation::RelType,
    Range, Read, Relation,
};
use tracing::warn;

#[derive(Debug)]
pub struct Builder {
    pub logical_plan: LogicalPlanBuilder,
    pub partition: HashMap<String, Vec<Arc<MicroPartition>>>,
}

impl Builder {
    pub fn plain(logical_plan: LogicalPlanBuilder) -> Self {
        Self {
            logical_plan,
            partition: HashMap::new(),
        }
    }

    pub fn new(
        logical_plan: LogicalPlanBuilder,
        partition: HashMap<String, Vec<Arc<MicroPartition>>>,
    ) -> Self {
        Self {
            logical_plan,
            partition,
        }
    }
}

pub fn to_logical_plan(relation: Relation) -> eyre::Result<Builder> {
    if let Some(common) = relation.common {
        warn!("Ignoring common metadata for relation: {common:?}; not yet implemented");
    };

    let Some(rel_type) = relation.rel_type else {
        bail!("Relation type is required");
    };

    match rel_type {
        RelType::Range(r) => range(r).wrap_err("Failed to apply range to logical plan"),
        RelType::Read(r) => read(r).wrap_err("Failed to apply read to logical plan"),
        plan => bail!("Unsupported relation type: {plan:?}"),
    }
}

fn range(range: Range) -> eyre::Result<Builder> {
    let Range {
        start,
        end,
        step,
        num_partitions,
    } = range;

    if let Some(num_partitions) = num_partitions {
        warn!("Ignoring num_partitions for range: {num_partitions:?}; not yet implemented");
    }

    let start = start.unwrap_or(0);
    ensure!(num_partitions.is_none(), "num_partitions is not supported");

    let step = usize::try_from(step).wrap_err("step must be a positive integer")?;
    ensure!(step > 0, "step must be greater than 0");

    let arrow_array: arrow2::array::Int64Array = (start..end).step_by(step).map(Some).collect();
    let len = arrow_array.len();

    let singleton_series = Series::try_from((
        "range",
        Box::new(arrow_array) as Box<dyn arrow2::array::Array>,
    ))
    .wrap_err("creating singleton series")?;

    let schema = Schema::new(vec![singleton_series.field().clone()])?;

    let singleton_table = Table::new_with_size(schema.clone(), vec![singleton_series], len)?;

    let schema = Arc::new(schema);

    let micropartition =
        MicroPartition::new_loaded(schema.clone(), vec![singleton_table].into(), None);

    let uuid = uuid::Uuid::new_v4();
    let partition: HashMap<_, _> = [(uuid.to_string(), vec![Arc::new(micropartition)])].into();

    let logical_plan =
        LogicalPlanBuilder::in_memory_scan_not_python(&uuid.to_string(), schema, 1, 20, 1);

    Ok(Builder::new(logical_plan, partition))
}

pub fn read(read: Read) -> eyre::Result<Builder> {
    let Read {
        is_streaming,
        read_type,
    } = read;

    warn!("Ignoring is_streaming for read: {is_streaming}; not yet implemented");

    let Some(read_type) = read_type else {
        bail!("Read type is required");
    };

    match read_type {
        ReadType::NamedTable(table) => {
            bail!("Table read not yet implemented");
        }
        ReadType::DataSource(data_source) => {
            let res = read_data_source(data_source)
                .wrap_err("Failed to apply data source to logical plan");

            warn!("Read data source: {:?}", res);
            res
        }
    }
}

fn read_data_source(data_source: read::DataSource) -> eyre::Result<Builder> {
    println!("{:?}", data_source);
    let read::DataSource {
        format,
        schema,
        options,
        paths,
        predicates,
    } = data_source;

    let Some(format) = format else {
        bail!("Format is required");
    };

    if let Some(schema) = schema {
        warn!("Ignoring schema for data source: {schema:?}; not yet implemented");
    }

    if !options.is_empty() {
        warn!("Ignoring options for data source: {options:?}; not yet implemented");
    }

    if !predicates.is_empty() {
        warn!("Ignoring predicates for data source: {predicates:?}; not yet implemented");
    }

    ensure!(!paths.is_empty(), "Paths are required");

    let plan = std::thread::scope(move |s| {
        s.spawn(move || {
            parquet_scan(paths)
                .finish()
                .wrap_err("Failed to create parquet scan")
        })
        .join()
    })
    .unwrap()?;

    let builder = Builder::plain(plan);
    Ok(builder)
}
