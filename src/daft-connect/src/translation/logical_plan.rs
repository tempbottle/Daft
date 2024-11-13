use daft_logical_plan::LogicalPlanBuilder;
use daft_scan::builder::{parquet_scan, ParquetScanBuilder};
use eyre::{bail, ensure, Context};
use spark_connect::{
    read,
    read::{DataSource, ReadType},
    relation::RelType,
    Range, Read, Relation,
};
use tracing::warn;

pub fn to_logical_plan(relation: Relation) -> eyre::Result<LogicalPlanBuilder> {
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

fn range(range: Range) -> eyre::Result<LogicalPlanBuilder> {
    let Range {
        start,
        end,
        step,
        num_partitions,
    } = range;

    if let Some(num_partitions) = num_partitions {
        warn!("Ignoring num_partitions for range: {num_partitions:?}; not yet implemented");
    }

    bail!("Not yet implemented");
}

pub fn read(read: Read) -> eyre::Result<LogicalPlanBuilder> {
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

fn read_data_source(data_source: read::DataSource) -> eyre::Result<LogicalPlanBuilder> {
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

    Ok(plan)
}
