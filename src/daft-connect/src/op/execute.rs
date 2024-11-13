use std::{
    collections::{HashMap, HashSet},
    future::ready,
    path::PathBuf,
    pin::Pin,
};

use arrow2::io::ipc::write::StreamWriter;
use common_daft_config::DaftExecutionConfig;
use common_file_formats::FileFormat;
use daft_scan::builder::{parquet_scan, ParquetScanBuilder};
use daft_table::Table;
use eyre::Context;
use futures::{stream, Stream, StreamExt, TryStreamExt};
use spark_connect::{
    execute_plan_response::{ArrowBatch, ResponseType, ResultComplete},
    spark_connect_service_server::SparkConnectService,
    write_operation::{SaveMode, SaveType},
    ExecutePlanResponse, Relation, WriteOperation,
};
use tonic::Status;
use tracing::{error, warn};
use uuid::Uuid;

use crate::{
    invalid_argument_err, not_found_err, translation::relation_to_stream, DaftSparkConnectService,
    Session,
};

pub type ExecuteStream = <DaftSparkConnectService as SparkConnectService>::ExecutePlanStream;
pub type ExecuteRichStream =
    Pin<Box<dyn Stream<Item = eyre::Result<ExecutePlanResponse>> + Send + Sync>>;

pub struct PlanIds {
    session: String,
    server_side_session: String,
    operation: String,
}

impl PlanIds {
    pub fn new(
        client_side_session_id: impl Into<String>,
        server_side_session_id: impl Into<String>,
    ) -> Self {
        let client_side_session_id = client_side_session_id.into();
        let server_side_session_id = server_side_session_id.into();
        Self {
            session: client_side_session_id,
            server_side_session: server_side_session_id,
            operation: Uuid::new_v4().to_string(),
        }
    }

    pub fn finished(&self) -> ExecutePlanResponse {
        ExecutePlanResponse {
            session_id: self.session.to_string(),
            server_side_session_id: self.server_side_session.to_string(),
            operation_id: self.operation.to_string(),
            response_id: Uuid::new_v4().to_string(),
            metrics: None,
            observed_metrics: vec![],
            schema: None,
            response_type: Some(ResponseType::ResultComplete(ResultComplete {})),
        }
    }

    pub fn gen_response(&self, table: &Table) -> eyre::Result<ExecutePlanResponse> {
        let mut data = Vec::new();

        let mut writer = StreamWriter::new(
            &mut data,
            arrow2::io::ipc::write::WriteOptions { compression: None },
        );

        let row_count = table.num_rows();

        let schema = table
            .schema
            .to_arrow()
            .wrap_err("Failed to convert Daft schema to Arrow schema")?;

        writer
            .start(&schema, None)
            .wrap_err("Failed to start Arrow stream writer with schema")?;

        let arrays = table.get_inner_arrow_arrays().collect();
        let chunk = arrow2::chunk::Chunk::new(arrays);

        writer
            .write(&chunk, None)
            .wrap_err("Failed to write Arrow chunk to stream writer")?;

        let response = ExecutePlanResponse {
            session_id: self.session.to_string(),
            server_side_session_id: self.server_side_session.to_string(),
            operation_id: self.operation.to_string(),
            response_id: Uuid::new_v4().to_string(), // todo: implement this
            metrics: None,                           // todo: implement this
            observed_metrics: vec![],
            schema: None,
            response_type: Some(ResponseType::ArrowBatch(ArrowBatch {
                row_count: row_count as i64,
                data,
                start_offset: None,
            })),
        };

        Ok(response)
    }
}

impl Session {
    pub async fn handle_root_command(
        &self,
        command: Relation,
        operation_id: String,
    ) -> Result<ExecuteStream, Status> {
        use futures::{StreamExt, TryStreamExt};

        let context = PlanIds {
            session: self.client_side_session_id().to_string(),
            server_side_session: self.server_side_session_id().to_string(),
            operation: operation_id.clone(),
        };

        let finished = ExecutePlanResponse {
            session_id: self.client_side_session_id().to_string(),
            server_side_session_id: self.server_side_session_id().to_string(),
            operation_id,
            response_id: Uuid::new_v4().to_string(),
            metrics: None,
            observed_metrics: vec![],
            schema: None,
            response_type: Some(ResponseType::ResultComplete(ResultComplete {})),
        };

        let stream = relation_to_stream(command, context)
            .map_err(|e| tonic::Status::internal(e.to_string()))?
            .map_err(|e| tonic::Status::internal(e.to_string()))
            .chain(stream::once(ready(Ok(finished))));

        Ok(Box::pin(stream))
    }

    pub async fn handle_write_operation(
        &self,
        command: WriteOperation,
    ) -> Result<ExecuteStream, Status> {
        println!("handling write operation {command:#?}");
        let WriteOperation {
            input,
            source,
            mode,
            sort_column_names,
            partitioning_columns,
            bucket_by,
            options,
            clustering_columns,
            save_type,
        } = command;

        let Some(input) = input else {
            return invalid_argument_err!("input is required");
        };

        let Some(source) = source else {
            return invalid_argument_err!("source is required");
        };

        let Ok(mode) = SaveMode::try_from(mode) else {
            return invalid_argument_err!("mode is invalid");
        };

        if !sort_column_names.is_empty() {
            warn!(
                "sort_column_names is not yet implemented; got {:?}",
                sort_column_names
            );
        }

        if !partitioning_columns.is_empty() {
            warn!(
                "partitioning_columns is not yet implemented; got {:?}",
                partitioning_columns
            );
        }

        if let Some(bucket_by) = bucket_by {
            warn!("bucket_by is not yet implemented; got {:?}", bucket_by);
        }

        if !options.is_empty() {
            warn!("options is not yet implemented; got {:?}", options);
        }

        if !clustering_columns.is_empty() {
            warn!(
                "clustering_columns is not yet implemented; got {:?}",
                clustering_columns
            );
        }

        let Some(save_type) = save_type else {
            return invalid_argument_err!("save_type is required");
        };

        let plan_ids = PlanIds::new(self.client_side_session_id(), self.server_side_session_id());

        match save_type {
            SaveType::Path(path) => {
                self.write_to_path(plan_ids, path, input, source, mode)
                    .await
            }
            SaveType::Table(..) => {
                invalid_argument_err!("Table is not yet implemented. use path instead")
            }
        }
    }

    async fn write_to_path(
        &self,
        plan_ids: PlanIds,
        path: String,
        input: Relation,
        source: String,
        mode: SaveMode,
    ) -> Result<ExecuteStream, Status> {
        if source != "parquet" {
            return not_found_err!("{source} is not yet implemented; use parquet instead");
        }

        match mode {
            SaveMode::Unspecified => {}
            SaveMode::Append => {}
            SaveMode::Overwrite => {}
            SaveMode::ErrorIfExists => {}
            SaveMode::Ignore => {}
        }

        let builder = match crate::translation::to_logical_plan(input) {
            Ok(plan) => plan,
            Err(e) => {
                error!("Failed to build logical plan: {e:?}");
                return invalid_argument_err!("Failed to build logical plan: {e:?}");
            }
        };

        println!("writing to path: {path}");
        let builder = builder
            .table_write(&path, FileFormat::Parquet, None, None, None)
            .unwrap();

        let logical_plan = builder.build();

        let physical_plan = std::thread::scope(|s| {
            s.spawn(|| daft_local_plan::translate(&logical_plan).unwrap())
                .join()
        })
        .unwrap();

        println!("physical plan: {physical_plan:#?}");

        let psets = HashMap::new();
        let cfg = DaftExecutionConfig::default();
        let results =
            daft_local_execution::run_local(&physical_plan, psets, cfg.into(), None).unwrap();

        // todo: remove
        std::thread::scope(|s| {
            s.spawn(|| {
                for result in results {
                    println!("result: {result:?}");
                }
            });
        });

        let res = futures::stream::once(async move { Ok(plan_ids.finished()) });
        Ok(Box::pin(res))
    }
}
