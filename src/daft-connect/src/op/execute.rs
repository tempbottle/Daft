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

mod root;
mod write;

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

impl Session {}
