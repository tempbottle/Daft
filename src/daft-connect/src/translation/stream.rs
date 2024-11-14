//! Relation handling for Spark Connect protocol.
//!
//! A Relation represents a structured dataset or transformation in Spark Connect.
//! It can be either a base relation (direct data source) or derived relation
//! (result of operations on other relations).
//!
//! The protocol represents relations as trees of operations where:
//! - Each node is a Relation with metadata and an operation type
//! - Operations can reference other relations, forming a DAG
//! - The tree describes how to derive the final result
//!
//! Example flow for: SELECT age, COUNT(*) FROM employees WHERE dept='Eng' GROUP BY age
//!
//! ```text
//! Aggregate (grouping by age)
//!   ↳ Filter (department = 'Engineering')
//!       ↳ Read (employees table)
//! ```
//!
//! Relations abstract away:
//! - Physical storage details
//! - Distributed computation
//! - Query optimization
//! - Data source specifics
//!
//! This allows Spark to optimize and execute queries efficiently across a cluster
//! while providing a consistent API regardless of the underlying data source.
//! ```mermaid
//!
//! ```

use std::collections::HashMap;

use common_daft_config::DaftExecutionConfig;
use common_error::DaftError;
use eyre::{eyre, Context};
use futures::{Stream, StreamExt, TryStreamExt};
use spark_connect::{relation::RelType, ExecutePlanResponse, Relation};
use tonic::codegen::tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};
use tracing::trace;

mod range;
use range::range;

use crate::{
    op::execute::{ExecuteRichStream, ExecuteStream, PlanIds},
    translation,
    translation::to_logical_plan,
};

pub fn relation_to_stream(relation: Relation, context: PlanIds) -> eyre::Result<ExecuteRichStream> {
    // First check common fields if needed
    if let Some(common) = &relation.common {
        // contains metadata shared across all relation types
        // Log or handle common fields if necessary
        trace!("Processing relation with plan_id: {:?}", common.plan_id);
    }

    let rel_type = relation.rel_type.ok_or_else(|| eyre!("rel_type is None"))?;

    match rel_type {
        RelType::Range(input) => {
            let stream = range(input, &context).wrap_err("parsing Range")?;
            Ok(Box::pin(stream))
        }
        RelType::Read(read) => {
            let builder = translation::logical_plan::read(read)?;
            let logical_plan = builder.logical_plan.build();

            let (tx, rx) =
                tokio::sync::mpsc::unbounded_channel::<eyre::Result<ExecutePlanResponse>>();

            std::thread::spawn(move || {
                let physical_plan = match daft_local_plan::translate(&logical_plan) {
                    Ok(plan) => plan,
                    Err(e) => {
                        tx.send(Err(eyre!(e))).unwrap();
                        return;
                    }
                };

                let cfg = DaftExecutionConfig::default();
                let result = daft_local_execution::run_local(
                    &physical_plan,
                    builder.partition,
                    cfg.into(),
                    None,
                )
                .unwrap();

                for result in result {
                    let result = match result {
                        Ok(result) => result,
                        Err(e) => {
                            tx.send(Err(eyre!(e))).unwrap();
                            continue;
                        }
                    };

                    let tables = match result.get_tables() {
                        Ok(tables) => tables,
                        Err(e) => {
                            tx.send(Err(eyre!(e))).unwrap();
                            continue;
                        }
                    };

                    for table in tables.as_slice() {
                        let response = context.gen_response(table);

                        let response = match response {
                            Ok(response) => response,
                            Err(e) => {
                                tx.send(Err(eyre!(e))).unwrap();
                                continue;
                            }
                        };

                        tx.send(Ok(response)).unwrap();
                    }
                }
            });

            let recv_stream = UnboundedReceiverStream::new(rx);

            Ok(Box::pin(recv_stream))
        }
        other => Err(eyre!("Unsupported top-level relation: {other:?}")),
    }
}
