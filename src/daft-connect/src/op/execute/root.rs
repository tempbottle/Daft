use std::future::ready;

use futures::stream;
use spark_connect::Relation;
use tonic::Status;

use crate::{
    op::execute::{ExecuteStream, PlanIds},
    session::Session,
    translation::relation_to_stream,
};

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

        let finished = context.finished();

        let stream = relation_to_stream(command, context)
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(|e| Status::internal(e.to_string()))
            .chain(stream::once(ready(Ok(finished))));

        Ok(Box::pin(stream))
    }
}
