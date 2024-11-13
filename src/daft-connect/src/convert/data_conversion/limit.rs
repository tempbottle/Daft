use eyre::ensure;
use futures::Stream;
use spark_connect::{ExecutePlanResponse, Limit};

use crate::command::PlanIds;

pub fn limit(
    limit: Limit,
    context: &PlanIds,
) -> eyre::Result<impl Stream<Item = eyre::Result<ExecutePlanResponse>> + Unpin> {
    let Limit { input, limit } = limit;

    ensure!(limit >= 0, "limit must be greater than or equal to 0");
}
