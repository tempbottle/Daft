//! Translation between Spark Connect and Daft

mod logical_plan;
mod schema;
mod stream;

pub use logical_plan::to_logical_plan;
pub use schema::relation_to_schema;
pub use stream::relation_to_stream;
