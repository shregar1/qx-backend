//! qx-workflow — durable task DAG primitives.
//!
//! A `Task` is a unit of work with a status (Pending → Running → Done/Failed/Cancelled),
//! an optional parent, and binary payload/result. `Engine` is the durable store.
//! Cancellation propagates: cancelling a task cancels all non-terminal descendants
//! atomically.

mod engine;
mod error;
mod task;

pub use engine::Engine;
pub use error::{Result, WorkflowError};
pub use task::{Task, TaskId, TaskStatus};
