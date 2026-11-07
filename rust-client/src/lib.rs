//! # RockServer Rust Client
//!
//! A modern, async, and robust Rust client for RockServer using gRPC.
//!
//! ## Overview
//!
//! This crate provides a high-level API to interact with RockServer instances.
//! It supports all core features including:
//! - Transaction Management
//! - Column Management (Schema creation/deletion)
//! - CRUD Operations (Put, Get, Delete, Merge)
//! - Batching and Multi-Key Operations
//! - Iterators and Range Scans
//! - Bounded Range Pages with Exclusive Continuations
//!
//! ## Write order
//!
//! Writes that are not awaited have no relative order, on one node or replicated: two
//! futures of writes to the same key, polled together, may apply in either order. Order
//! comes from awaiting each write before sending the next, from an update of the key, a
//! transaction, or a batch (`put_batch` sends its chunks one after another).
//!
//! ## Example
//!
//! ```rust,no_run
//! use rockserver_client::{RockserverClient, ColumnSchema, ColumnHashType};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = RockserverClient::connect(
//!         "http://[::1]:50051",
//!         rockserver_client::RequestContext::latency(std::time::Duration::from_secs(5)),
//!     ).await?;
//!     
//!     // Create a column
//!     let schema = ColumnSchema {
//!         fixed_keys: vec![8], // 64-bit integer key
//!         variable_tail_keys: vec![],
//!         has_value: true,
//!         merge_operator_name: None,
//!         merge_operator_version: None,
//!     };
//!     
//!     // ... use client ...
//!     Ok(())
//! }
//! ```

pub mod proto {
    #![allow(clippy::all)] // Suppress warnings in generated code
    #![allow(non_camel_case_types)]
    tonic::include_proto!("it.cavallium.rockserver.core.common.api.proto");
}

mod client;
pub mod sessions;
mod types;

pub use client::*;
pub use types::*;
