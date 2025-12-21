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
//! - Change Data Capture (CDC) streaming
//!
//! ## Example
//!
//! ```rust,no_run
//! use rockserver_client::{RockserverClient, ColumnSchema, ColumnHashType};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = RockserverClient::connect("http://[::1]:50051").await?;
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

pub mod datagen;
mod types;
mod client;

pub use types::*;
pub use client::*;
