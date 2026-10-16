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
//! - Change Data Capture (CDC) streaming
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

// Preserve the protobuf package hierarchy for cross-package RequestContext references.
mod generated {
    pub mod it {
        pub mod cavallium {
            pub mod rockserver {
                pub mod core {
                    pub mod common {
                        pub mod api {
                            pub mod proto {
                                #![allow(clippy::all, non_camel_case_types)]
                                tonic::include_proto!(
                                    "it.cavallium.rockserver.core.common.api.proto"
                                );
                            }
                            pub mod export {
                                pub mod proto {
                                    #![allow(clippy::all, non_camel_case_types)]
                                    tonic::include_proto!(
                                        "it.cavallium.rockserver.core.common.api.export.proto"
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    pub mod rockserver {
        pub mod schema { tonic::include_proto!("rockserver.schema"); }
        pub mod cdc {
            #![allow(clippy::all, non_camel_case_types)]
            tonic::include_proto!("rockserver.cdc");
        }
    }
}
pub use generated::it::cavallium::rockserver::core::common::api::export::proto as export_proto;
pub use generated::it::cavallium::rockserver::core::common::api::proto;
pub use generated::rockserver::cdc as cdc_proto;
pub mod cdc;
pub use cdc::CdcClient;
pub mod export;
pub use export::ExportClient;

mod client;
mod types;

pub use client::*;
pub use types::*;

pub mod remote_mutation;
pub use remote_mutation::*;

pub mod replica_physical;
pub use replica_physical::*;

pub use generated::rockserver::schema as schema_proto;
pub mod schema_operations;
pub use schema_operations::*;
