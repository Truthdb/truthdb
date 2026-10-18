//! TruthDB's S3-compatible HTTP API: path-style REST/XML requests, signed
//! with AWS Signature Version 4, served over the object engine.

pub mod api;
pub mod error;
pub mod payload;
pub mod server;
pub mod sigv4;
pub mod time;
