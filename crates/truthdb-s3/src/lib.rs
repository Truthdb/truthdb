//! TruthDB's S3-compatible HTTP API: path-style REST/XML requests, signed
//! with AWS Signature Version 4, served over the object engine.

pub mod api;
pub mod authz;
pub mod copy;
pub mod delete;
pub mod error;
pub mod iam_api;
pub mod listing;
pub mod multipart;
pub mod object_lock;
pub mod objects;
pub mod payload;
pub mod policy;
pub mod post;
pub mod public_access;
pub mod restore;
pub mod scope;
pub mod server;
pub mod signed_body;
pub mod sigv4;
pub mod sse;
pub mod ssec;
pub mod tagging;
pub mod time;
pub mod vectors;
