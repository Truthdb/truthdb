//! A Kafka-compatible listener over the streams engine, for consumers:
//! Kafka clients read topics, commit offsets and balance partitions in
//! consumer groups as they would against a single Kafka broker. It speaks
//! Kafka's protocol in the versions before its flexible encoding (Kafka
//! 2.3's, which current clients still speak), over TLS only, and
//! authenticates with SASL PLAIN against the S3 API's access keys. Topics
//! are created, and written, by the server (`stream create topic`, bucket
//! event notifications): there is no Produce.

mod api;
mod batch;
mod errors;
mod group;
mod server;
mod wire;

pub use api::KafkaState;
pub use group::Coordinator;
pub use server::serve;
