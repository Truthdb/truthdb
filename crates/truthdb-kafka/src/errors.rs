//! The Kafka error codes the listener answers with.

pub const UNKNOWN_SERVER_ERROR: i16 = -1;
pub const NONE: i16 = 0;
pub const OFFSET_OUT_OF_RANGE: i16 = 1;
pub const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
pub const COORDINATOR_NOT_AVAILABLE: i16 = 15;
pub const ILLEGAL_GENERATION: i16 = 22;
pub const INCONSISTENT_GROUP_PROTOCOL: i16 = 23;
pub const INVALID_GROUP_ID: i16 = 24;
pub const UNKNOWN_MEMBER_ID: i16 = 25;
pub const INVALID_SESSION_TIMEOUT: i16 = 26;
pub const REBALANCE_IN_PROGRESS: i16 = 27;
pub const UNSUPPORTED_SASL_MECHANISM: i16 = 33;
pub const UNSUPPORTED_VERSION: i16 = 35;
pub const SASL_AUTHENTICATION_FAILED: i16 = 58;
pub const FETCH_SESSION_ID_NOT_FOUND: i16 = 70;
pub const INVALID_FETCH_SESSION_EPOCH: i16 = 71;
pub const MEMBER_ID_REQUIRED: i16 = 79;
