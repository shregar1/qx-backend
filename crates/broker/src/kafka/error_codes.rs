//! Kafka protocol error codes.
//!
//! Only the codes we actually surface. Full list:
//! <https://kafka.apache.org/protocol.html#protocol_error_codes>.

#![allow(dead_code)]

pub const NONE: i16 = 0;
pub const UNKNOWN_SERVER_ERROR: i16 = -1;
pub const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
pub const LEADER_NOT_AVAILABLE: i16 = 5;
pub const NOT_LEADER_OR_FOLLOWER: i16 = 6;
pub const REQUEST_TIMED_OUT: i16 = 7;
pub const BROKER_NOT_AVAILABLE: i16 = 8;
pub const MESSAGE_TOO_LARGE: i16 = 10;
pub const NETWORK_EXCEPTION: i16 = 13;
pub const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;
pub const COORDINATOR_NOT_AVAILABLE: i16 = 15;
pub const NOT_COORDINATOR: i16 = 16;
pub const INVALID_TOPIC_EXCEPTION: i16 = 17;
pub const UNKNOWN_MEMBER_ID: i16 = 25;
pub const REBALANCE_IN_PROGRESS: i16 = 27;
pub const INVALID_GROUP_ID: i16 = 24;
pub const UNSUPPORTED_VERSION: i16 = 35;
pub const TOPIC_ALREADY_EXISTS: i16 = 36;
pub const INVALID_PARTITIONS: i16 = 37;
pub const INVALID_REQUEST: i16 = 42;
pub const UNSUPPORTED_FOR_MESSAGE_FORMAT: i16 = 43;
pub const OUT_OF_ORDER_SEQUENCE_NUMBER: i16 = 45;
pub const DUPLICATE_SEQUENCE_NUMBER: i16 = 46;
pub const INVALID_PRODUCER_EPOCH: i16 = 47;
pub const INVALID_PRODUCER_ID_MAPPING: i16 = 49;
pub const OFFSET_OUT_OF_RANGE: i16 = 1;
